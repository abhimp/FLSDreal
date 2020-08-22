import io
import math
import time
import json
import multiprocessing as mp
import random
import os
import sys
import numpy as np
# from urllib.request import urlopen
from urllib.request import urljoin
import requests


from util.videoHandlerAsync import VideoHandler
from util.videoHandlerAsync import VideoStorage
# from . import multiprocwrap as mp
from . import cprint
from . import nestedObject
from util.misc import CallableObj
from util.misc import getTraceBack

def assertLog(cond, *k):
    if not cond:
        cprint.red("Assert failed:", *k)
    assert cond

class AutoUpdateObject():
    def __init__(self, callback, onUpdateCB=None):
        self.__int_values = {}
        self.__int_cb = callback
        self.__int_autoCommit = False
        self.__int_readonly = False
        self.__int_onUpdateCB = onUpdateCB
        self.__int_changed = set()

    def __getattr__(self, name):
        if name.startswith("__int_") or name.startswith("_AutoUpdateObject"):
            raise AttributeError(f"{name} not found")
        if name not in self.__int_values:
            raise AttributeError(f"{name} not found")
        return self.__int_values[name]

    def __setattr__(self, name, val):
        if name.startswith("__int_") or name.startswith("_AutoUpdateObject"):
            super().__setattr__(name, val)
            return
        if self.__int_readonly:
            raise PermissionError("Readonly object")
        self.__int_values[name] = val
        self.__int_changed.add(name)
        if self.__int_autoCommit:
            self.commit()
        if self.__int_onUpdateCB and callable(self.__int_onUpdateCB):
            self.__int_onUpdateCB(name)

    def update(self, kwargs):
        self.__int_values.update(kwargs)

    def setAutoCommit(self, autoCommit = True):
        if self.__int_cb is not None and callable(self.__int_cb):
            return
        self.__int_autoCommit = autoCommit

    def setReadOnly(self, readonly=True):
        self.__int_readonly = readonly

    def commit(self):
        if self.__int_readonly:
            raise PermissionError("Readonly object")
        if self.__int_cb is None or not callable(self.__int_cb):
            return
        keys = self.__int_changed.copy()
        self.__int_changed.clear()
        self.__int_cb({x: self.__int_values[x] for x in keys})

    def getCurrentStatus(self):
        return self.__int_values

    def getCb(self):
        return self.__int_cb

def toDict(**kw):
    return kw

def inWorker(func):
    def wrapperRunInWorker(ref, *a, **b):
        ref.mRunInWorkerThread(func, ref, *a, **b)
    return wrapperRunInWorker

def inMain(func):
    def wrapperRunInMain(ref, *a, **b):
        ref.mRunInMainThread(func, ref, *a, **b)
    return wrapperRunInMain

class GroupRpc:
    def __init__(self, eloop):
        self.vMyGid = None #same as my addr
        self.vEloop = eloop
        self.vAddress = None

        #======PEER STATS=======
        self.vIdle = True
        self.vWorkingFrom = None
        self.vIdleFrom = time.time()

    def mRunInMainThread(self, func, *a, **b):
        self.vEloop.addTask(func, *a, **b)

    def mRunInWorkerThread(self, func, *a, **b):
        if self.vEloop.amIMainThread():
            self.vEloop.runInWorker(func, *a, **b)
            return
        cllObj = CallableObj(self.vEloop.runInWorker, func)
        self.vEloop.addTask(cllObj, *a, **b)

    @inWorker
    def mFetch(self, url, cb):
        st = time.time()
        ret = None
        resp = requests.get(url)
        if resp.status_code == 200:
            ret = 200, resp.content, resp.headers, st, time.time()
        else:
            ret = resp.status_code, None, resp.headers, st, time.time()
        cb(*ret)

    def mGroupRecvRpc(self, cb, content):
        rpc = None
        try:
            rpc = json.loads(content)
        except:
            return self.mGroupRpcResponse(cb, error="Json parsing error")

        func = rpc.get("func", "noop")
        a = rpc.get("args", [])
        b = rpc.get("kwargs", {})
        if not hasattr(self, func):
            return self.mGroupRpcResponse(cb, error=f"{func} not found")

        func = getattr(self, func)
        if not callable(func):
            return self.mGroupRpcResponse(cb, error=f"{func} not found")

        try:
            ret = func(*a, **b)
        except:
            return self.mGroupRpcResponse(cb, error=getTraceBack(sys.exc_info())) #Sending exception

        self.mGroupRpcResponse(cb, ret)

    def mGroupRpcResponse(self, cb, ret = None, error=None):
        if error is None:
            self.mRunInMainThread(cb, toDict(res="ok", ret=ret))
            return
        self.mRunInMainThread(cb, toDict(res="error", error=error))

    def mGroupSendRpc(self, cb, func, *a, **b): #blocking, so run in worker
        assert self.vAddress is not None
        if callable(func):
            func = func.__name__
        url = self.vAddress + "/groupcomm"
        rpc = {"func": func, "args": a, "kwargs": b}
        resp = requests.post(url, data = json.dumps(rpc).encode())
        ret = None
        if resp.status_code == 200:
            ret = resp.content
        if ret is not None:
            try:
                ret = json.loads(ret.decode())
            except:
                ret = None

        if ret is not None:
            res = ret.get("res", None)
            if res == "ok":
                ret = ret.get("ret")
            elif res == "error":
                cprint.red("RPC res:", ret.get("error"))
                return
            else:
                cprint.red("RPC error unknow")
                return
        cb(ret) #cb should run in main thread

    def mGetRpcObj(self, name, cb=None):
        assert self.vAddress is not None
        if cb is None:
            cb = self.mNoop
        cllObj = CallableObj(self.mGroupSendRpc, cb, name)
        return cllObj

    def mNoop(self, *a, **b):
        pass


class DummyPlayer(GroupRpc):
    def __init__(self, eloop, options):
        super().__init__(eloop)
        self.vOptions = options
        self.vVidHandler = VideoHandler(options.mpdPath)
        self.vVidStorage = VideoStorage(self.vEloop, self.vVidHandler, options.tmpDir)

        self.vPlaybackTime = 0 # seconds
        self.vPlayerBuffers = [] # seconds
        self.vTotalStalled = 0 # seconds
        self.vSetPlaybackTime = 0

        self.vStartPlaybackTime = -1  # seconds
        self.vNextSegId = 0
        self.vNextBuffVidSegId = 0
        self.vNextBuffAudSegId = 0
        self.vStartSengId = -1
        self.vMinBufferLength = 30

        #========Group Info================
        self.vGroupStarted = False # atleast neighbor require
        self.vGroupStartedSegId = float('inf')
        self.vGroupInited = False
        self.vIAmStarter = False
        self.vNeighbors = {}

        self.vGrpDownloadQueue = []
        self.vGrpDownloading = False
        self.vGrpNextSegIdAsIAmTheLeader = -1 #value become positive when I am leader else it stays negetive
        #==================================

        self.mInit()

    def mInit(self):
        dur = self.vVidHandler.getSegmentDur()
        self.vSetPlaybackTime = self.vVidHandler.expectedPlaybackTime()
        if self.vStartPlaybackTime == -1:
            self.vStartPlaybackTime = self.vSetPlaybackTime
#         print(self.vSetPlaybackTime, dur)
        self.vNextSegId = int(self.vSetPlaybackTime/dur)
        self.vStartSengId = self.vNextBuffVidSegId = self.vNextBuffAudSegId = self.vNextSegId

    def mGetJson(self):
        return self.vVidHandler.getJson()

    def mBOLA(self, segId):
        typ = 'video'

        V = 0.93
        lambdaP = 5 # 13
        sleepTime = 0
        chunkHistory = self.vVidStorage.getDownloadHistory(typ) # [[segid, qlid, download_id]]
        if len(chunkHistory) == 0:
            return 0
        if self.vPlaybackTime < self.vStartPlaybackTime:
            return 0

        p = self.vVidHandler.getSegmentDur()
        bufUpto = p*segId
        buflen = bufUpto - self.vPlaybackTime

#         dlDetail = self.videoHandler.getDownloadStat(chunkHistory[-1][2]) # start (Sec), send (sec), clen(bytes)
        (segId, ql), clen, sttime, entime = chunkHistory[-1]
        lastThroughput = clen * 8 / (entime - sttime) # bps

        bitrates = self.vVidHandler.getBitrates(typ)
        SM = float(bitrates[-1])
        vms = [math.log(sm/SM) for sm in bitrates]

        lastM = ql #last bitrateindex
        Q = buflen/p
        Qmax = self.vMinBufferLength/p
        ts = self.vPlaybackTime - self.vStartPlaybackTime
        te = ts + 1 # bad hack, it should have been video.duration - playbacktime
        t = min(ts, te)
        tp = max(t/2, 3 * p)
        QDmax = min(Qmax, tp/p)

        VD = (QDmax - 1)/(vms[0] + lambdaP)

        M = np.argmax([((VD * vms[m] + VD*lambdaP - Q)/sm) \
                for m,sm in enumerate(bitrates)])
        M = int(M)
        if M < lastM:
            r = lastThroughput #throughput
            mprime = min([m for m,sm in enumerate(bitrates) if sm/p < max(r, SM)] + [len(bitrates)])
            mprime = 0 if mprime >= len(bitrates) else mprime
            if mprime <= M:
                mprime = M
            elif mprime > lastM:
                mprime = lastM
            elif False: #some special parameter TODO
                pass
            else:
                mprime = mprime - 1
            M = mprime
#         sleepTime = max(p * (Q - QDmax + 1), 0)
#         cprint.red("BOLA returns:", M, VD, (QDmax, vms,  lambdaP, lastThroughput), [((VD * vms[m] + VD*lambdaP - Q)/sm) \
#                 for m,sm in enumerate(bitrates)])
        return M

    @inMain
    def mBuffered(self, cb, typ, segId, ql, status, resp, headers, st, ed):
        assert status == 200
        headers = dict(headers)
        dt = headers.get('X-Chunk-Sizes', "{}")
        dt = json.loads(dt)
        self.vVidStorage.updateChunkSizes(dt)
        self.vVidStorage.storeChunk(typ, segId, ql, resp, st, ed)

        cb()

    def mBufferVideo(self, cb):
        segDur = self.vVidHandler.getSegmentDur()
        curPlaybackTime = self.vPlaybackTime if self.vPlaybackTime >= self.vSetPlaybackTime else self.vSetPlaybackTime
        bufVidUpto = self.vNextBuffVidSegId * segDur
        bufVidLen = bufVidUpto - curPlaybackTime

        if bufVidLen >= self.vMinBufferLength:
            return cb()

        if len(self.vVidStorage.getAvailability('video', self.vNextBuffVidSegId, '*')) > 0:
            self.vNextBuffVidSegId += 1
            return cb()

        ql = 0
        if self.vGroupStarted and (self.vGroupStartedSegId <= self.vNextBuffVidSegId or self.vIAmStarter):
            if self.vIAmStarter:
                self.vIAmStarter = False
#                 self.mGrpSetDownloader(self.vNextBuffVidSegId)
                self.vGroupStartedSegId = self.vNextBuffVidSegId
                self.mBroadcast(self.mGrpSetDownloader, self.vMyGid, self.vNextBuffVidSegId)
                return cb()
            if bufVidLen > (2*segDur):
                return cb()
            else:
                cprint.blue(f"{self.vMyGid}: {self.vNextBuffVidSegId} fallbacking, groupstarted: {self.vGroupStartedSegId}")
                ql = 0 #fallback
        else:
            ql = self.mBOLA(self.vNextBuffVidSegId)

        url = self.vVidHandler.getChunkUrl('video', self.vNextBuffVidSegId, ql)
        cllObj = CallableObj(self.mBuffered, cb, 'video', self.vNextBuffVidSegId, ql)
        self.mFetch(url, cllObj)
        self.vNextBuffVidSegId += 1
        if self.vNextBuffAudSegId - self.vStartSengId == 3:
            self.mGroupStart()

    def mBufferAudio(self, cb):
        segDur = self.vVidHandler.getSegmentDur()
        curPlaybackTime = self.vPlaybackTime if self.vPlaybackTime >= self.vSetPlaybackTime else self.vSetPlaybackTime
        bufAudUpto = self.vNextBuffAudSegId * segDur
        bufAudLen = bufAudUpto - curPlaybackTime

        if bufAudLen >= self.vMinBufferLength:
            return cb()

        if self.vVidStorage.getAvailability('audio', self.vNextBuffAudSegId, 0):
            self.vNextBuffAudSegId += 1
            return cb()
        url = self.vVidHandler.getChunkUrl('audio', self.vNextBuffAudSegId, 0)
        cllObj = CallableObj(self.mBuffered, cb, 'audio', self.vNextBuffAudSegId, 0)
        self.mFetch(url, cllObj)
        self.vNextBuffAudSegId += 1

    def mSendResponse(self, cb):
        if self.vVidStorage.ended(self.vNextSegId):
            return cb({}, [{"eof" : True}], [], 0)

        actions = {}
        if self.vSetPlaybackTime > 0:
            buffered = False
            for x in self.vPlayerBuffers:
                if x[0] <= self.vSetPlaybackTime and x[1] >= self.vSetPlaybackTime:
                    buffered = True
                    break
            if buffered:
                actions["seekto"] = self.vSetPlaybackTime
                self.vSetPlaybackTime = -1

        segDur = self.vVidHandler.getSegmentDur()
        curPlaybackTime = self.vPlaybackTime if self.vPlaybackTime >= self.vSetPlaybackTime else self.vSetPlaybackTime
        bufUpto = self.vNextSegId * segDur
        bufLen = bufUpto - curPlaybackTime

        if bufLen > segDur:
            return cb(actions, [], [], 0)

        vql = self.vVidStorage.getAvailableMaxQuality('video', self.vNextSegId)
        if vql < 0:
            return cb(actions, [], [], 0)

        qualities = toDict(audio = 0, video = vql)
        l = 0
        segs = []
        fds = []
        for mt in ["audio", "video"]:
            ql = qualities[mt]
            seg = {}
            seg['seg'] = self.vNextSegId
            seg['type'] = mt
            seg['rep'] = ql
            seg['ioff'] = l
            seg['ilen'] = self.vVidStorage.getChunkSize(mt, 'init', ql)
            l += seg['ilen']
            fds += [self.vVidStorage.getFileDescriptor(mt, 'init', ql)]

            seg['coff'] = l
            seg['clen'] = self.vVidStorage.getChunkSize(mt, self.vNextSegId, ql)
            l += seg['clen']
            fds += [self.vVidStorage.getFileDescriptor(mt, self.vNextSegId, ql)]
            segs += [seg]

        self.vNextSegId += 1
#         cprint.red("playbackTime:", self.vPlaybackTime, "expectedPlaybackTime:", self.vVidHandler.expectedPlaybackTime())
        cb(actions, segs, fds, l)

    def mGetNextChunks(self, cb, playbackTime, buffers, totalStalled):
        self.vPlayerBuffers = buffers[:]
        self.vPlaybackTime = playbackTime
        self.vTotalStalled = totalStalled

        cllObj = CallableObj(self.mSendResponse, cb)
        cllObj = CallableObj(self.mBufferVideo, cllObj)
        self.mBufferAudio(cllObj) #call sequence, audio->video->response

#================================================
# Core algorithm
#================================================
    def mGroupSelectNextDownloader(self):
        peers = list(self.vNeighbors.values()) + [self]
        gids = [p.vMyGid for p in peers]
        now = time.time()
        idleTimes = [0 if p.vIdleFrom is None else (now - p.vIdleFrom) for p in peers]
        workingTimes = [0 if p.vWorkingFrom is None else (now - p.vWorkingFrom) for p in peers]
        idleTimes = np.array(idleTimes)
        workingTimes = np.array(workingTimes)
        cprint.orange(f"gids: {gids} idleTimes: {idleTimes} workingTimes: {workingTimes}")

        res = idleTimes - workingTimes
        downloader = np.argmax(res)
        downloader = peers[downloader]
        return downloader #return arbit (first) peer if none of them are idle

    def groupSelectNextQuality(self, segId):
        now = time.time()
        peers = list(self.vNeighbors.keys()) + [self]
        segDur = self.vVidHandler.getSegmentDur()
        deadLine = segId*segDur - max([n.status.playbackTime for n in peers])

        self.deadLines[segId] = now + deadLine
        prog = self.status.dlQLen * 100

        targetQl = max(self.videoHandler.getCachedQuality(self.groupStartedFromSegId - 1, "video"))
        futureQl = None
        pastQl = None
        pastQls = []
        i = 1
        while segId - i >= self.groupStartedFromSegId:
            try:
                if segId + 1 in self.groupInfo.segQuality and futureQl is None:
                    futureQl = max([q for gid, q in self.groupInfo.segQuality[segId + 1].items()])
                    assertLog(futureQl >= 0, f"futureQl={futureQl}")
                if segId - 1 in self.groupInfo.segQuality:
                    pql = max([q for gid, q in self.groupInfo.segQuality[segId - 1].items()])
                    if pastQl is None: pastQl = pql
                    pastQls += [pql]
                if len(pastQls) > 4: break
            except RuntimeError:
                continue

        clens = [int(segDur * x / 8) for x in self.videoHandler.vidInfo["bitrates"]]

        if segId in self.groupInfo.sizes["video"]:
            segInfo = self.groupInfo.sizes["video"][segId]
            clens = [segInfo[x] for x in segInfo]


        last5dls = np.array(self.videoHandler.downloadStat[-5:])
        last5Time = (last5dls[:, 1]-last5dls[:, 0])
        last5Thrpt = last5dls[:, 2] / last5Time # byte/sec
        hthrpt = [1/x for x in last5Thrpt]  #harmonic average
        hthrpt = len(hthrpt)/sum(hthrpt)


        thrpt = min(hthrpt, last5Thrpt[-1], self.videoHandler.weightedThroughput/8) #BYTE/SEC

        if deadLine <= 0:
            return 0 #lowest quality as there is no time

        times = [(deadLine - cl/thrpt, i) for i, cl in enumerate(clens)]
        times = [x for x in times if x[0] > 0]
        if len(times) == 0: #trivial case for
            return 0
        times.sort(key = lambda x:x[0])
        ql = times[0][1]
        if futureQl is not None and ql > futureQl:
            cprint.orange(segId, "futureQl", futureQl)
            return futureQl
        if pastQl is not None and ql > pastQl:
            if len(pastQls) >= 4 and min(pastQls) == max(pastQls):
                cprint.orange(segId, "pastQl + 1", pastQl + 1)
                return pastQl + 1
            else:
                cprint.orange(segId, "pastQl", pastQl)
                return pastQl

        cprint.orange(segId, "targetQl", targetQl)
        return targetQl


        #targetQl = lastQl[-1] if len(lastQl) > 1 else self._vAgent._vQualitiesPlayed[-1]
        ql = random.choice(self.videoQualities)
        return ql #TODO add algo

#================================================
# group related task
#================================================
    def mGroupStart(self):
        if self.vOptions.neighbourAddress is None: # I am the starter
            self.vGroupInited = True
            self.vIAmStarter = True
            cprint.red("Group started as self group")
            return
        self.mRunInWorkerThread(self.mGroupStartInWorker)

    def mGroupStartInWorker(self):
        rpc = (self.vOptions.groupPort, self.vOptions.neighbourAddress) #TODO add more to send more info
        resp = requests.post("http://" + self.vOptions.neighbourAddress + "/groupjoin", data = json.dumps(rpc).encode())
        ret = None
        if resp.status_code == 200:
            ret = resp.content
        else:
            return
        self.mGroupStartRecvReply(ret)

    def mGroupStartRecvReply(self, reply):
        assert reply is not None and len(reply) > 0
        reply = json.loads(reply)
        if reply[0] == "NotStarted":
            cprint.red("other side not ready yet. restarting after 2s")
            self.vEloop.setTimeout(2, self.mGroupStart)
            return
        elif reply[0] == "denied":
            raise NotImplementedError("denied not implemented") #TODO
        elif reply[0] != "accepted":
            raise NotImplementedError("Error") #TODO

        #accepted
        status, myAddr, neighbors = reply
        self.vMyGid = myAddr
        self.vNeighbors = {}
        for x, idle in neighbors:
            if x == self.vMyGid:
                continue
            peer = GroupRpc(self.vEloop)
            peer.vMyGid = x
            peer.vAddress = 'http://' + x
            peer.vIdle = idle
            if not idle:
                peer.vIdleFrom = None
                peer.vWorkingFrom = time.time()
            self.vNeighbors[x] = peer
        self.vGroupInited = True
        cprint.red(f"Group started started {len(self.vNeighbors)} neighbors")

    def mGroupJoin(self, cb, peerIp, content):
        if not self.vGroupInited:
            return cb(("NotStarted",))
        data = None
        try:
            data = json.loads(content)
        except:
            return
        peerPort, myAddr = data
        if self.vMyGid is None:
            self.vMyGid = myAddr.split(":")[0] + ":" + str(self.vOptions.groupPort)
            self.vGroupStarted = True #Iamstarted

        peerAddr = f"{peerIp}:{peerPort}"
        nPeers = list(self.vNeighbors.values()) + [self]
        nInfo = [(p.vMyGid, p.vIdle) for p in nPeers]
        self.mBroadcast(self.mGroupPeerJoined, peerAddr)
        cb(("accepted", peerAddr, nInfo))

    @inWorker
    def mBroadcast(self, func, *a, **b):
#         cprint.cyan(f"broadcasting {func}")
        funcname = func.__name__
        for gid,peer in self.vNeighbors.items():
            func = peer.mGetRpcObj(funcname)
            func(self.vMyGid, *a, **b)
        func = getattr(self, funcname)
        self.mRunInMainThread(func, self.vMyGid, *a, **b) #need to run in main thread

    def mAddToGroupDownloadQueue(self, segId):
        if not self.vVidHandler.isSegmentAvaibleAtTheServer(segId):
            wait = self.vVidHandler.timeToSegmentAvailableAtTheServer(segId)
            self.vEloop.setTimeout(wait, self.mAddToGroupDownloadQueue, segId)
            return
        if self.vGrpDownloading:
            self.vGrpDownloadQueue.append(segId)
            return
        self.mBroadcast(self.mGrpSetIdle, False)
        self.mStartGrpDownloading(segId)

    @inMain
    def mStartGrpDownloading(self, segId):
        assert not self.vGrpDownloading
        self.vGrpDownloading = True
        ql = 0 #FIXME select quality based on the group
        cllObj = CallableObj(self.mGrpDownloaded, segId, ql)
        url = self.vVidHandler.getChunkUrl('video', segId, ql)
        self.mFetch(url, cllObj)

    @inMain
    def mGrpDownloaded(self, segId, ql, status, resp, headers, st, ed):
        headers = dict(headers)
        dt = headers.get('X-Chunk-Sizes', "{}")
        dt = json.loads(dt)
        self.mBuffered(self.mNoop, 'video', segId, ql, status, resp, headers, st, ed)
        self.mBroadcast(self.mGrpInformDownloaded, segId, ql, dt)
        self.vGrpDownloading = False
        if len(self.vGrpDownloadQueue) > 0:
            segId = self.vGrpDownloadQueue.pop(0)
            self.mStartGrpDownloading(segId) #seg is available as it is present in queue
        else:
            self.mBroadcast(self.mGrpSetIdle, True)

    def mGrpSelectNextLeader(self):
        if self.vGrpNextSegIdAsIAmTheLeader < 0:
            return
#         cprint.orange(f"Trying to find leader for segId: {self.vGrpNextSegIdAsIAmTheLeader}")
        nextSegId = self.vGrpNextSegIdAsIAmTheLeader
        if not self.vVidHandler.isSegmentAvaibleAtTheServer(nextSegId):
            wait = self.vVidHandler.timeToSegmentAvailableAtTheServer(nextSegId)
#             cprint.orange(f"Need to wait for {wait} before scheduling: segId: {self.vGrpNextSegIdAsIAmTheLeader}")
            self.vEloop.setTimeout(wait, self.mGrpSelectNextLeader)
            return
#         idles = [p for p in list(self.vNeighbors.values())+[self] if p.vIdle]
#         if len(idles) == 0:
#             return
#         peer = idles.pop(0) #FIXME select leader properly
        peer = self.mGroupSelectNextDownloader()
        if not peer.vIdle: #will try again when some peer gets free
#             cprint.orange(f"Need to wait while scheduling: segId: {self.vGrpNextSegIdAsIAmTheLeader}: {peer.vMyGid} is idle {peer.vIdle}")
            return

        self.vGrpNextSegIdAsIAmTheLeader = -1
        if self.vVidStorage.ended(nextSegId):
            cprint.orange("GAME OVER")
            return # end of video

#         cprint.orange(f"Found leader for segId: {nextSegId} => {peer.vMyGid}")
        self.mBroadcast(self.mGrpSetDownloader, peer.vMyGid, nextSegId)

    def mGrpMediaRequest(self, cb, content):
        try:
            typ, segId, ql = json.loads(content)
            cprint.blue(f"Request recv: {(typ, segId, ql)}")
            ln = self.vVidStorage.getChunkSize(typ, segId, ql)
            fd = self.vVidStorage.getFileDescriptor(typ, segId, ql)
            self.mRunInWorkerThread(cb, fd, ln)
        except:
            cprint.red(getTraceBack(sys.exc_info()))
            cb(None, None, 503)



#===================GROUP COMM=================
    def mGrpInformDownloaded(self, gid, segId, ql, dt):
        if gid == self.vMyGid:
            return
        peer = self.vNeighbors[gid]
#         url = urljoin(peer.address, f"/grpmedia/video_{segId}_{ql}")
        self.vVidStorage.storeRemoteChunk('video', segId, ql, peer.vAddress)
        self.vVidStorage.updateChunkSizes(dt)

    def mGrpSetDownloader(self, srcGid, gid, segId):
        cprint.green(f"{self.vMyGid}: {gid} is supposed to download seg {segId}")
        if not self.vGroupStarted:
            self.vGroupStarted = True
            self.vGroupStartedSegId = segId
        if gid != self.vMyGid:
            return
        self.vGrpNextSegIdAsIAmTheLeader = segId + 1
        self.mAddToGroupDownloadQueue(segId)
        self.mGrpSelectNextLeader()

    def mGrpSetIdle(self, srcGid, status):
        peer = self
        cprint.orange(f"status from {srcGid}: {status}")
        if srcGid != self.vMyGid:
            peer = self.vNeighbors[srcGid]
        if peer.vIdle == status: #Can ignore.
            return
        cprint.orange(f"status from {srcGid} updating to {status}")
        peer.vIdle = status
        if status:
            peer.vIdleFrom = time.time()
            peer.vWorkingFrom = None
            self.mGrpSelectNextLeader()
        else:
            peer.vIdleFrom = None
            peer.vWorkingFrom = time.time()

    def mGroupPeerJoined(self, srcGid, peerAddr):
#         if self.vMyGid == srcGid: return
        peer = GroupRpc(self.vEloop)
        peer.vMyGid = peerAddr
        peer.vAddress = "http://" + peerAddr
        self.vNeighbors[peerAddr] = peer

