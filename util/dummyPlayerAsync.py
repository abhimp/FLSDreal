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

class PlayerStat():
    def __init__(self):
        self.vNativePlaybackTime = 0 # seconds
        self.vNativePlayerBuffers = [] # seconds
        self.vNativeTotalStalled = 0 # seconds

        self.vBuffUpto = 0 #should come from nextSegId
        self.vOverAllBufUpto = 0 #should come from nextBufferSegId
        self.vLastUpdatedAt = time.time()

    def mGetBuffers(self):
        return self.vNativePlayerBuffers

    def mGetBufUpto(self, overall=True):
        if overall:
            return self.vOverAllBufUpto
        return self.vBuffUpto

    def mGetPlaybackTime(self):
        now = time.time()
        timeSpent = now - self.vLastUpdatedAt
        expPlaybackTime = self.vNativePlaybackTime + timeSpent
        return min(expPlaybackTime, self.mGetBufUpto())

    def mGetTotalStalled(self):
        now = time.time()
        timeSpent = now - self.vLastUpdatedAt
        expPlaybackTime = self.vNativePlaybackTime + timeSpent
        pTime = min(expPlaybackTime, self.mGetBufUpto())
        newStall = max(0, expPlaybackTime - pTime)

    def mUpdateStat(self, nativePlaybackTime, nativePlayerBuffers, nativeTotalStalled, buffUpto, overAllBufUpto):
        self.vNativePlaybackTime = nativePlaybackTime
        self.vNativePlayerBuffers = nativePlayerBuffers
        self.vNativeTotalStalled = nativeTotalStalled
        self.vBuffUpto = buffUpto
        self.vOverAllBufUpto = overAllBufUpto
        self.vLastUpdatedAt = time.time()

class GroupRpc:
    def __init__(self, eloop):
        self.vMyGid = None #same as my addr
        self.vEloop = eloop
        self.vAddress = None

        #======PEER STATS=======
        self.vIdle = True
        self.vWorkingFrom = None
        self.vIdleFrom = time.time()

        self.vPlayerStat = PlayerStat()


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

    @inWorker
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

        #======Playback Stat====
        self.vNativePlaybackTime = 0 # seconds
        self.vNativePlayerBuffers = [] # seconds
        self.vNativeTotalStalled = 0 # seconds
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

        self.vSendUpdate = False
        #==================================

        self.vQoeLogFd = None

        self.mInit()

    def mInit(self):
        dur = self.vVidHandler.getSegmentDur()
        self.vSetPlaybackTime = self.vVidHandler.expectedPlaybackTime()
        if self.vStartPlaybackTime == -1:
            self.vStartPlaybackTime = self.vSetPlaybackTime
#         print(self.vSetPlaybackTime, dur)
        self.vNextSegId = int(self.vSetPlaybackTime/dur)
        self.vStartSengId = self.vNextBuffVidSegId = self.vNextBuffAudSegId = self.vNextSegId
        if self.options.logDir is not None:
            self.vQoeLogFd = open(os.path.join(self.options.logDir, "QoE.log"), "w")

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
        if self.vNativePlaybackTime < self.vStartPlaybackTime:
            return 0

        p = self.vVidHandler.getSegmentDur()
        bufUpto = p*segId #it is safe to assume that segment upto segid is already in the buffer
        buflen = bufUpto - self.vNativePlaybackTime

        (segId, ql), clen, sttime, entime = chunkHistory[-1]
        lastThroughput = clen * 8 / (entime - sttime) # bps

        bitrates = self.vVidHandler.getBitrates(typ)
        SM = float(bitrates[-1])
        vms = [math.log(sm/SM) for sm in bitrates]

        lastM = ql #last bitrateindex
        Q = buflen/p
        Qmax = self.vMinBufferLength/p
        ts = self.vNativePlaybackTime - self.vStartPlaybackTime
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
#         cprint.blue("Buffering video vNextBuffVidSegId={self.vNextBuffVidSegId}")
        segDur = self.vVidHandler.getSegmentDur()
        curPlaybackTime = self.vNativePlaybackTime if self.vNativePlaybackTime >= self.vSetPlaybackTime else self.vSetPlaybackTime
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
        self.mFetch(url, cllObj) #calling
        self.vNextBuffVidSegId += 1
        if self.vNextBuffAudSegId - self.vStartSengId == 3:
            self.mGroupStart()

    def mBufferAudio(self, cb):
#         cprint.blue("Buffering audio vNextBuffAudSegId={self.vNextBuffAudSegId}")
        segDur = self.vVidHandler.getSegmentDur()
        curPlaybackTime = self.vNativePlaybackTime if self.vNativePlaybackTime >= self.vSetPlaybackTime else self.vSetPlaybackTime
        bufAudUpto = self.vNextBuffAudSegId * segDur
        bufAudLen = bufAudUpto - curPlaybackTime

        if bufAudLen >= self.vMinBufferLength:
            return cb()

        if self.vVidStorage.getAvailability('audio', self.vNextBuffAudSegId, 0):
            self.vNextBuffAudSegId += 1
            return cb()
        url = self.vVidHandler.getChunkUrl('audio', self.vNextBuffAudSegId, 0)
        cllObj = CallableObj(self.mBuffered, cb, 'audio', self.vNextBuffAudSegId, 0)
        self.mFetch(url, cllObj) #calling
        self.vNextBuffAudSegId += 1

    def mSendResponse(self, cb):
#         cprint.blue("Responding")
        if self.vVidStorage.ended(self.vNextSegId):
            if self.vQoeLogFd is not None:
                self.vQoeLogFd.close()
                self.vQoeLogFd = None
            return cb({}, [{"eof" : True}], [], 0)

        actions = {}
        if self.vSetPlaybackTime > 0:
            buffered = False
            for x in self.vNativePlayerBuffers:
                if x[0] <= self.vSetPlaybackTime and x[1] >= self.vSetPlaybackTime:
                    buffered = True
                    break
            if buffered:
                actions["seekto"] = self.vSetPlaybackTime
                self.vSetPlaybackTime = -1

        segDur = self.vVidHandler.getSegmentDur()
        curPlaybackTime = self.vNativePlaybackTime if self.vNativePlaybackTime >= self.vSetPlaybackTime else self.vSetPlaybackTime
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

        nexts = []
        for mt in ['audio', 'video']:
            n = mt, "init", qualities[mt]
            nexts += [n]
            n = mt, self.vNextSegId, qualities[mt]
            nexts += [n]

        if self.vQoeLogFd is not None:
            ql = qualities['video']
            bitrate = self.videoHandler.getBitrates('video')[ql]
            print(self.vNativePlaybackTime, self.vNativeTotalStalled, ql, bitrate, self.nextSegId, file=self.vQoeLogFd, flush=True)

        mt, segId, ql = nexts.pop(0)
        nextThis = (None, mt, segId, ql)
        cllObj = CallableObj(self.mAppendFds, cb, actions, segs, fds, 0, nextThis, nexts)
        self.mGetFdFromVidStorage(cllObj, mt, segId, ql)

    @inWorker
    def mGetFdFromVidStorage(self, cb, mt, segId, ql):
        self.vVidStorage.getFileDescriptor(cb, mt, segId, ql)

    def mAppendFds(self, cb, actions, segs, fds, tl, this, nexts, fd):
        seg, mt, segId, ql = this
        if segId == 'init':
            seg = {}
            seg['type'] = mt
            seg['rep'] = ql
            seg['ioff'] = tl
            seg['ilen'] = self.vVidStorage.getChunkSize(mt, segId, ql)
            tl += seg['ilen']
        else:
            seg['seg'] = self.vNextSegId
            seg['coff'] = tl
            seg['clen'] = self.vVidStorage.getChunkSize(mt, segId, ql)
            tl += seg['clen']
            segs += [seg]
            seg = None
        fds += [fd]
        if len(nexts) == 0:
            self.mFinishSendingResponse(cb, actions, segs, fds, tl)
            return

        mt, segId, ql = nexts.pop(0)
        nextThis = (seg, mt, segId, ql)
        cllObj = CallableObj(self.mAppendFds, cb, actions, segs, fds, tl, nextThis, nexts)
        self.mGetFdFromVidStorage(cllObj, mt, segId, ql)

    @inMain
    def mFinishSendingResponse(self, cb, actions, segs, fds, l):
        self.vNextSegId += 1
        cb(actions, segs, fds, l)
        self.vSendUpdate = True

    def mGetNextChunks(self, cb, playbackTime, buffers, totalStalled):
        self.vNativePlayerBuffers = buffers[:]
        self.vNativePlaybackTime = playbackTime
        self.vNativeTotalStalled = totalStalled

        cprint.blue(f"CHUNK request playbackTime={playbackTime}, buffers={buffers}, totalStalled={totalStalled}, nextSegId={self.vNextSegId}")

        cllObj = CallableObj(self.mSendResponse, cb)
        cllObj = CallableObj(self.mBufferVideo, cllObj)
        self.mBufferAudio(cllObj) #call sequence, audio->video->response

        if not self.vSendUpdate: return
        self.vSendUpdate = False

        nextBufferSegId = min(self.vNextBuffVidSegId, self.vNextBuffAudSegId)
        segDur = self.vVidHandler.getSegmentDur()
        self.mBroadcast(self.mGroupSetCurStatus,
                        self.vNativePlaybackTime,
                        self.vNativePlayerBuffers,
                        self.vNativeTotalStalled,
                        self.vNextSegId * segDur,
                        nextBufferSegId * segDur)

#================================================
# Core algorithm
#================================================
    def mGroupSelectNextDownloader(self):
        peers = list(self.vNeighbors.values()) + [self]
        gids = [p.vMyGid for p in peers]
        now = time.time()
        idleTimes = [0 if p.vIdleFrom is None else (now - p.vIdleFrom) for p in peers]
        workingTimes = [0 if p.vWorkingFrom is None else (now - p.vWorkingFrom) for p in peers]
        cprint.orange(f"gids: {gids} idleTimes: {idleTimes} workingTimes: {workingTimes}")
        idleTimes = np.array(idleTimes)
        workingTimes = np.array(workingTimes)

        res = idleTimes - workingTimes
        downloader = np.argmax(res)
        downloader = peers[downloader]
        return downloader #return arbit (first) peer if none of them are idle

    def mGroupSelectNextQuality(self, segId):
        typ = 'video'
        now = time.time()
        peers = list(self.vNeighbors.values()) + [self]
        segDur = self.vVidHandler.getSegmentDur()
#         deadLine = segId*segDur - max([n.vPlayerStat.mGetPlaybackTime() for n in peers])
#
#         self.deadLines[segId] = now + deadLine
#         prog = self.status.dlQLen * 100

        targetQl = -1
        lastSegId = segId - 1
        while targetQl < 0:
            targetQl = self.vVidStorage.getAvailableMaxQuality(typ, lastSegId)
            lastSegId -= 1

#         assert segId - lastSegId < 4

        curMaxPlaybackTime = max([n.vPlayerStat.mGetPlaybackTime() for n in peers])
        timeToDl = segId*segDur - curMaxPlaybackTime

        if timeToDl < 1: #no time to dl
            return 0

        chunkHistory = self.vVidStorage.getDownloadHistory(typ)
        x, last5clens, last5sttimes, last5entimes = zip(*chunkHistory[-5:])
        #(segId, ql), clen, sttime, entime = chunkHistory[-1]

        last5clens = np.array(last5clens)
        last5sttimes = np.array(last5sttimes)
        last5entimes = np.array(last5entimes)
        last5durs = last5entimes - last5sttimes
        last5thrps = last5clens / last5durs #bytes/sec

        hthrpt = [1/x for x in last5thrps]  #harmonic average
        hthrpt = len(hthrpt)/sum(hthrpt)
        thrpt = min(hthrpt, last5thrps[-1])

        dlLimit = thrpt*timeToDl

        bitrates = self.vVidHandler.getBitrates(typ)
        chunkSizes = [self.vVidStorage.getChunkSize(typ, segId, i) for i, x in enumerate(bitrates)]

        matchDl = [dlLimit - c for c in chunkSizes]
        matchDl = [float('inf') if md < 0 else md for md in matchDl]

        suitableQl = int(np.argmin(matchDl))

        if suitableQl <= targetQl:
            return suitableQl
        return targetQl + 1

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
        ql = self.mGroupSelectNextQuality(segId) #FIXME select quality based on the group
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
            self.mGetFdFromVidStorage(CallableObj(self.mGrpMediaRequestFdCb, cb, ln), typ, segId, ql)
#             self.mRunInWorkerThread(cb, fd, ln)
        except:
            cprint.red(getTraceBack(sys.exc_info()))
            cb(None, None, 503)

    @inWorker
    def mGrpMediaRequestFdCb(self, cb, l, fd):
        cb(fd, l)


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
        cprint.green(f"I am supposed to download {segId}")
        self.vGrpNextSegIdAsIAmTheLeader = segId + 1
        self.mAddToGroupDownloadQueue(segId)
        self.mGrpSelectNextLeader()

    def mGrpSetIdle(self, srcGid, status):
        peer = self
#         cprint.orange(f"status from {srcGid}: {status}")
        if srcGid != self.vMyGid:
            peer = self.vNeighbors[srcGid]
        if peer.vIdle == status: #Can ignore.
            return
#         cprint.orange(f"status from {srcGid} updating to {status}")
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
#         self.vNeighbors[peerAddr] = peer
        self.vNeighbors = dict(list(self.vNeighbors.items()) + [(peerAddr, peer)]) #complication to avoid modification while iteration

    def mGroupSetCurStatus(self, srcGid, nativePlaybackTime, nativePlayerBuffers, nativeTotalStalled, buffUptom, overAllBufUpto):
        peer = self
#         cprint.orange(f"status from {srcGid}: {status}")
        if srcGid != self.vMyGid:
            peer = self.vNeighbors[srcGid]
        peer.vPlayerStat.mUpdateStat(nativePlaybackTime, nativePlayerBuffers, nativeTotalStalled, buffUptom, overAllBufUpto)
