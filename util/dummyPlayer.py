import io
import math
from urllib.request import urlopen
import queue
import threading
import time
import json
import multiprocessing as mp
import socket
import random
import os
import numpy as np

# from . import multiprocwrap as mp
from . import groupRpyc as GroupMan
from . import cprint
from . import nestedObject
from _ast import Try

class CallableObj:
    def __init__(self, cb, *args):
        self.cb = cb
        self.args = args
    def __call__(self, *a, **b):
        self.cb(*self.args, *a, **b)

class Socket:
    def __init__(self, sock):
        self.sock = sock

    def read(self, p):
        return self.sock.recv(p)

    def __getattr__(self, name):
        return getattr(self.sock, name)

def assertLog(cond, *k):
    if not cond:
        cprint.red("Assert failed:", *k)
    assert cond

class AutoUpdateObject():
    def __init__(self, callback):
        self.__int_values = {}
        self.__int_cb = callback
        self.__int_autoCommit = False
        self.__int_readonly = False
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

class DummyPlayer(GroupMan.RpcPeer):
    def __init__(self, videoHandler, options):
        self.playbackTime = 0 # seconds
        self.startPlaybackTime = -1  # seconds
        self.playerBuffers = [] # seconds
        self.totalStalled = 0 # seconds

        self.minBufferLength = 30 # seconds
        self.nextBufferingSegId = 0
        self.setPlaybackTime = 0
        self.nextSegId = 0
        self.startSegment = 0
        self.videoHandler = videoHandler
        self.options = options
        self.status = None
        self.videoQualities = list(range(len(self.videoHandler.vidInfo["bitrates"])))
        self.qoeLogFd = None

        self.groupLockReleased = False # the true identifier if a group is actually started or not
        self.groupStarted = False
        self.connectedToNeighbour = False
        self.grpMan = None
        self.groupReady = False
        self.neighbours = {}
        self.neighboursLock = threading.Lock()
        self.peerIds = set()
        self.gid = 0
        self.groupStartedFromSegId = -1
        self.iamStarter = False
        self.downloadQueue = queue.PriorityQueue()
        self.teamplayerQueue = queue.Queue()
        self.deadLines = {}
        self.loadingChunk = False
        self.teamplayerStartSem = threading.Semaphore(0)
        self.downloadFrmQSem = threading.Semaphore(0)
        self.groupSegTryCnt = 0

        self.groupInfo = None
        self.getChunkFromGroup = {}

        #=============================
        self.downloadStartedAt = 0
        self.downloadFinishedAt = -1
        #=============================

        super().__init__()

        self.init()

    def BOLA(self, segId):
        typ = 'video'

        V = 0.93
        lambdaP = 5 # 13
        sleepTime = 0
        chunkHistory = self.videoHandler.getChunkDownloadDetails(typ) # [[segid, qlid, download_id]]
        if len(chunkHistory) == 0:
            return 0
        if self.playbackTime < self.startPlaybackTime:
            return 0

        p = self.videoHandler.getSegmentDur()
        bufUpto = p*segId
        buflen = bufUpto - self.playbackTime

        dlDetail = self.videoHandler.getDownloadStat(chunkHistory[-1][2]) # start (Sec), send (sec), clen(bytes)
        lastThroughput = dlDetail[2] * 8 / (dlDetail[1] - dlDetail[0]) # bps

        bitrates = self.videoHandler.getBitrates(typ)
        SM = float(bitrates[-1])
        vms = [math.log(sm/SM) for sm in bitrates]

        lastM = chunkHistory[-1][1] #last bitrateindex
        Q = buflen/p
        Qmax = self.minBufferLength/p
        ts = self.playbackTime - self.startPlaybackTime
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

    def init(self):
        dur = self.videoHandler.getSegmentDur()
        self.setPlaybackTime = self.videoHandler.expectedPlaybackTime()
        if self.startPlaybackTime == -1:
            self.startPlaybackTime = self.setPlaybackTime
        print(self.setPlaybackTime, dur)
        self.nextSegId = int(self.setPlaybackTime/dur)
        self.startSegment = self.nextSegId
        self.nextBufferingSegId = self.nextSegId
        if self.options.logDir is not None:
            self.qoeLogFd = open(os.path.join(self.options.logDir, "QoE.log"), "w")

    def updateState(self, playbackTime, buffers, totalStalled):
        self.playerBuffers = buffers[:]
        self.playbackTime = playbackTime
        self.totalStalled = totalStalled
        if self.grpMan is None:
            return
        self.status.playbackTime = playbackTime
        self.status.idleTime = 0 if self.downloadStartedAt > self.downloadFinishedAt else time.time() - self.downloadFinishedAt
#         cprint.blue("performing status update, the", callable," func is", self.status.getCb())
        self.status.commit()

    def getChunkSize(self, ql, num, mt):
        if mt == "audio":
            return self.videoHandler.getChunkSize(0, num, mt) #forcing ql to 0
        cprint.red("Assert failed")
        assert False

    def getChunkFileDescriptor(self, ql, segId, mt):
        qls = self.videoHandler.getCachedQuality(segId, mt)
        if mt == "audio" or (ql, segId) not in self.getChunkFromGroup:
            return self.videoHandler.getChunkFileDescriptor(ql, segId, mt)

        assertLog(mt == "video", f"mt={mt}")
        assertLog(self.groupInfo is not None, f"self.groupInfo={self.groupInfo}")

        qls = self.groupInfo.chunkInfo.setdefault(segId, [])
        pgid = [q[0] for q in qls if q[1] == ql][0]
        assertLog(pgid != self.gid, f"pgid={pgid} self.gid={self.gid}")
        rpeer = self.neighbours[pgid]
        con = rpeer.getChunk(mt, ql, segId)
        ret = con.recv(1)
        assertLog(len(ret) >= 1, f"len(ret)={len(ret)}")
        assertLog(ret[0] == 1, f"ret[0]={ret[0]}")
        return Socket(con)

    def doSelfLoadBuffer(self, videoQl): #audio quality is 0
        cprint.green("Going to download video ql:", videoQl, "for seg", self.nextBufferingSegId)
        self.videoHandler.loadChunk(videoQl, self.nextBufferingSegId, "video")
        self.videoHandler.loadChunk(0, self.nextBufferingSegId, "audio")
        self.nextBufferingSegId += 1

    # Entry point from the player. return segment if available other wise return null
    # Player might stall if returned without any segment.
    def getNextSeg(self):
        curPlaybackTime = self.playbackTime if self.playbackTime >= self.setPlaybackTime else self.setPlaybackTime
        segDur = self.videoHandler.getSegmentDur()
        if True:
            bufUpto = self.nextBufferingSegId * segDur
            bufLen = bufUpto - curPlaybackTime

            if not self.groupLockReleased:
                if (self.nextBufferingSegId == self.groupStartedFromSegId or self.iamStarter): #best place to hold it.
                    cprint.cyan("releasing sem2")
                    self.downloadFrmQSem.release()
                    cprint.magenta("releasing sem1")
                    self.teamplayerStartSem.release()
                    cprint.magenta("released sem1")
                    self.groupLockReleased = True
                if bufLen < self.minBufferLength and self.videoHandler.isSegmentAvaibleAtTheServer(self.nextBufferingSegId): #need to add the deadline concept here
                    videoQl = self.BOLA(self.nextBufferingSegId)
                    self.doSelfLoadBuffer(videoQl)
                else: # start group and other group related activity
                    if self.grpMan is None and self.nextBufferingSegId - self.startSegment >= 3:
                        cprint.red("Starting group")
                        self.startGroup()
                    if self.grpMan is not None and self.options.neighbourAddress is not None and not self.connectedToNeighbour:
                        cprint.green("Connection failed before, trying again")
                        self.connectToNeighbour()
#             elif len(self.videoHandler.getCachedQuality(self.nextBufferingSegId, "video")) > 0:
            elif self.groupGetVidQuality(self.nextBufferingSegId) >= 0:
                self.nextBufferingSegId += 1
            elif bufLen < segDur: # in group thing, they never bother about the nextsegid or next buffering segid
                self.doSelfLoadBuffer(0)

        segmentPlaying = int(curPlaybackTime/segDur)
        if segmentPlaying + 1 < self.nextSegId:
            return [], [], 0

        segs = []
        fds = []
        l = 0
        qualities = {"audio": 0, "video": 0}
        if self.videoHandler.ended(self.nextSegId):
            cprint.green("Sending eof")
            if self.qoeLogFd is not None:
                self.qoeLogFd.close()
                self.qoeLogFd = None
            return [{"eof" : True}], [], 0
#         qls = self.videoHandler.getCachedQuality(self.nextSegId, "video")
#         if len(qls) == 0: #segment is not yet available, try again later
#             return [], [], 0
        vql = self.groupGetVidQuality(self.nextSegId)
        if vql < 0:
            return [], [], 0
        qualities['video'] = vql

        if self.qoeLogFd is not None:
            ql = qualities['video']
            bitrate = self.videoHandler.getBitrates('video')[ql]
            print(self.playbackTime, self.totalStalled, ql, bitrate, self.nextSegId, file=self.qoeLogFd, flush=True)
        for mt in ["audio", "video"]:
            ql = qualities[mt]
            seg = {}
            seg['seg'] = self.nextSegId
            seg['type'] = mt
            seg['rep'] = ql
            seg['ioff'] = l
            seg['ilen'] = self.videoHandler.getChunkSize(ql, 'init', mt)
            l += seg['ilen']
            fds += [self.videoHandler.getInitFileDescriptor(ql, mt)]

            seg['coff'] = l
            seg['clen'] = self.videoHandler.getChunkSize(ql, self.nextSegId, mt)
            l += seg['clen']
            fds += [self.getChunkFileDescriptor(ql, self.nextSegId, mt)]
            segs += [seg]

        self.nextSegId += 1
        return segs, fds, l


#===========================================
#   Group related methods
#===========================================
    def groupInitInfo(self):
        self.groupInfo = nestedObject.Obj(toDict(sizes={}, chunkInfo={}, downloader={}, segQuality={}), nested=False)

        self.groupInfo.sizes.update(self.videoHandler.chunkSizes)

    def groupGetVidQuality(self, segId):
        if self.iamStarter:
            self.iamStarter = False
            self.groupStartGrpThreads(self.nextBufferingSegId)
            self.broadcast(self.exposed_setNextDownloader, self.nextBufferingSegId, self.gid)

        ql = self.videoHandler.getCachedQuality(segId, "video")
        if len(ql) > 0:
            return max(ql)
        if self.groupInfo is not None: #
            qls = self.groupInfo.chunkInfo.setdefault(segId, [])
            if len(qls) > 0:
                ql = max([q[1] for q in qls])
                self.getChunkFromGroup[(ql, segId)] = True
                return ql
        return -1

    def groupStartGrpThreads(self, segId):
        assertLog(not self.groupReady, f"self.groupReady={self.groupReady}")
        self.groupInitInfo()
        self.groupStartedFromSegId = segId
        cprint.orange("groupstarting from", segId)
        self.groupReady = True
        self.downloadFinishedAt = time.time()
        self.groupDownloaderThread = threading.Thread(target=self.groupDownloadAsTeamplayer); self.groupDownloaderThread.start()
        self.groupActionThread = threading.Thread(target=self.groupDownloadFromDownloadQueue); self.groupActionThread.start()

    def groupLoadChunk(self, ql, segId, typ):
        segId = int(segId)
        qls = self.videoHandler.getCachedQuality(segId, typ)
#         assertLog(ql not in qls, f"ql={ql}", f"qls={qls}")

        self.broadcast(self.exposed_qualityDownloading, segId, ql)
        url = self.videoHandler.getChunkUrl(ql, segId, typ)
#         print(url, segId)
        self.downloadStartedAt = time.time()
        res = urlopen(url)
        dt = res.getheader('X-Chunk-Sizes')
        resData = res.read()
        if dt is not None:
            dt = json.loads(dt)
            self.videoHandler.updateChunkSizes(dt)
            self.broadcast(self.exposed_updateChunkSizes, dt)
        self.downloadFinishedAt = time.time()
        self.videoHandler.addChunk(ql, segId, typ, resData, True) #hack, overwrite
        self.broadcast(self.exposed_downloaded, segId, ql)
        self.videoHandler.updateDownloadStat(self.downloadStartedAt, self.downloadFinishedAt, len(resData), segId, ql, typ)

    def groupSelectNextQuality(self, segId):
        now = time.time()
        peers = [y for x, y in self.getNeighbours()] + [self]
        segDur = self.videoHandler.getSegmentDur()
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

#         cprint.green("lalast5Thrpt", last5Thrpt, "hthrpt", hthrpt)

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

    def groupSelectNextDownloader(self, segId):
        peers = [y for x, y in self.getNeighbours()] + [self]
        idleTimes = [p.status.idleTime for p in peers]
        idleTimes = np.array(idleTimes)
        qlen = [0 if p.status.dlQLen <= 0 else p.status.dlQLen for p in peers]
        qlen = np.array(qlen) * 100

        res = idleTimes - qlen
        downloader = np.argmax(res)
        downloader = peers[downloader]
#         cprint.orange(res)
        return downloader.gid

    def groupDownloadAsTeamplayer(self):
        cprint.magenta("wait for sem1")
        self.teamplayerStartSem.acquire()
        cprint.magenta("sem1 released")
        while True:
            action = self.teamplayerQueue.get()
            cprint.magenta("recvd action", action)
            segId = action[0]
            self.status.dlQLen += 1
            ql = self.groupSelectNextQuality(segId) # ADDED for experiment
            self.broadcast(self.exposed_qualityDownloading, segId, ql) # informing everyone that I am waiting for other to finish it
            self.downloadQueue.put((segId, ql))
            sleepTime = self.videoHandler.timeToSegmentAvailableAtTheServer(segId)
            if sleepTime > 0:
                time.sleep(sleepTime)
            nextDownloader = self.groupSelectNextDownloader(segId + 1)
            self.broadcast(self.exposed_setNextDownloader, segId+1, nextDownloader)

    def groupDownloadFromDownloadQueue(self):
        cprint.cyan("wait for sem2")
        self.downloadFrmQSem.acquire()
        cprint.cyan("sem2 released")
        while True:
            segId, ql = self.downloadQueue.get()
            self.status.dlQLen -= 1
            if ql == "*": #i.e. run quality selection
                ql = self.groupSelectNextQuality(segId)
                assertLog(ql is not None, f"ql={ql}")
            self.groupLoadChunk(ql, segId, "video")

    def groupPrepareStatusObj(self, node, cb=None):
        node.status = AutoUpdateObject(cb)
        node.status.playbackTime = 0
        node.status.idleTime = 0
        node.status.dlQLen = 0


    def groupSendChunk(self, con, mt, ql, segId):
        fd = self.videoHandler.getChunkFileDescriptor(ql, segId, mt)
        con.send(chr(1).encode())
        sent = con.sendfile(fd)
        sRet = self.videoHandler.getChunkSize(ql, segId, mt)
        assertLog(sent == sRet, f"sent={sent}, sRet={sRet}")

    def groupSendChunkThroughSocket(self, con, mt, ql, segId):
        qls = self.videoHandler.getCachedQuality(segId, mt)
        if ql not in qls:
            con.send(chr(0).encode())
            cprint.red(mt, ql, segId, "requsted but not sending")
            con.close()
            return
        proc = mp.Process(target=self.groupSendChunk, args=(con, mt, ql, segId))
        proc.start()
        con.close()


#===========================================
#   Group handler starts
#===========================================
    def startGroup(self):
        self.grpMan = GroupMan.RpcManager(self.options.groupPort, self)
        self.grpMan.start()
        self.grpMan.addPeerRemovedCB(self.peerRemoved)
        self.grpMan.addDataConnRecvCB(self.groupSendChunkThroughSocket)
        self.groupPrepareStatusObj(self, self.sendStatus)
        if self.options.neighbourAddress is not None:
            self.connectToNeighbour()
        else:
            self.peerIds.add(self.gid)
            self.groupStarted = True

    def connectToNeighbour(self):
        addr, port = self.options.neighbourAddress.split(":")
        try:
            peer = self.grpMan.connectTo((addr, int(port)))
        except ConnectionRefusedError:
            cprint.red("connectionFailed")
            return
        neighbours, mid, rid = peer.addme()
        self.gid = mid
        peer.gid = rid
        self.addNeighbour(peer, rid)
        self.peerIds.add(mid)
        self.peerIds.add(rid)
        peer.status.setReadOnly(True)

        addPeerSem = threading.Semaphore(0)
        for pid, addr in neighbours.items():
            rpeer = self.grpMan.connectTo(addr)
            cb = CallableObj(self.addPeerCallBack, addPeerSem, rpeer, pid)
            rpeer.fyiIamNewHere.asyncCall(cb, self.gid)
        for pid, addr in neighbours.items():
            addPeerSem.acquire()
        self.connectedToNeighbour = True
        self.groupStarted = True
        cprint.blue(self.neighbours)

    def addPeerCallBack(self, sem, rpeer, pid, rid, err):
        if rid != pid: cprint.orange("Not matching", rid, pid)
        assertLog(rid == pid, f"rid={rid} pid={pid}")
        rpeer.gid = pid
        self.addNeighbour(rpeer)
        rpeer.status.setReadOnly(True)
        sem.release()
        pass


    def broadcast(self, func, *args, **kwargs):
        assertLog(self.groupReady, f"self.groupReady={self.groupReady}")
        fname = func.__name__
        for x, peer in self.getNeighbours():
            fn = getattr(peer, fname)
            fn.asyncCall(self.dummyCB, *args, **kwargs)
        fn = getattr(self, fname)
        fn(self, *args, **kwargs)

    def dummyCB(self, ret, err):
        pass

    def shutdown(self):
        if self.grpMan is not None:
            self.grpMan.shutdown()

    def peerRemoved(self, peer):
        cprint.cyan(peer, peer.gid)
        self.removeNeighbour(peer.gid)

    def sendStatus(self, status):
        for x, peer in self.getNeighbours():
            peer.updateStatus.asyncCall(self.dummyCB, status)

    def getNeighbours(self):
        self.neighboursLock.acquire()
        neighbours = [(x,y) for x,y in self.neighbours.items()]
        self.neighboursLock.release()
        return neighbours

    def addNeighbour(self, rpeer, gid = None):
        gid = gid if gid is not None else rpeer.gid
        self.groupPrepareStatusObj(rpeer)
        self.neighboursLock.acquire()
        self.neighbours[gid] = rpeer
        self.neighboursLock.release()

    def removeNeighbour(self, gid):
        self.neighboursLock.acquire()
        if gid in self.neighbours: del self.neighbours[gid]
        self.neighboursLock.release()
    def isNeighbour(self, gid):
        self.neighboursLock.acquire()
        ret = gid in self.neighbours
        self.neighboursLock.release()
        return ret

#===========================================
    def exposed_addme(self, rpeer):
        yid = len(self.neighbours) + 1
        neighbours = {x:y.getAddr() for x,y in self.getNeighbours()}
        rpeer.gid = yid
        self.addNeighbour(rpeer, yid)
        rpeer.status.setReadOnly(True)
        self.peerIds.add(yid)
        if len(self.peerIds) == 2:
            self.iamStarter = True
        return neighbours, yid, self.gid

    def exposed_updateStatus(self, rpeer, status):
        if not self.groupReady: return
        rpeer.status.update(status)

    def exposed_fyiIamNewHere(self, rpeer, rid):
        if not self.groupStarted: return
        rpeer.gid = rid
        self.peerIds.add(rid)
        self.addNeighbour(rpeer, rid)
#         self.neighbours[rid] = rpeer
        return self.gid

    def exposed_curStat(self, rpeer, playbackTime):
        if not self.groupReady: return
        rpeer.status.playbackTime = playbackTime

    def exposed_qualityDownloading(self, rpeer, segId, ql):
        if not self.groupReady: return
        self.groupInfo.segQuality.setdefault(segId, {})[rpeer.gid] = ql

    def exposed_updateChunkSizes(self, rpeer, chunkSizes):
        if not self.groupReady: return
        for segId, info in chunkSizes.items():
            if info is None:
                continue
            segId = int(segId)
            for mt, chunks in info.items():
                for ch in chunks:
                    num = segId
                    self.groupInfo.sizes.setdefault(mt, {}).setdefault(num, {})[ch[0]] = ch[1]
#         self.groupInfo.sizes.update(chunkSizes)

    def exposed_downloaded(self, rpeer, segId, ql):
        if not self.groupReady: return
        cprint.magenta(segId, "downloaded by", rpeer.gid, "with ql =", ql)
        self.groupInfo.chunkInfo.setdefault(segId, []).append((rpeer.gid, ql))

    def exposed_setNextDownloader(self, rpeer, segId, gid):
        if not self.groupReady:
            assertLog(not self.iamStarter, f"self.iamStarter={self.iamStarter}")
            assertLog(self.gid != gid, f"self.gid={self.gid} gid={gid}")
            gSegId = segId
            if gSegId <= self.nextBufferingSegId:
                gSegId = self.nextBufferingSegId + 1
            self.groupStartGrpThreads(gSegId) #if i am not starter, TODO think about it. I am the next downloader
        self.groupInfo.downloader.setdefault(segId, []).append(gid)
        if self.gid == gid:
            self.teamplayerQueue.put((segId,))

    def exposed_getGroupChunkInfos(self, rpeer):
        return self.groupInfo.sizes, self.groupInfo.chunkInfo, self.groupInfo.downloader
