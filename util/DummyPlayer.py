import io
from urllib.request import urlopen
import queue
import threading
import time
import json
import multiprocessing as mp
import socket
import random
import numpy as np

from . import groupRpyc as GroupMan
from . import cprint
from . import nestedObject

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
        self.playbackTime = 0
        self.setPlaybackTime = 0
        self.nextSegId = 0
        self.startSegment = 0
        self.buffer = []
        self.videoHandler = videoHandler
        self.options = options
        self.status = None
        self.videoQualities = list(range(len(self.videoHandler.vidInfo["bitrates"])))

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

        self.groupInfo = None
        self.getChunkFromGroup = {}

        #=============================
        self.downloadStartedAt = 0
        self.downloadFinishedAt = -1
        #=============================

        super().__init__()

        self.init()

    def init(self):
        dur = self.videoHandler.getSegmentDur()
        self.setPlaybackTime = self.videoHandler.expectedPlaybackTime()
        print(self.setPlaybackTime, dur)
        self.nextSegId = int(self.setPlaybackTime/dur)
        self.startSegment = self.nextSegId

    def initGroupInfo(self):
        self.groupInfo = nestedObject.Obj(toDict(sizes={}, chunkInfo={}, downloader={}, segQuality={}), nested=False)

        self.groupInfo.sizes.update(self.videoHandler.chunkSizes)


    def updateState(self, playbackTime, buffers):
        if self.grpMan is None:
            return
        self.status.playbackTime = playbackTime
        self.status.idleTime = 0 if self.downloadStartedAt > self.downloadFinishedAt else time.time() - self.downloadFinishedAt
#         cprint.blue("performing status update, the", callable," func is", self.status.getCb())
        self.status.commit()

    def getChunkSize(self, ql, num, mt):
        if mt == "audio":
            return self.videoHandler.getChunkSize(0, num, mt) #forcing ql to 0
        assert False

    def getGroupVidQuality(self):
        if self.iamStarter:
            self.iamStarter = False
            self.startGroupRelatedThreads(self.nextSegId)
            self.broadcast(self.exposed_setNextDownloader, self.nextSegId, self.gid)
        ql = self.videoHandler.getCachedQuality(self.nextSegId, "video")
        if len(ql) > 0:
            return max(ql)
        if self.groupInfo is not None:
            qls = self.groupInfo.chunkInfo.setdefault(self.nextSegId, [])
            if len(qls) > 0:
                ql = max([q[1] for q in qls])
                self.getChunkFromGroup[(ql, self.nextSegId)] = True
                return ql
        return -1

    def getChunkFileDescriptor(self, ql, segId, mt):
        qls = self.videoHandler.getCachedQuality(segId, mt)
        if mt == "audio" or (ql, segId) not in self.getChunkFromGroup:
            return self.videoHandler.getChunkFileDescriptor(ql, segId, mt)

        assert mt == "video"
        assert self.groupInfo is not None

        qls = self.groupInfo.chunkInfo.setdefault(segId, [])
        pgid = [q[0] for q in qls if q[1] == ql][0]
        assert pgid != self.gid
        rpeer = self.neighbours[pgid]
        con = rpeer.getChunk(mt, ql, segId)
        ret = con.recv(1)
        assert ret[0] == 1
        return Socket(con)

    def startGroupRelatedThreads(self, segId):
        assert not self.groupReady
        self.initGroupInfo()
        self.groupStartedFromSegId = segId
        cprint.orange("groupstarting from", segId)
        self.groupReady = True
        self.downloadFinishedAt = time.time()
        self.groupDownloaderThread = threading.Thread(target=self.downloadAsTeamplayer); self.groupDownloaderThread.start()
        self.groupActionThread = threading.Thread(target=self.downloadFromDownloadQueue); self.groupActionThread.start()

    def loadChunk(self, ql, num, typ):
        num = int(num)
        qls = self.videoHandler.getCachedQuality(num, typ)
        assert ql not in qls

        self.broadcast(self.exposed_qualityDownloading, num, ql)
        url = self.videoHandler.getChunkUrl(ql, num, typ)
        print(url, num)
        self.downloadStartedAt = time.time()
        res = urlopen(url)
        dt = res.getheader('X-Chunk-Sizes')
        resData = res.read()
        if dt is not None:
            dt = json.loads(dt)
            self.videoHandler.updateChunkSizes(dt)
            self.broadcast(self.exposed_updateChunkSizes, dt)
        self.downloadFinishedAt = time.time()
        self.videoHandler.addChunk(ql, num, typ, resData)
        self.broadcast(self.exposed_downloaded, num, ql)
        self.videoHandler.updateDownloadStat(self.downloadStartedAt, self.downloadFinishedAt, len(resData))

    def selectGroupQl(self, segId):
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
                    assert futureQl >= 0
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

        cprint.green("lalast5Thrpt", last5Thrpt, "hthrpt", hthrpt)

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

    def selectNextDownloader(self, segId):
        peers = [y for x, y in self.getNeighbours()] + [self]
        idleTimes = [p.status.idleTime for p in peers]
        idleTimes = np.array(idleTimes)
        qlen = [0 if p.status.dlQLen <= 0 else p.status.dlQLen for p in peers]
        qlen = np.array(qlen) * 100

        res = idleTimes - qlen
        downloader = np.argmax(res)
        downloader = peers[downloader]
        cprint.orange(res)
        return downloader.gid

    def downloadAsTeamplayer(self):
#         if self.gid > 1:
#             gsizes, gchunkInfo, gdownloader = self.neighbours[0].getGroupChunkInfos()
#             self.groupInfo.sizes.update(gsizes)
#             self.groupInfo.chunkInfo.update(gchunkInfo)
#             self.groupInfo.downloader.update(gdownloader)
        self.teamplayerStartSem.acquire()
        while True:
            action = self.teamplayerQueue.get()
            cprint.magenta("recvd action", action)
            segId = action[0]
            self.status.dlQLen += 1
            self.downloadQueue.put((segId, "*"))
            sleepTime = self.videoHandler.timeToSegmentAvailableAtTheServer(segId)
            if sleepTime > 0:
                time.sleep(sleepTime)
            nextDownloader = self.selectNextDownloader(segId + 1)
            self.broadcast(self.exposed_setNextDownloader, segId+1, nextDownloader)

    def downloadFromDownloadQueue(self):
        self.downloadFrmQSem.acquire(0)
        while True:
            segId, ql = self.downloadQueue.get()
            self.status.dlQLen -= 1
            if ql == "*": #i.e. run quality selection
                ql = self.selectGroupQl(segId)
                assert ql is not None
            self.loadChunk(ql, segId, "video")

    def prepareStatusObj(self, node, cb=None):
        node.status = AutoUpdateObject(cb)
        node.status.playbackTime = 0
        node.status.idleTime = 0
        node.status.dlQLen = 0

    # Entry point from the player. return segment if available other wise return null
    # Player might stall if returned without any segment.
    def getNextSeg(self):
        segs = []
        fds = []
        l = 0
        qualities = {"audio": 0, "video": 0}
        if self.nextSegId == self.groupStartedFromSegId:
            self.downloadFrmQSem.release()
            self.teamplayerStartSem.release()
        if self.iamStarter or (self. groupReady and self.nextSegId >= self.groupStartedFromSegId):
            ql = self.getGroupVidQuality()
            if ql < 0:
                return [], [], 0
            qualities["video"] = ql
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
        if self.grpMan is None and self.nextSegId - self.startSegment >= 3:
            self.startGroup()
        if self.grpMan is not None and self.options.neighbourAddress is not None and not self.connectedToNeighbour:
            cprint.green("Connection failed before, trying again")
            self.connectToNeighbour()
        return segs, fds, l

    def sendChunk(self, con, mt, ql, segId):
        fd = self.videoHandler.getChunkFileDescriptor(ql, segId, mt)
        con.send(chr(1).encode())
        sent = con.sendfile(fd)
        assert sent ==  self.videoHandler.getChunkSize(ql, segId, mt)

    def sendChunkThroughSocket(self, con, mt, ql, segId):
        qls = self.videoHandler.getCachedQuality(segId, mt)
        if ql not in qls:
            con.send(chr(0).encode())
            cprint.red(mt, ql, segId, "requsted but not sending")
            con.close()
            return
        proc = mp.Process(target=self.sendChunk, args=(con, mt, ql, segId))
        proc.start()
        con.close()


#===========================================
#   Group handler starts
#===========================================
    def startGroup(self):
        self.grpMan = GroupMan.RpcManager(self.options.groupPort, self)
        self.grpMan.start()
        self.grpMan.addPeerRemovedCB(self.peerRemoved)
        self.grpMan.addDataConnRecvCB(self.sendChunkThroughSocket)
        self.prepareStatusObj(self, self.sendStatus)
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
        assert rid == pid
        rpeer.gid = pid
        self.addNeighbour(rpeer)
        rpeer.status.setReadOnly(True)
        sem.release()
        pass


    def broadcast(self, func, *args, **kwargs):
        assert self.groupReady
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
        self.prepareStatusObj(rpeer)
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
        self.groupInfo.chunkInfo.setdefault(segId, []).append((rpeer.gid, ql))

    def exposed_setNextDownloader(self, rpeer, segId, gid):
        if not self.groupReady:
            assert not self.iamStarter
            self.startGroupRelatedThreads(segId)
        self.groupInfo.downloader.setdefault(segId, []).append(gid)
        if self.gid == gid:
            self.teamplayerQueue.put((segId,))

    def exposed_getGroupChunkInfos(self, rpeer):
        return self.groupInfo.sizes, self.groupInfo.chunkInfo, self.groupInfo.downloader
