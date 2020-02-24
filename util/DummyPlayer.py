import io
from urllib.request import urlopen
import queue
import threading
import time
import json
import multiprocessing as mp
import socket
import random

from . import groupRpyc as GroupMan
from . import cprint
from . import nestedObject

class CallableObj:
    def __init__(self, cb, *args):
        self.cb = cb
        self.args = args
    def __call__(self, *a, **b):
        self.cb(*self.args, *a, **b)
#         cprint.green,cc(f"{self.name} returned", *a)

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
#         self.__setattr__ = self.__setattr
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
#         cprint.blue("status updated", self.__int_values)
    def setAutoCommit(self, autoCommit = True):
        if self.__int_cb is not None and callable(self.__int_cb):
            return
        self.__int_autoCommit = autoCommit
    def setReadOnly(self, readonly=True):
        self.__int_readonly = readonly
    def commit(self):
        if self.__int_readonly:
            raise PermissionError("Readonly object")
        if self.__int_cb is not None and callable(self.__int_cb):
            return
        keys = self.__int_changed.copy()
        self.__int_changed.clear()
        self.__int_cb({x: self.__int_values[x] for x in keys})

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

        self.grpMan = None
        self.groupReady = False
        self.neighbours = {}
        self.peerIds = set()
        self.gid = 0
        self.groupStartedFromSegId = -1
        self.iamStarter = False
        self.downloadQueue = queue.PriorityQueue()
        self.teamplayerQueue = queue.Queue()

        self.groupInfo = None
        self.getChunkFromGroup = {}

        super().__init__()

        self.init()

    def init(self):
        dur = self.videoHandler.getSegmentDur()
        self.setPlaybackTime = self.videoHandler.expectedPlaybackTime()
        print(self.setPlaybackTime, dur)
        self.nextSegId = int(self.setPlaybackTime/dur)
        self.startSegment = self.nextSegId

    def initGroupInfo(self):
        self.groupInfo = nestedObject.Obj(toDict(sizes={}, chunkInfo={}, downloader={}), nested=False)
        self.groupInfo.sizes.update(self.videoHandler.chunkSizes)


    def updateState(self, playbackTime, buffers):
        if self.grpMan is None:
            return
        self.status.playbackTime = playbackTime
#         self.status.commit()

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
#             cprint.orange("getGroupVidQuality", qls)
            if len(qls) > 0:
                ql = max([q[1] for q in qls])
                self.getChunkFromGroup[(ql, self.nextSegId)] = True
                return ql
        return -1

    def getChunkFileDescriptor(self, ql, segId, mt):
        qls = self.videoHandler.getCachedQuality(segId, mt)
        if mt == "audio" or (ql, segId) not in self.getChunkFromGroup:
#         if ql in qls or not self.groupInfo or mt == "audio":
            return self.videoHandler.getChunkFileDescriptor(ql, segId, mt)

#         cprint.orange(ql, segId, mt, qls, self.videoHandler.getChunkSize(ql, segId, mt))
        assert mt == "video"
        assert self.groupInfo is not None

        qls = self.groupInfo.chunkInfo.setdefault(segId, [])
#         cprint.orange("getChunkFileDescriptor", qls)
        pgid = [q[0] for q in qls if q[1] == ql][0]
#         cprint.orange(pgid, self.gid, self.groupInfo.chunkInfo[segId])
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
        self.groupDownloaderThread = threading.Thread(target=self.downloadAsTeamplayer); self.groupDownloaderThread.start()
        self.groupActionThread = threading.Thread(target=self.downloadFromDownloadQueue); self.groupActionThread.start()
#         cprint.magenta("GROUP STARTED")

    def loadChunk(self, ql, num, typ):
        num = int(num)
        qls = self.videoHandler.getCachedQuality(num, typ)
        assert ql not in qls

#         cprint.magenta(f"downloading task recvd for {num}, {ql}")
        self.broadcast(self.exposed_qualityDownloading, num, ql)
#         cprint.magenta(f"downloading {num}, {ql}")
        url = self.videoHandler.getChunkUrl(ql, num, typ)
        print(url, num)
        res = urlopen(url)
        dt = res.getheader('X-Chunk-Sizes')
        if dt is not None:
            dt = json.loads(dt)
            self.videoHandler.updateChunkSizes(dt)
            self.broadcast(self.exposed_updateChunkSizes, dt)
#         cprint.blue(f"reading data for segId {num}")
        self.videoHandler.addChunk(ql, num, typ, res.read())
#         cprint.blue(f"broadcasting read data for segId {num}")
        self.broadcast(self.exposed_downloaded, num, ql)
#         cprint.blue(f"broadcasted read data for segId {num}")


    def selectGroupQl(self, segId):
        ql = random.choice(self.videoQualities)
        return ql
        return 0 #TODO add algo

    def selectNextDownloader(self, segId):
        tmp = list(self.neighbours.keys())
        nextDownloader = self.gid
        if len(tmp) > 0:
            nextDownloader = random.choice(tmp)#[0] #TODO call selectNextDownloader
        return nextDownloader

    def downloadAsTeamplayer(self):
        if self.gid > 1:
            gsizes, gchunkInfo, gdownloader = self.neighbours[0].getGroupChunkInfos()
            self.groupInfo.sizes.update(gsizes)
            self.groupInfo.chunkInfo.update(gchunkInfo)
            self.groupInfo.downloader.update(gdownloader)
        while True:
            action = self.teamplayerQueue.get()
            cprint.magenta("recvd action", action)
            segId = action[0]
            self.downloadQueue.put((segId, "*"))
            sleepTime = self.videoHandler.timeToSegmentAvailableAtTheServer(segId)
            if sleepTime > 0:
                time.sleep(sleepTime)
            nextDownloader = self.selectNextDownloader(segId + 1)
            self.broadcast(self.exposed_setNextDownloader, segId+1, nextDownloader)

    def downloadFromDownloadQueue(self):
        while True:
            segId, ql = self.downloadQueue.get()
            if ql == "*": #i.e. run quality selection
                ql = self.selectGroupQl(segId)
            self.loadChunk(ql, segId, "video")

    # Entry point from the player. return segment if available other wise return null
    # Player might stall if returned without any segment.
    def getNextSeg(self):
        segs = []
        fds = []
        l = 0
        qualities = {"audio": 0, "video": 0}
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
            #fds += [self.videoHandler.getChunkFileDescriptor(ql, self.nextSegId, mt)]
            fds += [self.getChunkFileDescriptor(ql, self.nextSegId, mt)]
            segs += [seg]

        self.nextSegId += 1
        if self.grpMan is None and self.nextSegId - self.startSegment >= 3:
            self.startGroup()
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
#         cprint.orange(mt, ql, segId, "requsted and sending")
        con.close()


#===========================================
#   Group handler starts
#===========================================
    def startGroup(self):
        self.grpMan = GroupMan.RpcManager(self.options.groupPort, self)
        self.grpMan.start()
        self.grpMan.addPeerRemovedCB(self.peerRemoved)
        self.grpMan.addDataConnRecvCB(self.sendChunkThroughSocket)
        if self.options.neighbourAddress is not None:
            addr, port = self.options.neighbourAddress.split(":")
            peer = self.grpMan.connectTo((addr, int(port)))
            neighbours, mid, rid = peer.addme()
            self.gid = mid
            peer.gid = rid
            self.neighbours[rid] = peer
            self.peerIds.add(mid)
            self.peerIds.add(rid)
            peer.status = AutoUpdateObject(None)
            peer.status.setReadOnly(True)
#             self.iamStarter = False

            addPeerSem = threading.Semaphore(0)
            for pid, addr in neighbours.items():
                rpeer = self.grpMan.connectTo(addr)
                cb = CallableObj(self.addPeerCallBack, addPeerSem, rpeer, pid)
                rpeer.fyiIamNewHere.asyncCall(cb, self.gid)
            for pid, addr in neighbours.items():
                addPeerSem.acquire()
            cprint.blue(self.neighbours)
        else:
            self.peerIds.add(self.gid)
        self.status = AutoUpdateObject(self.sendStatus)
#         self.status.setAutoCommit(True)

    def addPeerCallBack(self, sem, rpeer, pid, rid, err):
#         cprint.orange(rid, pid)
        assert rid == pid
        rpeer.gid = pid
        self.neighbours[pid] = rpeer
        rpeer.status = AutoUpdateObject(None)
        rpeer.status.setReadOnly(True)
        sem.release()
        pass


    def broadcast(self, func, *args, **kwargs):
        assert self.groupReady
        fname = func.__name__
        for x, peer in self.neighbours.items():
            fn = getattr(peer, fname)
#             fn.asyncCall(CallableObj(fname), *args, **kwargs)
            fn.asyncCall(self.dummyCB, *args, **kwargs)
        fn = getattr(self, fname)
        fn(self, *args, **kwargs)

    def dummyCB(self, ret, err):
#         cprint.green("dummyCB")
        pass

    def shutdown(self):
        if self.grpMan is not None:
            self.grpMan.shutdown()

    def peerRemoved(self, peer):
        cprint.cyan(peer, peer.gid)
        del self.neighbours[peer.gid]

    def sendStatus(self, status):
        for x, peer in self.neighbours.items():
            peer.updateStatus.asyncCall(self.dummyCB, status)

#===========================================
    def exposed_addme(self, rpeer):
#         if not self.groupReady: return
        yid = len(self.neighbours) + 1
        neighbours = {x:y.getAddr() for x,y in self.neighbours.items()}
        rpeer.gid = yid
        self.neighbours[yid] = rpeer
        rpeer.status = AutoUpdateObject(None)
        rpeer.status.setReadOnly(True)
        self.peerIds.add(yid)
        if len(self.peerIds) == 2:
            self.iamStarter = True
#             self.groupReady = True
        return neighbours, yid, self.gid

    def exposed_updateStatus(self, rpeer, status):
        if not self.groupReady: return
        rpeer.status.update(status)

    def exposed_fyiIamNewHere(self, rpeer, rid):
        if not self.groupReady: return
        rpeer.gid = rid
        self.peerIds.add(rid)
        self.neighbours[rid] = rpeer
        return self.gid

    def exposed_curStat(self, rpeer, playbackTime):
        if not self.groupReady: return
        rpeer.status.playbackTime = playbackTime
#         cprint.cyan(f"{self.gid}:", rpeer.gid, f"playbackTime={playbackTime}")

    def exposed_qualityDownloading(self, rpeer, segId, ql):
        if not self.groupReady: return
        self.groupInfo.downloader.setdefault(segId, {})[rpeer.gid] = ql
#         cprint.red(f"{self.gid}:", f"seg {segId} of ql {ql} download started by {rpeer.gid}")

    def exposed_updateChunkSizes(self, rpeer, chunkSizes):
        if not self.groupReady: return
        self.groupInfo.sizes.update(chunkSizes)
#         cprint.red(f"{self.gid}:", f"chunksize updated by {rpeer.gid} for chunks {chunkSizes.keys()}")

    def exposed_downloaded(self, rpeer, segId, ql):
        if not self.groupReady: return
        self.groupInfo.chunkInfo.setdefault(segId, []).append((rpeer.gid, ql))
        cprint.red(f"{self.gid}:", f"seg {segId} of ql {ql} downloaded by {rpeer.gid}")

    def exposed_setNextDownloader(self, rpeer, segId, gid):
        if not self.groupReady:
            assert not self.iamStarter
            self.startGroupRelatedThreads(segId)
#         cprint.red(f"{self.gid}:", f"setNetdownloader {gid} is assigned for seg {segId} by {rpeer.gid}")
        self.groupInfo.downloader.setdefault(segId, {})[gid] = -1
        if self.gid == gid:
            self.teamplayerQueue.put((segId,))

    def exposed_getGroupChunkInfos(self, rpeer):
        return self.groupInfo.sizes, self.groupInfo.chunkInfo, self.groupInfo.downloader
