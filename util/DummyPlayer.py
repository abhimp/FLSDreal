import io
from urllib.request import urlopen

from . import groupRpyc as GroupMan
from . import cprint


class DummyPlayer(GroupMan.RpcPeer):
    def __init__(self, videoHandler, options):
        self.playbackTime = 0
        self.setPlaybackTime = 0
        self.nextSegId = 0
        self.startSegment = 0
        self.buffer = []
        self.videoHandler = videoHandler
        self.options = options
        self.grpMan = None

        self.neighbours = {}
        self.myId = 0

        super().__init__()

        self.init()

    def init(self):
        dur = self.videoHandler.getSegmentDur()
        self.setPlaybackTime = self.videoHandler.expectedPlaybackTime()
        print(self.setPlaybackTime, dur)
        self.nextSegId = int(self.setPlaybackTime/dur)
        self.startSegment = self.nextSegId

    def updateState(self, playbackTime, buffers):
        if self.grpMan is None:
            return
        for x, y in self.neighbours.items():
            y.exposed_curStat(playbackTime)
            pass

    def getNextSeg(self):
        segs = []
        fds = []
        l = 0
        ql = 0
        for mt in ["audio", "video"]:
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
            fds += [self.videoHandler.getChunkFileDescriptor(ql, self.nextSegId, mt)]
            segs += [seg]

        self.nextSegId += 1
        if self.grpMan is None and self.nextSegId - self.startSegment >= 3:
            self.startGroup()
        return segs, fds, l

    def startGroup(self):
        self.grpMan = GroupMan.RpcManager(self.options.groupPort, self)
        self.grpMan.start()
        self.grpMan.addPeerRemovedCB(self.peerRemoved)
        if self.options.neighbourAddress is not None:
            addr, port = self.options.neighbourAddress.split(":")
            peer = self.grpMan.connectTo((addr, int(port)))
            neighbours, mid, rid = peer.addme()
            self.myId = mid
            peer.id = rid
            self.neighbours[rid] = peer

            for pid, addr in neighbours.items():
                rpeer = self.grpMan.connectTo(addr)
                rid = rpeer.hello(self.myId)
                assert rid == pid
                rpeer.id = pid
                self.neighbours[pid] = rpeer
            cprint.blue(self.neighbours)

    def shutdown(self):
        if self.grpMan is not None:
            self.grpMan.shutdown()

    def peerRemoved(self, peer):
        cprint.cyan(peer, peer.id)
        del self.neighbours[peer.id]

#===========================================
    def exposed_addme(self, rpeer):
        yid = len(self.neighbours) + 1
        neighbours = {x:y.getAddr() for x,y in self.neighbours.items()}
        rpeer.id = yid
        self.neighbours[yid] = rpeer
        return neighbours, yid, self.myId

    def exposed_hello(self, rpeer, rid):
        rpeer.id = rid
        self.neighbours[rid] = rpeer
        return self.myId

    def exposed_curStat(self, rpeer, playbackTime):
        cprint.cyan(rpeer.id, f"playbackTime={playbackTime}")

