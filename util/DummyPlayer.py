import io
from urllib.request import urlopen
from .groupManager import GroupManager as GroupMan


class DummyPlayer:
    def __init__(self, videoHandler, options):
        self.playbackTime = 0
        self.setPlaybackTime = 0
        self.nextSegId = 0
        self.startSegment = 0
        self.buffer = []
        self.videoHandler = videoHandler
        self.chunks = {}
        self.options = options
        self.grpMan = None

        self.init()

    def init(self):
        dur = self.videoHandler.getSegmentDur()
        self.setPlaybackTime = self.videoHandler.expectedPlaybackTime()
        print(self.setPlaybackTime, dur)
        self.nextSegId = int(self.setPlaybackTime/dur)
        self.startSegment = self.nextSegId
        self.getInitFiles()

    def getInitFiles(self):
        initAudio = [urlopen(self.videoHandler.getChunkUrl(x, "init", "audio")).read() for x,m in enumerate(self.videoHandler.audInfo["repIds"])]
        initVideo = [urlopen(self.videoHandler.getChunkUrl(x, "init", "video")).read() for x,m in enumerate(self.videoHandler.vidInfo["repIds"])]
        self.initFiles = {"video": initVideo, "audio": initAudio}

#         self.initSizes = {x: [len(z) for z in y] for x, y in self.initFiles.items()}

    def updateState(self, playbackTime, buffers):
        if self.grpMan is None:
            return
        self.grpMan.updateMyState(playbackTime, buffers)

    def getChunkSize(self, ql, num, typ):
        if num == "init":
            return len(self.initFiles[typ][ql])

        num = int(num)

        chunks = self.chunks.setdefault(typ, {}).setdefault(ql, {})

        if num not in chunks:
            url = self.videoHandler.getChunkUrl(ql, num, typ)
            print(url, num)
            chunks[num] = urlopen(url).read()

        return len(chunks[num])

    def getInitFileDescriptor(self, ql, mt):
        fd = io.BytesIO(self.initFiles[mt][ql])
        return fd

    def getChunkFileDescriptor(self, ql, num, mt):
        fd = io.BytesIO(self.chunks[mt][ql][num])
        return fd

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
            seg['ilen'] = self.getChunkSize(ql, 'init', mt)
            l += seg['ilen']
            fds += [self.getInitFileDescriptor(ql, mt)]

            seg['coff'] = l
#             print("dasd", ql, self.nextSegId, mt)
            seg['clen'] = self.getChunkSize(ql, self.nextSegId, mt)
            l += seg['clen']
            fds += [self.getChunkFileDescriptor(ql, self.nextSegId, mt)]
            segs += [seg]

        self.nextSegId += 1
        if self.grpMan is None and self.nextSegId - self.startSegment >= 3:
            self.startGroup()
        return segs, fds, l

    def startGroup(self):
        self.grpMan = GroupMan(self.options)
        self.grpMan.startGroup()

    def shutdown(self):
        if self.grpMan is not None:
            self.grpMan.shutdown()
