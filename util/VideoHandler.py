import datetime
import json
import io
from urllib.request import urlopen
from urllib.parse import urljoin

class VideoHandler:
    def __init__(self, mpd):
        infos = json.loads(urlopen(mpd).read())
        self.audInfo = infos["audInfo"]
        self.vidInfo = infos["vidInfo"]
        self.infos = {"video": infos["vidInfo"], "audio": infos["audInfo"]}
        self.startTime = infos["startTime"]
        self.timeUrl = infos["timeUrl"]
        self.mpdUrl = mpd
        self.chunks = {}
        self.chunkSizes = {}

        self.getTimeDrift()
        self.getInitFiles()

    def getSegmentDur(self):
        assert self.vidInfo['segmentDuration'] == self.audInfo['segmentDuration']
        return self.vidInfo['segmentDuration']

    def getTimeDrift(self):
        timeUrl = urljoin(self.mpdUrl, self.timeUrl)
        time = urlopen(timeUrl).read().decode("utf8")
        time = datetime.datetime.strptime(time, "%Y-%m-%dT%H:%M:%SZ").timestamp()
        sysTime = datetime.datetime.now().timestamp()
        self.drift = time - int(sysTime)

    def getChunkUrl(self, ql, num, typ):
        url = urljoin(self.mpdUrl, f"chunk/{typ}-{ql}-{num}")
        return url

    def getJson(self):
        return json.dumps({"vidInfo": self.vidInfo, "audInfo": self.audInfo})

    def getTime(self):
        time = datetime.datetime.now().timestamp() + self.drift
        return time

    def expectedPlaybackTime(self):
        time = self.getTime()
        playbackTime = time - self.startTime
        return playbackTime

    def isSegmentAvaibleAtTheServer(self, segId):
        return self.timeToSegmentAvailableAtTheServer(segId) < 0

    def timeToSegmentAvailableAtTheServer(self, segId):
        curPlaybackTime = self.expectedPlaybackTime()
        segEndTime = (segId+1)*self.getSegmentDur()
        return segEndTime - curPlaybackTime - 5*self.getSegmentDur()

    def getInitFiles(self):
        initAudio = [urlopen(self.getChunkUrl(x, "init", "audio")).read() for x,m in enumerate(self.audInfo["repIds"])]
        initVideo = [urlopen(self.getChunkUrl(x, "init", "video")).read() for x,m in enumerate(self.vidInfo["repIds"])]
        self.initFiles = {"video": initVideo, "audio": initAudio}

    def updateChunkSizes(self, fs):
        for segId, info in fs.items():
            if info is None:
                continue
            segId = int(segId)
            for mt, chunks in info.items():
                for ch in chunks:
                    num = segId
                    self.chunkSizes.setdefault(mt, {}).setdefault(num, {})[ch[0]] = ch[1]


    def getChunkSize(self, ql, num, typ): #need a massive change to support group based solution
        if num == "init":
            return len(self.initFiles[typ][ql])
        num = int(num)
        chunkSizes = self.chunkSizes.setdefault(typ, {}).setdefault(num, {})
        if ql in chunkSizes:
            return chunkSizes[ql]
        actNum = num
        sizeUrl = urljoin(self.mpdUrl, f"sizes/-{actNum}")
#         print(sizeUrl)
        fs = urlopen(sizeUrl).read().decode()
        fs = {actNum: json.loads(fs)}
        self.updateChunkSizes(fs)
        return chunkSizes[ql]

    def loadChunk(self, ql, num, typ):
        num = int(num)
        chunks = self.chunks.setdefault(typ, {}).setdefault(ql, {})
        assert num not in chunks

        url = self.getChunkUrl(ql, num, typ)
#         print(url, num)
        res = urlopen(url)
        dt = res.getheader('X-Chunk-Sizes')
        if dt is not None:
            dt = json.loads(dt)
            self.updateChunkSizes(dt)
        chunks[num] = res.read()

    def addChunk(self, ql, num, typ, data):
        num = int(num)
        chunks = self.chunks.setdefault(typ, {}).setdefault(ql, {})
        assert num not in chunks
        chunks[num] = data


    def getInitFileDescriptor(self, ql, mt):
        fd = io.BytesIO(self.initFiles[mt][ql])
        return fd

    def getCachedQuality(self, num, mt):
        assert type(num) == int and num >= 0
        chunks = self.chunks.setdefault(mt, {})
        qls = []
        for ql, chs in chunks.items():
            if num in chs:
                qls += [ql]
        return qls

    def getChunkFileDescriptor(self, ql, num, mt):
        chunks = self.chunks.setdefault(mt, {}).setdefault(ql, {})
        if num not in chunks:
            self.loadChunk(ql, num, mt)
        fd = io.BytesIO(self.chunks[mt][ql][num])
        return fd

