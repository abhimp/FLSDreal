import datetime
import json
import io
import time
from urllib.parse import urljoin
from urllib.request import urlopen
from urllib.request import Request

from util import cprint

class VideoHandler:
    def __init__(self, mpdpath):
        infos = json.loads(urlopen(mpdpath).read())
        self.audInfo = infos["audInfo"]
        self.vidInfo = infos["vidInfo"]
        self.infos = {"video": infos["vidInfo"], "audio": infos["audInfo"]}
        self.startTime = infos["startTime"]
        self.timeUrl = infos["timeUrl"]
        self.mpdUrl = mpdpath

        self.calculateTimeDrift()
        self.loadInitFiles()

    def getSegmentDur(self):
        assert self.vidInfo['segmentDuration'] == self.audInfo['segmentDuration']
        return self.vidInfo['segmentDuration']

    def getBitrates(self, typ):
        return self.infos[typ]['bitrates']

    def calculateTimeDrift(self):
        timeUrl = urljoin(self.mpdUrl, self.timeUrl)
        time = urlopen(timeUrl).read().decode("utf8")
        time = datetime.datetime.strptime(time, "%Y-%m-%dT%H:%M:%SZ").timestamp()
        sysTime = datetime.datetime.now().timestamp()
        self.drift = time - int(sysTime)

    def getChunkUrl(self, typ, segId, ql):
        url = urljoin(self.mpdUrl, f"chunk/{typ}-{ql}-{segId}")
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

    def loadInitFiles(self):
        initAudio = [urlopen(self.getChunkUrl("audio", "init", x)).read() for x,m in enumerate(self.audInfo["repIds"])]
        initVideo = [urlopen(self.getChunkUrl("video", "init", x)).read() for x,m in enumerate(self.vidInfo["repIds"])]
        self.initFiles = {"video": initVideo, "audio": initAudio}

    def getInitSize(self, typ, ql): #need a massive change to support group based solution
        return len(self.initFiles[typ][ql])

    def getInitFileDescriptor(self, typ, ql):
        fd = io.BytesIO(self.initFiles[typ][ql])
        return fd

    def getQlIndices(self, typ):
        return list(range(len(self.infos[typ]['bitrates'])))

class VideoStorage():
    def __init__(self, eloop, vidHandler, tmpDir):
        self.mediaContent = {} # [(typ, segId, ql)] = videoContent TODO move it to file System
        self.mediaSizes = {} # [(typ, segId, ql)] = size
        self.mediaDownloadInfo = {} # [(typ, segId, ql)] = (st, ed)
        self.availability = {
                    'typ_ql': {}, #[(typ, ql)] = segs
                    'typ_segId': {} #[(typ, segId)] = qls
                }
        self.dlHistory = {} #[typ] = ((segId, ql), clen, sttime, edtime)
        self.eloop = eloop
        self.vidHandler = vidHandler
        self.lastSeg = -1
        self.tmpDir = tmpDir

    def ended(self, segId):
        return self.lastSeg != -1 and segId > self.lastSeg

    def getAvailability(self, typ, segId, ql):
        assert typ in ['audio', 'video']
        if segId == 'init':
            if ql != "*":
                return self.vidHandler.getQlIndices(typ)
            else:
                return True
        if ql == '*' and segId == '*':
            raise NotImplementedError("Not implemented")
        if segId == '*':
            return self.availability['typ_ql'].get((typ, ql), [])
        segId = int(segId)
        if ql == '*':
            return self.availability['typ_segId'].get((typ, segId), [])
        return self.mediaContent.get((typ, segId, ql), False) != False

    def updateChunkSizes(self, fs):
        for segId, info in fs.items():
            if info is None:
                continue
            segId = int(segId)
            if len(info) == 0:
                if self.lastSeg == -1 or self.lastSeg >= segId:
                    self.lastSeg = segId - 1
            for mt, chunks in info.items():
                for ch in chunks:
                    self.mediaSizes[(mt, segId, ch[0])] = ch[1]

    def storeChunk(self, typ, segId, ql, content, sttime, entime):
        clen = len(content)
        assert (typ, segId, ql) not in self.mediaSizes or self.mediaSizes[(typ, segId, ql)] == clen
        url = f"{self.tmpDir}/{typ}_{segId}_{ql}"
        with open(url, "wb") as fp:
            fp.write(content)
        self.mediaContent[(typ, segId, ql)] = "file://" + url
        self.mediaDownloadInfo[(typ, segId, ql)] = (sttime, entime)
        self.availability["typ_ql"].setdefault((typ, ql), set()).add(segId)
        self.availability["typ_segId"].setdefault((typ, segId), set()).add(ql)
        self.dlHistory.setdefault(typ,[]).append(((segId, ql), clen, sttime, entime))
        if (typ, segId, ql) not in self.mediaSizes:
            self.mediaSizes[(typ, segId, ql)] = clen

    def storeRemoteChunk(self, typ, segId, ql, url):
        req = Request(urljoin(url, "/groupmedia"), data=json.dumps((typ, segId, ql)).encode(), method='POST')
        self.mediaContent[(typ, segId, ql)] = req
        self.availability["typ_ql"].setdefault((typ, ql), set()).add(segId)
        self.availability["typ_segId"].setdefault((typ, segId), set()).add(ql)

    def getChunkSize(self, typ, segId, ql):
        if segId == 'init':
            return self.vidHandler.getInitSize(typ, ql)
        l = self.mediaSizes.get((typ, segId, ql), -1)
        assert l > -1
        return l

    def getFileDescriptor(self, cb, typ, segId, ql):
        if segId == 'init':
            fd = self.vidHandler.getInitFileDescriptor(typ, ql)
            return cb(fd)

        url = self.mediaContent.get((typ, segId, ql), None) #elf.chunks.setdefault(mt, {}).setdefault(ql, {})
        fd = urlopen(url)
        return cb(fd) #making it async if required
#         content = self.mediaContent.get((typ, segId, ql), None) #elf.chunks.setdefault(mt, {}).setdefault(ql, {})
#         assert content is not None
#         fd = io.BytesIO(content)
#         return fd

    def getAvailableMaxQuality(self, typ, segId):
        vqls = self.getAvailability('video', segId, '*')
        if len(vqls) == 0:
            return -1
        return max(vqls)

    def getDownloadHistory(self, typ):
        return self.dlHistory.get(typ, [])[:]
