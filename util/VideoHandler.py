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

        self.getTimeDrift()
        self.getInitFiles()

    def getSegmentDur(self):
        return self.vidInfo['segmentDuration']

    def getTimeDrift(self):
        timeUrl = urljoin(self.mpdUrl, self.timeUrl)
        time = urlopen(timeUrl).read().decode("utf8")
        time = datetime.datetime.strptime(time, "%Y-%m-%dT%H:%M:%SZ").timestamp()
        sysTime = datetime.datetime.now().timestamp()
        self.drift = time - int(sysTime)

    def getInitFiles(self):
        initAudio = [urlopen(urljoin(self.mpdUrl, f"{x}-init")).read() for x in self.audInfo["repIds"]]
        initVideo = [urlopen(urljoin(self.mpdUrl, f"{x}-init")).read() for x in self.vidInfo["repIds"]]
        self.initFiles = {"video": initVideo, "audio": initAudio}

#         self.initSizes = {x: [len(z) for z in y] for x, y in self.initFiles.items()}

    def getJson(self):
        return json.dumps({"vidInfo": self.vidInfo, "audInfo": self.audInfo})

    def getChunkSize(self, ql, num, typ):
        if num == "init":
            return len(self.initFiles[typ][ql])

        num = int(num)

        chunks = self.chunks.setdefault(typ, {}).setdefault(ql, {})

        if num not in chunks:
            rep = self.infos[typ]["repIds"][ql]
            actNum = self.infos[typ]["startNumber"][ql] + num
            url = urljoin(self.mpdUrl, f"{rep}-{actNum}")
            print(url, num)
            chunks[num] = urlopen(url).read()

        return len(chunks[num])

    def getInitFileDescriptor(self, ql, mt):
        fd = io.BytesIO(self.initFiles[mt][ql])
        return fd

    def getChunkFileDescriptor(self, ql, num, mt):
        fd = io.BytesIO(self.chunks[mt][ql][num])
        return fd

    def getTime(self):
        time = datetime.datetime.now().timestamp() + self.drift
        return time

    def expectedPlaybackTime(self):
        time = self.getTime()
        playbackTime = time - self.startTime
        return playbackTime
