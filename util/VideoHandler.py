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

        self.getTimeDrift()
#         self.getInitFiles()

    def getSegmentDur(self):
        return self.vidInfo['segmentDuration']

    def getTimeDrift(self):
        timeUrl = urljoin(self.mpdUrl, self.timeUrl)
        time = urlopen(timeUrl).read().decode("utf8")
        time = datetime.datetime.strptime(time, "%Y-%m-%dT%H:%M:%SZ").timestamp()
        sysTime = datetime.datetime.now().timestamp()
        self.drift = time - int(sysTime)

    def getChunkUrl(self, ql, num, typ):
        rep = self.infos[typ]["repIds"][ql]
        actNum = self.infos[typ]["startNumber"][ql] + num if num != "init" else num
        url = urljoin(self.mpdUrl, f"{rep}-{actNum}")
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
