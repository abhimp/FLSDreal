import io
import sys
import os
import json
import time
import urllib.request as urllib
import simulatelivempd as mpdparser

class FileWrapper:
    def __init__(self, fd, length):
        self.fd = fd
        self._length = length

    def __getattr__(self, name):
        try:
            return self.fd.__getattr__(name)
        except AttributeError as err:
            if name == "length":
                return self._length
            raise err

class ByteIoProxy(io.BytesIO):
    @property
    def length(self):
        cur = self.tell()
        self.seek(0, 2)
        l = self.tell()
        self.seek(cur, 0)
        return l

class VideoPlayerVoD:
    def __init__(self, url, options):
        if not os.path.exists(url):
            raise OSError("File does not exists!!")
        self.url = url
        self.mpd = mpdparser.parse(url)
        self.startNumbers = {}
        self.fileSizes = {} #[mt][segid][ql]
        self.initSizes = {}
        self.repId2MtQl = {}
        self.numSegs = -1

        self.autoStart = options.autoStart
        self.origStartTime = int(time.mktime(time.gmtime())/(60))*(60)# - 500 #debug
        self.readFileSizes()

    @property
    def duration(self):
        return self.mpd.getPlaybackDuration()

    def readFileSizes(self):
        for mt in ["audio", "video"]:
            vidInfo = self.getMpdObject(mt)
            initSizes = self.initSizes.setdefault(mt, [])
            self.startNumbers[mt] = vidInfo.startNumber
            for x, ql in enumerate(vidInfo.repIds):
                self.repId2MtQl[ql] = (mt, x)
                chk = self.mpd.getInitUrl(ql)
                try:
                    fs = os.stat(chk)
                    initSizes.append(fs.st_size)
                except FileNotFoundError:
                    initSizes.append(-1)

            if self.mpd.getPlaybackDuration() is None: # live
                continue
            duration = self.mpd.getPlaybackDuration()
            segId = 0
            fileSizes = self.fileSizes.setdefault(mt, [])
            while(True):
                startTime = self.mpd.segmentDuration * segId
                if startTime >= duration:
                    self.numSegs = segId
                    break
                p = []
                for x, ql in enumerate(vidInfo.repIds):
                    num = segId + vidInfo.startNumber[x]
                    chk = self.mpd.getFileUrl(ql, num)
                    fs = os.stat(chk)
                    p.append(fs.st_size)
                fileSizes.append(p)
                segId += 1

    def getMpdObject(self, mtype):
        if mtype in "video":
            return self.mpd.getVideoInfo()
        elif mtype == "audio":
            return self.mpd.getAudioInfo()
        return None

    def getVideoObj(self):
        return self.mpd.getVideoInfo()

    def getAudioObj(self):
        return self.mpd.getAudioInfo()

    def getXML(self):
        return self.mpd.getXML()

    def getJson(self):
        startTimeInt = int(time.mktime(time.gmtime())/(10*60))*(10*60)
        if not self.autoStart:
            startTimeInt = self.origStartTime
        pop = {
                "startTime": startTimeInt,
                "startTimeReadable": time.ctime(startTimeInt),
                "currentTimeReadable": time.ctime(int(time.mktime(time.gmtime()))),
                "timeUrl" : "/media/time",
                "vidInfo" : self.getVideoObj().toDict(),
                "audInfo" : self.getAudioObj().toDict(),
            }
        return json.dumps(pop)

    def getChunkSizes(self, segId, rep='*'):
        segId = int(segId)
        reps = [rep]
        if rep == '*':
            reps = self.repId2MtQl.keys()
        res = {}
        for r in reps:
            mt, ql = self.repId2MtQl[r]
            size = 0
            if segId == "init":
                size = self.initSizes[mt][ql]
            else:
                try:
                    num = segId
                    size = self.fileSizes[mt][num][ql]
                except Exception as e:
                    actNum = segId + self.startNumbers[mt][ql]
                    chk = self.mpd.getFileUrl(r, actNum)
                    if not os.path.exists(chk):
                        break
#                     print("Chunk", chk, ql, r, segId, num)
                    fs = os.stat(chk)
                    size = fs.st_size
            res.setdefault(mt, []).append([ql, size])
        return res

    def getChunkSize(self, ql, segId, mtype="video"):
        if ql >= len(self.initSizes[mtype]):
            raise Exception("Error")
        if segId == "init":
            return self.initSizes[mtype][ql]
        if segId >= len(self.fileSizes[mtype]):
            if self.mpd.getPlaybackDuration() is None: # live
                raise Exception("Not Implemented")
            else:
                raise Exception("Out of bound index")
            return
        return self.fileSizes[mtype][segId][ql]

    def getChunkFileDescriptor(self, rep, segId, mt):
        vidInfo = self.getMpdObject(mt)
        ql = vidInfo.repIds[rep]
        num = vidInfo.startNumber[rep] + segId
        return self.getChunkFdQl(ql, num)

    def getInitFileDescriptor(self, rep, mt):
        vidInfo = self.getMpdObject(mt)
        ql = vidInfo.repIds[rep]
        num = "init"
        return self.getChunkFdQl(ql, num)

    def getChunkFd(self, chunkName):
        ql, num = chunkName.split('-')
        return self.getChunkFdQl(ql, num)

    def getChunkFdQl(self, ql, num):
        chk = None
        if num == "init":
            chk = self.mpd.getInitUrl(ql)
        else:
            num = int(num)
            chk = self.mpd.getFileUrl(ql, num)
        try:
            fd = None
            mime = "application/octet-stream"
            print(chk)
            if os.path.exists(chk):
                fd = open(chk, "rb")
            else:
                url = chk
                fd = urllib.urlopen(url)
                mime = fd.headers.get_content_type()
            return fd, mime
        except:
            fd = ByteIoProxy()
            fd.write(b"Not Found")
            fd.seek(0, 0)
            return fd, "text/plain"

def VideoPlayer(*a, **b):
    return VideoPlayerVoD(*a, **b)

class VideoPlayerOld:
    def __init__(self, url):
        self.url = url
        self.mpd = mpdparser.parse(url)

    def getMpdJson(self):
        return json.dumps(self.mpd.adpFiles)

    def getXML(self):
        return self.mpd.getXML()

    def getChunkFd(self, chunkName):
        print("requested", chunkName)
        ql, num = chunkName.split('-')
        url = None
        try:
            num = int(num)
            chk = self.mpd.getFileUrl(ql, num)
        except:
            if num == "init":
                chk = self.mpd.getInitUrl(ql)

        try:
            print(chk, "ql,segid", ql, num)
            url = chk
            fd = urllib.urlopen(url)
            mime = fd.headers.get_content_type()
            return fd, mime
        except:
            try:
                fd = open(chk, "rb")
                return fd, "application/octet-stream"
            except:
                return None, None
                fd = ByteIoProxy()
                fd.write(b"Not Found")
                fd.seek(0,0)
                return fd, "text/plain"

