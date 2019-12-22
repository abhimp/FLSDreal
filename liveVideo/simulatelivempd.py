from mpegdash.parser import MPEGDASHParser
import mpegdash
import time
from xml.dom import minidom
import urllib.parse as urlparse
from mpegdash.utils import write_child_node
import os
import json
import io
import isodate

REPRESENTATION_ID = "representationid"
NUMBER = "number"


def getDict(**kws):
    return kws

def getFormattedString(ids, rep, num):
    if len(ids) == len(rep):
        return str(num)
    return ids[len(rep):]%(num)

class SegTemp():
    def __init__(self, duration=None, initialization=None, media=None, startNumber=None, timescale=None, bitstreamSwitching=False):
        self.duration = duration
        self.initialization = initialization
        self.media = media
        self.startNumber = startNumber
        self.timescale = timescale
        self.bitstreamSwitching = bitstreamSwitching

        self.segs = None
        self.baseUrl = None
        self.segCnt = 1

        self.__num = 1

    def __iter__(self):
        self.__num = 0
        return self

    def __next__(self):
        if self.__num == 0:
            self.__num += 1
            return self
        else:
            raise StopIteration

    def getParsedUrl(self, composit, representionId=None, num=None):
        valid = ""
        offset = 0
        while True:
            s = composit.find('$', offset)
            if s < 0:
                break
            e = composit.find('$', s+1)
            valid += composit[offset:s]
            ids = composit[s+1:e]
            if ids.lower().startswith(REPRESENTATION_ID):
                valid += getFormattedString(ids, REPRESENTATION_ID, representionId)
            elif ids.lower().startswith(NUMBER):
                valid += getFormattedString(ids, NUMBER, num)
            else:
                raise Exception("error")
            offset = e + 1
        valid += composit[offset:]
        return urlparse.urljoin(self.baseUrl, valid)

    def getInitUrl(self, repId = None):
        return self.getParsedUrl(self.initialization, repId)

    def getFileUrl(self, repId = None, number = None):
        return self.getParsedUrl(self.media, repId, number)

    @staticmethod
    def create(segTmp, duration=None, baseUrl=None):
        if not segTmp:
            return None
        if type(segTmp) == list:
            tmp = [SegTemp.create(x, duration, baseUrl) for x in segTmp]
            return tmp if len(tmp) != 1 else tmp[0]

        self = SegTemp(
                    timescale = segTmp.timescale,
                    initialization = segTmp.initialization,
                    media = segTmp.media,
                    startNumber = segTmp.start_number,
                    duration = segTmp.duration,
                )

        if segTmp.segment_timelines:
            st = segTmp.segment_timelines[0]
            if st.Ss or len(st.Ss) > 0:
                self.segCnt = 0
            for s in st.Ss:
                if not self.duration and s.d:
                    self.duration = s.d
                self.segCnt += 1
                if s.r:
                    self.segCnt += int(s.r)
#             print(self.segCnt)

        self.baseUrl = baseUrl
        return self



class Representation():
    def __init__(self, bandwidth=None, codecs=None, frameRate=None, height=None, repId=None, mimeType=None, width=None, startNumber = None, audioSamplingRate = None):
        self.bandwidth = bandwidth
        self.codecs = codecs
        self.frameRate = frameRate
        self.height = height
        self.repId = repId
        self.mimeType = mimeType
        self.width = width
        self.startNumber = startNumber
        self.audioSamplingRate = audioSamplingRate

        self.segTmp = None

        self.__num = 1

    def __iter__(self):
        self.__num = 0
        return self

    def __next__(self):
        if self.__num == 0:
            self.__num += 1
            return self
        else:
            raise StopIteration

    def getInitUrl(self, segTmp=None):
        if not segTmp:
            segTmp = self.segTmp
        return segTmp.getInitUrl(self.repId)

    def getStartNumber(self):
        if self.startNumber:
            return startNumber
        return self.segTmp.startNumber

    def getFileUrl(self, number, segTmp=None):
        if not segTmp:
            segTmp = self.segTmp
        return segTmp.getFileUrl(self.repId, number)

    def getSegmentDuration(self):
        durations = [st.duration/st.timescale for st in self.segTmp]
        assert min(durations) == max(durations)
        return min(durations)

    @staticmethod
    def create(rep, duration=None, baseUrl=None):
        if not rep:
            return None
        if type(rep) == list:
            tmp = {x.id : Representation.create(x, duration, baseUrl) for x in rep}
            return tmp if len(tmp) != 1 else tmp[0]

        self = Representation(
                repId = rep.id,
                mimeType = rep.mime_type,
                codecs = rep.codecs,
                bandwidth = rep.bandwidth,
                width = rep.width,
                height = rep.height,
                frameRate = rep.frame_rate,
                audioSamplingRate = rep.audio_sampling_rate,
                )

        if rep.segment_templates:
            self.segTmp = SegTemp.create(rep.segment_templates, duration, baseUrl)
#             print(len(self.segTmp))
        return self


class AdaptationSet():
    def __init__(self, bitstreamSwitching=None, contentType=None, adpId=None, segmentAlignment=None, frameRate=None, lang=None):
        self.bitstreamSwitching = bitstreamSwitching
        self.contentType = contentType
        self.adpId = adpId
        self.segmentAlignment = segmentAlignment
        self.frameRate = frameRate
        self.lang = lang

        self.reps = None
        self.segTmp = None

        self.__num = 1

    def __iter__(self):
        self.__num = 0
        return self

    def __next__(self):
        if self.__num == 0:
            self.__num += 1
            return self
        else:
            raise StopIteration

#     def getBitrates(self):
#         return [rep.bandwidth for rep in self.reps]
#
    def getMimesNCodecs(self):
        mcs = [(rep.bandwidth, rep.mimeType, rep.codecs, rep.repId, rep.getStartNumber()) for x, rep in self.reps.items()]
        mcs.sort(key=lambda x:x[0])
        return list(zip(*mcs))

    def getSegmentDuration(self):
        durations = [rep.getSegmentDuration() for x, rep in self.reps.items()]
        assert min(durations) == max(durations)
        return min(durations)

    def getFileUrl(self, repId, segId):
        return self.reps[repId].getFileUrl(segId)

    def getInitUrl(self, repId):
        return self.reps[repId].getInitUrl()

    @property
    def channel(self):
        return self.contentType

    @staticmethod
    def create(adpSet, duration=None, baseUrl = None):
        if not adpSet:
            return None
        if type(adpSet) == list:
            tmp = [AdaptationSet.create(x, duration, baseUrl) for x in adpSet]
            return tmp if len(tmp) != 1 else tmp[0]

        self = AdaptationSet(
                    adpId = adpSet.id,
                    contentType = adpSet.content_type,
                    segmentAlignment = adpSet.segment_alignment,
                    bitstreamSwitching = adpSet.bitstream_switching,
                    frameRate = adpSet.frame_rate,
                    lang = adpSet.lang,
                )

        self.segTmp = SegTemp.create(adpSet.segment_templates, duration, baseUrl)
        self.reps = Representation.create(adpSet.representations, duration, baseUrl)

        return self

class AVInfo:
    def __init__(self, segmentDuration, bitrates, mime, codecs, repIds, startNumber, duration):
        self.bitrates = bitrates
        self.bitratesKbps = [x/1000 for x in bitrates]
        self.segmentDuration = segmentDuration
        self.mimeTypes = mime
        self.codecs = codecs
        self.repIds = repIds
        self.startNumber = startNumber
        self.duration = duration

    @property
    def segmentCount(self):
        if self.duration is not None:
            return math.ceil(self.duration/self.segmentDuration)

    def toJSON(self):
        return json.dumps(self.toDict())

    def toDict(self):
        return {
            "bitrates" : self.bitrates,
            "segmentDuration" : self.segmentDuration,
            "mimeTypes" : self.mimeTypes,
            "codecs" : self.codecs,
            "repIds" : self.repIds,
            "startNumber" : self.startNumber,
            "duration" : self.duration,
            "bitratesKbps" : self.bitratesKbps,
        }


def updateBaseUrl(oldUrl, newBase):
    return urlparse.urljoin(oldUrl, newBase)
#     assert neVwBase == None
#     objold =
#     obj = urlparse.urlparse(oldUrl)
#     baseUrl = obj.scheme + "://" + obj.netloc + os.path.dirname(obj.path)
#     return oldUrl

def getChannel(typ):
    channel = None
    if typ and "audio" in typ.lower():
        channel = "AUDIO"
    if typ and "video" in typ.lower():
        channel = "VIDEO"
    return channel

class MPDObject():
    def __init__(self):
        self.adpSets = {}
        self.newMpd = None
        self.oldmpd = None
        self.duration = None
        self.baseUrl = None

    @staticmethod
    def create(baseUrl, mpd=None):
        if not mpd:
            mpd = MPEGDASHParser.parse(baseUrl)

        assert len(mpd.periods) == 1
        durationStr = mpd.media_presentation_duration
        period = mpd.periods[0]

        adpSets = AdaptationSet.create(period.adaptation_sets, baseUrl = baseUrl)
        adpSetV = None
        adpSetA = None
        for x in adpSets:
            if x.channel.upper() == "VIDEO" and adpSetV is None:
                adpSetV = x
            elif x.channel.upper() == "AUDIO" and adpSetA is None:
                adpSetA = x

        self = MPDObject()
        self.adpSets = {"video": adpSetV, "audio": adpSetA}
        self.duration = None
        self.baseUrl = baseUrl
        self.oldmpd = mpd
        return self

    @property
    def segmentDuration(self):
        return self.adpSets["video"].getSegmentDuration()

    def getPlaybackDuration(self):
        return self.duration

    def getVideoInfo(self):
        mcs = self.adpSets["video"].getMimesNCodecs()
        return AVInfo(self.segmentDuration, *mcs, duration = self.duration)

    def getAudioInfo(self):
        mcs = self.adpSets["audio"].getMimesNCodecs()
        return AVInfo(self.segmentDuration, *mcs, duration = self.duration)

    @property
    def adpFiles(self):
        return self.getVideoInfo()

    def getXMLF(self):
        xml_doc = minidom.Document()
        write_child_node(xml_doc, 'MPD', self.mpd)
        f = io.StringIO()
        xml_doc.writexml(f)
        f.seek(0)
        return f

    def getXML(self):
        f = self.getXMLF()
        data = f.read()
        f.close()
        return data

    def getFileUrl(self, repId, segId):
        for mt, adp in self.adpSets.items():
            try:
                return adp.getFileUrl(repId, segId)
            except KeyError:
                pass

    def getInitUrl(self, repId):
        for key, adp in self.adpSets.items():
            try:
                return adp.getInitUrl(repId)
            except KeyError:
                pass

    @property
    def mpd(self):
        if self.newMpd:
            return self.newMpd
        mpd = mpegdash.nodes.MPEGDASH()
        mpd.xmlns = self.oldmpd.xmlns
        mpd.profiles = self.oldmpd.profiles
        mpd.min_buffer_time = "PT8S"
        mpd.type = "dynamic"

        minUpdate = self.oldmpd.minimum_update_period
        if not minUpdate:
            duration = self.oldmpd.media_presentation_duration
            if not duration:
                duration = "PT2100S"
            minUpdate = duration

        mpd.minimum_update_period = minUpdate
        mpd.availability_start_time = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(int(time.time()/(30*60))*(30*60)))

        mpd.utc_timings = []
        utc = mpegdash.nodes.Descriptor()
        utc.scheme_id_uri = 'urn:mpeg:dash:utc:http-iso:2014'
        utc.value = "/media/time"
        mpd.utc_timings.append(utc)

        period = mpegdash.nodes.Period()
        period.id = 0
        period.start = "PT0.0S"


        adpSets = []
        for mt, localAdpset in self.adpSets.items():
            if localAdpset is None:
                continue
            adpSet = mpegdash.nodes.AdaptationSet()
            adpSet.id = 0
            adpSet.content_type = localAdpset.contentType
            adpSet.segment_alignment = True
            adpSet.bitstream_switching = True

            channel = localAdpset.contentType.upper()

            reps = []
            st = localAdpset.segTmp
            for i,localRep in localAdpset.reps.items():
                if localRep.repId in set(["2", "5"]): continue #making mpd pensiev complient
                rep = mpegdash.nodes.Representation()
                rep.id = localRep.repId
                rep.mime_type = localRep.mimeType
                rep.codecs = localRep.codecs
                rep.bandwidth = localRep.bandwidth

                rep.frame_rate = localRep.frameRate
                rep.width = localRep.width
                rep.height = localRep.height
                rep.audio_sampling_rate = localRep.audioSamplingRate

                stmp = st if not localRep.segTmp else localRep.segTmp

                segtmp = mpegdash.nodes.SegmentTemplate()
                segtmp.timescale = stmp.timescale
                segtmp.duration = stmp.duration
                segtmp.media = "$RepresentationID$-$Number%05d$"
                segtmp.initialization = "$RepresentationID$-init"
                segtmp.start_number = stmp.startNumber

                rep.segment_templates = [segtmp]

                reps.append(rep)

            adpSet.representations = reps
            adpSets.append(adpSet)

        mpd.periods = [period]
        period.adaptation_sets = adpSets
        self.newMpd = mpd
        return mpd



def parse(url):
    return MPDObject.create(url)

def testFormat():
    compo = 'chunk-stream$RepresentationID$-$Number%05d$.m4s'
    [print(getParsedUrl(compo, x, y)) for x in [5,9,'4'] for y in range(10)]


def test():
    testFormat()


if __name__ == "__main__":
#     test()
    x = getFileListsFromMpd("http://10.5.20.129:9876/dash/0b4SVyP0IqI/media/vid.mpd")
    print(json.dumps(x))
