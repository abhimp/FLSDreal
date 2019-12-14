function BufferManager(msrc, info, type){
    var mediaType = type
    var srcBuf = null
    var initFiles = {}
    var segments = []
    var loadingInit = false
    var intervalTimer = null
    var segmentUpdating = false
    var playbackSegmentDetails = []
    var updatingSegmentDetail = null
    var lastRepId = -1
    var onSegmentUpdate = []

    function init() {
        var mimeCodec = info.mimeTypes[0] + "; codecs=\"" + info.codecs[0] + "\""
        if(!MediaSource.isTypeSupported(mimeCodec)){
            throw "mimeCodec `"+mimeCodec+"` does not supported"
        }

        srcBuf = msrc.addSourceBuffer(mimeCodec)
        srcBuf.addEventListener("updateend", segmentLoaded)
        srcBuf.addEventListener("error", segmentLoadError)
    }

    function segmentLoadError(e) { //TODO use it later
        console.error(e)
    }

    function segmentLoaded() { //TODO use it later
        segmentUpdating = false
        if(loadingInit){
            setTimeout(function(){
                loadNextSeg()
            }, 10)
        }
        else{
            if(intervalTimer === null) {
                intervalTimer = setInterval(loadNextSeg, 1000)
            }

            var repId = updatingSegmentDetail[0]
            var segId = updatingSegmentDetail[2]
            playbackSegmentDetails[segId] = repId
            updatingSegmentDetail = null
            for (var i = onSegmentUpdate.length - 1; i >= 0; i--) {
                onSegmentUpdate[i](repId, segId, mediaType)
            }
        }
    }

    function loadNextSeg() {
        if(segments.length == 0)
            return
        if(segmentUpdating)
            return

        var repId = segments[0][0]
        var seg = segments[0][1]
        var num = segments[0][2]

        segmentUpdating = true
        updatingSegmentDetail = segments[0]
        if(lastRepId == repId){
            loadingInit = false
            segments.shift()
            srcBuf.appendBuffer(seg)
        }
        else{
            loadingInit = true
            try {
                //console.log(initFiles[repId])
                srcBuf.appendBuffer(initFiles[repId])
                lastRepId = repId
            } catch (error){
                delete initFiles[repId]
                console.error(error)
            }
        }
    }

    this.initAvailable = function(){
        p=[];
        for(var k in initFiles) {
            p.push(parseInt(k))
        }
        return p;
    }

    this.insertChunk = function(repId, segData, segId) {
        segments.push([repId, segData, segId]) //TODO
        if(intervalTimer === null) {
            intervalTimer = setInterval(loadNextSeg, 1000)
        }
    }

    this.insertInit = function(repId, initData) {
        initFiles[repId] = initData
    }

    this.getRepId = function(segId) {
        return playbackSegmentDetails[segId]
    }

    this.onSegmentUpdated = function(cb) {
        onSegmentUpdate.push(cb)
    }

    if (typeof msrc == "undefined" || typeof info == "undefined")
        throw "Invalid argument"
    init()
}
