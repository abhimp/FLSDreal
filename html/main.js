//VIDEO INFO
var vidBufMan = null
var audBufMan = null

var intervalTimer = null


//Elements
var videoElement = null

//Counter
var audSegUpto = -1
var vidSegUpto = -1

var SegmentUpdatedCallBack = null

function onSegmentAdded(repId, segId, mediaType){
    if(mediaType == "vid")
        vidSegUpto = segId
    if(mediaType == "aud")
        audSegUpto = segId

    if(audSegUpto == -1 || audSegUpto != vidSegUpto)
        return

    if(SegmentUpdatedCallBack !== null){
        SegmentUpdatedCallBack()
        SegmentUpdatedCallBack = null
    }

}

function initiate(headerInfo){

    var retData = headerInfo //JSON.parse(headerInfo)
    vidInfo = retData["vidInfo"]
    audInfo = retData["audInfo"]

    initVideoPlayer(function() {
        vidBufMan = new BufferManager(msrc, vidInfo, "vid")
        audBufMan = new BufferManager(msrc, audInfo, "aud")
        vidBufMan.onSegmentUpdated(onSegmentAdded)
        audBufMan.onSegmentUpdated(onSegmentAdded)
        setTimeout(function(){
            getAction()
        }, 100)
    })
}



function initVideoPlayer(callback) {
    if ('MediaSource' in window) {
        msrc = new MediaSource;
        videoElement.src = URL.createObjectURL(msrc)
        msrc.addEventListener('sourceopen', function(_) {
            callback()
        })
    }
    else{
        console.error("error")
    }
}

function applyAction(body, info) {
    if (typeof info["actions"] == "undefined")
        return

    if (typeof info["segs"] == "undefined")
        return

    var segs = info["segs"]
    for(i = 0; i < segs.length; i++){
        var seg = segs[i]
        if(seg === null)
            continue

        var segId = seg['seg']
        var repId = seg['rep']
        var initOff = seg["ioff"]
        var initLen = seg["ilen"]
        var cOff = seg["coff"]
        var cLen = seg["clen"]
        var type = seg["type"]

        var bm = audBufMan
        if(type == "video"){
            bm = vidBufMan
        }
        if(initLen > 0){
            var initData = body.slice(initOff, initOff + initLen)
            bm.insertInit(repId, initData)
        }

        var c = body.slice(cOff, cOff + cLen)
        bm.insertChunk(repId, c, segId)
    }

    actions = info["actions"]
    if (typeof actions["seekto"] != "undefined"){
        videoElement.currentTime = actions["seekto"]
    }

    try{
        videoElement.play()
    } catch (e) {
        console.log(e)
    }
}

function getAction(){
    var xhr = new XMLHttpRequest()
    xhr.open("POST", "action", true)
    try{
        xhr.responseType = 'arraybuffer';
    }catch(err){}

    xhr.onreadystatechange = function() {
        if (this.readyState == 4) {
            setTimeout(getAction, 1000)
            if(this.status == 200) {
                var hd = this.getResponseHeader('X-Action-Info')
                var info = JSON.parse(hd)
                var body = this.response
                applyAction(body, info)
            }
        }
    }

    xhr.setRequestHeader("X-PlaybackTime", roundd(videoElement.currentTime, 3))
    xhr.setRequestHeader("X-Buffer", serializeTimerange(videoElement.buffered))

    xhr.send()
}


function setDomElement() {
    document.body.innerHTML = ""
    videoElement = document.createElement("video")
	document.body.appendChild(videoElement)
    videoElement.controls = true
    videoElement.autoplay = true
    //videoElement.muted = true
}

function setUpStart() {
    var button = document.createElement("button")
    button.style["position"] = "absolute"
    button.style["top"] = "50%"
    button.innerText = "Start"
    button.style["font-size"] = "2vw"

    var div = document.createElement("div")
    div.style["text-align"] = "center"

    div.appendChild(button)


    button.onclick = loadVideo
    document.body.appendChild(div)

}

function loadVideo(){
    setDomElement()
    var xhr = new XMLHttpRequest()
    xhr.open("POST", "init", true)
    try{
        xhr.responseType = 'text';
    }catch(err){}

    xhr.onreadystatechange = function() {
        if (this.readyState == 4 && this.status == 200) {
            var data = JSON.parse(this.response)
            initiate(data)
        }
    }
    xhr.send()
}

function serializeTimerange(timerange) {
    var p = []
    for(var i = 0; i < timerange.length; i++) {
        p.push([roundd(timerange.start(i), 3), roundd(timerange.end(i), 3)])
    }
    return JSON.stringify(p)
}

function roundd(num, digits) {
    var d = Math.pow(10, digits)
    return Math.round(num*d)/d
}

function start() {
//     setUpStart()
    loadVideo()
}

