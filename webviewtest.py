import argparse
import threading
import queue
import json
import http.server as httpserver
import shutil
import time
import multiprocessing as mp
from urllib.request import urlopen
import os


from util.VideoHandler import VideoHandler


INITIALCSS = """
body{color: black !important;}
video{width: 98vw;margin: 1vw 1vw 1vw 1vw;background-color: black;}
"""


videoHandler = None
mainWindow = None
theQ = None

BROWSER_READY   = "READY"
PYVIEW_READY    = "PYREADY"
MAINJS_LOADED   = "JSLOADED"
BROWSER_CLOSING = "CLOSING"
PLAYER_READY    = "PLAYER"


class MyHandler(httpserver.SimpleHTTPRequestHandler):
    def __init__(self, req, client, server, directory=None):
        if directory is None:
            directory = "html"
        self.extraHeaders = {}
        super().__init__(req, client, server, directory=directory)

    def addHeaders(self, name, value):
        self.extraHeaders[name] = value

    def send_ok_header(self, contentlen, contenttype):
        self.send_response(200)
        self.send_header("Content-type", contenttype)
        self.send_header("Content-Length", contentlen)
        self.send_header("Access-Control-Allow-Origin", "*")
        for k, v in self.extraHeaders.items():
            self.send_header(k, v)
        self.end_headers()

    def do_POST(self):
        if self.path.endswith("init"):
            info = initVideo()
            self.send_ok_header(len(info), "application/json")
            self.wfile.write(info.encode("utf8"))
            return
        elif self.path.endswith("action"):
            playbackTime = float(self.headers.get("X-PlaybackTime"))
            buffered = json.loads(self.headers.get("X-buffer"))
            actions, segs, fds, l = getNextChunks(playbackTime, buffered)

            datas = {"actions": actions, "segs": segs}

            self.addHeaders("X-Action-Info", json.dumps(datas))
            self.send_ok_header(l, "application/octet-stream")
            for fd in fds:
                shutil.copyfileobj(fd, self.wfile)


class MyHttpServer(httpserver.HTTPServer):
    pass

class DummyPlayer:
    def __init__(self, videoHandler):
        self.playbackTime = 0
        self.setPlaybackTime = 0
        self.nextSegId = 0
        self.buffer = []
        self.videoHandler = videoHandler

        self.init()

    def init(self):
        dur = self.videoHandler.getSegmentDur()
        self.setPlaybackTime = self.videoHandler.expectedPlaybackTime()
        print(self.setPlaybackTime, dur)
        self.nextSegId = int(self.setPlaybackTime/dur)

dPlayer = None

def serveWithHttp(httpd):
    print("serving")
    httpd.serve_forever()

def initVideo():
    global dPlayer
    print("Main Loaded")
    dPlayer = DummyPlayer(videoHandler)
    return videoHandler.getJson()

def getNextChunks(playbackTime, buffers):
    global dPlayer
    segDur = videoHandler.vidInfo["segmentDuration"]
    print(playbackTime, segDur, buffers)
    actions = {}
    if dPlayer.setPlaybackTime > 0:
        buffered = False
        for x in buffers:
            if x[0] <= dPlayer.setPlaybackTime and x[1] >= dPlayer.setPlaybackTime:
                buffered = True
                break
        playbackTime = dPlayer.setPlaybackTime
        if buffered:
            actions["seekto"] = dPlayer.setPlaybackTime
            dPlayer.setPlaybackTime = -1


    segmentPlaying = int(playbackTime/segDur)
    if segmentPlaying + 1 < dPlayer.nextSegId:
        return actions, [], [], 0
    segs = []
    fds = []
    l = 0
    ql = 0
    for mt in ["audio", "video"]:
        seg = {}
        seg['seg'] = dPlayer.nextSegId
        seg['type'] = mt
        seg['rep'] = ql
        seg['ioff'] = l
        seg['ilen'] = videoHandler.getChunkSize(ql, 'init', mt)
        l += seg['ilen']
        fds += [videoHandler.getInitFileDescriptor(ql, mt)]

        seg['coff'] = l
        print("dasd", ql, dPlayer.nextSegId, mt)
        seg['clen'] = videoHandler.getChunkSize(ql, dPlayer.nextSegId, mt)
        l += seg['clen']
        fds += [videoHandler.getChunkFileDescriptor(ql, dPlayer.nextSegId, mt)]

        segs += [seg]

    dPlayer.nextSegId += 1
    return actions, segs, fds, l


def parseCmdArgument():
    MPD_PATH = "/home/abhijit/Downloads/dashed/bbb/media/pens.mpd"
    parser = argparse.ArgumentParser(description = "Viscous test with post")

    parser.add_argument('-m', '--mpd-path', dest="mpd_path", default=MPD_PATH, type=str)

    options = parser.parse_args()
    return options


def getVideoInfo(options):
    global videoHandler
    videoHandler = VideoHandler(options.mpd_path)

def startWeb(port):
    cmdLine = "chromium-browser --app=\"http://127.0.0.1:"+str(port)+"/index.html\""
    cmdLine += " --no-user-gesture-required"
    cmdLine += " --incognito"
#     cmdLine += " --aggressive-cache-discard"
    os.system(cmdLine)
    return

def main():
    global mainWindow, videoHandler, theQ
    port = 0 #9876
    options = parseCmdArgument()
    getVideoInfo(options)
    with MyHttpServer(("",port), MyHandler) as httpd:
        print(httpd.server_address)
        p = mp.Process(target=startWeb, args = (httpd.server_address[1],))
        p.start()
        serveWithHttp(httpd)

if __name__ == "__main__":
    main()
