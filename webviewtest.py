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
import tempfile


from util.VideoHandler import VideoHandler
from util.DummyPlayer import DummyPlayer


INITIALCSS = """
body{color: black !important;}
video{width: 98vw;margin: 1vw 1vw 1vw 1vw;background-color: black;}
"""


videoHandler = None
theQ = None
options = None

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

dPlayer = None

def serveWithHttp(httpd):
    print("serving")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt as e:
        pass
    dPlayer.shutdown()

def initVideo():
    global dPlayer
    videoHandler = VideoHandler(options.mpd_path)
    print("Main Loaded")
    dPlayer = DummyPlayer(videoHandler, options)
    return videoHandler.getJson()

def getNextChunks(playbackTime, buffers):
    global dPlayer
    segDur = dPlayer.videoHandler.vidInfo["segmentDuration"]
    dPlayer.updateState(playbackTime, buffers)
    print(playbackTime, segDur, buffers, dPlayer.nextSegId, dPlayer.setPlaybackTime)
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

    segs, fds, l = dPlayer.getNextSeg()
    return actions, segs, fds, l


def parseCmdArgument():
    global options
    MPD_PATH = "/home/abhijit/Downloads/dashed/bbb/media/pens.mpd"
    parser = argparse.ArgumentParser(description = "Viscous test with post")

    parser.add_argument('-m', '--mpd-path', dest="mpd_path", default=MPD_PATH, type=str)
    parser.add_argument('-p', '--groupListenPort', dest='group_port', default=10000, type=int)
    parser.add_argument('-n', '--neighbourAddress', dest='neighbour_address', default=None, type=str)

    options = parser.parse_args()


def startWeb(port):
    tmpdir = tempfile.TemporaryDirectory()
    cmdLine = "chromium-browser"
#     cmdLine += " --no-user-gesture-required"
    cmdLine += " --incognito"
    cmdLine += " --user-data-dir=\""+tmpdir.name+"\""
    cmdLine += " --no-proxy-server"
    cmdLine += " --autoplay-policy=no-user-gesture-required"
    cmdLine += " --no-first-run"
    cmdLine += " --enable-logging"
    cmdLine += " --log-level=0"
#     cmdLine += " --start-maximized"
    cmdLine += " --no-default-browser-check"
    cmdLine += " --app=\"http://127.0.0.1:"+str(port)+"/index.html\""
#     cmdLine += " --aggressive-cache-discard"
    os.system(cmdLine)
    tmpdir.cleanup()
    return

def main():
    port = 0 #9876
    parseCmdArgument()
    with MyHttpServer(("",port), MyHandler) as httpd:
        print(httpd.server_address)
        p = mp.Process(target=startWeb, args = (httpd.server_address[1],))
        p.start()
        serveWithHttp(httpd)

if __name__ == "__main__":
    main()
