# Although it is called webviewtest it is the main program
# It directly dependendent on the VideoHandler and DummyPlayer
import argparse
import threading
import queue
import json
import http.server as httpserver
import shutil
import time
import multiprocessing as mp
import os
import tempfile
import threading
import shlex
import socket
import signal

# from util import multiprocwrap as mp
from util.VideoHandler import VideoHandler
from util.DummyPlayer import DummyPlayer


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

    def log_message(self, format, *args):
         pass

    def addHeaders(self, name, value):
        self.extraHeaders[name] = value

    def send_ok_header(self, contentlen, contenttype="text/plain"):
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
            totalStalled = float(self.headers.get("X-Stall", '0'))
            actions, segs, fds, l = getNextChunks(playbackTime, buffered, totalStalled)

            datas = {"actions": actions, "segs": segs}

            self.addHeaders("X-Action-Info", json.dumps(datas))
            self.send_ok_header(l, "application/octet-stream")
            for fd in fds:
                shutil.copyfileobj(fd, self.wfile)
        elif self.path.endswith('playbackEnded'):
#             self.server.server_close()
            threading.Thread(target=kill_server, args=(self.server,)).start()
            self.send_ok_header(0)

class MyHttpServer(httpserver.HTTPServer):
    pass

def kill_server(server):
    print("killing the server")
    time.sleep(2)
    server.shutdown()

dPlayer = None

def serveWithHttp(httpd):
    print("serving")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt as e:
        pass
    print("video ended")
    dPlayer.shutdown()

def initVideo():
    global dPlayer
    videoHandler = VideoHandler(options.mpdPath)
    print("Main Loaded")
    dPlayer = DummyPlayer(videoHandler, options)
    return videoHandler.getJson()

def getNextChunks(playbackTime, buffers, totalStalled):
    global dPlayer
    segDur = dPlayer.videoHandler.getSegmentDur()#.vidInfo["segmentDuration"]
    dPlayer.updateState(playbackTime, buffers, totalStalled)
#     print(playbackTime, segDur, buffers, dPlayer.nextSegId, dPlayer.setPlaybackTime)
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
    parser = argparse.ArgumentParser(description = "FLSD test")

    parser.add_argument('-m', '--mpd-path', dest="mpdPath", default=MPD_PATH, type=str)
    parser.add_argument('-p', '--groupListenPort', dest='groupPort', default=10000, type=int)
    parser.add_argument('-n', '--neighbourAddress', dest='neighbourAddress', default=None, type=str)
    parser.add_argument('-b', '--browserCommand', dest='browserCommand', default=None, type=str)
    parser.add_argument('-L', '--logDir', dest='logDir', default=None, type=str)
    parser.add_argument('-F', '--finishedSocket', dest='finSock', default=None, type=str)

    options = parser.parse_args()


def startWebThroughCommand(url):
    cmdLine = options.browserCommand
    cmdLine += f" \"{url}\""
    os.system(cmdLine)

def startWeb(port):
    time.sleep(2)
    url = "http://127.0.0.1:"+str(port)+"/index.html"
    if options is not None and options.browserCommand is not None:
        startWebThroughCommand(url)
        return
    tmpdir = tempfile.TemporaryDirectory()
    cmdLine = "chromium-browser"
    cmdLine += " --incognito"
    cmdLine += " --user-data-dir=\""+tmpdir.name+"\""
    cmdLine += " --no-proxy-server"
    cmdLine += " --autoplay-policy=no-user-gesture-required"
    cmdLine += " --no-first-run"
    cmdLine += " --enable-logging"
    cmdLine += " --log-level=0"
    cmdLine += " --no-default-browser-check"
    cmdLine += " --mute-audio"
    cmdLine += f" --app=\"{url}\""
    print(cmdLine)
#     cmd = shlex.split(cmdLine)
#     pid = os.fork()
#     if pid == 0:
#         os.execvp(cmd[0], cmd)
#     else:
#         return pid

#     return subprocess.Popen(cmd)
    os.system(cmdLine)
#     os.execvp(cmd[0], cmd)
    tmpdir.cleanup()
#     return

def informClose():
    if options.finishedSocket is None:
        return
    try:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM, 0)
        sock.connect(options.finishedSocket)
        sock.write(b"done")
        sock.close()
    except Exception:
        pass

def main():
    port = 0 #9876
    parseCmdArgument()
    with MyHttpServer(("",port), MyHandler) as httpd:
        print(httpd.server_address)
        p = mp.Process(target=startWeb, args = (httpd.server_address[1],))
#         p = startWeb(httpd.server_address[1])
        p.start()
        serveWithHttp(httpd)
        p.terminate()
        p.join()
#         os.kill(p, signal.SIGTERM)
#         os.wait()
        informClose()
        exit()

try:
    import cProfile as profile
except:
    import profile

if __name__ == "__main__":
#     profile.run("main()")
    main()
