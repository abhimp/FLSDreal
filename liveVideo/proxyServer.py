import http.server as httpserver
import socketserver
import json
import time
import shutil
import urllib.request as urllib
import io
import os, sys
import re
import time

import simulatelivempd as mpdparser
from mpdHandler import VideoPlayer

class MyHttpHandler(httpserver.SimpleHTTPRequestHandler):
    def __init__(self, req, client, server, directory=None):
        if directory is None:
            directory = "html"
        self.extraHeaders = {}
        super().__init__(req, client, server, directory=directory)

    def goTarget(self, targets):
        path = self.server.pathre.sub("/", self.path)
        func = self.sendErr
        maxl = 0
        for x, y in targets.items():
            if path.startswith(x):
                if maxl < len(x):
                    func = y
                    maxl = len(x)
        func(path)

    def do_GET(self):
        targets = {
            "/media/mpd": self.sendMpd,
            "/media/time": self.sendTime,
            "/media/": self.sendChunk,
            "/media/chunk/": self.sendChunkAbs,
	    "/media/sizes/": self.sendChunkSizes,
            "/media/mpdjson": self.sendMpdJson,
        }
        self.goTarget(targets)

    def sendOkHeader(self, contentlen, contenttype):
        self.send_response(200)
        self.send_header("Content-type", contenttype)
        self.send_header("Content-Length", contentlen)
        self.send_header("Access-Control-Allow-Origin", "*")
        for x,y in self.extraHeaders.items():
            self.send_header(x, y)
        self.end_headers()

    def sendErr(self, path, msg=b"NOT FOUND", code=404):
        self.send_response(code)
        self.send_header("Content-type", "text/html")
        self.send_header("Content-Length", len(msg))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(msg)

    def sendFileSizes(self):
        pass

    def sendMpd(self, path):
        videoPlayer = self.server.spclArg
        data = videoPlayer.getXML()
        self.sendOkHeader(len(data), "text/xml")
        self.wfile.write(bytes(data, "utf8"))

    def sendMpdJson(self, path):
        videoPlayer = self.server.spclArg
        data = videoPlayer.getJson()
        data = bytes(data, "utf8")
        self.sendOkHeader(len(data), "application/json")
        self.wfile.write(data)

    def sendTime(self, path):
        curtime = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        self.sendOkHeader(len(curtime), "text/html")
        self.wfile.write(bytes(curtime, "utf8"))

    def sendChunkSizes(self, path):
        chunkName = path[13:]
#         print(chunkName)
        videoPlayer = self.server.spclArg
        ql, segId = chunkName.split('-')
        if ql == '':
            ql = '*'
        sizes = videoPlayer.getChunkSizes(segId, ql)
        dt = json.dumps(sizes)
        self.sendOkHeader(len(dt), "application/json")
        self.wfile.write(bytes(dt, "utf8"))

    def sendChunk(self, path):
        chunkName = path[7:]
        rep, segId = chunkName.split("-")
        videoPlayer = self.server.spclArg
        fs = {}
        if segId != 'init':
            segId = int(segId)
            fs[segId] = videoPlayer.getChunkSizes(segId)
            fs[segId+1] = videoPlayer.getChunkSizes(segId+1)
        fd, mime = videoPlayer.getChunkFd(chunkName)
        length = -1
        try:
            length = fd.length
        except:
            try:
                cur = fd.tell()
                fd.seek(0, 2)
                length = fd.tell()
                fd.seek(cur, 0)
            except:
                pass
        assert length >= 0
        self.extraHeaders["X-Chunk-Sizes"] = json.dumps(fs)
        self.sendOkHeader(length, mime)
        shutil.copyfileobj(fd, self.wfile)
        fd.close()

class MyHttpServer(socketserver.ForkingMixIn, httpserver.HTTPServer):
    def __init__(self, spclArg, *arg, **kwarg):
        self.spclArg = spclArg
        self.pathre = re.compile('//+')
        super().__init__(*arg, **kwarg)

class ByteIoProxy(io.BytesIO):
    @property
    def length(self):
        cur = self.tell()
        self.seek(0, 2)
        l = self.tell()
        self.seek(cur, 0)
        return l

if __name__ == "__main__":
    url = "http://10.5.20.129:9876/dash/0b4SVyP0IqI/media/vid.mpd"
    port = 9876
    if len(sys.argv) >= 2:
        url = sys.argv[1]
    if len(sys.argv) >= 3:
        port = int(sys.argv[2])
    if "http_proxy" in os.environ:
        del os.environ["http_proxy"]
    videoPlayer = VideoPlayer(url)
    # videoPlayer = VideoPlayer("http://127.0.0.1:8000/vid.mpd")
    # videoPlayer = VideoPlayer("http://127.0.0.1:8000/dst/media/vid.mpd")
    with MyHttpServer(videoPlayer, ("", port), MyHttpHandler) as httpd:
        print("serving at port", port)
        httpd.serve_forever()
