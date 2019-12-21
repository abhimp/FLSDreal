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
    def goTarget(self, targets):
        self.extraHeaders = {}

        path = self.server.pathre.sub("/", self.path)
        func = self.send_err
        maxl = 0
        for x, y in targets.items():
            if path.startswith(x):
                if maxl < len(x):
                    func = y
                    maxl = len(x)
        func(path)

    def do_GET(self):
        targets = {
            "/media/mpd": self.send_mpd,
            "/media/time": self.send_time,
            "/media/": self.send_chunk,
            "/media/mpdjson": self.send_mpd_json,
        }
        self.goTarget(targets)

    def send_ok_header(self, contentlen, contenttype):
        self.send_response(200)
        self.send_header("Content-type", contenttype)
        self.send_header("Content-Length", contentlen)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()

    def send_err(self, path, msg=b"NOT FOUND", code=404):
        self.send_response(code)
        self.send_header("Content-type", "text/html")
        self.send_header("Content-Length", len(msg))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(msg)

    def sendFileSizes(self):
        pass

    def send_mpd(self, path):
        videoPlayer = self.server.spclArg
        data = videoPlayer.getXML()
        self.send_ok_header(len(data), "text/xml")
        self.wfile.write(bytes(data, "utf8"))

    def send_mpd_json(self, path):
        videoPlayer = self.server.spclArg
        data = videoPlayer.getJson()
        data = bytes(data, "utf8")
        self.send_ok_header(len(data), "application/json")
        self.wfile.write(data)

    def send_time(self, path):
        curtime = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        self.send_ok_header(len(curtime), "text/html")
        self.wfile.write(bytes(curtime, "utf8"))

    def send_chunk(self, path):
        chunkName = path[7:]
        videoPlayer = self.server.spclArg
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
        self.send_ok_header(length, mime)
        shutil.copyfileobj(fd, self.wfile)
        fd.close()

class MyHttpServer(socketserver.ForkingMixIn, httpserver.HTTPServer):
# class MyHttpServer(httpserver.HTTPServer):
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
