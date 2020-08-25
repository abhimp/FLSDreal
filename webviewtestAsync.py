# Although it is called webviewtest it is the main program
# It directly dependendent on the VideoHandler and DummyPlayer
import argparse
import threading
import queue
import json
import shutil
import time
import os
import tempfile
import multiprocessing as mp
import subprocess
import shlex
import socket
import signal
import sys
import http.server as httpserver
from http import HTTPStatus
import traceback as tb

from util.eventQueue import EventLoop
from util import cprint
# from util import multiprocwrap as mp
from util.misc import getTraceBack
from util.videoHandlerAsync import VideoHandler
from util.dummyPlayerAsync import DummyPlayer
from util.misc import CallableObj


options = None

class MyHandler(httpserver.SimpleHTTPRequestHandler):
    def __init__(self, req, client, server, directory=None):
        if directory is None:
            directory = "html"
        self.extraHeaders = {}

#==================================================
#      SimpleHTTPRequestHandler
#==================================================
        if directory is None:
            directory = os.getcwd()
        self.directory = os.fspath(directory)

#==================================================
#      BaseRequestHandler
#==================================================
        self.request = req
        self.client_address = client
        self.server = server
        self.setup()
        try:
#             cprint.blue("start:", self.request)
            self.handle(self.finish)
        except Exception:
            self.finish()

    def finish(self):
#         cprint.green("finished:", self.request)
        super().finish()
        self.server.shutdown_request(self.request)

#==================================================
    def handle_one_request(self, cb):
        """Handle a single HTTP request.
        You normally don't need to override this method; see the class
        __doc__ string for information on how to handle specific HTTP
        commands such as GET and POST.
        """
        try:
            self.raw_requestline = self.rfile.readline(65537)
            if len(self.raw_requestline) > 65536:
                self.requestline = ''
                self.request_version = ''
                self.command = ''
                self.send_error(HTTPStatus.REQUEST_URI_TOO_LONG)
                return cb()
            if not self.raw_requestline:
                self.close_connection = True
                return cb()
            if not self.parse_request():
                # An error code has been sent, just exit
                return cb()
            mname = 'do_cb_' + self.command
            if hasattr(self, mname):
                method = getattr(self, mname)
                try:
                    method(cb)
                except Exception as err:
                    info = sys.exc_info()
                    print(f"{getTraceBack(info)}", file=sys.stderr)
                    self.send_error(HTTPStatus.INTERNAL_SERVER_ERROR, "Internal erro")
                return
            mname = 'do_' + self.command
            if not hasattr(self, mname):
                self.send_error(
                    HTTPStatus.NOT_IMPLEMENTED,
                    "Unsupported method (%r)" % self.command)
                return cb()
            method = getattr(self, mname)
            method()
            self.wfile.flush() #actually send the response if not already done.
            cb()
        except socket.timeout as e:
            #a read or a write timed out.  Discard this connection
            self.log_error("Request timed out: %r", e)
            self.close_connection = True
            cb()
            return

    def handle(self, cb):
        """Handle multiple requests if necessary."""
        self.close_connection = True

        self.handle_one_request(cb)
#         while not self.close_connection:
#             self.handle_one_request()
#==================================================

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

    def sendNextSeg(self, cb, actions, segs, fds, totalLength):
        datas = {"actions": actions, "segs": segs}
        self.addHeaders("X-Action-Info", json.dumps(datas))
        self.send_ok_header(totalLength, "application/octet-stream")
#         cprint.red(f"fds: {fds}")
        for fd in fds:
            shutil.copyfileobj(fd, self.wfile)
        cb()

    def sendResponseFromFd(self, cb, fd, ln, status = 200):
        if status == 200:
            self.send_ok_header(ln, 'application/octet-stream')
            shutil.copyfileobj(fd, self.wfile)
            cb()
        else:
            self.send_error(status, "Some error")

    def sendResponse(self, cb, data=b""):
        l = 0
        rdata = b""
        ctyp = "application/octet-stream"
        if data is None:
            pass
        elif type(data) == list or type(data) == dict or type(data) == tuple:
            rdata = json.dumps(data).encode()
            ctype = 'application/json'
        elif type(data) == str:
            rdata = data.encode()
            ctyp = 'text/plain'
        elif type(data) == bytes:
            rdata = data
        else:
            pass
        l = len(rdata)
        self.send_ok_header(l, ctyp)
        self.wfile.write(rdata)
        cb()

    def callFuncInMainThread(self, func, cb, *a, **b):
        self.server.eloop.addTask(func, cb, *a, **b)

    def do_cb_POST(self, cb):
        prvtDt = self.server.privateData
        if self.path.startswith("/init"):
#             cprint.red("privateData:", prvtDt)
            if "dummyPlayer" not in prvtDt:
                prvtDt["dummyPlayer"] = DummyPlayer(self.server.eloop, options)
            info = prvtDt['dummyPlayer'].mGetJson()
            self.send_ok_header(len(info), "application/json")
            self.wfile.write(info.encode("utf8"))
#             cprint.red("cb:", cb)
            return cb()

        elif self.path.startswith("/action"):
            playbackTime = float(self.headers.get("X-PlaybackTime"))
            buffered = json.loads(self.headers.get("X-buffer"))
            totalStalled = float(self.headers.get("X-Stall", '0'))
            dummyPlayer = prvtDt.get("dummyPlayer", None)
            cb = CallableObj(self.sendNextSeg, cb)
            self.callFuncInMainThread(dummyPlayer.mGetNextChunks, cb, playbackTime, buffered, totalStalled)
#             cllObj = CallableObj(self.sendNextSeg, cb)
#             self.server.eloop.addTask(dummyPlayer.getNextChunks, playbackTime, buffered, totalStalled, cllObj)

        elif self.path.startswith('/playbackEnded'):
            cprint.red("playbackEnded")
            dummyPlayer = prvtDt.get("dummyPlayer", None)
            dummyPlayer.mPlaybackEnded()
            self.server.eloop.addTask(self.server.shutdown)
            self.send_ok_header(0)
            cb()

        elif self.path.startswith('/groupcomm'):
            contentlen = int(float(self.headers.get("Content-Length", 0)))
            if contentlen == 0:
                return cb()
            content = self.rfile.read(contentlen).decode()
            dummyPlayer = prvtDt.get("dummyPlayer", None)
            cb = CallableObj(self.sendResponse, cb)
            self.callFuncInMainThread(dummyPlayer.mGroupRecvRpc, cb, content)
#             DummyPlayer.groupRecv(content, cb)

        elif self.path.startswith('/groupjoin'):
            contentlen = int(float(self.headers.get("Content-Length", 0)))
            if contentlen == 0:
                return cb()
            content = self.rfile.read(contentlen).decode()
            dummyPlayer = prvtDt.get("dummyPlayer", None)
            cb = CallableObj(self.sendResponse, cb)
            clientIp = self.client_address[0]
            self.callFuncInMainThread(dummyPlayer.mGroupJoin, cb, clientIp, content)

        elif self.path.startswith('/groupmedia'):
            contentlen = int(float(self.headers.get("Content-Length", 0)))
            if contentlen == 0:
                return cb()
            content = self.rfile.read(contentlen).decode()
            dummyPlayer = prvtDt.get("dummyPlayer", None)
            cb = CallableObj(self.sendResponseFromFd, cb)
            clientIp = self.client_address[0]
            self.callFuncInMainThread(dummyPlayer.mGroupMediaRequest, cb, content)

        else:
            cb()
#         elif self.path.endswith('playbackEnded'):
#             self.server.server_close()
#             threading.Thread(target=kill_server, args=(self.server,)).start()
#             self.send_ok_header(0)


class EventLoopServer(httpserver.HTTPServer):
    eloop = None
    privateData = {}
    def process_request_thread(self, request, client_address):
        """Same as in BaseServer but as a thread.
        In addition, exception handling is done here.
        """
        try:
            self.finish_request(request, client_address)
        except Exception:
            self.handle_error(request, client_address)
            self.shutdown_request(request)

    def process_request(self, request, client_address):
        assert self.eloop is not None
        self.eloop.addTask(self.process_request_thread, request, client_address)

    def server_close(self):
        super().server_close()

def serveForever(eloop, httpd):
    httpd.serve_forever()
    cprint.red("serverClonsed")
    eloop.shutdown()

def servInEloop(httpd, eloop):
    eloop.runInWorker(serveForever, eloop, httpd)

def serveWithHttp(httpd):
    elogDir = None if options.logDir is None else (options.logDir + "/eloop.log")
    eloop = EventLoop(logFile=elogDir)
    httpd.eloop = eloop
    eloop.addTask(servInEloop, httpd, eloop)
    print("serving")
    eloop.run()
#     try:
#         httpd.serve_forever()
#     except KeyboardInterrupt:
#         pass
    print("video ended")

def parseCmdArgument():
    global options
    MPD_PATH = "/home/abhijit/Downloads/dashed/bbb/media/pens.mpd"
    parser = argparse.ArgumentParser(description = "FLSD test")


    parser.add_argument('-m', '--mpd-path', dest="mpdPath", default=MPD_PATH, type=str)
    parser.add_argument('-p', '--listenPort', dest='groupPort', default=10000, type=int)
    parser.add_argument('-n', '--neighbourAddress', dest='neighbourAddress', default=None, type=str)
    parser.add_argument('-b', '--browserCommand', dest='browserCommand', default=None, type=str)
    parser.add_argument('-L', '--logDir', dest='logDir', default=None, type=str)
    parser.add_argument('-F', '--finishedSocket', dest='finSock', default=None, type=str)
    parser.add_argument('-d', '--tmpDir', dest='tmpDir', default=None, type=str)

    options = parser.parse_args()

    if options.logDir is not None and not os.path.isdir(options.logDir):
        os.makedirs(options.logDir)
    if options.tmpDir is None or not os.path.isdir(options.tmpDir):
        if options.logDir is None:
            options.tmpDir = tempfile.mkdtemp(prefix="FLSD-")
        else:
            options.tmpDir = os.path.join(options.logDir, "tmpDir")
            os.makedirs(options.tmpDir)



def startWebThroughCommand(cmd, url):
    cmdLine = cmd
    cmdLine += f" \"{url}\""
    p = subprocess.Popen(shlex.split(cmdLine))
#     os.system(cmdLine)
#     cprint.red("Browser killed")
    return p

def startWeb(port):
    time.sleep(2)
    url = "http://127.0.0.1:"+str(port)+"/index.html"
    cprint.green("hello")
    if options is not None and options.browserCommand is not None:
        return startWebThroughCommand(options.browserCommand, url)

    return startWebThroughCommand("./browsers/chromium", url)

def informClose():
    if options.finSock is None:
        return
    try:
        print("Connecting to", options.finSock)
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM, 0)
        sock.connect(options.finSock)
        sock.send(b"done")
        sock.close()
    except Exception:
        exec_info = sys.exc_info()
        print("Connection to", options.finSock, "failed")
        print(getTraceBack(exec_info))
        pass

def main():
    parseCmdArgument()
    port = options.groupPort
    with EventLoopServer(("",port), MyHandler) as httpd:
        print(httpd.server_address)
        p = startWeb(httpd.server_address[1])
        serveWithHttp(httpd)
        p.terminate()
        cprint.red("Pkilled")
        informClose()
        shutil.rmtree(options.tmpDir)
        exit()

try:
    import cProfile as profile
except:
    import profile

if __name__ == "__main__":
#     profile.run("main()")
    main()
