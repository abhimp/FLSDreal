import threading
import select
import socket
import sys
import json
import pickle
import traceback as tb

from . import cprint
from . import ipc

class CallableStubObj:
    def __init__(self, name, origFunc):
        self.name = name
        self.origFunc = origFunc
    def __call__(self, *a, **b):
        return self.origFunc(self.name, None, *a, **b)
    def asyncCall(self, callback, args=[], kwargs={}):
        return self.origFunc(self.name, callback, *args, **kwargs)

class RpcPeer:
    def __init__(self, addr = None):
        self.ready = False
        self.sock = None
        self.addr = addr
        self.rpcs = None
        self.callback = None
        self.callId = 0
        self.lock = threading.Lock()
        self.callSems = {}
        self.callReturns = {}
        self.callAsyncCBs = {}

    def getNextCallId(self):
        self.lock.acquire()
        clid = self.callId
        self.callId += 1
        self.lock.release()
        return clid

    def setRpc(self, stub, cb):
        stubWithMethod = {x: CallableStubObj(x, self.call) for x in stub}
        self.rpcs = stubWithMethod
        self.callback = cb
        pass

    def call(self, name, asyncCB, *a, **b):
        sem = None
        clid = self.getNextCallId()
        if asyncCB is None:
            sem = threading.Semaphore(0)
            self.callSems[clid] = sem
        else:
            self.callAsyncCBs[clid] = asyncCB
        self.callback(name, self.sock, clid, *a, **b)
        if asyncCB is not None:
            return
        sem.acquire()
        del self.callSems[clid]
        ret = self.callReturns.get(clid, None)
        retval, error = None, None
        if ret is not None:
            retval, error = ret
            del self.callReturns[clid]
        if error is not None:
            raise Exception(error)
        return retval


    def returnRecvd(self, blob, ret, error):
        clid = blob
        sem = self.callSems.get(clid, None)
        asyncCB = self.callAsyncCBs.get(clid, None)
        assert (sem is not None and asyncCB is None) or (sem is None and asyncCB is not None)
        if sem is None:
            asyncCB(ret, error)
            return
        self.callReturns[clid] = (ret, error)
        sem.release()

    def __getattr__(self, name):
        if name in self.rpcs:
            return self.rpcs[name]
        if "exposed_" + name in self.rpcs:
            return self.rpcs["exposed_" + name]
        raise AttributeError("Attribute "+name+" not found")


def encodeObject(obj):
    getDict = getattr(obj, "getDict", None)
    if getDict is not None and callable(getDict):
        return obj.getDict()
    raise TypeError(obj)

class RpcManager:
    def __init__(self, port, stub):
        assert stub is not None
        self.port = port
        self.me = stub
        self.stubFunctions = {x: getattr(stub, x) for x in dir(stub) if x.startswith("exposed") and callable(getattr(stub, x))}
        self.neighbours = {}
        self.peerConnections = []
        self.server = None

        self.newConnectionLock = threading.Lock()
        self.newConnectionSem = threading.Semaphore(0)
        self.newConnectionSocket = None

    def start(self):
        self.notificationPipe, notificationSock = socket.socketpair() # creating local pipe
                # to communicate with thread. I choose local socket over queue because it
                # bidirectional. notificationSock is for this thread and notificationPipe
                # main program.
        self.listeningThread = threading.Thread(target=self.startListening, args=(notificationSock,))
        self.listeningThread.start()
        self.notificationPipe.recv(512)

    def join(self):
        self.listeningThread.join()

    def startListening(self, notificationSock):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.setblocking(0)

        notificationSock.setblocking(0) # making thread end point no blocking as we are
                # going to poll using select call

        # Bind the socket to the port. expect new connections from other peers
        server_address = ('0.0.0.0', self.port)
        cprint.red('starting up on {} port {}'.format(*server_address), file=sys.stderr)
        server.bind(server_address)

        # Listen for incoming connections
        server.listen(5)

        self.server = server

        self.msgq = {}

        notificationSock.send(b"done") #inform parent thread that I am ready

        while server is not None:
            inputs = [server, notificationSock] + self.peerConnections[:]
            readable, writable, exceptions = select.select(inputs, [], []) # Wait to for some data

            if notificationSock in readable:
                while True:
                    try:
                        notificationSock.recv(512) # When we add a extra connection to the
                            # self.peerConnections from main thread, we need to update get out
                            # of select call once. Otherwise, select wont poll the new socket.
                    except BlockingIOError:
                        break
                continue

            for s in readable:
                if s is server: # this is boiler plate server acceptance code.
                    try:
                        con, addr = s.accept()
                    except Exception:
                        server = None
                        break
                    print("new con req from", addr)
                    self.addSocketToMonitor(con) # Socket monitor is a special type of deserializer
                        # it serialize input data in form of [json_object, byte_array].
                else:
                    dt = s.recv(1024)
                    if dt:
                        self.msgq[s].append(dt)
                        while True:
                            p = self.msgq[s].getObject()
                            if p is None:
                                break
                            self.recvMsg(p, s)
                    else:
                        self.removeSocketFromMonitor(s)
                        cprint.red("closing", s)

            for s in exceptions:
                if s in inputs:
                    self.removeSocketFromMonitor(s)
                print("closing", s)
                s.close()
        cprint.red("Closed")

    def recvMsg(self, msg, con):
        if self.me.addr is None:
            self.me.addr = (con.getsockname()[0], self.port)

        kind = msg[0].get("kind", "unknown")

        funcs = {
            "call": self.handleCall,
            "return": self.handleReturn,
            "init": self.handleInit,
            "initRet": self.handleInitRet,
        }

        func = funcs.get(kind, None)
        if func is not None:
            func(msg, con)

    def handleReturn(self, msg, con):
        peer = self.neighbours.get(con, None)
        if peer is None:
            cprint.red("return recieved but no peer found, discarding", "I should close the connection")
            return
        payload = pickle.loads(msg[1])
        ret, error, blob = payload
        peer.returnRecvd(blob, ret, error)

    def handleInitRet(self, msg, con):
        assert con not in self.neighbours
        self.addNeighbourStub(msg, con)
        if self.newConnectionSocket == con:
            self.newConnectionSem.release()
        cprint.red("connected")

    def call(self, name, con, blob, *args, **kwargs):
        rpc = (name, args, kwargs, blob)
        msg = {"kind": "call"}
        self.sendMsg(con, msg, rpc)

    def addNeighbourStub(self, msg, con):
        stub = msg[0]["stub"]
        for x in stub:
            if x not in self.stubFunctions:
                return False

        peer = RpcPeer()
        peer.setRpc(stub, self.call)
        peer.addr = con.getpeername()
        peer.sock = con
        peer.read = True
        self.neighbours[con] = peer
        return peer

    def handleInit(self, msg, con):
        assert con not in self.neighbours
        if not self.addNeighbourStub(msg, con):
            con.close()
            self.removeSocketFromMonitor(con)
            return

        rmsg = {
                "kind": "initRet",
                "stub": list(self.stubFunctions.keys()),
                }
        self.sendMsg(con, rmsg)

    def handleCall(self, msg, con):
        assert con in self.neighbours
#         cprint.magenta(msg[0], len(msg[1]))
        rpc = pickle.loads(msg[1]) #0: func name, 1: args, 2: kwargs, 3: blob
        fname, args, kwargs, blob = rpc
        rmsg = {"kind": "return"}
        ret = None
        error = None
        try:
            func = self.stubFunctions[fname]
            ret = func(*args, **kwargs)
        except Exception:
            track = tb.format_exc()
            error = track
        payload = [ret, error, blob]
        self.sendMsg(con, rmsg, payload)

    def sendMsg(self, sock, msg, payload = None):
        pl = b""
        if payload is not None:
            pl = pickle.dumps(payload)
            msg["payloadLen"] = len(pl)
        bmsg = json.dumps(msg, default=encodeObject).encode("utf8")
        sock.send(bmsg)
#         print(msg, len(pl))
        if len(pl) > 0:
            sock.send(pl)

    def addSocketToMonitor(self, con):
        self.peerConnections.append(con)
        self.msgq[con] = ipc.RecvData()
        con.setblocking(0)
        self.notificationPipe.send(b"p")

    def removeSocketFromMonitor(self, sock):
        self.peerConnections.remove(sock)
        if self.newConnectionSocket == sock:
            self.newConnectionSem.release()
        del self.msgq[sock]
        del self.neighbours[sock]

    def connectTo(self, addr):
        self.newConnectionLock.acquire()
        assert self.msgq != None
        assert self.newConnectionSocket == None
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        s.connect(addr)
        self.addSocketToMonitor(s)
        msg = {
                "kind": "init",
                "stub": list(self.stubFunctions.keys()),
                }
        self.newConnectionSocket = s
        self.sendMsg(s,msg)
        self.newConnectionSem.acquire()
        peer = self.neighbours.get(s, None)
        self.newConnectionLock.release()
        return peer

    def shutdown(self):
        if self.server is not None:
            self.server.shutdown(socket.SHUT_RDWR)
            self.listeningThread.join()



class TestPeer(RpcPeer):
    def __init__(self, id, addr=None, playbackTime=-1, **kwargs):
        self.id = id
        self.curPlaybackTime = playbackTime
        super().__init__(addr)

    def setStatus(self, playbackTime):
        self.curPlaybackTime = playbackTime

    def exposed_myname(self):
        return "ds" + str(self.id)

    def getDict(self):
        return {
            "id" : self.id,
            "playbackTime": self.curPlaybackTime,
            }

    def __repr__(self):
        return "<TestPeer " \
                + f"{self.getDict()}" \
                + ">"

def testAsParent():
    p = TestPeer(9800)
    mon = RpcManager(9800, p)
    mon.start()
    mon.join()

def ret(ret, error):
    cprint.cyan(ret, error)

def testAsClient():
    p = TestPeer(9801)
    mon = RpcManager(9801, p)
    mon.start()
    p = mon.connectTo(("localhost", 9800))
    cprint.green(p)
    cprint.blue(p.exposed_myname.asyncCall(ret))
    cprint.blue(p.exposed_myname())
    mon.shutdown()

