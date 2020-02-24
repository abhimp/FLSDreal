import threading
import select
import socket
import sys
import json
import pickle
import queue
import traceback as tb
import time
import multiprocessing as mp

from . import cprint
from . import ipc

KIND_KIND = "kind"
KIND_CALL = "call"
KIND_RETURN = "ret"
KIND_INIT = "init"
KIND_DATA_INIT = "data_init"
KIND_INIT_RET = "initret"
KIND_UPDATE_STATUS = "us"
KIND_UNKNOWN = "unknown"

class Semaphore:
    def __init__(self, *a, **b):
        self.sem = threading.Semaphore(*a, **b)
    def acquire(self, *a, **b):
        self.sem.acquire(*a, **b)
    def release(self, *a, **b):
        self.sem.release(*a, **b)

class CallableStubObj:
    def __init__(self, name, origFunc):
        self.name = name
        self.origFunc = origFunc
    def __call__(self, *a, **b):
        return self.origFunc(self.name, None, *a, **b)
    def asyncCall(self, callback, *args, **kwargs):
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
        self.callIdLock = threading.Lock()
        self.timeers = {}
#         self.callSems = {}
        self.callReturns = {}
        self.callName = {}
        self.callAsyncCBs = {}
        self.sem = Semaphore(0)
        self.createDataSocket = None

    def getChunk(self, mt, ql, segId): #it should return a socket
        if self.createDataSocket is None or not callable(self.createDataSocket):
            cprint.orange("returning None")
            return None
        return self.createDataSocket(self, mt, ql, segId)

    def getNextCallId(self):
        self.callIdLock.acquire()
        clid = self.callId
        self.callId += 1
        self.callIdLock.release()
        return clid

    def getAddr(self):
        return self.addr

    def setRpc(self, stub, cb):
        stubWithMethod = {x: CallableStubObj(x, self.call) for x in stub}
        self.rpcs = stubWithMethod
        self.callback = cb
        pass

    def call(self, name, asyncCB, *a, **b):
        sem = None
        clid = self.getNextCallId()
#         cprint.orange(f"RPC clid:{clid} for {name}")
        if asyncCB is None:
            self.lock.acquire()
#             sem = threading.Semaphore(0)
#             self.callSems[clid] = sem
        else:
            self.callAsyncCBs[clid] = asyncCB
#         cprint.orange(f"RPC {name} with clid:{clid} firing {asyncCB is None}")
        self.timeers[clid] = time.time()
        self.callName[clid] = name
        self.callback(name, self.sock, clid, *a, **b)
        if asyncCB is not None:
            return
        self.sem.acquire()
        self.lock.release()
#         sem.acquire()
#         del self.callSems[clid]
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
#         cprint.orange(f"RPC returned clid:{clid}")
#         cprint.orange(f"rpc {self.callName[clid]} took {time.time() - self.timeers[clid]}")
#         sem = self.callSems.get(clid, None)
        asyncCB = self.callAsyncCBs.get(clid, None)
        if asyncCB is not None:
            asyncCB(ret, error)
            return
        self.callReturns[clid] = (ret, error)
        self.sem.release()

    def __getattr__(self, name):
        if self.rpcs is None:
            raise AttributeError()
        if name in self.rpcs:
            return self.rpcs[name]
        if "exposed_" + name in self.rpcs:
            return self.rpcs["exposed_" + name]
        raise AttributeError("Attribute "+name+" not found")

class SendQueue:
    def __init__(self, tident):
        assert tident is not None
        self.tident = tident
        self.buffer = b""
        self.queue = queue.Queue()
        self.sem = threading.Semaphore(1)
        self.lock = threading.Lock()

    def put(self, buf, *args):
        assert self.tident != threading.get_ident()
        self.lock.acquire()
        if len(args) > 0:
            for x in args:
                buf += x
        self.queue.put((buf, True))
        self.sem.acquire()
        self.lock.release()

    def putnowait(self, buf, *args):
        assert self.tident == threading.get_ident()
        if len(args) > 0:
            for x in args:
                buf += x
        self.queue.put((buf, False))

    def send(self, con):
        assert self.tident == threading.get_ident()
        buf = self.buffer
        if len(buf) > 0:
            l = con.send(buf)
            buf = buf[l:]
        while len(buf) == 0 and not self.queue.empty():
            buf, clearSem = self.queue.get()
            if clearSem:
                self.sem.release()
            if len(buf) > 0:
                l = con.send(buf)
                buf = buf[l:]
        self.buffer = buf

    def empty(self):
        return len(self.buffer) == 0 and self.queue.empty()
    def clear(self):
        while not self.queue.empty(): self.queue.get_nowait()

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
        self.msgq = None
        self.sendq = None
        self.listeningThread = None

        self.peerRemovedCB = None
        self.dataConnRecvCB = None

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
        self.sendq = {}

        notificationSock.send(b"done") #inform parent thread that I am ready

        while server is not None:
            assert server.fileno() > 2
            inputs = [server, notificationSock] + self.peerConnections[:]
            output = [x for x, y in self.sendq.items() if not y.empty()]
#             print(inputs, output)
            readable, writable, exceptions = select.select(inputs, output, []) # Wait to for some data
#             cprint.blue(f"breaking off select at {time.time()}, in:", [x.fileno() for x in readable], "out:", [x.fileno() for x in writable])

            if notificationSock in readable:
                while True:
                    try:
                        notificationSock.recv(512) # When we add a extra connection to the
                            # self.peerConnections from main thread, we need to update get out
                            # of select call once. Otherwise, select wont poll the new socket.
                    except BlockingIOError:
                        break
                #continue

            for con in writable:
                self.sendq[con].send(con)

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
                elif s != notificationSock:
                    try:
                        dt = s.recv(1024)
                        if dt:
                            self.msgq[s].append(dt)
                            while True and s in self.msgq:
                                p = self.msgq[s].getObject()
                                if p is None:
                                    break
                                self.recvMsg(p, s)
                        else:
                            self.removeSocketFromMonitor(s)
                            cprint.red("closing", s)
                    except ConnectionResetError:
                        self.removeSocketFromMonitor(s)
                        cprint.red("closing due to exceptions", s)

            for s in exceptions:
                if s in inputs:
                    self.removeSocketFromMonitor(s)
                print("closing", s)
                s.close()
        cprint.red("Closed")

    def recvMsg(self, msg, con):
        if self.me.addr is None:
            self.me.addr = (con.getsockname()[0], self.port)

        kind = msg[0].get(KIND_KIND, KIND_UNKNOWN)

        funcs = {
            KIND_CALL: self.handleCall,
            KIND_RETURN: self.handleReturn,
            KIND_INIT: self.handleInit,
            KIND_DATA_INIT: self.handleDataInit,
            KIND_INIT_RET: self.handleInitRet,
            KIND_UPDATE_STATUS: self.handleUpdateStatus,
        }

        func = funcs.get(kind, None)
        if func is not None:
            func(msg, con)

    def handleUpdateStatus(self, msg, con):
        assert con in self.neighbours
        peer = self.neighbours[con]
        status = pickle.loads(msg[1]) #0: func name, 1: args, 2: kwargs, 3: blob
        peer.status.update(status)

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
        msg = {KIND_KIND: KIND_CALL}
#         cprint.cyan(f"calling {name} for remote")
        self.sendMsg(con, msg, rpc)

    def addNeighbourStub(self, msg, con):
        stub = msg[0]["stub"]
        lport = msg[0]["lport"]
        for x in stub:
            if x not in self.stubFunctions:
                return False

        peer = RpcPeer()
        peer.setRpc(stub, self.call)
        peer.addr = (con.getpeername()[0], lport)
        peer.sock = con
        peer.read = True
        peer.createDataSocket = self.createDataSocket
        self.neighbours[con] = peer
        return peer

    def handleDataInit(self, msg, con):
        mt, ql, segId = msg[0]["rdata"]
        self.peerConnections.remove(con)
        sq = self.sendq[con]
        del self.msgq[con]
        del self.sendq[con]
#         del self.neighbours[con]
        sq.clear()
        self.notificationPipe.send(b"p")
        con.setblocking(True)
        if self.dataConnRecvCB is None or not callable(self.dataConnRecvCB):
            cprint.orange("data connection received and closing", self.dataConnRecvCB)
            con.send(chr(0).encode())
            con.close()
            return
        self.dataConnRecvCB(con, mt, ql, segId)
#         con.close()

    def handleInit(self, msg, con):
        assert con not in self.neighbours
        if not self.addNeighbourStub(msg, con):
            con.close()
            self.removeSocketFromMonitor(con)
            return

        rmsg = {
                KIND_KIND: KIND_INIT_RET,
                "stub": list(self.stubFunctions.keys()),
                "lport": self.port,
                }
        self.sendMsg(con, rmsg, sameThread=True)
#         con.close()

    def handleCall(self, msg, con):
        assert con in self.neighbours
        peer = self.neighbours[con]
        rpc = pickle.loads(msg[1]) #0: func name, 1: args, 2: kwargs, 3: blob
        fname, args, kwargs, blob = rpc
        rmsg = {KIND_KIND: KIND_RETURN}
        ret = None
        error = None

#         cprint.cyan(f"broadcasting {fname} from {peer}")
#         cprint.green(f"RPC {fname} received with callid {blob}")
        time1 = time.time()
        try:
            func = self.stubFunctions[fname]
            ret = func(peer, *args, **kwargs)
        except Exception:
            track = tb.format_exc()
            error = track
        time2 = time.time()
#         cprint.green(f"RPC {fname} returning with callid {blob}")
        payload = [ret, error, blob]
        self.sendMsg(con, rmsg, payload, sameThread=True)

    def sendMsg(self, sock, msg, payload = None, sameThread=False):
        pl = b""
        if payload is not None:
            pl = pickle.dumps(payload)
            msg["payloadLen"] = len(pl)
        bmsg = json.dumps(msg, default=encodeObject).encode("utf8")
        if sameThread:
            self.sendq[sock].putnowait(bmsg, pl)
        else:
            self.sendq[sock].put(bmsg, pl)
        self.notificationPipe.send(b"p")

    def addSocketToMonitor(self, con):
        assert self.listeningThread is not None
        self.peerConnections.append(con)
        self.msgq[con] = ipc.RecvData()
        self.sendq[con] = SendQueue(self.listeningThread.ident)
        con.setblocking(0)
        self.notificationPipe.send(b"p")

    def removeSocketFromMonitor(self, sock):
        self.peerConnections.remove(sock)
        if self.newConnectionSocket == sock:
            self.newConnectionSem.release()
        if self.peerRemovedCB is not None and callable(self.peerRemovedCB):
            peer = self.neighbours[sock]
            self.peerRemovedCB(peer)
        del self.msgq[sock]
        sq = self.sendq[sock]
        del self.sendq[sock]
        del self.neighbours[sock]
        sq.clear()

    def createDataSocket(self, peer, mt, ql, segId):
        laddr = peer.addr
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
#         print(laddr)
        s.connect(laddr)
        msg = {
                KIND_KIND: KIND_DATA_INIT,
                "rdata": [mt, ql, segId],
                }
        dmsg = json.dumps(msg).encode() #bypassing sendmsg as it need in stream format
                                        #also it will blocking socket
        s.send(dmsg)
        return s

    def connectTo(self, addr):
        self.newConnectionLock.acquire()
#         cprint.orange(addr)
        assert self.msgq != None
        assert self.newConnectionSocket == None
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        s.connect(addr)
        self.addSocketToMonitor(s)
        msg = {
                KIND_KIND: KIND_INIT,
                "stub": list(self.stubFunctions.keys()),
                "lport": self.port,
                }
        self.newConnectionSocket = s
        self.sendMsg(s, msg)
        self.newConnectionSem.acquire()
        peer = self.neighbours.get(s, None)
        self.newConnectionSocket = None
        self.newConnectionLock.release()
        return peer

    def shutdown(self):
        if self.server is not None:
            self.server.shutdown(socket.SHUT_RDWR)
            self.listeningThread.join()

    def addPeerRemovedCB(self, cb):
        assert callable(cb)
        self.peerRemovedCB = cb

    def addDataConnRecvCB(self, cb):
        assert callable(cb)
        self.dataConnRecvCB = cb



class TestPeer(RpcPeer):
    def __init__(self, id, addr=None, playbackTime=-1, **kwargs):
        self.id = id
        self.curPlaybackTime = playbackTime
        super().__init__(addr)

    def setStatus(self, playbackTime):
        self.curPlaybackTime = playbackTime

    def exposed_myname(self, *a):
        return "ds" + str(self.id)

    def exposed_num(self, rp, num):
        print(f"got call with num={num}")
        threading.Timer(1, rp.num2.asyncCall, args=(ret, num+200)).start()
        return num + 1

    def exposed_num2(self, rp, num):
        print(f"got call with num={num}")

    def getDict(self):
        return {
            "id" : self.id,
            "playbackTime": self.curPlaybackTime,
            }

    def __repr__(self):
        return "<TestPeer " \
                + f"{self.getDict()}" \
                + ">"
def dataSockr(sock, p):
    p = sock.read()
    sock.close()
    exit(0)
def dataSockRecv(sock, *a):
    print(sock, a)
    proc = mp.Process(target=dataSockr, args=(sock, a))
    proc.start()

def testAsParent():
    p = TestPeer(9800)
    mon = RpcManager(9800, p)
#     mon.dataConnRecvCB = dataSockRecv
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
    for x in range(100):
        cprint.blue(p.exposed_num.asyncCall(ret, x))
        cprint.blue(p.exposed_num(x))
    s = p.getChunk(4,5,6)
    print(s, p)
    time.sleep(5)
    s.close()
    mon.shutdown()

