import threading
import select
import socket
import sys
import json

from . import cprint
from . import ipc


RES_STATUS_OK = 200
RES_STATUS_UNKNOWN = 401

GROUP_STATE_NOT_CONNECTED = 0
GROUP_STATE_CONNECTED = 1
GROUP_STATE_REQ_TO_LEADER = 2
GROUP_STATE_REQ_TO_OTHER = 3

class Peer:
    def __init__(self, id, addr=None, playbackTime=-1, **kwargs):
        self.id = id
        self.addr = addr
        self.curPlaybackTime = playbackTime
        self.sock = None
        self.ready = False

    def setStatus(self, playbackTime):
        self.curPlaybackTime = playbackTime

    def getDict(self):
        return {
            "id" : self.id,
            "addr": self.addr,
            "playbackTime": self.curPlaybackTime,
            }

    def __repr__(self):
        return "<Peer " \
                + f"{self.getDict()}" \
                + ">"

def encodeObject(obj):
    getDict = getattr(obj, "getDict", None)
    if getDict is not None and callable(getDict):
        return obj.getDict()
    raise TypeError(obj)

class GroupManager:
    def __init__(self, options):
        self.otherUsers = []
        self.options = options
        self.peerConnections = []

        self.chunkConnections = []
        self.server = None
        self.peers = {}
        self.me = None
        self.msgq = None

        self.grpState = GROUP_STATE_NOT_CONNECTED
        self.pendingResponses = 0

        self.sem = threading.Semaphore(0)
        self.notificationPipe = None

    def updateMyState(self, playbackTime, buffers):
        if self.me is None:
            return
        self.me.setStatus(playbackTime)

    def startGroup(self):
        self.listeningThread = threading.Thread(target=self.startListening)
        self.listeningThread.start()

        self.sem.acquire()
        self.me = Peer(0)
        self.me.ready = True
        if self.options.neighbour_address is not None:
            self.connectToNeighbour()
        else:
            self.grpState = GROUP_STATE_CONNECTED #self connected

    def connectToNeighbour(self):
        assert self.msgq != None
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        addr, port = self.options.neighbour_address.split(":")
        s.connect((addr, int(port)))
        self.addSocketToMonitor(s)
        self.grpState = GROUP_STATE_REQ_TO_LEADER
        msg = {"typ": "join", "port": self.options.group_port}
        self.sendRequest(msg, s)

    def recvMsg(self, msg, sock):
        if self.me.addr is None:
            self.me.addr = (sock.getsockname()[0], self.options.group_port)

        kind = msg[0].get("kind", "unknown")

        funcs = {
            "res": self.handleResponse,
            "req": self.handleRequest,
            "brd": self.handleRcvBroadcast,
        }
        cprint.blue("msg", msg[0])

        func = funcs.get(kind, None)
        if func is not None:
            func(msg, sock)

    def handleResponse(self, msg, sock):
        status = msg[0].get("status", RES_STATUS_UNKNOWN)
        if status != RES_STATUS_OK:
            cprint.red("!!ERROR!!", msg[0])
            return
        typ = msg[0].get("typ", None)
        assert typ != None
        if self.grpState == GROUP_STATE_REQ_TO_LEADER:
            assert typ == "newjoin"
            memsg = msg[0]
            self.me.id = memsg["uid"]
            leader = memsg["myuid"]
            nmsg = {"typ": "newpeer", "me": self.me}
            for peer in memsg["users"]:
                s = sock
                if peer["id"] != leader:
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
                    s.connect(peer["addr"])
                    self.addSocketToMonitor(s)
                self.sendRequest(nmsg, s)
                self.pendingResponses += 1
            self.grpState = GROUP_STATE_REQ_TO_OTHER

        elif self.grpState == GROUP_STATE_REQ_TO_OTHER:
            assert self.pendingResponses > 0
            assert typ == "newpeer"
            peer = msg[0]["me"]
            self.peers[peer["id"]] = Peer(**peer)
            self.peers[peer["id"]].ready = True
            self.peers[peer["id"]].sock = sock
            self.pendingResponses -= 1
            nmsg = {"typ": "readyForGroup", "meuid": self.me.id} #broadcast
            if self.pendingResponses == 0:
                self.grpState = GROUP_STATE_CONNECTED
                self.broadcast(nmsg)
                print(self.peers)

        else:
            cprint.red(msg)
#         elif self.grpState == GROUP_STATE_REQ_TO_OTHER

    def handleRequest(self, msg, sock):
        typ = msg[0].get("typ", "")
        if typ == "join": #synchronisation i.e. portable
            self.newPlayer(msg[0], sock)

        elif typ == "newpeer":
            self.newPeer(msg[0], sock)

    def handleRcvBroadcast(self, msg, sock):
#         cprint.cyan(msg[0], sock)
        typ = msg[0].get("typ", "unknown")
        if typ == "readyForGroup":
            uid = msg[0]["meuid"]
            assert uid in self.peers
            p = self.peers[uid]
            p.ready = False
            print(self.peers)

    def newPeer(self, msg, sock):
        nPeer = msg["me"]
        self.peers[nPeer["id"]] = Peer(**nPeer)
        self.peers[nPeer["id"]].sock = sock
        nmsg = {"typ": "newpeer", "res": "accept", "me": self.me}
        self.responseSuccess(nmsg, sock)

    def newPlayer(self, msg, sock):
        port = msg.get("port")
        ip = sock.getpeername()[0]
        uid = max([x.id for x in list(self.peers.values()) + [self.me]]) + 1
        nmsg = {"typ": "newjoin", "join_res": "accept", "users": list(self.peers.values()) + [self.me], "uid": uid, "myuid": self.me.id}
        self.responseSuccess(nmsg, sock)

    def shutdown(self):
        if self.server is not None:
            self.server.shutdown(socket.SHUT_RDWR)
            self.listeningThread.join()

    def responseSuccess(self, msg, sock):
        msg["status"] = RES_STATUS_OK
        msg["kind"] = "res"
        sock.send(json.dumps(msg, default=encodeObject).encode("utf8"))

    def sendRequest(self, msg, sock):
        msg["kind"] = "req"
        sock.send(json.dumps(msg, default=encodeObject).encode("utf8"))

    def broadcast(self, msg):
        msg["kind"] = "brd"
        msg["me"] = self.me
        for sock in self.peerConnections:
            sock.send(json.dumps(msg, default=encodeObject).encode("utf8"))

#=======================================
    def startListening(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.setblocking(0)

        self.notificationPipe, notificationSock = socket.socketpair()
        notificationSock.setblocking(0)

        # Bind the socket to the port
        server_address = ('0.0.0.0', self.options.group_port)
        cprint.red('starting up on {} port {}'.format(*server_address),
              file=sys.stderr)
        server.bind(server_address)

        # Listen for incoming connections
        server.listen(5)

        self.server = server

        inputs = [server]
        outputs = []
        errors = []

        self.msgq = {}

        self.sem.release()
        while server is not None:
            inputs = [server, notificationSock] + self.peerConnections
            readable, writable, exceptions = select.select(inputs, outputs, inputs)

            if notificationSock in readable:
                while True:
                    try:
                        notificationSock.recv(512)
                    except BlockingIOError:
                        break
                continue

            for s in readable:
                if s is server:
                    try:
                        con, addr = s.accept()
                    except Exception:
                        server = None
                        break
                    print("new con req from", addr)
                    self.addSocketToMonitor(con)
                else:
                    dt = s.recv(1024)
                    if dt:
#                         cprint.green("recv len", len(dt), dt)
                        self.msgq[s].append(dt)
                        while True:
                            p = self.msgq[s].getObject()
#                             cprint.green(p)
                            if p is None:
#                                 cprint.green(self.msgq[s].getState())
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

    def addSocketToMonitor(self, con):
        self.peerConnections.append(con)
        self.msgq[con] = ipc.RecvData()
        con.setblocking(0)
        self.notificationPipe.send(b"p")

    def removeSocketFromMonitor(self, sock):
        self.peerConnections.remove(sock)
        del self.msgq[sock]
