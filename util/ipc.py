import select
import socket
import sys
import queue
import json

class RecvData:
    def __init__(self):
        self.buf = b""
        self.parsed = []
        self.braces = 0
        self.pos = 0
        self.slash = False
        self.quote = False
        self.braceOpen = None
        self.braceClose = None
        self.mode = "json" ## json or stream
        self.request = None

    def getState(self):
        string = "" \
            + f"buf = {self.buf}, " \
            + f"parsed = {self.parsed}, " \
            + f"braces = {self.braces}, " \
            + f"pos = {self.pos}, " \
            + f"slash = {self.slash}, " \
            + f"quote = {self.quote}, " \
            + f"braceOpen = {self.braceOpen}, " \
            + f"braceClose = {self.braceClose}, " \
            + f"mode = {self.mode}, " \
            + f"request = {self.request} "
        return string

#=========================================================
    def append(self, buf):
        self.buf += buf
#         buf = self.buf
        while self.pos <= len(self.buf):
            if self.request is not None:
                pLen = self.request[0].get("payloadLen", 0)
                assert self.pos == 0
                blen = len(self.buf)
                if blen < pLen:
                    break
                payload = self.buf[:pLen]
                self.buf = self.buf[pLen:]
                self.request[1] = payload
                self.parsed += [self.request]
                self.request = None
                continue
            if self.pos >= len(self.buf):
                break
            pos = self.pos
            self.pos += 1
            c = self.buf[pos]
            if self.braces == 0:
                if c == ord('{'):
                    self.braces += 1
                    self.braceOpen = ord('{')
                    self.braceClose = ord('}')
                    self.buf = self.buf[pos:]
                    self.pos = 1
                elif c == ord('[') and False: #json always starts with {
                    self.braces += 1
                    self.braceOpen = ord('[')
                    self.braceClose = ord(']')
                    self.buf = self.buf[pos:]
                    self.pos = 1
                continue
            if self.quote:
                if self.slash:
                    self.slash = False
                elif c == ord('"'):
                    self.quote = False
                elif c == ord("\\"):
                    self.slash = True
                continue
            else:
                if c == ord('"'):
                    self.quote = True
                elif c == self.braceOpen:
                    self.braces += 1
                elif c == self.braceClose:
                    self.braces -= 1
                    if self.braces == 0:
                        try:
                            tmp = self.buf[:pos + 1]
                            jobj = json.loads(tmp.decode("utf8"))
                            self.buf = self.buf[pos+1:]
                            self.pos = 0
                            self.request = [jobj, b""]
                        except json.decoder.JSONDecodeError as e:
                            print("error in parsing", tmp)
                            raise e

#=========================================================
    def getObject(self):
        if len(self.parsed) == 0:
            return None
        ret = self.parsed[0]
        del self.parsed[0]
        return ret


#=========================================================
#=========================================================

def listenAsGroup(port = 10000, groupMan=None):
    # Create a TCP/IP socket
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setblocking(0)

    # Bind the socket to the port
    server_address = ('0.0.0.0', port)
    print('starting up on {} port {}'.format(*server_address),
          file=sys.stderr)
    server.bind(server_address)

    # Listen for incoming connections
    server.listen(5)

    inputs = [server]
    outputs = []
    errors = []

    msgq = {}

    while inputs:
        readable, writable, exceptions = select.select(inputs, outputs, inputs)

        for s in readable:
            if s is server:
                con, addr = s.accept()
                con.setblocking(0)
                inputs.append(con)
                msgq[con] = RecvData()
            else:
                dt = s.recv(1024)
                if dt:
                    msgq[s].append(dt)
                    while True:
                        p = msgq[s].getObject()
                        if p is None:
                            break
                        if groupMan is not None and "recvMsg" in groupMan.__dir__():
                            groupMan.recvMsg(p, s)
                        else:
                            print(p)
                else:
                    inputs.remove(s)
                    del msgq[s]
                    print("closing", s)

        for s in exceptions:
            print("closing", s)
            s.close()

if __name__ == "__main__":
#     listenAsGroup()
    grp = RecvData()
    grp.append(b'{"typ": "newjoin", "join_res": "accept", "users": [{"id": 0, "addr": ["10.5.20.220", 7845], "playbackTime": 348.067}], "uid": 1, "myuid": 0, "status": 200, "kind": "res"}')
    print(grp.getState())
