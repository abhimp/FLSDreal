import select
import socket
import sys
import queue
import json

class RecvData:
    def __init__(self):
        self.buf = ""
        self.parsed = []
        self.braces = 0
        self.pos = 0
        self.slash = False
        self.quote = False
        self.braceOpen = None
        self.braceClose = None

#=========================================================
    def append(self, buf):
        assert type(buf) == str
        self.buf += buf
        buf = self.buf
        while self.pos < len(self.buf):
            pos = self.pos
            self.pos += 1
            c = self.buf[pos]
            if self.braces == 0:
                if c == '{':
                    self.braces += 1
                    self.braceOpen = '{'
                    self.braceClose = '}'
                    self.buf = self.buf[pos:]
                    self.pos = 1
                elif c == '[' and False: #json always starts with {
                    self.braces += 1
                    self.braceOpen = '['
                    self.braceClose = ']'
                    self.buf = self.buf[pos:]
                    self.pos = 1
                continue
            if self.quote:
                if self.slash:
                    self.slash = False
                elif c == '"':
                    self.quote = False
                elif c == "\\":
                    self.slash = True
                continue
            else:
                if c == '"':
                    self.quote = True
                elif c == self.braceOpen:
                    self.braces += 1
                elif c == self.braceClose:
                    self.braces -= 1
                    if self.braces == 0:
                        try:
                            tmp = self.buf[:pos + 1]
                            jobj = json.loads(tmp)
                            self.buf = self.buf[pos+1:]
                            self.pos = 0
                            self.parsed += [jobj]
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


# Create a TCP/IP socket
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setblocking(0)

# Bind the socket to the port
server_address = ('0.0.0.0', 10000)
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
                msgq[s].append(dt.decode("utf8"))
                while True:
                    p = msgq[s].getObject()
                    print(p)
                    if p is None: break
            else:
                inputs.remove(s)
                del msgq[s]
                print("closing", s)

    for s in exceptions:
        print("closing", s)
        s.close()

