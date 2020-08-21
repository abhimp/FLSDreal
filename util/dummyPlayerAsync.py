import io
import math
import time
import json
import multiprocessing as mp
import random
import os
import numpy as np
# from urllib.request import urlopen
import requests


from util.videoHandlerAsync import VideoHandler
from util.videoHandlerAsync import VideoStorage
# from . import multiprocwrap as mp
from . import cprint
from . import nestedObject
from util.misc import CallableObj

class Socket:
    def __init__(self, sock):
        self.sock = sock

    def read(self, p):
        return self.sock.recv(p)

    def __getattr__(self, name):
        return getattr(self.sock, name)

def assertLog(cond, *k):
    if not cond:
        cprint.red("Assert failed:", *k)
    assert cond

class AutoUpdateObject():
    def __init__(self, callback, onUpdateCB=None):
        self.__int_values = {}
        self.__int_cb = callback
        self.__int_autoCommit = False
        self.__int_readonly = False
        self.__int_onUpdateCB = onUpdateCB
        self.__int_changed = set()

    def __getattr__(self, name):
        if name.startswith("__int_") or name.startswith("_AutoUpdateObject"):
            raise AttributeError(f"{name} not found")
        if name not in self.__int_values:
            raise AttributeError(f"{name} not found")
        return self.__int_values[name]

    def __setattr__(self, name, val):
        if name.startswith("__int_") or name.startswith("_AutoUpdateObject"):
            super().__setattr__(name, val)
            return
        if self.__int_readonly:
            raise PermissionError("Readonly object")
        self.__int_values[name] = val
        self.__int_changed.add(name)
        if self.__int_autoCommit:
            self.commit()
        if self.__int_onUpdateCB and callable(self.__int_onUpdateCB):
            self.__int_onUpdateCB(name)

    def update(self, kwargs):
        self.__int_values.update(kwargs)

    def setAutoCommit(self, autoCommit = True):
        if self.__int_cb is not None and callable(self.__int_cb):
            return
        self.__int_autoCommit = autoCommit

    def setReadOnly(self, readonly=True):
        self.__int_readonly = readonly

    def commit(self):
        if self.__int_readonly:
            raise PermissionError("Readonly object")
        if self.__int_cb is None or not callable(self.__int_cb):
            return
        keys = self.__int_changed.copy()
        self.__int_changed.clear()
        self.__int_cb({x: self.__int_values[x] for x in keys})

    def getCurrentStatus(self):
        return self.__int_values

    def getCb(self):
        return self.__int_cb

def toDict(**kw):
    return kw

def fetch(url, cb = None):
    st = time.time()
#     print("starttime")
    ret = None
    resp = requests.get(url)
    if resp.status_code == 200:
        ret = 200, resp.content, resp.headers, st, time.time()
    else:
        ret = resp.status_code, None, resp.headers, st, time.time()
    if cb is None:
        return ret
    cb(*ret)

def fetchInWorker(eloop, url, cb):
    eloop.runInWorker(fetch, url, CallableObj(eloop.addTask, cb))

def getRunInMainThreadObj(eloop, cb, *a):
    return CallableObj(eloop.addTask, cb, *a)

def getVideoHandler(eloop, options):
    status, res, headers, stt, edt = fetch(options.mpdPath)
#     print(options.mpdPath)
    if res is not None:
        return VideoHandler(options.mpdPath, json.loads(res.decode()))

class GroupRpc:
    def __init__(self, eloop):
        self.vEloop = eloop
        self.vAddress = None

    def mRunInMainThread(self, func, *a, **b):
        self.vEloop.addTask(func, *a, **b)

    def mRunInWorkerThread(self, func, *a, **b):
        self.vEloop.runInWorker(func, *a, **b)

    def mGroupRecvRpc(self, cb, content):
        rpc = None
        try:
            rpc = json.loads(content)
        except:
            return self.mGroupRpcResponse(cb, error="Json parsing error")

        func = rpc.get("func", "noop")
        a = rpc.get("args", [])
        b = rpc.get("kwargs", {})
        if not hasattr(self, func):
            return self.mGroupRpcResponse(cb, error=f"{func} not found")

        attr = getattr(self, func)
        if not callable(attr):
            return self.mGroupRpcResponse(cb, error=f"{func} not found")

        try:
            ret = attr(*a, **b)
        except:
            return self.mGroupRpcResponse(cb, error=f"exception occured") #TODO send the exception

        self.mGroupRpcResponse(cb, ret)

    def mGroupRpcResponse(self, cb, ret = None, error=None):
        if error is None:
            self.mRunInMainThread(cb, toDict(res="ok", ret=ret))
            return
        self.mRunInMainThread(cb, toDict(res="error", error=error))

    def mGroupSendRpc(self, cb, func, *a, **b): #blocking, so run in worker
        assert self.vAddress is not None
        if callable(func):
            func = func.__name__
        url = self.vAddress + "/groupcomm"
        rpc = {"func": func, "args": a, "kwargs": b}
        resp = requests.post(url, data = json.dumps(rpc).encode())
        ret = None
        if resp.status_code == 200:
            ret = resp.content
        if ret is not None:
            try:
                ret = json.loads(ret.decode())
            except:
                ret = None

        if ret is not None:
            res = ret.get("res", None)
            if res == "ok":
                ret = ret.get("ret")
            elif res == "error":
                cprint.red("RPC res:", ret.get("error"))
                return
            else:
                cprint.red("RPC error unknow")
                return
        cb(ret) #cb should run in main thread

    def mGetRpcObj(self, name, cb=None):
        assert self.vAddress is not None
        if cb is None:
            cb = self.mNoop
        cllObj = CallableObj(self.mGroupSendRpc, cb, name)

    def mNoop(self, *a, **b):
        pass


class DummyPlayer(GroupRpc):
    def __init__(self, eloop, options):
        super().__init__(eloop)
        self.vOptions = options
        self.vVidHandler = getVideoHandler(eloop, options)
        self.vVidStorage = VideoStorage(self.vEloop, self.vVidHandler)

        self.vPlaybackTime = 0 # seconds
        self.vPlayerBuffers = [] # seconds
        self.vTotalStalled = 0 # seconds
        self.vSetPlaybackTime = 0

        self.vStartPlaybackTime = -1  # seconds
        self.vNextSegId = 0
        self.vNextBuffVidSegId = 0
        self.vNextBuffAudSegId = 0
        self.vStartSengId = -1
        self.vMinBufferLength = 30

        #========Group Info================
        self.vGroupStarted = False
        self.vNeighbors = {}
        self.vMyGid = None #same as my addr
        #==================================

        self.mInit()

    def mInit(self):
        dur = self.vVidHandler.getSegmentDur()
        self.vSetPlaybackTime = self.vVidHandler.expectedPlaybackTime()
        if self.vStartPlaybackTime == -1:
            self.vStartPlaybackTime = self.vSetPlaybackTime
#         print(self.vSetPlaybackTime, dur)
        self.vNextSegId = int(self.vSetPlaybackTime/dur)
        self.vStartSengId = self.vNextBuffVidSegId = self.vNextBuffAudSegId = self.vNextSegId

    def mGetJson(self):
        return self.vVidHandler.getJson()

    def mBuffered(self, cb, typ, segId, ql, status, resp, headers, st, ed):
        assert status == 200
        headers = dict(headers)
        dt = headers.get('X-Chunk-Sizes', "{}")
        dt = json.loads(dt)
        self.vVidStorage.updateChunkSizes(dt)
        self.vVidStorage.storeChunk(typ, segId, ql, resp, st, ed)

        cb()

    def mBufferVideo(self, cb):
        ql = 0 # TODO calculate
        if self.vVidStorage.getAvailability('video', self.vNextBuffVidSegId, ql):
            return cb
        url = self.vVidHandler.getChunkUrl('video', self.vNextBuffVidSegId, ql)
        fetchInWorker(self.vEloop, url, getRunInMainThreadObj(self.vEloop, self.mBuffered, cb, 'video', self.vNextBuffVidSegId, 0))
        self.vNextBuffVidSegId += 1
        if self.vNextBuffAudSegId - self.vStartSengId == 3:
            self.mGroupStart

    def mBufferAudio(self, cb):
        if self.vVidStorage.getAvailability('audio', self.vNextBuffAudSegId, 0):
            return cb()
        url = self.vVidHandler.getChunkUrl('audio', self.vNextBuffAudSegId, 0)
        fetchInWorker(self.vEloop, url, getRunInMainThreadObj(self.vEloop, self.mBuffered, cb, 'audio', self.vNextBuffAudSegId, 0))
        self.vNextBuffAudSegId += 1

    def mSendResponse(self, cb):
        if self.vVidStorage.ended(self.vNextSegId):
            return cb({}, [{"eof" : True}], [], 0)

        actions = {}
        if self.vSetPlaybackTime > 0:
            buffered = False
            for x in self.vPlayerBuffers:
                if x[0] <= self.vSetPlaybackTime and x[1] >= self.vSetPlaybackTime:
                    buffered = True
                    break
            if buffered:
                actions["seekto"] = self.vSetPlaybackTime
                self.vSetPlaybackTime = -1

        segDur = self.vVidHandler.getSegmentDur()
        curPlaybackTime = self.vPlaybackTime if self.vPlaybackTime >= self.vSetPlaybackTime else self.vSetPlaybackTime
        bufUpto = self.vNextSegId * segDur
        bufLen = bufUpto - curPlaybackTime

        if bufLen > segDur:
            return cb(actions, [], [], 0)

        vqls = self.vVidStorage.getAvailability('video', self.vNextSegId, '*')
        if len(vqls) == 0:
            return cb(actions, [], [], 0)

        qualities = toDict(audio = 0, video = max(vqls))
        l = 0
        segs = []
        fds = []
        for mt in ["audio", "video"]:
            ql = qualities[mt]
            seg = {}
            seg['seg'] = self.vNextSegId
            seg['type'] = mt
            seg['rep'] = ql
            seg['ioff'] = l
            seg['ilen'] = self.vVidStorage.getChunkSize(mt, 'init', ql)
            l += seg['ilen']
            fds += [self.vVidStorage.getFileDescriptor(mt, 'init', ql)]

            seg['coff'] = l
            seg['clen'] = self.vVidStorage.getChunkSize(mt, self.vNextSegId, ql)
            l += seg['clen']
            fds += [self.vVidStorage.getFileDescriptor(mt, self.vNextSegId, ql)]
            segs += [seg]

        self.vNextSegId += 1
#         cprint.red("playbackTime:", self.vPlaybackTime, "expectedPlaybackTime:", self.vVidHandler.expectedPlaybackTime())
        cb(actions, segs, fds, l)

    def mGetNextChunks(self, cb, playbackTime, buffers, totalStalled):
        self.vPlayerBuffers = buffers[:]
        self.vPlaybackTime = playbackTime
        self.vTotalStalled = totalStalled


        segDur = self.vVidHandler.getSegmentDur()
        curPlaybackTime = self.vPlaybackTime if self.vPlaybackTime >= self.vSetPlaybackTime else self.vSetPlaybackTime
        bufAudUpto = self.vNextBuffAudSegId * segDur
        bufAudLen = bufAudUpto - curPlaybackTime
        bufVidUpto = self.vNextBuffVidSegId * segDur
        bufVidLen = bufVidUpto - curPlaybackTime

        if bufAudLen > self.vMinBufferLength and bufVidLen > self.vMinBufferLength:
            return self.mSendResponse(cb)

        cllObj = CallableObj(self.mSendResponse, cb)

        if bufAudLen > self.vMinBufferLength:
            return self.mBufferVideo(cllObj)
        if bufVidLen > self.vMinBufferLength:
            return self.mBufferAudio(cllObj)

        cllObj = CallableObj(self.mBufferVideo, cllObj)
        self.mBufferAudio(cllObj)

#================================================
# group related task
#================================================
    def mGroupStart(self):
        if self.vOptions.neighbourAddress is None: # I am the starter
            self.vGroupStarted = True
            cprint.red("Group started as self group")
            return
        self.mRunInWorkerThread(self.mGroupStartInWorker)

    def mGroupStartInWorker(self):
        rpc = (self.vOptions.groupPort, self.vOptions.neighbourAddress) #TODO add more to send more info
        resp = requests.post(self.vOptions.neighbourAddress + "/groupjoin", data = json.dumps(rpc).encode())
        ret = None
        if resp.status_code == 200:
            ret = resp.content
        else:
            return
        self.mGroupStartRecvReply(ret)

    def mGroupStartRecvReply(self, reply):
        assert reply is not None and len(reply) > 0
        reply = json.loads(reply)
        if reply[0] == "NotStarted":
            cprint.red("other side not ready yet. restarting after 2s")
            self.vEloop.setTimeout(2, self.mGroupStart)
            return
            #TODO schedule
        elif reply[0] == "denied":
            raise NotImplementedError("denied not implemented") #TODO
        elif reply[0] != "accepted":
            raise NotImplementedError("Error") #TODO

        #accepted
        status, myAddr, neighbors = reply
        self.vMyGid = myAddr
        self.vNeighbors = {}
        for x in neighbors:
            if x == self.vMyGid:
                continue
            peer = GroupRpc(self.vEloop)
            peer.address = x
            self.vNeighbors[x] = peer
        cprint.red("Group started started {len(self.vNeighbors} neighbors")

    def mGroupJoin(self, cb, peerIp, content):
        if not self.vGroupStarted:
            return cb(("NotStarted",))
        data = None
        try:
            data = json.loads(content)
        except:
            return
        peerPort, myAddr = data
        if self.vMyGid is None:
            self.vMyGid = myAddr.split(":")[0] + ":" + self.vOptions.groupPort

        peerAddr = f"{peerIp}:{peerPort}"
        self.mBroadcast(self.mGroupPeerJoined, peerAddr)
        cb(("accepted", peerAddr, list(self.vNeighbors.keys())+[self.vMyGid]))

    def mGroupPeerJoined(self, peerAddr):
        peer = GroupRpc(self.vEloop)
        peer.vAddress = peerAddr
        self.vNeighbors[peerAddr] = peer

    def mBroadcast(self, func, *a, **b):
        self.mRunInWorkerThread(self.__mBroadcast, func, *a, **b)

    def __mBroadcast(self, func, *a, **b):
        funcname = func.__name__
        for gid,peer in self.vNeighbors:
            func = peer.getRpcObj(funcname)
            func(*a, **b)
        func = getattr(self, funcname)
        func(*a, **b)
