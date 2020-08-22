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

def inWorker(func):
    def wrapperRunInWorker(ref, *a, **b):
        ref.mRunInWorkerThread(func, ref, *a, **b)
    return wrapperRunInWorker

def inMain(func):
    def wrapperRunInMain(ref, *a, **b):
        ref.mRunInMainThread(func, ref, *a, **b)
    return wrapperRunInMain

class GroupRpc:
    def __init__(self, eloop):
        self.vEloop = eloop
        self.vAddress = None

    def mRunInMainThread(self, func, *a, **b):
        self.vEloop.addTask(func, *a, **b)

    def mRunInWorkerThread(self, func, *a, **b):
        if self.vEloop.amIMainThread():
            self.vEloop.runInWorker(func, *a, **b)
            return
        cllObj = CallableObj(self.vEloop.runInWorker, func)
        self.vEloop.addTask(cllObj, *a, **b)

    @inWorker
    def mFetch(self, url, cb):
        st = time.time()
        ret = None
        resp = requests.get(url)
        if resp.status_code == 200:
            ret = 200, resp.content, resp.headers, st, time.time()
        else:
            ret = resp.status_code, None, resp.headers, st, time.time()
        cb(*ret)

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
        self.vVidHandler = VideoHandler(options.mpdPath)
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
        self.vGroupStarted = False # atleast neighbor require
        self.vGroupInited = False
        self.vIAmTheLeader = False
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

    @inMain
    def mBuffered(self, cb, typ, segId, ql, status, resp, headers, st, ed):
        assert status == 200
        headers = dict(headers)
        dt = headers.get('X-Chunk-Sizes', "{}")
        dt = json.loads(dt)
        self.vVidStorage.updateChunkSizes(dt)
        self.vVidStorage.storeChunk(typ, segId, ql, resp, st, ed)

        cb()

    def mBufferVideo(self, cb):
        segDur = self.vVidHandler.getSegmentDur()
        curPlaybackTime = self.vPlaybackTime if self.vPlaybackTime >= self.vSetPlaybackTime else self.vSetPlaybackTime
        bufVidUpto = self.vNextBuffVidSegId * segDur
        bufVidLen = bufVidUpto - curPlaybackTime

        if bufVidLen >= self.vMinBufferLength:
            return cb()

        ql = 0
        if self.vGroupStarted:
            if self.bufVidLen > (2*segDur):
                return cb()
            else:
                ql = 0 #fallback
        else:
            ql = 0 #TODO calculate

        if self.vVidStorage.getAvailability('video', self.vNextBuffVidSegId, ql):
            return cb()
        url = self.vVidHandler.getChunkUrl('video', self.vNextBuffVidSegId, ql)
        cllObj = CallableObj(self.mBuffered, cb, 'video', self.vNextBuffVidSegId, ql)
        self.mFetch(url, cllObj)
        self.vNextBuffVidSegId += 1
        if self.vNextBuffAudSegId - self.vStartSengId == 3:
            self.mGroupStart()

    def mBufferAudio(self, cb):
        segDur = self.vVidHandler.getSegmentDur()
        curPlaybackTime = self.vPlaybackTime if self.vPlaybackTime >= self.vSetPlaybackTime else self.vSetPlaybackTime
        bufAudUpto = self.vNextBuffAudSegId * segDur
        bufAudLen = bufAudUpto - curPlaybackTime

        if bufAudLen >= self.vMinBufferLength:
            return cb()

        if self.vVidStorage.getAvailability('audio', self.vNextBuffAudSegId, 0):
            return cb()
        url = self.vVidHandler.getChunkUrl('audio', self.vNextBuffAudSegId, 0)
        cllObj = CallableObj(self.mBuffered, cb, 'audio', self.vNextBuffAudSegId, 0)
        self.mFetch(url, cllObj)
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

        vql = self.vVidStorage.getAvailableMaxQuality('video', self.vNextSegId)
        if vql < 0:
            return cb(actions, [], [], 0)

        qualities = toDict(audio = 0, video = vql)
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


        cllObj = CallableObj(self.mSendResponse, cb)
        cllObj = CallableObj(self.mBufferVideo, cllObj)
        self.mBufferAudio(cllObj) #call sequence, audio->video->response

#================================================
# group related task
#================================================
    def mGroupStart(self):
        if self.vOptions.neighbourAddress is None: # I am the starter
            self.vGroupInited = True
            cprint.red("Group started as self group")
            return
        self.mRunInWorkerThread(self.mGroupStartInWorker)

    def mGroupStartInWorker(self):
        rpc = (self.vOptions.groupPort, self.vOptions.neighbourAddress) #TODO add more to send more info
        resp = requests.post("http://" + self.vOptions.neighbourAddress + "/groupjoin", data = json.dumps(rpc).encode())
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
        self.vGroupStarted = True
        self.vGroupInited = True
        cprint.red(f"Group started started {len(self.vNeighbors)} neighbors")

    def mGroupJoin(self, cb, peerIp, content):
        if not self.vGroupInited:
            return cb(("NotStarted",))
        data = None
        try:
            data = json.loads(content)
        except:
            return
        peerPort, myAddr = data
        if self.vMyGid is None:
            self.vMyGid = myAddr.split(":")[0] + ":" + str(self.vOptions.groupPort)
            self.vGroupStarted = True
            #TODO start group

        peerAddr = f"{peerIp}:{peerPort}"
        self.mBroadcast(self.mGroupPeerJoined, peerAddr)
        cb(("accepted", peerAddr, list(self.vNeighbors.keys())+[self.vMyGid]))

    def mGroupPeerJoined(self, peerAddr):
        peer = GroupRpc(self.vEloop)
        peer.vAddress = "http://" + peerAddr
        self.vNeighbors[peerAddr] = peer

    def mBroadcast(self, func, *a, **b):
        self._mBroadcast(func, *a, **b)

    @inWorker
    def _mBroadcast(self, func, *a, **b):
        funcname = func.__name__
        for gid,peer in self.vNeighbors:
            func = peer.getRpcObj(funcname)
            func(*a, **b)
        func = getattr(self, funcname)
        func(*a, **b)
