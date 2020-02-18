import threading
import select
import socket
import sys
import json

from . import cprint
from . import ipc
from . import groupManagerRpyc as GroupMan


RES_STATUS_OK = 200
RES_STATUS_UNKNOWN = 401

GROUP_STATE_NOT_CONNECTED = 0
GROUP_STATE_CONNECTED = 1
GROUP_STATE_REQ_TO_LEADER = 2
GROUP_STATE_REQ_TO_OTHER = 3

STATUS_KEY_PARENT = "status"
STATUS_KEY_PLAYBACKTIME = "p"

class GroupPeer(GroupMan.RpcPeer):
    def __init__(self):
        self.id = 0
        self.curPlaybackTime = 0

    def setStatus(self, playbackTime):
        self.curPlaybackTime = playbackTime

    def exposed_addMe(self):
        return {"id" : len(

def encodeObject(obj):
    getDict = getattr(obj, "getDict", None)
    if getDict is not None and callable(getDict):
        return obj.getDict()
    raise TypeError(obj)

class GroupManager:
    def __init__(self, options):
        self.options = options
        self.grpMan = None
        self.me = GroupPeer()

    def startGroup(self):
        self.grpMan = GroupMan.RpcManager(self.options.groupPort)
        if self.options.neighbourAddress is not None:
            addr, port = self.options.neighbourAddress.split(":")
            peer = self.grpMan.connectTo((addr, int(port)))

