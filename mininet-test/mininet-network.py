#!/usr/bin/python

from mininet.net import Mininet
from mininet.node import Controller, RemoteController, OVSController
from mininet.node import CPULimitedHost, Host, Node
from mininet.node import OVSKernelSwitch, UserSwitch
from mininet.node import IVSSwitch
from mininet.cli import CLI
from mininet.log import setLogLevel, info
from mininet.link import TCLink, Intf
from subprocess import call
from mininet.term import runX11, makeTerm

import argparse
import socket
import time
import os
import tempfile
import multiprocessing as mp
import numpy as np

def runX11WithHost(host, cmd, **kwargs):
    os.environ["MININET_HOST"] = str(host)
    for x in kwargs:
        os.environ[x] = kwargs[x]
    ret = runX11(host, cmd)
    del os.environ["MININET_HOST"]
    for x in kwargs:
        del os.environ[x]
    return ret

class CMultiProc(mp.Process):
    def wait(self):
        return self.join()
    def poll(self):
#         print("it is", "alive" if self.is_alive() else "dead")
        return not self.is_alive()

class TraceProcess:
    def __init__(self):
        self.mTraceMap = {} # interface: interface object trace, target and few more info
        self.mRunProc = [] # important
        self.mRunning = False
        self.mProcessesToBeTerminated = []
        self.mProcessesToBeWaited = []

        self.mRunningProc = []
        self.mProcessesTerminated = []

    def addTrace(self, intf, tracepath, targetIntf): #might add more param
        assert not self.mRunning
        traces = np.array([int(float(l.strip().split()[0])) for l in open(tracepath)]) * 8
        traces = traces.tolist()
        destIp = targetIntf.IP()
        name = str(intf)
        otherInfo = []
        self.mTraceMap.setdefault(name, []).append([intf, traces, destIp, otherInfo])

    def runX11At(self, at, host, cmd, waitToEnd=False, terminateAfter=0, **env):
        print("environment:", env)
        self.mRunProc += [[at, waitToEnd, terminateAfter, "x11", (host, cmd, env)]]

    def runMultiProxAt(self, at, waitToEnd=False, terminateAfter=0, **extra):
        self.mRunProc += [[at, waitToEnd, terminateAfter, "mp", extra]]


    def runIntfCmd(self, intf, cmd):
#         print(cmd)
        res = intf.cmd(cmd)
#         print(res)
        return res

    def startTrace(self):
        for intfName, traceInfos in self.mTraceMap.items():
            for index, tr in enumerate(traceInfos):
                intf, trace, destIp, otherInfo = tr

                filterId = index + 3
                markId = index+10
                otherInfo.append(filterId)
                otherInfo.append(markId)
                otherInfo.append(1)

                self.runIntfCmd(intf, f"tc class add dev {intfName} parent 5:0 classid 5:{filterId} htb rate {trace[0]} ceil {trace[0]} prio 0")
                self.runIntfCmd(intf, f"iptables -A OUTPUT -t mangle -p tcp -j MARK --set-mark {markId}")
                self.runIntfCmd(intf, f"tc filter add dev {intfName} parent 5:0 prio 0 protocol ip handle {markId} fw flowid 5:{filterId}")
                self.runIntfCmd(intf, f"iptables -A OUTPUT -t mangle -p tcp -s {destIp} -j MARK --set-mark {markId}")

    def runTrace(self):
        for intfName, traceInfos in self.mTraceMap.items():
            for index, tr in enumerate(traceInfos):
                intf, trace, destIp, otherInfo = tr

                filterId, markId, nextId = otherInfo
                otherInfo[2] = (otherInfo[2] + 1) % len(trace)

                self.runIntfCmd(intf, f"tc class change dev {intfName} parent 5:0 classid 5:{filterId} htb rate {trace[nextId]} ceil {trace[nextId]} prio 0")

    def runMpProc(self, args):
        proc = CMultiProc(**args)
        proc.start()
        return (None, proc)

    def runProc(self, nextProc):
#             at, host, cmd, waitToEnd, terminateAfter, env = nextProc
            at, waitToEnd, terminateAfter, procType, extra = nextProc
            proc = None
            if procType == "x11":
                host, cmd, env = extra
                print(f"Runneing `{cmd}`")
                proc = runX11WithHost(host, cmd, **env)
            elif procType == "mp":
                proc = self.runMpProc(extra)
            assert proc is not None
            if waitToEnd:
                self.mProcessesToBeWaited.append(proc)
            else:
                self.mProcessesToBeTerminated.append([terminateAfter, proc])
            self.mRunningProc.append(nextProc)

    def run(self):
        assert not self.mRunning
        self.startTrace()
        self.mRunProc.sort(key=lambda x: x[0])
        nextTraceAt = 5
        curTime = 0
        terminated = False
        self.mRunning = True

        while True:
            nextRunAt = nextTraceAt + 2
            nextTerminateAt = nextTraceAt + 2
            if len(self.mRunProc):
                nextRunAt = self.mRunProc[0][0]
            if terminated and len(self.mProcessesToBeTerminated):
                nextTerminateAt = self.mProcessesToBeTerminated[0][0]

            if len(self.mRunProc) == 0 and len(self.mProcessesToBeTerminated) == 0 and len(self.mProcessesToBeWaited) == 0:
                break

            for i, proc in enumerate(self.mProcessesToBeWaited[:]):
                res = proc[1].poll()
                if res:
                    proc = self.mProcessesToBeWaited.pop(i)
                    self.mProcessesTerminated.append(proc)
                    if len(self.mProcessesToBeWaited) == 0:
                        terminated = True
                        for i, x in enumerate(self.mProcessesToBeTerminated):
                            x[0] += curTime
                        self.mProcessesToBeTerminated.sort(key=lambda x: x[0])

            nextActionAt = min(nextTraceAt, nextRunAt, nextRunAt)
            sleepTime = nextActionAt - curTime
            assert sleepTime >= 0
            if sleepTime > 0:
                time.sleep(sleepTime)
                curTime += sleepTime

            if nextTraceAt <= curTime:
                self.runTrace()
                nextTraceAt += 5

            if nextRunAt <= curTime:
                assert self.mRunProc[0][0] == nextRunAt
                nextProc = self.mRunProc.pop(0)
                self.runProc(nextProc)

            if nextTerminateAt <= curTime:
                assert self.mProcessesToBeTerminated[0][0] == nextTerminateAt
                at, proc = self.mProcessesToBeTerminated.pop(0)
                proc[1].terminate()
                self.mProcessesTerminated.append(proc)


def myNetwork():

    WD = os.path.dirname(os.path.abspath(__file__))
    os.environ['MININET_WD'] = WD

    net = Mininet( topo=None,
                   build=False,
                   ipBase='10.0.0.0/8')

    info( '*** Adding controller\n' )
    info( '*** Add switches\n')
    s1 = net.addSwitch('s1', cls=OVSKernelSwitch, failMode='standalone')

    info( '*** Add hosts\n')
    h1 = net.addHost('h1', cls=Host, ip='10.0.0.1', defaultRoute=None)
    h2 = net.addHost('h2', cls=Host, ip='10.0.0.2', defaultRoute=None)
    HN = []
    for n in range(options.neighbor):
        hx = net.addHost(f"h{n+3}", cls=Host, ip=f"10.0.0.{n+3}", defaultRoute=None)
        HN += [hx]
#     h3 = net.addHost('h3', cls=Host, ip='10.0.0.3', defaultRoute=None)
#     h4 = net.addHost('h4', cls=Host, ip='10.0.0.4', defaultRoute=None)
#     h5 = net.addHost('h5', cls=Host, ip='10.0.0.5', defaultRoute=None)

    info( '*** Add links\n')
    h1s1 = {'bw': 6,'delay':'10ms'}
    s1pl = {'bw': 10, 'delay': '5ms'}
    net.addLink(h1, s1, cls=TCLink, **h1s1)
    net.addLink(s1, h2, cls=TCLink, **s1pl)
    for hx in HN:
        net.addLink(s1, hx, cls=TCLink, **s1pl)
#     net.addLink(s1, h3, cls=TCLink, **s1pl)
#     net.addLink(s1, h4, cls=TCLink, **s1pl)
#     net.addLink(s1, h5, cls=TCLink, **s1pl)

    info( '*** Starting network\n')
    net.build()
    info( '*** Starting controllers\n')
    for controller in net.controllers:
        controller.start()

    info( '*** Starting switches\n')
    net.get('s1').start([])

    info( '*** Post configure switches and hosts\n')

#     CLI(net)
#     net.stop()
#     return

    xterm = "xterm -fa 'Monospace' -fs 12 -bg #460001 -fg #ffffff "

#     tp = TraceProcess()
#     tp.runX11At(3, h2, xterm + "/tmp/ser.sh", terminateAfter=4)
#     tp.runX11At(5, h1, xterm + "/tmp/cli.sh", terminateAfter=3, IPERF_SERVER_IP = f"{h2.IP()}")
#     if len(options.traceFiles) > 0:
#         for i, hn in enumerate([h2] + HN[:]):
#             tp.addTrace(h1.intfs[0], options.traceFiles[i%len(options.traceFiles)], hn.intfs[0])
#     intfNames = [x for x in h2.intfList() if str(x).startswith("h2")]
#     tp.runX11At(6, h2, f"python3.7 /home/exp/dynamic_plot.py {intfNames[0]}", waitToEnd=True)
#     tp.run()
#     net.stop()
#     return

    ##===============
    tp = TraceProcess()
    if options.logDir is not None:
        cmd = xterm + "-T '" + h1.name + "'"
        cmd += f" {WD}/dumpcap.sh"
        tp.runX11At(1, h1, cmd, terminateAfter=5, MININET_IFC=f"{h1.intf()}", MININET_TRACE_FILE=os.path.join(options.logDir, "trace.pcap"))
    if len(options.traceFiles) > 0:
        for i, hn in enumerate([h2] + HN[:]):
            tp.addTrace(h1.intfs[0], options.traceFiles[i%len(options.traceFiles)], hn.intfs[0])

    #Adding server
    cmd = xterm + "-T '" + h1.name + "'"
    cmd += f" {WD}/server.sh "
    henv = setParamFromHost(h1)
    tp.runX11At(3, h1, cmd, terminateAfter=4, **henv)

    #Adding primary
    cmd = xterm + "-T '" + h2.name + "'"
    cmd += f" {WD}/client-0.sh "
    henv = setParamFromHost(h2)
    tp.runX11At(6, h2, cmd, terminateAfter=3, **henv)

    runAfter = 24
    for i in range(options.neighbor):
        print(f"Running client {2+i}")
        hx = HN[i]
        cmd = xterm + "-T '" + hx.name + "'"
        if options.standAlone:
            cmd += f" {WD}/client-0.sh "
        else:
            cmd += f" {WD}/client-1.sh "
        henv = setParamFromHost(hx)
        tp.runX11At(runAfter + (i*10), hx, cmd, terminateAfter=3, **henv)

    tp.runMultiProxAt(4, waitToEnd=True, target=waitForSocket)

    tp.run()
    net.stop()
    return

    # End


# #     time.sleep(120)
#     print("waiting for socket")
#     waitForSocket()
#     time.sleep(1)
#
#     cterm[1].terminate()
#     for oterm in nterms:
#         oterm[1].terminate()
#     sterm[1].terminate()
#     if dpop is not None:
#         dpop[1].terminate()
#
#     #CLI(net)
#     net.stop()

def setParamFromHost(node):
    envStr = ""
    if options.logDir is not None:
        envStr += " -L " + os.path.join(options.logDir, node.name)

    if options.finSock is not None:
        envStr += " -F " + options.finSock

    return {"WEBVIEWTEST_PARAM": envStr}

def waitForSocket():
    print("Waiting for socket at", options.finSock)
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM, 0)
    sock.bind(options.finSock)
    sock.listen(1)
    os.chmod(options.finSock, 0o777)
    con, addr = sock.accept()
    con.recv(2)
    con.close()
    sock.close()

    print("Success at", options.finSock)
    os.remove(options.finSock)

def parseCmdArgument():
    global options
    parser = argparse.ArgumentParser(description = "FLSD mininet options")

    tfile = tempfile.mktemp()

    parser.add_argument('-n', '--neighbor', dest='neighbor', type=int, default=3)
    parser.add_argument('-a', '--stand-alone', dest='standAlone', action='store_true')
    parser.add_argument('-m', '--mpd-path', dest='mpdPath', default=os.environ['HOME'] + 'dashed/bbb/media/vid.mpd', type=str)
    parser.add_argument('-L', '--logDir', dest='logDir', default=None, type=str)
    parser.add_argument('-F', '--finishedSocket', dest='finSock', default=tfile, type=str)
    parser.add_argument('-t', '--trace-file', dest='traceFiles', type=str, nargs='*', default=[])

    options = parser.parse_args()

    if options.logDir is not None and not os.path.isdir(options.logDir):
        os.makedirs(options.logDir)
        os.chmod(options.logDir, 0o777)


    os.putenv("MPD_SERVER_VIDEO_PATH", options.mpdPath)


if __name__ == '__main__':
    parseCmdArgument()
    setLogLevel( 'info' )
    myNetwork()

