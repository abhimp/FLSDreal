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

def runX11WithHost(host, cmd, **kwargs):
    os.environ["MININET_HOST"] = str(host)
    for x in kwargs:
        os.environ[x] = kwargs[x]
    ret = runX11(host, cmd)
    del os.environ["MININET_HOST"]
    for x in kwargs:
        del os.environ[x]
    return ret

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
    h1s1 = {'bw':5,'delay':'50ms'}
    s1pl = {'bw': 10, 'delay': '10ms'}
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

    xterm = "xterm -fa 'Monospace' -fs 12 -bg #460001 -fg #ffffff "

    dpop = None
    if options.logDir is not None:
        print("Running pcap")
        cmd = xterm + "-T '" + h1.name + "'"
#         cmd += f" dumpcap -i {h1.intf()} -w /tmp/fuck"
        cmd += f" {WD}/dumpcap.sh"# {h1.intf()} " + os.path.join(options.logDir, "trace.pcap")
        print(cmd)
        dpop = runX11WithHost(h1, cmd, MININET_IFC=f"{h1.intf()}", MININET_TRACE_FILE=os.path.join(options.logDir, "trace.pcap"))
        time.sleep(2)

    print("Running server")
    cmd = xterm + "-T '" + h1.name + "'"
    cmd += f" {WD}/server.sh "
    sterm = runX11WithHost(h1, cmd)

    time.sleep(3)

    print("Running client 1")
    cmd = xterm + "-T '" + h2.name + "'"
    cmd += f" {WD}/client-0.sh "
    setParamFromHost(h2)
    cterm = runX11WithHost(h2, cmd)

    time.sleep(24)

    nterms = []
    for i in range(options.neighbor):
        print(f"Running client {2+i}")
        hx = HN[i]
        cmd = xterm + "-T '" + hx.name + "'"
        if options.standAlone:
            cmd += f" {WD}/client-0.sh "
        else:
            cmd += f" {WD}/client-1.sh "
        setParamFromHost(hx)
        oterm = runX11WithHost(hx, cmd)
        nterms += [oterm]

        time.sleep(10)


#     time.sleep(120)
    print("waiting for socket")
    waitForSocket()
    time.sleep(1)

    cterm[1].terminate()
    for oterm in nterms:
        oterm[1].terminate()
    sterm[1].terminate()
    if dpop is not None:
        dpop[1].terminate()

    #CLI(net)
    net.stop()

def setParamFromHost(node):
    envStr = ""
    if options.logDir is not None:
        envStr += " -L " + os.path.join(options.logDir, node.name)
    if options.finSock is not None:
        envStr += " -F " + options.finSock

    os.putenv("WEBVIEWTEST_PARAM", envStr)

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

    options = parser.parse_args()

    if not os.path.isdir(options.logDir):
        os.makedirs(options.logDir)
        os.chmod(options.logDir, 0o777)


    os.putenv("MPD_SERVER_VIDEO_PATH", options.mpdPath)


if __name__ == '__main__':
    parseCmdArgument()
    setLogLevel( 'info' )
    myNetwork()

