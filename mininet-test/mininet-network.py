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

import time
import os

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
    h3 = net.addHost('h3', cls=Host, ip='10.0.0.3', defaultRoute=None)
    h4 = net.addHost('h4', cls=Host, ip='10.0.0.4', defaultRoute=None)
    h5 = net.addHost('h5', cls=Host, ip='10.0.0.5', defaultRoute=None)

    info( '*** Add links\n')
    h1s1 = {'bw':5,'delay':'50ms'}
    s1pl = {'bw': 10, 'delay': '10ms'}
    net.addLink(h1, s1, cls=TCLink, **h1s1)
    net.addLink(s1, h2, cls=TCLink, **s1pl)
    net.addLink(s1, h3, cls=TCLink, **s1pl)
    net.addLink(s1, h4, cls=TCLink, **s1pl)
    net.addLink(s1, h5, cls=TCLink, **s1pl)

    info( '*** Starting network\n')
    net.build()
    info( '*** Starting controllers\n')
    for controller in net.controllers:
        controller.start()

    info( '*** Starting switches\n')
    net.get('s1').start([])

    info( '*** Post configure switches and hosts\n')

    xterm = "xterm -fa 'Monospace' -fs 12 -bg #460001 "

    cmd = xterm + "-T '" + h1.name + f"' {WD}/server.sh "
    sterm = runX11WithHost(h1, cmd)

    time.sleep(3)

    cmd = xterm + "-T '" + h2.name + f"' {WD}/client-0.sh "
    cterm = runX11WithHost(h2, cmd)

    time.sleep(24)
    cmd = xterm + "-T '" + h3.name + f"' {WD}/client-1.sh "
    cterm2 = runX11WithHost(h3, cmd)

    time.sleep(5)
    cmd = xterm + "-T '" + h4.name + f"' {WD}/client-1.sh "
    cterm3 = runX11WithHost(h4, cmd)

    time.sleep(120)

    cterm[1].terminate()
    cterm2[1].terminate()
    cterm3[1].terminate()
    sterm[1].terminate()

    #CLI(net)
    net.stop()

if __name__ == '__main__':
    setLogLevel( 'info' )
    myNetwork()

