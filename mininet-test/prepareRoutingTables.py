import networkx as nx
import sys
import netaddr
import math
import json
import re
import os
import time
import argparse
import shutil

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

class NAT( Node ):
    "NAT: Provides connectivity to external network"

    def __init__( self, name, subnet='10.0/8',
                  localIntf=None, flush=False, **params):
        """Start NAT/forwarding between Mininet and external network
           subnet: Mininet subnet (default 10.0/8)
           flush: flush iptables before installing NAT rules"""
        super( NAT, self ).__init__( name, **params )

        self.subnet = subnet
        self.localIntf = localIntf
        self.flush = flush
        self.forwardState = self.cmd( 'sysctl -n net.ipv4.ip_forward' ).strip()

    def config( self, **params ):
        """Configure the NAT and iptables"""
        super( NAT, self).config( **params )

        if not self.localIntf:
            self.localIntf = self.defaultIntf()

        if self.flush:
            self.cmd( 'sysctl net.ipv4.ip_forward=0' )
            self.cmd( 'iptables -F' )
            self.cmd( 'iptables -t nat -F' )
            # Create default entries for unmatched traffic
            self.cmd( 'iptables -P INPUT ACCEPT' )
            self.cmd( 'iptables -P OUTPUT ACCEPT' )
            self.cmd( 'iptables -P FORWARD DROP' )

        # Install NAT rules
        self.cmd( 'iptables -I FORWARD',
                  '-i', self.localIntf, '-d', self.subnet, '-j DROP' )
        self.cmd( 'iptables -A FORWARD',
                  '-i', self.localIntf, '-s', self.subnet, '-j ACCEPT' )
        self.cmd( 'iptables -A FORWARD',
                  '-o', self.localIntf, '-d', self.subnet, '-j ACCEPT' )
        self.cmd( 'iptables -t nat -A POSTROUTING',
                  '-s', self.subnet, "'!'", '-d', self.subnet,
                  '-j MASQUERADE' )

        # Instruct the kernel to perform forwarding
        self.cmd( 'sysctl net.ipv4.ip_forward=1' )

        # Prevent network-manager from messing with our interface
        # by specifying manual configuration in /etc/network/interfaces
#         intf = self.localIntf
#         cfile = '/etc/network/interfaces'
#         line = '\niface %s inet manual\n' % intf
#         config = open( cfile ).read()
#         if ( line ) not in config:
#             info( '*** Adding "' + line.strip() + '" to ' + cfile + '\n' )
#             with open( cfile, 'a' ) as f:
#                 f.write( line )
#         # Probably need to restart network-manager to be safe -
#         # hopefully this won't disconnect you
#         self.cmd( 'service network-manager restart' )

    def terminate( self ):
        "Stop NAT/forwarding between Mininet and external network"
        # Remote NAT rules
        self.cmd( 'iptables -D FORWARD',
                   '-i', self.localIntf, '-d', self.subnet, '-j DROP' )
        self.cmd( 'iptables -D FORWARD',
                  '-i', self.localIntf, '-s', self.subnet, '-j ACCEPT' )
        self.cmd( 'iptables -D FORWARD',
                  '-o', self.localIntf, '-d', self.subnet, '-j ACCEPT' )
        self.cmd( 'iptables -t nat -D POSTROUTING',
                  '-s', self.subnet, '\'!\'', '-d', self.subnet,
                  '-j MASQUERADE' )
        # Put the forwarding state back to what it was
        self.cmd( 'sysctl net.ipv4.ip_forward=%s' % self.forwardState )
        super( NAT, self ).terminate()

VISCOUS_INFO_PATH="/tmp/viscous/"

def parsOption():
    parser = argparse.ArgumentParser(description = "Viscous test with post")
    parser.add_argument('networkmap', type=str)
    parser.add_argument('-s', '--server', dest="server_script", default="bash", type=str)
    parser.add_argument('-c', '--client', dest="client_script", default="bash", type=str)
    parser.add_argument('-d', '--delay', dest="delay", default="1", type=str)
    parser.add_argument('-b', '--bw', dest="bw", default=10, type=int)
    parser.add_argument('-r', '--reverse', dest="rev", default=False, action='store_true')
    parser.add_argument('-p', '--prompt', dest="prompt", default=False, action='store_true')
    parser.add_argument('-I', '--base-ip', dest="base_ip", default="10.20.0.0/16", type=str)
    options = parser.parse_args()
    return options


def createNetworkConfig(path, iface, ip, gw):
    if not os.path.exists(path):
        os.makedirs(path)
    fpath = os.path.join(path, ip)
    fp = open(fpath, "w")
    print("iface="+iface, file=fp)
    print("ip="+ip, file=fp)
    print("gw="+gw, file=fp)
    fp.close()

def removeInfoPath():
    if os.path.exists(VISCOUS_INFO_PATH):
        shutil.rmtree(VISCOUS_INFO_PATH)

def getPaths(hostname, nonce="31"):
    visPath = VISCOUS_INFO_PATH
    procPath = os.path.join(visPath, nonce, "proc", str(hostname))
    netPath = os.path.join(visPath, nonce, "net", str(hostname))

    if not os.path.exists(procPath):
        os.makedirs(procPath)

    if not os.path.exists(netPath):
        os.makedirs(netPath)

    return netPath, procPath

def setEnvForHosts(hostname, nonce="31"):
    netPath, procPath = getPaths(str(hostname), nonce)
    os.environ["VISCOUS_NET_INFO_DIR"] = netPath
    os.environ["VISCOUS_PROC_INFO_DIR"] = procPath


def runX11WithHost(host, cmd):
    setEnvForHosts(host)
    os.environ["MININET_HOST"] = str(host)
    x = runX11(host, cmd)
    del os.environ["MININET_HOST"]
    return x


def savedToJson(fpath, obj):
    fppath = open(fpath, "w")
    json.dump(obj, fppath)
    fppath.close()


def startExperiment(net, options, ipBase = None):
    xterm = "xterm -fa 'Monospace' -fs 12 -bg #460001 "
    vhosts = None
    serverTerms = []
    clientTerms = []
    ser = options.server_script
    cli = options.client_script

    vhosts = [x for x in net.hosts if x.name.startswith("hc")]

    for hc in vhosts:
        if hc.name.startswith("r"):
            continue

        cmd = xterm + "-T '" + hc.name + "' " + cli + " "

        clientTerms += [runX11WithHost(hc, cmd)]

    for term in clientTerms:
        #term[1].terminate()
        term[1].wait()
#     for term in serverTerms:
#         term[1].terminate()
#         #term[1].wait()

def addNat(net, name='nat0', connect=True, inNamespace=False,
                **params):
    nat = net.addHost( name, cls=NAT, inNamespace=inNamespace,
                        subnet=net.ipBase, **params )
    # find first switch and create link
    if connect:
        if not isinstance( connect, Node ):
            # Use first switch if not specified
            connect = net.switches[ 0 ]
        # Connect the nat to the switch
        net.addLink( nat, connect )
        # Set the default route on hosts
        natIP = nat.params[ 'ip' ].split('/')[ 0 ]
        for host in net.hosts:
            if host.inNamespace and len(net.linksBetween(host, connect)) > 0:
                host.setDefaultRoute( 'via %s' % natIP )
    return nat

def prepareNetwork(options):
    edges = [x.strip().split() for x in open(options.networkmap) if x.strip() != ""]
    perRouterRT, G, edges = getRoutingTable(edges, baseIp = options.base_ip)

    routersNames = set(x[1] for x in edges if x[1].startswith('r'))
    hostsNames = set(x[1] for x in edges if x[1].startswith('h'))
    natNames = set(x[1] for x in edges if x[1].startswith('n'))
    switchesNames = set(x[0] for x in edges)

    net = Mininet( topo=None,
                   build=False,
                   ipBase=options.base_ip)

    routers = {}
    hosts = {}
    switches = {}
    nats = {}
    info( '*** Adding switches\n' )
    for x in switchesNames:
        switches[x] = net.addSwitch(x, cls=OVSKernelSwitch, failMode='standalone')

    info( '*** Adding routers\n' )
    for x in routersNames:
        tmp = net.addHost(x, cls=Node)
        routers[x] = tmp
        tmp.cmd('sysctl -w net.ipv4.ip_forward=1')

    info( '*** Adding hosts\n' )
    for x in hostsNames:
        h,i = x.split("x")
        if h in net:
            tmp = net.get(h)
        else:
            tmp = net.addHost(h, cls=Node)
        hosts[x] = tmp

    info( '*** Adding Nat')
    for n in natNames:
        tmp = net.addHost(n , cls=NAT, inNamespace=False, subnet=net.ipBase )
        nats[n] = tmp


    linkPropCli = {'bw':options.bw,'delay':options.delay+'ms', "max_queue_size":10000}
    linkPropSer = {'bw':options.bw * 5,'delay':str(2)+'ms', "max_queue_size":10000}
    linkPropCor = {'bw':options.bw * 10,'delay':str(3)+'ms', "max_queue_size":10000}
    info( '*** Adding links\n')
    for edge in edges:
        r = edge[1]
        s = edge[0]
        if r in routers:
            linkProp = linkPropCor
            net.addLink(switches[s], routers[r], cls=TCLink, **linkProp)
        elif r in hosts:
            if r.startswith("hvs"):
                linkProp = linkPropSer
            else:
                linkProp = linkPropCli
            net.addLink(switches[s], hosts[r], cls=TCLink, **linkProp)
        elif r in nats:
            net.addLink(r, s )
#             nats[r].config()
        else:
            raise Exception("error")

#     for s in switchesNames:
#         if not s.startswith('sp'):
#             continue
#         addNat(net, connect=switches[s])


    info( '*** Starting network\n')
    net.build()

    info( '*** Starting switches\n')
    for x in switchesNames:
        net.get(x).start([])

    ipBase = setupIpAndRouting(net, G, perRouterRT, hosts, routers, switches, nats)

    #net.ping([x for x in set(hosts.values())])

    if options.prompt:
        CLI(net)
    else:
        startExperiment(net, options, ipBase)


    net.stop()

def getRouterInterface(link):
    intf = link.intf1
    sName = link.intf2.node.name
    rName = link.intf1.node.name
    if not sName.startswith("s"):
        intf = link.intf2
        sName = link.intf1.node.name
        rName = link.intf2.node.name

    return intf, sName, rName

def getIPdata(G, rName, sName):
    Gedge = dict(G.adjacency())
    if rName.startswith('r'):
        return Gedge[rName][sName]["attr"]
    else:
        for o in Gedge[sName]:
            if o.startswith(rName):
                return Gedge[o][sName]["attr"]
    raise Exception("error")

def setupIpAndRouting(net, G, perRouterRT, hosts, routers, switches, nats):
    links = net.links
    for link in links:
        intf, sName, rName = getRouterInterface(link)
        ipdata = getIPdata(G, rName, sName) #G.edge[rName][sName]["attr"]
        cmd = "ifconfig %s %s netmask %s"%(intf.name, ipdata[0], ipdata[2])
        #print cmd
        assert sName.startswith('s')
        if rName.startswith('n'):
            for h in net.hosts:
                if h.inNamespace and len(net.linksBetween(h, switches[sName])) > 0:
                    h.setDefaultRoute( 'via %s' % ipdata[0] )
        intf.node.cmd(cmd)
        intf.updateAddr()

    #import pdb; pdb.set_trace()
    ruleTable = {}
    routeStr = {}
    hostIps = {}
    for node in list(routers.keys())+list(hosts.keys()) +list(nats.keys()):
        rnode = None
        if node.startswith("r"):
            rnode = routers[node]
            r = node
        elif node.startswith('h'):
            rnode = hosts[node]
            r, idx = node.split("x")
        elif node.startswith('n'):
            continue

        if node not in perRouterRT or len(perRouterRT[node])==0:
            raise Exception("routing table doesnot esists")

        switch2itf = {}
        for i in rnode.intfs:
            inf = rnode.intfs[i]
            link = inf.link
            if link.intf1 == inf:
                switch2itf[link.intf2.node.name] = inf.name
            else:
                switch2itf[link.intf1.node.name] = inf.name

        Gedge = dict(G.adjacency())
        for rt in perRouterRT[node]:
            targetNet = rt[0]
            nextSwitch = rt[1]
            nextHop = rt[2]
            attr = Gedge[nextHop][nextSwitch]['attr']
            nextHopIp = attr[0]
            intfname = switch2itf[nextSwitch]
            cmd = "route add -net %s/%s gw %s dev %s"%(targetNet.network, targetNet.prefixlen, nextHopIp, intfname)
            rnode.cmd(cmd)
            rtr = routeStr.setdefault(r, [])
            rtr += [cmd]

            if not node.startswith("h"):
                continue
                x = re.search("(\d+)$", intfname)
                if not x:
                    raise Exception("Error")
                idx = int(x.groups()[0])+1

            ip = Gedge[node][nextSwitch]['attr']
            netPath,procPath = getPaths(r)
            createNetworkConfig(netPath, intfname, ip[0], nextHopIp)
            tmpIpBase = hostIps.setdefault(r, [])

            tmpIpBase+= [(ip[0], nextHopIp)]

            rules = ruleTable.setdefault(r, set())
            if idx in rules:
                continue
            rules.add(idx)

            cmd = "ip rule add from %s table %s"%(ip[0], idx)
            rnode.cmd(cmd)
            tmpnet = netaddr.IPNetwork("%s/%s"%(ip[0], ip[1]))
            cmd = "ip route add %s/%s dev %s scope link table %s"%(tmpnet.network, tmpnet.prefixlen, intfname, idx)
            rnode.cmd(cmd)
            cmd = "ip route add default via %s dev %s table %s"%(nextHopIp, intfname, idx)
            rnode.cmd(cmd)

    savedToJson("/tmp/routstr", routeStr)
    return hostIps


def generateLocalNetworks(simLinks, baseIp="10.1.0.0/16", **kw):
    localNetworks = {}
    maxNetSize = 0
    for x in simLinks:
        node1 = x[0]
        node2 = x[1]
        if not node1.startswith("s") and not node2.startswith("s"):
            raise Exception("Error")
        switch = node1
        other = node2
        if node2.startswith("s"):
            switch = node2
            other = node1
        localNetworks.setdefault(switch, []).append(other)

    newLinks = []

    maxNetSize = max([len(localNetworks[x]) for x in localNetworks])
    suffixLen = int(math.ceil(math.log(maxNetSize+2, 2)))
    ipNet, _ = baseIp.split('/')
    if "IP_ADDRESS" in os.environ and (ipNet is None or ipNet==""):
        ipNet = os.environ["IP_ADDRESS"]

    net = netaddr.IPNetwork("%s/%s"%(ipNet, 32 - 2 - suffixLen))
    for switch, hops in localNetworks.items():
        assert not switch.startswith("sp") or len(hops) == 2
        ips = [y.ip for y in net.subnet(32) if y.ip!=net.network and y.ip!=net.broadcast]
        ips.reverse()
        for hop in hops:
            x = [switch, hop, str(ips.pop()), net.prefixlen, str(net.netmask)] #add def marker
#             assert not hop.startswith("rn") or switch.startswith("sp")
            newLinks += [x]

        net = net.next() #next(net)

#     for x in newLinks:
#         print " ".join(str(y) for y in x)
#     exit(1)
    return newLinks


def getMaximumConnectedSubNetwork(simLinks, **kw):
    if len(simLinks[0]) != 5:
        simLinks = generateLocalNetworks(simLinks, **kw)
    G = nx.Graph()
    for edge in simLinks:
        s = edge[0]
        o = edge[1]
        if s.startswith('s'):
            if o.startswith('s'):
                raise Exception("switch to swtich")
        elif o.startswith('s'):
            s, o = o, s
        else:
            raise Exception("one of the link have to be switch")

        if o.startswith('r') or o.startswith('n'):
            G.add_edge(s, o, attr=edge[2:])
        elif o.startswith('h'):
            i = 1
            no = "%sx%s"%(o, i)
            while no in G.nodes:
                i += 1
                no = "%sx%s"%(o, i)
            G.add_edge(s, no, attr=edge[2:])
        #G.add_node(edge[0], attr=edge[2:])
        #G.add_node(edge[1], attr=edge[2:])

    #import pdb; pdb.set_trace()
    numNode = 0
    sg = None
#     for x in nx.connected_component_subgraphs(G):
    for x in nx.connected_components(G):
        x = G.subgraph(x)
        if len(x.nodes) > numNode:
            numNode = len(x.nodes)
            sg = x
    #pdb.set_trace()
    newEdges = []
    G = sg
    edges = {}
    for edg in G.edges:
        edges.setdefault(edg[0], []).append(edg[1])
        edges.setdefault(edg[1], []).append(edg[0]) # redundunt so that it can be removed latter
    for sname in edges:
        if not sname.startswith("s"):
            continue
        for node in edges[sname]:
            edg = [sname, node] + G.get_edge_data(sname, node)['attr'] # + edges[sname][node]["attr"]
            newEdges += [edg]

    #import pdb; pdb.set_trace()
    return G, newEdges

def getRoutingTable(simLinks, **kw):
    G, simLinks = getMaximumConnectedSubNetwork(simLinks, **kw)
    T = nx.minimum_spanning_tree(G)
    allPath = nx.all_pairs_shortest_path(T)
    #allPath = nx.all_pairs_dijkstra_path(G)
    #allPath = nx.floyd_warshall(T)
    allPath = dict(allPath)

    savedToJson("/tmp/allpath", allPath)

    nets = {x[0]:getNet(x[2:]) for x in simLinks}
    routers = set([x[1] for x in simLinks if x[1].startswith('r') ])
    hosts = set([x[1] for x in simLinks if x[1].startswith('h') ])

    perRouterRT = {}

    for s in nets:
        for r in routers:
#             if r in perRouterRT and s in perRouterRT[r]:
#                 continue
            path = allPath[r][s]
            idx = 0
            while idx < len(path)-2:
                rn = path[idx]
                sn = path[idx + 1]
                nrn = path[idx + 2]
#                 if rn not in perRouterRT:
#                     perRouterRT[r] = []
#                 if s in perRouterRT[r]:
#                     break

                perRouterRT.setdefault(rn, []).append([nets[s], sn, nrn])
                if s.startswith("sp"):
                    perRouterRT.setdefault(rn, []).append([netaddr.IPNetwork("0.0.0.0/0"), sn, nrn])
#                     perRouterRT[rn][-1] += ["default"]
                break
            if path[1].startswith("sp") and r.startswith("rn"): #the nat
                perRouterRT.setdefault(nrn, []).append([netaddr.IPNetwork("0.0.0.0/0"), sn, rn])



#     x = {r:[[nets[s]]+perRouterRT[r][s] for s in perRouterRT[r]] for r in perRouterRT}
#     perRouterRT = {y : compressRoutingTable(x[y]) for y in x}
    perRouterRT = {y : compressRoutingTable(perRouterRT[y]) for y in perRouterRT}
    Gedge = dict(G.adjacency())
    for h in hosts:
        if len(Gedge[h]) > 1:
            raise Exception("some error")
        s = list(Gedge[h].keys())[0]
        r = None
        for pop in Gedge[s]:
            if pop.startswith('r'):
                r = pop
                break
        if not r:
            raise Exception("error in data")
        ip = Gedge[s][h]["attr"]
        netw = netaddr.IPNetwork("0.0.0.0/0")
        perRouterRT[h] = [[netw, s, r]]



    savedToJson("/tmp/routes", {x:[str(y) for y in perRouterRT[x]] for x in perRouterRT})
    return perRouterRT, G, simLinks



def getNet(x):
    net = netaddr.IPNetwork("%s/%s"%(x[0],x[1]))
    return net


def compressRoutingTablePerHop(routingTable, srtd = False):
    if not srtd:
        routingTable.sort()
    #for x in routingTable:
    #    print x
    #exit()
    newRT = []
    i = 0
    lrt = len(routingTable)
    while i < lrt:
        j = i + 1
        x = routingTable[i]
        if j >= lrt:
            newRT += [x]
            break
        y = routingTable[j]
        if x == netaddr.IPNetwork("0.0.0.0/0"):
            newRT += [x]
        elif x.next() == y and y.supernet(y.prefixlen-1)[0] == x.supernet(x.prefixlen-1)[0]:
            newRT += [x.supernet(x.prefixlen-1)[0]]
            i = j
        else:
            newRT += [x]
        i += 1

    #print len(routingTable), len(newRT)
    if len(routingTable) != len(newRT):
        newRT = compressRoutingTablePerHop(newRT, True)

    return newRT


def compressRoutingTable(routingTable):
    nextHopRouting = {}
    #srcHost = routingTable[0][0]
    for rt in routingTable:
        x = nextHopRouting.setdefault((rt[1], rt[2]), [])
        x += [rt[0]]
#         if len(rt) >= 4 and rt[3] == "default":
#             x += [netaddr.IPNetwork("0.0.0.0/0")]
    #print len(nextHopRouting)
    newRT = []
    for x in nextHopRouting:
        #print x
        pop = compressRoutingTablePerHop(nextHopRouting[x])
        for rt in pop:
            newRT += [[rt, x[0], x[1]]]
    #exit()
    return newRT


if __name__ == "__main__":
    options = parsOption()
    removeInfoPath()
    prepareNetwork(options)
