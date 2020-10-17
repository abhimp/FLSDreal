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

VISCOUS_INFO_PATH="/tmp/viscous/"

def parsOption():
    parser = argparse.ArgumentParser(description = "Viscous test with post")
    parser.add_argument('networkmap', type=str)
    parser.add_argument('-s', '--server', dest="server_script", default="bash", type=str)
    parser.add_argument('-c', '--client', dest="client_script", default="bash", type=str)
    parser.add_argument('-d', '--delay', dest="delay", default="1", type=str)
    parser.add_argument('-b', '--bw', dest="bw", default=10, type=int)
    parser.add_argument('-r', '--reverse', dest="rev", default=False, action='store_true')
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
    if not ipBase:
        vhosts = [x for x in net.hosts if x.name.startswith("hv")]
    else:
        vhosts = [net.get(x) for x in ipBase if x.startswith("hv")]
        hs = [x for x in ipBase if x.startswith("hv")]
        for s in hs:
            print(s, "->", end=' ')
            for t in hs:
                if s == t:
                    continue
                for ip in ipBase[t]:
                    cmd = "ping -c1 %s"%ip[0]
                    #print s, cmd,
                    ret = net.get(s).pexec(cmd)[-1]
                    if ret == 0:
                        print(t, end=' ')
                    else:
                        print("X")
            print("")

    serverTerms = []
    clientTerms = []
    hnames = set()
    ser = options.server_script
    cli = options.client_script
    for host in vhosts:
        hnames.add(host.name)

    for cliH in hnames:
        if not cliH.startswith("hvc"):
            continue
        serH = "hvs"+cliH[3:]
        if serH not in hnames:
            continue
        server = net.get(serH)
        client = net.get(cliH)

        if options.rev:
            client = net.get(serH)
            server = net.get(cliH)

        cmd = xterm + "-T '" + server.name + "' " + ser + " "

        os.environ["VISCOUS_CLIENT"] = client.IP()
        os.environ["VISCOUS_COUNTER_PART"] = str(client)
        serverTerms += [runX11WithHost(server, cmd)]

        del os.environ["VISCOUS_COUNTER_PART"]
        del os.environ["VISCOUS_CLIENT"]
        print(server.name, "->", server.IP())
        #break

    time.sleep(3)

    for cliH in hnames:
        if not cliH.startswith("hvc"):
            continue
        serH = "hvs"+cliH[3:]
        if serH not in hnames:
            continue
        server = net.get(serH)
        client = net.get(cliH)

        if options.rev:
            client = net.get(serH)
            server = net.get(cliH)

        cmd = xterm + "-T '" + client.name + "' " + cli + " "

        os.environ["VISCOUS_SERVER"] = server.IP()
        os.environ["VISCOUS_COUNTER_PART"] = str(server)
        clientTerms += [runX11WithHost(client, cmd)]

        del os.environ["VISCOUS_COUNTER_PART"]
        del os.environ["VISCOUS_SERVER"]
        print(client.name, "->", client.IP())
        #break


    for term in clientTerms:
        #term[1].terminate()
        term[1].wait()
    for term in serverTerms:
        term[1].terminate()
        #term[1].wait()


def prepareNetwork(options):
    edges = [x.strip().split() for x in open(options.networkmap) if x.strip() != ""]
    perRouterRT, G, edges = getRoutingTable(edges)

    routersNames = set(x[1] for x in edges if x[1].startswith('r'))
    hostsNames = set(x[1] for x in edges if x[1].startswith('h'))
    switchesNames = set(x[0] for x in edges)

    net = Mininet( topo=None,
                   build=False,
                   ipBase='10.0.0.0/8')

    routers = {}
    hosts = {}
    switches = {}
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


    linkPropCli = {'bw':options.bw,'delay':options.delay+'ms', "max_queue_size":10000}
    linkPropSer = {'bw':options.bw * 5,'delay':str(2)+'ms', "max_queue_size":10000}
    linkPropCor = {'bw':options.bw * 10,'delay':str(3)+'ms', "max_queue_size":10000}
    info( '*** Add links\n')
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
        else:
            raise Exception("error")

    info( '*** Starting network\n')
    net.build()

    info( '*** Starting switches\n')
    for x in switchesNames:
        net.get(x).start([])

    ipBase = setupIpAndRouting(net, G, perRouterRT, hosts, routers, switches)

    #net.ping([x for x in set(hosts.values())])

    startExperiment(net, options, ipBase)

    #CLI(net)

    net.stop()

def getRouterInterface(link):
    intf = link.intf1
    sName = link.intf2.node.name
    rName = link.intf1.node.name
    if not link.intf1.node.name.startswith("r"):
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

def setupIpAndRouting(net, G, perRouterRT, hosts, routers, switches):
    links = net.links
    for link in links:
        intf, sName, rName = getRouterInterface(link)
        ipdata = getIPdata(G, rName, sName) #G.edge[rName][sName]["attr"]
        cmd = "ifconfig %s %s netmask %s"%(intf.name, ipdata[0], ipdata[2])
        #print cmd
        intf.node.cmd(cmd)
        intf.updateAddr()

    #import pdb; pdb.set_trace()
    ruleTable = {}
    routeStr = {}
    hostIps = {}
    for node in list(routers.keys())+list(hosts.keys()):
        rnode = None
        if node.startswith("r"):
            rnode = routers[node]
            r = node
        else:
            rnode = hosts[node]
            r, idx = node.split("x")

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


def generateLocalNetworks(simLinks):
    localNetworks = {}
    maxNetSize = 0
    for x in simLinks:
        node1 = x[0]
        node2 = x[1]
        if not node1.startswith("s") and not node2.startswith("s"):
            raise Exception("Error")
        if node1.startswith("s"):
            x = localNetworks.setdefault(node1, [])
            x += [node2]
        elif node2.startswith("s"):
            x = localNetworks.setdefault(node2, [])
            x += [node1]

    newLinks = []

    maxNetSize = max([len(localNetworks[x]) for x in localNetworks])
    suffixLen = int(math.ceil(math.log(maxNetSize+2, 2)))
    ipNet = "10.1.0.0"
    if "IP_ADDRESS" in os.environ:
        ipNet = os.environ["IP_ADDRESS"]

    net = netaddr.IPNetwork("%s/%s"%(ipNet, 32 - 2 - suffixLen))
    for switch in localNetworks:
        ips = [y.ip for y in net.subnet(32) if y.ip!=net.network and y.ip!=net.broadcast]
        ips.reverse()
        for hop in localNetworks[switch]:
            x = [switch, hop, str(ips.pop()), net.prefixlen, str(net.netmask)]
            newLinks += [x]

        net = net.next() #next(net)

    #for x in newLinks:
    #    print " ".join(str(y) for y in x)
    #exit(1)
    return newLinks


def getMaximumConnectedSubNetwork(simLinks):
    if len(simLinks[0]) != 5:
        simLinks = generateLocalNetworks(simLinks)
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

        if o.startswith('r'):
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
        edges.setdefault(edg[1], []).append(edg[0])
    for sname in edges:
        if not sname.startswith("s"):
            continue
        for node in edges[sname]:
            edg = [sname, node] + G.get_edge_data(sname, node)['attr'] # + edges[sname][node]["attr"]
            newEdges += [edg]

    #import pdb; pdb.set_trace()
    return G, newEdges

def getRoutingTable(simLinks):
    G, simLinks = getMaximumConnectedSubNetwork(simLinks)
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
            if r in perRouterRT and s in perRouterRT[r]:
                continue
            path = allPath[r][s]
            idx = 0
            while idx < len(path)-2:
                rn = path[idx]
                sn = path[idx + 1]
                nrn = path[idx + 2]
                if rn not in perRouterRT:
                    perRouterRT[r] = {}
                if s in perRouterRT[r]:
                    break

                perRouterRT[rn][s] = [sn, nrn]

    x = {r:[[nets[s]]+perRouterRT[r][s] for s in perRouterRT[r]] for r in perRouterRT}
    perRouterRT = {y : compressRoutingTable(x[y]) for y in x}
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
        if x.next() == y and y.supernet(y.prefixlen-1)[0] == x.supernet(x.prefixlen-1)[0]:
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
