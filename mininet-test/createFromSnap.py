import networkx as nx
dt = [x.strip().split("\t") for x in open("as19990829.txt") if x.strip() != "" and x[0] != '#']
G = nx.Graph()
for x, y in dt:
    G.add_edge(x, y)

accumulator = []
lst = ['1']
adj = dict(G.adjacency())
while len(lst) > 0:
    if len(accumulator) > 16:
        break
    x = lst[0]
    lst = lst[1:]
    accumulator.append(x)
    l = 0
    for y in list(adj[x].keys()):
        if y in accumulator:
            continue
        lst.append(y)
        l += 1
        if l >= 3:
            break

S = G.subgraph(accumulator)

T = nx.minimum_spanning_tree(S)

adj = dict(T.adjacency())

deg = list(T.degree)
deg.sort(key=lambda x: x[1], reverse=True)

newedges = []

print(deg)
sc = 0
for x, z in deg:
    switches = {}
    for i, y in enumerate(list(T.neighbors(x))):
        T.remove_edge(x, y)
        switches.setdefault(f"s{i%3}", []).append(y)
    for s, r in switches.items():
        sc += 1
        newedges.append((f"r{x}", f"s{sc}"))
        for i in r:
            newedges.append((f"r{i}", f"s{sc}"))

hcnt = 0
for x, z in deg:
    if z > 1:
        continue
    sc += 1
    newedges.append((f"r{x}", f"s{sc}"))
    hcnt += 1
    newedges.append((f"h{hcnt}", f"s{sc}"))
    hcnt += 1
    newedges.append((f"h{hcnt}", f"s{sc}"))


for x, y in newedges:
    print(x,y)
