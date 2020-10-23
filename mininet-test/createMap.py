import random
import networkx

random.seed(1)
edges = []
root = 0
nodes = []
leaves = [(root, 0, 0, 'r')]

nodeid = 0
while len(leaves) > 0:
    node = leaves[0]
    leaves = leaves[1:]
    if node[1] > 2 and node[3] == 'r':
        nodes.append((node[0], node[2], node[3]))
        continue
    children = random.randint(3, 5)
    dpth = node[1] if node[3] == 'r' else node[1] + 1
    tp = 'r' if node[3] == 's' else 's'
    for x in range(children - node[2]):
        nodeid += 1
        leaves.append((nodeid, dpth, 1, tp))
        edges.append((node[0], nodeid))
    nodes.append((node[0], children, node[3]))



G = networkx.Graph()
for x, y in edges:
    G.add_edge(x, y)


print("nodes", nodes)
print("len", len([x for x in nodes if x[2] == 'r']))
print("edges", len(edges))
print("G.nodes", len(G.nodes))
