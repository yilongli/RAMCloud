#!/usr/bin/env python3

'''
For each node, plot the completion times of all runs as a scatter graph.

Example usage:
    ./allShuffleMachineDiffPlot.py shuffle_node_diff_55_nodes_100KB_messages.txt
'''

from sys import argv
import matplotlib.pyplot as plt

xs = []
ys = []
with open(argv[1]) as f:
    run = 0
    for line in f:
        for time in [float(x) * 1e-3 for x in line.split(',')[:-1]]:
            xs.append(run)
            ys.append(time)
        run += 1

plt.xlabel('Node')
plt.ylabel('Latency (us)')
plt.scatter(xs, ys, s = 0.01)
plt.xlim(0, None)
plt.legend()
plt.show()
