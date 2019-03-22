#!/usr/bin/env python3

'''
For each shuffle operation, plot the completion times of each individual node
as a scatter graph.

Example usage:
    ./allShuffleStragglerBandPlot.py shuffle_55_nodes_100KB_messages.txt
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

plt.xlabel('Run')
plt.ylabel('Latency (us)')
plt.scatter(xs, ys, s = 0.1)
plt.xlim(0, None)
plt.ylim(bottom=0)
plt.legend()
plt.show()
