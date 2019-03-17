#!/usr/bin/env python3

"""
Stand-alone script which reads a CSV file of data measurements (specified on
the command line) and generates a textual cdf, printed to standard out.
"""

from statistics import mean
import operator
import matplotlib.pyplot as plt

num_nodes = 55

tree_sizes = list(range(1, 56))
ten_nomial_tree = [0.0, 2.95, 3.56, 4.12, 4.74, 5.44, 5.84, 6.59, 7.09, 7.63,
                   8.39, 8.37, 8.37, 8.28, 8.43, 8.91, 9.41, 10.00, 10.53,
                   11.47, 11.38, 11.36, 11.37, 11.40, 11.45, 11.42, 11.38,
                   11.43, 11.69, 12.23, 12.16, 12.14, 12.09, 12.21, 12.25,
                   12.28, 12.05, 12.39, 12.27, 12.91, 12.93, 12.91, 12.88,
                   12.91, 12.96, 12.96, 12.95, 12.91, 13.17, 13.51, 13.44,
                   13.47, 13.56, 13.45, 13.53]
binary_tree = [0.0, 3.25, 3.92, 6.63, 7.42, 7.54, 7.98, 10.11, 10.84, 11.08,
               11.38, 11.70, 11.87, 11.93, 12.18, 13.54, 14.47, 14.59, 14.99,
               15.19, 15.29, 15.57, 15.88, 16.07, 15.90, 16.36, 16.36, 16.35,
               16.60, 16.70, 16.95, 17.83, 18.49, 18.53, 19.09, 18.96, 19.46,
               19.53, 19.94, 19.81, 20.18, 20.20, 20.39, 20.36, 20.63, 20.58,
               21.05, 20.97, 21.02, 21.12, 20.98, 21.04, 21.13, 21.40, 21.39]
binomial_tree = [0.0, 3.18, 3.99, 6.60, 6.75, 7.59, 7.70, 10.32, 10.29, 10.38,
                 10.31, 11.21, 11.21, 11.30, 11.52, 14.00, 14.08, 14.12, 14.05,
                 14.17, 14.15, 14.18, 14.16, 15.09, 15.20, 15.16, 15.17, 15.33,
                 15.45, 15.53, 15.93, 18.13, 18.12, 18.04, 18.07, 17.87, 17.82,
                 18.30, 18.19, 18.30, 18.15, 18.15, 18.16, 18.26, 18.28, 18.34,
                 18.29, 18.98, 19.05, 19.01, 19.09, 19.13, 19.19, 18.93, 19.19]

def binary_tree_model(num_nodes, one_way_delay, rpc_overhead):
    predicted_completion_times = [0.0]
    node_latency = [0.0]
    max_latency = 0
    for rank in range(1, num_nodes):
        parent = (rank - 1) // 2
        latency = node_latency[parent]
        if rank % 2 == 0:
            latency += rpc_overhead
        latency += one_way_delay
        node_latency.append(latency)
        if latency > max_latency:
            max_latency = latency
        predicted_completion_times.append(max_latency)

    return predicted_completion_times

def k_nomial_tree_get_children(k, num_nodes, parent):
    # If d is the number of digits in the base-k representation of parent
    # then let delta = k^d.
    delta = 1
    p = parent
    while p > 0:
        p = p // k
        delta = delta * k

    # The children of a node in the k-nomial tree will have exactly one more
    # non-zero digit in a more significant position than the parent's most
    # significant digit. For example, in a trinomial tree (i.e., k = 3) of
    # size 27, the children of node 7 (21 in base-3) will be node 16 (121 in
    # base-3) and 25 (221 in base-3).
    children = []
    while True:
        for i in range(1, k):
            child = parent + i * delta
            if child >= num_nodes:
                return children
            children.append(child)
        delta = delta * k


def k_nomial_tree_model(k, num_nodes, one_way_delay, rpc_overhead):
    predicted_completion_times = [0.0]
    node_latency = [0.0] * num_nodes

    for rank in range(0, num_nodes):
        children = k_nomial_tree_get_children(k, num_nodes, rank)
        child_idx = 0
        for child in children:
            node_latency[child] = \
                node_latency[rank] + one_way_delay + child_idx * rpc_overhead
            child_idx += 1

    max_latency = 0
    for rank in range(1, num_nodes):
        if node_latency[rank] > max_latency:
            max_latency = node_latency[rank]
        node_latency[rank] = max_latency
        predicted_completion_times.append(max_latency)

    return predicted_completion_times


# ranks = [1, 3, 7, 15, 31]
# one_way_delay = mean(
#         [binary_tree[ranks[i]] / (i + 1) for i in range(len(ranks))])
#
# rpc_overhead = mean(
#         map(operator.sub, ten_nomial_tree[2:11], ten_nomial_tree[1:10]))

one_way_delay = 3.6
rpc_overhead = 0.6

binary_predictions = binary_tree_model(1000, one_way_delay, rpc_overhead)
binomial_predictions = k_nomial_tree_model(2, 1000, one_way_delay, rpc_overhead)
ten_nomial_predictions = k_nomial_tree_model(10, 1000, one_way_delay, rpc_overhead)
print(f'1000-node latency = {ten_nomial_predictions[999]:.2f} us')
# print(f'10000-node latency = {ten_nomial_predictions[9999]:.2f} us')

plt.xlabel('# Nodes')
plt.ylabel('Latency (us)')
plt.plot(tree_sizes, binary_tree_model(num_nodes, one_way_delay, rpc_overhead),
         marker='x',
         label=f"binary_predict({one_way_delay:.2f},{rpc_overhead:.2f})")
plt.plot(tree_sizes, binary_tree, marker='x', label="binary")
plt.plot(tree_sizes,
         k_nomial_tree_model(2, num_nodes, one_way_delay, rpc_overhead),
         marker='x',
         label=f"binomial_predict({one_way_delay:.2f},{rpc_overhead:.2f})")
plt.plot(tree_sizes, binomial_tree, marker='x', label="binomial")
plt.plot(tree_sizes,
         k_nomial_tree_model(10, num_nodes, one_way_delay, rpc_overhead),
         marker='x',
         label=f"10-nomial_predict({one_way_delay:.2f},{rpc_overhead:.2f})")
plt.plot(tree_sizes, ten_nomial_tree, marker='x', label="10-nomial")
plt.legend()
plt.show()
