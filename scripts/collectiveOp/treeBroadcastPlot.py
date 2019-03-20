#!/usr/bin/env python3

from statistics import mean
import operator
import matplotlib.pyplot as plt

num_nodes = 55

tree_sizes = list(range(1, num_nodes + 1))
flat_tree = [0.0, 3.18, 3.91, 4.52, 5.03, 5.76, 6.29, 6.85, 7.48, 8.14, 8.71,
             9.26, 9.86, 10.56, 11.04, 11.60, 12.31, 12.92, 13.43, 14.30, 14.85,
             15.44, 16.13, 16.58, 17.25, 17.78, 18.27, 18.76, 19.59, 20.06,
             20.65, 21.22, 21.87, 22.60, 22.95, 23.55, 24.31, 24.92, 25.38,
             26.08, 26.88, 27.01, 27.92, 28.54, 29.04, 29.66, 30.11, 30.77,
             31.53, 31.99, 32.44, 32.98, 33.84, 34.23, 34.91]
flat_tree_1KB = [0.0, 4.74, 5.80, 6.53, 7.30, 8.19, 8.94, 9.79, 10.60, 11.49,
                 12.08, 12.86, 13.59, 14.39, 15.18, 15.81, 16.56, 17.53, 18.31,
                 19.40, 20.21, 20.93, 21.61, 22.41, 23.18, 23.80, 24.64, 25.46,
                 26.11, 26.87, 27.69, 28.08, 29.18, 30.17, 31.03, 31.60, 32.57,
                 33.51, 34.15, 35.08, 35.79, 36.04, 37.39, 37.84, 38.69, 39.65,
                 40.44, 41.19, 41.97, 42.89, 43.47, 44.08, 45.12, 46.00, 46.57]
chain_tree = [0.0, 2.97, 6.33, 9.79, 12.91, 16.55, 20.10, 23.69, 27.16, 30.88,
              34.25, 37.54, 41.40, 44.88, 48.71, 52.25, 56.38, 59.71, 63.13,
              67.25, 70.13, 74.58, 77.94, 82.18, 86.01, 89.62, 92.75, 97.29,
              101.35, 104.80, 108.02, 112.70, 116.78, 120.91, 124.86, 128.59,
              131.69, 137.06, 141.59, 145.83, 150.02, 155.12, 158.57, 163.23,
              167.61, 171.02, 175.87, 180.25, 185.62, 191.66, 195.75, 199.22,
              204.34, 210.02, 214.32]
chain_tree_1KB = [0.0, 4.69, 9.59, 14.74, 19.79, 25.13, 30.19, 35.28, 40.71,
                  45.36, 51.00, 56.47, 61.74, 67.60, 72.41, 77.49, 83.46, 88.73,
                  94.75, 100.20, 105.65, 111.80, 117.03, 122.37, 127.47, 133.81,
                  138.71, 144.25, 150.43, 155.36, 161.49, 166.61, 172.26,
                  178.42, 183.40, 189.85, 194.92, 199.45, 206.65, 213.25,
                  217.39, 224.18, 231.62, 238.65, 244.79, 250.50, 256.84,
                  263.74, 272.14, 276.14, 285.62, 292.85, 299.36, 308.18,
                  314.07]
ten_nomial_tree = [0.0, 3.23, 3.93, 4.53, 5.09, 5.87, 6.37, 6.98, 7.49, 8.08,
                   8.73, 8.78, 8.83, 8.86, 8.84, 9.30, 9.92, 10.49, 11.02,
                   11.69, 11.79, 11.95, 11.74, 11.94, 11.85, 11.77, 11.93,
                   11.77, 12.04, 12.44, 12.57, 12.73, 12.64, 12.68, 12.64,
                   12.57, 12.56, 12.69, 13.15, 13.27, 13.20, 13.36, 13.26,
                   13.20, 13.21, 13.19, 13.31, 13.31, 13.55, 13.80, 13.75,
                   13.82, 13.84, 13.87, 13.84]
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
optimal_tree = [0.0, 3.16, 3.93, 4.66, 5.20, 5.97, 6.59, 7.17, 7.19, 7.96, 7.91,
                8.10, 8.76, 8.64, 8.69, 8.73, 9.22, 9.24, 9.19, 9.53, 9.63,
                10.21, 10.25, 10.27, 10.45, 10.46, 10.48, 10.84, 10.89, 10.92,
                11.05, 11.08, 11.32, 11.16, 11.40, 11.40, 11.58, 11.90, 11.86,
                11.88, 12.00, 12.05, 11.98, 12.10, 12.21, 12.15, 12.54, 12.55,
                12.60, 12.67, 12.61, 12.64, 12.63, 12.58, 12.64]
optimal_tree_1KB = [0.0, 4.66, 5.77, 6.33, 7.25, 8.25, 9.02, 9.88, 10.04, 10.70,
                    11.04, 11.29, 11.71, 12.02, 12.42, 12.39, 12.54, 12.79,
                    13.21, 13.44, 13.31, 13.78, 13.92, 14.35, 14.39, 14.55,
                    14.56, 14.79, 14.91, 15.20, 15.28, 15.42, 15.41, 15.57,
                    15.58, 15.79, 16.12, 16.38, 16.30, 16.43, 16.51, 16.59,
                    16.74, 16.77, 16.80, 17.23, 17.28, 17.30, 17.37, 17.38,
                    17.63, 17.64, 17.78, 17.77, 17.81]

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

def optimal_tree_model(num_nodes, one_way_delay, rpc_overhead):
    # one_way_delay includes rpc_overhead at the very beginning; thus, -1.
    k = int(one_way_delay  / rpc_overhead - 1)

    # Set of nodes ready to broadcast in the current timestep.
    actionable_nodes = [0]
    # When, in us, a node received the broadcast message.
    node_latency = [0.0] * num_nodes
    # Ranks of the child nodes.
    child_nodes = [[] for _ in range(num_nodes)]
    # Current timestep in the simulation.
    current_timestep = 0
    # Broadcast messages that have been sent but not yet received.
    # Each entry in the list is a pair where the second element is the target
    # node and the first element is the timestep at the beginning of which the
    # target node will receive this message (and ready to broadcast).
    bcast_inflight = []
    # Next node to send a broadcast message.
    next_node_to_bcast = 1
    # True if we have reached enough nodes.
    stopSimulation = False
    while not stopSimulation:
        # List `bcast_inflight` is sorted in ascending order of the arrival
        # timestep. Check the head of list for newly arriving messages.
        while len(bcast_inflight) > 0:
            timestep, node = bcast_inflight[0]
            if current_timestep == timestep:
                bcast_inflight = bcast_inflight[1:]
                actionable_nodes.append(node)
            else:
                break

        for node in actionable_nodes:
            # This broadcast message is expected to arrive at the end of
            # timestep `current_timestep + k`, or the beginning of
            # `current_timestep + k + 1`.
            bcast_inflight.append((current_timestep + k + 1, next_node_to_bcast))
            node_latency[next_node_to_bcast] = \
                    node_latency[node] + one_way_delay + \
                    len(child_nodes[node]) * rpc_overhead
            child_nodes[node].append(next_node_to_bcast)
            next_node_to_bcast += 1
            if next_node_to_bcast >= num_nodes:
                stopSimulation = True
                break
        current_timestep += 1

    predicted_times = [0.0]
    max_latency = 0
    for rank in range(1, num_nodes):
        if node_latency[rank] > max_latency:
            max_latency = node_latency[rank]
        node_latency[rank] = max_latency
        predicted_times.append(max_latency)
    return predicted_times


# Estimate OWD & rpc_overhead based on flat-broadcast & chain-broadcast numbers.
one_way_delay = mean(map(operator.sub, chain_tree_1KB[2:], chain_tree_1KB[1:-1]))
rpc_overhead = mean(map(operator.sub, flat_tree_1KB[2:], flat_tree_1KB[1:-1]))
print(f'1KB message, one_way_delay = {one_way_delay:.2f}, rpc_overhead = {rpc_overhead:.2f}')
one_way_delay = mean(map(operator.sub, chain_tree_1KB[2:10], chain_tree_1KB[1:9]))
rpc_overhead = mean(map(operator.sub, flat_tree_1KB[2:10], flat_tree_1KB[1:9]))
print(f'1KB message, one_way_delay = {one_way_delay:.2f}, rpc_overhead = {rpc_overhead:.2f}')

one_way_delay = mean(map(operator.sub, chain_tree[2:], chain_tree[1:-1]))
rpc_overhead = mean(map(operator.sub, flat_tree[2:], flat_tree[1:-1]))
print(f'10B message, one_way_delay = {one_way_delay:.2f}, rpc_overhead = {rpc_overhead:.2f}')

one_way_delay, rpc_overhead = 3.6, 0.6
# one_way_delay, rpc_overhead = 1.15, 0.1

binary_predictions = binary_tree_model(num_nodes, one_way_delay, rpc_overhead)
binomial_predictions = k_nomial_tree_model(2, num_nodes, one_way_delay, rpc_overhead)
ten_nomial_predictions = k_nomial_tree_model(10, num_nodes, one_way_delay, rpc_overhead)
optimal_predictions = optimal_tree_model(num_nodes, one_way_delay, rpc_overhead)

plt.xlabel('# Nodes')
plt.ylabel('Latency (us)')
# plt.plot(tree_sizes, chain_tree, marker='x', label="chain")
# plt.plot(tree_sizes, flat_tree, marker='x', label="flat")

# plt.plot(tree_sizes, binary_predictions, marker='x',
#          label=f"binary_predict({one_way_delay:.2f},{rpc_overhead:.2f})")
# plt.plot(tree_sizes, binary_tree, marker='x', label="binary")

# plt.plot(tree_sizes, binomial_predictions, marker='x',
#          label=f"binomial_predict({one_way_delay:.2f},{rpc_overhead:.2f})")
# plt.plot(tree_sizes, binomial_tree, marker='x', label="binomial")

# plt.plot(tree_sizes, ten_nomial_predictions, marker='x',
#          label=f"10-nomial_predict({one_way_delay:.2f},{rpc_overhead:.2f})")
# plt.plot(tree_sizes, ten_nomial_tree, marker='x', label="10-nomial")

plt.plot(tree_sizes, optimal_predictions, marker='x',
         label=f"optimal_tree_predict({one_way_delay:.2f},{rpc_overhead:.2f})")
plt.plot(tree_sizes, optimal_tree, marker='x', label="optimal")
plt.plot(tree_sizes, optimal_tree_1KB, marker='x', label="optimal_1KB")

plt.legend()
plt.show()
