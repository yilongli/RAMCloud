#!/usr/bin/env python3

from statistics import mean
import matplotlib.pyplot as plt

num_nodes = 55

group_sizes = list(range(1, num_nodes + 1))
flat_gather_10B = [0.0, 3.60, 4.31, 5.08, 5.83, 6.69, 7.46, 8.33, 9.02, 9.90,
                   10.60, 11.50, 12.37, 13.41, 14.18, 14.87, 15.65, 16.90,
                   17.88, 18.46, 19.00, 19.84, 20.86, 21.45, 21.96, 23.71,
                   24.32, 24.86, 25.85, 26.45, 27.08, 27.95, 28.89, 29.44,
                   30.23, 31.43, 31.71, 32.80, 34.04, 34.49, 35.30, 36.60,
                   37.35, 37.95, 38.87, 39.90, 40.62, 41.41, 41.15, 42.71,
                   43.38, 44.00, 45.15, 46.60, 46.65]
flat_gather_1KB = [0.0, 5.24, 5.83, 6.50, 7.73, 8.80, 9.78, 10.64, 11.36, 12.35,
                   12.92, 13.80, 14.75, 15.33, 16.44, 17.44, 18.44, 19.75,
                   20.07, 20.64, 21.23, 21.74, 22.86, 24.28, 24.73, 25.78,
                   26.16, 26.90, 27.82, 28.65, 28.80, 30.47, 31.45, 32.22,
                   32.31, 33.74, 33.85, 35.37, 36.54, 37.56, 37.76, 39.05,
                   40.13, 41.42, 41.50, 42.22, 43.37, 44.07, 45.02, 45.26,
                   45.86, 46.80, 47.53, 48.67, 49.00]
flat_gather_8KB = [0.0, 9.18, 11.27, 13.33, 15.81, 18.15, 20.67, 23.07, 25.34,
                   27.69, 30.06, 32.39, 34.84, 37.19, 39.18, 41.62, 43.91,
                   46.33, 48.65, 50.95, 53.20, 55.44, 57.81, 59.98, 62.33,
                   64.89, 67.11, 69.37, 71.69, 74.12, 76.20, 78.60, 80.89,
                   83.08, 85.48, 87.83, 90.17, 92.42, 94.78, 97.06, 99.33,
                   101.61, 103.80, 106.20, 108.56, 110.81, 113.19, 115.53,
                   117.82, 119.99, 122.46, 124.57, 127.02, 129.33, 131.57]
five_nomial_tree_1KB = [0.0, 5.33, 6.31, 7.17, 7.99, 8.59, 11.48, 12.38, 13.23,
                        14.64, 14.57, 15.10, 15.36, 16.84, 17.50, 17.38, 18.05,
                        18.59, 19.45, 20.88, 20.54, 21.27, 21.87, 22.91, 24.21,
                        23.87, 24.13, 23.77, 24.51, 25.84, 25.78, 26.38, 26.31,
                        27.52, 29.69, 29.75, 29.33, 30.24, 30.42, 31.39, 31.25,
                        32.19, 32.66, 33.15, 34.36, 34.39, 34.74, 34.58, 35.09,
                        36.27, 36.36, 36.21, 36.57, 36.80, 36.55]
ten_nomial_tree_1KB = [0.0, 5.20, 6.19, 7.11, 7.93, 8.85, 10.05, 10.87, 11.17,
                       12.24, 12.99, 14.10, 13.90, 14.28, 14.91, 15.62, 16.35,
                       16.69, 18.14, 18.74, 18.85, 19.27, 19.37, 19.79, 19.58,
                       19.67, 20.28, 21.36, 22.13, 22.63, 22.88, 23.20, 23.50,
                       23.62, 24.08, 24.37, 24.61, 25.13, 26.12, 27.02, 27.08,
                       27.40, 28.29, 28.91, 29.64, 30.02, 30.25, 31.19, 31.73,
                       32.15, 32.37, 32.67, 32.50, 32.58, 32.72]
ten_eight_tree_1KB = [0.0, 5.09, 6.06, 6.77, 7.81, 8.78, 9.77, 10.62, 11.16,
                    12.00, 12.94, 13.84, 14.57, 15.04, 15.73, 17.23, 18.20,
                    20.15, 20.91, 20.95, 20.83, 20.35, 20.96, 21.37, 21.93,
                    23.02, 23.69, 23.54, 23.48, 23.35, 23.34, 23.44, 24.25,
                    25.88, 26.42, 26.15, 25.97, 26.26, 26.19, 26.30, 27.41,
                    28.66, 28.60, 29.00, 28.84, 28.66, 29.00, 29.39, 30.22,
                    31.43, 31.51, 31.43, 31.36, 31.46, 31.43]
ten_eight_tree_1KB_median = [0.0, 5.53, 6.53, 7.61, 8.62, 9.74, 10.72, 11.76,
                             12.81, 14.94, 15.78, 16.44, 16.51, 16.83, 17.60,
                             19.00, 20.49, 22.42, 23.50, 23.45, 23.48, 23.41,
                             23.49, 23.57, 24.22, 25.65, 26.26, 26.09, 26.23,
                             26.05, 26.09, 26.29, 26.93, 28.87, 29.60, 29.49,
                             29.40, 29.32, 29.54, 29.43, 30.14, 31.51, 31.80,
                             32.03, 32.00, 32.00, 32.23, 32.24, 33.09, 34.23,
                             34.45, 34.33, 34.39, 34.25, 34.32]

def flat_gather_model(num_nodes, one_way_delay, rpc_overhead):
    predicted_times = [0.0, one_way_delay]
    for i in range(3, num_nodes + 1):
        predicted_times.append(predicted_times[-1] + rpc_overhead)
    return predicted_times


def compute_network_throughput(per_node_bytes, completion_times):
    throughput_gbps = [0.0]
    for i in range(1, len(completion_times)):
        total_bytes = i * per_node_bytes
        throughput_gbps.append(total_bytes * 8 / (completion_times[i] * 1e3))
    return throughput_gbps

def compute_k1k2_tree_step_delta(k2, completion_times):
    step_deltas = []
    for i in range(len(completion_times)):
        if i >= k2:
            step_deltas.append(completion_times[i] - completion_times[i-k2])
    return mean(step_deltas[-25:])

rpc_overhead_10B = \
    (flat_gather_10B[-1] - flat_gather_10B[1]) / (len(flat_gather_10B) - 2)
flat_gather_10B_model = flat_gather_model(num_nodes, flat_gather_10B[1],
          rpc_overhead_10B)

rpc_overhead_1KB = \
    (flat_gather_1KB[-1] - flat_gather_1KB[1]) / (len(flat_gather_1KB) - 2)
flat_gather_1KB_model = flat_gather_model(num_nodes, flat_gather_1KB[1],
          rpc_overhead_1KB)

rpc_overhead_8KB = \
    (flat_gather_8KB[-1] - flat_gather_8KB[1]) / (len(flat_gather_8KB) - 2)
flat_gather_8KB_model = flat_gather_model(num_nodes, flat_gather_8KB[1],
          rpc_overhead_8KB)

# Print out some estimated statistics
print('RPC receive overhead:')
print(f'10B : {rpc_overhead_10B:.2f} us')
print(f'1KB : {rpc_overhead_1KB:.2f} us')
print(f'8KB : {rpc_overhead_8KB:.2f} us')

print('Avg. additional cost of gathering 8 more 1KB messages in a 10-8 tree:')
print(f'min : {compute_k1k2_tree_step_delta(8, ten_eight_tree_1KB):.2f} us')
print(f'p50 : {compute_k1k2_tree_step_delta(8, ten_eight_tree_1KB_median):.2f} us')


plt.xlabel('# Nodes')
plt.ylabel('Latency (us)')
# plt.plot(group_sizes, flat_gather_10B, marker='x', label="flat(10B)")
# plt.plot(group_sizes, flat_gather_10B_model, marker='x', label="flat_predict(10B)")

# plt.plot(group_sizes, flat_gather_1KB, marker='x', label="flat(1KB)")
# plt.plot(group_sizes, flat_gather_1KB_model, marker='x', label="flat_predict(1KB)")

# plt.plot(group_sizes, flat_gather_8KB, marker='x', label="flat(8KB)")
# plt.plot(group_sizes, flat_gather_8KB_model, marker='x', label="flat_predict(8KB)")

plt.plot(group_sizes, five_nomial_tree_1KB, marker='x', label="5-nomial(1KB)")
plt.plot(group_sizes, ten_nomial_tree_1KB, marker='x', label="10-nomial(1KB)")

plt.plot(group_sizes, ten_eight_tree_1KB, marker='x', label="10/8-tree(1KB)")
# plt.plot(group_sizes, compute_network_throughput(1000, ten_eight_tree_1KB),
#          marker='+', label="10/8-tree(1KB) BW")
# plt.plot(group_sizes, ten_eight_tree_1KB_median, marker='x', label="10/8-tree(1KB) median")

plt.legend()
plt.xlim(0, None)
plt.ylim(0, None)
plt.show()
