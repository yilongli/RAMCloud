#!/usr/bin/env python3

import matplotlib.pyplot as plt

num_nodes = 55

group_sizes = list(range(1, 56))
flat_gather_10B = [0.0, 3.58, 4.28, 5.77, 7.03, 8.49, 10.26, 11.83, 13.07,
                   14.82, 16.55, 18.23, 20.27, 21.75, 24.00, 25.73, 27.73,
                   28.59, 30.76, 32.23, 34.62, 36.45, 37.87, 40.09, 39.85,
                   43.80, 45.44, 47.12, 48.35, 51.25, 53.48, 54.81, 57.26,
                   58.83, 60.64, 62.22, 62.59, 65.93, 67.99, 69.83, 71.44,
                   73.90, 75.47, 78.04, 80.05, 80.92, 83.39, 84.64, 86.30,
                   87.82, 91.83, 92.27, 93.44, 95.89, 98.39]
flat_gather_1KB = [0.0, 4.76, 5.81, 7.02, 8.46, 10.08, 11.75, 13.67, 15.12,
                   16.51, 18.75, 20.58, 22.24, 24.11, 25.96, 28.12, 29.09,
                   31.87, 33.32, 34.79, 37.78, 38.17, 41.45, 43.35, 43.42,
                   46.89, 48.14, 50.54, 51.56, 53.97, 56.18, 57.73, 59.84,
                   61.73, 60.70, 66.26, 66.70, 68.77, 71.11, 74.08, 75.88,
                   77.12, 78.60, 80.59, 82.69, 85.31, 84.21, 88.06, 90.08,
                   92.22, 93.51, 95.21, 98.22, 100.13, 102.17]
binomial_tree_1KB = [0.0, 5.27, 6.42, 11.93, 11.70, 14.90, 15.29, 20.70, 20.76,
                     21.62, 21.46, 24.43, 24.57, 25.53, 25.55, 31.97, 32.23,
                     33.36, 33.08, 33.95, 33.74, 34.30, 34.43, 38.38, 39.30,
                     39.86, 40.00, 40.96, 40.62, 41.76, 41.43, 48.19, 47.87,
                     48.63, 48.59, 50.55, 50.38, 51.66, 50.78, 52.62, 52.43,
                     52.98, 52.72, 54.20, 53.91, 54.42, 53.82, 59.44, 59.63,
                     60.47, 60.43, 61.68, 60.96, 61.41, 61.56]
five_nomial_tree_1KB = [0.0, 5.25, 6.44, 8.19, 9.37, 11.06, 13.04, 13.32, 14.37,
                        16.53, 16.88, 15.99, 16.91, 18.13, 19.66, 20.30, 20.21,
                        21.28, 21.85, 23.44, 24.09, 24.16, 24.87, 26.38, 27.61,
                        28.72, 28.49, 28.71, 28.43, 29.96, 29.78, 29.80, 29.91,
                        30.46, 31.72, 31.67, 32.06, 32.06, 32.63, 33.17, 33.13,
                        34.35, 35.05, 36.11, 37.16, 38.09, 38.11, 38.81, 39.10,
                        40.47, 40.26, 40.19, 39.92, 41.16, 42.12]
ten_nomial_tree_1KB = [0.0, 5.28, 6.76, 7.91, 9.37, 10.99, 12.52, 13.96, 16.15,
                       17.72, 19.82, 20.51, 20.38, 20.96, 20.99, 21.87, 22.11,
                       22.63, 23.78, 25.36, 25.51, 25.81, 25.96, 25.86, 26.18,
                       26.55, 26.54, 27.05, 27.68, 28.97, 28.97, 29.74, 29.41,
                       30.48, 30.47, 29.85, 30.88, 30.65, 31.57, 32.90, 33.88,
                       34.20, 34.84, 35.49, 36.23, 36.95, 37.21, 37.72, 39.59,
                       41.26, 41.63, 41.48, 41.70, 41.44, 41.87]

flat_gather_10B_model = flat_gather_10B[:1]
rpc_overhead_10B = flat_gather_10B[-1] / (len(flat_gather_10B) - 1)
for i in range(2, 56):
    completion_time = flat_gather_10B_model[-1] + rpc_overhead_10B
    flat_gather_10B_model.append(completion_time)

rpc_overhead_1KB = flat_gather_1KB[-1] / (len(flat_gather_1KB) - 1)
flat_gather_1KB_model = flat_gather_1KB[:1]
for i in range(2, 56):
    completion_time = flat_gather_1KB_model[-1] + rpc_overhead_1KB
    flat_gather_1KB_model.append(completion_time)

plt.xlabel('# Nodes')
plt.ylabel('Latency (us)')
plt.plot(group_sizes, flat_gather_10B, marker='x', label="flat(10B)")
# plt.plot(group_sizes, flat_gather_10B_model, marker='x', label="flat_predict(10B)")

plt.plot(group_sizes, flat_gather_1KB, marker='x', label="flat(1KB)")
# plt.plot(group_sizes, flat_gather_1KB_model, marker='x', label="flat_predict(1KB)")

plt.plot(group_sizes, binomial_tree_1KB, marker='x', label="binomial(1KB)")
plt.plot(group_sizes, five_nomial_tree_1KB, marker='x', label="5-nomial(1KB)")
# plt.plot(group_sizes, ten_nomial_tree_1KB, marker='x', label="10-nomial(1KB)")
plt.legend()
plt.show()
