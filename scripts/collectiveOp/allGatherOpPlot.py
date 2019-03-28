#!/usr/bin/env python3

import matplotlib.pyplot as plt

num_nodes = 55

group_sizes = list(range(1, 56))
recursive_doubling_100B = [0.0, 3.91, 10.08, 8.81, 15.03, 15.86, 19.97, 14.61,
                           20.83, 21.62, 23.82, 24.08, 27.62, 27.84, 28.07,
                           21.15, 27.52, 28.10, 28.60, 28.86, 31.40, 31.32,
                           31.81, 31.98, 35.81, 36.67, 37.22, 37.15, 37.36,
                           37.69, 37.94, 29.12, 36.38, 37.05, 37.70, 37.89,
                           38.68, 39.11, 39.38, 39.18, 41.31, 42.12, 42.53,
                           43.13, 43.39, 43.55, 43.76, 43.81, 47.57, 48.39,
                           49.33, 49.51, 49.55, 49.92, 50.18]
recursive_doubling_1KB = [0.0, 5.72, 15.43, 13.22, 22.92, 25.20, 32.84, 22.91,
                          34.28, 35.10, 38.83, 40.31, 48.16, 49.10, 50.06,
                          34.85, 48.69, 50.19, 51.86, 53.61, 59.14, 60.62,
                          62.15, 63.08, 71.16, 73.02, 73.77, 77.67, 77.28,
                          78.48, 79.89, 53.72, 73.42, 75.20, 76.89, 77.94,
                          83.97, 84.97, 86.99, 88.86, 99.11, 99.32, 100.51,
                          111.88, 114.39, 114.48, 115.92, 116.90, 118.24,
                          118.98, 119.02, 120.90, 122.73, 124.63, 125.84]

def compute_stage_cost(completion_times):
    times = [0.0]
    stage_costs = []
    for i in range(1, 100):
        x = 2**i
        if x - 1 < len(completion_times):
            times.append(completion_times[x - 1])
            stage_costs.append(times[-1] - times[-2])
        else:
            return stage_costs

print(compute_stage_cost(recursive_doubling_100B))
print(compute_stage_cost(recursive_doubling_1KB))

plt.xlabel('# Nodes')
plt.ylabel('Latency (us)')
plt.plot(group_sizes, recursive_doubling_100B, marker='x', label="hypercube(100B)")
plt.plot(group_sizes, recursive_doubling_1KB, marker='x', label="hypercube(1KB)")
plt.legend()
plt.show()
