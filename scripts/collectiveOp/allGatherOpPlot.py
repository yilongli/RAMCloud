#!/usr/bin/env python3

import matplotlib.pyplot as plt

num_nodes = 55

group_sizes = list(range(1, 56))
recursive_doubling_100B = [0.0, 6.36, 19.92, 13.79, 28.59, 27.49, 27.42, 21.67,
                           38.31, 37.75, 37.04, 37.20, 37.41, 37.03, 37.21,
                           30.27, 48.99, 47.96, 47.85, 47.54, 48.39, 48.25,
                           48.29, 48.48, 48.57, 48.00, 48.92, 48.61, 48.98,
                           48.76, 48.99, 41.26, 62.76, 61.80, 62.40, 62.01,
                           61.66, 61.29, 62.86, 62.18, 61.87, 63.20, 62.58,
                           62.86, 63.16, 63.00, 63.80, 63.53, 64.22, 63.76,
                           64.68, 63.91, 64.50, 64.35, 64.17]
recursive_doubling_1KB = [0.0, 8.45, 27.50, 18.49, 41.68, 42.41, 41.78, 31.27,
                          57.40, 58.54, 58.57, 59.47, 59.61, 60.10, 60.66,
                          45.77, 79.52, 80.10, 79.40, 80.24, 82.39, 83.03,
                          83.67, 85.86, 85.87, 85.91, 86.22, 85.93, 87.59,
                          88.00, 88.02, 65.83, 110.79, 111.37, 111.84, 113.48,
                          115.98, 117.10, 119.14, 121.35, 120.20, 122.61,
                          122.95, 124.99, 126.64, 126.82, 128.56, 128.04,
                          128.27, 131.07, 128.69, 129.16, 132.23, 133.47,
                          133.13]

plt.plot(group_sizes, recursive_doubling_100B, marker='x', label="hypercube(100B)")
plt.plot(group_sizes, recursive_doubling_1KB, marker='x', label="hypercube(1KB)")
plt.legend()
plt.show()
