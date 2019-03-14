#!/usr/bin/env python3

import matplotlib.pyplot as plt

num_nodes = 55

group_sizes = list(range(1, 56))
flat_gather_10B = [0.0, 3.58, 4.28, 5.77, 7.03, 8.49, 10.26, 11.83, 13.07, 14.82,
                   16.55, 18.23, 20.27, 21.75, 24.00, 25.73, 27.73, 28.59,
                   30.76, 32.23, 34.62, 36.45, 37.87, 40.09, 39.85, 43.80,
                   45.44, 47.12, 48.35, 51.25, 53.48, 54.81, 57.26, 58.83,
                   60.64, 62.22, 62.59, 65.93, 67.99, 69.83, 71.44, 73.90,
                   75.47, 78.04, 80.05, 80.92, 83.39, 84.64, 86.30, 87.82,
                   91.83, 92.27, 93.44, 95.89, 98.39]

plt.plot(group_sizes, flat_gather_10B, marker='x', label="flat(10B)")
# plt.plot(group_sizes, flat_gather_1KB, marker='x', label="flat(1KB)")
plt.legend()
plt.show()
