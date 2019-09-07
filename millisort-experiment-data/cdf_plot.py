#!/usr/bin/env python3

import sys
import matplotlib.pyplot as plt

if len(sys.argv) == 2:
    cdf_file = sys.argv[1]
else:
    print('Usage: cdf_plot.py <cdf-file>')

x = []
y = []
with open(cdf_file, 'r') as cdf:
    for line in cdf:
        x0, y0 = line.split()
        x.append(float(x0))
        y.append(float(y0))

# Plot out the CDF with steps style:
#     http://joelotz.github.io/step-functions-in-matplotlib.html
# plt.plot(x, y, drawstyle='steps', linestyle='-', label='steps-pre', alpha=0.5,)
# plt.plot(x, y, drawstyle='steps-mid', linestyle='--', label='steps-mid', alpha=0.5,)
# plt.plot(x, y, drawstyle='steps-post', linestyle=':', label='steps-post', alpha=0.5,)
plt.plot(x, y, drawstyle='steps-post', linestyle='-', label='CDF', alpha=0.5,)
plt.legend(loc='upper left')
plt.xlim([x[0], x[-1]])
plt.ylim([0, 1])
plt.show()