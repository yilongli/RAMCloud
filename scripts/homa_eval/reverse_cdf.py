#!/usr/bin/python

# Usage:
# Example one: Generate the RCDF graph for a particular message size
# grep -h "^{message_size}," client1*.log | ./reverse_cdf.py
#
# Example two: Generate the RCDF graphs for every message size
# egrep "^[0-9][1-9]*," client1*.log | cut -d, -f 1 > sizes.txt
# while read s; do grep -h "^$s," client1*.log | ./reverse_cdf.py; done <sizes.txt

from sys import argv
from sys import stdin

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


def main():
    data = stdin.read().split(',')
    message_size = data[0]
    samples = [int(float(micros)*1000) for micros in data[1:]]

    num_samples = len(samples)
    plt.title('Message Size = {} Bytes, # Samples = {}'.format(message_size, num_samples))
    plt.xlabel("Nanoseconds (Log Scale)")
    plt.ylabel("Fraction of Samples Longer than Given Time (Log Scale)")
    plt.xscale('log')
    plt.yscale('log')
    plt.grid(True)

    plt.xlim(1000, samples[-1] + 1)
    plt.ylim(1e-4, 1.0)
    fractions = [float(num_samples - i) / num_samples for i in range(num_samples)]
    plt.plot(samples, fractions)
    plt.savefig('rcdf_message_size_{}.png'.format(message_size))

if __name__ == '__main__':
    main()
