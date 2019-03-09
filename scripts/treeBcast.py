#!/usr/bin/env python3

"""
Stand-alone script which reads a CSV file of data measurements (specified on
the command line) and generates a textual cdf, printed to standard out.
"""

from sys import argv
import re
import matplotlib.pyplot as plt

def print_cdf(filename, args):
    """
    Read data values from file given by filename, and produces a cdf in text
    form.  Each line in the printed output will contain a fraction and a
    number, such that the given fraction of all numbers in the log file have
    values less than or equal to the given number.
    """

    regex_patterns = []
    while len(args) > 0:
        num_nodes, node_rank = int(args[0]), int(args[1])
        args = args[2:]
        p = str(num_nodes) + ','
        if node_rank == 0:
            p += ' '
        else:
            p += str(node_rank)
        p += ','
        regex_patterns.append(p)

    # Read the file into an array of numbers.
    for line in open(filename, 'r'):
        numbers = []

        for pattern in regex_patterns:
            if not re.match(pattern, line):
                continue
            numbers = [int(x) for x in line.split(',')[2:]]
            numbers.sort()
            numbers = numbers[:int(len(numbers) * 0.9999)]

            # Generate a RCDF from the array
            X, Y = [], []
            for i in range(1, len(numbers)-1):
                if (numbers[i] != numbers[i-1] or numbers[i] != numbers[i+1]):
                    X.append(numbers[i])
                    Y.append(1 - (i / len(numbers)))
            X.append(numbers[-1])
            Y.append(1/ len(numbers))
            X = [x / 1000 for x in X]
            plt.plot(X, Y, label=pattern[:-1])

    plt.yscale('log')
    plt.legend()
    plt.show()

    # Generate a CDF from the array.
    # numbers.sort()
    # X = [.0, numbers[0]]
    # Y = [.0, 1/len(numbers)]
    # for i in range(1, 100):
    #     X.append(numbers[int(len(numbers)*i/100)])
    #     Y.append(i/100.0)
    # X.extend([numbers[int(len(numbers)*999/1000)], numbers[int(len(numbers)*9999/10000)], numbers[-1]])
    # Y.extend([.999, .9999, 1.0])
    # plt.plot(X, Y, 'r--')
    # plt.show()

def usage():
    doc = """
    Usage: ./treeBcast.py <input-file> (num_nodes node_rank)+

    Sample Input File:
    0.1210,0.1210,0.1200,0.1210,0.1200,0.1210,0.1200,0.1200,0.1200,0.1200
    0.1210,0.1200,0.1200,0.1210,0.1210,0.1200,0.1200,0.1200,0.1200,0.1200
    0.1200,0.1200,0.1200,0.1210,0.1210,0.1200,0.1251,0.1200,0.1200,0.1200
    0.1200,0.1200,0.1210,0.1200,0.1200,0.1238,0.1200,0.1200,0.1200,0.1210
    ...

    Sample Output:
    0.0000       0.000
    0.1190       0.000
    0.1371       0.010
    0.1381       0.020
    0.1384       0.030
    0.1394       0.040
    0.1405       0.050
    0.1415       0.060
    ...
    0.2421       0.980
    0.2649       0.990
    0.3716       0.999
    3.1641       0.9999
    7.0694       1.000

    """
    print(doc)
    exit(0)

if __name__ == '__main__':
    if (len(argv) - 2) % 2 > 0:
        usage()
    else:
        print_cdf(argv[1], argv[2:])
