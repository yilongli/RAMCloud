#!/usr/bin/env python3

import numpy as np
from scipy import special
import math
import matplotlib.pyplot as plt
import sys

# Example usage:
# ./test num_simulations num_nodes num_records_per_node num_pivots input_distribution
#
# To generate CDF for shuffle message sizes:
# egrep "^[0-9]" <output_of_this_script> | cut -d' ' -f2- > message_sizes.txt
# scripts/cdf.py message_sizes.txt

# Change to True to enable debug log messages.
DEBUG = False

def eval_splitter(keys, splitters):
    '''
    Evaluate how balanced a set of splitters divide a given list of keys.
    :param keys:
        Sorted list of keys.
    :param splitters:
        Sorted list of splitters.
    :return:
        The normalized size of each partition. A value of `x` suggests the size
        of the partition is  `x * len(keys) / len(splitters)`.
    '''
    ideal_part_size = len(keys) * 1.0 / len(splitters)
    keyIdx = 0
    result = []
    for splitter in splitters:
        part_size = 0
        while keyIdx < len(keys) and keys[keyIdx] <= splitter:
            part_size += 1
            keyIdx += 1
        result.append(part_size * 1.0 / ideal_part_size)
    return result


def generate_keys(distribution):
    '''
    Generate random keys conforming to some distribution.
    :param distribution:
        Distribution of the generated keys.
    :return:
        List of generated keys.
    '''
    if distribution == 0:
        # Uniform distribution.
        keys = np.random.random_integers(0, 2**62, size=num_records_per_node)
    elif distribution == 1:
        # Normal/gaussian distribution
        mu, sigma = 0, 100
        keys = np.random.normal(mu, sigma, size=num_records_per_node)\
                .round().astype(np.int)
        if DEBUG:
            # https://docs.scipy.org/doc/numpy/reference/generated/numpy.random.normal.html
            count, bins, ignored = plt.hist(keys, 100, density=True)
            plt.plot(bins, 1/(sigma * np.sqrt(2 * np.pi)) *
                    np.exp( - (bins - mu)**2 / (2 * sigma**2) ),
                    linewidth=2, color='r')
            plt.show()
            quit()
    else:
        # Zipfian distribution
        a = 2.
        keys = np.random.zipf(a, size=num_records_per_node)
        if DEBUG:
            # https://docs.scipy.org/doc/numpy/reference/generated/numpy.random.zipf.html
            count, bins, ignored = plt.hist(keys[keys<50], 50, density=True)
            x = np.arange(1., 50.)
            y = x**(-a) / special.zetac(a)
            plt.plot(x, y/max(y), linewidth=2, color='r')
            plt.show()
            quit()
    return keys

def partition(keys, num_parts):
    '''
    Divide a list of keys into several partitions evenly.
    :param keys:
        List of sorted keys to partition.
    :param num_parts:
        # partitions desired.
    :return:
        Splitter values (i.e., right-inclusive bounds) selected to partition
        the keys as evenly as possible.
    '''
    num_keys = len(keys)
    # Let part_size be a real value to reduce rounding error.
    part_size = num_keys * 1.0 / num_parts
    part_end = 0.0
    splitters = []
    for i in range(num_parts):
        part_end += part_size
        # Round part_end to integer to obtain the splitter index.
        index = min(num_keys, int(round(part_end)))
        splitters.append(keys[index - 1])

    return splitters


if len(sys.argv) != 6:
    print('./test num_sims num_nodes num_records_per_node num_pivots input_distribution')
    quit()
else:
    num_sims, num_nodes, num_records_per_node, num_pivots, distribution = \
        int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4]),\
        int(sys.argv[5])

for simulation_id in range(num_sims):
    keys_at_node = []
    # pivots are always *inclusive* right bound
    pivots = []
    for node_id in range(num_nodes):
        keys = generate_keys(distribution)
        keys.sort()
        extended_keys = []
        for i in range(num_records_per_node):
            extended_keys.append((keys[i], node_id, i))
        keys_at_node.append(extended_keys)

        # Draw samples (or, local pivots) from the local data.
        # Note: we must add the smallest key to our samples to better
        # approximate our key distribution since pivots are right bounds.
        local_pivots = \
            [extended_keys[0]] + partition(extended_keys, num_pivots)
        pivots.extend(local_pivots)
        if DEBUG:
            print(f"pivots@node {node_id} = {local_pivots}")

    # Compute final splitters based on all the samples.
    pivots.sort()
    splitters = partition(pivots, num_nodes)
    if DEBUG:
        print(f"splitters = {splitters}")

    # Evaluate the quality of final splitters based on two metrics: individual
    # shuffle message size and final data skewness factor.
    final_data_sizes = [0] * num_nodes
    messages_at_node = []
    for node_id in range(num_nodes):
        shuffle_msg_sizes = eval_splitter(keys_at_node[node_id], splitters)
        for i in range(num_nodes):
            final_data_sizes[i] += shuffle_msg_sizes[i] / num_nodes
        messages_at_node.append(shuffle_msg_sizes)

    dist_str = ['UNIFORM', 'GAUSS', 'ZIPF']
    print(f"Simulation {simulation_id}: #nodes = {num_nodes}, "
          f"#records/node = {num_records_per_node}, "
          f"#pivots/node = {num_pivots}, dist = {dist_str[distribution]}, "
          f"skewness = {max(final_data_sizes):.5f}")

    for node_id in range(num_nodes):
        print(f"{node_id}", end='')
        for size in messages_at_node[node_id]:
            print(f", {size:.3f}", end='')
        print()
    print("S", end='')
    for skew in final_data_sizes:
        print(f", {skew:.3f}", end='')
    print()
