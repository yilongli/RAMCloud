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
        # keys = np.random.random_integers(0, 2**62, size=num_records_per_node)
        # keys = np.random.random_integers(0, 10000, size=num_records_per_node)
        keys = np.random.random_integers(0, 10*1000*1000, size=num_records_per_node)
    elif distribution == 1:
        # Normal/gaussian distribution
        mu, sigma = 0, 100
        keys = np.random.normal(mu, sigma, size=num_records_per_node)
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


def compute_splitters(tagged_pivots, data_units_per_bucket):
    '''
    Implements an approximation of the gradient-based splitter selection
    algorithm proposed by John. The original gradient-based algorithm doesn't
    apply to arbitrary (e.g., non-numeric) keys; it's also harder to implement
    efficiently.

    :param tagged_pivots:
        A list of sorted pivots; a tag is attached each pivot indicating its
        original position in the local node (e.g., start pivot, end pivot, or
        none of the above).
    :param data_units_per_bucket:
         Ideal # data units between two splitters.
    :return:
        A list of splitters selected to divide the pivots evenly.
    '''

    # Number of non-begin pivots we have passed so far.
    num_nb_pivots_passed = 0
    # Number of local nodes whose end pivots are yet to pass.
    num_active_sources = 0
    splitters = []

    next_split_point = data_units_per_bucket
    for pivot, tag in tagged_pivots:
        if tag < 0:
            # begin pivot
            num_active_sources += 1
        elif tag > 0:
            # end pivot
            num_active_sources -= 1
            num_nb_pivots_passed += 1
        else:
            # normal pivot
            num_nb_pivots_passed += 1

        approx_cml_data = num_nb_pivots_passed + (num_active_sources - 1) * 0.5
        if approx_cml_data >= next_split_point:
            splitters.append(pivot)
            next_split_point += data_units_per_bucket

    if num_nb_pivots_passed + (num_active_sources - 1) * 0.5 < next_split_point:
        last_pivot, _ = tagged_pivots[-1]
        splitters.append(last_pivot)
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
    tagged_pivots = []
    for node_id in range(num_nodes):
        keys = generate_keys(distribution)
        keys.sort()
        extended_keys = []
        for i in range(num_records_per_node):
            extended_keys.append((keys[i], node_id, i))
        keys_at_node.append(extended_keys)

        # Select pivots from keys. Tag a key with -1, 0, or 1 to specify if it's
        # a start pivot, a normal pivot, or a end pivot; this tag is used in the
        # gradient-based splitter selection algorithm.
        local_pivots = partition(extended_keys, num_pivots)
        pivots.extend(local_pivots)
        tagged_local_pivots = [(extended_keys[0], -1)] + \
                [(x, 0) for x in local_pivots[:-1]] + [(local_pivots[-1], 1)]
        tagged_pivots.extend(tagged_local_pivots)
        if DEBUG:
            print(f"pivots@node {node_id} = {local_pivots}")

    # Compute final splitters based on all the samples.
    GRADIENT_BASED = True
    if GRADIENT_BASED:
        tagged_pivots.sort()
        splitters = compute_splitters(tagged_pivots, num_pivots)
    else:
        pivots.sort()
        splitters = partition(pivots, num_nodes)
        PRINT_PIVOT_ONLY = False
        if PRINT_PIVOT_ONLY:
            for key, _, _ in pivots:
                print(key)
            exit()
    if DEBUG:
        print(f"splitters = {splitters}")

    # Evaluate the quality of final splitters based on two metrics: individual
    # shuffle message size and final data skewness factor.
    final_data_sizes = [0] * num_nodes
    buckets_at_node = []
    for node_id in range(num_nodes):
        bucket_sizes = eval_splitter(keys_at_node[node_id], splitters)
        for i in range(num_nodes):
            final_data_sizes[i] += bucket_sizes[i] / num_nodes
        buckets_at_node.append(bucket_sizes)

    PRINT_SKEWNESS_ONLY = False
    if PRINT_SKEWNESS_ONLY:
        print(f"{max(final_data_sizes):.5f}", flush=True)
        continue

    dist_str = ['UNIFORM', 'GAUSS', 'ZIPF']
    print(f"Simulation {simulation_id}: #nodes = {num_nodes}, "
          f"#records/node = {num_records_per_node}, "
          f"#pivots/node = {num_pivots}, dist = {dist_str[distribution]}, "
          f"skewness = {max(final_data_sizes):.5f}")

    for node_id in range(num_nodes):
        print(f"Node {node_id}", end='')
        for size in buckets_at_node[node_id]:
            print(f", {size:.3f}", end='')
        print()

    print()
    largest_msg_per_step = [0.0] * num_nodes
    print(f"{'Node ID'}", end="")
    for node_id in range(num_nodes):
        print(f"  {node_id:>5d}", end="")
    print(f"  {'Max':>5s}")
    for lock_step in range(num_nodes):
        print(f"Step {lock_step:2d}", end="")
        for i in range(num_nodes):
            # Everyone sends to its i-th left-neighbor at step i.
            msg_size = buckets_at_node[i][(i + num_nodes - lock_step) % num_nodes]
            largest_msg_per_step[i] = max(largest_msg_per_step[i], msg_size)
            print(f", {msg_size:5.2f}", end="")
        print(f", {largest_msg_per_step[lock_step]:5.2f}")

    print("MaxShuffleMsg", end='')
    for largest_msg in largest_msg_per_step:
        print(f", {largest_msg:.3f}", end='')
    print()

    # TODO: 2-level shuffle
    print()
    group_size = round(math.sqrt(num_nodes))
    num_groups = math.ceil(num_nodes * 1.0 / group_size)
    print(f"Group size {group_size}, # groups {num_groups}")
    if group_size * num_groups != num_nodes:
        print(f"Unsupported node size: {num_nodes}")
        exit()
    # largest_msg_per_step = [0.0] * num_nodes
    # print(f"{'Node ID'}", end="")
    # for node_id in range(num_nodes):
    #     print(f"  {node_id:>5d}", end="")
    # print(f"  {'Max':>5s}")
    # Stage 1: intra-group shuffle
    group_bucket_sizes = [0] * num_groups
    for lock_step in range(group_size):
        print(f"Step {lock_step:2d}", end="")
        for sender_id in range(num_nodes):
            receiver_id = (sender_id - 1) % num_nodes
            if sender_id // group_size != receiver_id // group_size:
                receiver_id = sender_id + group_size - 1
            # group_id = sender_id // group_size
            # sender_local_id = sender_id % group_size
            # receiver_local_id = \
            #         (sender_local_id + group_size - lock_step) % group_size
            # receiver_id = group_id * group_size + receiver_local_id
            msg_size = 0
            for i in range(num_groups):
                # print(f"D{sender_id},{group_size * i + (receiver_id % group_size)}",end=",")
                msg_size += buckets_at_node[sender_id][group_size * i + (receiver_id % group_size)]
            print(f", {msg_size:5.2f}", end="")

        # for group_id in range(num_groups):
        #     for i in range(group_size):
        #         # Everyone sends to its i-th left-neighbor (within the group)
        #         # at step i.
        #         if (num_nodes % group_size == 0) or (group_id < num_groups - 1):
        #             actual_group_size = group_size
        #         else:
        #             actual_group_size = num_nodes % group_size
        #
        #         src = group_id * group_size + i
        #         dst = group_id * group_size + \
        #               (i + actual_group_size - lock_step) % actual_group_size
        #         msg_size = buckets_at_node[src][dst]
        #         group_bucket_sizes[group_id] += msg_size
        #         print(f", {msg_size:5.2f}", end="")
        print()
    # Stage 2: inter-group shuffle
    for lock_step in range(num_groups):
        print(f"Step {lock_step:2d}", end="")
        for sender_id in range(num_nodes):
            receiver_id = (sender_id - group_size * i) % num_nodes
            msg_size = 0
            for i in range(group_size):
                # print(f"D{(sender_id // group_size) + i},{receiver_id}",end=",")
                msg_size += buckets_at_node[(sender_id // group_size) + i][receiver_id]
            print(f", {msg_size:5.2f}", end="")

        # for group_id in range(num_groups):
        #     # Everyone sends to its peer in the i-th left-group at step i.
        #     msg_size = group_bucket_sizes[(group_id + num_groups - lock_step) % num_groups]
        #     print(f", {msg_size:5.2f}", end="")
        print()

    print()
    print("FinalBucketSize", end='')
    max_skew = max(final_data_sizes)
    max_bucket_ids = []
    cml_data = 0
    bucket_id = 0
    for skew in final_data_sizes:
        cml_data += skew
        print(f", {skew:.3f} ({cml_data:.3f})", end='')

        if max_skew - skew < 1e-6:
            max_bucket_ids.append(bucket_id)
        bucket_id = bucket_id + 1
    print()
    final_data_sizes.sort()
    top_ten_buckets = ", ".join(
            f"{x:.3f}" for x in reversed(final_data_sizes[-10:]))
    print(f"Top 10 buckets: {top_ten_buckets}");
    print(f"MaxBucketIDs: {max_bucket_ids}")

    # for x in range(20,600):
    #     s = int(math.sqrt(x))
    #     while x % s > 0:
    #         s -= 1
    #     print(x, s, x / s)