#!/usr/bin/env python3

'''
Extract the time spent on each phase of millisort from client1.*.log and print
it in a way easy to compare against the cost estimator.

Example usage:
    grep "=== Time Breakdown" client1.*.log -A 14 > millisort_time_X_nodes.txt
    ./impl_perf_numbers.py
'''

import re
import sys

def is_number(str):
    try:
        val = float(str)
        return True
    except ValueError:
        return False


def average(list):
    if len(list) == 0:
        return .0
    return sum(list) * 1.0 / len(list)


def median(list):
    return list[int(len(list) * 0.5)]

print(f'{"MachineCount":>15} {"tuplesPerNode":>15} {"totalTime":>15} '
      f'{"localSort":>15} {"partition":>15} {"shuffleKeys":>15} '
      f'{"shuffleValues":>15} {"reorderInitVals":>15} {"reorderFinalVals":>15} '
      f'{"copyOutShuffleVal":>15}'
      f'{"gatherPivots":>20} {"gatherSuPivots":>20} {"bcastPivBucBound":>20}'
      f'{"shufflePivots":>20} {"allGather&BcastPivots":>20}'
      f'{"SUM(Last5Cols)":>20}')

tuplesPerNode = 19100
for machineCount in range(2, 21):
    local_sort = []
    partition = []
    shuffle_keys = []
    shuffle_vals = []
    copy_out_shuffle_value = []
    rearrange_init_vals = []
    rearrange_final_vals = []
    gather_pivots = []
    gather_super_pivots = []
    bcast_pivot_bucket_boundaries = []
    shuffle_pivots = []
    allgather_bcast_pivots = []
    total = []

    filename = "ms_time/millisort_time_" + str(machineCount) + "_nodes.txt"
    with open(filename, "r") as f:
        num_runs = 0
        for line in f:
            words = line.strip().split()
            if len(words) < 2:
                continue
            if words[0] == "===" and words[1] == "Time":
                num_runs += 1
            if num_runs < 10:
                continue

            numbers = [float(x) for x in words if is_number(x)]
            if len(numbers) < 3:
                continue

            if words[0] == "Total":
                total.append(numbers[0])
            elif words[0] == "Partition":
                partition.append(numbers[0])
            elif words[0] == "Local":
                local_sort.append(numbers[0])
            elif words[0] == "Shuffle" and words[1] == "keys":
                shuffle_keys.append(numbers[0])
            elif words[0] == "Shuffle" and words[1] == "values":
                shuffle_vals.append(numbers[0])
            elif words[0] == "Copy-out" and words[1] == "shuffle":
                copy_out_shuffle_value.append(numbers[0])
            elif words[0] == "Rearrange" and words[1] == "ini.":
                rearrange_init_vals.append(numbers[0])
            elif words[0] == "Rearrange" and words[1] == "final":
                rearrange_final_vals.append(numbers[0])
            elif words[0] == "Gather" and words[1] == "pivots":
                gather_pivots.append(numbers[0])
            elif words[0] == "Gather" and words[1] == "super-pivots":
                gather_super_pivots.append(numbers[0])
            elif words[0] == "Broadcast" and words[1] == "pivot":
                bcast_pivot_bucket_boundaries.append(numbers[0])
            elif words[0] == "Shuffle" and words[1] == "pivots":
                shuffle_pivots.append(numbers[0])
            elif words[0] == "All-gather":
                allgather_bcast_pivots.append(numbers[0])

    samples = []
    for i in range(len(total)):
        samples.append((total[i], local_sort[i], partition[i], shuffle_keys[i],
                shuffle_vals[i], rearrange_init_vals[i], rearrange_final_vals[i],
                copy_out_shuffle_value[i], gather_pivots[i], gather_super_pivots[i],
                bcast_pivot_bucket_boundaries[i], shuffle_pivots[i],
                allgather_bcast_pivots[i]))
    samples.sort()
    p50 = median(samples)

    print(f'{machineCount:15d} {tuplesPerNode:15d} {p50[0]:15.2f} {p50[1]:15.2f} '
          f'{p50[2]:15.2f} {p50[3]:15.2f} {p50[4]:15.2f} {p50[5]:15.2f} {p50[6]:15.2f} '
          f'{p50[7]:15.2f} {p50[8]:20.2f} {p50[9]:20.2f} {p50[10]:20.2f} {p50[11]:20.2f} '
          f'{p50[12]:20.2f} {sum(p50[-5:]):20.2f}')
