#!/usr/bin/python
# Usage: compute_slowdown.py baseline_data experiment_data [transport workload load_factor]
#
# Compute echo RPC slowdowns by normalizing the actual RPC times aginst the
# best-case RPC times. The first two arguments specify the paths to the two
# files that contain the best-case and actual RPC times, respectively. Three
# more arguments can be provided optionally to specify the transport used to
# run the experiment, the workload, and the load factor, respectively; when
# absent, the script will try to infer these information from the filename of
# `experiment_data`.

from sys import argv

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

def main():
    baseline_data = argv[1]
    experiment_data = argv[2]
    if len(argv) > 3:
        transport, workload, load_factor = argv[3:]
    else:
        transport, workload, load_factor = \
                experiment_data.split('/')[-1].split("_")[:3]

    # Read best-case RPC times of all message sizes from file
    rpc_time_min = {}
    with open(baseline_data) as f:
        for line in f.readlines():
            size, time, unit = line.strip().split(" ");
            size = int(size)
            time = float(time)
            if unit == "ms":
                time *= 1000
            elif unit == "s":
                time *= 1000000
            rpc_time_min[size] = time

    # Read median and 99%-tile tail RPC time of all message sizes from file
    # Example format:
    #    Size  Samples  Min  Median  90%  99%  99.9%  Max
    num_samples = {}
    rpc_time_median = {}
    rpc_time_tail = {}
    total_samples = 0.0
    with open(experiment_data) as f:
        for line in f.readlines():
            data = line.strip(' ').split()
            if len(data) != 8:
                continue
            data = [float(x) for x in data]

            size, num_samples[size], _, rpc_time_median[size], _, rpc_time_tail[size] = data[:6]
            total_samples += num_samples[size]

    # Print out RPC slowdowns
    for size in sorted(num_samples.iterkeys()):
        print("%s %s %s %8d %5d %10.7f %8.2f %8.2f" % (transport,
                load_factor, workload, size, num_samples[size],
                num_samples[size] / total_samples,
                rpc_time_median[size] / rpc_time_min[size],
                rpc_time_tail[size] / rpc_time_min[size]))

if __name__ == '__main__':
    main()
