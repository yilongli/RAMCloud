#!/usr/bin/python

# from docopt import docopt
import re
from sys import argv


def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

doc = """
Usage: ./compute_slowdown.py transport workload_type load_factor

    -h,--help                  show this
    -b,--baseline <file>       path to the baseline data file
    -e,--experiment <file>     path to the experiment data file
"""


def main():
    # options = docopt(doc)
    if len(argv) != 4:
        print(doc)
        quit()

    transport = argv[1]
    workload_type = argv[2]
    load_factor = int(argv[3])

    regex = re.compile("echo(\d+).min\s+(\d+\.\d+) (us|ms|s)")

    # Example: echo50.min 4.7 us     send 50B message, receive 50B message minimum
    baseline_data = "%s_baseline.txt" % workload_type
    #print(baseline_data)

    rtt_min = {}
    with open(baseline_data) as f:
        # x = [regex.match(line) for line in f.readlines()]

        for line in f.readlines():
            match_obj = regex.match(line)
            if match_obj is None:
                continue

            size, time, unit = match_obj.groups()
            size = int(size)
            time = float(time)
            if unit == "ms":
                time *= 1000
            elif unit == "s":
                time *= 1000000
            rtt_min[size] = time

    # Example: Size   Samples       Min    Median       90%       99%     99.9%       Max
    experiment_data = "%s_%s_%d_experiment.txt" % (transport, workload_type, load_factor)
    #print(experiment_data)
    num_samples = {}
    rtt_median = {}
    rtt_tail = {}
    total_samples = 0.0
    with open(experiment_data) as f:
        for line in f.readlines():
            data = [float(x) for x in line.strip(' ').split() if is_number(x)]
            if len(data) != 8:
                continue

            size = int(data[0])
            samples = int(data[1])
            rtt_median[size] = data[3]
            rtt_tail[size] = data[5]

            num_samples[size] = samples
            total_samples += samples

    for size in sorted(num_samples.iterkeys()):
        print("%s %d %s %8d %5d %10.7f %8.2f %8.2f" % (transport, load_factor,
                workload_type, size, num_samples[size],
                num_samples[size]/total_samples, rtt_median[size]/rtt_min[size],
                rtt_tail[size]/rtt_min[size]))

if __name__ == '__main__':
    main()
