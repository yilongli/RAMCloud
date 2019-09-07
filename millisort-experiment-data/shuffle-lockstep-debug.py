#!/usr/bin/env python3

import glob
import sys

# Get all server log files
log_files = sorted(glob.glob('server*.log'))
# print(log_files)

# Populate server_dict, which maps RAMCloud server IDs to POD node hostnames.
server_dict = {}
for log_fname in log_files:
    server_id, hostname, _ = log_fname.split(".")
    server_id = int(server_id[6:])
    server_dict[server_id] = hostname
# print(server_dict)

# Parse records
message_bw = []
num_servers = len(server_dict)
for server_id in sorted(server_dict.keys()):
    hostname = server_dict[server_id]
    with open(f'server{server_id}.{hostname}.log', 'r') as log_file:
        for line in log_file:
            # "Shuffle lockstep run %d startOffset %u time %u"
            words = line.strip().split()
            if len(words) < 5:
                continue
            if words[-4] == "startOffset" and words[-2] == "time":
                run = int(words[-5])
                start_offset = int(words[-3])
                bandwidth_gbps = 1000000.0 / int(words[-1]) * 8.0
                message_bw.append(bandwidth_gbps)

                # We are doing pull-based shuffle so the hostname of the current
                # log file is the receiver's hostname.
                tx_host = server_dict[1 + (server_id + start_offset) % num_servers]
                rx_host = hostname
                print(f'{start_offset} {run} {tx_host} {rx_host} {bandwidth_gbps:.2f}')

# Generate CDF for message bandwidth, in text format.
with open("message_bw.cdf", 'w') as outfile:
    message_bw.sort()
    result = []
    outfile.write("%8.4f    %8.3f\n" % (0.0, 0.0))
    outfile.write("%8.4f    %8.3f\n" % (message_bw[0], 1/len(message_bw)))
    for i in range(1, 100):
        outfile.write("%8.4f    %8.3f\n" % (message_bw[int(len(message_bw)*i/100)], i/100))
    outfile.write("%8.4f    %8.3f\n" % (message_bw[int(len(message_bw)*999/1000)], .999))
    outfile.write("%8.4f    %9.4f\n" % (message_bw[int(len(message_bw)*9999/10000)], .9999))
    outfile.write("%8.4f    %8.3f\n" % (message_bw[-1], 1.0))


