#!/usr/bin/env python3

'''
Example usage: ./computePacketDelay.py > packet_delays.txt
'''

import re
import subprocess
import os

packet_trace = subprocess.check_output('grep "" logs/server*tt',
# packet_trace = subprocess.check_output('grep "bytes of reply, " logs/server*tt',
        encoding='UTF-8', shell=True).split('\n')

p = re.compile('.*(transmitted|received) ([0-9]+) bytes of reply, '
               'clientId ([0-9]+), seq ([0-9]+)')

# Packet ID contained in each line of the file.
packet_ids = []
# One-way delay of each packet.
one_way_delay = {}
count = {}

for line in packet_trace:
    words = line.strip().split()
    match = p.match(line)
    if not match:
        packet_ids.append(None)
        continue
    action = match.group(1)
    num_bytes = int(match.group(2))
    client_id = int(match.group(3))
    sequence = int(match.group(4))
    timestamp = float(words[1])

    packet_id = (client_id, sequence, num_bytes)
    packet_ids.append(packet_id)
    if packet_id not in one_way_delay:
        one_way_delay[packet_id] = 0.0
        count[packet_id] = 0
    if action == 'transmitted':
        one_way_delay[packet_id] -= timestamp
        count[packet_id] += 1
    else:
        one_way_delay[packet_id] += timestamp
        count[packet_id] += 1

# Append the packet OWD to the end of each line.
line_num = -1
for line in packet_trace:
    line_num += 1
    if packet_ids[line_num] is None:
        print(line)
        continue
    line = line.rstrip()
    if count[packet_ids[line_num]] < 2:
        print(f'{line}, OWD NaN us')
    else:
        print(f'{line}, OWD {one_way_delay[packet_ids[line_num]]:.2f} us')