#!/usr/bin/env python3

import pickle
import re
import subprocess
import sys

# Example usage:
# ./get_pull_timeline.sh 41 0 | egrep -o "clientId.*seq [0-9]+" | uniq | tr , ' '  | cut -d' ' -f 2,5 | xargs ./getConcurrentRpcs.py

if len(sys.argv) != 3:
    print('./getConcurrentRpcs.py client_id sequence')
    quit()
else:
    target_rpc = (int(sys.argv[1]), int(sys.argv[2]))

server_id_2_hostname = {}
with open('server_id_2_hostname.txt', 'r') as file:
    p = re.compile('server([0-9]+) rc([0-9]+)')
    for line in file:
        match = p.match(line)
        if not match:
            continue
        server_id, hostname = int(match.group(1)), int(match.group(2))
        server_id_2_hostname[server_id] = hostname

# Example message:
# logs/server23.tt:T1     3.730 us (+  0.108 us): shuffle-server: start sending pull response, clientId 1406209628, seq 302651
timetrace_messages = subprocess.check_output(
        'egrep "start sending pull resp|received 100004" logs/* | sort -k2n',
        encoding='UTF-8', shell=True).split('\n')
# print(timetrace_messages)

# Map from RpcId to start time, in us.
open_rpcs = {}
rpc_client = {}
rpc_server = {}

# Don't modify open_rpcs once target_rpc_closed is True.
target_rpc_closed = False

rpc_id_pattern = re.compile('logs/server([0-9]+).*clientId ([0-9]+), seq ([0-9]+)')
for message in timetrace_messages:
    match = rpc_id_pattern.match(message)
    if not match:
        continue

    words = message.strip().split()
    is_start = words[8] == 'sending'
    timestamp = float(words[1])
    server_id = int(match.group(1))
    rpc_id = (int(match.group(2)), int(match.group(3)))

    if is_start:
        rpc_server[rpc_id] = server_id_2_hostname[server_id]
        if not target_rpc_closed:
            open_rpcs[rpc_id] = timestamp
    else:
        rpc_client[rpc_id] = server_id_2_hostname[server_id]
        if target_rpc == rpc_id:
            target_rpc_closed = True
        if not target_rpc_closed:
            open_rpcs.pop(rpc_id)

# Set of "rcXX => rcYY" paths that are in conflict with the target RPC.
colliding_pairs = set()
with open('collision_database.bin', 'rb') as file:
    db = pickle.load(file)
    colliding_pairs = db[(f'rc{rpc_server[target_rpc]:02}',
                          f'rc{rpc_client[target_rpc]:02}')]
# print(colliding_pairs)

print(f'{len(open_rpcs)} RPCs overlap with RPC{target_rpc} (self included):')
for (client_id, sequence), start_time in sorted(open_rpcs.items(),
                                                key=lambda kv: kv[1]):
    rpc_id = (client_id, sequence)
    sender = f'rc{rpc_server[rpc_id]:02}'
    receiver = f'rc{rpc_client[rpc_id]:02}'

    tag = ''
    if (sender, receiver) in colliding_pairs:
        tag = '   <<< COLLISION WARNING'

    print(f'{start_time:.2f} us, clientId {client_id}, seq {sequence}, '
          f'{sender} => {receiver}{tag}')

