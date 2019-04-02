#!/usr/bin/env python3

import re
import subprocess
import os
import pickle

start = re.compile('rc([0-9]+) rc([0-9]+)')
edge = re.compile('\[([0-9]+)\] -> (switch|ca) port.*lid ([0-9]+).*"(.*)"')

forward_path = {}

with open('ib-routing.txt', 'r') as file:
    for line in file:
        match = start.match(line)
        if match:
            src_dest = tuple(line.strip().split())
            forward_path[src_dest] = []
            switch_lid = None
            continue
        match = edge.match(line)
        if not match:
            continue

        node_type = match.group(2)
        out_port = int(match.group(1))
        if switch_lid is not None:
            forward_path[src_dest].append((switch_lid, switch_name, out_port))
            
        switch_lid = int(match.group(3))
        switch_name = match.group(4)

collision_database = {}
host_pairs = sorted(forward_path.keys())
for a, b in host_pairs:
    path0 = forward_path[(a, b)]
    for c, d in host_pairs:
        if (a, b) >= (c, d) or len({a, b} & {c, d}) > 0:
            continue
        path1 = forward_path[(c, d)]

        for lid0, name, out0 in path0:
            for lid1, _, out1 in path1:
                if lid0 == lid1 and out0 == out1:
                    # Packets from a to b and c to d collide at the output port
                    # of some switch.
                    print(f"{a} => {b} X {c} => q{d} @ {lid0}[{out0}] ({name})")

                    # Populate the collision database
                    if (a, b) not in collision_database:
                        collision_database[(a, b)] = set()
                    if (c, d) not in collision_database:
                        collision_database[(c, d)] = set()
                    collision_database[(a, b)].add((c, d))
                    collision_database[(c, d)].add((a, b))

db_name = 'collision_database.bin'
print(f"Collision database populated at ./{db_name}")
with open(db_name, 'wb') as file:
    pickle.dump(collision_database, file)