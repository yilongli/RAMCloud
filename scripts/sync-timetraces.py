#!/usr/bin/python

import itertools
import glob
import re
from sys import argv

class Grant(object):

    def __init__(self, grant_id):
        self.id = grant_id
        self.sender = None
        self.receiver = None
        self.tx_timestamp = None
        self.rx_timestamp = None

#
grants = {}

#
hosts = set()

#
timestamp_diffs = {}

LOG_FILE_REGEX = re.compile("(client|server)[0-9]*\.(.*)\.log")
# Example: 1492031792.531004386 TimeTrace.cc:172 in printInternal NOTICE[8]: 255789.0 ns (+ 114.9 ns): server received DATA, sequence 61257, offset 13846, length 2008, flags 1
TIMETRACE_MSG_REGEX = re.compile(".*TimeTrace.cc:.*:\s+(\d+.\d+) ns.*: (.*)")
#GRANT_MSG_REGEX = re.compile("^.*: (.*) ns .*(client|server) (sending|received) GRANT.*clientId (\d+), sequence (\d+), offset (\d+)")
ALLDATA_MSG_REGEX = re.compile("^.*: (.*) ns .*(client|server) (sending|received) ALL_DATA.*clientId (\d+), sequence (\d+)")

for fname in glob.glob("*.log"):
    match_result = LOG_FILE_REGEX.match(fname)
    if not match_result:
        continue
    print "Scanning %s..." % fname

    host = match_result.group(2)
    hosts.add(host)

    with open(fname, 'r') as f:
        for line in f.readlines():
            #match_result = GRANT_MSG_REGEX.match(line)
            match_result = ALLDATA_MSG_REGEX.match(line)
            if not match_result:
                continue
            timestamp = float(match_result.group(1))
            who = match_result.group(2)
            is_sender = match_result.group(3) == "sending"
            client_id = match_result.group(4)
            sequence_num = int(match_result.group(5))
            #offset = int(match_result.group(6))

            server2client = (who == "client") ^ is_sender
            grant_id = (server2client, client_id, sequence_num)
#            grant_id = (server2client, client_id, sequence_num, offset)
            grant = grants[grant_id] if grant_id in grants else None
            is_complete = True
            if grant is None:
                is_complete = False
                grant = Grant(grant_id)
                grants[grant_id] = grant
            if is_sender:
                grant.sender = host
                grant.tx_timestamp = timestamp
            else:
                grant.receiver = host
                grant.rx_timestamp = timestamp

            if is_complete:
                tx_rx = (grant.sender, grant.receiver)
                tsc_diff = grant.rx_timestamp - grant.tx_timestamp
                if tx_rx not in timestamp_diffs:
                    timestamp_diffs[tx_rx] = []
                timestamp_diffs[tx_rx].append(tsc_diff)

                # Done processing this grant. Remove it.
                grants.pop(grant_id)

print "hosts = %s" % hosts
#print "timestamp_diffs = %s" % timestamp_diffs

# Compute clock offsets between hosts
hosts = sorted(hosts)
clock_offset_table = {x : {y : None if x != y else 0 for y in hosts} for x in hosts}
for x, y in itertools.combinations(hosts, 2):
    if (x, y) in timestamp_diffs and (y, x) in timestamp_diffs:
        min_tx_diff = min(timestamp_diffs[(x,y)])
        min_rx_diff = min(timestamp_diffs[(y,x)])
        offset = (min_rx_diff - min_tx_diff) / 2
        clock_offset_table[x][y] = offset
        clock_offset_table[y][x] = -offset

        print min_tx_diff, min_rx_diff
        one_way_delay = min_tx_diff + offset
        print "synchronizing %s and %s: delta = %.2f, one-way-delay = %.2f" % (x, y, offset, one_way_delay)
        assert one_way_delay > 0
# TODO: HOW TO RECONCILE THE DESCREPENCIES INSIDE THE TABLE?
for x, y in itertools.combinations(hosts, 2):
    if clock_offset_table[x][y] is None:
        # No enough direct communication between x and y
        for z in set(hosts) - set([x, y]):
            if not clock_offset_table[x][z] or not clock_offset_table[z][y]:
                continue
            offset = clock_offset_table[x][z] + clock_offset_table[z][y]
            clock_offset_table[x][y] = offset
            clock_offset_table[y][x] = -offset
            print "synchronizing %s and %s using %s: %.2f" % (x, y, z, offset)

print clock_offset_table

# Rewrite the timestamps of all timetraces
# Step 1: TODO
reference_host = min(hosts, key=lambda x: clock_offset_table[hosts[0]][x])
earliest_timestamps = []
for fname in glob.glob("*.%s.log" % reference_host):
    with open(fname, 'r') as f:
        earliest_timetrace_record = next(l for l in f.readlines() if TIMETRACE_MSG_REGEX.match(l))
        earliest_timestamps.append(float(TIMETRACE_MSG_REGEX.match(earliest_timetrace_record).group(1)))
timestamp_base = min(earliest_timestamps)
print "reference_host %s, timestamp_base %.2f" % (reference_host, timestamp_base)

for fname in glob.glob("*.log"):
    match_result = LOG_FILE_REGEX.match(fname)
    if not match_result:
        continue
    print "Rewriting timetraces in %s..." % fname

    host = match_result.group(2)

    prev_timestamp = None
    with open(fname, 'r') as f:
        with open(fname[:-3] + "trace", 'w') as out:
            for line in f.readlines():
                match_result = TIMETRACE_MSG_REGEX.match(line)
                if not match_result:
                    continue

                timestamp = float(match_result.group(1))
                log_msg = match_result.group(2)

                adjusted_timestamp = timestamp + clock_offset_table[reference_host][host] - timestamp_base
                if prev_timestamp is None:
                    prev_timestamp = adjusted_timestamp
                delta = adjusted_timestamp - prev_timestamp
                prev_timestamp = adjusted_timestamp
                out.write("%7.2f us (+%7.1f ns): %s\n" % (adjusted_timestamp / 1000, delta, log_msg))

