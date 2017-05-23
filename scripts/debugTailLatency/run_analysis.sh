#!/bin/bash
SCRIPT_DIR="/shome/RAMCloud/scripts/debugTailLatency"
$SCRIPT_DIR/sync-timetraces.py

dir=pkt_delay
mkdir ${dir}
$SCRIPT_DIR/per-packet-delay.py merged.tt > ${dir}/all_tx_delay.txt &
$SCRIPT_DIR/per-packet-delay.py --orderByRx merged.tt > ${dir}/all_rx_delay.txt &
wait

# Example delay message:
# client1 -> server1 | 7.86 (+ 6.40)  5.47 (2139013751, 71122, 271950, 1470)
awk_script='BEGIN {p=0} {delta=sprintf("%.2f)", $5-p); sub(".*", delta, $7); print $0; p=$5;}'
for host in client1 client2 server1 server2; do
    # Grep delay messages from a specific sender and rewrite the timestamp delta
    grep "$host ->" ${dir}/all_tx_delay.txt | awk "$awk_script" > ${dir}/${host}_tx_delay.txt &
    grep " -> $host" ${dir}/all_rx_delay.txt | awk "$awk_script" > ${dir}/${host}_rx_delay.txt &
done
$SCRIPT_DIR/badTailLatencies.sh merged.tt > bad_tail_latency.txt
