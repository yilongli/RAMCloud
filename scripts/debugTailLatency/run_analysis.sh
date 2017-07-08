#!/bin/bash
SCRIPT_DIR="/shome/RAMCloud/scripts/debugTailLatency"

echo $(date -u) "Synchronizing timestamps"
$SCRIPT_DIR/sync-timetraces.py

echo $(date -u) "Computing per-packet delays"
dir=pkt_delay
mkdir ${dir}
$SCRIPT_DIR/per-packet-delay.py merged.tt > ${dir}/all_tx_delay.txt &
$SCRIPT_DIR/per-packet-delay.py --orderByRx merged.tt > ${dir}/all_rx_delay.txt &
wait

# Example delay message:
# client1 -> server1 | 7.86 (+ 6.40)  5.47 (2139013751, 71122, 271950, 1470)
awk_script='BEGIN {p=0} {delta=sprintf("%.2f)", $5-p); sub(".*", delta, $7); print $0; p=$5;}'
for logfile in *.log; do
    [[ $logfile == coordinator* ]] && continue
    who=$(echo $logfile | cut -d . -f 1)
    # Grep delay messages from a specific sender and rewrite the timestamp delta
    grep "$who ->" ${dir}/all_tx_delay.txt | awk "$awk_script" > ${dir}/${who}_tx_delay.txt &
    grep " -> $who" ${dir}/all_rx_delay.txt | awk "$awk_script" > ${dir}/${who}_rx_delay.txt &
done

# Debugging network utilization:
# 1) extract performance monitoring messages generated periodically;
echo $(date -u) "Extracting performance monitoring messages"
for tt_file in *.tt; do
    [[ $tt_file == merged* ]] && continue
    who=$(echo $tt_file | cut -d . -f 1)
    egrep "data packets goodput [1-9]" -A 5 $tt_file > perfMon.$who.txt &
done

# TODO: Document why?
egrep -o "run out of grants [1-9][0-9]* times" perfMon.* | sort -nr -k 5 > goodputWastedByLateGrants.txt
cut -d: -f 1 goodputWastedByLateGrants.txt | sort | uniq -c >> goodputWastedByLateGrants.txt

# 2) extract messages useful in explaining bubbles passed by senders.
echo $(date -u) "Finding late grants"
roundTripBytes=$((1470*7))
find_late_grants='/not enough/{fst=1} /sent data/{ if (fst && $13 >'$roundTripBytes' ) printf "%s %s %s,\n", $9, $11, $13-'$roundTripBytes'; fst=0 }'
for tt_file in *.tt; do
    [[ $tt_file == merged* ]] && continue
    who=$(echo $tt_file | cut -d . -f 1)
    egrep "sent data|sent (GRANT|control).*idle time [1-9]|not enough GRANTs" $tt_file | \
        awk -f $SCRIPT_DIR/fix_tt_delta.awk > sentData.$who.txt

    # Find late GRANTs that are blocking the sender
    awk "$find_late_grants" sentData.$who.txt > lateGrantId.$who.txt
    egrep -f lateGrantId.$who.txt pkt_delay/${who}_tx_delay.txt > lateGrantRTT.$who.txt
    #rm lateGrantId.$who.txt
    # TODO: it's too complex to find correspond "sending GRANT" messages on the receiver side and insert them
    # back to sendData.$who.txt using bash + grep + awk. Need to find out who is the receiver, whether this
    # is a request or response message, etc.
done

echo $(date -u) "Done"
exit

# Extract RPCs that experienced high latency.
$SCRIPT_DIR/badTailLatencies.sh merged.tt > bad_tail_latency.txt

# Summarize the cost of calling Driver::receivedPackets
grep -h "start of polling" -A 1 server*.tt | \
    grep "server received" | \
    awk '{s+=$4;c++;print} END {print s/c, c}' > receivedPackets_cost.txt

# Summarize the server turnaround time for small RPCs
grep -h "server received ALL_DATA" -A 1 server*.tt |
    grep "sendReply invoked.*length [0-9]\{2\}$" | \
    awk '{s+=$4;c++;print} END {print s/c, c}' > server_rpc_turnaround.txt
