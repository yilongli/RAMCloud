#!/usr/bin/env bash

# Extract the timeline of a specific pull RPC.
#
# Usage: get_pull_timeline.sh from_rank to_rank

from_rank=$1
to_rank=$2
logs_dir=logs
trace_file1=$logs_dir/server$((from_rank+1)).tt
trace_file2=$logs_dir/server$((to_rank+1)).tt
rpc_id=`grep "to rank $to_rank completed" -B 5 $trace_file1 | grep "RX throughput" | egrep "clientId.*seq [0-9]+" -o`

temp_file=/tmp/temp.txt
echo $trace_file1
egrep "to rank $to_rank$|$rpc_id" $trace_file1 > $temp_file
../../ttfix.py "" $temp_file
echo $trace_file2
egrep "from rank $from_rank$|$rpc_id" $trace_file2 > $temp_file
../../ttfix.py "" $temp_file
