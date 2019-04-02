#!/usr/bin/env bash

# Extract from server logs all timetrace entries between a number of runs.
#
# Usage: run.sh num_servers first_run last_run

num_servers=$1
run1=$2
run2=$3
for (( i = 1; i <= $num_servers; i++ )); do
    start_line_num=`grep "started, run $run1" -n -m 1 server$i.*.log | cut -f1 -d:`
    end_line_num=`grep "run $run2 completed" -n -m 1 server$i.*.log | cut -f1 -d:`
    head -n $end_line_num server$i.*.log | tail -n +$start_line_num > temp.txt
    ../../scripts/ttgrep.py "" temp.txt > server$i.tt
done
rm temp.txt