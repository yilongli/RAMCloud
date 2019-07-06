#!/usr/bin/env bash
start_time=`grep "TimeTrace.cc:184" -m 1 server*.log | cut -d: -f4 | cut -d' ' -f3 | sort -n | head -n 1`
for server_log in server*log; do
	`dirname "$0"`/ttgrep.py "" $server_log $start_time > tt_$server_log
done