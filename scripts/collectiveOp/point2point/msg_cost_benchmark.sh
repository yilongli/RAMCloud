#!/usr/bin/env bash

# Usage: run.sh connection_type min_size max_size size_interval server
# Example:
# 	msg_cost_benchmark.sh UD 1 4096 10 rc02

connection_type=$1
min_size=$2
max_size=$3
interval=$4
server=$5
for (( size = $min_size; size <= $max_size; size += interval )); do
	command_args="--connection $connection_type --size $size --iters 1000"
	if [ $server == $(hostname -s) ]
	then
		ib_send_lat $command_args
	else
		ib_send_lat $command_args $server | egrep "^ [0-9]+"
		sleep 0.01
	fi
done