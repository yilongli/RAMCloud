#!/usr/bin/env bash

num_simulations=1
num_nodes=600
num_records_per_node=10000
num_pivots=600
dist=0

work_dir=simulation_${num_nodes}_${num_records_per_node}_${num_pivots}_${dist}
rm -rf $work_dir
mkdir $work_dir
for cpu in {0..30}; do
	args="$num_simulations $num_nodes $num_records_per_node $num_pivots $dist"
	./regularSamplingQualTest.py $args > $work_dir/cpu$cpu.out &
done