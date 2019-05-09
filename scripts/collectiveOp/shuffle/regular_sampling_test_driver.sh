#!/usr/bin/env bash

# for dist in {0..2}; do for pivots in {100..1200..100}; do ./regular_sampling_test_driver.sh 10000 600 10000 $pivots $dist; done; done

num_simulations=$1
num_nodes=$2
num_records_per_node=$3
num_pivots=$4
dist=$5

sim_id=simulation_${num_nodes}_${num_records_per_node}_${num_pivots}_${dist}
work_dir=/tmp/$sim_id
mkdir -p $work_dir

num_cpus=`lscpu | grep -m1 "CPU(s):" | awk '{print $2}'`
num_sims_per_core=$(($num_simulations/$num_cpus+1))
for (( cpu = 0; cpu < ${num_cpus}; cpu++ )); do
	args="$num_sims_per_core $num_nodes $num_records_per_node $num_pivots $dist"
	./regularSamplingQualTest.py $args >> $work_dir/cpu$cpu.out &
done
wait

sort -n $work_dir/cpu* > $sim_id.data
awk -v totaldata=`cat $sim_id.data | wc -l` '{cnt++; print $1, cnt/totaldata}' $sim_id.data > $sim_id.cdf