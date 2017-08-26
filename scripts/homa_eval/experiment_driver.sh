#!/bin/bash

# Always invoke this script from the top-level RAMCloud directory.
[[ ! -f scripts/clusterperf.py ]] &&
        printf "Please cd to the top RAMCloud directory!\n" && exit
out_dir=.homa_playground


# Set absolute paths to files.
workload=w4
workload_cdf=`pwd`/${workload}_cdf.txt
baseline_data=`pwd`/homa_eval/${workload}_baseline.txt  # Do not change; hardcoded in compute_slowdown.py
config_file=`pwd`/config/transport.txt                  # Do not change; hardcoded in HomaTransport.cc
slowdown_data=`pwd`/$out_dir/slowdownImpl.txt           # Do not change; hardcoded in slowdownImpl.r

compute_slowdown_cmd=`pwd`/scripts/homa_eval/proc_data.py
plot_slowdown=`pwd`/scripts/homa_eval/slowdownImpl.r

# Test our path settings for prerequesite files.
[[ ! -f $workload_cdf ]] &&
    printf "Wordload cdf file not found: $workload_cdf\n" && exit
[[ ! -f $baseline_data ]] &&
    printf "Baseline data file not found: $baseline_data\n" && exit
[[ ! -f $compute_slowdown_cmd ]] &&
    printf "Script not found: $compute_slowdown_cmd\n" && exit
[[ ! -f $plot_slowdown ]] &&
    printf "Script not found: $plot_slowdown\n" && exit

# Encode different experiment settings (ordered roughly in decreasing slowdown of short messages).
plot_label=(TCP Basic HomaP1O7 HomaP1O3 HomaP2O7 HomaP2O3 HomaP4O7 HomaP4O3 HomaP8O7 HomaP8O3)
protocol=(tcp basic homa homa homa homa homa homa homa homa)
driver=("" dpdk dpdk dpdk dpdk dpdk dpdk dpdk dpdk dpdk)
params=("" ""
        "rttMicros=8,unschedPrio=1,degreeOC=7,dpdkPrio=1"
        "rttMicros=8,unschedPrio=1,degreeOC=3,dpdkPrio=1"
        "rttMicros=8,unschedPrio=1,degreeOC=7,dpdkPrio=2"
        "rttMicros=8,unschedPrio=1,degreeOC=3,dpdkPrio=2"
        "rttMicros=8,unschedPrio=1,degreeOC=7,dpdkPrio=4"
        "rttMicros=8,unschedPrio=1,degreeOC=3,dpdkPrio=4"
        "rttMicros=8,unschedPrio=1,degreeOC=7,dpdkPrio=8"
        "rttMicros=8,unschedPrio=1,degreeOC=3,dpdkPrio=8")

# Initialize the output directory and start the experiments
rm -rf $out_dir; mkdir $out_dir
echo > $slowdown_data
cp $baseline_data $out_dir

T=30    # Run experiment for T seconds
N=8     # Run experiment with N clients and N servers
for (( i = 0; i < ${#protocol[@]}; i++ )); do
    transport=${protocol[i]}
    [[ ! -z ${driver[i]} ]] && transport+=+${driver[i]}
    echo $transport:${params[i]} > $config_file

    for load_factor in 48 76; do
        printf "Running experiment (%s, %s, %s)\n" "${plot_label[i]}" "$workload" "$load_factor"
        profile=${plot_label[i]}_${workload}_${load_factor}
        scripts/clusterperf.py --superuser --dpdkPort 1 --replicas 0 --disjunct --transport $transport --servers $N --clients $N --messageSizeCdfFile $workload_cdf --seconds $T --loadFactor 0.$load_factor --verbose echo_workload > $out_dir/${profile}_experiment.txt
        cp logs/latest/client1*.log $out_dir/client1.$profile.log
        cd $out_dir; $compute_slowdown_cmd ${plot_label[i]} $workload $load_factor >> $slowdown_data; cd ..
    done
done
cd $out_dir; Rscript $plot_slowdown; cd ..
