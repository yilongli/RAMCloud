#!/bin/bash
HOMA_EVAL=homa_eval
WORKLOAD_TYPE=w4
WORKLOAD=${WORKLOAD_TYPE}_cdf.txt
scripts/clusterperf.py --superuser --dpdkPort 1 --replicas 0 --disjunct --transport basic+dpdk --servers 1 --clients 1 --messageSizeCdfFile $WORKLOAD --timeout 10000 --verbose echo_basic
grep "minimum" logs/latest/client1.*.log > ${HOMA_EVAL}/basic_${WORKLOAD_TYPE}_baseline.txt
scripts/clusterperf.py --superuser --dpdkPort 1 --replicas 0 --disjunct --transport homa+dpdk --servers 1 --clients 1 --messageSizeCdfFile $WORKLOAD --timeout 10000 --verbose echo_basic
grep "minimum" logs/latest/client1.*.log > ${HOMA_EVAL}/homa_${WORKLOAD_TYPE}_baseline.txt
diff -y *_baseline.txt > ${WORKLOAD_TYPE}_baseline.txt

LOAD_FACTOR=48
scripts/clusterperf.py --superuser --dpdkPort 1 --replicas 0 --disjunct --transport basic+dpdk --servers 8 --clients 8 --messageSizeCdfFile $WORKLOAD --seconds 30 --loadFactor 0.${LOAD_FACTOR} --verbose echo_workload > ${HOMA_EVAL}/basic_${WORKLOAD_TYPE}_${LOAD_FACTOR}_experiment.txt
scripts/clusterperf.py --superuser --dpdkPort 1 --replicas 0 --disjunct --transport homa+dpdk --servers 8 --clients 8 --messageSizeCdfFile $WORKLOAD --seconds 30 --loadFactor 0.${LOAD_FACTOR} --verbose echo_workload > ${HOMA_EVAL}/homa_${WORKLOAD_TYPE}_${LOAD_FACTOR}_experiment.txt
scripts/homa_eval/proc_data.py basic $WORKLOAD_TYPE $LOAD_FACTOR > ${HOMA_EVAL}/basic_${WORKLOAD_TYPE}_${LOAD_FACTOR}_slowdown.txt
scripts/homa_eval/proc_data.py homa $WORKLOAD_TYPE $LOAD_FACTOR > ${HOMA_EVAL}/homa_${WORKLOAD_TYPE}_${LOAD_FACTOR}_slowdown.txt
diff -y ${HOMA_EVAL}/*_${WORKLOAD_TYPE}_${LOAD_FACTOR}_slowdown.txt > ${HOMA_EVAL}/homa_vs_basic_${WORKLOAD_TYPE}_${LOAD_FACTOR}.txt

LOAD_FACTOR=76
scripts/clusterperf.py --superuser --dpdkPort 1 --replicas 0 --disjunct --transport basic+dpdk --servers 8 --clients 8 --messageSizeCdfFile $WORKLOAD --seconds 30 --loadFactor 0.${LOAD_FACTOR} --verbose echo_workload > ${HOMA_EVAL}/basic_${WORKLOAD_TYPE}_${LOAD_FACTOR}_experiment.txt
scripts/clusterperf.py --superuser --dpdkPort 1 --replicas 0 --disjunct --transport homa+dpdk --servers 8 --clients 8 --messageSizeCdfFile $WORKLOAD --seconds 30 --loadFactor 0.${LOAD_FACTOR} --verbose echo_workload > ${HOMA_EVAL}/homa_${WORKLOAD_TYPE}_${LOAD_FACTOR}_experiment.txt
scripts/homa_eval/proc_data.py basic $WORKLOAD_TYPE $LOAD_FACTOR > ${HOMA_EVAL}/basic_${WORKLOAD_TYPE}_${LOAD_FACTOR}_slowdown.txt
scripts/homa_eval/proc_data.py homa $WORKLOAD_TYPE $LOAD_FACTOR > ${HOMA_EVAL}/homa_${WORKLOAD_TYPE}_${LOAD_FACTOR}_slowdown.txt
diff -y ${HOMA_EVAL}/*_${WORKLOAD_TYPE}_${LOAD_FACTOR}_slowdown.txt > ${HOMA_EVAL}/homa_vs_basic_${WORKLOAD_TYPE}_${LOAD_FACTOR}.txt
