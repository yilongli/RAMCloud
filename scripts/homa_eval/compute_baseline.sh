#!/bin/bash
HOMA_EVAL=homa_eval
WORKLOAD_TYPE=w4
WORKLOAD=${WORKLOAD_TYPE}_cdf.txt
scripts/clusterperf.py --superuser --dpdkPort 1 --replicas 0 --disjunct --transport basic+dpdk --servers 1 --clients 1 --messageSizeCdfFile $WORKLOAD --timeout 10000 --verbose echo_basic
grep "minimum" logs/latest/client1.*.log > ${HOMA_EVAL}/basic_${WORKLOAD_TYPE}_baseline.txt
scripts/clusterperf.py --superuser --dpdkPort 1 --replicas 0 --disjunct --transport homa+dpdk --servers 1 --clients 1 --messageSizeCdfFile $WORKLOAD --timeout 10000 --verbose echo_basic
grep "minimum" logs/latest/client1.*.log > ${HOMA_EVAL}/homa_${WORKLOAD_TYPE}_baseline.txt
diff -y *_baseline.txt > ${WORKLOAD_TYPE}_baseline.txt
