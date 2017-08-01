#!/bin/bash
WORKLOAD=Facebook_HadoopDist_All.txt
scripts/clusterperf.py --superuser --dpdkPort 1 --replicas 0 --disjunct --transport basic+dpdk --servers 1 --clients 1 --messageSizeCdfFile $WORKLOAD --verbose echo_basic
grep "minimum" logs/latest/client1.*.log > basic_W4_min_baseline.txt
scripts/clusterperf.py --superuser --dpdkPort 1 --replicas 0 --disjunct --transport homa+dpdk --servers 1 --clients 1 --messageSizeCdfFile $WORKLOAD --verbose echo_basic
grep "minimum" logs/latest/client1.*.log > homa_W4_min_baseline.txt
diff -y *_baseline.txt > W4_min_baseline.txt
