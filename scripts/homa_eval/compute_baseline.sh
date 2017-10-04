#!/bin/bash
HOMA_EVAL=homa_eval
scripts/clusterperf.py --replicas 0 --disjunct --transport infrc --servers 1 --clients 1 --messageSizeCdfFile w3_cdf.txt --timeout 10000 --verbose echo_basic
grep "minimum" logs/latest/client1.*.log > ${HOMA_EVAL}/infrc_w3_baseline.txt
scripts/clusterperf.py --replicas 0 --disjunct --transport infrc --servers 1 --clients 1 --messageSizeCdfFile w4_cdf.txt --timeout 10000 --verbose echo_basic
grep "minimum" logs/latest/client1.*.log > ${HOMA_EVAL}/infrc_w4_baseline.txt
scripts/clusterperf.py --replicas 0 --disjunct --transport infrc --servers 1 --clients 1 --messageSizeCdfFile w5_cdf.txt --timeout 10000 --verbose echo_basic
grep "minimum" logs/latest/client1.*.log > ${HOMA_EVAL}/infrc_w5_baseline.txt