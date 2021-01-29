#!/bin/bash

scripts/clusterperf.py -r 0 --disjunct --transport basic+dpdk --dpdkPort 1 --servers 4 --superuser millisort --verbose --nodesPerPivotServer 10 --dataTuplesPerNode 9550 --count 10

scripts/clusterperf.py -r 0 --disjunct --transport basic+dpdk --dpdkPort 1 --servers 8 --superuser allShuffle --verbose --size 100000 --count 100
scripts/clusterperf.py -r 0 --disjunct --transport basic+dpdk --dpdkPort 1 --servers 2 --superuser allShuffle --verbose --size 8000000 --count 10

# Run the treeBcast benchmark with message size <msg-size>; sweep # nodes to broadcast from 2 to <num-servers>; repeat each experiment <num-runs> times
scripts/clusterperf.py -r 0 --disjunct --transport basic+dpdk --dpdkPort 1 --servers <num-servers> --superuser treeBcast --verbose --count <num-runs> --size <msg-size>

# Run the allShuffle benchmark with message size <msg-size>; sweep # nodes to shuffle from 2 to <num-servers>; repeat each experiment <num-runs> times
scripts/clusterperf.py -r 0 --disjunct --transport basic+infud --servers <num-servers> allShuffle --verbose --count <num-runs> --size <msg-size>

# --cpusPerServer fixed # cores per server; used on POD cluster to avoid interference between MilliSort nodes on the same phys. machine
--disjunct --cpusPerServer 8

# POD 1-cpu build machine sanity test
scripts/clusterperf.py -r 0 --disjunct --transport basic+ofiud millisort --verbose --servers 2 --cpusPerServer 8 --count 10 --nodesPerPivotServer 10 --dataTuplesPerNode 10000

# POD submit millisort job
# echo_basic sanity test
./submit-clusterperf 3 1 clusterperf.py -r 0 --disjunct --transport basic+ofiud echo_basic --verbose --cpusPerServer 10

# p2p microbenchmark: sweep message size from 0B to 50000B
~./submit-clusterperf 2 2 clusterperf.py -r 0 --disjunct --transport basic+ofiud point2point --verbose --cpusPerServer 10 --servers 2 --count 100000 --size 50000

# broadcast microbenchmark: 10 servers + client + coordinator, 2 instances per node
./submit-clusterperf 6 2 clusterperf.py -r 0 --disjunct --transport basic+ofiud treeBcast --verbose --cpusPerServer 10 --servers 10 --count 10000 --size 100

# Small scale: 18 millisort nodes on 10 S30 machines
pod-scripts/submit-clusterperf 10 2 clusterperf.py -r 0 --transport basic+infud millisort --servers 18 --verbose --cpusPerServer 16 --nodesPerPivotServer 10 --dataTuplesPerNode 9550 --count 20

# Scalability test with small # MilliSort nodes (packing 8 millisort nodes per physical machine)
pod-scripts/submit-clusterperf 5 8 clusterperf.py -r 0 --transport basic+infud millisort --servers 38 --verbose --cpusPerServer 4 --nodesPerPivotServer 10 --dataTuplesPerNode 9550 --count 10
pod-scripts/submit-clusterperf 20 8 clusterperf.py -r 0 --transport basic+infud millisort --servers 158 --verbose --cpusPerServer 4 --nodesPerPivotServer 10 --dataTuplesPerNode 9550 --count 10
pod-scripts/submit-clusterperf 45 8 clusterperf.py -r 0 --transport basic+infud millisort --servers 358 --verbose --cpusPerServer 4 --nodesPerPivotServer 10 --dataTuplesPerNode 9550 --count 5

# Small MilliSort experiment used to do performance tuning after switching to push-based shuffle
./submit-clusterperf 10 1 clusterperf.py -r 0 --disjunct --transport basic+ofiud millisort --verbose --cpusPerServer 16 --servers 8 --count 10 --nodesPerPivotServer 10 --dataTuplesPerNode 100000
