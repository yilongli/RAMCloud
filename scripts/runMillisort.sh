#!/bin/bash

scripts/clusterperf.py -r 0 --disjunct --transport basic+dpdk --dpdkPort 1 --servers 4 --superuser millisort --verbose --nodesPerPivotServer 10 --dataTuplesPerNode 9550 --count 10

scripts/clusterperf.py -r 0 --disjunct --transport basic+dpdk --dpdkPort 1 --servers 8 --superuser allShuffle --verbose --size 100000 --count 100
scripts/clusterperf.py -r 0 --disjunct --transport basic+dpdk --dpdkPort 1 --servers 2 --superuser allShuffle --verbose --size 8000000 --count 10

# Run the treeBcast benchmark with message size <msg-size>; sweep # nodes to broadcast from 2 to <num-servers>; repeat each experiment <num-runs> times
scripts/clusterperf.py -r 0 --disjunct --transport basic+dpdk --dpdkPort 1 --servers <num-servers> --superuser treeBcast --verbose --count <num-runs> --size <msg-size>

# --cpusPerServer fixed # cores per server; used on POD cluster to avoid interference between MilliSort nodes on the same phys. machine
--disjunct --cpusPerServer 8
