#!/bin/bash

scripts/clusterperf.py -r 0 --transport basic+dpdk --dpdkPort 1 --servers 4 --superuser millisort --verbose --nodesPerPivotServer 10 --dataTuplesPerNode 1000 --count 10
