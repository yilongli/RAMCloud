#!/usr/bin/env bash

# Example usage:
# ./get_ib_rounting_path > ib-routing.txt

machine_lids=`ibnetdiscover | egrep -o "rc[0-9]+.*lid [0-9]+" | cut -d' ' -f1,4 | sort`
while read src_host src_lid; do
	while read dest_host dest_lid; do
		echo $src_host $dest_host
		ibtracert --Lid $src_lid $dest_lid
		echo
	done < <(echo "$machine_lids")
done < <(echo "$machine_lids")