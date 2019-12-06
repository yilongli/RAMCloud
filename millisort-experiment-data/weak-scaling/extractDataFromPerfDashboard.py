#!/usr/bin/env python3

import sys
import statistics

if len(sys.argv) == 2:
    client_log = sys.argv[1]
else:
    print('Usage: extractDataFromPerfDashboard.py <client-log>')
    exit(0)

record_table = {}

node_range_set = set()
data_range_set = set()

num_nodes = -1
num_items_per_node = -1
run_id = -1

with open(client_log, 'r') as file:
    for line in file:
        if line.startswith("Experiment ID = "):
            run_id = int(line.split()[-1])
            continue
        if line.startswith("numNodes = "):
            num_nodes = int(line.split()[-1])
            node_range_set.add(num_nodes)
            continue
        if line.startswith("numItemsPerNode = "):
            num_items_per_node = int(line.split()[-1])
            data_range_set.add(num_items_per_node)
            continue
        if line.startswith("  Total time (us)") or \
                line.startswith("  Local sorting") or \
                line.startswith("  Rearrange (overlap)") or \
                line.startswith("  Partitioning (overall)") or \
                line.startswith("    Shuffle pivots") or \
                line.startswith("  Shuffle records") or \
                line.startswith("  MergeSort + Rearrange") or \
                line.startswith("    MergeSort (us)"):
            words = line.split()
            key = (num_nodes, num_items_per_node, run_id,
                   words[0] + ' ' + words[1])
            record_table[key] = [float(x) for x in line.split()[-num_nodes:]]

print(f'{"nodes":>12} {"records":>12} {"total (M)":>12} {"min":>12} {"max":>12} {"p50":>12} '
      f'{"p90":>12} {"localSort":>12} {"rearrange1":>12} {"partition":>12} {"shuffleRecs":>12} '
      f'{"rearrange2":>12} {"mergeSort":>12} {"shufflePiv":>12} '
      f'{"records/ms":>12} {"sortImbal":>12} {"partImbal":>12} '
      f'{"shufPivImbal":>12} {"shufRecImbal":>12} {"rearrImbal":>12} '
      f'{"runID (p50)":>12}')

for num_items_per_node in sorted(data_range_set):
    for num_nodes in sorted(node_range_set):
        total_times = []
        max_run_id = 0
        for run_id in range(1000):
            max_run_id = run_id
            server_total_times = record_table.get(
                (num_nodes, num_items_per_node, run_id, "Total time"), None)
            if server_total_times is None:
                break
            else:
                total_times.append((max(server_total_times), run_id))
        total_times.sort()
        min_total_time, min_run_id = total_times[0]
        max_total_time, max_run_id = total_times[-1]
        p50_total_time, p50_run_id = total_times[int(len(total_times) * 0.50)]
        p90_total_time, p90_run_id = total_times[int(len(total_times) * 0.90)]
        # p95_total_time, p95_run_id = total_times[int(len(total_times) * 0.95)]

        # run_id = min_run_id
        run_id = p50_run_id
        # run_id = p90_run_id
        local_sort_time = \
            max(record_table[(num_nodes, num_items_per_node, run_id, "Local sorting")])
        local_sort_imbalance = \
            max(record_table[(num_nodes, num_items_per_node, run_id, "Local sorting")]) / \
            min(record_table[(num_nodes, num_items_per_node, run_id, "Local sorting")])
        rearrange1_time = \
            max(record_table[(num_nodes, num_items_per_node, run_id, "Rearrange (overlap)")])
        partition_time = \
            max(record_table[(num_nodes, num_items_per_node, run_id, "Partitioning (overall)")])
        partition_imbalance = \
            max(record_table[(num_nodes, num_items_per_node, run_id, "Partitioning (overall)")]) / \
            min(record_table[(num_nodes, num_items_per_node, run_id, "Partitioning (overall)")])
        shuffle_pivots_time = \
            max([x for x in record_table[(num_nodes, num_items_per_node, run_id, "Shuffle pivots")] if x > 0])
        shuffle_pivots_imbalance = \
            max(record_table[(num_nodes, num_items_per_node, run_id, "Shuffle pivots")]) / \
            min([x for x in record_table[(num_nodes, num_items_per_node, run_id, "Shuffle pivots")] if x > 0])
        shuffle_records_time = \
            max(record_table[(num_nodes, num_items_per_node, run_id, "Shuffle records")])
        shuffle_records_imbalance = \
            max(record_table[(num_nodes, num_items_per_node, run_id, "Shuffle records")]) / \
            min(record_table[(num_nodes, num_items_per_node, run_id, "Shuffle records")])
        rearrange2_time = \
            max(record_table[(num_nodes, num_items_per_node, run_id, "MergeSort +")])
        try:
            rearrange2_imbalance = \
                max(record_table[(num_nodes, num_items_per_node, run_id, "MergeSort +")]) / \
                min(record_table[(num_nodes, num_items_per_node, run_id, "MergeSort +")])
        except ZeroDivisionError:
            rearrange2_imbalance = float('Inf')
        mergesort_time = \
            max(record_table[(num_nodes, num_items_per_node, run_id, "MergeSort (us)")])

        print(f'{num_nodes:12} {num_items_per_node:12} {num_nodes*num_items_per_node*1e-6:12.2f} '
              f'{min_total_time:12.2f} {max_total_time:12.2f} '
              f'{p50_total_time:12.2f} {p90_total_time:12.2f} '
              f'{local_sort_time:12.2f} {rearrange1_time:12.2f} '
              f'{partition_time:12.2f} {shuffle_records_time:12.2f} '
              f'{rearrange2_time:12.2f} {mergesort_time:12.2f} '
              f'{shuffle_pivots_time:12.2f} {num_items_per_node/p50_total_time*1e3:12.2f} '
              f'{local_sort_imbalance:12.2f} {partition_imbalance:12.2f} '
              f'{shuffle_pivots_imbalance:12.2f} '
              f'{shuffle_records_imbalance:12.2f} {rearrange2_imbalance:12.2f} '
              f'{run_id:12}')
