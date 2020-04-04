#!/usr/bin/env python3

import sys
from statistics import mean

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
                line.startswith("    MergeSort (us)") or \
                line.startswith("  Online merge-sort (us)") or \
                line.startswith("  Rearrange final values (us)"):
            words = line.split()
            key = (num_nodes, num_items_per_node, run_id,
                   words[0] + ' ' + words[1])
            record_table[key] = [float(x) for x in line.split('|')[1].split()[:num_nodes]]

print(f'{"nodes":>12} {"records":>12} {"total(M)":>12} {"min":>12} {"max":>12} {"p50":>12} '
      f'{"p90":>12} {"localSort":>12} {"rearrange1":>12} {"partition":>12} {"shuffleRecs":>12} '
      f'{"rearrange2":>12} {"mergeSort":>12} {"shufflePiv":>12} '
      f'{"records/ms":>12} {"runID(p50)":>12} {"sortImbal":>12} {"partImbal":>12} '
      f'{"shufPivImbal":>12} {"shufRecImbal":>12} {"rearrImbal":>12} '
      f'{"shuffleGbps":>12}')

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

        run_id = p50_run_id

        # To compute the cluster-wise elapsed time of each phase, use the
        # average from runs with total time that is less than 1% different
        # from the p50 run.
        run_ids = [run_id for t, run_id in total_times if abs(1 - t / p50_total_time) <= 0.01]

        def compute_elapsed_time(str_phase):
            return mean([max(record_table[(num_nodes, num_items_per_node, run_id, str_phase)]) for run_id in run_ids])

        def compute_imbalance(str_phase):
            key = (num_nodes, num_items_per_node, run_id, str_phase)
            return max(record_table[key]) / min([x for x in record_table[key] if x > 0])

        local_sort_time = compute_elapsed_time("Local sorting")
        local_sort_imbalance = compute_imbalance("Local sorting")
        rearrange1_time = compute_elapsed_time("Rearrange (overlap)")
        partition_time = compute_elapsed_time("Partitioning (overall)")
        partition_imbalance = compute_imbalance("Partitioning (overall)")
        shuffle_pivots_time = compute_elapsed_time("Shuffle pivots")
        shuffle_pivots_imbalance = compute_imbalance("Shuffle pivots")
        shuffle_records_time = compute_elapsed_time("Shuffle records")
        shuffle_records_imbalance = compute_imbalance("Shuffle records")
        rearrange2_time = compute_elapsed_time("MergeSort +")
        # Computing max/min for the final rearranging step requires switching
        # to use individual servers' elapsed time; the reason is that some
        # servers may finish rearrangement early even before some other servers
        # start (after all, final rearrangement is not a collective operation),
        # which results in unreasonably large imbalance ratio.
        rearrange2_indiv = [sum(x) for x in zip(
            record_table[(num_nodes, num_items_per_node, run_id, "Online merge-sort")],
            record_table[(num_nodes, num_items_per_node, run_id, "Rearrange final")])]
        rearrange2_imbalance = max(rearrange2_indiv) / min(rearrange2_indiv)
        # try:
        #     rearrange2_imbalance = \
        #         max(record_table[(num_nodes, num_items_per_node, run_id, "MergeSort +")]) / \
        #         min(record_table[(num_nodes, num_items_per_node, run_id, "MergeSort +")])
        # except ZeroDivisionError:
        #     rearrange2_imbalance = float('Inf')
        mergesort_time = compute_elapsed_time("MergeSort (us)")

        print(f'{num_nodes:12} {num_items_per_node:12} {num_nodes*num_items_per_node*1e-6:12.2f} '
              f'{min_total_time:12.2f} {max_total_time:12.2f} '
              f'{p50_total_time:12.2f} {p90_total_time:12.2f} '
              f'{local_sort_time:12.2f} {rearrange1_time:12.2f} '
              f'{partition_time:12.2f} {shuffle_records_time:12.2f} '
              f'{rearrange2_time:12.2f} {mergesort_time:12.2f} '
              f'{shuffle_pivots_time:12.2f} {num_items_per_node/p50_total_time*1e3:12.2f} '
              f'{run_id:12} '
              f'{local_sort_imbalance:12.2f} {partition_imbalance:12.2f} '
              f'{shuffle_pivots_imbalance:12.2f} '
              f'{shuffle_records_imbalance:12.2f} {rearrange2_imbalance:12.2f} '
              f'{num_items_per_node*106*8*1e-3/shuffle_records_time:12.2f}')
