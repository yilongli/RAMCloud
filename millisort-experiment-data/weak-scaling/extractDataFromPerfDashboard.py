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
                line.startswith("  Partitioning (overall)") or \
                line.startswith("    Shuffle pivots") or \
                line.startswith("  Shuffle keys") or \
                line.startswith("  Shuffle values") or \
                line.startswith("  Online merge-sort") or \
                line.startswith("  Rearrange final"):
            words = line.split()
            key = (num_nodes, num_items_per_node, run_id,
                   words[0] + ' ' + words[1])
            record_table[key] = [float(x) for x in line.split()[-num_nodes:]]

print(f'{"nodes":>12} {"records":>12} {"min":>12} {"max":>12} {"p50":>12} '
      f'{"p90":>12} {"localSort":>12} {"partition":>12} {"shuffleKeys":>12} '
      f'{"shuffleVals":>12} {"mergeSort":>12} {"rearrange":>12} '
      f'{"records/ms":>12} {"shufflePiv":>12} '
      f'{"PartRatio":>12} {"SPRatio":>12} ' 
      f'{"SKRatio":>12} {"RearrRatio":>12} ' 
      f'{"runID (p50)":>12}')

for num_items_per_node in sorted(data_range_set):
    for num_nodes in sorted(node_range_set):
        # Take into account the merge-sort time that is currently not
        # counted towards the total time in the perf. dashboard.
        if num_items_per_node > 100*1000:
            est_cost_per_item = 7
        elif num_items_per_node > 10*1000:
            est_cost_per_item = 10
        else:
            est_cost_per_item = 11
        local_sort_time = est_cost_per_item * num_items_per_node * 1e-3;
        # local_sort_time = record_table[(num_nodes,
        #        num_items_per_node, 10, "Local sorting")][0]

        total_times = []
        max_run_id = 0
        for run_id in range(1000):
            max_run_id = run_id
            server_total_times = record_table.get(
                (num_nodes, num_items_per_node, run_id, "Total time"), None)
            if server_total_times is None:
                break
            else:
                # Adjust the local sort time model
                old_local_sort_time = \
                    statistics.mean(record_table[(num_nodes, num_items_per_node, run_id, "Local sorting")])
                fix_local_sort_time = local_sort_time - old_local_sort_time
                merge_sort_time = statistics.median(record_table[(num_nodes,
                       num_items_per_node, run_id, "Online merge-sort")])
                merge_sort_time = min(local_sort_time, merge_sort_time)
                # print(merge_sort_time)
                total_times.append((max(server_total_times) + fix_local_sort_time + merge_sort_time, run_id))
        total_times.sort()
        min_total_time, min_run_id = total_times[0]
        max_total_time, max_run_id = total_times[-1]
        p50_total_time, p50_run_id = total_times[int(len(total_times) * 0.50)]
        p90_total_time, p90_run_id = total_times[int(len(total_times) * 0.90)]
        # p95_total_time, p95_run_id = total_times[int(len(total_times) * 0.95)]

        # for metric in ["Partitioning (overall)", "Shuffle pivots", "Shuffle keys", "Rearrange final"]:
        #     values = []
        #     ratios = []
        #     for run_id in range(max_run_id + 1):
        #         if metric == "Partitioning (overall)":
        #             value = statistics.mean(record_table[(num_nodes, num_items_per_node, run_id, "Partitioning (overall)")])
        #             ratio = max(record_table[(num_nodes, num_items_per_node, run_id, "Partitioning (overall)")]) / \
        #                     min(record_table[(num_nodes, num_items_per_node, run_id, "Partitioning (overall)")])
        #         elif metric == "Shuffle pivots":
        #             value = \
        #                 statistics.mean([x for x in record_table[(num_nodes, num_items_per_node, run_id, "Shuffle pivots")] if x > 0])
        #             ratio = \
        #                 max(record_table[(num_nodes, num_items_per_node, run_id, "Shuffle pivots")]) / \
        #                 min([x for x in record_table[(num_nodes, num_items_per_node, run_id, "Shuffle pivots")] if x > 0])
        #         elif metric == "Shuffle keys":
        #             value = \
        #                 statistics.mean(record_table[(num_nodes, num_items_per_node, run_id, "Shuffle keys")])
        #             ratio = \
        #                 max(record_table[(num_nodes, num_items_per_node, run_id, "Shuffle keys")]) / \
        #                 min(record_table[(num_nodes, num_items_per_node, run_id, "Shuffle keys")])
        #         else:
        #             rearrange_times = [x + local_sort_time for x in record_table[(num_nodes, num_items_per_node, run_id, "Rearrange final")]]
        #             value = statistics.mean(rearrange_times)
        #             ratio = max(rearrange_times) / min(rearrange_times)
        #         values.append(value)
        #         ratios.append(ratio)
        #
        #     values.sort()
        #     ratios.sort()
        #     print(f'{metric}: {statistics.median(values):.1f} {statistics.median(ratios):.2f}')

        # run_id = min_run_id
        run_id = p50_run_id
        # run_id = p90_run_id
        # local_sort_time = \
        #     statistics.mean(record_table[(num_nodes, num_items_per_node, run_id, "Local sorting")])
        partition_time = \
            statistics.mean(record_table[(num_nodes, num_items_per_node, run_id, "Partitioning (overall)")])
        partition_imbalance = \
            max(record_table[(num_nodes, num_items_per_node, run_id, "Partitioning (overall)")]) / \
            min(record_table[(num_nodes, num_items_per_node, run_id, "Partitioning (overall)")])
        shuffle_pivots_time = \
            statistics.mean([x for x in record_table[(num_nodes, num_items_per_node, run_id, "Shuffle pivots")] if x > 0])
        shuffle_pivots_imbalance = \
            max(record_table[(num_nodes, num_items_per_node, run_id, "Shuffle pivots")]) / \
            min([x for x in record_table[(num_nodes, num_items_per_node, run_id, "Shuffle pivots")] if x > 0])
        shuffle_keys_time = \
            statistics.mean(record_table[(num_nodes, num_items_per_node, run_id, "Shuffle keys")])
        shuffle_keys_imbalance = \
            max(record_table[(num_nodes, num_items_per_node, run_id, "Shuffle keys")]) / \
            min(record_table[(num_nodes, num_items_per_node, run_id, "Shuffle keys")])
        shuffle_vals_time = \
            statistics.mean(record_table[(num_nodes, num_items_per_node, run_id, "Shuffle values")])
        merge_sort_time = \
            statistics.mean(record_table[(num_nodes, num_items_per_node, run_id, "Online merge-sort")])
        rearrange_final_val_time = \
            statistics.mean(record_table[(num_nodes, num_items_per_node, run_id, "Rearrange final")])
        rearrange_times = [x + y for x, y in zip(
                record_table[(num_nodes, num_items_per_node, run_id, "Online merge-sort")],
                record_table[(num_nodes, num_items_per_node, run_id, "Rearrange final")])]
        rearrange_imbalance = max(rearrange_times) / min(rearrange_times)

        print(f'{num_nodes:12} {num_items_per_node:12} '
              f'{min_total_time:12.2f} {max_total_time:12.2f} '
              f'{p50_total_time:12.2f} {p90_total_time:12.2f} '
              f'{local_sort_time:12.2f} {partition_time:12.2f} '
              f'{shuffle_keys_time:12.2f} {shuffle_vals_time:12.2f} '
              f'{merge_sort_time:12.2f} {rearrange_final_val_time:12.2f} '
              f'{num_items_per_node/p50_total_time*1e3:12.2f} '
              f'{shuffle_pivots_time:12.2f} '
              f'{partition_imbalance:12.2f} {shuffle_pivots_imbalance:12.2f} '
              f'{shuffle_keys_imbalance:12.2f} {rearrange_imbalance:12.2f} '
              f'{run_id:12}')
