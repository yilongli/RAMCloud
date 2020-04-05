#!/usr/bin/env python3

import sys
from statistics import mean

query_no = 0
if len(sys.argv) == 3:
    query_no = int(sys.argv[1])
    client_log = sys.argv[2]
else:
    print('Usage: extractDataFromBQDashboard.py <query-no> <client-log>')
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
        if line.startswith("  Elapsed time (ms)") or \
                line.startswith("  Step"):
            words = line.split()
            key = (num_nodes, num_items_per_node, run_id,
                   words[0] + ' ' + words[1])
            record_table[key] = [float(x) for x in line.split('|')[1].split()[:num_nodes]]

column_titles = f'{"nodes":>12} {"records":>12} {"total(M)":>12} {"min":>12} ' \
                f'{"max":>12} {"p50":>12} {"p90":>12} {"records/ms":>12} ' \
                f'{"runs":>12} {"runID(p50)":>12} '
if query_no == 1:
    max_steps = 2
    column_titles += f'{"localScan":>12} {"localScanImb":>12} {"gather":>12} {"gatherImb":>12}'
elif query_no == 2:
    max_steps = 4
    column_titles += f'{"hashPart":>12} {"hashPartImb":>12} {"shuffle":>12} {"shuffleImb":>12} ' \
                     f'{"countUniq":>12} {"countUniqImb":>12} {"gather":>12} {"gatherImb":>12}'
else:
    max_steps = 8
    column_titles += f'{"step1":>12} {"step1Imb":>12} {"step2":>12} ' \
                     f'{"step2Imb":>12} {"step3":>12} {"step3Imb":>12} ' \
                     f'{"step4":>12}  {"step4Imb":>12} {"step5":>12} ' \
                     f'{"step5Imb":>12} {"step6":>12} {"step6Imb":>12} ' \
                     f'{"step7":>12} {"step7Imb":>12} {"step8":>12} {"step8Imb":>12}'
print(column_titles)

for num_items_per_node in sorted(data_range_set):
    for num_nodes in sorted(node_range_set):
        total_times = []
        max_run_id = 0
        for run_id in range(1000):
            max_run_id = run_id
            server_total_times = record_table.get(
                (num_nodes, num_items_per_node, run_id, "Elapsed time"), None)
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

        step_time = [.0] * max_steps
        step_imbalance = [1] * max_steps
        for step in range(max_steps):
            try:
                step_time[step] = compute_elapsed_time(f"Step {step + 1}")
                step_imbalance[step] = compute_imbalance(f"Step {step + 1}")
            except:
                pass

        result = f'{num_nodes:12} {num_items_per_node:12} ' \
                 f'{num_nodes*num_items_per_node*1e-6:12.2f} ' \
                 f'{min_total_time:12.2f} {max_total_time:12.2f} ' \
                 f'{p50_total_time:12.2f} {p90_total_time:12.2f} ' \
                 f'{num_items_per_node/p50_total_time:12.1f} ' \
                 f'{len(total_times):12} {run_id:12} '
        for step in range(max_steps):
            result += f'{step_time[step]:12.2f} {step_imbalance[step]:12.2f} '
        print(result)
