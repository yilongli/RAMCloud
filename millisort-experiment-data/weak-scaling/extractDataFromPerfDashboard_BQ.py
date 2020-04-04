#!/usr/bin/env python3

import sys
from statistics import mean

if len(sys.argv) == 2:
    client_log = sys.argv[1]
else:
    print('Usage: extractDataFromBQDashboard.py <client-log>')
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

print(f'{"nodes":>12} {"records":>12} {"total(M)":>12} {"min":>12} {"max":>12} {"p50":>12} '
      f'{"p90":>12} {"records/ms":>12} {"runs":>12} {"runID(p50)":>12} '
      f'{"step1":>12} {"step1Imbal":>12} {"step2":>12} {"step2Imbal":>12} '
      f'{"step3":>12} {"step3Imbal":>12} {"step4":>12}  {"step4Imbal":>12} '
      f'{"step5":>12} {"step5Imbal":>12} {"step6":>12} {"step6Imbal":>12} '
      f'{"step7":>12} {"step7Imbal":>12} {"step8":>12} {"step8Imbal":>12}')

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

        max_steps = 8
        step_time = [.0] * max_steps
        step_imbalance = [1] * max_steps
        for step in range(max_steps):
            try:
                step_time[step] = compute_elapsed_time(f"Step {step + 1}")
                step_imbalance[step] = compute_imbalance(f"Step {step + 1}")
            except:
                pass

        print(f'{num_nodes:12} {num_items_per_node:12} {num_nodes*num_items_per_node*1e-6:12.2f} '
              f'{min_total_time:12.2f} {max_total_time:12.2f} '
              f'{p50_total_time:12.2f} {p90_total_time:12.2f} '
              f'{num_items_per_node/p50_total_time:12.1f} {len(total_times):12} {run_id:12} '
              f'{step_time[0]:12.2f} {step_imbalance[0]:12.2f} '
              f'{step_time[1]:12.2f} {step_imbalance[1]:12.2f} '
              f'{step_time[2]:12.2f} {step_imbalance[2]:12.2f} '
              f'{step_time[3]:12.2f} {step_imbalance[3]:12.2f} '
              f'{step_time[4]:12.2f} {step_imbalance[4]:12.2f} '
              f'{step_time[5]:12.2f} {step_imbalance[5]:12.2f} '
              f'{step_time[6]:12.2f} {step_imbalance[6]:12.2f} '
              f'{step_time[7]:12.2f} {step_imbalance[7]:12.2f} '
              )