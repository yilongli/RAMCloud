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
group_comm_steps = set()
if query_no == 1:
    max_steps = 2
    group_comm_steps = {1}
    column_titles += f'{"localScan":>12} {"localScanImb":>12} {"gather":>12}'
elif query_no == 2:
    max_steps = 4
    group_comm_steps = {1, 3}
    column_titles += f'{"hashPart":>12} {"hashPartImb":>12} {"shuffle":>12} {"shuffleImb":>12} ' \
                     f'{"countUniq":>12} {"countUniqImb":>12} {"gather":>12}'
elif query_no == 3:
    max_steps = 6
    group_comm_steps = {1, 3, 5}
    column_titles += f'{"shflCommit":>12} {"step1Imb":>12} {"foundUniq":>12} {"step2Imb":>12} ' \
                     f'{"shflLang":>12} {"step3Imb":>12} {"join":>12} {"step4Imb":>12} ' \
                     f'{"shflGroupBy":>12} {"step5Imb":>12} {"gather":>12}'
else:
    max_steps = 6
    column_titles += f'{"step1":>12} {"step1Imb":>12} {"step2":>12} ' \
                     f'{"step2Imb":>12} {"step3":>12} {"step3Imb":>12} ' \
                     f'{"step4":>12}  {"step4Imb":>12} {"step5":>12} ' \
                     f'{"step5Imb":>12} {"step6":>12} {"step6Imb":>12} ' \
                     f'{"step7":>12} {"step7Imb":>12} {"step8":>12} {"step8Imb":>12}'
print('=========================== WARNING ==============================')
print('1. The time breakdown of each step shown in this table is the\n'
      '   so-called "collective" elapsed time.')
print('2. The imbalance factor of each step is computed differently for\n'
      '   local computation step and group communication step.')
print('==================================================================')
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

        step_elapsed_time = [.0] * max_steps
        step_imbalance = [1] * max_steps
        node_start_time = [.0] * num_nodes
        for step in range(max_steps):
            step_str = f"Step {step + 1}"
            key = (num_nodes, num_items_per_node, run_id, step_str)
            node_elapsed_time = record_table[key]

            node_finish_time = node_start_time.copy()
            for i in range(num_nodes):
                node_finish_time[i] += node_elapsed_time[i]

            # Compute the collective start and finish time of this step.
            # Here we chose the collective start/fin. time to be the latest
            # individual start/fin. time. The overall elapsed time of this
            # step is defined to be the gap between the collective finish and
            # start times
            latest_start_time = max(node_start_time)
            latest_finish_time = max(node_finish_time)
            step_elapsed_time[step] = latest_finish_time - latest_start_time

            # Depending on the type of this step, compute/report the imbalance
            # factor differently; the reason is that nodes can have different
            # start times in group communication operations and that slow
            # starters have negative impacts on the finish times of other nodes.
            if step in group_comm_steps:
                # For group comm. operations, the imbalance factor is computed
                # using the same collective start time for all nodes.
                step_imbalance[step] = step_elapsed_time[step] / \
                        (min(node_finish_time) - latest_start_time)
            else:
                step_imbalance[step] = max(node_elapsed_time) / \
                                       min(node_elapsed_time)

            node_start_time = node_finish_time

        result = f'{num_nodes:12} {num_items_per_node:12} ' \
                 f'{num_nodes*num_items_per_node*1e-6:12.2f} ' \
                 f'{min_total_time:12.2f} {max_total_time:12.2f} ' \
                 f'{p50_total_time:12.2f} {p90_total_time:12.2f} ' \
                 f'{num_items_per_node/p50_total_time:12.1f} ' \
                 f'{len(total_times):12} {run_id:12} '
        # Hack: we treat the last step (which is always a gather?) in BigQuery
        # specially; as it's a many-to-one operation, there is no well-defined
        # meaning for the imbalance factor.
        for step in range(max_steps - 1):
            result += f'{step_elapsed_time[step]:12.2f} {step_imbalance[step]:12.2f} '
        result += f'{step_elapsed_time[-1]:12.2f}'
        print(result)
