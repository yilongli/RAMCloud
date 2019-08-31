#!/usr/bin/env python3

import glob
import sys
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from matplotlib import collections as mc

if len(sys.argv) > 2:
    logs_dir = sys.argv[1]
    draw_rx = int(sys.argv[2]) == 0
else:
    logs_dir = 'logs'
    draw_rx = int(sys.argv[1]) == 0

_, ax = plt.subplots()
plt.xlabel("Time (us)")
plt.ylabel("Rank")
ax.yaxis.set_major_locator(ticker.MultipleLocator(5))
ax.yaxis.set_minor_locator(ticker.MultipleLocator(1))

def get_packet_time_us(bytes):
    # A full packet is 4096B and the bandwidth is 24Gbps, or 3B/ns.
    # return bytes * 8.0 / 24.0 / 1000
    return bytes * 8.0 / 50.0 / 1000

num_nodes = len(glob.glob(logs_dir + '/server*.tt'))
for node_id in range(num_nodes):
    with open(logs_dir + '/server' + str(node_id + 1) + '.tt', 'r') as file:
        total_busy_time = 0.0
        operation_start = []
        receive_packets = []
        transmit_packets = []
        handle_pull_rpc = []
        rpc_complete = []

        for line in file:
            words = line.strip().split()
            time = float(words[1])
            # Example: ALL_SHUFFLE benchmark started, run 50
            if words[-2] == 'run' and words[-5] == "ALL_SHUFFLE":
                operation_start.append(time)
            # Example: infud received packet, size 4096, batch size 2
            elif words[-2] == 'size' and words[-5] == "size":
                receive_packets.append((time, int(words[-4][:-1])))
            # Example: infud: transmit buffer XYZ of size B enqueued
            elif words[-1] == 'enqueued':
                transmit_packets.append((time, int(words[-2])))
            # Example: shuffle-server: delayed 10 us, handled pull request from
            # rank 0
            elif words[-6] == 'handled' and words[-2] == "rank":
                handle_pull_rpc.append((time, int(words[-1])))
            # Example: shuffle-client: pull request 22 to rank 7 completed, bytes XXX
            elif words[-3] == 'completed,' and words[-5] == "rank":
                rpc_complete.append((time, int(words[-7])))

        offset = .0

        receive_packets.reverse()
        network_rx_busy_segs = []
        y = node_id + offset
        busy_end, packet_size = receive_packets[0]
        busy_start = busy_end - get_packet_time_us(packet_size)
        total_slack_time = 0
        for time, packet_size in receive_packets[1:]:
            if busy_start <= time:
                busy_start -= get_packet_time_us(packet_size)
            else:
                # Print out large slack time for debugging
                slack = busy_start - time
                if slack > 0.1:
                    print(f'rank {node_id}, slack {slack:.2f} us at {time:.2f}')
                total_slack_time += busy_start - time

                network_rx_busy_segs.append([(busy_start, y), (busy_end, y)])
                total_busy_time += busy_end - busy_start
                busy_end = time
                busy_start = busy_end - get_packet_time_us(packet_size)
        network_rx_busy_segs.append([(busy_start, y), (busy_end, y)])
        total_busy_time += busy_end - busy_start
        print(f'Total slack time at rank {node_id}: {total_slack_time:.2f} us')
        print(f'Total busy time at rank {node_id}: {total_busy_time:.2f} us')

        network_tx_busy_segs = []
        if len(transmit_packets) > 0:
            y = node_id - offset
            busy_start, packet_size = transmit_packets[0]
            busy_end = busy_start + get_packet_time_us(packet_size)
            for time, packet_size in transmit_packets[1:]:
                if time < busy_end:
                    busy_end += get_packet_time_us(packet_size)
                else:
                    y = node_id - offset
                    network_tx_busy_segs.append([(busy_start, y), (busy_end, y)])
                    busy_start = time
                    busy_end = busy_start + get_packet_time_us(packet_size)
            network_tx_busy_segs.append([(busy_start, y), (busy_end, y)])

        # Plot network busy ranges.
        network_rx_busy_line = mc.LineCollection(network_rx_busy_segs, linewidths=5)
        network_tx_busy_line = mc.LineCollection(network_tx_busy_segs, color='g', linewidths=5)
        if draw_rx:
            ax.add_collection(network_rx_busy_line)
        else:
            ax.add_collection(network_tx_busy_line)

        plt.scatter(operation_start, [node_id] * len(operation_start),
                    marker='|', color='r', s=200)
        plt.scatter([t for t, _ in rpc_complete], [node_id] * len(rpc_complete),
                    marker='|', color='r', s=50)
        # if node_id > 27:
        #     for time, rpc_id in rpc_complete:
        #         # text = str(rpc_id)
        #         text = str((node_id + rpc_id + 1) % num_nodes)
        #         plt.text(time, node_id, text, color='r', fontsize=7)
        # else:
        #     for time, sender_id in handle_pull_rpc:
        #         plt.text(time, node_id, str(sender_id), color='g', fontsize=7)

        for time, rpc_id in rpc_complete:
            # text = str(rpc_id)
            text = str((node_id + rpc_id + 1) % num_nodes)
            plt.text(time, node_id, text, color='r', fontsize=7)
        # for time, sender_id in handle_pull_rpc:
        #     plt.text(time, node_id, str(sender_id), color='g', fontsize=10)

    plt.axvline(operation_start[0] + total_busy_time, c = 'g')

plt.legend()
plt.show()

