#!/usr/bin/env python3

import matplotlib.pyplot as plt
import pickle

plt.xlabel("Message Size (B)")
plt.ylabel("Latency (us)")

for data_source in ['millisort_p2p_benchmark_rccluster',
                    'millisort_p2p_benchmark_pod']:
    with open(data_source + '.txt') as file:
        message_size = []
        message_cost = []
        cost_model = {}

        # Skip the first line (i.e., the header) of the file
        next(file)
        for line in file:
            # Plot with the median latency.
            words = line.split()
            size = int(words[1])
            # cost = float(words[3]) # use min
            cost = float(words[4]) # use p50
            message_size.append(size)
            message_cost.append(cost)

            # Populate the cost model.
            cost_model[size] = cost

        plt.scatter(message_size, message_cost, marker='o', s = 5,
                label=data_source)

        # Serialize the performance model (i.e., a dictionary) to disk.
        # with open(data_source + '_model.bin', 'wb') as model_file:
        #     pickle.dump(cost_model, model_file)

plt.legend()
plt.show()

# TODO: compute percentage of per-message overhead and plot; John says 135KB is
# when per-message overhead only accounts for 10% completion time
