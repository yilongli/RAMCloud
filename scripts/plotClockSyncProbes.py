#!/usr/bin/python3
import matplotlib.pyplot as plt

fastest_probe_xs = []
fastest_probe_ys = []

with open('../server1.rc02.out') as f:
    line_num = 0
    xs = []
    ys = []

    for line in f:
        if line_num == 0:
            server_id = int(line)
            print(server_id)
        elif line_num == 1:
            num_probes = int(line)
            print(num_probes)
        elif line_num <= 4:
            client_tsc, server_tsc = [int(x) for x in line.split(' ')]
            fastest_probe_xs.append(client_tsc)
            fastest_probe_ys.append(server_tsc - client_tsc)
        else:
            client_tsc, server_tsc, _ = \
                    [int(x) for x in line.split(' ')]
            xs.append(client_tsc)
            ys.append(server_tsc - client_tsc)
        line_num += 1

    plt.scatter(xs, ys, s = 0.1)

with open('../server2.rc03.out') as f:
    line_num = 0
    xs = []
    ys = []

    for line in f:
        if line_num == 0:
            server_id = int(line)
            print(server_id)
        elif line_num == 1:
            num_probes = int(line)
            print(num_probes)
        elif line_num <= 4:
            client_tsc, server_tsc = [int(x) for x in line.split(' ')]
            fastest_probe_xs.append(server_tsc)
            fastest_probe_ys.append(client_tsc - server_tsc)
        else:
            client_tsc, server_tsc, _ = \
                [int(x) for x in line.split(' ')]
            xs.append(server_tsc)
            ys.append(client_tsc - server_tsc)
        line_num += 1

    plt.scatter(xs, ys, s = 0.1)

plt.scatter(fastest_probe_xs, fastest_probe_ys, s = 10, c = 'red', marker = 'x')
plt.show()
# plt.savefig('huygens_scatter.png', dpi = 300)
