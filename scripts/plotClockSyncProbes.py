#!/usr/bin/python3
import matplotlib.pyplot as plt
import numpy as np

def split(u, v, points):
    # return points on left side of UV
    return [p for p in points if np.cross(p - u, v - u) < 0]

def extend(u, v, points):
    if not points:
        return []

    # find furthest point W, and split search to WV, UW
    w = min(points, key=lambda p: np.cross(p - u, v - u))
    p1, p2 = split(w, v, points), split(u, w, points)
    return extend(w, v, p1) + [w] + extend(u, w, p2)

def convex_hull(points):
    # find two hull points, U, V, and split to left and right search
    u = min(points, key=lambda p: p[0])
    v = max(points, key=lambda p: p[0])
    left, right = split(u, v, points), split(v, u, points)

    # find convex hull on each side
    return [v] + extend(u, v, left) + [u] + extend(v, u, right) + [v]

fastest_probe_xs = []
fastest_probe_ys = []

with open('../server1.rc02.out') as f:
    points = []

    for line in f:
        numbers = [int(x) for x in line.strip().split(' ')]
        if len(numbers) == 1:
            if numbers[0] < 100:
                server_id = numbers[0]
            else:
                num_probes = numbers[0]
                print("server id =", server_id, "# probes =", num_probes)
            continue
        if server_id != 2:
            continue

        if len(numbers) == 2:
            client_tsc, server_tsc = numbers
            fastest_probe_xs.append(client_tsc)
            fastest_probe_ys.append(server_tsc - client_tsc)
        elif len(numbers) == 3:
            client_tsc, server_tsc, _ = numbers
            points.append([client_tsc, server_tsc - client_tsc])

    points = np.array(points)
    plt.scatter(points[:, 0], points[:, 1], s = 0.5)

    # hull = np.array(convex_hull(points))
    # plt.scatter(hull[:, 0], hull[:, 1], s = 5, c = 'red', marker = 'o')


with open('../server2.rc03.out') as f:
    points = []

    for line in f:
        numbers = [int(x) for x in line.strip().split(' ')]
        if len(numbers) == 1:
            if numbers[0] < 100:
                server_id = numbers[0]
            else:
                num_probes = numbers[0]
                print("server id =", server_id, "# probes =", num_probes)
            continue
        if server_id != 1:
            continue

        if len(numbers) == 2:
            client_tsc, server_tsc = numbers
            fastest_probe_xs.append(server_tsc)
            fastest_probe_ys.append(client_tsc - server_tsc)
        elif len(numbers) == 3:
            client_tsc, server_tsc, _ = numbers
            points.append([server_tsc, client_tsc - server_tsc])

    points = np.array(points)
    plt.scatter(points[:, 0], points[:, 1], s = 0.5)

    # hull = np.array(convex_hull(points))
    # plt.scatter(hull[:, 0], hull[:, 1], s = 5, c = 'red', marker = 'o')

plt.plot(fastest_probe_xs, fastest_probe_ys, c = 'red')
plt.scatter(fastest_probe_xs, fastest_probe_ys, s = 10, c = 'black', marker = 'x')
plt.show()
