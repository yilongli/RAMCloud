#!/usr/bin/env python3

# Give a list of free S30 nodes on POD and the number of nodes to allocate,
# generate a list of nodes to exclude that enforces the nodes to allocate to
# spread evenly across edge switches.

import sys

# if len(sys.argv) == 3:
#     free_nodes = sys.argv[1]
#     nodes_to_alloc = int(sys.argv[2])
# else:
#     print('Usage: ./pod_print_excludenodes.py <free-nodes> <num-nodes>')
#     print('Example: ./pod_print_excludenodes.py "n801 n802" 1')
#     exit(0)

# Test input:
free_nodes = 'n750 n753 n755 n756 n760 n763 n768 n769 n771 n772 n773 n774 n775 n777 n778 n782 n783 n788 n796 n804 n806 n814 n815 n816 n817 n819 n820 n822 n824 n825 n826 n827 n828 n831 n832 n833 n835 n837 n839 n840 n841 n843 n845 n846 n848 n849 n852 n854 n855 n856 n858 n860 n861 n889 n892 n893 n894 n895 n897 n901 n902 n903 n919 n928 n929 n931 n935 n936 n938 n939 n940 n941 n943 n945 n946 n947 n949 n951 n952 n953 n956 n958 n962 n964 n965 n966 n967 n968 n971 n974 n975 n978 n979 n981'
nodes_to_alloc = 21

# The following arrays are generated from the output of `pbsnodes`.
# egrep -o "S30_ib_edge=[^;]*" <(pbsnodes -a) | cut -d= -f2 | sort | uniq > edge_switches.txt
# cat edge_switches.txt | while read x; do echo $x; grep $x pbsnodes.txt | egrep -o "n[0-9]{3,}" | tr '\n' ' ' | tr 'n' ','; echo ""; done
edge_switch = [[]] * 12
edge_switch[0] = [739]
edge_switch[1] = [790, 791, 792, 793, 794, 796, 797, 798, 799, 800, 801, 802, 804, 805, 806, 807, 808, 809, 810, 811, 813]
edge_switch[2] = [863, 867, 872, 876, 877, 878, 881, 882, 884, 885]
edge_switch[3] = [934, 935, 936, 937, 938, 939, 940, 941, 942, 943, 944, 945, 946, 947, 948, 949, 950, 951, 952, 953, 954, 955, 956]
edge_switch[4] = [705, 708, 709, 712, 713, 714, 717]
edge_switch[5] = [744, 746, 747, 750, 752, 753, 754, 755, 756, 757, 758, 759, 760, 761, 762, 763, 764]
edge_switch[6] = [766, 768, 769, 771, 772, 773, 774, 775, 776, 777, 778, 779, 780, 781, 782, 783, 784, 785, 786, 787, 788, 789]
edge_switch[7] = [814, 815, 816, 817, 818, 819, 820, 821, 822, 823, 824, 825, 826, 827, 828, 831, 832, 833, 834, 835, 836, 837]
edge_switch[8] = [838, 839, 840, 841, 843, 844, 845, 846, 848, 849, 850, 851, 852, 854, 855, 856, 857, 858, 860, 861]
edge_switch[9] = [886, 888, 889, 890, 891, 892, 893, 894, 895, 896, 897, 898, 899, 900, 901, 902, 903, 904, 905, 906, 907, 908, 909]
edge_switch[10] = [910, 912, 913, 914, 915, 916, 918, 919, 921, 922, 923, 926, 927, 928, 929, 931, 932, 933]
edge_switch[11] = [958, 959, 960, 961, 962, 963, 964, 965, 966, 967, 968, 969, 970, 971, 972, 973, 974, 975, 976, 977, 978, 979, 980, 981]

switch_id = 0
for nodes in edge_switch:
    switch_id += 1
    print(f'edge switch {switch_id:2} ({len(nodes):2} nodes): {nodes}')
print(f'{sum([len(nodes) for nodes in edge_switch])} nodes in total')

avail_by_switch = [[] for _ in edge_switch]

free_nodes = [int(n[1:]) for n in free_nodes.split() if n.startswith('n')]
free_nodes = [n for n in free_nodes if n >= 700]
for node_id in free_nodes:
    found = False
    for switch_id in range(len(edge_switch)):
        if node_id in edge_switch[switch_id]:
            avail_by_switch[switch_id].append(node_id)
            found = True
            break

    if not found:
        print(f'Warning: couldn\'t fine node n{node_id}!')

nodes_to_remove = len(free_nodes) - nodes_to_alloc
exclude_nodes = []
colocate_nodes = True
# colocate_nodes = False
for _ in range(nodes_to_remove):
    cnt = 9999 if colocate_nodes else 0
    sid = None
    for switch_id in range(len(avail_by_switch)):
        nodes_left = len(avail_by_switch[switch_id])
        if nodes_left == 0:
            continue

        update = (nodes_left < cnt) if colocate_nodes else (nodes_left > cnt)
        if update:
            cnt = nodes_left
            sid = switch_id

    exclude_nodes.append(avail_by_switch[sid][-1])
    avail_by_switch[sid].pop(-1)

print()
exclude_nodes = ':'.join([f'n{n}' for n in exclude_nodes])
print(f'excludeNodes = "{exclude_nodes}"')
print(f'{sum([len(x) for x in avail_by_switch])} nodes available = "{avail_by_switch}"')


# simpler version of getting exclude_nodes
all_nodes = set(range(700, 982))
include_nodes = set([
    # 839, 840, 841, 843, 845, 846, 848, 849, 852, 854, 855, 856, 858, 860, 861])
    # 935, 936, 937, 938, 939, 940, 941, 943, 945, 946, 947, 949, 951, 952, 953, 956,
    935, 936, 937, 938, 939, 940, 941, 943, 945, 946, 947, 949, 951,
    # 768, 769, 771, 772, 773, 774, 775, 777, 778, 780, 781, 782, 783, 786, 788,
    768, 769, 771, 772, 773, 774, 775, 777, 778, 780, 781, 782, 783,
    # 814, 815, 816, 817, 819, 820, 822, 824, 825, 826, 827, 828, 831, 832, 833, 835, 837,
    814, 815, 816, 817, 819, 820, 822, 824, 825, 826, 827, 828, 831,
    # 839, 840, 841, 843, 845, 846, 848, 849, 852, 854, 855, 856, 858, 860, 861,
    839, 840, 841, 843, 845, 846, 848, 849, 852, 854, 855, 856, 858,
    886, 889, 892, 893, 894, 895, 897, 898, 901, 902, 903, 904,
    958, 964, 965, 967, 969, 974, 975, 978, 981
    ])
    # 814, 815, 817, 819, 826, 827, 832, 835, 837, 839, 843, 845, 848, 854, 856, 858, 860, 861, 886, 892, 893, 894, 897, 901, 902, 907, 936, 938, 939, 946, 949])
exclude_nodes = all_nodes - include_nodes
print(":".join([f'n{n}' for n in sorted(list(exclude_nodes))]))
