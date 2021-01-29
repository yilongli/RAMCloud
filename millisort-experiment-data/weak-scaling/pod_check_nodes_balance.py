#!/usr/bin/env python3

# Given a list of S30 nodes allocated in an experiment, count how many nodes
# are attached to each edge switch.

import sys

# if len(sys.argv) == 2:
#     alloc_nodes = int(sys.argv[1])
# else:
#     print('Usage: ./pod_check_nodes_balance.py <node-list>')
#     exit(0)

# Test input
# Note: the following 20 nodes are GOOD!
alloc_nodes = 'n814 n817 n819 n826 n827 n832 n835 n837 n839 n843 n845 n848 n854 n856 n858 n860 n861 n893 n901 n902 n923'
# Note: what about the following 30 nodes?
# not good
alloc_nodes = 'n814 n817 n819 n824 n825 n826 n827 n828 n831 n832 n833 n835 n837 n839 n841 n843 n845 n846 n848 n849 n854 n855 n856 n858 n860 n861 n893 n894 n901 n902 n923'
# better
alloc_nodes = 'n761 n814 n815 n817 n819 n826 n827 n832 n835 n837 n839 n843 n845 n848 n854 n856 n858 n860 n861 n886 n892 n893 n894 n897 n901 n902 n907 n936 n938 n939 n946'
# very bad
alloc_nodes = 'n744 n750 n752 n753 n755 n756 n757 n758 n760 n761 n762 n774 n814 n815 n816 n817 n819 n822 n825 n826 n827 n839 n841 n843 n845 n846 n848 n849 n854 n886 n946'
# really nice!
alloc_nodes = 'n814 n815 n817 n819 n826 n827 n832 n835 n837 n839 n843 n845 n848 n854 n856 n858 n860 n861 n886 n892 n893 n894 n897 n901 n902 n907 n936 n938 n939 n946 n949'
# a very nice set of nodes for 80 servers
alloc_nodes = 'n839 n843 n845 n846 n848 n849 n854 n855 n856 n858 n860 n861 n938 n940 n941 n946 n947 n949 n952 n953 n956'
# a very nice set of nodes for 160 servers
alloc_nodes = 'n814 n815 n816 n817 n819 n822 n824 n825 n826 n827 n828 n831 n832 n833 n835 n837 n839 n841 n843 n845 n846 n848 n849 n854 n855 n856 n858 n860 n861 n938 n939 n940 n941 n945 n946 n951 n952 n953 n956 n966 n969'
# a set of OK nodes (perf. quite for 1ms scaleDown graph; not so much for 10ms) for 240 servers
alloc_nodes = 'n750 n752 n753 n755 n756 n757 n758 n760 n762 n763 n772 n773 n774 n777 n814 n815 n816 n817 n819 n822 n824 n825 n826 n827 n828 n831 n832 n833 n835 n837 n839 n841 n843 n845 n846 n848 n849 n854 n855 n856 n858 n860 n861 n886 n894 n897 n936 n938 n939 n940 n941 n945 n946 n947 n949 n951 n952 n953 n956 n966 n969'
# a set of OK nodes (perf. quite for 1ms scaleDown graph; not so much for 10ms) for 200 servers
alloc_nodes = 'n768 n769 n771 n772 n773 n774 n775 n777 n778 n782 n814 n815 n816 n817 n819 n822 n824 n825 n826 n827 n828 n831 n839 n841 n843 n845 n846 n849 n854 n855 n856 n858 n860 n861 n936 n939 n940 n941 n945 n946 n947 n949 n951 n952 n953 n956 n962 n966 n968 n971 n979'
# Free nodes obtained from `grep "n[789]" /public/apps/pod/data/preemptee_nodes | tr '\n' ' '`
alloc_nodes = 'n750 n753 n755 n756 n760 n763 n768 n769 n771 n772 n773 n774 n775 n777 n778 n782 n783 n788 n796 n804 n806 n814 n815 n816 n817 n819 n820 n822 n824 n825 n826 n827 n828 n831 n832 n833 n835 n837 n839 n840 n841 n843 n845 n846 n848 n849 n852 n854 n855 n856 n858 n860 n861 n889 n892 n893 n894 n895 n897 n901 n902 n903 n919 n928 n929 n931 n935 n936 n938 n939 n940 n941 n943 n945 n946 n947 n949 n951 n952 n953 n956 n958 n962 n964 n965 n966 n967 n968 n971 n974 n975 n978 n979 n981'

alloc_nodes = 'n772 n773 n774 n777 n778 n780 n814 n815 n816 n817 n819 n822 n824 n826 n827 n839 n843 n845 n846 n848 n854 n855 n856 n858 n886 n893 n895 n898 n901 n902 n904 n936 n938 n941 n945 n946 n947 n949 n951 n969 n978'

num_edge_switches = 12

# The following arrays are generated from the output of `pbsnodes`.
# egrep -o "S30_ib_edge=[^;]*" <(pbsnodes -a) | cut -d= -f2 | sort | uniq > edge_switches.txt
# cat edge_switches.txt | while read x; do echo $x; grep $x pbsnodes.txt | egrep -o "n[0-9]{3,}" | tr '\n' ' ' | tr 'n' ','; echo ""; done
edge_switch = [[]] * num_edge_switches
edge_switch[0] = [739]
edge_switch[1] = [790 ,791 ,792 ,793 ,794 ,796 ,797 ,798 ,799 ,800 ,801 ,802 ,804 ,805 ,806 ,807 ,808 ,809 ,810 ,811 ,813]
edge_switch[2] = [863 ,867 ,872 ,876 ,877 ,878 ,881 ,882 ,884 ,885]
edge_switch[3] = [934 ,935 ,936 ,937 ,938 ,939 ,940 ,941 ,942 ,943 ,944 ,945 ,946 ,947 ,948 ,949 ,950 ,951 ,952 ,953 ,954 ,955 ,956]
edge_switch[4] = [705 ,708 ,709 ,712 ,713 ,714 ,717]
edge_switch[5] = [744 ,746 ,747 ,750 ,752 ,753 ,754 ,755 ,756 ,757 ,758 ,759 ,760 ,761 ,762 ,763 ,764]
edge_switch[6] = [766 ,768 ,769 ,771 ,772 ,773 ,774 ,775 ,776 ,777 ,778 ,779 ,780 ,781 ,782 ,783 ,784 ,785 ,786 ,787 ,788 ,789]
edge_switch[7] = [814 ,815 ,816 ,817 ,818 ,819 ,820 ,821 ,822 ,823 ,824 ,825 ,826 ,827 ,828 ,831 ,832 ,833 ,834 ,835 ,836 ,837]
edge_switch[8] = [838 ,839 ,840 ,841 ,843 ,844 ,845 ,846 ,848 ,849 ,850 ,851 ,852 ,854 ,855 ,856 ,857 ,858 ,860 ,861]
edge_switch[9] = [886 ,888 ,889 ,890 ,891 ,892 ,893 ,894 ,895 ,896 ,897 ,898 ,899 ,900 ,901 ,902 ,903 ,904 ,905 ,906 ,907 ,908 ,909]
edge_switch[10] = [910 ,912 ,913 ,914 ,915 ,916 ,918 ,919 ,921 ,922 ,923 ,926 ,927 ,928 ,929 ,931 ,932 ,933]
edge_switch[11] = [958 ,959 ,960 ,961 ,962 ,963 ,964 ,965 ,966 ,967 ,968 ,969 ,970 ,971 ,972 ,973 ,974 ,975 ,976 ,977 ,978 ,979 ,980 ,981]

switch_id = 0
for nodes in edge_switch:
    switch_id += 1
    print(f'edge switch {switch_id:2} ({len(nodes):2} nodes): {nodes}')
print(f'{sum([len(nodes) for nodes in edge_switch])} nodes in total')

count_by_switch = [[] for _ in range(num_edge_switches)]

alloc_nodes = [int(n[1:]) for n in alloc_nodes.split() if n.startswith('n')]
for node_id in alloc_nodes:
    for switch_id in range(num_edge_switches):
        if node_id in edge_switch[switch_id]:
            count_by_switch[switch_id].append(node_id)

print()
print(f'About to allocate {len(alloc_nodes)} nodes')
print(f'avg nodes per edge switch = {len(alloc_nodes)*1./num_edge_switches:.2f}')
for switch_id in range(num_edge_switches):
    print(f'edge switch {switch_id + 1}: {count_by_switch[switch_id]} {len(count_by_switch[switch_id])}/{len(edge_switch[switch_id])} ({len(count_by_switch[switch_id]) / len(edge_switch[switch_id]) * 100:.2f}% free)')