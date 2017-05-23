#!/bin/bash
fix_delta='
BEGIN {
    prevTime=0
    startTime=0
}
$3~/^[0-9]+/ {
    delta = sprintf("%.2f", $3-prevTime)
    if (startTime == 0)
        startTime = $3
    prevTime = $3
    sub(".*", delta, $6)
    sub(".*", "us)", $7)
    print
    next
};{
    if (prevTime > 0)
        printf("Elapsed time: %.2f us\n", prevTime-startTime)
    startTime = 0
    print
}
END {
    printf("Elapsed time: %.2f us\n", prevTime-startTime)
}
'
timetrace=$1
grep "BAD TAIL LATENCY" -B 2 $timetrace | \
    grep -Po "ALL_DATA, \KclientId [0-9]*, sequence [0-9]*" | \
    xargs -I % sh -c 'echo "%:"; grep "%" '$timetrace | \
    awk "$fix_delta"
