BEGIN {
    prevTime=0
    startTime=0
}
$1~/^(-)?[0-9]+/ {
    delta = sprintf("%.2f", $1-prevTime)
    if (startTime == 0)
        startTime = $1
    prevTime = $1
    sub(".*", delta, $4)
    sub(".*", "us)", $5)
    print
    next
};{
    startTime = 0
    print
}
