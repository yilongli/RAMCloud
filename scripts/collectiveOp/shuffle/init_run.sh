#!/usr/bin/env bash

scp rcmaster:/home/yilongl/temp/RAMCloud/logs/latest/server*tt logs/
 ./networkUtilPlot.py 0 > slack_time.txt &
./computePacketDelay.py > packet_delay.txt

# ./get_pull_timeline.sh 40 0 > scratch-pad.txt

# grep "server1.*transmitted.*" packet_delay.txt | cut -d':' -f2- > /tmp/server1.tx.pt
# ../../ttfix.py "" /tmp/server1.tx.pt > server1.tx.pt