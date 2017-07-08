#!/usr/bin/python

import glob
import re

class Rpc(object):

    def __init__(self, rpcId):
        self.rpcId = rpcId
        self.client = None
        self.server = None
        self.tsc0 = None
        self.tsc1 = None
        self.tsc2 = None
        self.tsc3 = None
        self.retransmission = False

    def isComplete(self):
        return self.tsc0 and self.tsc1 and self.tsc2 and self.tsc3

    def getElapsedTime(self):
        return self.tsc3 - self.tsc0

LOG_FILE_REGEX = re.compile("(client|server)[0-9]*\.(.*)\.log")
END_OF_CPERF_REGEX = re.compile(".*End of ClusterPerf experiment at TSC (\d+)")
# Example: 1492031792.531004386 TimeTrace.cc:172 in printInternal NOTICE[8]: 255789.0 ns (+ 114.9 ns): server received DATA, sequence 61257, offset 13846, length 2008, flags 1
STARTING_TSC_REGEX = re.compile(".*TimeTrace.cc:.*: Starting TSC (\d+), cyclesPerSec (\d+)")
TIMETRACE_MSG_REGEX = re.compile(".*TimeTrace.cc:.*:\s+(\d+) cyc.*: (.*)")
CALIBR_TSC_MSG_REGEX = re.compile(".*TSC_Freq\((.*)\) \* (.*) = TSC_Freq")
#ALLDATA_MSG_REGEX = re.compile("^.*: (.*) ns .*(client|server) (sending|received) ALL_DATA.*clientId (\d+), sequence (\d+)")
ALLDATA_MSG_REGEX = re.compile("^.*: (.*) cyc .*(client|server) (sending|received) ALL_DATA.*clientId (\d+), sequence (\d+)")
cyclesPerMicros = 1995.379 # TODO

#
hosts = sorted(set([x.split('.')[1] for x in glob.glob('*.log')]))
role2Host = {x.split('.')[0] : x.split('.')[1] for x in glob.glob('*.log')}
print("hosts = %s" % hosts)

# TSC_Freq(x) = TSC_Freq(y) * scalars[x][y]
scalars = {x : {y : None if x != y else 1 for y in hosts} for x in hosts}

# List of rpcs indexed by (client, server)-pair.
rpcsByCS = {}
rpcs = {}
endingTSC = None
for fname in glob.glob("*.log"):
    matchResult = LOG_FILE_REGEX.match(fname)
    if not matchResult:
        continue
    print("Scanning %s..." % fname)

    host = matchResult.group(2)
    baseTSC = None

    with open(fname, 'r') as f:
        startingTSC = 0
        for line in f.readlines():
            if endingTSC is None:
                matchResult = END_OF_CPERF_REGEX.match(line)
                if matchResult:
                    endingTSC = int(matchResult.group(1))
                    continue

            matchResult = STARTING_TSC_REGEX.match(line)
            if matchResult:
                startingTSC = int(matchResult.group(1))
                if baseTSC is None:
                    baseTSC = startingTSC
                    startingTSC = 0
                    if fname.startswith("client1"):
                        endingTSC -= baseTSC
                else:
                    startingTSC -= baseTSC
                continue

            matchResult = CALIBR_TSC_MSG_REGEX.match(line)
            if matchResult:
                role = matchResult.group(1)
                scalar = float(matchResult.group(2))
                scalars[host][role2Host[role]] = scalar
                continue

            matchResult = ALLDATA_MSG_REGEX.match(line)
            if not matchResult:
                continue
            timestamp = startingTSC + int(matchResult.group(1))
            who = matchResult.group(2)
            isMsgSender = matchResult.group(3) == "sending"
            clientId = int(matchResult.group(4))
            sequenceNum = int(matchResult.group(5))

            rpcId = (clientId, sequenceNum)
            if rpcId in rpcs:
                rpc = rpcs[rpcId]
            else:
                rpc = Rpc(rpcId)
                rpcs[rpcId] = rpc

            setattr(rpc, who, host)
            if isMsgSender:
                if who == "client":
                    rpc.retransmission = rpc.retransmission or rpc.tsc0
                    rpc.tsc0 = timestamp
                else:
                    rpc.retransmission = rpc.retransmission or rpc.tsc2
                    rpc.tsc2 = timestamp
            else:
                if who == "server":
                    rpc.tsc1 = timestamp
                else:
                    rpc.tsc3 = timestamp

            if rpc.isComplete():
                cs = (rpc.client, rpc.server)
                if cs not in rpcsByCS:
                    rpcsByCS[cs] = []
                rpcsByCS[cs].append(rpc)

# TSC(x) = TSC(y) * scalars[x][y] + offset[x][y]
offsets = {x : {y : None if x != y else 0 for y in hosts} for x in hosts}
for cs in rpcsByCS.keys():
    scalar = scalars[cs[0]][cs[1]]
    if scalar is None:
        continue

    minRequestDelay = min([x.tsc1 * scalar - x.tsc0 for x in rpcsByCS[cs]])
    minReplyDelay = min([x.tsc3 - x.tsc2 * scalar for x in rpcsByCS[cs]])
    minProcessingTime = min([(x.tsc2 - x.tsc1) * scalar for x in rpcsByCS[cs]])
    minElapsedTime = minRequestDelay + minProcessingTime + minReplyDelay

    offset = (minReplyDelay - minRequestDelay) / 2
    minOneWayDelay = minRequestDelay + offset
    offsets[cs[0]][cs[1]] = offset

    print("TSC(%s) = TSC(%s) x %.15f + %d" % (cs[0], cs[1], scalar, offset))
    print("Estimated min. one-way delay = %d cyc" % minOneWayDelay)
    assert minOneWayDelay > 3000

    # Sanity check
    numWarnings = 0
    for x in rpcsByCS[cs]:
        extraTime = x.getElapsedTime() - minElapsedTime

        outgoingOneWayDelay = x.tsc1 * scalar + offset - x.tsc0
        incomingOneWayDelay = x.tsc3 - (x.tsc2  * scalar + offset)
        processingTime = (x.tsc2 - x.tsc1) * scalar

        eps = 0.001
        if outgoingOneWayDelay + eps < minOneWayDelay:
            numWarnings += 1
            print("Warning: computed outgoing one-way delay %d less than %d" % (outgoingOneWayDelay, minOneWayDelay))
        elif incomingOneWayDelay + eps < minOneWayDelay:
            numWarnings += 1
            print("Warning: computed incoming one-way delay %d less than %d" % (incomingOneWayDelay, minOneWayDelay))
        elif processingTime + eps < minProcessingTime:
            numWarnings += 1
            print("Warning: computed processing time %d less than %d" % (processingTime, minProcessingTime))
    if numWarnings > 0:
        print("#Warnings = %d, warning rate = %.2f%%" % (numWarnings, 100.0 * numWarnings / len(rpcsByCS[cs])))

# Convert TSC(ClientN) to TSC(Client1) via TSC(Server1)
client1 = role2Host["client1"]
server1 = role2Host["server1"]
for clientN in [role2Host[c] for c in role2Host.keys() if c.startswith("client") and c != "client1"]:
    if scalars[client1][clientN] is not None:
        continue
    scalar = scalars[client1][server1] / scalars[clientN][server1]
    scalars[client1][clientN] = scalar
    offsets[client1][clientN] = offsets[client1][server1] - scalar * offsets[clientN][server1]

# Rewrite all timetraces using TSC(Client1)
timetraceEntries = []
for role, host in role2Host.iteritems():
    if role == "coordinator":
        continue

    fileName = "%s.%s.log" % (role, host)
    with open(fileName, 'r') as f:
        with open("%s.%s.tt" % (role, host), 'w') as out:
            prevTSC = None
            for line in f.readlines():
                matchResult = TIMETRACE_MSG_REGEX.match(line)
                if not matchResult:
                    continue
                currentTSC = int(matchResult.group(1)) * scalars[client1][host] + offsets[client1][host]
                message = matchResult.group(2)

                # Discard messages outside the scope (ie. [0, endingTSC]) of the client1's log
                if currentTSC < 0:
                    continue
                if currentTSC > endingTSC:
                    break

                if prevTSC is None:
                    prevTSC = currentTSC
                deltaTSC = currentTSC - prevTSC
                prevTSC = currentTSC

                out.write("%7.2f us (+%7.1f ns): %s\n" % (currentTSC / cyclesPerMicros, deltaTSC * 1000 / cyclesPerMicros, message))
                timetraceEntries.append((role, currentTSC, message))

timetraceEntries = sorted(timetraceEntries, key=lambda e : e[1])
with open("merged.tt", 'w') as out:
    prevTSC = timetraceEntries[0][1]
    for e in timetraceEntries:
        deltaTSC = e[1] - prevTSC
        prevTSC = e[1]
        out.write(" %s | %7.2f us (+%7.1f ns): %s\n" % (e[0], e[1] / cyclesPerMicros, deltaTSC * 1000 / cyclesPerMicros, e[2]))
