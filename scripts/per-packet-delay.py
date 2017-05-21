#!/usr/bin/python
import fileinput
import os
import pickle
import re
import sys


class MessageParser:

    def __init__(self):
        self.regex = re.compile("|".join([
            " (.*) \| (.*) us .*: (sent) data, clientId (\d+), sequence (\d+), offset (\d+), length (\d+)",
            " (.*) \| (.*) us .*: \D+ (received) DATA, clientId (\d+), sequence (\d+), offset (\d+)",
            " (.*) \| (.*) us .*: \D+ (received) ALL_DATA, clientId (\d+), sequence (\d+)"]))

    def parse(self, message):
        matchObj = self.regex.match(message)
        if matchObj is None:
            return None

        matchedGroups = [x for x in matchObj.groups() if x]
        who = matchedGroups[0]
        time = float(matchedGroups[1])
        isRequest = who.startswith("client")
        sendingData = matchedGroups[2] == "sent"
        if sendingData:
            # sent data
            clientId, sequenceNum, offset, lengthOrDataFlag = \
                    [int(x) for x in matchedGroups[3:]]
        elif len(matchedGroups) == 6:
            # received DATA
            clientId, sequenceNum, offset = [int(x) for x in matchedGroups[3:]]
            lengthOrDataFlag = False
        else:
            # received ALL_DATA
            assert len(matchedGroups) == 5
            clientId, sequenceNum = [int(x) for x in matchedGroups[3:]]
            offset = 0
            lengthOrDataFlag = True

        return (isRequest, clientId, sequenceNum, offset), \
                (who, sendingData, time, lengthOrDataFlag)

if __name__ == "__main__":
    parser = MessageParser()
    packetSent = {}
    records = []

    # Parse command-line arguments
    senderPrefix = receiverPrefix = ""
    i = 1
    if sys.argv[i].find("->") >= 0:
        # The first argument specifies the query filter
        senderPrefix, receiverPrefix = \
            [s.strip() for s in sys.argv[1].split('->')]
        i += 1
    if i == len(sys.argv):
        # No input file specified
        print("Loading records from perPacketDelay.obj...")
        records = pickle.load(open('perPacketDelay.obj', 'r'))
    elif i + 1 == len(sys.argv):
        for line in fileinput.input(sys.argv[-1]):
            result = parser.parse(line)
            if result is None:
                continue

            packetId, (who, sendingData, time, lengthOrDataFlag) = result
            if sendingData:
                packetSent[packetId] = (who, time, lengthOrDataFlag)
            elif packetId in packetSent:
                sender, sentTime, length = packetSent[packetId]
                delay = time - sentTime
                records.append((sender, who, sentTime, delay,
                        packetId[1:]+(length,),
                        'S' if lengthOrDataFlag is True else ''))

        # Sort packets by the time they are sent
        records = sorted(records, key=lambda r : r[2])
        # Cache the records for future queries
        pickle.dump(records, open("perPacketDelay.obj", 'w'))
    else:
        print("Too many arguments!")
        exit()

    # Print per-packet delay filtered by specific sender(s) and/or receiver(s)
    prevTime = 0
    for r in [x for x in records if x[0].startswith(senderPrefix)
            and x[1].startswith(receiverPrefix)]:
        r = r[:3] + (r[2] - prevTime,) + r[3:]
        prevTime = r[2]
        print(" %s -> %s | %.2f (+ %.2f) %5.2f %s %s\n" % r)
