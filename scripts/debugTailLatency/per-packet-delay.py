#!/usr/bin/python
import fileinput
from optparse import OptionParser
import os
import re
import sys


class MessageParser:
    '''
    Parse time trace log messages and extract per-packet information including:
    sender, receiver, TX time, RX time, length, etc.
    '''

    def __init__(self):
        self.regex = re.compile("|".join([
            " (.*) \| (.*) us .*: (sent) data, clientId (\d+), sequence (\d+), offset (\d+), length (\d+)",
            " (.*) \| (.*) us .*: \D+ (received) DATA, clientId (\d+), sequence (\d+), offset (\d+)",
            " (.*) \| (.*) us .*: \D+ (received) ALL_DATA, clientId (\d+), sequence (\d+)"]))

    def parse(self, message):
        '''
        Given a log message, returns the (packetId, info) pair if it matches
        the regex pattern of sending/receiving data; otherwise, None.
        '''

        matchObj = self.regex.match(message)
        if matchObj is None:
            return None

        matchedGroups = [x for x in matchObj.groups() if x]
        who = matchedGroups[0]
        time = float(matchedGroups[1])
        sendingData = matchedGroups[2] == "sent"
        isRequest = who.startswith("server") ^ sendingData
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
    optParser = OptionParser(description=
            'Extract the per-packet delay information from a time trace file.',
            usage='%prog [options] inputFile',
            conflict_handler='resolve')
    optParser.add_option('-s', '--sender', default='', metavar='TX',
            dest='sender', help='only print packets sent from TX')
    optParser.add_option('-r', '--receiver', default='', metavar='RX',
            dest='receiver', help='only print packets received by RX')
    optParser.add_option('--orderByRx', action='store_true', default=False,
            help='sort packets by their received time')
    (options, args) = optParser.parse_args()
    if len(args) != 1:
        print("Wrong number of arguments!")
        exit()

    # Parse time trace messages
    parser = MessageParser()
    packetSent = {}
    records = []
    for line in fileinput.input(args[0]):
        result = parser.parse(line)
        if result is None:
            continue

        packetId, (who, sendingData, time, lengthOrDataFlag) = result
        if sendingData:
            packetSent[packetId] = (who, time, lengthOrDataFlag)
        elif packetId in packetSent:
            sender, sentTime, length = packetSent[packetId]
            delay = time - sentTime
            records.append((sender, who,
                    time if options.orderByRx else sentTime,
                    delay, packetId[1:]+(length,),
                    'S' if lengthOrDataFlag is True else ''))
    records = sorted(records, key=lambda r : r[2])

    # Print per-packet delay filtered by specific sender(s) and/or receiver(s)
    records = filter(lambda x :
            (options.sender == '' or x[0] == options.sender) and
            (options.receiver == '' or x[1] == options.receiver), records)
    print("# options: %s" % options)
    prevTime = records[0][2]
    for r in records:
        r = r[:3] + (r[2] - prevTime,) + r[3:]
        prevTime = r[2]
        print(" %s -> %s | %.2f (+ %.2f) %5.2f %s %s" % r)
