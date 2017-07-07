#!/usr/bin/python
import fileinput
from optparse import OptionParser
import os
import re
import sys
import math
from itertools import groupby

# Minimum cost to receive a full packet, in microseconds
MIN_RX_PACKET_COST = 0.3

# How many full packets does the HomaTransport::roundTripBytes have?
NUM_RTT_PACKETS = 7

# Driver::getMaxPacketSize
MAX_PACKET_SIZE = 1470

class Packet:
    '''
    TODO
    '''
    def __init__(self, packetId):
        # Unique identifier of the packet as a quadruple:
        # (RequestOrReply, clientId, sequenceNum, offset)
        self.packetId = packetId

        # Sender and recipient of the packet
        self.sender = None
        self.recipient = None

        # Other properties of the packet
        self.length = float('NaN')
        self.priority = None
        self.isAllData = None

        # Timestamps
        self.sentTime = float('NaN')
        self.recvTime = float('NaN')
        self.sentGrantTime = float('NaN')
        self.recvGrantTime = float('NaN')
        self.orderTime = None

        # Runtime properties
        self.delay = float('NaN')
        self.extraDelay = None
        self.txQueuedBytes = float('NaN')
        self.roundTripTime = float('NaN')   # recvGrantTime - sentTime
        self.grantRespTime = float('NaN')   # sentGrantTime - recvTime
        self.grantDelay = float('NaN')      # recvGrantTime - sentGrantTime
        self.grantTxQueuedBytes = float('NaN')

        # Sources of delays:
        # 1) Queueing delay at the NIC's transmit queue
        # 2) Queueing delay at TOR switch
        # 3) Polling gap: corresponding to the max. polling delay
        # 4) Software overhead of receiving packets
        self.txQueueDelay = None
        self.maxTorQueueDelay = float('NaN')
        self.maxPollingDelay = float('NaN')
        self.rxDelay = float('NaN')

    def isComplete(self):
        return not math.isnan(self.sentTime) and \
                not math.isnan(self.recvTime) and self.priority

    def getMessageId(self):
        return self.packetId[0], self.packetId[1:3]


class MessageParser:
    '''
    Parse time trace log messages and extract per-packet information including:
    sender, receiver, TX time, RX time, length, etc.
    '''

    def __init__(self):
        self.regex = re.compile("|".join([
            " (.*) \| (.*) us .*: (start) of polling iteration \d+, last poll was (\d+) ns ago",
            " (.*) \| (.*) us .*: \D+ (sending) ALL_DATA, clientId (\d+), sequence (\d+), priority (\d+)",
            " (.*) \| (.*) us .*: \D+ (sending) DATA, clientId (\d+), sequence (\d+), offset (\d+), priority (\d+)",
            " (.*) \| (.*) us .*: (sent) data, clientId (\d+), sequence (\d+), offset (\d+), (\d+) bytes queued ahead",
            " (.*) \| (.*) us .*: \D+ (received) DATA, clientId (\d+), sequence (\d+), offset (\d+), length (\d+)",
            " (.*) \| (.*) us .*: \D+ (received) ALL_DATA, clientId (\d+), sequence (\d+), length (\d+)",
            " (.*) \| (.*) us .*: (sent GRANT), clientId (\d+), sequence (\d+), offset (\d+), (\d+) bytes queued ahead",
            " (.*) \| (.*) us .*: \D+ (received GRANT), clientId (\d+), sequence (\d+), offset (\d+)"]))
        self.packets = {}
        self.startOfPoll = {}
        self.maxPollingDelays = {}

        # RpcId :-> (client, server)
        self.rpcClientServer = {}

    def getPackets(self):
        return [p for p in self.packets.values()]

    def parse(self, message):
        '''
        Given a log message, returns the (packetId, info) pair if it matches
        the regex pattern of sending/receiving data; otherwise, None.
        '''

        matchObj = self.regex.match(message)
        if matchObj is None:
            return

        matchedGroups = [x for x in matchObj.groups() if x]
        who = matchedGroups[0]
        time = float(matchedGroups[1])
        action = matchedGroups[2]
        isRequest = who.startswith("server") ^ (action in ["sent", "sending", "received GRANT"])

        if action == "start":
            # Case 0: the beginning of a polling iteration
            lastIdleTime = int(matchedGroups[3])
            self.startOfPoll[who] = time
            self.maxPollingDelays[who] = lastIdleTime / 1000.0
        elif action == "sending":
            # Case 1: about to send a packet (note: this message won't be
            # necessary if RAMCloud::TimeTrace::record can take > 4 arguments)
            if len(matchedGroups) == 6:
                clientId, sequenceNum, priority = \
                        [int(x) for x in matchedGroups[3:]]
                offset = 0
            else:
                clientId, sequenceNum, offset, priority = \
                        [int(x) for x in matchedGroups[3:]]

            packetId = (isRequest, clientId, sequenceNum, offset)
            packet = Packet(packetId)
            packet.sender = who
            packet.priority = priority
            self.packets[packetId] = packet
        elif action == "sent":
            # Case 2: a data packet has been sent
            clientId, sequenceNum, offset, queueSize = \
                    [int(x) for x in matchedGroups[3:]]

            packetId = (isRequest, clientId, sequenceNum, offset)
            if packetId not in self.packets:
                self.packets[packetId] = Packet(packetId)
            packet = self.packets[packetId]
            packet.sender = who
            # Note: We are using the timestamp from the "sent data" message,
            # rather than the "sending" message, as the packet enqueue time.
            # This is usually more accurate. However, it's also an upper bound
            # rather than lower bound. So if a jitter occurs right before we
            # log the "sent data" message, we'll get a unrealistically small
            # packet delay later in `pkt.delay = pkt.recvTime - pkt.sentTime`.
            packet.sentTime = time
            packet.txQueuedBytes = queueSize
        elif action == "received":
            # Case 3: received a data packet
            isAllData = len(matchedGroups) == 6
            if isAllData:
                assert len(matchedGroups) == 6
                clientId, sequenceNum, length = \
                        [int(x) for x in matchedGroups[3:]]
                offset = 0
            else:
                clientId, sequenceNum, offset, length = \
                        [int(x) for x in matchedGroups[3:]]

            packetId = (isRequest, clientId, sequenceNum, offset)
            if packetId not in self.packets:
                # We don't have the TX record of this packet.
                self.packets[packetId] = Packet(packetId)
            if (packetId not in self.packets) or (who not in self.maxPollingDelays):
                # We don't have the polling start record
                self.maxPollingDelays[who] = float('NaN')

            packet = self.packets[packetId]
            packet.recipient = who
            packet.recvTime = time
            packet.length = length
            packet.isAllData = isAllData

            packet.delay = packet.recvTime - packet.sentTime
            if who not in self.startOfPoll:
                self.startOfPoll[who] = float('NaN')
                self.maxPollingDelays[who] = float('NaN')
            packet.maxPollingDelay = self.maxPollingDelays[who]
            packet.rxDelay = max(0,
                    time - self.startOfPoll[who] - MIN_RX_PACKET_COST)

            # TODO: WHAT ABOUT JITTER BEFORE PREVIOUS POLL? CAN IT AFFECT THIS PACKET?

            self.updateRpcInfo(packet)
        elif action == "sent GRANT":
            # Case 4: sending a GRANT
            clientId, sequenceNum, offset, queueSize = \
                    [int(x) for x in matchedGroups[3:]]
            packetId = (isRequest, clientId, sequenceNum,
                    offset - (NUM_RTT_PACKETS + 1) * MAX_PACKET_SIZE)
            if packetId in self.packets:
                packet = self.packets[packetId]
                packet.sentGrantTime = time
                packet.grantRespTime = time - packet.recvTime
                packet.grantTxQueuedBytes = queueSize
        else:
            # Case 5: received a GRANT
            assert(action == "received GRANT")
            clientId, sequenceNum, offset = \
                    [int(x) for x in matchedGroups[3:]]
            packetId = (isRequest, clientId, sequenceNum,
                    offset - (NUM_RTT_PACKETS + 1) * MAX_PACKET_SIZE)
            if packetId in self.packets:
                packet = self.packets[packetId]
                packet.recvGrantTime = time
                packet.grantDelay = time - packet.sentGrantTime
                packet.roundTripTime = time - packet.sentTime
                if not math.isnan(packet.grantDelay) and \
                        not math.isnan(packet.delay):
                    print "Incorrect MAX_PACKET_SIZE or NUM_RTT_PACKETS?"
                    assert(abs(packet.delay + packet.grantRespTime
                            + packet.grantDelay - packet.roundTripTime) < 1e-3)

    def updateRpcInfo(self, packet):
        '''
        Infer the sender and recipient of an RPC based on the sender
        and recipient of a data packet.
        '''
        if packet.sender is None or packet.recipient is None:
            return
        isRequest, rpcId = packet.getMessageId()
        if rpcId not in self.rpcClientServer:
            self.rpcClientServer[rpcId] = (packet.sender, packet.recipient) \
                    if isRequest else (packet.recipient, packet.sender)


if __name__ == "__main__":
    optParser = OptionParser(description=
            'Extract the per-packet delay information from a time trace file.',
            usage='%prog [options] inputFile',
            conflict_handler='resolve')
    optParser.add_option('-b', '--bandwidth', default=10, metavar='BW',
            dest='bandwidth', help='bandwidth of the NIC, in Gb/s')
    optParser.add_option('-s', '--sender', default=None, metavar='TX',
            dest='sender', help='only print packets sent from TX')
    optParser.add_option('-r', '--receiver', default=None, metavar='RX',
            dest='receiver', help='only print packets received by RX')
    optParser.add_option('--orderByRx', action='store_true', default=False,
            help='sort packets by their received time')
    (options, args) = optParser.parse_args()
    if len(args) != 1:
        print("Wrong number of arguments!")
        exit()

    bwBytesPerMicros = options.bandwidth / 8.0 * 1000.0
    print "Bandwidth = %.1f B/us" % bwBytesPerMicros
    print("# options: %s" % options)

    # Parse time trace messages
    parser = MessageParser()
    map(parser.parse, fileinput.input(args[0]))

    # Fix up missing senders/recipients if possible
    packets = parser.getPackets()
    for p in packets:
        if p.sender is None or p.recipient is None:
            isRequest, rpcId = p.getMessageId()
            if rpcId in parser.rpcClientServer:
                if isRequest:
                    p.sender, p.recipient = parser.rpcClientServer[rpcId]
                else:
                    p.recipient, p.sender = parser.rpcClientServer[rpcId]

    # Analytics
    # Step 1: compute min. delay for each packet size
    minDelay = {None : float('NaN')}
    ps = sorted([p for p in packets if p.length is not None \
            and not math.isnan(p.delay)], key=lambda p : p.length)
    for pktLength, group in groupby(ps, lambda p : p.length):
        ds = map(lambda p : p.delay, group)
        minDelay[pktLength] = min(ds)
        print "packet size %d, #samples %d, min. delay %.2f us" % \
                (pktLength, len(ds), minDelay[pktLength])

    # Step 1.5: print runtime RTT distribution
    rtts = sorted(filter(lambda x : not math.isnan(x),
            map(lambda p : p.roundTripTime, packets)))
    print "roundTripTime at runtime, min. %.2f us, median %.2f us, " \
            "90%% %.2f us, 99%% %.2f us" % (rtts[0], rtts[int(len(rtts)*0.5)],
            rtts[int(len(rtts)*0.9)], rtts[int(len(rtts)*0.99)])

    # Step 2: derive the rest of the properties
    packets = [p for p in packets if
            (options.sender is None or options.sender == p.sender) and
            (options.receiver is None or options.receiver == p.recipient)]
    for pkt in packets:
        pkt.orderTime = pkt.recvTime if options.orderByRx else pkt.sentTime
        pkt.extraDelay = pkt.delay - minDelay.get(pkt.length, float('NaN'))
        pkt.txQueueDelay = pkt.txQueuedBytes / bwBytesPerMicros
        pkt.maxTorQueueDelay = max(0, pkt.extraDelay - pkt.txQueueDelay \
                - pkt.maxPollingDelay - pkt.rxDelay)

    # Print per-packet delay
    line = 0
    if len(packets) > 0:
        packets = sorted(filter(lambda p : not math.isnan(p.orderTime), packets),
                key=lambda p : p.orderTime)
        prevTime = packets[0].orderTime
        for p in packets:
            deltaTime = p.orderTime - prevTime
            prevTime = p.orderTime
            normalize = lambda x : x / p.extraDelay if p.extraDelay > 0 else float('NaN')

            if line % 50 == 0:
                print " sender     receiver|    time     delta    delay  extra|   TX queue    TOR switch    polling      RX overhead|   RTT    resp.  G.delay  txQueue|  clientId   seq.   off.   len.  prio"
            line += 1
            print(" %s -> %s | %9.2f (+ %.2f) %6.2f %6.2f | %5.2f (%4.2f) %5.2f (%4.2f) %5.2f (%4.2f) %5.2f (%4.2f) | %6.2f %6.2f   %6.2f  %6.2f  | %s%s" % (
                    '   ?   ' if p.sender is None else p.sender,
                    '   ?   ' if p.recipient is None else p.recipient,
                    p.orderTime, deltaTime, p.delay, p.extraDelay,
                    p.txQueueDelay, normalize(p.txQueueDelay),
                    p.maxTorQueueDelay, normalize(p.maxTorQueueDelay),
                    p.maxPollingDelay, normalize(p.maxPollingDelay),
                    p.rxDelay, normalize(p.rxDelay),
                    p.roundTripTime, p.grantRespTime, p.grantDelay,
                    p.grantTxQueuedBytes / bwBytesPerMicros,
                    p.packetId[1:]+(p.length, p.priority),
                    ' S' if p.isAllData else ''))

