/* Copyright (c) 2015-2016 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */
#include <algorithm>

#include "HomaTransport.h"
#include "Service.h"
#include "TimeTrace.h"
#include "WorkerManager.h"
#include "Util.h"

namespace RAMCloud {

// Change 0 -> 1 in the following line to compile detailed time tracing in
// this transport.
#define TIME_TRACE 1

// Provides a cleaner way of invoking TimeTrace::record, with the code
// conditionally compiled in or out by the TIME_TRACE #ifdef.
namespace {
    inline void
    timeTrace(const char* format,
            uint64_t arg0 = 0, uint64_t arg1 = 0, uint64_t arg2 = 0,
            uint64_t arg3 = 0)
    {
#if TIME_TRACE
        TimeTrace::record(format, uint32_t(arg0), uint32_t(arg1),
                uint32_t(arg2), uint32_t(arg3));
#endif
    }

    inline void
    timeTrace(uint64_t timestamp, const char* format,
            uint64_t arg0 = 0, uint64_t arg1 = 0, uint64_t arg2 = 0,
            uint64_t arg3 = 0)
    {
#if TIME_TRACE
        TimeTrace::record(timestamp, format, uint32_t(arg0), uint32_t(arg1),
                uint32_t(arg2), uint32_t(arg3));
#endif
    }
}

/**
 * Construct a new HomaTransport.
 * 
 * \param context
 *      Shared state about various RAMCloud modules.
 * \param locator
 *      Service locator that contains parameters for this transport.
 * \param driver
 *      Used to send and receive packets. This transport becomes owner
 *      of the driver and will free it in when this object is deleted.
 * \param clientId
 *      Identifier that identifies us in outgoing RPCs: must be unique across
 *      all servers and clients.
 */
HomaTransport::HomaTransport(Context* context, const ServiceLocator* locator,
        Driver* driver, uint64_t clientId)
    : context(context)
    , driver(driver)
    , locatorString("homa+"+driver->getServiceLocator())
    , poller(context, this)
    , maxDataPerPacket(driver->getMaxPacketSize() - sizeof32(DataHeader))
    , clientId(clientId)

    // For now, assume we can use all the priorities supported by the driver.
    , highestAvailPriority(driver->getHighestPacketPriority())
    , highestSchedPriority(0)
    , controlPacketPriority(highestAvailPriority)
    , lowestUnschedPrio(0)
    , nextClientSequenceNumber(1)
    , nextServerSequenceNumber(1)
    , receivedPackets()
    , grantRecipients()
    , messageBuffers()
    , serverRpcPool()
    , clientRpcPool()
    , outgoingRpcs()
    , outgoingRequests()
    , topOutgoingRequests()
    , incomingRpcs()
    , outgoingResponses()
    , serverTimerList()
    , roundTripBytes(getRoundTripBytes(locator))
    , grantIncrement(maxDataPerPacket)
    , timerInterval(0)
    , nextTimeoutCheck(0)
    , timeoutCheckDeadline(0)

    // As of 7/2016, the value for timeoutIntervals is set relatively high.
    // This is needed to handle issues on some machines (such as the NEC
    // Atom cluster) where threads can get descheduled by the kernel for
    // 10-30ms. This can result in delays in handling network packets, and
    // we don't want those delays to result in RPC timeouts.
    , timeoutIntervals(40)
    , pingIntervals(3)
    , unschedTrafficPrioBrackets()
    , activeMessages()
    , inactiveMessages()
    , highestGrantedPrio(-1)
    , maxGrantedMessages(3) // TODO: why 3?
    , lastMeasureTime(0)
    , lastDispatchActiveCycles(0)
    , lastTimeGrantRunDry(0)
    , monitorInterval()
    , monitorMillis()
    , numPacketsReceived(0)
    , numDataPacketsReceived(0)
    , numControlPacketsSent(0)
    , numDataPacketsSent(0)
    , numTimesGrantRunDry(0)
    , outputControlBytes(0)
    , outputDataBytes(0)
    , outputResentBytes(0)
    , processPacketCycles(0)
    , transmitDataCycles(0)
    , transmitGrantCycles(0)
    , unusedBandwidth(0)
{
    // Set up the timer to trigger at 2 ms intervals. We use this choice
    // (as of 11/2015) because the Linux kernel appears to buffer packets
    // for up to about 1 ms before delivering them to applications. Shorter
    // intervals result in unnecessary retransmissions.
    timerInterval = Cycles::fromMicroseconds(2000);
    nextTimeoutCheck = Cycles::rdtsc() + timerInterval;

    // Measure network throughput at 1 ms intervals.
    monitorMillis = 1;
    monitorInterval = Cycles::fromMicroseconds(1000*monitorMillis);

    // If we are allowed to use more than one priority, split the available
    // priorities equally between unscheduled and scheduled traffic.
    if (highestAvailPriority > 0) {
        lowestUnschedPrio = (highestAvailPriority + 1) >> 1;
        highestSchedPriority = lowestUnschedPrio - 1;
        maxGrantedMessages = std::min(maxGrantedMessages,
                downCast<uint32_t>(highestSchedPriority + 1));
    }

    // Set up the initial unscheduled traffic priority brackets for messages.
    // TODO: better name for brackets; and better name for unschedTrafficPrioBrackets...
    string brackets = "[0";
    int numUnschedPrio = highestAvailPriority - lowestUnschedPrio + 1;
    uint32_t nextCutoff = driver->getMaxPacketSize();
    for (int i = 0; i < numUnschedPrio - 1; i++) {
        unschedTrafficPrioBrackets.push_back(nextCutoff + 1);
        brackets += format(", %u] [%u", nextCutoff, nextCutoff + 1);
        nextCutoff <<= 1;
    }
    unschedTrafficPrioBrackets.push_back(~0u);
    brackets += format(", %u]", ~0u);

    LOG(NOTICE, "HomaTransport parameters: clientId %lu, maxDataPerPacket %u, "
            "roundTripBytes %u, grantIncrement %u, pingIntervals %d, "
            "timeoutIntervals %d, timerInterval %.2f ms, "
            "monitorInterval %.2f ms, "
            "highestAvailPriority %d, lowestUnschedPriority %d, "
            "highestSchedPriority %d, unscheduledTrafficPrioBrackets %s",
            clientId, maxDataPerPacket, roundTripBytes,
            grantIncrement, pingIntervals, timeoutIntervals,
            Cycles::toSeconds(timerInterval)*1e3,
            Cycles::toSeconds(monitorInterval)*1e3,
            highestAvailPriority, lowestUnschedPrio, highestSchedPriority,
            brackets.c_str());
}

/**
 * Destructor for HomaTransports.
 */
HomaTransport::~HomaTransport()
{
    // This cleanup is mostly for the benefit of unit tests: in production,
    // this destructor is unlikely ever to get called.

    // Reclaim all of the RPC objects.
    for (ServerRpcMap::iterator it = incomingRpcs.begin();
            it != incomingRpcs.end(); ) {
        ServerRpc* serverRpc = it->second;

        // Advance iterator; otherwise it will get invalidated by
        // deleteServerRpc.
        it++;
        deleteServerRpc(serverRpc);
    }
    for (ClientRpcMap::iterator it = outgoingRpcs.begin();
            it != outgoingRpcs.end(); ) {
        ClientRpc* clientRpc = it->second;

        // Advance iterator; otherwise it will get invalidated by
        // deleteClientRpc.
        it++;
        deleteClientRpc(clientRpc);
    }

    // Release all retained payloads after reclaiming all RPC objects.
    for (MessageAccumulator::MessageBuffer* messageBuffer : messageBuffers) {
        for (char* payload : *messageBuffer) {
            driver->release(payload);
        }
    }
}

// See Transport::getServiceLocator().
string
HomaTransport::getServiceLocator()
{
    return locatorString;
}

/**
 * When we are finished processing an outgoing RPC, this method is
 * invoked to delete the ClientRpc object and remove it from all
 * existing data structures.
 *
 * \param clientRpc
 *      An RPC that has either completed normally or is being
 *      aborted.
 */
void
HomaTransport::deleteClientRpc(ClientRpc* clientRpc)
{
    uint64_t sequence = clientRpc->sequence;
    TEST_LOG("RpcId %lu", sequence);
    outgoingRpcs.erase(sequence);
    if (clientRpc->transmitPending) {
        erase(outgoingRequests, *clientRpc);
    }
    if (clientRpc->topOutgoingRequest >= 0) {
        removeTopOutgoingRequest(clientRpc);
    }
    clientRpcPool.destroy(clientRpc);
    timeTrace("deleted client RPC, clientId %u, sequence %u",
            clientId, sequence);
}

/**
 * When we are finished processing an incoming RPC, this method is
 * invoked to delete the RPC object and remove it from all existing
 * data structures.
 * 
 * \param serverRpc
 *      An RPC that has either completed normally or should be
 *      aborted.
 */
void
HomaTransport::deleteServerRpc(ServerRpc* serverRpc)
{
    uint64_t sequence = serverRpc->rpcId.sequence;
    TEST_LOG("RpcId (%lu, %lu)", serverRpc->rpcId.clientId,
            sequence);
    // TODO: Profile how long it takes; might have to optimize it
    incomingRpcs.erase(serverRpc->rpcId);
    if (serverRpc->sendingResponse) {
        erase(outgoingResponses, *serverRpc);
    }
    if (serverRpc->sendingResponse || !serverRpc->requestComplete) {
        erase(serverTimerList, *serverRpc);
    }
    serverRpcPool.destroy(serverRpc);
    timeTrace("deleted server RPC, clientId %u, sequence %u",
            serverRpc->rpcId.clientId, sequence);
}

/**
 * Parse option values in a service locator to determine how many bytes
 * of data must be sent to cover the round-trip latency of a connection.
 * The result is rounded up to the next multiple of the packet size.
 *
 * \param locator
 *      Service locator that may contain "gbs" and "rttMicros" options.
 *      If NULL, or if any of the options  are missing, then defaults
 *      are supplied.  Note: as of 8/2016 these options don't work very well
 *      because they are only visible to servers, not clients.
 */
uint32_t
HomaTransport::getRoundTripBytes(const ServiceLocator* locator)
{
    uint32_t gBitsPerSec = 0;
    // TODO: The RTT in the m510 cluster is more like 8us (5 us of data packet
    // propagation delay plus 1 us of service time plus 1 us of grant packet
    // propagation delay). Figure out how to set "rttMicros" from command line.
    uint32_t roundTripMicros = 8;
//    uint32_t roundTripMicros = 7;

    if (locator  != NULL) {
        if (locator->hasOption("gbs")) {
            char* end;
            uint32_t value = downCast<uint32_t>(strtoul(
                    locator->getOption("gbs").c_str(), &end, 10));
            if ((*end == 0) && (value != 0)) {
                gBitsPerSec = value;
            } else {
                LOG(ERROR, "Bad HomaTransport gbs option value '%s' "
                        "(expected positive integer); ignoring option",
                        locator->getOption("gbs").c_str());
            }
        }
        if (locator->hasOption("rttMicros")) {
            char* end;
            uint32_t value = downCast<uint32_t>(strtoul(
                    locator->getOption("rttMicros").c_str(), &end, 10));
            if ((*end == 0) && (value != 0)) {
                roundTripMicros = value;
            } else {
                LOG(ERROR, "Bad HomaTransport rttMicros option value '%s' "
                        "(expected positive integer); ignoring option",
                        locator->getOption("rttMicros").c_str());
            }
        }
    }
    if (gBitsPerSec == 0) {
        gBitsPerSec = driver->getBandwidth() / 1000;
        if (gBitsPerSec == 0) {
            gBitsPerSec = 10;
        }
    }

    // Compute round-trip time in terms of full packets (round up).
    uint32_t roundTripBytes = (roundTripMicros*gBitsPerSec*1000)/8;
    roundTripBytes = ((roundTripBytes+maxDataPerPacket-1)/maxDataPerPacket)
            * maxDataPerPacket;
    return roundTripBytes;
}

/**
 * Decides which packet priority should be used to transmit the unscheduled
 * portion of a message.
 *
 * \param messageSize
 *      The size of the message to be transmitted.
 * \return
 *      The packet priority to use.
 */
uint8_t
HomaTransport::getUnschedTrafficPrio(uint32_t messageSize) {
    int numUnschedPrio = highestAvailPriority - lowestUnschedPrio + 1;
    for (int i = 0; i < numUnschedPrio - 1; i++) {
        if (messageSize < unschedTrafficPrioBrackets[i]) {
            return downCast<uint8_t>(highestAvailPriority - i);
        }
    }
    return downCast<uint8_t>(lowestUnschedPrio);
}

/**
 * Return a printable symbol for the opcode field from a packet.
 * \param opcode
 *     Opcode field from a packet.
 * \return
 *     The result is a static string, which may change on the next
 *
 */
string
HomaTransport::opcodeSymbol(uint8_t opcode) {
    switch (opcode) {
        case HomaTransport::PacketOpcode::ALL_DATA:
            return "ALL_DATA";
        case HomaTransport::PacketOpcode::DATA:
            return "DATA";
        case HomaTransport::PacketOpcode::GRANT:
            return "GRANT";
        case HomaTransport::PacketOpcode::LOG_TIME_TRACE:
            return "LOG_TIME_TRACE";
        case HomaTransport::PacketOpcode::RESEND:
            return "RESEND";
        case HomaTransport::PacketOpcode::ACK:
            return "ACK";
    }

    return format("%d", opcode);
}

/**
 * This method takes care of packet sizing and transmitting message data,
 * both for requests and for responses. When a method returns, the given
 * range of data will have been queued for the NIC but may not actually
 * have been transmitted yet.
 * 
 * \param address
 *      Identifies the destination for the message.
 * \param rpcId
 *      Unique identifier for the RPC.
 * \param message
 *      Contains the entire message.
 * \param offset
 *      Offset in bytes of the first byte to be transmitted.
 * \param maxBytes
 *      Maximum number of bytes to transmit. If offset + maxBytes exceeds
 *      the message length, then all of the remaining bytes in message,
 *      will be transmitted.
 * \param unscheduledBytes
 *      Unscheduled bytes sent unilaterally in this message.
 * \param priority
 *      Priority used to send the packets.
 * \param flags
 *      Extra flags to set in packet headers, such as FROM_CLIENT or
 *      RETRANSMISSION. Must at least specify either FROM_CLIENT or
 *      FROM_SERVER.
 * \param partialOK
 *      Normally, a partial packet will get sent only if it's the last
 *      packet in the message. However, if this parameter is true then
 *      partial packets will be sent anywhere in the message.
 * \return
 *      The number of bytes of data actually transmitted (may be 0 in
 *      some situations).
 */
uint32_t
HomaTransport::sendBytes(const Driver::Address* address, RpcId rpcId,
        Buffer* message, uint32_t offset, uint32_t maxBytes,
        uint32_t unscheduledBytes, uint8_t priority, uint8_t flags,
        bool partialOK)
{
    uint32_t messageSize = message->size();

    uint32_t curOffset = offset;
    uint32_t bytesSent = 0;
    while ((curOffset < messageSize) && (bytesSent < maxBytes)) {
        // Don't send less-than-full-size packets except for the last packet
        // of the message (unless the caller explicitly requested it).
        uint32_t bytesThisPacket =
                std::min(maxDataPerPacket, messageSize - curOffset);
        if ((bytesSent + bytesThisPacket) > maxBytes) {
            if (!partialOK) {
                break;
            }
            bytesThisPacket = maxBytes - bytesSent;
        }
        if (bytesThisPacket == messageSize) {
            // Entire message fits in a single packet.
            AllDataHeader header(rpcId, flags,
                    downCast<uint16_t>(messageSize));
            Buffer::Iterator iter(message, 0, messageSize);
            const char* fmt = (flags & FROM_CLIENT) ?
                    "client sending ALL_DATA, clientId %u, sequence %u, "
                    "priority %u" :
                    "server sending ALL_DATA, clientId %u, sequence %u, "
                    "priority %u";
            timeTrace(fmt, rpcId.clientId, rpcId.sequence, priority);
            driver->sendPacket(address, &header, &iter, priority);
        } else {
            DataHeader header(rpcId, message->size(), curOffset,
                    unscheduledBytes, flags);
            Buffer::Iterator iter(message, curOffset, bytesThisPacket);
            const char* fmt = (flags & FROM_CLIENT) ?
                    "client sending DATA, clientId %u, sequence %u, "
                    "offset %u, priority %u" :
                    "server sending DATA, clientId %u, sequence %u, "
                    "offset %u, priority %u";
            timeTrace(fmt, rpcId.clientId, rpcId.sequence, curOffset,
                    priority);
            driver->sendPacket(address, &header, &iter, priority);
        }
        bytesSent += bytesThisPacket;
        curOffset += bytesThisPacket;

        uint32_t bytesQueuedAhead = driver->getLastQueueingDelay();
        if (bytesQueuedAhead > 0) {
            timeTrace(driver->getLastTransmitTime(),
                    "sent data, clientId %u, sequence %u, offset %u, "
                    "%u bytes queued ahead", rpcId.clientId, rpcId.sequence,
                    curOffset, bytesQueuedAhead);
        } else {
            uint64_t idleInterval = driver->getTxQueueIdleInterval();
            unusedBandwidth += idleInterval;
            timeTrace(driver->getLastTransmitTime(),
                    "sent data, clientId %u, sequence %u, offset %u, "
                    "0 bytes queued ahead, idle time %u cyc",
                    rpcId.clientId, rpcId.sequence, curOffset, idleInterval);
        }

        // Update performance monitor metrics.
        numDataPacketsSent++;
        outputDataBytes += bytesThisPacket;
        if (flags & RETRANSMISSION) {
            outputResentBytes += bytesThisPacket;
        }
    }

    return bytesSent;
}

/**
 * Send out a single control packet.
 *
 * \param recipient
 *      Where to send the packet.
 * \param packet
 *      Address of the first byte of the control packet header.
 */
template<typename T>
void
HomaTransport::sendControlPacket(const Driver::Address* recipient,
        const T* packet)
{
    driver->sendPacket(recipient, packet, NULL, controlPacketPriority);
    uint64_t idleInterval = driver->getTxQueueIdleInterval();
    if (idleInterval > 0) {
        timeTrace("sent control packet, idle time %u cyc", idleInterval);
        unusedBandwidth += idleInterval;
    }
    outputControlBytes += static_cast<uint32_t>(sizeof(T));
    numControlPacketsSent++;
}

/**
 * Given a pointer to a HomaTransport packet, return a human-readable
 * string describing the information in its header.
 * 
 * \param packet
 *      Address of the first byte of the packet header, which must be
 *      contiguous in memory.
 * \param packetLength
 *      Size of the header, in bytes.
 */
string
HomaTransport::headerToString(const void* packet, uint32_t packetLength)
{
    string result;
    const HomaTransport::CommonHeader* common =
            static_cast<const HomaTransport::CommonHeader*>(packet);
    uint32_t headerLength = sizeof32(HomaTransport::CommonHeader);
    if (packetLength < headerLength) {
        goto packetTooShort;
    }
    result += HomaTransport::opcodeSymbol(common->opcode);
    if (common->flags & HomaTransport::FROM_CLIENT) {
        result += " FROM_CLIENT";
    } else {
        result += " FROM_SERVER";
    }
    result += format(", rpcId %lu.%lu",
            common->rpcId.clientId, common->rpcId.sequence);
    switch (common->opcode) {
        case HomaTransport::PacketOpcode::ALL_DATA:
            headerLength = sizeof32(HomaTransport::AllDataHeader);
            if (packetLength < headerLength) {
                goto packetTooShort;
            }
            break;
        case HomaTransport::PacketOpcode::DATA: {
            headerLength = sizeof32(HomaTransport::DataHeader);
            if (packetLength < headerLength) {
                goto packetTooShort;
            }
            const HomaTransport::DataHeader* data =
                    static_cast<const HomaTransport::DataHeader*>(packet);
            result += format(", totalLength %u, offset %u%s",
                    data->totalLength, data->offset,
                    common->flags & HomaTransport::RETRANSMISSION
                            ? ", RETRANSMISSION" : "");
            break;
        }
        case HomaTransport::PacketOpcode::GRANT: {
            headerLength = sizeof32(HomaTransport::GrantHeader);
            if (packetLength < headerLength) {
                goto packetTooShort;
            }
            const HomaTransport::GrantHeader* grant =
                    static_cast<const HomaTransport::GrantHeader*>(packet);
            result += format(", offset %u", grant->offset);
            // TODO: change to the following and fix unit tests
//            result += format(", offset %u, priority %u", grant->offset,
//                    grant->priority);
            break;
        }
        case HomaTransport::PacketOpcode::LOG_TIME_TRACE:
            headerLength = sizeof32(HomaTransport::LogTimeTraceHeader);
            if (packetLength < headerLength) {
                goto packetTooShort;
            }
            break;
        case HomaTransport::PacketOpcode::RESEND: {
            headerLength = sizeof32(HomaTransport::ResendHeader);
            if (packetLength < headerLength) {
                goto packetTooShort;
            }
            const HomaTransport::ResendHeader* resend =
                    static_cast<const HomaTransport::ResendHeader*>(
                    packet);
            result += format(", offset %u, length %u%s",
                    resend->offset, resend->length,
                    common->flags & HomaTransport::RESTART
                            ? ", RESTART" : "");
            break;
        }
        case HomaTransport::PacketOpcode::ACK:
            headerLength = sizeof32(HomaTransport::AckHeader);
            if (packetLength < headerLength) {
                goto packetTooShort;
            }
            break;
    }
    return result;

  packetTooShort:
    if (!result.empty()) {
        result += ", ";
    }
    result += format("packet too short (got %u bytes, need at least %u)",
            packetLength, headerLength);
    return result;
}

/**
 * This method queues one or more data packets for transmission, if (a) the
 * NIC queue isn't too long and (b) there is data that needs to be transmitted.
 * \return
 *      Total number of bytes transmitted.
 */
uint32_t
HomaTransport::tryToTransmitData()
{
    uint32_t totalBytesSent = 0;

    // Check to see if we can transmit any data packets. The overall goal
    // here is not to enqueue too many data packets at the NIC at once; this
    // allows us to preempt long messages with shorter ones, and data
    // packets with control packets. The code here only handles data packets;
    // control packets (and retransmitted data) are always passed to the
    // driver immediately.

    uint32_t transmitQueueSpace = static_cast<uint32_t>(
            driver->getTransmitQueueSpace(context->dispatch->currentTime));
    if (static_cast<int>(transmitQueueSpace) < 0) {
        return 0;
    }
    uint32_t maxBytes;
    uint32_t grantedBytesLeft;

    // Each iteration of the following loop transmits data packets for
    // a single request or response.
    while (true) {
        // Find an outgoing request or response that is ready to transmit.
        // The policy here is "shortest remaining processing time" (SRPT).

        // Note: this code used to use std::maps instead of lists; the maps
        // were sorted by message length to avoid the cost of scanning
        // all RPCs. However, the maps had a very high insertion cost
        // (~50ns) even when empty, so it is faster overall to use lists.
        // If these lists were to become very long, then this decision made
        // need to be revisited.
        ClientRpc* clientRpc = NULL;
        ServerRpc* serverRpc = NULL;
        uint32_t minBytesLeft = ~0u;
        if (arrayLength(topOutgoingRequests) > 0) {

        for (ClientRpc* rpc : topOutgoingRequests) {
            if (rpc != NULL) {
                if (rpc->transmitLimit <= rpc->transmitOffset) {
                    // Can't transmit this message: waiting for grants.
                    continue;
                }
                uint32_t bytesLeft =
                        rpc->request->size() - rpc->transmitOffset;
                if (bytesLeft < minBytesLeft) {
                    minBytesLeft = bytesLeft;
                    clientRpc = rpc;
                }
            }
        }
//#define unlikely(x) __glibc_unlikely(x)
//        if (unlikely(NULL == clientRpc && !outgoingRequests.empty())) {
//            timeTrace("slow path taken, iterating over %u outgoing requests",
//                    outgoingRequests.size());
//            for (OutgoingRequestList::iterator it = outgoingRequests.begin();
//                        it != outgoingRequests.end(); it++) {
//                ClientRpc* rpc = &(*it);
//                if (rpc->transmitLimit <= rpc->transmitOffset) {
//                    // Can't transmit this message: waiting for grants.
//                    continue;
//                }
//                uint32_t bytesLeft =
//                        rpc->request->size() - rpc->transmitOffset;
//                if (bytesLeft < minBytesLeft) {
//                    minBytesLeft = bytesLeft;
//                    clientRpc = rpc;
//                }
//            }
//        }

        } else {
            for (OutgoingRequestList::iterator it = outgoingRequests.begin();
                        it != outgoingRequests.end(); it++) {
                ClientRpc* rpc = &(*it);
                if (rpc->transmitLimit <= rpc->transmitOffset) {
                    // Can't transmit this message: waiting for grants.
                    continue;
                }
                uint32_t bytesLeft = rpc->request->size() - rpc->transmitOffset;
                if (bytesLeft < minBytesLeft) {
                    minBytesLeft = bytesLeft;
                    clientRpc = rpc;
                }
            }
        }

        for (OutgoingResponseList::iterator it = outgoingResponses.begin();
                    it != outgoingResponses.end(); it++) {
            ServerRpc* rpc = &(*it);
            if (rpc->transmitLimit <= rpc->transmitOffset) {
                // Can't transmit this message: waiting for grants.
                continue;
            }
            uint32_t bytesLeft = rpc->replyPayload.size() -
                    rpc->transmitOffset;
            if (bytesLeft < minBytesLeft) {
                minBytesLeft = bytesLeft;
                serverRpc = rpc;
                clientRpc = NULL;
            }
        }

        uint32_t bytesSent = 0;
        if (clientRpc != NULL) {
            // Transmit one or more request DATA packets from clientRpc,
            // if appropriate.
            grantedBytesLeft = std::min(clientRpc->transmitLimit,
                    clientRpc->request->size()) - clientRpc->transmitOffset;
            if (transmitQueueSpace < grantedBytesLeft) {
                if (transmitQueueSpace < maxDataPerPacket) {
                    break;
                }
                maxBytes = transmitQueueSpace;
            } else {
                maxBytes = grantedBytesLeft;
            }
            bytesSent = sendBytes(
                    clientRpc->session->serverAddress,
                    RpcId(clientId, clientRpc->sequence),
                    clientRpc->request, clientRpc->transmitOffset, maxBytes,
                    clientRpc->unscheduledBytes, clientRpc->transmitPriority,
                    FROM_CLIENT);
            clientRpc->transmitOffset += bytesSent;
            clientRpc->lastTransmitTime = driver->getLastTransmitTime();
            transmitQueueSpace -= bytesSent;
            totalBytesSent += bytesSent;
            if (clientRpc->transmitOffset >= clientRpc->request->size()) {
                erase(outgoingRequests, *clientRpc);
                clientRpc->transmitPending = false;
                if (clientRpc->topOutgoingRequest >= 0) {
                    removeTopOutgoingRequest(clientRpc);
                }
            } else if (clientRpc->topOutgoingRequest < 0) {
                tryToPromote(clientRpc);
            }
        } else if (serverRpc != NULL) {
            // Transmit one or more response DATA packets from serverRpc,
            // if appropriate.
            grantedBytesLeft = std::min(serverRpc->transmitLimit,
                    serverRpc->replyPayload.size()) - serverRpc->transmitOffset;
            if (transmitQueueSpace < grantedBytesLeft) {
                if (transmitQueueSpace < maxDataPerPacket) {
                    break;
                }
                maxBytes = transmitQueueSpace;
            } else {
                maxBytes = grantedBytesLeft;
            }
            bytesSent = sendBytes(serverRpc->clientAddress,
                    serverRpc->rpcId, &serverRpc->replyPayload,
                    serverRpc->transmitOffset, maxBytes,
                    serverRpc->unscheduledBytes, serverRpc->transmitPriority,
                    FROM_SERVER);
            serverRpc->transmitOffset += bytesSent;
            serverRpc->lastTransmitTime = driver->getLastTransmitTime();
            transmitQueueSpace -= bytesSent;
            totalBytesSent += bytesSent;
            if (serverRpc->transmitOffset >= serverRpc->replyPayload.size()) {
                // Delete the ServerRpc object as soon as we have transmitted
                // the last byte. This has the disadvantage that if some of
                // this data is lost we won't be able to retransmit it (the
                // whole RPC will be retried). However, this approach is
                // simpler and faster in the common case where data isn't lost.
                deleteServerRpc(serverRpc);
            }
        } else {
            // There are no messages with data that can be transmitted.
            uint64_t currentTime = context->dispatch->currentTime;
            // TODO: only correct on m510
            const uint32_t cyclesPerPacket = 2500;
            if ((!outgoingRequests.empty() || !outgoingResponses.empty())
                    && (totalBytesSent == 0)
                    && (lastTimeGrantRunDry + cyclesPerPacket < currentTime)) {
                uint64_t now = Cycles::rdtsc();
                // transmitQueueSpace is computed w.r.t. a stale timestamp.
                // It's too imprecise for the purpose of analyzing bandwidth
                // wasted in waiting for grants.
                bool transmitQueueEmpty = (driver->getTransmitQueueSpace(now)
                        == (int)driver->getMaxTransmitQueueSize());
                if (transmitQueueEmpty) {
                    timeTrace(now, "not enough GRANTs to transmit data");
                    numTimesGrantRunDry++;
                    lastTimeGrantRunDry = currentTime;
                }
            }
            break;
        }
    }

    return totalBytesSent;
}

/**
 * Construct a new client session.
 *
 * \throw TransportException
 *      The service locator couldn't be parsed (a log message will
 *      have been generated already).
 */
HomaTransport::Session::Session(HomaTransport* t,
        const ServiceLocator* locator, uint32_t timeoutMs)
    : Transport::Session(locator->getOriginalString())
    , t(t)
    , serverAddress(NULL)
    , aborted(false)
{
    try {
        serverAddress = t->driver->newAddress(locator);
    }
    catch (const Exception& e) {
        LOG(NOTICE, "%s", e.message.c_str());
        throw TransportException(HERE,
                "HomaTransport couldn't parse service locator");
    }
}

/**
 * Destructor for client sessions.
 */
HomaTransport::Session::~Session()
{
    abort();
    delete serverAddress;
}

// See Transport::Session::abort for docs.
void
HomaTransport::Session::abort()
{
    aborted = true;
    for (ClientRpcMap::iterator it = t->outgoingRpcs.begin();
            it != t->outgoingRpcs.end(); ) {
        ClientRpc* clientRpc = it->second;
        it++;
        if (clientRpc->session == this) {
            t->deleteClientRpc(clientRpc);
        }
    }
}

// See Transport::Session::cancelRequest for docs.
void
HomaTransport::Session::cancelRequest(RpcNotifier* notifier)
{
    for (ClientRpcMap::iterator it = t->outgoingRpcs.begin();
            it != t->outgoingRpcs.end(); it++) {
        ClientRpc* clientRpc = it->second;
        if (clientRpc->notifier == notifier) {
            t->deleteClientRpc(clientRpc);

            // It's no longer safe to use "it", but at this point we're
            // done (the RPC can't exist in the list twice).
            return;
        }
    }
}

// See Transport::Session::getRpcInfo for docs.
string
HomaTransport::Session::getRpcInfo()
{
    string result;
    for (ClientRpcMap::iterator it = t->outgoingRpcs.begin();
            it != t->outgoingRpcs.end(); it++) {
        ClientRpc* clientRpc = it->second;
        if (clientRpc->session != this) {
            continue;
        }
        if (result.size() != 0) {
            result += ", ";
        }
        result += WireFormat::opcodeSymbol(clientRpc->request);
    }
    if (result.empty())
        result = "no active RPCs";
    result += " to server at ";
    result += serviceLocator;
    return result;
}

void
HomaTransport::removeTopOutgoingRequest(ClientRpc *clientRpc) {
    int k = clientRpc->topOutgoingRequest;
    assert(0 <= k && k < arrayLength(topOutgoingRequests));
    clientRpc->topOutgoingRequest = -1;
    topOutgoingRequests[k] = NULL;
}

void
HomaTransport::tryToPromote(ClientRpc *clientRpc)
{
    assert(clientRpc->topOutgoingRequest == -1);
    uint32_t bytesLeft = 0;
    uint32_t maxBytesLeft = 0;
    int i = 0;
    int k = -1;
    for (ClientRpc* rpc : topOutgoingRequests) {
        if (NULL == rpc) {
            topOutgoingRequests[i] = clientRpc;
            clientRpc->topOutgoingRequest = i;
            return;
        }
        bytesLeft = rpc->request->size() - rpc->transmitOffset;
        if (maxBytesLeft < bytesLeft) {
            maxBytesLeft = bytesLeft;
            k = i;
        }
        i++;
    }

    bytesLeft = clientRpc->request->size() - clientRpc->transmitOffset;
    if (maxBytesLeft > bytesLeft) {
        topOutgoingRequests[k]->topOutgoingRequest = -1;
        topOutgoingRequests[k] = clientRpc;
        clientRpc->topOutgoingRequest = k;
    }
}

// See Transport::Session::sendRequest for docs.
void
HomaTransport::Session::sendRequest(Buffer* request, Buffer* response,
                RpcNotifier* notifier)
{
    timeTrace("sendRequest invoked, clientId %u, sequence %u, length %u, "
            "%u outgoing requests", t->clientId, t->nextClientSequenceNumber,
            request->size(), t->outgoingRequests.size());
//    timeTrace("request opcode %u",
//            request->getStart<WireFormat::RequestCommon>()->opcode);
    response->reset();
    if (aborted) {
        notifier->failed();
        return;
    }
    ClientRpc *clientRpc = t->clientRpcPool.construct(this,
            t->nextClientSequenceNumber, request, response, notifier);
    clientRpc->transmitLimit = t->roundTripBytes;
    clientRpc->unscheduledBytes = t->roundTripBytes;
    clientRpc->transmitPriority = t->getUnschedTrafficPrio(request->size());
    t->outgoingRpcs[t->nextClientSequenceNumber] = clientRpc;
    t->outgoingRequests.push_back(*clientRpc);
    clientRpc->transmitPending = true;
    t->tryToPromote(clientRpc);
    t->nextClientSequenceNumber++;
    uint32_t bytesSent = t->tryToTransmitData();
    if (bytesSent > 0) {
        timeTrace("sendRequest trasmitted %u bytes", bytesSent);
    }
}

/**
 * This method is invoked whenever a packet arrives. It is the top-level
 * dispatching method for dealing with incoming packets, both for requests
 * and responses.
 * 
 * \param received
 *      Information about the new packet.
 */
void
HomaTransport::handlePacket(Driver::Received* received)
{
    // The following method retrieves a header from a packet
    CommonHeader* common = received->getOffset<CommonHeader>(0);
    if (common == NULL) {
        RAMCLOUD_CLOG(WARNING, "packet from %s too short (%u bytes)",
                received->sender->toString().c_str(), received->len);
        return;
    }
    if (common->opcode == ALL_DATA || common->opcode == DATA) {
        numDataPacketsReceived++;
    }

    if (!(common->flags & FROM_CLIENT)) {
        // This packet was sent by the server, and it pertains to an RPC
        // for which we are the client.
        ClientRpcMap::iterator it = outgoingRpcs.find(
                common->rpcId.sequence);
        if (it == outgoingRpcs.end()) {
            // We have no record of this RPC; most likely this packet
            // pertains to an earlier RPC that we've already finished
            // with (e.g., we might have sent a RESEND just before the
            // server since the response). Discard the packet.
            if (common->opcode == LOG_TIME_TRACE) {
                // For LOG_TIME_TRACE requests, dump the trace anyway.
                LOG(NOTICE, "Client received LOG_TIME_TRACE request from "
                        "server %s for (unknown) sequence %lu",
                        received->sender->toString().c_str(),
                        common->rpcId.sequence);
                timeTrace("client received LOG_TIME_TRACE for clientId %u, "
                        "sequence %u",
                        common->rpcId.clientId, common->rpcId.sequence);
                TimeTrace::printToLogBackground(context->dispatch);
            }
            TEST_LOG("Discarding unknown packet, sequence %lu",
                    common->rpcId.sequence);
            return;
        }
        ClientRpc* clientRpc = it->second;
        clientRpc->silentIntervals = 0;
        switch (common->opcode) {
            // ALL_DATA from server
            case PacketOpcode::ALL_DATA: {
                // This RPC is now finished.
                AllDataHeader* header = received->getOffset<AllDataHeader>(0);
                if (header == NULL)
                    goto packetLengthError;
                uint32_t length;
                char *payload = received->steal(&length);
                uint32_t requiredLength =
                        downCast<uint32_t>(header->messageLength) +
                        sizeof32(AllDataHeader);
                if (length < requiredLength) {
                    RAMCLOUD_CLOG(WARNING, "ALL_DATA response from %s too "
                            "short (got %u bytes, expected %u)",
                            received->sender->toString().c_str(),
                            length, requiredLength);
                    driver->release(payload);
                    return;
                }
                timeTrace("client received ALL_DATA, clientId %u, sequence %u, "
                        "length %u", header->common.rpcId.clientId,
                        header->common.rpcId.sequence, length);
                Driver::PayloadChunk::appendToBuffer(clientRpc->response,
                        payload + sizeof32(AllDataHeader),
                        header->messageLength, driver, payload);
                clientRpc->notifier->completed();
                deleteClientRpc(clientRpc);
                return;
            }

            // DATA from server
            case PacketOpcode::DATA: {
                DataHeader* header = received->getOffset<DataHeader>(0);
                if (header == NULL)
                    goto packetLengthError;
                timeTrace("client received DATA, clientId %u, sequence %u, "
                        "offset %u, length %u",
                        header->common.rpcId.clientId,
                        header->common.rpcId.sequence,
                        header->offset, received->len);
                if (!clientRpc->accumulator) {
                    clientRpc->accumulator.construct(this, clientRpc->response,
                            uint32_t(header->totalLength));
                    if (header->totalLength > header->unscheduledBytes) {
                        clientRpc->scheduledMessage.construct(
                                clientRpc->rpcId, clientRpc->accumulator.get(),
                                uint32_t(header->unscheduledBytes),
                                clientRpc->session->serverAddress,
                                static_cast<uint8_t>(FROM_SERVER));
                    }
                }
                bool retainPacket = clientRpc->accumulator->addPacket(header,
                        received->len);
                if (clientRpc->scheduledMessage) {
                    bool scheduledPacket = (header->offset + received->len >
                            header->unscheduledBytes);
                    dataArriveForScheduledMessage(
                            clientRpc->scheduledMessage.get(),
                            scheduledPacket);
                }
                if (clientRpc->response->size() >= header->totalLength) {
                    // Response complete.
                    if (clientRpc->response->size() > header->totalLength) {
                        // We have more bytes than we want. This can happen
                        // if the last packet gets padded by the network
                        // layer to meet minimum size requirements. Just
                        // truncate the response.
                        clientRpc->response->truncate(header->totalLength);
                    }
                    clientRpc->notifier->completed();
                    deleteClientRpc(clientRpc);
                }

                if (retainPacket) {
                    uint32_t dummy;
                    received->steal(&dummy);
                }
                return;
            }

            // GRANT from server
            case PacketOpcode::GRANT: {
                GrantHeader* header = received->getOffset<GrantHeader>(0);
                if (header == NULL)
                    goto packetLengthError;
                timeTrace("client received GRANT, clientId %u, sequence %u, "
                        "offset %u",
                        header->common.rpcId.clientId,
                        header->common.rpcId.sequence,
                        header->offset);
                if (header->offset > clientRpc->transmitLimit) {
                    clientRpc->transmitLimit = header->offset;
                    clientRpc->transmitPriority = header->priority;
                }
                return;
            }

            // LOG_TIME_TRACE from server
            case PacketOpcode::LOG_TIME_TRACE: {
                LOG(NOTICE, "Client received LOG_TIME_TRACE request from "
                        "server %s for clientId %lu, sequence %lu",
                        received->sender->toString().c_str(),
                        common->rpcId.clientId, common->rpcId.sequence);
                timeTrace("client received LOG_TIME_TRACE for clientId %u, "
                        "sequence %u",
                        common->rpcId.clientId, common->rpcId.sequence);
                TimeTrace::printToLogBackground(context->dispatch);
                return;
            }

            // RESEND from server
            case PacketOpcode::RESEND: {
                ResendHeader* header = received->getOffset<ResendHeader>(0);
                if (header == NULL)
                    goto packetLengthError;
                timeTrace("client received RESEND, clientId %u, sequence %u, "
                        "offset %u, length %u",
                        header->common.rpcId.clientId,
                        header->common.rpcId.sequence,
                        header->offset, header->length);
                if (header->common.flags & RESTART) {
                    clientRpc->response->reset();
                    clientRpc->transmitOffset = 0;
                    clientRpc->transmitLimit = header->length;
                    clientRpc->accumulator.destroy();
                    clientRpc->scheduledMessage.destroy();
                    if (!clientRpc->transmitPending) {
                        clientRpc->transmitPending = true;
                        outgoingRequests.push_back(*clientRpc);
                        tryToPromote(clientRpc);
                    } else {
                        if (clientRpc->topOutgoingRequest >= 0) {
                            removeTopOutgoingRequest(clientRpc);
                            tryToPromote(clientRpc);
                        }
                    }
                    return;
                }
                uint32_t resendEnd = header->offset + header->length;
                if (resendEnd > clientRpc->transmitLimit) {
                    // Needed in case a GRANT packet was lost.
                    clientRpc->transmitLimit = resendEnd;
                }
                if ((header->offset >= clientRpc->transmitOffset)
                        || ((Cycles::rdtsc() - clientRpc->lastTransmitTime)
                        < timerInterval)) {
                    // One of two things has happened: either (a) we haven't
                    // yet sent the requested bytes for the first time (there
                    // must be other outgoing traffic with higher priority)
                    // or (b) we transmitted data recently. In either case,
                    // it's unlikely that bytes have been lost, so don't
                    // retransmit; just return an ACK so the server knows
                    // we're still alive.
                    AckHeader ack(header->common.rpcId, FROM_CLIENT);
                    sendControlPacket(clientRpc->session->serverAddress, &ack);
                    return;

                }
                double elapsedMicros = Cycles::toSeconds(Cycles::rdtsc()
                        - clientRpc->lastTransmitTime)*1e06;
                RAMCLOUD_CLOG(NOTICE, "Retransmitting to server %s: "
                        "sequence %lu, offset %u, length %u, elapsed "
                        "time %.1f us",
                        received->sender->toString().c_str(),
                        header->common.rpcId.sequence, header->offset,
                        header->length, elapsedMicros);
                // TODO: document why pass bytes directly to NIC
                sendBytes(clientRpc->session->serverAddress,
                        header->common.rpcId, clientRpc->request,
                        header->offset, header->length,
                        clientRpc->unscheduledBytes, header->priority,
                        FROM_CLIENT|RETRANSMISSION, true);
                // TODO: NO NEED TO CALL RDTSC DIRECTLY; GET THIS FROM DRIVER;
                // ALSO, MOVE THIS STATEMENT INTO sendBytes?
                clientRpc->lastTransmitTime = Cycles::rdtsc();
                return;
            }

            // ACK from server
            case PacketOpcode::ACK: {
                // Nothing to do.
                timeTrace("client received ACK, clientId %u, sequence %u",
                        common->rpcId.clientId, common->rpcId.sequence);
                return;
            }

            default:
            RAMCLOUD_CLOG(WARNING,
                    "unexpected opcode %s received from server %s",
                    opcodeSymbol(common->opcode).c_str(),
                    received->sender->toString().c_str());
            return;
        }
    } else {
        // This packet was sent by the client; it relates to an RPC
        // for which we are the server.

        // Find the record for this RPC, if one exists.
        ServerRpc* serverRpc = NULL;
        ServerRpcMap::iterator it = incomingRpcs.find(common->rpcId);
        if (it != incomingRpcs.end()) {
            serverRpc = it->second;
            serverRpc->silentIntervals = 0;
        }

        switch (common->opcode) {
            // ALL_DATA from client
            case PacketOpcode::ALL_DATA: {
                // Common case: the entire request fit in a single packet.

                AllDataHeader* header = received->getOffset<AllDataHeader>(0);
                if (header == NULL)
                    goto packetLengthError;
                timeTrace("server received ALL_DATA, clientId %u, sequence %u, "
                        "length %u",
                        header->common.rpcId.clientId,
                        header->common.rpcId.sequence,
                        header->messageLength);
                if (serverRpc != NULL) {
                    // This shouldn't normally happen: it means this packet is
                    // a duplicate, so we can just discard it.
                    return;
                }
                uint32_t length;
                char *payload = received->steal(&length);
                uint32_t requiredLength =
                        downCast<uint32_t>(header->messageLength) +
                        sizeof32(AllDataHeader);
                if (length < requiredLength) {
                    RAMCLOUD_CLOG(WARNING, "ALL_DATA request from %s too "
                            "short (got %u bytes, expected %u)",
                            received->sender->toString().c_str(),
                            length, requiredLength);
                    driver->release(payload);
                    return;
                }
                serverRpc = serverRpcPool.construct(this,
                        nextServerSequenceNumber, received->sender,
                        header->common.rpcId);
                nextServerSequenceNumber++;
                // TODO: the following operation is very expensive;
                // easily > 250ns
                incomingRpcs[header->common.rpcId] = serverRpc;
                Driver::PayloadChunk::appendToBuffer(&serverRpc->requestPayload,
                        payload + sizeof32(AllDataHeader),
                        header->messageLength, driver, payload);
                serverRpc->requestComplete = true;
                context->workerManager->handleRpc(serverRpc);
                return;
            }

            // DATA from client
            case PacketOpcode::DATA: {
                DataHeader* header = received->getOffset<DataHeader>(0);
                bool retainPacket = false;
                if (header == NULL)
                    goto packetLengthError;
                timeTrace("server received DATA, clientId %u, sequence %u, "
                        "offset %u, length %u",
                        header->common.rpcId.clientId,
                        header->common.rpcId.sequence,
                        header->offset, received->len);
                if ((header->offset > header->unscheduledBytes) &&
                        ((serverRpc == NULL) || (header->offset >
                        serverRpc->scheduledMessage->grantOffset))) {
                    // TODO: THIS COULD HAPPEN BECAUSE?
                    uint32_t grantOffset = (serverRpc == NULL) ?
                            header->unscheduledBytes :
                            serverRpc->scheduledMessage->grantOffset;
                    LOG(WARNING, "unexpected DATA from client %s, "
                            "id (%lu,%lu), offset %u, grantOffset %u",
                            received->sender->toString().c_str(),
                            header->common.rpcId.clientId,
                            header->common.rpcId.sequence, header->offset,
                            grantOffset);
                    goto serverDataDone;
                }
                if (serverRpc == NULL) {
                    serverRpc = serverRpcPool.construct(this,
                            nextServerSequenceNumber, received->sender,
                            header->common.rpcId);
                    nextServerSequenceNumber++;
                    incomingRpcs[header->common.rpcId] = serverRpc;
                    serverRpc->accumulator.construct(this,
                            &serverRpc->requestPayload,
                            uint32_t(header->totalLength));
                    if (header->totalLength > header->unscheduledBytes) {
                        serverRpc->scheduledMessage.construct(
                                serverRpc->rpcId, serverRpc->accumulator.get(),
                                uint32_t(header->unscheduledBytes),
                                serverRpc->clientAddress,
                                static_cast<uint8_t>(FROM_CLIENT));
                    }
                    serverTimerList.push_back(*serverRpc);
                } else if (serverRpc->requestComplete) {
                    // We've already received the full message, so
                    // ignore this packet.
                    TEST_LOG("ignoring extraneous packet");
                    goto serverDataDone;
                }
                retainPacket = serverRpc->accumulator->addPacket(header,
                        received->len);
                if (header->offset == 0) {
                    timeTrace("server received opcode %u, totalLength %u",
                            serverRpc->requestPayload.getStart<
                            WireFormat::RequestCommon>()->opcode,
                            header->totalLength);
                }
                if (serverRpc->scheduledMessage) {
                    if (header->offset > serverRpc->scheduledMessage->grantOffset) {
                        LOG(ERROR, "DATA offset %u larger than grant offset %u",
                                header->offset,
                                serverRpc->scheduledMessage->grantOffset);
                    }
                    bool scheduledPacket = (header->offset + received->len >
                            header->unscheduledBytes);
                    dataArriveForScheduledMessage(
                            serverRpc->scheduledMessage.get(),
                            scheduledPacket);
                }
                if (serverRpc->requestPayload.size() >= header->totalLength) {
                    // Message complete; start servicing the RPC.
                    if (serverRpc->requestPayload.size()
                            > header->totalLength) {
                        // We have more bytes than we want. This can happen
                        // if the last packet gets padded by the network
                        // layer to meet minimum size requirements. Just
                        // truncate the request.
                        serverRpc->requestPayload.truncate(header->totalLength);
                    }
                    erase(serverTimerList, *serverRpc);
                    serverRpc->requestComplete = true;
                    context->workerManager->handleRpc(serverRpc);
                }

                serverDataDone:
                if (retainPacket) {
                    uint32_t dummy;
                    received->steal(&dummy);
                }
                return;
            }

            // GRANT from client
            case PacketOpcode::GRANT: {
                GrantHeader* header = received->getOffset<GrantHeader>(0);
                if (header == NULL)
                    goto packetLengthError;
                timeTrace("server received GRANT, clientId %u, sequence %u, "
                        "offset %u",
                        header->common.rpcId.clientId,
                        header->common.rpcId.sequence,
                        header->offset);
                if ((serverRpc == NULL) || !serverRpc->sendingResponse) {
                    RAMCLOUD_LOG(WARNING, "unexpected GRANT from client %s, "
                            "id (%lu,%lu), grantOffset %u",
                            received->sender->toString().c_str(),
                            header->common.rpcId.clientId,
                            header->common.rpcId.sequence, header->offset);
                    return;
                }
                if (header->offset > serverRpc->transmitLimit) {
                    serverRpc->transmitLimit = header->offset;
                    serverRpc->transmitPriority = header->priority;
                }
                return;
            }

            // LOG_TIME_TRACE from client
            case PacketOpcode::LOG_TIME_TRACE: {
                LOG(NOTICE, "Server received LOG_TIME_TRACE request from "
                        "client %s for sequence %lu",
                        received->sender->toString().c_str(),
                        common->rpcId.sequence);
                timeTrace("server received LOG_TIME_TRACE for clientId %u, "
                        "sequence %u",
                        common->rpcId.clientId, common->rpcId.sequence);
                TimeTrace::printToLogBackground(context->dispatch);
                return;
            }

            // RESEND from client
            case PacketOpcode::RESEND: {
                ResendHeader* header = received->getOffset<ResendHeader>(0);
                if (header == NULL)
                    goto packetLengthError;
                timeTrace("server received RESEND, clientId %u, sequence %u, "
                        "offset %u, length %u",
                        header->common.rpcId.clientId,
                        header->common.rpcId.sequence,
                        header->offset, header->length);

                if (serverRpc == NULL) {
                    // This situation can happen if we never received the
                    // request, or if a packet of the response got lost but
                    // we have already freed the ServerRpc. In either case,
                    // ask the client to restart the RPC from scratch.
                    timeTrace("server requesting restart, clientId %u, "
                            "sequence %u",
                            common->rpcId.clientId, common->rpcId.sequence);
                    // TODO: roundTripBytes should be replaced with what?
                    ResendHeader resend(header->common.rpcId, 0,
                            roundTripBytes, 0, FROM_SERVER|RESTART);
                    sendControlPacket(received->sender, &resend);
                    return;
                }
                uint32_t resendEnd = header->offset + header->length;
                if (resendEnd > serverRpc->transmitLimit) {
                    // Needed in case GRANT packet was lost.
                    serverRpc->transmitLimit = resendEnd;
                }
                if (!serverRpc->sendingResponse
                        || (header->offset >= serverRpc->transmitOffset)
                        || ((Cycles::rdtsc() - serverRpc->lastTransmitTime)
                        < timerInterval)) {
                    // One of two things has happened: either (a) we haven't
                    // yet sent the requested bytes for the first time (there
                    // must be other outgoing traffic with higher priority)
                    // or (b) we transmitted data recently, so it might have
                    // crossed paths with the RESEND request. In either case,
                    // it's unlikely that bytes have been lost, so don't
                    // retransmit; just return an ACK so the client knows
                    // we're still alive.
                    AckHeader ack(serverRpc->rpcId, FROM_SERVER);
                    sendControlPacket(serverRpc->clientAddress, &ack);
                    return;
                }
                double elapsedMicros = Cycles::toSeconds(Cycles::rdtsc()
                        - serverRpc->lastTransmitTime)*1e06;
                RAMCLOUD_CLOG(NOTICE, "Retransmitting to client %s: "
                        "sequence %lu, offset %u, length %u, elapsed "
                        "time %.1f us",
                        received->sender->toString().c_str(),
                        header->common.rpcId.sequence, header->offset,
                        header->length, elapsedMicros);
                // TODO: document why pass the bytes directly to NIC
                sendBytes(serverRpc->clientAddress,
                        serverRpc->rpcId, &serverRpc->replyPayload,
                        header->offset, header->length,
                        serverRpc->unscheduledBytes, header->priority,
                        RETRANSMISSION|FROM_SERVER, true);
                serverRpc->lastTransmitTime = Cycles::rdtsc();
                return;
            }

            // ACK from client
            case PacketOpcode::ACK: {
                // Nothing to do.
                timeTrace("server received ACK, clientId %u, sequence %u",
                        common->rpcId.clientId, common->rpcId.sequence);
                return;
            }

            default:
                RAMCLOUD_CLOG(WARNING,
                        "unexpected opcode %s received from client %s",
                        opcodeSymbol(common->opcode).c_str(),
                        received->sender->toString().c_str());
                return;
        }
    }

    packetLengthError:
    RAMCLOUD_CLOG(WARNING, "packet of type %s from %s too short (%u bytes)",
            opcodeSymbol(common->opcode).c_str(),
            received->sender->toString().c_str(),
            received->len);

}

/**
 * Returns a string containing human-readable information about the client
 * that initiated this RPC. Right now this isn't formatted as a service
 * locator; it just describes a Driver::Address.
 */
string
HomaTransport::ServerRpc::getClientServiceLocator()
{
    return clientAddress->toString();
}

/**
 * This method is invoked when a server has finished processing an RPC.
 * It begins transmitting the response back to the client, but returns
 * before that process is complete.
 */
void
HomaTransport::ServerRpc::sendReply()
{
    timeTrace("sendReply invoked, clientId %u, sequence %u, length %u, "
            "%u outgoing responses", rpcId.clientId, rpcId.sequence,
            replyPayload.size(), t->outgoingResponses.size());
    sendingResponse = true;
    transmitLimit = t->roundTripBytes;
    unscheduledBytes = t->roundTripBytes;
    transmitPriority = t->getUnschedTrafficPrio(replyPayload.size());
    t->outgoingResponses.push_back(*this);
    t->serverTimerList.push_back(*this);
    uint32_t bytesSent = t->tryToTransmitData();
    if (bytesSent > 0) {
        timeTrace("sendReply transmitted %u bytes", bytesSent);
    }
}

/**
 * Construct a MessageAccumulator.
 *
 * \param t
 *      Overall information about the transport.
 * \param buffer
 *      The complete message will be assembled here; caller should ensure
 *      that this is initially empty. The caller owns the storage for this
 *      and must ensure that it persists as long as this object persists.
 * \param totalLength
 *      Total # bytes in the message.
 */
HomaTransport::MessageAccumulator::MessageAccumulator(HomaTransport* t,
        Buffer* buffer, uint32_t totalLength)
    : t(t)
    , assembledPayloads(new MessageBuffer())
    , buffer(buffer)
    , estimatedReceivedBytes(0)
    , fragments()
    , totalLength(totalLength)
{
    assert(buffer->size() == 0);
}

/**
 * Destructor for MessageAccumulators.
 */
HomaTransport::MessageAccumulator::~MessageAccumulator()
{
    // If there are any unassembled fragments, then we must release
    // them back to the driver.
    for (FragmentMap::iterator it = fragments.begin();
            it != fragments.end(); it++) {
        MessageFragment fragment = it->second;
        t->driver->release(fragment.header);
    }
    fragments.clear();
    t->messageBuffers.push_back(assembledPayloads);
}

/**
 * This method is invoked whenever a new DATA packet arrives for a partially
 * complete message. It saves information about the new fragment and
 * (eventually) combines all of the fragments into a complete message.
 *
 * \param header
 *      Pointer to the first byte of the packet, which must be a valid
 *      DATA packet.
 * \param length
 *      Total number of bytes in the packet.
 * \return
 *      The return value is true if we have retained a pointer to the
 *      packet (meaning that the caller should "steal" the Received, if
 *      it hasn't already). False means that the data in this packet
 *      was all redundant; we didn't save anything, so the caller need
 *      not steal the Received.
 */
bool
HomaTransport::MessageAccumulator::addPacket(DataHeader *header,
        uint32_t length)
{
    // Assume the payload contains no redundant bytes for now. The redundant
    // portion will be deducted when this packet is discarded/assembled.
    length -= sizeof32(DataHeader);
    estimatedReceivedBytes += length;

    if (header->offset > buffer->size()) {
        // Can't append this packet into the buffer because some prior
        // data is missing. Save the packet for later.
        if (fragments.find(header->offset) == fragments.end()) {
            fragments[header->offset] = MessageFragment(header, length);
            assert(buffer->size() < fragments.begin()->first);
            return true;
        } else {
            estimatedReceivedBytes -= length;
            return false;
        }
    }

    // Append this fragment to the assembled message buffer, then see
    // if some of the unappended fragments can now be appended as well.
    bool result = appendFragment(header, length);
    while (true) {
        FragmentMap::iterator it = fragments.begin();
        if (it == fragments.end()) {
            break;
        }
        uint32_t offset = it->first;
        if (offset > buffer->size()) {
            break;
        }
        MessageFragment fragment = it->second;
        if (!appendFragment(fragment.header, fragment.length)) {
            t->driver->release(fragment.header);
        };
        fragments.erase(it);
    }
    if (!fragments.empty()) {
        assert(buffer->size() < fragments.begin()->first);
    }
    return result;
}

/**
 * This method is invoked to append a fragment to a partially-assembled
 * message. It handles the special cases where part or all of the
 * fragment is already in the assembled message.
 *
 * \param header
 *      Address of the first byte of a DATA packet.
 * \param length
 *      Size of the payload, in bytes.
 * \return
 *      True means that (some of) the data in this fragment was
 *      incorporated into the message buffer. False means that
 *      the data in this fragment is entirely redundant, so we
 *      didn't save any pointers to it (the caller may want to
 *      free this packet).
 */
bool
HomaTransport::MessageAccumulator::appendFragment(DataHeader *header,
        uint32_t length)
{
    uint32_t bytesToSkip = buffer->size() - header->offset;
    estimatedReceivedBytes -= std::min(bytesToSkip, length);
    if (bytesToSkip >= length) {
        // This entire fragment is redundant.
        return false;
    }

    char* payload = reinterpret_cast<char*>(header);
    buffer->appendExternal(payload + sizeof32(DataHeader) + bytesToSkip,
            length - bytesToSkip);
    assembledPayloads->push_back(payload);
    return true;
}

/**
 * This method is invoked to issue a RESEND packet when it appears that
 * packets have been lost. It is used by both servers and clients.
 *
 * \param t
 *      Overall information about the transport.
 * \param address
 *      Network address to which the RESEND should be sent.
 * \param rpcId
 *      Unique identifier for the RPC in question.
 * \param grantOffset
 *      Largest grantOffset that we have sent for this message (i.e.
 *      this is how many total bytes we should have received already).
 *      May be 0 if the client never requested a grant (meaning that it
 *      planned to transmit the entire message unilaterally).
 * \param whoFrom
 *      Must be either FROM_CLIENT, indicating that we are the client, or
 *      FROM_SERVER, indicating that we are the server.
 *
 * \return
 *      The offset of the byte just after the last one whose retransmission
 *      was requested.
 */
uint32_t
HomaTransport::MessageAccumulator::requestRetransmission(HomaTransport *t,
        const Driver::Address* address, RpcId rpcId, uint32_t grantOffset,
        uint8_t whoFrom)
{
    if ((reinterpret_cast<uint64_t>(&fragments) < 0x1000lu)) {
        DIE("Bad fragment pointer: %p", &fragments);
    }
    uint32_t endOffset;
    FragmentMap::iterator it = fragments.begin();

    // Compute the end of the retransmission range.
    if (it != fragments.end()) {
        // Retransmit the entire gap up to the first fragment.
        endOffset = it->first;
    } else if (grantOffset > 0) {
        // Retransmit everything that we've asked the sender to send:
        // we don't seem to have received any of it.
        endOffset = grantOffset;
    } else {
        // We haven't issued a GRANT for this message; just request
        // the first round-trip's worth of data. Once this data arrives,
        // the normal grant mechanism should kick in if it's still needed.
        endOffset = t->roundTripBytes;
    }
    assert(endOffset > buffer->size());
    const char* fmt = (whoFrom == FROM_SERVER) ?
            "server requesting retransmission of bytes %u-%u, clientId %u, "
            "sequence %u" :
            "client requesting retransmission of bytes %u-%u, clientId %u, "
            "sequence %u";
    timeTrace(fmt, buffer->size(), endOffset, rpcId.clientId, rpcId.sequence);
    uint32_t length = endOffset - buffer->size();
    if (length > t->roundTripBytes) {
        // TODO: This is not only suspicious but could also cause significant queueing on
        // the other side's NIC since retransmission bytes are passed to the driver directly
        LOG(WARNING, "%s requesting retransmission of a large range %u-%u, "
                "clientId %lu, sequence %lu",
                whoFrom == FROM_SERVER ? "server" : "client",
                buffer->size(), endOffset, rpcId.clientId, rpcId.sequence);
    }
    // TODO: HOW TO DOCUMENT OUR CHOICE OF PRIO HERE?
    ResendHeader resend(rpcId, buffer->size(), length,
            t->getUnschedTrafficPrio(length), whoFrom);
    t->sendControlPacket(address, &resend);
    return endOffset;
}

/**
 * Construct a ScheduledMessage and notifies the scheduler the arrival of
 * this new scheduled message.
 *
 * \param rpcId
 *      Unique identifier for the RPC this message belongs to.
 * \param accumulator
 *      Overall information about this multi-packet message.
 * \param unscheduledBytes
 *      # bytes sent unilaterally.
 * \param senderAddress
 *      Network address of the message sender.
 * \param whoFrom
 *      Must be either FROM_CLIENT, indicating that this is a request, or
 *      FROM_SERVER, indicating that this is a response.
 */
HomaTransport::ScheduledMessage::ScheduledMessage(RpcId rpcId,
        MessageAccumulator* accumulator, uint32_t unscheduledBytes,
        const Driver::Address* senderAddress, uint8_t whoFrom)
    : accumulator(accumulator)
    , activeMessageLinks()
    , inactiveMessageLinks()
    , grantOffset(unscheduledBytes)
    , grantPriority(0)
    , rpcId(rpcId)
    , senderAddress(senderAddress)
    , senderHash(std::hash<std::string>{}(senderAddress->toString()))
    , state(NEW)
    , whoFrom(whoFrom)
{
    accumulator->t->tryToSchedule(this);
}

/**
 * Destructor for ScheduledMessages.
 */
HomaTransport::ScheduledMessage::~ScheduledMessage()
{
    if (state == ACTIVE) {
        accumulator->t->replaceActiveMessage(this, NULL, true);
    } else if (state == INACTIVE) {
        erase(accumulator->t->inactiveMessages, *this);
    }
}

/**
 * Compare the relative precedence of two scheduled messages in the message
 * scheduler.
 *
 * \param other
 *      The other message to compare with.
 * \return
 *      Negative number if this message has higher precedence; positive
 *      number if the other message has higher precedence; 0 if the two
 *      messages have equal precedence.
 */
int
HomaTransport::ScheduledMessage::compareTo(ScheduledMessage& other) const
{
    // Implement the SRPT policy.
    int r0 = accumulator->totalLength - accumulator->buffer->size();
    int r1 = other.accumulator->totalLength -
            other.accumulator->buffer->size();
    return r0 - r1;
}

// TODO: NOT SURE THIS IS THE RIGHT PLACE TO DEFINE IT.
#if TIME_TRACE
#define UPDATE_CYCLES(cycles) do { \
    currentTime = Cycles::rdtsc(); \
    cycles += currentTime - prevTime; \
    prevTime = currentTime; \
} while (0)
#define UPDATE_CYCLES_IF(cond, cycles) do { \
    if (!(cond)) break; \
    currentTime = Cycles::rdtsc(); \
    cycles += currentTime - prevTime; \
    prevTime = currentTime; \
} while (0)
#else
#define UPDATE_CYCLES(x) do {} while (0)
#define UPDATE_CYCLES_IF(x) do {} while (0)
#endif

/**
 * This method is invoked in the inner polling loop of the dispatcher;
 * it drives the operation of the transport.
 * \return
 *      The return value is 1 if this method found something useful to do,
 *      0 otherwise.
 */
int
HomaTransport::Poller::poll()
{
    int result = 0;

#if TIME_TRACE
    // See if we should compute the network throughput in the last interval.
    if (owner->currentTime > t->lastMeasureTime + t->monitorInterval) {
        uint32_t millis = t->monitorMillis;
        uint64_t activeCycles = PerfStats::threadStats.dispatchActiveCycles
                - t->lastDispatchActiveCycles;
        uint32_t wastedGoodput = t->numTimesGrantRunDry *
                t->driver->getMaxPacketSize()*8/1000/millis;
        uint64_t now = Cycles::rdtsc();
        timeTrace(now, "data bytes goodput %u Mbps, "
                "control bytes goodput %u Mbps, "
                "retransmission bytes goodput %u Mbps",
                t->outputDataBytes*8/1000/millis,
                t->outputControlBytes*8/1000/millis,
                t->outputResentBytes*8/1000/millis);
        timeTrace(now, "data packet TX rate %u kpps, "
                "control packet TX rate %u kpps, "
                "data packet RX rate %u kpps, "
                "control packet RX rate %u kpps,",
                t->numDataPacketsSent/millis,
                t->numControlPacketsSent/millis,
                t->numDataPacketsReceived/millis,
                (t->numPacketsReceived - t->numDataPacketsReceived)/millis);
        timeTrace(now, "dispatch utilization %u%%, process packets %u%%, "
                "transmit data %u%%, transmit grant %u%%",
                activeCycles*100/t->monitorInterval,
                t->processPacketCycles*100/t->monitorInterval,
                t->transmitDataCycles*100/t->monitorInterval,
                t->transmitGrantCycles*100/t->monitorInterval);
        timeTrace(now, "unused uplink bandwidth %u%%, "
                "run out of grants %u times, wasted goodput < %u Mbps",
                t->unusedBandwidth*100/t->monitorInterval,
                t->numTimesGrantRunDry, wastedGoodput);

        t->lastMeasureTime = owner->currentTime;
        t->lastDispatchActiveCycles =
                PerfStats::threadStats.dispatchActiveCycles;
        t->numPacketsReceived = 0;
        t->numDataPacketsReceived = 0;
        t->numControlPacketsSent = 0;
        t->numDataPacketsSent = 0;
        t->numTimesGrantRunDry = 0;
        t->outputControlBytes = 0;
        t->outputDataBytes = 0;
        t->outputResentBytes = 0;
        t->processPacketCycles = 0;
        t->transmitDataCycles = 0;
        t->transmitGrantCycles = 0;
        t->unusedBandwidth = 0;
    }

    uint64_t startTime = Cycles::rdtsc();
    uint64_t prevTime = startTime, currentTime;
    uint64_t lastIdleTime = t->driver->getRxQueueIdleTime();
#endif

    // Process any available incoming packets.
#define MAX_PACKETS 4
    uint32_t numPackets;
    uint32_t totalPackets = 0;
    do {
        t->driver->receivePackets(MAX_PACKETS, &t->receivedPackets);
        numPackets = downCast<uint>(t->receivedPackets.size());
#if TIME_TRACE
        // Log the beginning of poll() here so that timetrace entries do not
        // go back in time.
        if (totalPackets == 0 && numPackets > 0) {
            uint64_t ns = Cycles::toNanoseconds(startTime - lastIdleTime);
            timeTrace(startTime, "start of polling iteration %u, "
                    "last poll was %u ns ago", owner->iteration, ns);
        }
#endif
        for (uint i = 0; i < numPackets; i++) {
            t->handlePacket(&t->receivedPackets[i]);
        }
        t->receivedPackets.clear();
        totalPackets += numPackets;
    } while (numPackets == MAX_PACKETS);
    t->numPacketsReceived += totalPackets;
    result |= totalPackets;
    UPDATE_CYCLES(t->processPacketCycles);

    // Send out GRANTs that are produced in the previous processing step
    // in a batch.
    for (ScheduledMessage* recipient : t->grantRecipients) {
        uint8_t whoFrom = (recipient->whoFrom == FROM_CLIENT) ?
                FROM_SERVER : FROM_CLIENT;
        GrantHeader grant(recipient->rpcId, recipient->grantOffset,
                recipient->grantPriority, whoFrom);
        const char* fmt = (whoFrom == FROM_CLIENT) ?
                "client sending GRANT, clientId %u, sequence %u, offset %u, "
                "priority %u" :
                "server sending GRANT, clientId %u, sequence %u, offset %u, "
                "priority %u";
        timeTrace(fmt, recipient->rpcId.clientId, recipient->rpcId.sequence,
                grant.offset, grant.priority);

        t->sendControlPacket(recipient->senderAddress, &grant);
//        t->driver->bufferPacket(recipient->senderAddress, &grant, NULL,
//                t->controlPacketPriority);
    }
//    t->driver->flushTxBuffer();
    UPDATE_CYCLES_IF(!t->grantRecipients.empty(), t->transmitGrantCycles);
    t->grantRecipients.clear();

    // See if we should check for timeouts. Ideally, we'd like to do this
    // every timerInterval. However, it's better not to call checkTimeouts
    // when there are input packets pending, since checkTimeouts might then
    // request retransmission of a packet that's waiting in the NIC. Thus,
    // if necessary, we delay the call to checkTimeouts to find a time when
    // we are caught up on input packets. If a long time goes by without ever
    // catching up, then we invoke checkTimeouts anyway.
    //
    // Note: it isn't a disaster if we occasionally request an unnecessary
    // retransmission, since the protocol will handle this fine. However, if
    // too many of these happen, it will create noise in the logs, which will
    // make it harder to notice when a *real* problem happens. Thus, it's
    // best to eliminate spurious retransmissions as much as possible.
    uint64_t now = owner->currentTime;
    if (now >= t->nextTimeoutCheck) {
        if (t->timeoutCheckDeadline == 0) {
            t->timeoutCheckDeadline = now + t->timerInterval;
        }
        if ((numPackets < MAX_PACKETS)
                || (now >= t->timeoutCheckDeadline)) {
            if (numPackets == MAX_PACKETS) {
                RAMCLOUD_CLOG(NOTICE, "Deadline invocation of checkTimeouts");
                timeTrace("Deadline invocation of checkTimeouts");
            }
            t->checkTimeouts();
            result = 1;
            t->nextTimeoutCheck = now + t->timerInterval;
            t->timeoutCheckDeadline = 0;
        }
    }

    // Transmit data packets if possible.
    uint32_t totalBytesSent = t->tryToTransmitData();
    result |= totalBytesSent;
    UPDATE_CYCLES(t->transmitDataCycles);

    // TODO: INVESTIGATE WHY FLUSHING TX BUFFER HERE ACTUALLY MAKES TAIL LATENCY WORSE.
    // If no data packet is transmitted in the previous step, flush the GRANT
    // packets waiting in the driver's TX queue manually.
//    if (totalBytesSent == 0) {
//        t->driver->flushTxBuffer();
//    }

    // Release a few retained payloads to the driver. As of 02/2017, releasing
    // one payload to the DpdkDriver takes ~65ns. If we haven't found anything
    // useful to do in this method uptill now, try to release more payloads.
    const uint32_t maxRelease = result ? 2 : 5;
    uint32_t releaseCount = 0;
    while (!t->messageBuffers.empty()) {
        MessageAccumulator::MessageBuffer* messageBuffer =
                t->messageBuffers.back();
        while (!messageBuffer->empty() && (releaseCount < maxRelease)) {
            char* payload = messageBuffer->back();
            messageBuffer->pop_back();
            t->driver->release(payload);
            releaseCount++;
            result = 1;
        }

        if (messageBuffer->empty()) {
            t->messageBuffers.pop_back();
            delete messageBuffer;
        } else {
            break;
        }
    }

    if (result) {
        timeTrace("end of polling iteration %u, received %u packets, "
                "transmitted %u bytes, released %u packet buffers",
                owner->iteration, totalPackets, totalBytesSent, releaseCount);
    }
    return result;
}

/**
 * This method is invoked by poll at regular intervals to check for
 * unexpected lapses in communication. It implements all of the timer-related
 * functionality for both clients and servers, such as requesting packet
 * retransmission and aborting RPCs.
 */
void
HomaTransport::checkTimeouts()
{
    // Scan all of the ClientRpc objects.
    timeTrace("checkTimeouts invoked, %u outgoing RPCs, "
            "%u incomplete incoming requests",
            outgoingRpcs.size(), serverTimerList.size());
    for (ClientRpcMap::iterator it = outgoingRpcs.begin();
            it != outgoingRpcs.end(); ) {
        uint64_t sequence = it->first;
        ClientRpc* clientRpc = it->second;
        if (clientRpc->transmitOffset == 0) {
            // We haven't started transmitting this RPC yet (our transmit
            // queue is probably backed up), so no need to worry about whether
            // we have heard from the server.
            it++;
            continue;
        }
        clientRpc->silentIntervals++;

        // Advance the iterator here, so that it won't get invalidated if
        // we delete the ClientRpc below.
        it++;

        assert(timeoutIntervals > 2*pingIntervals);
        if (clientRpc->silentIntervals >= timeoutIntervals) {
            // A long time has elapsed with no communication whatsoever
            // from the server, so abort the RPC.
            RAMCLOUD_LOG(WARNING, "aborting %s RPC to server %s, "
                    "sequence %lu: timeout",
                    WireFormat::opcodeSymbol(clientRpc->request),
                    clientRpc->session->serverAddress->toString().c_str(),
                    sequence);
            clientRpc->notifier->failed();
            deleteClientRpc(clientRpc);
            continue;
        }

        ScheduledMessage* scheduledMessage = clientRpc->scheduledMessage.get();
        if (!clientRpc->accumulator) {
            // We haven't received any part of the response message.
            // Send occasional RESEND packets, which should produce some
            // response from the server, so that we know it's still alive
            // and working. Note: the wait time for this ping is longer
            // than the server's wait time to request retransmission (first
            // give the server a chance to handle the problem).
            if ((clientRpc->silentIntervals % pingIntervals) == 0) {
                timeTrace("client sending RESEND for clientId %u, sequence %u",
                        clientId, sequence);
                ResendHeader resend(RpcId(clientId, sequence), 0,
                        roundTripBytes, 0, FROM_CLIENT);
                // The RESEND packet is effectively a grant...
                sendControlPacket(clientRpc->session->serverAddress, &resend);
            }
        } else if (!scheduledMessage ||
                (scheduledMessage->state == ScheduledMessage::ACTIVE) ||
                (scheduledMessage->state == ScheduledMessage::PURGED)) {
            // We have received part of the response and expect to receive
            // more, meaning that the response message is either a unscheduled,
            // active or purged message. If the server has gone silent, this
            // must mean packets were lost, grants were lost, or the server
            // has preempted this response for higher priority messages, so
            // request retransmission anyway.
            if (clientRpc->silentIntervals >= 2) {
                uint32_t grantOffset = scheduledMessage ?
                        scheduledMessage->grantOffset : 0;
                if (scheduledMessage &&
                        grantOffset == clientRpc->accumulator->buffer->size())
                {
                    // TODO: NOT SURE WHY THIS COULD HAPPEN? WE HAVE RECEIVED
                    // ALL WE HAVE ASKED FOR; IT'S OUR PROBLEM TO NOT SEND ENOUGH
                    // GRANTs!!!
                    LOG(WARNING, "client rpc starved of grant, clientId %lu, "
                            "sequence %lu, grantOffset %u, %lu activeMessages",
                            clientRpc->rpcId.clientId, clientRpc->sequence,
                            grantOffset, activeMessages.size());
                    timeTrace("server rpc starved of grant, clientId %u, "
                            "sequence %u, grantOffset %u, %u active messages",
                            clientRpc->rpcId.clientId, clientRpc->sequence,
                            grantOffset, activeMessages.size());
                    continue;
                }
                clientRpc->accumulator->requestRetransmission(this,
                        clientRpc->session->serverAddress,
                        RpcId(clientId, sequence), grantOffset, FROM_CLIENT);
            }
        }
    }

    // Scan all of the ServerRpc objects for which network I/O is in
    // progress (either for the request or the response).
    for (ServerTimerList::iterator it = serverTimerList.begin();
            it != serverTimerList.end(); ) {
        ServerRpc* serverRpc = &(*it);
        if (serverRpc->sendingResponse && (serverRpc->transmitOffset == 0)) {
            // Looks like the transmit queue has been too backed up to start
            // sending the response, so no need to check for a timeout.
            it++;
            continue;
        }
        serverRpc->silentIntervals++;

        // Advance the iterator now, so it won't get invalidated if we
        // delete the ServerRpc below.
        it++;

        // If a long time has elapsed with no communication whatsoever
        // from the client, then abort the RPC. Note: this code should
        // only be executed when we're waiting to transmit or receive
        // (never if we're waiting for the RPC to execute locally).
        // The most common reasons for getting here are:
        // (a) The client has crashed
        // (b) The client sent us data for an RPC after we processed it,
        //     returned the result, and deleted the RPC; as a result, we
        //     created a new RPC that the client no longer cares about.
        //     Such extraneous data can occur if we requested a
        //     retransmission but then the original data arrived, so we
        //     could process the RPC before the retransmitted data arrived.
        assert(serverRpc->sendingResponse || !serverRpc->requestComplete);
        if (serverRpc->silentIntervals >= timeoutIntervals) {
            deleteServerRpc(serverRpc);
            continue;
        }

        // See if we need to request retransmission for part of the request
        // message.
        ScheduledMessage* scheduledMessage = serverRpc->scheduledMessage.get();
        if ((serverRpc->silentIntervals >= 2) && !serverRpc->requestComplete &&
                (!scheduledMessage ||
                (scheduledMessage->state == ScheduledMessage::ACTIVE) ||
                (scheduledMessage->state == ScheduledMessage::PURGED))) {
            uint32_t grantOffset = scheduledMessage ?
                    scheduledMessage->grantOffset : 0;
            if (grantOffset == serverRpc->accumulator->buffer->size()) {
                // TODO: NOT SURE WHY THIS COULD HAPPEN?
                LOG(WARNING, "server rpc starved of grant, clientId %lu, "
                        "sequence %lu, grantOffset %u, %lu active messages",
                        serverRpc->rpcId.clientId, serverRpc->rpcId.sequence,
                        grantOffset, activeMessages.size());
                timeTrace("server rpc starved of grant, clientId %u, "
                        "sequence %u, grantOffset %u, %u active messages",
                        serverRpc->rpcId.clientId, serverRpc->rpcId.sequence,
                        grantOffset, activeMessages.size());
                continue;
            }
            serverRpc->accumulator->requestRetransmission(this,
                    serverRpc->clientAddress, serverRpc->rpcId, grantOffset,
                    FROM_SERVER);
        }
    }
}

/**
 * A non-active (new or inactive) message needs to be inserted to the active
 * message list or an existing active message needs to move forward in the
 * list. Either case, put this message to the right place in the list that
 * reflects its precedence among the active messages.
 *
 * \param message
 *      A message that needs to be put at the right place in the active message
 *      list.
 */
void
HomaTransport::adjustSchedulingPrecedence(ScheduledMessage* message)
{
    assert(message->state != ScheduledMessage::PURGED);
    bool alreadyActive = (message->state == ScheduledMessage::ACTIVE);

    // The following loop iterates over the active message list to find the
    // right place to insert the given message.
    ScheduledMessage* insertHere = NULL;
    for (ScheduledMessage& m : activeMessages) {
        if (&m == message) {
            // This existing message is still in the right place: all preceding
            // messages are smaller.
            return;
        }

        if (message->compareTo(m) < 0) {
            if (alreadyActive) {
                erase(activeMessages, *message);
            }
            insertHere = &m;
            break;
        }
    }

    // Insert the message.
    if (message->state == ScheduledMessage::INACTIVE) {
        erase(inactiveMessages, *message);
    }
    message->state = ScheduledMessage::ACTIVE;
    if (insertHere) {
        insertBefore(activeMessages, *message, *insertHere);
    } else {
        activeMessages.push_back(*message);
    }
}

/**
 * Replace an active message by an non-active (new or inactive) one because
 * 1) our scheduling policy says it's a better choice, 2) the active message
 * has been fully granted or 3) the active message needs to be destroyed
 * (e.g. the RPC is cancelled).
 *
 * \param oldMessage
 *      A currently active message that is about to be deactivated.
 * \param newMessage
 *      A non-active message that should be activated. NULL means this method
 *      is invoked because \p oldMessage must be purged and it is the duty
 *      of this method to pick a replacement from the inactive message list.
 *      Otherwise, this method is invoked because \p newMessage is a better
 *      choice compared to \p oldMessage.
 * \param cancelled
 *      True means we are not interested in receiving \p oldMessage anymore;
 *      false, otherwise.
 */
void
HomaTransport::replaceActiveMessage(ScheduledMessage *oldMessage,
        ScheduledMessage *newMessage, bool cancelled)
{
    assert(oldMessage != newMessage);
    assert(oldMessage->state == ScheduledMessage::ACTIVE);
    assert(newMessage == NULL ||
            newMessage->state == ScheduledMessage::NEW ||
            newMessage->state == ScheduledMessage::INACTIVE);

    bool purgeOK = (oldMessage->grantOffset ==
            oldMessage->accumulator->totalLength);

    if (oldMessage == &activeMessages.front()) {
        // The top message is removed. Reclaim the highest granted priority.
        highestGrantedPrio--;
    }
    erase(activeMessages, *oldMessage);
    if (newMessage == NULL) {
        assert(purgeOK || cancelled);
        oldMessage->state = ScheduledMessage::PURGED;

        // No designated message to promote. Pick one from the inactive
        // messages.
        for (ScheduledMessage& candidate : inactiveMessages) {
            if (newMessage != NULL && newMessage->compareTo(candidate) <= 0) {
                continue;
            }

            for (ScheduledMessage& m : activeMessages) {
                if (m.senderHash == candidate.senderHash) {
                    // Active messages must come from distinct senders. Move on
                    // to check the next inactive message.
                    goto tryNextCandidate;
                }
            }

            newMessage = &candidate;
            tryNextCandidate:
            ;
        }
    } else {
        assert(!purgeOK);
        oldMessage->state = ScheduledMessage::INACTIVE;
        inactiveMessages.push_back(*oldMessage);
    }

    if (newMessage) {
        adjustSchedulingPrecedence(newMessage);
        if (newMessage == &activeMessages.front()
                && highestGrantedPrio < highestSchedPriority) {
            // This message is promoted to the top. Bump the highest granted
            // priority if we haven't used up all the priorities for scheduled
            // traffic.
            highestGrantedPrio++;
        } else if (highestGrantedPrio + 1 < int(activeMessages.size())) {
            // The priorities we are granting for scheduled traffic is not
            // enough to accommodate all the active messages.
            highestGrantedPrio++;
            assert(highestGrantedPrio + 1 == int(activeMessages.size()));
        }
    }

    // Packet the priorities for scheduled packets when there is no enough
    // message to buffer.
    if (activeMessages.size() < maxGrantedMessages) {
        highestGrantedPrio = int(activeMessages.size()) - 1;
    }

    assert(oldMessage->state == ScheduledMessage::INACTIVE ||
            oldMessage->state == ScheduledMessage::PURGED);
    assert(newMessage == NULL || newMessage->state == ScheduledMessage::ACTIVE);
}

/**
 * Attempts to schedule a message by placing it in the active message list.
 * This function is invoked when a new scheduled message arrives or an existing
 * inactive message tries to step up to the active message list.
 *
 * \param message
 *      A message that cannot be completely sent as unscheduled bytes.
 * \return
 *      True if the message will start to receive grants regularly after this
 *      this method returns; false, otherwise.
 */
bool
HomaTransport::tryToSchedule(ScheduledMessage* message)
{
    assert(message->state == ScheduledMessage::NEW ||
            message->state == ScheduledMessage::INACTIVE);
    bool newMessageArrives = (message->state == ScheduledMessage::NEW);

    // The following loop handles the special case where some active message
    // comes from the same sender as the new message to avoid violating the
    // constraint that active messages must come from distinct senders.
    for (ScheduledMessage &m : activeMessages) {
        if (m.senderHash != message->senderHash) {
            continue;
        }

        if (message->compareTo(m) < 0) {
            // The new message should replace an active message that is from
            // the same sender.
            replaceActiveMessage(&m, message);
        }
        goto schedulingDone;
    }

    // From now on, we can assume that the new message has a different sender
    // than the active messages.
    if (activeMessages.size() < maxGrantedMessages) {
        // We have not buffered enough messages. This also implies we have not
        // used up our priority levels for scheduled packets. Bump the highest
        // granted priority.
        assert(newMessageArrives); // TODO: EXPLAIN THE ASSERTION
        adjustSchedulingPrecedence(message);
        highestGrantedPrio++;
        assert(highestGrantedPrio <= highestSchedPriority);
        assert(highestGrantedPrio + 1 == int(activeMessages.size()));
    } else if (message->compareTo(activeMessages.back()) < 0) {
        // The new message should replace the "worst" active message.
        replaceActiveMessage(&activeMessages.back(), message);
    }

    schedulingDone:
    if (message->state == ScheduledMessage::ACTIVE) {
        return true;
    } else {
        if (newMessageArrives) {
            message->state = ScheduledMessage::INACTIVE;
            inactiveMessages.push_back(*message);
        }
        return false;
    }
}

/**
 * When a scheduled message receives a data packet, this method is invoked to
 * to see if the scheduler needs to 1) activate an inactive message and put it
 * into the active message list, 2) adjust its scheduling precedence among the
 * active messages and 3) send out a GRANT for another data packet.
 *
 * \param message
 *      The message that receives new data.
 * \param scheduledPacket
 *      True if the data packet contains scheduled bytes of the message; false,
 *      otherwise.
 */
void
HomaTransport::dataArriveForScheduledMessage(ScheduledMessage* message,
        bool scheduledPacket)
{
    // See if we need to adjust the scheduling precedence of this message.
    switch (message->state) {
        case ScheduledMessage::ACTIVE:
            adjustSchedulingPrecedence(message);
            break;
        case ScheduledMessage::INACTIVE:
            tryToSchedule(message);
            break;
        case ScheduledMessage::PURGED:
            // A scheduled message will be purged from the scheduler as soon
            // as it was fully granted. However, we will continue to receive
            // data packets from it for a while.
            break;
        default:
            LOG(ERROR, "unexpected message state %u", message->state);
            return;
    }

    // Output a GRANT if this packet is scheduled (i.e., it corresponds to a
    // GRANT sent in the past) or it belongs to a message is now active or
    // purged.
    bool needGrant = scheduledPacket ||
            (message->state == ScheduledMessage::ACTIVE) ||
            (message->state == ScheduledMessage::PURGED);
    if (!needGrant) {
        return;
    }

    // Find the first active message that could use a GRANT.
    ScheduledMessage* messageToGrant = NULL;
    uint8_t priority;
    if (highestGrantedPrio >= 0) {
        priority = downCast<uint8_t>(highestGrantedPrio);
    }  else {
        // No scheduled message.
        assert(activeMessages.size() + inactiveMessages.size() == 0);
        return;
    }
    for (ScheduledMessage& m : activeMessages) {
        // TODO: DO WE WANT TO MAKE THE OUTSTANDING BYTES CONFIGURABLE/EXPLICIT
        // BY INTRODUCING A VAR NAMED `outstandingBytesWindow`?
        if (m.grantOffset <
                m.accumulator->estimatedReceivedBytes + roundTripBytes) {
            messageToGrant = &m;
            break;
        }
        priority--;
    }

    if (messageToGrant == NULL) {
        return;
    }
    if (messageToGrant->accumulator->totalLength - messageToGrant->grantOffset
            <= roundTripBytes) {
        // For the last 1 RTT remaining bytes of a scheduled message
        // with size (1+a)RTT, use the same priority as an unscheduled
        // message that has size min{1, a}*RTT.
        priority = getUnschedTrafficPrio(std::min(roundTripBytes,
                messageToGrant->accumulator->totalLength - roundTripBytes));
    }

    messageToGrant->grantOffset += grantIncrement;
    messageToGrant->grantPriority = priority;
    if (messageToGrant->grantOffset >=
            messageToGrant->accumulator->totalLength) {
        // Slow path: a message has been fully granted. Purge it from the
        // scheduler.
        messageToGrant->grantOffset = messageToGrant->accumulator->totalLength;
        replaceActiveMessage(messageToGrant, NULL);
    }

    // Output a GRANT for the selected message.
    if (std::find(grantRecipients.begin(), grantRecipients.end(),
            messageToGrant) == grantRecipients.end()) {
        grantRecipients.push_back(messageToGrant);
    }
}

}  // namespace RAMCloud
