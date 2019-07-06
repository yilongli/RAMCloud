/* Copyright (c) 2010-2017 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER
 * RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF
 * CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

/**
 * \file
 * Implementation for #RAMCloud::InfUdDriver, an Infiniband packet
 * driver using unconnected datagram queue-pairs (UD).
 */


#include <errno.h>
#include <sys/types.h>

#include "Common.h"
#include "Cycles.h"
#include "CycleCounter.h"
#include "BitOps.h"
#include "InfUdDriver.h"
#include "PcapFile.h"
#include "PerfStats.h"
#include "ServiceLocator.h"
#include "ShortMacros.h"
#include "TimeTrace.h"

namespace RAMCloud {

// Change 0 -> 1 in the following line to compile detailed time tracing in
// this driver.
#define TIME_TRACE 0

// Provides a cleaner way of invoking TimeTrace::record, with the code
// conditionally compiled in or out by the TIME_TRACE #ifdef. Arguments
// are made uint64_t (as opposed to uin32_t) so the caller doesn't have to
// frequently cast their 64-bit arguments into uint32_t explicitly: we will
// help perform the casting internally.
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
}

// Change 0 -> 1 to trace transmit buffers containing more than 500B data.
// When combined with busy transmit CQ polling, it can be used to understand
// when a packet is on the wire.
#define TRACE_TRANSMIT_BUF 0

// Used to trace packet receipt. The resulting messages can be used to plot
// network util. graph.
#define TRACE_RECEIVE_PACKET 0

// Change 0 -> 1 in the following line to compile the code for collecting
// detailed PerfStats metrics in this driver.
#define COLLECT_LOW_LEVEL_PERFSTATS 0

/**
 * Construct an InfUdDriver.
 *
 * \param context
 *      Overall information about the RAMCloud server or client.
 * \param sl
 *      Service locator for transport that will be using this driver.
 *      May contain any of the following parameters, which are used
 *      to configure the new driver:
 *      dev -      Infiniband device name to use.
 *      devPort -  Infiniband port to use.
 *      gbs -      Bandwidth of the Infiniband network, in Gbits/sec.
 *                 Used for estimating transmit queue lengths.
 *      mac -      MAC address for this host; if ethernet is true.
 *      Specifies the Infiniband device and physical port to use.
 *      If NULL, the first device and port are used by default.
 * \param ethernet
 *      True means this driver sends and receives raw Ethernet packets
 *      using the Ethernet port. False means this driver sends and receives
 *      Infiniband unreliable datagrams, using the Infiniband port.
 */
InfUdDriver::InfUdDriver(Context* context, const ServiceLocator *sl,
        bool ethernet)
    : Driver(context)
    , realInfiniband()
    , infiniband()
    , rxPool()
    , rxBuffersInHca(0)
    , rxBufferLogThreshold(0)
    , txPool()
    , txBuffersInHca()
    , QKEY(ethernet ? 0 : 0xdeadbeef)
    , rxcq(0)
    , txcq(0)
    , qp()
    , ibPhysicalPort(ethernet ? 2 : 1)
    , lid(0)
    , mtu(0)
    , qpn(0)
    , localMac()
    , locatorString("infud:")
    , bandwidthGbps(~0u)
    , zeroCopyStart(NULL)
    , zeroCopyEnd(NULL)
    , zeroCopyRegion(NULL)
    , sendsSinceLastSignal(0)
{
    const char *ibDeviceName = NULL;
    bool macAddressProvided = false;

    if (sl != NULL) {
        locatorString = sl->getDriverLocatorString();

        if (ethernet) {
            try {
                localMac.construct(sl->getOption<const char*>("mac"));
                macAddressProvided = true;
                mtu = 1514;
            } catch (ServiceLocator::NoSuchKeyException& e) {}
        }

        try {
            ibDeviceName   = sl->getOption<const char *>("dev");
        } catch (ServiceLocator::NoSuchKeyException& e) {}

        try {
            ibPhysicalPort = sl->getOption<int>("devport");
        } catch (ServiceLocator::NoSuchKeyException& e) {}

        try {
            bandwidthGbps = sl->getOption<uint32_t>("gbs");
        } catch (ServiceLocator::NoSuchKeyException& e) {}
    }
    infiniband = realInfiniband.construct(ibDeviceName);
#if 1
    bandwidthGbps = std::min(bandwidthGbps,
            infiniband->getBandwidthGbps(ibPhysicalPort));
#else
    // As of 11/2018, the network bandwidth of our RC machines at Stanford is
    // actually limited by the effective bandwidth of PCIe 2.0x4, which should
    // be ~29Gbps when taking into account the overhead of PCIe headers.
    // For example, suppose the HCA's MTU is 256B and the PCIe headers are 24B
    // in total, the effective bandwidth of PCIe 2.0x4 is
    //      32Gbps * 256 / (256 + 24) = 29.25Gbps
    // Unfortunately, it appears that our ConnextX-2 HCA somehow cannot fully
    // utilize the 29Gbps PCIe bandwidth when sending UD packets. This can be
    // verified by running one or more ib_send_bw programs on two machines.
    // The maximum outgoing bandwidth we can achieve in practice is ~3020MB/s,
    // or 23.6Gbps. Note that we need to set the outgoing bandwidth slightly
    // higher than 24Gbps in order to saturate the 23.6Gbps outgoing bandwidth.
    // This is because the throughput of the HCA has non-negligible variation:
    // when it's running faster than 24Gbps, we don't want the transport to
    // throttle the throughput and leave the HCA idle.
    bandwidthGbps = std::min(bandwidthGbps, 26u);
#endif
    mtu = infiniband->getMtu(ibPhysicalPort);
    queueEstimator.setBandwidth(1000*bandwidthGbps);
    maxTransmitQueueSize = (uint32_t) (static_cast<double>(bandwidthGbps)
            * MAX_DRAIN_TIME / 8.0);
    uint32_t maxPacketSize = getMaxPacketSize();
    // Note: we were using 2 full packets for 10Gb Ethernet; adjust it to 3 full
    // packets for our 25Gbps infiniband network.
#define MAX_QUEUED_PACKETS 3
    if (maxTransmitQueueSize < MAX_QUEUED_PACKETS*maxPacketSize) {
        // Make sure that we advertise enough space in the transmit queue to
        // prepare the next packet while the current one is transmitting.
        maxTransmitQueueSize = MAX_QUEUED_PACKETS*maxPacketSize;
    }
    LOG(NOTICE, "InfUdDriver bandwidth: %u Gbits/sec, maxTransmitQueueSize: "
            "%u bytes, maxPacketSize %u bytes", bandwidthGbps,
            maxTransmitQueueSize, maxPacketSize);

    if (ethernet && !macAddressProvided)
        localMac.construct(MacAddress::RANDOM);

    // Allocate buffer pools.
    uint32_t bufSize = (maxPacketSize +
        (localMac ? sizeof32(EthernetHeader) : GRH_SIZE));
    bufSize = BitOps::powerOfTwoGreaterOrEqual(bufSize);
    uint64_t start = Cycles::rdtsc();

    // The "+0" syntax below is a hack that avoids linker "Undefined reference"
    // errors that would occur otherwise (as of 8/2016).
    rxPool.construct(infiniband, bufSize, TOTAL_RX_BUFFERS+0);
    rxBufferLogThreshold = TOTAL_RX_BUFFERS - 1000;
    txPool.construct(infiniband, bufSize, MAX_TX_QUEUE_DEPTH+0);
    double seconds = Cycles::toSeconds(Cycles::rdtsc() - start);
    LOG(NOTICE, "Initialized InfUdDriver buffers: %u receive buffers (%u MB), "
            "%u transmit buffers (%u MB), took %.1f ms",
            TOTAL_RX_BUFFERS, (TOTAL_RX_BUFFERS*bufSize)/(1024*1024),
            MAX_TX_QUEUE_DEPTH, (MAX_TX_QUEUE_DEPTH*bufSize)/(1024*1024),
            seconds*1e03);

    // Create completion queues for receive and transmit.
    rxcq = infiniband->createCompletionQueue(MAX_RX_QUEUE_DEPTH);
    if (rxcq == NULL) {
        LOG(ERROR, "failed to create receive completion queue");
        throw DriverException(HERE, errno);
    }

    txcq = infiniband->createCompletionQueue(MAX_TX_QUEUE_DEPTH);
    if (txcq == NULL) {
        LOG(ERROR, "failed to create transmit completion queue");
        throw DriverException(HERE, errno);
    }

    qp = infiniband->createQueuePair(localMac ? IBV_QPT_RAW_ETH
                                              : IBV_QPT_UD,
                                     ibPhysicalPort, NULL,
                                     txcq, rxcq, MAX_TX_QUEUE_DEPTH,
                                     MAX_RX_QUEUE_DEPTH,
                                     QKEY);

    // Cache these for easier access.
    lid = infiniband->getLid(ibPhysicalPort);
    qpn = qp->getLocalQpNumber();

    // Update our locatorString, if one was provided, with the dynamic
    // address.
    if (!locatorString.empty()) {
        char c = locatorString[locatorString.size()-1];
        if ((c != ':') && (c != ',')) {
            locatorString += ",";
        }
        if (localMac) {
            if (!macAddressProvided)
                locatorString += "mac=" + localMac->toString();
        } else {
            locatorString += format("lid=%u,qpn=%u", lid, qpn);
        }
        LOG(NOTICE, "Locator for InfUdDriver: %s", locatorString.c_str());
    }

    refillReceiver();
    qp->activate(localMac);
}

/**
 * Destroy an InfUdDriver and free allocated resources.
 */
InfUdDriver::~InfUdDriver()
{
    size_t buffersInUse = TOTAL_RX_BUFFERS - rxPool->freeBuffers.size()
            - rxBuffersInHca;
    if (buffersInUse != 0) {
        LOG(WARNING, "Infiniband destructor called with %lu receive "
                "buffers in use", buffersInUse);
    }

    delete qp;
    ibv_destroy_cq(rxcq);
    ibv_destroy_cq(txcq);
}

/*
 * See docs in the ``Driver'' class.
 */
uint32_t
InfUdDriver::getMaxPacketSize()
{
    return localMac ? mtu - sizeof32(EthernetHeader) : mtu - GRH_SIZE;
}

/**
 * Return a free transmit buffer, wrapped by its corresponding
 * BufferDescriptor. If there are none, block until one is available.
 *
 * Any errors from previous transmissions are basically
 *               thrown on the floor, though we do log them. We need
 *               to think a bit more about how this 'fire-and-forget'
 *               behaviour impacts our Transport API.
 * This code is copied from InfRcTransport. It should probably
 *               move in some form to the Infiniband class.
 */
InfUdDriver::BufferDescriptor*
InfUdDriver::getTransmitBuffer()
{
    // if we've drained our free tx buffer pool, we must wait.
    if (txPool->freeBuffers.empty()) {
        reapTransmitBuffers();
        if (txPool->freeBuffers.empty()) {
            // We are temporarily out of buffers. Time how long it takes
            // before a transmit buffer becomes available again (a long
            // time is a bad sign); in the normal case this code should
            // not be invoked.
            uint64_t start = Cycles::rdtsc();
            uint32_t count = 1;
            while (txPool->freeBuffers.empty()) {
                reapTransmitBuffers();
                count++;
            }
            timeTrace("TX buffers refilled after polling CQ %u times", count);
            double waitMillis = 1e03 * Cycles::toSeconds(Cycles::rdtsc()
                    - start);
            if (waitMillis > 1.0)  {
                LOG(WARNING, "Long delay waiting for transmit buffers "
                        "(%.1f ms elapsed, %lu buffers now free)",
                        waitMillis, txPool->freeBuffers.size());
            }
        }
    }

    BufferDescriptor* bd = txPool->freeBuffers.back();
    txPool->freeBuffers.pop_back();
    return bd;
}

/**
 * Check the NIC to see if it is ready to return transmit buffers
 * from previously-transmit packets. If there are any available,
 * reclaim them. This method also detects and logs transmission errors.
 */
void
InfUdDriver::reapTransmitBuffers()
{
#define MAX_TO_RETRIEVE 4
    ibv_wc retArray[MAX_TO_RETRIEVE];
    int cqes = ibv_poll_cq(txcq, MAX_TO_RETRIEVE, retArray);
    if (cqes) {
        timeTrace("polling TX completion queue returned %d CQEs", cqes);
    }
    for (int i = 0; i < cqes; i++) {
        BufferDescriptor* signaledCompletion =
            reinterpret_cast<BufferDescriptor*>(retArray[i].wr_id);
        bool found = false;
        while (!txBuffersInHca.empty()) {
            BufferDescriptor* bd = txBuffersInHca.front();
            txBuffersInHca.pop_front();
            txPool->freeBuffers.push_back(bd);
#if TRACE_TRANSMIT_BUF
            if (bd->packetLength > 500) {
                TimeTrace::record("infud: transmit buffer %u freed",
                        uint32_t(uint64_t(bd)));
            }
#endif
            if (bd == signaledCompletion) {
                found = true;
                timeTrace("reaped %d TX buffers", SIGNALED_SEND_PERIOD);
                break;
            }
        }
        if (!found) {
            DIE("Couldn't find the send request (SR) just completed");
        }

        if (retArray[i].status != IBV_WC_SUCCESS) {
            LOG(WARNING, "Infud transmit failed: %s",
                infiniband->wcStatusToString(retArray[i].status));
        }
    }
}

/*
 * See docs in the ``Driver'' class.
 */
void
InfUdDriver::registerMemory(void* base, size_t bytes)
{
    // We can only remember one region (the first)
    if (zeroCopyRegion == NULL) {
        zeroCopyRegion = ibv_reg_mr(infiniband->pd.pd, base, bytes,
            IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE);
        if (zeroCopyRegion == NULL) {
            LOG(ERROR, "ibv_reg_mr failed to register %lu bytes at %p",
                    bytes, base);
            return;
        }
        zeroCopyStart = reinterpret_cast<char*>(base);
        zeroCopyEnd = zeroCopyStart + bytes;
        RAMCLOUD_LOG(NOTICE, "Created zero-copy region with %lu bytes at %p",
                bytes, base);
    }
}

/*
 * See docs in the ``Driver'' class.
 */
void
InfUdDriver::release()
{
    while (!packetsToRelease.empty()) {
        // Payload points to the first byte of the packet buffer after the
        // Ethernet header or GRH header; from that, compute the address of its
        // corresponding buffer descriptor.
        char* payload = packetsToRelease.back();
        packetsToRelease.pop_back();
        if (localMac) {
            payload -= sizeof(EthernetHeader);
        } else {
            payload -= GRH_SIZE;
        }
        int index = downCast<int>((payload - rxPool->bufferMemory)
                /rxPool->descriptors[0].length);
        BufferDescriptor* bd = &rxPool->descriptors[index];
        assert(payload == bd->buffer);
        rxPool->freeBuffers.push_back(bd);
    }
}

/*
 * See docs in the ``Driver'' class.
 */
void
InfUdDriver::sendPacket(const Driver::Address* addr,
                        const void* header,
                        uint32_t headerLen,
                        Buffer::Iterator* payload,
                        int priority,
                        TransmitQueueState* txQueueState)
{
#if COLLECT_LOW_LEVEL_PERFSTATS
    uint64_t startTime = Cycles::rdtscp();
#endif
    uint32_t totalLength = headerLen +
                           (payload ? payload->size() : 0);
    assert(totalLength <= getMaxPacketSize());

    BufferDescriptor* bd = getTransmitBuffer();
    bd->packetLength = totalLength;

    // Create the Infiniband work request.
    char *p = bd->buffer;
    if (localMac) {
        auto& ethHdr = *new(p) EthernetHeader;
        memcpy(ethHdr.destAddress,
               static_cast<const MacAddress*>(addr)->address, 6);
        memcpy(ethHdr.sourceAddress, localMac->address, 6);
        ethHdr.etherType = HTONS(0x8001);
        ethHdr.length = downCast<uint16_t>(totalLength);
        p += sizeof(ethHdr);
        bd->packetLength += sizeof32(ethHdr);
    }
    memcpy(p, header, headerLen);
    p += headerLen;

    ibv_sge sges[2];
    sges[0].addr = reinterpret_cast<uint64_t>(bd->buffer);
    sges[0].length = bd->packetLength;
    sges[0].lkey = bd->memoryRegion->lkey;
    int numSges = 1;
    while (payload && !payload->isDone()) {
        // Use zero copy for the last chunk of the packet, if it's in the
        // zero copy region and is large enough to justify the overhead
        // of an addition scatter-gather element.
        const char *currentChunk =
                reinterpret_cast<const char*>(payload->getData());
        if ((payload->getLength() >= 500)
                && (currentChunk >= zeroCopyStart)
                && ((currentChunk + payload->getLength()) <= zeroCopyEnd)
                && (payload->getLength() >= payload->size())) {
            sges[1].addr = reinterpret_cast<uint64_t>(currentChunk);
            sges[1].length = payload->getLength();
            sges[1].lkey = zeroCopyRegion->lkey;
            sges[0].length -= payload->getLength();
            numSges = 2;
            break;
        } else {
            memcpy(p, currentChunk, payload->getLength());
            p += payload->getLength();
            timeTrace("0-copy not applicable; copied %u bytes",
                    payload->getLength());
        }
        payload->next();
    }

    ibv_send_wr workRequest;
    memset(&workRequest, 0, sizeof(workRequest));

    // This id is used to locate the BufferDescriptor from the
    // completion notification.
    workRequest.wr_id = reinterpret_cast<uint64_t>(bd);
    const Address* address = static_cast<const Address*>(addr);
    workRequest.wr.ud.ah = address->getHandle();
    workRequest.wr.ud.remote_qpn = address->getQpn();
    workRequest.wr.ud.remote_qkey = QKEY;
    workRequest.next = NULL;
    workRequest.sg_list = sges;
    workRequest.num_sge = numSges;
    workRequest.opcode = IBV_WR_SEND;
    sendsSinceLastSignal++;
    if (sendsSinceLastSignal >= SIGNALED_SEND_PERIOD) {
        workRequest.send_flags = IBV_SEND_SIGNALED;
        sendsSinceLastSignal = 0;
    }

    // We can get a substantial latency improvement (nearly 2usec less per RTT)
    // by inlining data with the WQE for small messages. The Verbs library
    // automatically takes care of copying from the SGEs to the WQE.
    if (bd->packetLength <= Infiniband::MAX_INLINE_DATA)
        workRequest.send_flags |= IBV_SEND_INLINE;

#if COLLECT_LOW_LEVEL_PERFSTATS
    uint64_t prepareWorkRequest = Cycles::rdtscp();
#endif
    ibv_send_wr *bad_txWorkRequest;
    lastTransmitTime = Cycles::rdtsc();
    if (ibv_post_send(qp->qp, &workRequest, &bad_txWorkRequest)) {
        LOG(WARNING, "Error posting transmit packet: %s", strerror(errno));
        txPool->freeBuffers.push_back(bd);
        return;
    } else {
        txBuffersInHca.push_back(bd);
#if TRACE_TRANSMIT_BUF
        if (bd->packetLength > 500) {
            TimeTrace::record("infud: transmit buffer %u of size %u enqueued",
                    bd->packet_len, uint32_t(uint64_t(bd)));
            reapTransmitBuffers();
        }
#endif
    }
    timeTrace("sent packet with %u bytes, %u free buffers", totalLength,
            txPool->freeBuffers.size());
#if COLLECT_LOW_LEVEL_PERFSTATS
    uint64_t ibvPostSend = Cycles::rdtscp();
#endif
    queueEstimator.packetQueued(bd->packetLength, lastTransmitTime,
            txQueueState);
    PerfStats::threadStats.networkOutputBytes += bd->packetLength;
    PerfStats::threadStats.networkOutputPackets++;
#if COLLECT_LOW_LEVEL_PERFSTATS
    uint64_t stopTime = Cycles::rdtscp();
    PerfStats::threadStats.infudDriverTxCycles += stopTime - startTime;
    PerfStats::threadStats.infudDriverTxPrepareCycles +=
            prepareWorkRequest - startTime;
    PerfStats::threadStats.infudDriverTxPostSendCycles +=
            ibvPostSend - prepareWorkRequest;
    PerfStats::threadStats.infudDriverTxPostProcessCycles +=
            stopTime - ibvPostSend;
#endif
}

/*
 * See docs in the ``Driver'' class.
 */
void
InfUdDriver::receivePackets(uint32_t maxPackets,
            std::vector<Received>* receivedPackets)
{
#if COLLECT_LOW_LEVEL_PERFSTATS
    uint64_t startTime = Cycles::rdtscp();
#endif
    static const int MAX_COMPLETIONS = 50;
    ibv_wc wc[MAX_COMPLETIONS];
    uint32_t maxToReceive = (maxPackets < MAX_COMPLETIONS) ? maxPackets
            : MAX_COMPLETIONS;
    int numPackets = ibv_poll_cq(qp->rxcq, maxToReceive, wc);
    if (numPackets <= 0) {
        if (numPackets < 0) {
            LOG(ERROR, "ibv_poll_cq failed with result %d", numPackets);
        }
        return;
    }
    lastReceiveTime = Cycles::rdtsc();
    timeTrace("InfUdDriver received %d packets", numPackets);
#if COLLECT_LOW_LEVEL_PERFSTATS
    uint64_t ibvPollCq = Cycles::rdtscp();
#endif

    // First, prefetch the initial bytes of all the incoming packets. This
    // allows us to process multiple cache misses concurrently, which improves
    // throughput under load.
    for (int i = 0; i < numPackets; i++) {
        ibv_wc* incoming = &wc[i];
        BufferDescriptor *bd =
                reinterpret_cast<BufferDescriptor*>(incoming->wr_id);
        prefetch(bd->buffer,
                incoming->byte_len > 256 ? 256 : incoming->byte_len);
#if TRACE_RECEIVE_PACKET
        if (incoming->byte_len > 500) {
            TimeTrace::record(lastReceiveTime, "infud received packet, size %u,"
                    " batch size %u", incoming->byte_len, uint32_t(numPackets));
        }
#endif
    }
    refillReceiver();
    timeTrace("receive queue refilled");

    rxBuffersInHca -= numPackets;
    if (rxBuffersInHca == 0) {
        RAMCLOUD_CLOG(WARNING, "Infiniband receiver temporarily ran "
                "out of packet buffers; could result in dropped packets");
    }

#if COLLECT_LOW_LEVEL_PERFSTATS
    uint64_t startProcessPacket = Cycles::rdtscp();
#endif
    // Each iteration of the following loop processes one incoming packet.
    for (int i = 0; i < numPackets; i++) {
        ibv_wc* incoming = &wc[i];
        BufferDescriptor *bd =
                reinterpret_cast<BufferDescriptor*>(incoming->wr_id);
        if (incoming->status != IBV_WC_SUCCESS) {
            LOG(ERROR, "Infiniband receive error (%d: %s)",
                incoming->status,
                infiniband->wcStatusToString(incoming->status));
            goto error;
        }

        bd->packetLength = incoming->byte_len;
        if (bd->packetLength < (localMac ? 60 : GRH_SIZE)) {
            LOG(ERROR, "received impossibly short packet: %d bytes",
                    bd->packetLength);
            goto error;
        }

        PerfStats::threadStats.networkInputBytes += bd->packetLength;
        PerfStats::threadStats.networkInputPackets++;
        if (localMac) {
            EthernetHeader* ethHdr = reinterpret_cast<EthernetHeader*>(
                    bd->buffer);
            if (ethHdr->length + sizeof(EthernetHeader) > bd->packetLength) {
                LOG(ERROR, "corrupt packet (data length %d, packet length %d",
                        ethHdr->length, bd->packetLength);
                goto error;
            }
            bd->macAddress.construct(ethHdr->sourceAddress);
            uint32_t length = ethHdr->length;
            receivedPackets->emplace_back(bd->macAddress.get(), this,
                    length, bd->buffer + sizeof(EthernetHeader));
        } else {
            bd->infAddress.construct(*infiniband,
                    ibPhysicalPort, incoming->slid, incoming->src_qp);
            receivedPackets->emplace_back(bd->infAddress.get(), this,
                    bd->packetLength - GRH_SIZE, bd->buffer + GRH_SIZE);
        }
        continue;

      error:
        rxPool->freeBuffers.push_back(bd);
    }
    timeTrace("InfUdDriver::receivePackets done");
#if COLLECT_LOW_LEVEL_PERFSTATS
    uint64_t stopTime = Cycles::rdtscp();
    PerfStats::threadStats.infudDriverRxCycles += stopTime - startTime;
    PerfStats::threadStats.infudDriverRxPollCqCycles += ibvPollCq - startTime;
    PerfStats::threadStats.infudDriverRxRefillCycles +=
            startProcessPacket - ibvPollCq;
    PerfStats::threadStats.infudDriverRxProcessPacketCycles +=
            stopTime - startProcessPacket;
#endif
}

/**
 * See docs in the ``Driver'' class.
 */
string
InfUdDriver::getServiceLocator()
{
    return locatorString;
}

// See docs in Driver class.
uint32_t
InfUdDriver::getBandwidth()
{
    return bandwidthGbps*1000;
}

/**
 * Fill up the HCA's queue of pending receive buffers.
 */
void
InfUdDriver::refillReceiver()
{
    while ((rxBuffersInHca < MAX_RX_QUEUE_DEPTH)
            && !rxPool->freeBuffers.empty()) {
        BufferDescriptor* bd = rxPool->freeBuffers.back();
        rxPool->freeBuffers.pop_back();
        rxBuffersInHca++;

        ibv_sge isge = {reinterpret_cast<uint64_t>(bd->buffer), bd->length,
                bd->memoryRegion->lkey};
        ibv_recv_wr workRequest;
        memset(&workRequest, 0, sizeof(workRequest));
        workRequest.wr_id   = reinterpret_cast<uint64_t>(bd);
        workRequest.next    = NULL;
        workRequest.sg_list = &isge;
        workRequest.num_sge = 1;

        ibv_recv_wr *badWorkRequest;
        if (ibv_post_recv(qp->qp, &workRequest, &badWorkRequest) != 0) {
            DIE("Couldn't post Infiniband receive buffer: %s",
                    strerror(errno));
        }
    }

    // Generate log messages every time buffer usage reaches a significant new
    // high. Running out of buffers is a bad thing, so we want warnings in the
    // log long before that happens.
    uint32_t freeBuffers = downCast<uint32_t>(rxPool->freeBuffers.size());
    if (freeBuffers <= rxBufferLogThreshold) {
        double percentUsed = 100.0*static_cast<double>(
                TOTAL_RX_BUFFERS - freeBuffers)/TOTAL_RX_BUFFERS;
        LOG((percentUsed >= 80.0) ? WARNING : NOTICE,
                "%u receive buffers now in use (%.1f%%)",
                TOTAL_RX_BUFFERS - freeBuffers, percentUsed);
        do {
            rxBufferLogThreshold -= 1000;
        } while (freeBuffers < rxBufferLogThreshold);
    }
}

/**
 * Constructor for BufferPool objects.
 * 
 * \param infiniband
 *      Infiniband object that the buffers will be associated with.
 * \param bufferSize
 *      Size of each packet buffer, in bytes.
 * \param numBuffers
 *      Number of buffers to allocate in the pool.
 */
InfUdDriver::BufferPool::BufferPool(Infiniband* infiniband,
        uint32_t bufferSize, uint32_t numBuffers)
    : bufferMemory(NULL)
    , memoryRegion(NULL)
    , descriptors()
    , freeBuffers()
    , numBuffers(numBuffers)
{
    // Allocate space for the packet buffers (page aligned, full pages).
    size_t bytesToAllocate = ((bufferSize * numBuffers) + 4095) & ~0xfff;
    bufferMemory = reinterpret_cast<char*>(Memory::xmemalign(HERE, 4096,
            bytesToAllocate));

    memoryRegion = ibv_reg_mr(infiniband->pd.pd, bufferMemory, bytesToAllocate,
                IBV_ACCESS_LOCAL_WRITE);
    if (memoryRegion == NULL) {
        DIE("Couldn't register Infiniband memory region: %s", strerror(errno));
    }
    descriptors = reinterpret_cast<BufferDescriptor*>(
            malloc(numBuffers*sizeof(BufferDescriptor)));
    char* buffer = bufferMemory;
    for (uint32_t i = 0; i < numBuffers; i++) {
        new(&descriptors[i]) BufferDescriptor(buffer, bufferSize, memoryRegion);
        freeBuffers.push_back(&descriptors[i]);
        buffer += bufferSize;
    }
}

/**
 * Destructor for BufferPools.
 */
InfUdDriver::BufferPool::~BufferPool()
{
    if (memoryRegion != NULL) {
        ibv_dereg_mr(memoryRegion);
    }
    // `bufferMemory` and `descriptors` are allocated using malloc.
    free(bufferMemory);
    for (uint32_t i = 0; i < numBuffers; i++) {
        descriptors[i].~BufferDescriptor();
    }
    free(descriptors);
}

} // namespace RAMCloud
