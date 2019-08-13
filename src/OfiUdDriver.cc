/* Copyright (c) 2019 Stanford University
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
 * Implementation for #RAMCloud::OfiUdDriver, a libfabric packet driver using
 * unreliable datagrams
 */

#include <fstream>
#include <sys/types.h>

#include <rdma/fi_cm.h>
#include <rdma/fi_errno.h>

#include "Common.h"
#include "Cycles.h"
#include "BitOps.h"
#include "OfiUdDriver.h"
#include "OptionParser.h"
#include "NetUtil.h"
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

#define STR(token) #token

// Most libfabric APIs return negative values to indicate errors. This macro
// provides a simple way to invoke such an API and check its return value.
// For simplicity, this macro doesn't try to recover from the error; it simply
// logs a message and exits upon error.
#define FI_CHK_CALL(fn, ...)                                        \
        do {                                                        \
            int ret = fn(__VA_ARGS__);                              \
            if (ret) {                                              \
                DIE("%s failed: %s", STR(fn), fi_strerror(-ret));   \
            }                                                       \
        } while (0)

// Short-hand to obtain the starting address of a BufferDescriptor based on its
// libfabric context address.
#define context_to_bd(ctx) reinterpret_cast<BufferDescriptor*>( \
    static_cast<char*>(ctx) - OFFSET_OF(BufferDescriptor, context))

// TODO: hmm, actually, this is not absolutely necessary; we could just pass
// NULL desc, right?
fid_mr OfiUdDriver::NO_MEMORY_REGION = {};
            
/**
 * Construct an OfiUdDriver.
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
OfiUdDriver::OfiUdDriver(Context* context, const ServiceLocator *sl)
    : Driver(context)
    , fabric()
    , info()
    , domain()
    , endpoint()
    , addressVector()
    , rxcq()
    , txcq()
//    , corkedPackets()
    , loopbackPkts()
    , rxPool()
    , rxBuffersInNic(0)
    , rxBufferLogThreshold(0)
    , txPool()
    , txBuffersInNic()
    , datagramPrefixSize()
    , maxInlineData(MAX_INLINE_DATA)
    , maxPostTxBuffers()
    , maxPostRxBuffers()
    , mtu(0)
    , locatorString("ofiud:")
    , bandwidthGbps(0)
//    , sendRequests()
    , enableUnsignalSendOpt(false)
    , sendsSinceLastSignal(0)
    , zeroCopyStart(NULL)
    , zeroCopyEnd(NULL)
    , zeroCopyRegion(NULL)
{
    ServiceLocator config = readDriverConfigFile();
    const char* provider = config.getOption<const char*>("prov", NULL);
    bandwidthGbps = config.getOption<uint32_t>("gbs", 0);
    LOG(NOTICE, "OfiUdDriver config: %s", config.getOriginalString().c_str());

    // Fill out the hints struct to indicate the capabilities we need and the
    // operation modes we support.
    fi_info* hints = fi_allocinfo();
    hints->fabric_attr->prov_name = strdup(provider);
    // We need an endpoint that 1) supports sending and receiving messages or
    // datagrams and 2) returns source address as part of its completion data.
    // TODO: wtf, provider verbs doesn't support FI_SOURCE? how to get src addr then?
    hints->caps = FI_MSG | FI_SOURCE;
    // Make it clear that we need the endpoint for connectionless, unreliable
    // datagram (not a reliable, connection-oriented endpoint like FI_EP_MSG).
    hints->ep_attr->type = FI_EP_DGRAM;
    // We can support at least three operation modes: FI_CONTEXT, FI_MSG_PREFIX,
    // and FI_RX_CQ_DATA. The operation modes required by each provider can be
    // found at:
    //   https://github.com/ofiwg/libfabric/wiki/Provider-Feature-Matrix-master
    hints->mode = FI_CONTEXT | FI_MSG_PREFIX | FI_RX_CQ_DATA;
    // We have no restriction on the operational or memory registration modes
    // of the domain. FI_MR_BASIC and FI_MR_SCALABLE are obsolete and shouldn't
    // be used with other bits.
    hints->domain_attr->mode = ~0lu;
    hints->domain_attr->mr_mode = ~(FI_MR_BASIC | FI_MR_SCALABLE);

    // Query libfabric to obtain the most performant qualified endpoint.
    fi_info* infoList;
    FI_CHK_CALL(fi_getinfo, FI_VERSION(1, 8), NULL, NULL, 0, hints, &infoList);
    info = fi_dupinfo(infoList);
    fi_freeinfo(hints);
    fi_freeinfo(infoList);
    LOG(NOTICE, "Fabric interface info:\n%s", fi_tostr(info, FI_TYPE_INFO));

    // Set the runtime parameters of this driver based on the fabric info.
    info->tx_attr->op_flags = FI_INJECT_COMPLETE;
    if (info->tx_attr->size > MAX_TX_QUEUE_DEPTH) {
        info->tx_attr->size = MAX_TX_QUEUE_DEPTH;
    }
    if (info->rx_attr->size > MAX_RX_QUEUE_DEPTH) {
        info->rx_attr->size = MAX_RX_QUEUE_DEPTH;
    }
    if (info->tx_attr->comp_order & FI_ORDER_STRICT) {
        // Enable the selective notification optimization for sending packets.
        enableUnsignalSendOpt = true;
        info->tx_attr->comp_order = FI_ORDER_STRICT;
    }
    datagramPrefixSize = downCast<uint32_t>(info->ep_attr->msg_prefix_size);
    if (info->tx_attr->inject_size < maxInlineData) {
        maxInlineData = downCast<uint32_t>(info->tx_attr->inject_size);
    }
    maxPostTxBuffers = downCast<uint32_t>(info->tx_attr->iov_limit);
    maxPostRxBuffers = downCast<uint32_t>(info->rx_attr->iov_limit);

    // Compute link speed and MTU to setup the queue estimator.
    // FIXME: hack to workaround info->nic == NULL for psm2
    if (info->nic) {
        if (bandwidthGbps == 0) {
            bandwidthGbps = downCast<uint32_t>(
                    static_cast<double>(info->nic->link_attr->speed) /
                    (1024.0*1024.0*1024.0));
        }
        mtu = downCast<uint32_t>(info->nic->link_attr->mtu);
    } else {
        mtu = 4096;
    }
    queueEstimator.setBandwidth(1000*bandwidthGbps);
    maxTransmitQueueSize = (uint32_t) (static_cast<double>(bandwidthGbps)
            * MAX_DRAIN_TIME / 8.0);
    uint32_t maxPacketSize = getMaxPacketSize();
    if (maxTransmitQueueSize < 2*maxPacketSize) {
        // Make sure that we advertise enough space in the transmit queue to
        // prepare the next packet while the current one is transmitting.
        maxTransmitQueueSize = 2*maxPacketSize;
    }
    LOG(NOTICE, "OfiUdDriver bandwidth: %u Gbits/sec, maxTransmitQueueSize: "
            "%u bytes, maxPacketSize %u bytes", bandwidthGbps,
            maxTransmitQueueSize, maxPacketSize);

    // Initialize libfabric objects in the following order: fabric, domain,
    // endpoint, address vector, and completion queues. Note that the address
    // vector and the completion queues must be bound to the endpoint.
    FI_CHK_CALL(fi_fabric, info->fabric_attr, &fabric, NULL);
    FI_CHK_CALL(fi_domain, fabric, info, &domain, NULL);
    FI_CHK_CALL(fi_endpoint, domain, info, &endpoint, NULL);

    // Create the address vector and bind it to the endpoint.
    fi_av_attr av_attr = {};
    av_attr.type = FI_AV_MAP;
    FI_CHK_CALL(fi_av_open, domain, &av_attr, &addressVector, NULL);
    FI_CHK_CALL(fi_ep_bind, endpoint, &addressVector->fid, 0);

    // Create completion queues for receive and transmit. Note: a completion
    // queue isn't absolutely necessary for the transmit queue; a completion
    // counter should suffice.
    fi_cq_attr tx_cq_attr = {};
    tx_cq_attr.size = MAX_TX_QUEUE_DEPTH;
    tx_cq_attr.format = FI_CQ_FORMAT_CONTEXT;
    FI_CHK_CALL(fi_cq_open, domain, &tx_cq_attr, &txcq, NULL);
    FI_CHK_CALL(fi_ep_bind, endpoint, &txcq->fid, FI_TRANSMIT);
    // FIXME: deal with selective notification later!
//    FI_CHK_CALL(fi_ep_bind, endpoint, &txcq->fid,
//            FI_TRANSMIT | FI_SELECTIVE_COMPLETION);

    fi_cq_attr rx_cq_attr = {};
    rx_cq_attr.size = MAX_RX_QUEUE_DEPTH;
    rx_cq_attr.format = FI_CQ_FORMAT_CONTEXT;
    FI_CHK_CALL(fi_cq_open, domain, &rx_cq_attr, &rxcq, NULL);
    FI_CHK_CALL(fi_ep_bind, endpoint, &rxcq->fid, FI_RECV);

    // Activate the endpoint and update locatorString with the dynamic address.
    FI_CHK_CALL(fi_enable, endpoint);
    uint8_t rawAddress[64];
    size_t addrlen = 64;
    FI_CHK_CALL(fi_getname, (fid_t)endpoint, rawAddress, &addrlen);
    locatorString += format("addr=%u", rawAddress[0]);
    for (size_t i = 1; i < addrlen; i++) {
        locatorString += format(".%u", rawAddress[i]);
    }
    LOG(NOTICE, "Locator for OfiUdDriver: %s", locatorString.c_str());

    // Allocate buffer pools.
    uint32_t bufSize = BitOps::powerOfTwoGreaterOrEqual(mtu);
    rxPool.construct(domain, info->domain_attr, bufSize, TOTAL_RX_BUFFERS);
    rxBufferLogThreshold = TOTAL_RX_BUFFERS - 1000;
    txPool.construct(domain, info->domain_attr, bufSize, MAX_TX_QUEUE_DEPTH);
    LOG(NOTICE, "Initialized OfiUdDriver buffers: %u receive buffers (%u MB), "
            "%u transmit buffers (%u MB)",
            TOTAL_RX_BUFFERS, (TOTAL_RX_BUFFERS*bufSize)/(1024*1024),
            MAX_TX_QUEUE_DEPTH, (MAX_TX_QUEUE_DEPTH*bufSize)/(1024*1024));
    refillReceiver();

    DIE("Done for now!");
}

/**
 * Destroy an OfiUdDriver and free allocated resources.
 */
OfiUdDriver::~OfiUdDriver()
{
    fi_freeinfo(info);
    fi_close(&rxcq->fid);
    fi_close(&txcq->fid);
    fi_close(&addressVector->fid);
    fi_close(&endpoint->fid);
    fi_close(&domain->fid);
    fi_close(&fabric->fid);

    size_t buffersInUse = TOTAL_RX_BUFFERS - rxPool->freeBuffers.size()
            - rxBuffersInNic;
    if (buffersInUse != 0) {
        LOG(WARNING, "destructor called with %lu receive buffers in use",
                buffersInUse);
    }
}

/*
 * See docs in the ``Driver'' class.
 */
uint32_t
OfiUdDriver::getMaxPacketSize()
{
    return mtu - datagramPrefixSize;
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
OfiUdDriver::BufferDescriptor*
OfiUdDriver::getTransmitBuffer()
{
    // if we've drained our free tx buffer pool, we must wait.
    if (unlikely(txPool->freeBuffers.empty())) {
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
 * Read the driver configuration file (if there is one) and parse the ofiud
 * config to return a service locator.
 */
ServiceLocator
OfiUdDriver::readDriverConfigFile()
{
    string configDir = "config";
    if (context->options) {
        configDir = context->options->getConfigDir();
    }
    std::ifstream configFile(configDir + "/driver.conf");
    Tub<ServiceLocator> config;
    if (configFile.is_open()) {
        std::string sl;
        try {
            while (std::getline(configFile, sl)) {
                if ((sl.find('#') == 0) || (sl.find("ofiud") == string::npos)) {
                    // Skip comments and irrelevant lines.
                    continue;
                }
                return ServiceLocator(sl);
            }
        } catch (ServiceLocator::BadServiceLocatorException&) {
            LOG(ERROR, "Ignored bad driver configuration: '%s'", sl.c_str());
        }
    }
    return ServiceLocator("ofiud:");
}

/**
 * Check the NIC to see if it is ready to return transmit buffers
 * from previously-transmit packets. If there are any available,
 * reclaim them. This method also detects and logs transmission errors.
 */
void
OfiUdDriver::reapTransmitBuffers()
{
#define MAX_TO_RETRIEVE 4
    // FIXME: can we use fi_cq_entry instead?
    fi_cq_tagged_entry retArray[MAX_TO_RETRIEVE];
    int cqes = fi_cq_read(txcq, retArray, MAX_TO_RETRIEVE);
    if (cqes < 0) {
        DIE("fi_cq_read failed: %s", fi_strerror(-cqes));
    } else if (cqes > 0) {
        timeTrace("polling TX completion queue returned %d CQEs", cqes);
    }
    for (int i = 0; i < cqes; i++) {
        BufferDescriptor* signaledCompletion = context_to_bd(retArray[i].op_context);
        bool found = false;
        while (!txBuffersInNic.empty()) {
            BufferDescriptor* bd = txBuffersInNic.front();
            txBuffersInNic.pop_front();
            txPool->freeBuffers.push_back(bd);
            if (bd == signaledCompletion) {
                found = true;
                timeTrace("reaped %d TX buffers", SIGNALED_SEND_PERIOD);
                break;
            }
        }
        if (!found) {
            DIE("Couldn't find the send request (SR) just completed");
        }

        // FIXME: do we need to check error inside fi_cq_err_entry?
//        if (retArray[i].status != IBV_WC_SUCCESS) {
//            LOG(WARNING, "Infud transmit failed: %s",
//                infiniband->wcStatusToString(retArray[i].status));
//        }
    }
}

/*
 * See docs in the ``Driver'' class.
 */
void
OfiUdDriver::registerMemory(void* base, size_t bytes)
{
    if (info->domain_attr->mr_mode & FI_MR_LOCAL) {
        // We can only remember one region (the first)
        if (zeroCopyRegion == NULL) {
            int ret = fi_mr_reg(domain, base, bytes,
                    FI_SEND | FI_RECV, 0, 0, 0, &zeroCopyRegion, NULL);
            if (ret) {
                LOG(ERROR, "fi_mr_reg failed to register %lu bytes at %p",
                        bytes, base);
                return;
            }
            zeroCopyStart = reinterpret_cast<char*>(base);
            zeroCopyEnd = zeroCopyStart + bytes;
            LOG(NOTICE, "Created zero-copy region with %lu bytes at %p",
                    bytes, base);
        }
    } else {
        LOG(NOTICE, "Provider %s requires no memory registration",
                info->fabric_attr->prov_name);
    }
}

/*
 * See docs in the ``Driver'' class.
 */
void
OfiUdDriver::release()
{
    while (!packetsToRelease.empty()) {
        // Payload points to the first byte of the packet buffer after the
        // datagram prefix buffer; from that, compute the address of its
        // corresponding buffer descriptor.
        char* payload = packetsToRelease.back();
        packetsToRelease.pop_back();
        payload -= datagramPrefixSize;
        int index = downCast<int>((payload - rxPool->bufferMemory)
                / rxPool->descriptors[0].length);
        BufferDescriptor* bd = &rxPool->descriptors[index];
        assert(payload == bd->buffer);
        rxPool->freeBuffers.push_back(bd);
    }
}

/*
 * See docs in the ``Driver'' class.
 */
void
OfiUdDriver::sendPacket(const Driver::Address* addr,
                        const void* header,
                        uint32_t headerLen,
                        Buffer::Iterator* payload,
                        int priority,
                        TransmitQueueState* txQueueState)
{
    uint32_t payloadSize = payload ? payload->size() : 0;
    uint32_t totalLength = headerLen + payloadSize;
    assert(totalLength <= getMaxPacketSize());

    // Grab a free packet buffer.
    BufferDescriptor* bd = getTransmitBuffer();
    bd->packetLength = totalLength;

    // Copy transport header into packet buffer.
    char *dst = bd->buffer + datagramPrefixSize;
    memcpy(dst, header, headerLen);
    dst += headerLen;

    // Copy payload into packet buffer or apply zero-copy when approapriate.
    while (payload && !payload->isDone()) {
        memcpy(dst, payload->getData(), payload->getLength());
        dst += payload->getLength();
        payload->next();
    }

    // FIXME: how to use selective completion? 1) set FI_SELECTIVE_COMPLETION
    // when binding txcq to the endpoint; 2) use fi_sendmsg (rather than
    // fi_send to post the transmit buffers).
//    sendsSinceLastSignal++;
//    if (sendsSinceLastSignal >= SIGNALED_SEND_PERIOD) {
//        workRequest.send_flags = IBV_SEND_SIGNALED;
//        sendsSinceLastSignal = 0;
//    }

    // FIXME: how to use fi_inject for small message?
//    if (bd->packetLength <= MAX_INLINE_DATA)
//        workRequest.send_flags |= IBV_SEND_INLINE;

    // FIXME: how to reap transmit buffer? how to identify
    // Create the IB work request. wr_id is used to locate the BufferDescriptor
    // from the completion notification.
    const Address* address = static_cast<const Address*>(addr);
    lastTransmitTime = Cycles::rdtsc();
    int ret = fi_send(endpoint, bd->buffer, bd->packetLength,
            fi_mr_desc(bd->memoryRegion), address->addr, &bd->context);
    if (ret) {
        DIE("Error posting transmit packet: %s", fi_strerror(-ret));
    } else {
        txBuffersInNic.push_back(bd);
    }

    timeTrace("sent packet with %u bytes, %u free buffers", totalLength,
            txPool->freeBuffers.size());
    queueEstimator.packetQueued(bd->packetLength, lastTransmitTime,
            txQueueState);
    PerfStats::threadStats.networkOutputBytes += bd->packetLength;
    PerfStats::threadStats.networkOutputPackets++;
}

/*
 * See docs in the ``Driver'' class.
 */
void
OfiUdDriver::receivePackets(uint32_t maxPackets,
        std::vector<Received>* receivedPackets)
{
    static const int MAX_COMPLETIONS = 16;
    fi_cq_tagged_entry wc[MAX_COMPLETIONS];
    fi_addr_t srcAddr[MAX_COMPLETIONS];
    uint32_t maxToReceive = (maxPackets < MAX_COMPLETIONS) ? maxPackets
            : MAX_COMPLETIONS;
    int numPackets = fi_cq_readfrom(rxcq, wc, maxToReceive, srcAddr);
    if (numPackets <= 0) {
        if (unlikely(numPackets < 0)) {
            LOG(ERROR, "ibv_poll_cq failed with result %d", numPackets);
        }
        return;
    }
    lastReceiveTime = Cycles::rdtsc();
    timeTrace("InfUdDriver received %d packets", numPackets);

    rxBuffersInNic -= numPackets;
    if (unlikely(rxBuffersInNic == 0)) {
        RAMCLOUD_CLOG(WARNING, "Infiniband receiver temporarily ran "
                "out of packet buffers; could result in dropped packets");
    }

    // Give the RX queue a chance to replenish.
    refillReceiver();

    // Each iteration of the following loop processes one incoming packet.
    for (int i = 0; i < numPackets; i++) {
        fi_cq_tagged_entry* incoming = &wc[i];
        BufferDescriptor *bd = context_to_bd(incoming->op_context);
//        if (unlikely(incoming->status != IBV_WC_SUCCESS)) {
//            DIE("Infiniband receive error (%d: %s)", incoming->status,
//                    infiniband->wcStatusToString(incoming->status));
//        }

        bd->packetLength = incoming->len;
        bd->sourceAddress.construct(srcAddr[i]);
        PerfStats::threadStats.networkInputBytes += bd->packetLength;
        PerfStats::threadStats.networkInputPackets++;
        receivedPackets->emplace_back(bd->sourceAddress.get(), this,
                bd->packetLength, bd->buffer);
        continue;

      error:
        rxPool->freeBuffers.push_back(bd);
    }
    timeTrace("OfiUdDriver::receivePackets done");
}

/**
 * See docs in the ``Driver'' class.
 */
string
OfiUdDriver::getServiceLocator()
{
    return locatorString;
}

// See docs in Driver class.
uint32_t
OfiUdDriver::getBandwidth()
{
    return bandwidthGbps*1000;
}

/**
 * Fill up the HCA's queue of pending receive buffers.
 */
void
OfiUdDriver::refillReceiver()
{
    // Always refill in batch to amortize the cost of posting receive buffers.
    const uint32_t refillBatch = std::min(maxPostRxBuffers, 16u);
    uint32_t maxRefill = std::min(MAX_RX_QUEUE_DEPTH - rxBuffersInNic,
            downCast<uint32_t>(rxPool->freeBuffers.size()));
    if (maxRefill < refillBatch) {
        return;
    }

    // Create a list of receive buffers to be posted to the RX queue.
    // FIXME: use batch-oriented recvv instead!!!
    for (int i = 0; i < refillBatch; i++) {
        BufferDescriptor* bd = rxPool->freeBuffers.back();
        rxPool->freeBuffers.pop_back();
        void* desc = fi_mr_desc(bd->memoryRegion);
        FI_CHK_CALL(fi_recv, endpoint, bd->buffer, bd->length, desc,
                (fi_addr_t)0, &bd->context);
    }
    rxBuffersInNic += refillBatch;
    timeTrace("receive queue refilled");

    // Generate log messages every time buffer usage reaches a significant new
    // high. Running out of buffers is a bad thing, so we want warnings in the
    // log long before that happens.
    uint32_t freeBuffers = downCast<uint32_t>(rxPool->freeBuffers.size());
    if (unlikely(freeBuffers <= rxBufferLogThreshold)) {
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

void
OfiUdDriver::uncorkTransmitQueue()
{
}

/**
 * Constructor for BufferPool objects.
 * 
 * \param domain
 *      Libfabric resource domain the buffer pool belongs to.
 * \param attr
 *      Attributes of the domain.
 * \param bufferSize
 *      Size of each packet buffer, in bytes.
 * \param numBuffers
 *      Number of buffers to allocate in the pool.
 */
OfiUdDriver::BufferPool::BufferPool(fid_domain* domain, fi_domain_attr* attr,
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

    // FIXME: hack to skip memory registration for psm2 provider; psm2 provider
    // doesn't need registration because it can't do zero-copy anyway, right?
    // Steal from: https://github.com/yilongli/libfabric/blob/master/util/pingpong.c#L1324
    if (attr->mr_mode & FI_MR_LOCAL) {
        int ret = fi_mr_reg(domain, bufferMemory, bytesToAllocate,
                FI_SEND | FI_RECV, 0, 0, 0, &memoryRegion, NULL);
        if (ret) {
            DIE("Couldn't register memory region: %s", fi_strerror(-ret));
        }
    } else {
        memoryRegion = &NO_MEMORY_REGION;
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
OfiUdDriver::BufferPool::~BufferPool()
{
    if (memoryRegion != NULL) {
        fi_close(&memoryRegion->fid);
    }
    // `bufferMemory` and `descriptors` are allocated using malloc.
    free(bufferMemory);
    for (uint32_t i = 0; i < numBuffers; i++) {
        descriptors[i].~BufferDescriptor();
    }
    free(descriptors);
}

} // namespace RAMCloud
