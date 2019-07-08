/* Copyright (c) 2015-2017 Stanford University
 * Copyright (c) 2014-2015 Huawei Technologies Co. Ltd.
 * Copyright (c) 2014-2016 NEC Corporation
 * The original version of this module was contributed by Anthony Iliopoulos
 * at DBERC, Huawei
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

#define __STDC_LIMIT_MACROS
#include <errno.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>

#pragma GCC diagnostic ignored "-Wconversion"
#include <rte_config.h>
#include <rte_common.h>
#include <rte_errno.h>
#include <rte_ethdev.h>
#include <rte_mbuf.h>
#include <rte_memcpy.h>
#include <rte_ring.h>
#include <rte_version.h>
#pragma GCC diagnostic warning "-Wconversion"

#include "Common.h"
#include "Cycles.h"
#include "ShortMacros.h"
#include "DpdkDriver.h"
#include "NetUtil.h"
#include "PerfStats.h"
#include "StringUtil.h"
#include "TimeTrace.h"
#include "Util.h"

namespace RAMCloud
{

// Change 0 -> 1 in the following line to compile detailed time tracing in
// this driver.
#define TIME_TRACE 0

// Provides a cleaner way of invoking TimeTrace::record, with the code
// conditionally compiled in or out by the TIME_TRACE #ifdef.
namespace {
    inline void
    timeTrace(const char* format,
            uint32_t arg0 = 0, uint32_t arg1 = 0, uint32_t arg2 = 0,
            uint32_t arg3 = 0)
    {
#if TIME_TRACE
        TimeTrace::record(format, arg0, arg1, arg2, arg3);
#endif
    }
}

// Short-hand to obtain the starting address of a DPDK rte_mbuf based on its
// payload address.
#define payload_to_mbuf(payload) reinterpret_cast<struct rte_mbuf*>( \
    payload - ETHER_HDR_LEN - RTE_PKTMBUF_HEADROOM - sizeof(struct rte_mbuf))

constexpr uint16_t DpdkDriver::PRIORITY_TO_PCP[8];

#if TESTING
/*
 * Construct a mock DpdkDriver, used for testing only.
 */
DpdkDriver::DpdkDriver()
    : Driver(NULL)
    , packetBufsUtilized(0)
    , locatorString()
    , localMac()
    , portId(0)
    , mbufPool(NULL)
    , loopbackRing(NULL)
    , hasHardwareFilter(true)
    , bandwidthMbps(10000)
    , fileLogger(NOTICE, "DPDK: ")
{
    localMac.construct("01:23:45:67:89:ab");
    queueEstimator.setBandwidth(bandwidthMbps);
    maxTransmitQueueSize = (uint32_t) (static_cast<double>(bandwidthMbps)
            * MAX_DRAIN_TIME / 8000.0);
    uint32_t maxPacketSize = getMaxPacketSize();
    if (maxTransmitQueueSize < 2*maxPacketSize) {
        // Make sure that we advertise enough space in the transmit queue to
        // prepare the next packet while the current one is transmitting.
        maxTransmitQueueSize = 2*maxPacketSize;
    }
}
#endif

/*
 * Construct a DpdkDriver.
 *
 * \param context
 *      Overall information about the RAMCloud server or client.
 * \param port
 *      Selects which physical port to use for communication.
 */

DpdkDriver::DpdkDriver(Context* context, int port)
    : Driver(context)
    , packetBufsUtilized(0)
    , locatorString()
    , localMac()
    , portId(0)
    , mbufPool(NULL)
    , loopbackRing(NULL)
    , hasHardwareFilter(true)             // Cleared later if not applicable
    , bandwidthMbps(10000)                // Default bandwidth = 10 gbs
    , fileLogger(NOTICE, "DPDK: ")
{
    struct ether_addr mac;
    uint8_t numPorts;
    struct rte_eth_conf portConf;
    int ret;

    portId = downCast<uint8_t>(port);

    // Initialize the DPDK environment with some default parameters.
    // --file-prefix is needed to avoid false lock conflicts if servers
    // run on different nodes, but with a shared NFS home directory.
    // This is a bug in DPDK as of 9/2016; if the bug gets fixed, then
    // the --file-prefix argument can be removed.
    LOG(NOTICE, "Using DPDK version %s", rte_version());
    char nameBuffer[1000];
    if (gethostname(nameBuffer, sizeof(nameBuffer)) != 0) {
        throw DriverException(HERE, format("gethostname failed: %s",
                strerror(errno)));
    }
    nameBuffer[sizeof(nameBuffer)-1] = 0;   // Needed if name was too long.
    // Compute the list of cores the current thread pinned to.
    string coreList;
    int lcore = 0;
    for (char c : Util::getCpuAffinityString()) {
        if (c == 'X') {
            coreList.append(std::to_string(lcore));
            coreList.append(",");
        }
        lcore++;
    }
    coreList.pop_back();
    const char *argv[] = {"rc", "--file-prefix", nameBuffer,
            "-l", coreList.c_str(), "-n", "4", NULL};
    int argc = static_cast<int>(sizeof(argv) / sizeof(argv[0])) - 1;

    rte_openlog_stream(fileLogger.getFile());
    ret = rte_eal_init(argc, const_cast<char**>(argv));
    if (ret < 0) {
        throw DriverException(HERE, "rte_eal_init failed");
    }

    // create an memory pool for accommodating packet buffers
    mbufPool = rte_mempool_create("mbuf_pool", NB_MBUF,
            MBUF_SIZE, 32,
            sizeof32(struct rte_pktmbuf_pool_private),
            rte_pktmbuf_pool_init, NULL,
            rte_pktmbuf_init, NULL,
            rte_socket_id(), 0);

    if (!mbufPool) {
        throw DriverException(HERE, format(
                "Failed to allocate memory for packet buffers: %s",
                rte_strerror(rte_errno)));
    }

    // ensure that DPDK was able to detect a compatible and available NIC
    numPorts = rte_eth_dev_count();

    if (numPorts <= portId) {
        throw DriverException(HERE, format(
                "Ethernet port %u doesn't exist (%u ports available)",
                portId, numPorts));
    }

    // Read the MAC address from the NIC via DPDK.
    rte_eth_macaddr_get(portId, &mac);
    localMac.construct(mac.addr_bytes);
    locatorString = format("dpdk:mac=%s", localMac->toString().c_str());

    // configure some default NIC port parameters
    memset(&portConf, 0, sizeof(portConf));
//    portConf.rxmode.max_rx_pkt_len = ETHER_MAX_VLAN_FRAME_LEN;
    // FIXME: why doesn't vlan strip offload work on xl170?
    // https://stackoverflow.com/q/52330474/11495205
    portConf.rxmode.offloads = DEV_RX_OFFLOAD_VLAN_STRIP;
    ret = rte_eth_dev_configure(portId, 1, 1, &portConf);
    if (ret < 0) {
        throw DriverException(HERE, format(
                "Failed to configure port %d", portId));
    }

    // Set up a NIC/HW-based filter on the ethernet type so that only
    // traffic to a particular port is received by this driver.
    struct rte_eth_ethertype_filter filter;
    ret = rte_eth_dev_filter_supported(portId, RTE_ETH_FILTER_ETHERTYPE);
    if (ret < 0) {
        LOG(NOTICE, "ethertype filter is not supported on port %u.", portId);
        hasHardwareFilter = false;
    } else {
        memset(&filter, 0, sizeof(filter));
        ret = rte_eth_dev_filter_ctrl(portId, RTE_ETH_FILTER_ETHERTYPE,
                RTE_ETH_FILTER_ADD, &filter);
        if (ret < 0) {
            LOG(WARNING, "failed to add ethertype filter\n");
            hasHardwareFilter = false;
          }
    }

    // setup and initialize the receive and transmit NIC queues,
    // and activate the port.
    rte_eth_rx_queue_setup(portId, 0, NDESC, 0, NULL, mbufPool);
    rte_eth_tx_queue_setup(portId, 0, NDESC, 0, NULL);

    // set the MTU that the NIC port should support
    ret = rte_eth_dev_set_mtu(portId, MAX_PAYLOAD_SIZE);
    if (ret != 0) {
        throw DriverException(HERE, format(
                "Failed to set the MTU on Ethernet port  %u: %s",
                portId, rte_strerror(rte_errno)));
    }

    ret = rte_eth_dev_start(portId);
    if (ret != 0) {
        throw DriverException(HERE, format(
                "Couldn't start port %u, error %d (%s)", portId,
                ret, strerror(ret)));
    }

    // Retrieve the link speed and compute information based on it.
    struct rte_eth_link link;
    rte_eth_link_get(portId, &link);
    if (!link.link_status) {
        throw DriverException(HERE, format(
                "Failed to detect a link on Ethernet port %u", portId));
    }
    if (link.link_speed != ETH_SPEED_NUM_NONE) {
        // Be conservative about the link speed. We use bandwidth in
        // QueueEstimator to estimate # bytes outstanding in the NIC's
        // TX queue. If we overestimate the bandwidth, under high load,
        // we may keep queueing packets faster than the NIC can consume,
        // and build up a queue in the TX queue.
        bandwidthMbps = (uint32_t) (link.link_speed * 0.98);
    } else {
        LOG(WARNING, "Can't retrieve network bandwidth from DPDK; "
                "using default of %d Mbps", bandwidthMbps);
    }
    queueEstimator.setBandwidth(bandwidthMbps);
    maxTransmitQueueSize = (uint32_t) (static_cast<double>(bandwidthMbps)
            * MAX_DRAIN_TIME / 8000.0);
    uint32_t maxPacketSize = getMaxPacketSize();
    if (maxTransmitQueueSize < 2*maxPacketSize) {
        // Make sure that we advertise enough space in the transmit queue to
        // prepare the next packet while the current one is transmitting.
        maxTransmitQueueSize = 2*maxPacketSize;
    }

    // create an in-memory ring, used as a software loopback in order to handle
    // packets that are addressed to the localhost.
    loopbackRing = rte_ring_create("dpdk_loopback_ring", 4096,
            SOCKET_ID_ANY, 0);
    if (NULL == loopbackRing) {
        throw DriverException(HERE, format(
                "Failed to allocate loopback ring: %s",
                rte_strerror(rte_errno)));
    }

    LOG(NOTICE, "DpdkDriver locator: %s, bandwidth: %d Mbits/sec, "
            "maxTransmitQueueSize: %u bytes",
            locatorString.c_str(), bandwidthMbps, maxTransmitQueueSize);

    // DPDK during initialization (rte_eal_init()) pins the running thread
    // to a single processor. This becomes a problem as the master worker
    // threads are created after the initialization of the transport, and
    // thus inherit the (very) restricted affinity to a single core. This
    // essentially kills performance, as every thread is contenting for a
    // single core. Revert this, by restoring the affinity to the default
    // (all cores).
    // FIXME: we are using Arachne! can't clear affinity; no need to worry
    // master worker threads affinity anyway; they are arachnified
//    Util::clearCpuAffinity();
}

/**
 * Destroy the DpdkDriver.
 */
DpdkDriver::~DpdkDriver()
{
    if (packetBufsUtilized != 0)
        LOG(ERROR, "DpdkDriver deleted with %d packets still in use",
            packetBufsUtilized);

    // Free the various allocated resources (e.g. ring, mempool) and close
    // the NIC.
    rte_eth_dev_stop(portId);
    rte_eth_dev_close(portId);
    rte_openlog_stream(NULL);
}

// See docs in Driver class.
int
DpdkDriver::getHighestPacketPriority()
{
    return arrayLength(PRIORITY_TO_PCP) - 1;
}

// See docs in Driver class.
uint32_t
DpdkDriver::getMaxPacketSize()
{
    return MAX_PAYLOAD_SIZE;
}

// See docs in Driver class.
uint32_t
DpdkDriver::getPacketOverhead()
{
    return ETHER_VLAN_HDR_LEN + ETHER_PACKET_OVERHEAD;
}

// See docs in Driver class.
void
DpdkDriver::receivePackets(uint32_t maxPackets,
            std::vector<Received>* receivedPackets)
{
#define MAX_PACKETS_AT_ONCE 32
    if (maxPackets > MAX_PACKETS_AT_ONCE) {
        maxPackets = MAX_PACKETS_AT_ONCE;
    }
    struct rte_mbuf* mPkts[MAX_PACKETS_AT_ONCE];

#if TIME_TRACE
    uint64_t timestamp = Cycles::rdtsc();
#endif
    // attempt to dequeue a batch of received packets from the NIC
    // as well as from the loopback ring.
    uint32_t incomingPkts = rte_eth_rx_burst(portId, 0, mPkts,
            downCast<uint16_t>(maxPackets));
    if (incomingPkts > 0) {
        lastReceiveTime = Cycles::rdtsc();
#if TIME_TRACE
        TimeTrace::record(timestamp, "DpdkDriver about to receive packets");
        TimeTrace::record("DpdkDriver received %u packets", incomingPkts);
#endif
    }
    uint32_t loopbackPkts = rte_ring_count(loopbackRing);
    if (incomingPkts + loopbackPkts > maxPackets) {
        loopbackPkts = maxPackets - incomingPkts;
    }
    for (uint32_t i = 0; i < loopbackPkts; i++) {
        rte_ring_dequeue(loopbackRing,
                reinterpret_cast<void**>(&mPkts[incomingPkts + i]));
    }
    uint32_t totalPkts = incomingPkts + loopbackPkts;

    // Process received packets by constructing appropriate Received objects.
    for (uint32_t i = 0; i < totalPkts; i++) {
        struct rte_mbuf* m = mPkts[i];
        struct ether_hdr* ethHdr = rte_pktmbuf_mtod(m, struct ether_hdr*);
        uint16_t ether_type = ethHdr->ether_type;
        uint32_t headerLength = ETHER_HDR_LEN;
        char* payload = reinterpret_cast<char *>(ethHdr + 1);
        if (ether_type == rte_cpu_to_be_16(ETHER_TYPE_VLAN)) {
            struct vlan_hdr* vlanHdr =
                    reinterpret_cast<struct vlan_hdr*>(payload);
            ether_type = vlanHdr->eth_proto;
            headerLength += VLAN_TAG_LEN;
            payload += VLAN_TAG_LEN;
        }
        assert(ether_type == rte_cpu_to_be_16(
                NetUtil::EthPayloadType::RAMCLOUD));

        // By default, we would like to construct the Received object using
        // the payload directly (as opposed to copying out the payload to a
        // PacketBuf first). Therefore, we use the headroom in rte_mbuf to
        // store the packet sender address (required by the Received object)
        // and the packet buf type (so we know where this payload comes from).
        // See http://dpdk.org/doc/guides/prog_guide/mbuf_lib.html for the
        // diagram of rte_mbuf's internal structure.
        assert(rte_pktmbuf_headroom(m) > sizeof(MacAddress));
        MacAddress* sender = reinterpret_cast<MacAddress*>(m->buf_addr);
        new(sender) MacAddress(ethHdr->s_addr.addr_bytes);
        uint32_t length = rte_pktmbuf_pkt_len(m) - headerLength;
        receivedPackets->emplace_back(sender, this, length, payload);
        PerfStats::threadStats.networkInputBytes += rte_pktmbuf_pkt_len(m);
        packetBufsUtilized++;
        timeTrace("received packet processed, payload size %u", length);
    }
}

// See docs in Driver class.
void
DpdkDriver::release()
{
    packetBufsUtilized -= static_cast<int>(packetsToRelease.size());
    assert(packetBufsUtilized >= 0);
    while (!packetsToRelease.empty()) {
        rte_pktmbuf_free(payload_to_mbuf(packetsToRelease.back()));
        packetsToRelease.pop_back();
    }
}

// See docs in Driver class.
void
DpdkDriver::sendPacket(const Address* addr,
                       const void* header,
                       uint32_t headerLen,
                       Buffer::Iterator* payload,
                       int priority,
                       TransmitQueueState* txQueueState)
{
    // Convert transport-level packet priority to Ethernet priority.
    assert(priority >= 0 && priority <= getHighestPacketPriority());

    uint32_t etherPayloadLength = headerLen + (payload ? payload->size() : 0);
    assert(etherPayloadLength <= MAX_PAYLOAD_SIZE);
    timeTrace("sendPacket invoked, payload size %u", etherPayloadLength);
    uint32_t frameLength = etherPayloadLength + ETHER_VLAN_HDR_LEN;
    uint32_t physPacketLength = frameLength + ETHER_PACKET_OVERHEAD;

#if TESTING
    struct rte_mbuf mockMbuf;
    struct rte_mbuf* mbuf = &mockMbuf;
#else
    struct rte_mbuf* mbuf = rte_pktmbuf_alloc(mbufPool);
#endif
    if (unlikely(NULL == mbuf)) {
        DIE("Failed to allocate a packet buffer; dropping packet; "
                "%u mbufs available, %u mbufs in use",
                rte_mempool_avail_count(mbufPool),
                rte_mempool_in_use_count(mbufPool));
    }

#if TESTING
    uint8_t mockEtherFrame[ETHER_MAX_VLAN_FRAME_LEN] = {};
    char* data = reinterpret_cast<char*>(mockEtherFrame);
#else
    char* data = rte_pktmbuf_append(mbuf, downCast<uint16_t>(frameLength));
#endif

    // Fill out the destination and source MAC addresses plus the Ethernet
    // frame type (i.e., IEEE 802.1Q VLAN tagging).
    char *p = data;
    struct ether_hdr* ethHdr = reinterpret_cast<struct ether_hdr*>(p);
    rte_memcpy(&ethHdr->d_addr, static_cast<const MacAddress*>(addr)->address,
            ETHER_ADDR_LEN);
    rte_memcpy(&ethHdr->s_addr, localMac->address, ETHER_ADDR_LEN);
    ethHdr->ether_type = rte_cpu_to_be_16(ETHER_TYPE_VLAN);
    p += ETHER_HDR_LEN;

    // Fill out the PCP field and the Ethernet frame type of the encapsulated
    // frame (DEI and VLAN ID are not relevant and trivially set to 0).
    struct vlan_hdr* vlanHdr = reinterpret_cast<struct vlan_hdr*>(p);
    vlanHdr->vlan_tci = rte_cpu_to_be_16(PRIORITY_TO_PCP[priority]);
    vlanHdr->eth_proto = rte_cpu_to_be_16(NetUtil::EthPayloadType::RAMCLOUD);
    p += VLAN_TAG_LEN;

    // Copy `header` and `payload`.
    rte_memcpy(p, header, headerLen);
    p += headerLen;
    while (payload && !payload->isDone())
    {
        rte_memcpy(p, payload->getData(), payload->getLength());
        p += payload->getLength();
        payload->next();
    }
    timeTrace("about to enqueue outgoing packet");

#if TESTING
    string hexEtherHeader;
    for (unsigned i = 0; i < ETHER_VLAN_HDR_LEN; i++) {
        char hex[3];
        snprintf(hex, sizeof(hex), "%02x", mockEtherFrame[i]);
        hexEtherHeader += hex;
    }
    LOG(NOTICE, "Ethernet frame header %s, payload %s", hexEtherHeader.c_str(),
            &mockEtherFrame[ETHER_VLAN_HDR_LEN]);
#else
    // TODO: is it worth the effort of memcmp for this corner case? I don't think so.
    // Even if we want loop back; it should be offloaded to hw
    // FIXME: wtf? if I remove the following code and send loopback packets
    // normally; they got dropped somewhere?
    // loopback if src mac == dst mac
    if (!memcmp(static_cast<const MacAddress*>(addr)->address,
            localMac->address, 6)) {
        int ret = rte_ring_enqueue(loopbackRing, mbuf);
        if (unlikely(ret != 0)) {
            LOG(WARNING, "rte_ring_enqueue returned %d; packet may be lost?",
                    ret);
            rte_pktmbuf_free(mbuf);
        }
        timeTrace("loopback packet enqueued");
        return;
    }

    lastTransmitTime = Cycles::rdtsc();
    uint32_t ret = rte_eth_tx_burst(portId, 0, &mbuf, 1);
    if (unlikely(ret != 1)) {
        LOG(WARNING, "rte_eth_tx_burst returned %u; packet may be lost?", ret);
        // The congestion at the TX queue must be pretty bad if we got here:
        // set the queue size to be relatively large.
        queueEstimator.setQueueSize(maxTransmitQueueSize*2, Cycles::rdtsc());
        rte_pktmbuf_free(mbuf);
        return;
    }
    timeTrace("outgoing packet enqueued");
#endif
    queueEstimator.packetQueued(physPacketLength, lastTransmitTime,
            txQueueState);
    PerfStats::threadStats.networkOutputBytes += physPacketLength;
}

// See docs in Driver class.
void
DpdkDriver::sendPackets(const Address* addr,
                        const void* headers,
                        uint32_t headerLen,
                        Buffer::Iterator* messageIt,
                        int priority,
                        TransmitQueueState* txQueueState)
{
    assert(priority >= 0 && priority <= getHighestPacketPriority());
    const uint32_t messageBytes = messageIt->size();
    timeTrace("sendPackets invoked, message bytes %u", messageBytes);

#define MAX_TX_PACKETS 16
    rte_mbuf* mbufs[MAX_TX_PACKETS];
    uint32_t maxPayload = getMaxPacketSize() - headerLen;
    uint32_t numPackets = (messageIt->size() + maxPayload - 1) / maxPayload;
    assert(numPackets <= MAX_TX_PACKETS);

    // Each iteration of the following loop fills out the header and payload
    // of one mbuf.
    const char* header = reinterpret_cast<const char*>(headers);
    for (uint32_t i = 0; i < numPackets; i++) {
        mbufs[i] = rte_pktmbuf_alloc(mbufPool);
        if (unlikely(NULL == mbufs[i])) {
            DIE("Failed to allocate a packet buffer; dropping packet; "
                    "%u mbufs available, %u mbufs in use",
                    rte_mempool_avail_count(mbufPool),
                    rte_mempool_in_use_count(mbufPool));
        }

        uint32_t payloadSize = std::min(maxPayload, messageIt->size());
        char* dst = rte_pktmbuf_append(mbufs[i], downCast<uint16_t>(
                ETHER_VLAN_HDR_LEN + headerLen + payloadSize));

        // Fill out the destination and source MAC addresses plus the Ethernet
        // frame type (i.e., IEEE 802.1Q VLAN tagging).
        struct ether_hdr* ethHdr = reinterpret_cast<struct ether_hdr*>(dst);
        rte_memcpy(&ethHdr->d_addr, static_cast<const MacAddress*>(addr)->address,
                ETHER_ADDR_LEN);
        rte_memcpy(&ethHdr->s_addr, localMac->address, ETHER_ADDR_LEN);
        ethHdr->ether_type = rte_cpu_to_be_16(ETHER_TYPE_VLAN);
        dst += ETHER_HDR_LEN;

        // Fill out the PCP field and the Ethernet frame type of the encapsulated
        // frame (DEI and VLAN ID are not relevant and trivially set to 0).
        struct vlan_hdr* vlanHdr = reinterpret_cast<struct vlan_hdr*>(dst);
        vlanHdr->vlan_tci = rte_cpu_to_be_16(PRIORITY_TO_PCP[priority]);
        vlanHdr->eth_proto = rte_cpu_to_be_16(NetUtil::EthPayloadType::RAMCLOUD);
        dst += VLAN_TAG_LEN;

        // Copy RAMCloud transport protocol header into mbuf.
        rte_memcpy(dst, header, headerLen);
        header += headerLen;
        dst += headerLen;
        uint32_t bytesToCopy = payloadSize;

        // Copy payload into mbuf.
        while (messageIt->size() > 0) {
            // The current buffer chunk contains the rest of the packet.
            if (messageIt->getLength() >= bytesToCopy) {
                rte_memcpy(dst, messageIt->getData(), bytesToCopy);
                messageIt->advance(bytesToCopy);
                break;
            }

            rte_memcpy(dst, messageIt->getData(), messageIt->getLength());
            dst += messageIt->getLength();
            bytesToCopy -= messageIt->getLength();
            messageIt->next();
        }
    }

    // FIXME: figure out why we have to handle loopback here in SW.
    if (!memcmp(static_cast<const MacAddress*>(addr)->address,
            localMac->address, 6)) {
        for (int i = 0; i < numPackets; i++) {
            rte_ring_enqueue(loopbackRing, mbufs[i]);
        }
        timeTrace("loopback packets enqueued");
        return;
    }

    lastTransmitTime = Cycles::rdtsc();
    uint32_t ret = rte_eth_tx_burst(portId, 0, mbufs, numPackets);
    if (unlikely(ret < numPackets)) {
        LOG(ERROR, "rte_eth_tx_burst returned %u; packet may be lost?", ret);
        for (uint32_t i = ret; i < numPackets; i++) {
            rte_pktmbuf_free(mbufs[i]);
        }
    }
    timeTrace("outgoing packets enqueued");

    uint32_t rawBytes = messageBytes + numPackets *
            (ETHER_PACKET_OVERHEAD + ETHER_VLAN_HDR_LEN + headerLen);
    queueEstimator.packetQueued(rawBytes, lastTransmitTime, txQueueState);
    PerfStats::threadStats.networkOutputBytes += rawBytes;
}

// See docs in Driver class.
string
DpdkDriver::getServiceLocator()
{
    return locatorString;
}

// See docs in Driver class.
uint32_t
DpdkDriver::getBandwidth()
{
    return bandwidthMbps;
}

} // namespace RAMCloud
