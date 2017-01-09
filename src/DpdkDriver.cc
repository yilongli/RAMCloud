/* Copyright (c) 2015-2016 Stanford University
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
#include "StringUtil.h"
#include "TimeTrace.h"
#include "Util.h"

namespace RAMCloud
{

// Change 0 -> 1 in the following line to compile detailed time tracing in
// this transport.
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

/*
 * Construct a DpdkDriver.
 *
 * \param context
 *      Overall information about the RAMCloud server or client.
 * \param port
 *      Selects which physical port to use for communication.
 */

DpdkDriver::DpdkDriver(Context* context, int port)
    : context(context)
    , packetBufPool()
    , packetBufsUtilized(0)
    , locatorString()
    , localMac()
    , portId(0)
    , packetPool(NULL)
    , loopbackRing(NULL)
    , hasHardwareFilter(true) // Cleared if not applicable at NIC initialization
    , bandwidthMbps(10000)                // Default bandwidth = 10 gbs
    , queueEstimator(0)
    , maxTransmitQueueSize(0)
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
    const char *argv[] = {"rc", "--file-prefix", nameBuffer, "-c", "1",
            "-n", "1", NULL};
    int argc = static_cast<int>(sizeof(argv) / sizeof(argv[0])) - 1;

    rte_openlog_stream(fileLogger.getFile());
    ret = rte_eal_init(argc, const_cast<char**>(argv));
    if (ret < 0) {
        throw DriverException(HERE, "rte_eal_init failed");
    }

    // create an memory pool for accommodating packet buffers
    packetPool = rte_mempool_create("mbuf_pool", NB_MBUF,
                                    MBUF_SIZE, 32,
                                    sizeof32(struct rte_pktmbuf_pool_private),
                                    rte_pktmbuf_pool_init, NULL,
                                    rte_pktmbuf_init, NULL,
                                    rte_socket_id(), 0);

    if (!packetPool) {
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

    // Reading the MAC address from the NIC via DPDK.
    rte_eth_macaddr_get(portId, &mac);
    localMac.construct(mac.addr_bytes);
// TODO: replace the Driver interface getServiceLocator with getSLParams?
//    locatorString = format("basic+dpdk:mac=%s",
//            localMac->toString().c_str());
    locatorString = format("dpdk:mac=%s", localMac->toString().c_str());

    // configure some default NIC port parameters
    memset(&portConf, 0, sizeof(portConf));
    portConf.rxmode.max_rx_pkt_len = ETHER_MAX_VLAN_FRAME_LEN;
    rte_eth_dev_configure(portId, 1, 1, &portConf);

    // Set up a NIC/HW-based filter on the ethernet type so that only
    // traffic to a particular port is received by this driver.
    struct rte_eth_ethertype_filter filter;
    ret = rte_eth_dev_filter_supported(portId,
                                       RTE_ETH_FILTER_ETHERTYPE);
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
    rte_eth_rx_queue_setup(portId, 0, NDESC, 0, NULL, packetPool);
    rte_eth_tx_queue_setup(portId, 0, NDESC, 0, NULL);
    ret = rte_eth_dev_start(portId);
    if (ret != 0) {
        throw DriverException(HERE, format(
                "Couldn't start port %u, error %d (%s)", portId,
                ret, strerror(ret)));
    }

    // Retrieve the link speed and compute information based on it.
    struct rte_eth_link link;
    rte_eth_link_get_nowait(portId, &link);
    if (!link.link_status) {
        throw DriverException(HERE, format(
                "Failed to detect a link on Ethernet port %u", portId));
    }
    if (link.link_speed != ETH_SPEED_NUM_NONE) {
        bandwidthMbps = link.link_speed;
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

    // set the MTU that the NIC port should support
    // TODO: why set mtu?
    ret = rte_eth_dev_set_mtu(portId, ETHER_MTU);
    if (ret != 0) {
        throw DriverException(HERE, format(
                "Failed to set the MTU on Ethernet port  %u: %s",
                portId, rte_strerror(rte_errno)));
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
    Util::clearCpuAffinity();
}

/**
 * Destroy the DpdkDriver.
 */
DpdkDriver::~DpdkDriver()
{
    if (packetBufsUtilized != 0)
        LOG(ERROR, "DpdkDriver deleted with %d packets still in use",
            packetBufsUtilized);

    // Free the various allocated resources (e.g. ring, mempool) and release
    // the NIC.
    rte_eth_dev_stop(portId);
    rte_eth_dev_close(portId);
    char devname[RTE_ETH_NAME_MAX_LEN+1];
    if (rte_eth_dev_detach(portId, devname) != 0) {
        LOG(ERROR, "DpdkDriver failed to detach Ethernet device: "
                "portId = %u, devname = %s", portId, devname);
    }
    rte_openlog_stream(NULL);
}

// See docs in Driver class.
int
DpdkDriver::getHighestPacketPriority()
{
    // Assume we are allowed to use all 8 ethernet priorities specified
    // in the Priority Code Point (PCP) field.
    // TODO: IN PRACTICE, WE NEED TO BE CONFIGURE THE DPDK DRIVER TO ONLY USE A SUBSET
    return 7;
}

// See docs in Driver class.
uint32_t
DpdkDriver::getMaxPacketSize()
{
    return MAX_PAYLOAD_SIZE;
}

// See docs in Driver class.
int
DpdkDriver::getTransmitQueueSpace(uint64_t currentTime)
{
    return maxTransmitQueueSize - queueEstimator.getQueueSize(currentTime);
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

    // attempt to dequeue a batch of received packets from the NIC
    // as well as from the loopback ring.
    uint32_t incomingPkts = rte_eth_rx_burst(portId, 0, mPkts,
            downCast<uint16_t>(maxPackets/2));
    if (incomingPkts > 0) {
        timeTrace("DpdkDriver received %u packets", incomingPkts);
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

    // Process received packets by constructing appropriate Received
    // objects and copying the payload from the DPDK packet buffers.
    for (uint32_t i = 0; i < totalPkts; i++) {
        struct rte_mbuf* m = mPkts[i];
        rte_prefetch0(rte_pktmbuf_mtod(m, void *));
        if (m->nb_segs > 1) {
            RAMCLOUD_CLOG(WARNING,
                    "Can't handle packet with %u segments; discarding",
                    m->nb_segs);
            rte_pktmbuf_free(m);
            continue;
        }

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
        if (!hasHardwareFilter) {
            // Perform packet filtering by software to skip irrelevant
            // packets such as ipmi or kernel TCP/IP traffics.
            if (ether_type !=
                    rte_cpu_to_be_16(NetUtil::EthPayloadType::RAMCLOUD)) {
                rte_pktmbuf_free(m);
                continue;
            }
        }

        PacketBuf* buffer = packetBufPool.construct();
        packetBufsUtilized++;
        buffer->sender.construct(ethHdr->s_addr.addr_bytes);
        uint32_t length = rte_pktmbuf_pkt_len(m) - headerLength;
        assert(length <= MAX_PAYLOAD_SIZE);
        rte_memcpy(buffer->payload, payload, length);
        receivedPackets->emplace_back(buffer->sender.get(), this,
                length, buffer->payload);
        rte_pktmbuf_free(m);
    }
}

// See docs in Driver class.
void
DpdkDriver::release(char *payload)
{
    // Must sync with the dispatch thread, since this method could potentially
    // be invoked in a worker.
    Dispatch::Lock _(context->dispatch);

    // Note: the payload is actually contained in a PacketBuf structure,
    // which we return to a pool for reuse later.
    packetBufsUtilized--;
    assert(packetBufsUtilized >= 0);
    packetBufPool.destroy(
        reinterpret_cast<PacketBuf*>(payload - OFFSET_OF(PacketBuf, payload)));
}

// See docs in Driver class.
void
DpdkDriver::sendPacket(const Address* addr,
                       const void* header,
                       uint32_t headerLen,
                       Buffer::Iterator* payload,
                       int priority)
{
    struct rte_mbuf *mbuf = NULL;
    char *data = NULL;

    uint32_t totalLength = headerLen +
            (payload ? payload->size() : 0);
    uint32_t frameLength = totalLength + ETHER_HDR_LEN + VLAN_TAG_LEN;

    assert(totalLength <= MAX_PAYLOAD_SIZE);

    mbuf = rte_pktmbuf_alloc(packetPool);
    if (NULL == mbuf) {
        RAMCLOUD_CLOG(NOTICE,
                "Failed to allocate a packet buffer; dropping packet");
        return;
    }

    data = rte_pktmbuf_append(mbuf, downCast<uint16_t>(frameLength));

    char *p = data;

    if (NULL == data) {
        RAMCLOUD_CLOG(NOTICE,
                "rte_pktmbuf_append call failed; dropping packet");
        rte_pktmbuf_free(mbuf);
        return;
    }

    struct ether_hdr* ethHdr = reinterpret_cast<struct ether_hdr*>(p);
    rte_memcpy(&ethHdr->d_addr, static_cast<const MacAddress*>(addr)->address,
            ETHER_ADDR_LEN);
    rte_memcpy(&ethHdr->s_addr, localMac->address, ETHER_ADDR_LEN);
    ethHdr->ether_type = rte_cpu_to_be_16(ETHER_TYPE_VLAN);
    p += ETHER_HDR_LEN;

    struct vlan_hdr* vlanHdr = reinterpret_cast<struct vlan_hdr*>(p);
    uint16_t pcp;
    switch (priority) {
        case 0 :
            // PCP = 1 is the lowest priority: Background (BK)
            pcp = 1; break;
        case 1 :
            // PCP = 0 is the 2nd lowest priority: Best Effort (BE)
            pcp = 0; break;
        default:
            pcp = downCast<uint16_t>(priority);
    }
    uint16_t vlan_tci = downCast<uint16_t>(pcp << 13);
    vlanHdr->vlan_tci = rte_cpu_to_be_16(vlan_tci);
    vlanHdr->eth_proto = rte_cpu_to_be_16(NetUtil::EthPayloadType::RAMCLOUD);
    p += VLAN_TAG_LEN;

    rte_memcpy(p, header, headerLen);
    p += headerLen;
    while (payload && !payload->isDone())
    {
        rte_memcpy(p, payload->getData(), payload->getLength());
        p += payload->getLength();
        payload->next();
    }
    timeTrace("about to enqueue outgoing packet");

    // loopback if src mac == dst mac
    if (!memcmp(static_cast<const MacAddress*>(addr)->address,
            localMac->address, 6)) {
        rte_ring_enqueue(loopbackRing, mbuf);
    } else {
        uint32_t ret = rte_eth_tx_burst(portId, 0, &mbuf, 1);
        if (ret != 1) {
            LOG(WARNING, "rte_eth_tx_burst returned %u; packet may be lost?",
                    ret);
        }
    }
    timeTrace("outgoing packet enqueued");
    queueEstimator.packetQueued(totalLength, Cycles::rdtsc());
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
