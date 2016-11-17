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
#include "ServiceLocator.h"
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

    // create an memory pool for accommodating packet buffers
    packetPool = rte_mempool_create("mbuf_pool", NB_MBUF,
                                      MBUF_SIZE, 32,
              static_cast<uint32_t>(sizeof(struct rte_pktmbuf_pool_private)),
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

    // if the locator string specified an all-zeroes MAC address,
    // use the default one by reading it from the NIC via DPDK.
    // Fix-up the locator string to reflect the real MAC address.
    rte_eth_macaddr_get(portId, &mac);
    localMac.construct(mac.addr_bytes);
    locatorString = format("basic+dpdk:mac=%s",
            localMac->toString().c_str());

    // configure some default NIC port parameters
    memset(&portConf, 0, sizeof(portConf));
    portConf.rxmode.max_rx_pkt_len = MAX_PAYLOAD_SIZE +
            static_cast<uint32_t>(sizeof(NetUtil::EthernetHeader));
    rte_eth_dev_configure(portId, 1, 1, &portConf);

    // Set up a NIC/HW-based filter on the ethernet type so that only
    // only traffic to a particular port is received by this driver.
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
    ret = rte_eth_dev_set_mtu(portId, static_cast<uint16_t>(MAX_PAYLOAD_SIZE +
            static_cast<uint32_t>(sizeof(NetUtil::EthernetHeader))));
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

    // Currently DPDK does not provide methods for freeing the various
    // allocated resources (e.g. ring, mempool) and releasing the NIC.
    // All we can do at this point is stop the packet reception
    // by disabling the activated NIC port.
    rte_eth_dev_stop(portId);
    rte_openlog_stream(NULL);
}

// See docs in Driver class.

uint32_t
DpdkDriver::getMaxPacketSize()
{
    return MAX_PAYLOAD_SIZE
            - static_cast<uint32_t>(sizeof(NetUtil::EthernetHeader));
}

// See docs in Driver class.
int
DpdkDriver::getTransmitQueueSpace(uint64_t currentTime)
{
    return maxTransmitQueueSize - queueEstimator.getQueueSize(currentTime);
}

// See docs in Driver class.
void
DpdkDriver::receivePackets(int maxPackets,
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
            MAX_PACKETS_AT_ONCE/2);
    if (incomingPkts > 0) {
        timeTrace("DpdkDriver received %u packets", incomingPkts);
    }
    uint32_t loopbackPkts = rte_ring_count(loopbackRing);
    if (incomingPkts + loopbackPkts > MAX_PACKETS_AT_ONCE) {
        loopbackPkts = MAX_PACKETS_AT_ONCE - incomingPkts;
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
        PacketBuf* buffer = packetBufPool.construct();

        if (!hasHardwareFilter) {
            // Perform packet filtering by software to skip irrelevant
            // packets such as ipmi or kernel TCP/IP traffics.
            char *data = rte_pktmbuf_mtod(m, char *);
            auto& eth_header = *reinterpret_cast<EthernetHeader*>(data);
            if (eth_header.etherType !=
                    HTONS(NetUtil::EthPayloadType::RAMCLOUD)) {
                packetBufPool.destroy(buffer);
                rte_pktmbuf_free(m);
                continue;
            }
        }
        packetBufsUtilized++;
        buffer->sender.construct(
                rte_pktmbuf_mtod(m, uint8_t *) + sizeof(struct ether_addr));
        uint32_t length = rte_pktmbuf_pkt_len(m) -
                sizeof32(NetUtil::EthernetHeader);
        rte_memcpy(buffer->payload,
                static_cast<char*>(rte_pktmbuf_mtod(m, void *))
                + sizeof(NetUtil::EthernetHeader), length);
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
DpdkDriver::sendPacket(const Address *addr,
                       const void *header,
                       uint32_t headerLen,
                       Buffer::Iterator *payload,
                       uint8_t priority)
{
    struct rte_mbuf *mbuf = NULL;
    char *data = NULL;

    uint32_t totalLength = headerLen +
            (payload ? payload->size() : 0);
    uint32_t datagramLength = totalLength + sizeof32(NetUtil::EthernetHeader);

    assert(totalLength <= MAX_PAYLOAD_SIZE);

    mbuf = rte_pktmbuf_alloc(packetPool);

    if (NULL == mbuf) {
        RAMCLOUD_CLOG(NOTICE,
                "Failed to allocate a packet buffer; dropping packet");
        return;
    }

    data = rte_pktmbuf_append(mbuf, downCast<uint16_t>(datagramLength));

    char *p = data;

    if (NULL == data) {
        RAMCLOUD_CLOG(NOTICE,
                "rte_pktmbuf_append call failed; dropping packet");
        rte_pktmbuf_free(mbuf);
        return;
    }

    NetUtil::EthernetHeader* ethHdr = reinterpret_cast<
            NetUtil::EthernetHeader*>(p);

    rte_memcpy(&ethHdr->destAddress,
            static_cast<const MacAddress*>(addr)->address, 6);
    rte_memcpy(&ethHdr->srcAddress, localMac->address, 6);
    ethHdr->etherType = HTONS(NetUtil::EthPayloadType::RAMCLOUD);
    p += sizeof(*ethHdr);
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
int
DpdkDriver::getBandwidth()
{
    return bandwidthMbps;
}

} // namespace RAMCloud
