/* Copyright (c) 2015-2016 Stanford University
 * Copyright (c) 2014-2015 Huawei Technologies Co. Ltd.
 * Copyright (c) 2014-2016 NEC Corporation
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#ifndef RAMCLOUD_DPDKDRIVER_H
#define RAMCLOUD_DPDKDRIVER_H

#include <vector>

#include "Dispatch.h"
#include "Driver.h"
#include "FileLogger.h"
#include "MacAddress.h"
#include "NetUtil.h"
#include "ObjectPool.h"
#include "QueueEstimator.h"
#include "ServiceLocator.h"
#include "Tub.h"

// Number of descriptors to allocate for the tx/rx rings
#define NDESC 256
// Maximum number of packet buffers that the memory pool can hold
#define NB_MBUF 8192
// per-element size for the packet buffer memory pool
#define MBUF_SIZE (2048 + static_cast<uint32_t>(sizeof(struct rte_mbuf)) \
                   + RTE_PKTMBUF_HEADROOM)

// Forward declarations, so we don't have to include DPDK headers here.
struct rte_mempool;
struct rte_ring;

namespace RAMCloud
{

/**
 * A Driver for  DPDK communication.  Simple packet send/receive
 * style interface. See Driver.h for more detail.
 */

class DpdkDriver : public Driver
{
  public:
    static const uint32_t MAX_PAYLOAD_SIZE = 1400;

    explicit DpdkDriver(Context* context, int port = 0);
    virtual ~DpdkDriver();
    void close();
    virtual uint32_t getMaxPacketSize();
    virtual int getBandwidth();
    virtual int getTransmitQueueSpace(uint64_t currentTime);
    virtual void receivePackets(int maxPackets,
            std::vector<Received>* receivedPackets);
    virtual void release(char *payload);
    virtual void sendPacket(const Address *addr,
                            const void *header,
                            uint32_t headerLen,
                            Buffer::Iterator *payload,
                            uint8_t priority = 0);
    virtual string getServiceLocator();

    typedef Driver::PacketBuf<MacAddress, MAX_PAYLOAD_SIZE> PacketBuf;

    virtual Address* newAddress(const ServiceLocator* serviceLocator)
    {
        return new MacAddress(serviceLocator->getOption<const char*>("mac"));
    }

    Context* context;

    /// Holds packet buffers that are no longer in use, for use in future
    /// requests; saves the overhead of calling malloc/free for each request.
    ObjectPool<PacketBuf> packetBufPool;

    /// Tracks number of outstanding allocated payloads.  For detecting leaks.
    int packetBufsUtilized;

    /// The original ServiceLocator string. May be empty if the constructor
    /// argument was NULL. May also differ if dynamic ports are used.
    string locatorString;

    /// Stores the MAC address of the NIC (either native or overriden).
    Tub<MacAddress> localMac;

    /// Stores the NIC's physical port id addressed by the instantiated driver.
    uint8_t portId;

    /// Holds packet buffers that are dequeued from the NIC's HW queues
    /// via DPDK.
    struct rte_mempool *packetPool;

    /// Holds packets that are addressed to localhost instead of going through
    /// the HW queues.
    struct rte_ring *loopbackRing;

    /// Hardware packet filter is provided by the NIC
    bool hasHardwareFilter;

    /// Ethernet Header struct used in software packet filter
    struct EthernetHeader {
        uint8_t destAddress[6];
        uint8_t sourceAddress[6];
        uint16_t etherType; // network order
        uint16_t length;    // host order, length of payload
    } __attribute__((packed));

    // Effective network bandwidth, in Mbits/second.
    int bandwidthMbps;

    /// Used to estimate # bytes outstanding in the NIC's transmit queue.
    QueueEstimator queueEstimator;

    /// Upper limit on how many bytes should be queued for transmission
    /// at any given time.
    uint32_t maxTransmitQueueSize;

    /// Used to redirect log entries from the DPDK log into the RAMCloud log.
    FileLogger fileLogger;

    DISALLOW_COPY_AND_ASSIGN(DpdkDriver);
};

} // end RAMCloud

#endif  // RAMCLOUD_DPDKDRIVER_H
