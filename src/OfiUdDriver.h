/* Copyright (c) 2019 Stanford University
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

/**
 * \file
 * Header file for #RAMCloud::OfiUdDriver.
 */

#ifndef RAMCLOUD_OFIUDDRIVER_H
#define RAMCLOUD_OFIUDDRIVER_H

#include <deque>

#include <rdma/fabric.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>

#include "Common.h"
#include "Dispatch.h"
#include "Driver.h"
#include "ObjectPool.h"
#include "QueueEstimator.h"
#include "ServiceLocator.h"
#include "SpinLock.h"
#include "Tub.h"

namespace RAMCloud {

/**
 * A Driver for OpenFabrics Interfaces (OFI) unreliable datagram (DGRAM)
 * communication. Simple packet send/receive style interface. See Driver for
 * more detail.
 */
class OfiUdDriver : public Driver {
  public:
    explicit OfiUdDriver(Context* context,
            const ServiceLocator* localServiceLocator);
    virtual ~OfiUdDriver();
    virtual uint32_t getMaxPacketSize();
    virtual uint32_t getBandwidth();
    virtual void receivePackets(uint32_t maxPackets,
            std::vector<Received>* receivedPackets);
    virtual void registerMemory(void* base, size_t bytes);
    virtual void release();
    virtual void sendPacket(const Driver::Address* addr, const void* header,
                            uint32_t headerLen, Buffer::Iterator* payload,
                            int priority = 0,
                            TransmitQueueState* txQueueState = NULL);

    virtual string getServiceLocator();

    virtual Address* newAddress(const ServiceLocator* serviceLocator) {
        std::stringstream sstream(serviceLocator->getOption("addr"));
        std::string byteStr;
        std::vector<uint8_t> rawAddress;
        while (std::getline(sstream, byteStr, '.')) {
            rawAddress.push_back(downCast<uint8_t>(stoul(byteStr)));
        }
        fi_addr_t fi_addr;
        fi_av_insert(addressVector, rawAddress.data(), 1, &fi_addr, 0, NULL);
        return new Address(fi_addr);
    }

    virtual void uncorkTransmitQueue();

  PRIVATE:
    /**
     * Identifies the address of a libfabric endpoint.
     */
    class Address : public Driver::Address {
      public:
        Address(fi_addr_t addr)
            : Driver::Address()
            , addr(addr)
        {}

        virtual ~Address() {}

        virtual uint64_t getHash() const {
            return addr;
        }

        virtual string toString() const {
            return format("%lu", addr);
        }

        /// Opaque libfabric internal address that identifies the endpoint.
        fi_addr_t addr;
    };

    /**
     * Stores information about a single packet buffer (used for both
     * transmit and receive buffers).
     */
    struct BufferDescriptor {
        /// First byte of the packet buffer.
        char* buffer;

        /// Length of the buffer, in bytes.
        uint32_t length;

        /// Memory region in which buffer is allocated.
        fid_mr* memoryRegion;

        // Fields above here do not change once this structure has been
        // allocated. Fields below are modified based on the buffer's usage,
        // and may not always be valid.

        /// Opaque structure used by libfabric to keep track of outstanding
        /// send/recv operations. Required by providers that operate under
        /// FI_CONTEXT mode. See also:
        ///     https://github.com/ofiwg/ofi-guide/blob/master/OFIGuide.md#mode-bits
        fi_context context;

        /// If the buffer currently holds a packet, this gives the length
        /// of that packet, in bytes.
        uint32_t packetLength;

        /// Source address of the received packet. Not for transmitted packets.
        Tub<Address> sourceAddress;

        BufferDescriptor(char *buffer, uint32_t length, fid_mr* region)
            : buffer(buffer), length(length), memoryRegion(region),
              context(), packetLength(0), sourceAddress() {}

      private:
        DISALLOW_COPY_AND_ASSIGN(BufferDescriptor);
    };

    /**
     * Represents a collection of buffers allocated in a memory region
     * that has been registered with the NIC.
     */
    struct BufferPool {
        BufferPool(fid_domain* domain, fi_domain_attr* attr,
                uint32_t bufferSize, uint32_t numBuffers);
        ~BufferPool();

        /// Dynamically allocated memory for the buffers (must be freed).
        char *bufferMemory;

        /// Memory region associated with bufferMemory.
        fid_mr* memoryRegion;

        /// Dynamically allocated array holding one descriptor for each
        /// packet buffer in bufferMemory, in the same order as the
        /// corresponding packet buffers.
        BufferDescriptor* descriptors;

        /// Buffers that are currently unused.
        vector<BufferDescriptor*> freeBuffers;

        /// Total number of buffers (and descriptors) allocated.
        uint32_t numBuffers;

        DISALLOW_COPY_AND_ASSIGN(BufferPool);
    };

    /**
     * Stores information about a work request that will be posted to the TX
     * queue (i.e., a send request).
     */
//    struct SendRequest {
//        /// Work request to be posted to the TX queue.
//        ibv_send_wr wr;
//
//        /// Scatter-gather list within the work request.
//        ibv_sge sges[2];
//
//        /// Zeros out all member structs.
//        explicit SendRequest()
//            : wr(), sges() {}
//    };

    BufferDescriptor* getTransmitBuffer();
    ServiceLocator readDriverConfigFile();
    void reapTransmitBuffers();
    void refillReceiver();

    /// Dummy memory region identifer; used by providers that don't require
    /// or support memory registration (e.g., psm2).
    static fid_mr NO_MEMORY_REGION;

    /// Maximum number of bytes of datagrams to be sent with fi_inject,
    /// which is optimized for small message latency.
    static constexpr uint32_t MAX_INLINE_DATA = 400;

    /// Total receive buffers allocated. At any given time, some may be in
    /// the possession of the NIC, some (holding received data) may be in
    /// the possession of higher-level software processing requests, and
    /// some may be idle (in freeRxBuffers). We need a *lot* of these,
    /// if we're going to handle multiple 8-MB incoming RPCs at once.
    static constexpr uint32_t TOTAL_RX_BUFFERS = 50000;

    /// Maximum number of receive buffers that will be in the possession
    /// of the NIC at once.
    static constexpr uint32_t MAX_RX_QUEUE_DEPTH = 1000;

    /// Maximum number of transmit buffers that may be outstanding at once.
    static constexpr uint32_t MAX_TX_QUEUE_DEPTH = 128;

    /// Post a signaled send request, which generates a work completion entry
    /// when it completes, after posting SIGNALED_SEND_PERIOD-1 unsignaled send
    /// requests. The signal period should be small enough compared to the total
    /// number of transmit buffers (i.e., MAX_TX_QUEUE_DEPTH) so that the sender
    /// won't get blocked at getTransmitBuffer waiting for the completion signal
    /// of the last send request.
    /// As of 11/2018, refilling 64 transmit buffers takes only ~250ns on our
    /// rc machines.
    static constexpr int SIGNALED_SEND_PERIOD = 16;

    /// Identifier of the fabric this node belongs to. A fabric can be roughly
    /// thought of as a single cluster. Not owned by this class. See also:
    ///     https://github.com/ofiwg/ofi-guide/blob/master/OFIGuide.md#fabric-1
    fid_fabric* fabric;

    /// Overall information of the fabric. Owned by this class; must be freed
    /// upon destruction.
    fi_info* info;

    /// Identifier of the fabric domain that connects this node into the fabric.
    /// A domain is basically a port on the local NIC. Not owned by this class.
    /// See also:
    ///     https://github.com/ofiwg/ofi-guide/blob/master/OFIGuide.md#domains
    fid_domain* domain;

    /// Identifier of the connection-less endpoint used to send and receive
    /// data packets. Not owned by this class. See also:
    ///     https://github.com/ofiwg/ofi-guide/blob/master/OFIGuide.md#active
    fid_ep* endpoint;

    /// Identifier of the local addressing table that maps provider-specific
    /// addresses (i.e., those returned by fi_getname) to opaque libfabric
    /// address (i.e., fi_addr_t). Not owned by this class. See also:
    ///     https://github.com/ofiwg/ofi-guide/blob/master/OFIGuide.md#connection-less-communications
    ///     https://github.com/ofiwg/ofi-guide/blob/master/OFIGuide.md#address-vectors-1
    fid_av* addressVector;

    /// Identifier of the completion queue for receiving incoming packets.
    /// Not owned by this class.
    fid_cq* rxcq;

    /// Identifier of the completion queue used by the NIC to return buffers
    /// for transmitted packets. Not owned by this class.
    fid_cq* txcq;

    /// Outgoing packets currently queued up in the driver because the transmit
    /// queue is corked.
//    std::vector<SendRequest> corkedPackets;

    /// FIFO queue which holds packets addressed to the local host. Only used
    /// in raw ethernet mode.
    std::deque<BufferDescriptor*> loopbackPkts;

    /// Packet buffers used for receiving incoming packets.
    Tub<BufferPool> rxPool;

    /// Number of receive buffers currently in the possession of the NIC.
    uint32_t rxBuffersInNic;

    /// Used to log messages when receive buffer usage hits a new high.
    /// Log the next message when the number of free receive buffers
    /// drops to this level.
    uint32_t rxBufferLogThreshold;

    /// Packet buffers used to transmit outgoing packets.
    Tub<BufferPool> txPool;

    /// Transmit buffers currently in the possession of the NIC.
    std::deque<BufferDescriptor*> txBuffersInNic;

    /// Size of the prefix buffer space in all packet buffers, in bytes,
    /// that are left for use by libfabric (similar to GRH in Infiniband UD).
    uint32_t datagramPrefixSize;

    /// Maximum # bytes of a datagram that can be sent with fi_inject,
    /// which is optimized for small message latency.
    uint32_t maxInlineData;

    /// Maximum # buffers that can be posted to the transmit queue in a batch.
    uint32_t maxPostTxBuffers;

    /// Maximum # buffers that can be posted to the receive queue in a batch.
    uint32_t maxPostRxBuffers;

    /// Active maximum MTU enabled on #ibPhysicalPort to transmit and receive.
    /// This is the maximum message size that an UD QP can transmit.
    uint32_t mtu;

    /// Our ServiceLocator, including the dynamic lid and qpn
    string locatorString;

    /// Effective outgoing network bandwidth, in Gbits/second.
    uint32_t bandwidthGbps;

    /// Holds send requests to be posted to the TX queue: one send request for
    /// each outgoing packet. This is only used temporarily during #sendPackets,
    /// but it's allocated here so that we only pay the cost for storage
    /// allocation once.
//    std::vector<SendRequest> sendRequests;

    /// True means enabling the selective notification optimization when
    /// sending packets.
    bool enableUnsignalSendOpt;

    /// Used to post a signaled send request after every Nth packet is sent.
    int sendsSinceLastSignal;

    /// Address of the first byte of the "zero-copy region". This is an area
    /// of memory that is addressable directly by the NIC. When transmitting
    /// data from this region, we don't need to copy the data into packet
    /// buffers; we can point the NIC at the memory directly. NULL if no
    /// zero-copy region.
    char* zeroCopyStart;

    /// Address of the byte just after the last one of the zero-copy region.
    /// NULL if no zero-copy region.
    char* zeroCopyEnd;

    /// Memory region associated with the zero-copy region, or NULL if there
    /// is no zero-copy region.
    fid_mr* zeroCopyRegion;

    DISALLOW_COPY_AND_ASSIGN(OfiUdDriver);
};

} // end RAMCloud

#endif  // RAMCLOUD_OFIUDDRIVER_H
