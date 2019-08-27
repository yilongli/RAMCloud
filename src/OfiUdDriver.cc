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

#include <cstdlib>
#include <fstream>
#include <sys/types.h>

#include <rdma/fi_cm.h>
#include <rdma/fi_errno.h>

#include "Arachne/DefaultCorePolicy.h"
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
#define TIME_TRACE 1

// Provides a cleaner way of invoking TimeTrace::record, with the code
// conditionally compiled in or out by the TIME_TRACE #ifdef. Arguments
// are made uint64_t (as opposed to uin32_t) so the caller doesn't have to
// frequently cast their 64-bit arguments into uint32_t explicitly: we will
// help perform the casting internally.
namespace {
    inline uint64_t
    timeTrace(const char* format,
            uint64_t arg0 = 0, uint64_t arg1 = 0, uint64_t arg2 = 0,
            uint64_t arg3 = 0)
    {
#if TIME_TRACE
        return TimeTrace::record(format, uint32_t(arg0), uint32_t(arg1),
                uint32_t(arg2), uint32_t(arg3));
#else
        return 0;
#endif
    }
}

// FIXME: figure out why we can't use the result of fi_av_straddr in
// ServiceLocator and use fi_av_insertsvc to convert that into fi_addr?
#define SL_USE_FI_ADDR_STR 0

#define STR(token) #token

// Most libfabric APIs return negative values to indicate errors. This macro
// provides a simple way to invoke such an API and check its return value.
// For simplicity, this macro doesn't try to recover from the error; it simply
// logs a message and exits upon error.
#define FI_CHK_CALL(fn, ...)                                        \
        do {                                                        \
            int ret = downCast<int>(fn(__VA_ARGS__));               \
            if (ret < 0) {                                          \
                DIE("%s failed: %s", STR(fn), fi_strerror(-ret));   \
            }                                                       \
        } while (0)

// Short-hand to obtain the starting address of a BufferDescriptor based on its
// libfabric context address.
#define context_to_bd(ctx) reinterpret_cast<BufferDescriptor*>( \
    static_cast<char*>(ctx) - OFFSET_OF(BufferDescriptor, context))

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
    , scalableEp()
//    , endpoint()
    , addressVector()
    , rxcq()
    , txcq()
    , addressLength()
    , addressMap()
//    , corkedPackets()
    , loopbackPkts()

    // FIXME: newly added fields for RX-side scaling; not yet properly ordered
    , receiverThreads()
    , receiverThreadStop()
    , rxPacketsLock()
    , rxPackets()
    , rxThreadContext()

    , rxPool()
    , rxBuffersInNic()
    , rxBufferLogThreshold(0)
    , txPool()
    , txBuffersInNic()
    , datagramPrefixSize()
    , maxInlineData(MAX_INLINE_DATA)
    , mtu(0)
    , mustIncludeLocalAddress(true)
    , mustRegisterLocalMemory()
    , locatorString("ofiud:")
    , bandwidthGbps(0)
//    , sendRequests()
    , enableSeletiveCompOpt(false)
    , sendsSinceLastSignal(0)
    , zeroCopyStart(NULL)
    , zeroCopyEnd(NULL)
    , zeroCopyRegion(NULL)
{
    ServiceLocator config = readDriverConfigFile();
    const char* provider = config.getOption<const char*>("prov", NULL);
    bandwidthGbps = config.getOption<uint32_t>("gbs", 0);
    mtu = config.getOption<uint32_t>("mtu", 0);
    LOG(NOTICE, "OfiUdDriver config: %s", config.getOriginalString().c_str());

    // FIXME: print PSM2 related env variables
    if (strcmp(provider, "psm2") == 0) {
        // FIXME: instead of setting env vars in ~/.bashrc, set it here?
        // Example .bashrc:
        //      export PSM2_MQ_EAGER_SDMA_SZ=16384
        //      export PSM2_MQ_RNDV_HFI_THRESH=1000000000
        //      export FI_PSM2_LOCK_LEVEL=0
        static const char* ENV_VARS[] = {"PSM2_MQ_EAGER_SDMA_SZ",
                "PSM2_MQ_RNDV_HFI_THRESH", "FI_PSM2_LOCK_LEVEL",
                "FI_PSM2_PROG_INTERVAL", "FI_PSM2_PROG_AFFINITY"};
        for (const char* env : ENV_VARS) {
            char* value = std::getenv(env);
            if (value) {
                LOG(NOTICE, "%s = %s", env, value);
            }
        }
    }

    // Fill out the hints struct to indicate the capabilities we need and the
    // operation modes we support.
    fi_info* hints = fi_allocinfo();
    hints->fabric_attr->prov_name = strdup(provider);
    // We need an endpoint that 1) supports sending and receiving messages or
    // datagrams and 2) returns source address as part of its completion data.
    // TODO: wtf, provider verbs doesn't support FI_SOURCE? how to get src addr then?
    // TODO: FI_LOCAL_COMM, FI_REMOTE_COMM are also relevant; deal with them
    hints->caps = FI_MSG;
//    hints->caps = FI_MSG | FI_SOURCE;
    // Make it clear that we need the endpoint for connectionless, unreliable
    // datagram (not a reliable, connection-oriented endpoint like FI_EP_MSG).
    hints->ep_attr->type = FI_EP_DGRAM;
    // TODO: scalable EPs; for now, only uses one Tx queue as SDMA + large MTU
    // is enough to keep up with 100Gbps outbound BW; uses >= 2 RX queues to
    // allow parallelizing data copying in eager RX mode.
    hints->ep_attr->tx_ctx_cnt = 1;
    hints->ep_attr->rx_ctx_cnt = MAX_RX_QUEUES;
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
    // FIXME: use auto-progress for psm2 provider? NO! It's unclear to me what
    // falls under the responsibility of the background thread (e.g., copying
    // data in eager receive mode doesn't seem to be covered, which is one of
    // the reasons why I wanted to try auto-progress mode). Using auto progress
    // mode is piling even more opaque shit on the (already) fucked-up psm2
    // provider.
//    hints->domain_attr->data_progress = FI_PROGRESS_AUTO;
    hints->domain_attr->data_progress = FI_PROGRESS_MANUAL;
    // Generate a CQE for an outgoing packet as soon as it's put on the wire.
    hints->tx_attr->op_flags = FI_INJECT_COMPLETE;

    // Query libfabric to obtain the most performant qualified endpoint.
    fi_info* infoList;
    FI_CHK_CALL(fi_getinfo, FI_VERSION(1, 8), NULL, NULL, 0, hints, &infoList);
    info = fi_dupinfo(infoList);
    fi_freeinfo(hints);
    fi_freeinfo(infoList);
    LOG(DEBUG, "Fabric interface info:\n%s", fi_tostr(info, FI_TYPE_INFO));

    // Set the runtime parameters of this driver based on the fabric info and
    // further tune up a few attributes.
    addressLength = downCast<uint32_t>(info->src_addrlen);
    if (info->tx_attr->size > MAX_TX_QUEUE_DEPTH) {
        info->tx_attr->size = MAX_TX_QUEUE_DEPTH;
    }
    if (info->rx_attr->size > MAX_RX_QUEUE_DEPTH) {
        info->rx_attr->size = MAX_RX_QUEUE_DEPTH;
    }
    if (info->tx_attr->comp_order & FI_ORDER_STRICT) {
        // Enable the selective notification optimization for sending packets.
        enableSeletiveCompOpt = true;
        txBuffersInNic.construct();
        info->tx_attr->comp_order = FI_ORDER_STRICT;
    } else {
        info->tx_attr->comp_order = FI_ORDER_NONE;
    }
    datagramPrefixSize = downCast<uint32_t>(info->ep_attr->msg_prefix_size);
    if (mustIncludeLocalAddress) {
        // Place the source address info right after the prefix buffer space.
        datagramPrefixSize += addressLength;
    }
    if (info->tx_attr->inject_size < maxInlineData) {
        maxInlineData = downCast<uint32_t>(info->tx_attr->inject_size);
    }
    mustRegisterLocalMemory = info->domain_attr->mr_mode & FI_MR_LOCAL;

    // Compute link speed and MTU to setup the queue estimator.
    // FIXME: hack to workaround info->nic == NULL for psm2
    if (info->nic) {
        if (bandwidthGbps == 0) {
            bandwidthGbps = downCast<uint32_t>(
                    static_cast<double>(info->nic->link_attr->speed) /
                    (1024.0*1024.0*1024.0));
        }
        mtu = downCast<uint32_t>(info->nic->link_attr->mtu);
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
    FI_CHK_CALL(fi_scalable_ep, domain, info, &scalableEp, NULL);

    // Create the address vector and bind it to the endpoint.
    fi_av_attr av_attr = {};
    av_attr.type = FI_AV_MAP;
    FI_CHK_CALL(fi_av_open, domain, &av_attr, &addressVector, NULL);
    FI_CHK_CALL(fi_scalable_ep_bind, scalableEp, &addressVector->fid, 0);
    addressMap.construct(addressVector);

    // Create completion queues for receive and transmit. Note: a completion
    // queue isn't absolutely necessary for the transmit queue; a completion
    // counter should suffice.
    fi_cq_attr tx_cq_attr = {};
    tx_cq_attr.size = MAX_TX_QUEUE_DEPTH;
    tx_cq_attr.format = FI_CQ_FORMAT_CONTEXT;
    FI_CHK_CALL(fi_cq_open, domain, &tx_cq_attr, &txcq, NULL);
    for (int i = 0; i < int(info->ep_attr->tx_ctx_cnt); i++) {
        fi_tx_context(scalableEp, i, info->tx_attr, &transmitContext[i], NULL);
    }
    FI_CHK_CALL(fi_ep_bind, transmitContext[0], &txcq->fid, FI_TRANSMIT);
    // FIXME: deal with selective notification later!
//    FI_CHK_CALL(fi_ep_bind, endpoint, &txcq->fid,
//            FI_TRANSMIT | FI_SELECTIVE_COMPLETION);

    fi_cq_attr rx_cq_attr = {};
    rx_cq_attr.size = MAX_RX_QUEUE_DEPTH;
    rx_cq_attr.format = FI_CQ_FORMAT_MSG;
    for (int i = 0; i < int(MAX_RX_QUEUES); i++) {
        fi_rx_context(scalableEp, i, info->rx_attr, &receiveContext[i], NULL);
    }
    for (uint32_t i = 0; i < MAX_RX_QUEUES; i++) {
        FI_CHK_CALL(fi_cq_open, domain, &rx_cq_attr, &rxcq[i], NULL);
        FI_CHK_CALL(fi_ep_bind, receiveContext[i], &rxcq[i]->fid, FI_RECV);
    }

    // Activate the endpoint and update locatorString with the dynamic address.
    FI_CHK_CALL(fi_enable, scalableEp);
    size_t addrlen = 64;
    std::vector<uint8_t> localAddress;
    localAddress.resize(addrlen);
#if 0
    // Print raw addresses of all receive contexts.
    FI_CHK_CALL(fi_getname, (fid_t)scalableEp, localAddress.data(), &addrlen);
    LOG(NOTICE, "scalableEp: %s",
            RawAddress(localAddress.data(), addrlen).toString().c_str());
    for (uint32_t i = 0; i < MAX_RX_QUEUES; i++) {
        FI_CHK_CALL(fi_getname, (fid_t)receiveContext[i], localAddress.data(),
                &addrlen);
        RawAddress rawAddress(localAddress.data(), addrlen);
        LOG(NOTICE, "rxCtx[%u]: %s", i, rawAddress.toString().c_str());
    }
#endif
    FI_CHK_CALL(fi_getname, (fid_t)receiveContext[0], localAddress.data(),
            &addrlen);
    if (addrlen != addressLength) {
        DIE("Unexpected address length %lu (expecting %u)", addrlen,
                addressLength);
    }
#if SL_USE_FI_ADDR_STR
    char addrStr[100];
    size_t addrStrLen = 100;
    fi_av_straddr(addressVector, localAddress.data(), addrStr, &addrStrLen);
    addrStr[addrStrLen] = 0;
    locatorString += format("addr=%s", addrStr);
#else
    RawAddress rawAddress(localAddress.data(), downCast<uint32_t>(addrlen));
    locatorString += "addr=" + rawAddress.toString();
#endif
    LOG(NOTICE, "Locator for OfiUdDriver: %s", locatorString.c_str());

    // Allocate buffer pools.
    uint32_t bufSize = BitOps::powerOfTwoGreaterOrEqual(mtu);
    rxPool.construct(this, bufSize, TOTAL_RX_BUFFERS);
    rxBufferLogThreshold = TOTAL_RX_BUFFERS - 1000;
    txPool.construct(this, bufSize, MAX_TX_QUEUE_DEPTH);
    LOG(NOTICE, "Initialized OfiUdDriver buffers: %u receive buffers (%u MB), "
            "%u transmit buffers (%u MB)",
            TOTAL_RX_BUFFERS, (TOTAL_RX_BUFFERS*bufSize)/(1024*1024),
            MAX_TX_QUEUE_DEPTH, (MAX_TX_QUEUE_DEPTH*bufSize)/(1024*1024));

    // Fill in the src address in every transmit buffer, if necessary.
    if (mustIncludeLocalAddress) {
        for (BufferDescriptor* bd : txPool->freeBuffers) {
            char* srcAddr = bd->buffer + datagramPrefixSize - addressLength;
            memcpy(srcAddr, localAddress.data(), addrlen);
        }
    }

    // Pre-post some receive buffers in each RX queue.
    for (int rxid = 0; rxid < int(MAX_RX_QUEUES); rxid++) {
        refillReceiver(rxid, true);
    }

    // Spawn receive threads for polling RX queues (and copying data into RX
    // buffers if 0-copy RX is not supported); spread out the threads on
    // available cores evenly.
    auto receiverThreadMain = [this] (int rxid) {
        LOG(NOTICE, "OfiUdDriver: RX thread %d started on core %d", rxid,
                Arachne::core.id);
        while (!receiverThreadStop.load(std::memory_order_acquire)) {
            receivePacketsImpl(rxid, 8);
            Arachne::yield();

            // Each iteration of the following loop processes one pending copy
            // request enqueued by the transport; the loop doesn't return or
            // yield until it has finished all the pending requests.
            RxThreadContext* threadCtx = &rxThreadContext[rxid];
            CopyRequest copyRequest;
            while (true) {
                // Critical section: check if there is any pending copy request;
                // if so, fetch the first one.
                {
                    SpinLock::Guard _(threadCtx->lock);
                    if (threadCtx->pendingRequests.empty()) {
                        break;
                    }
                    copyRequest = threadCtx->pendingRequests.front();
                    threadCtx->pendingRequests.pop_front();
                }

                uint32_t nbytes = copyRequest.len;
                timeTrace("ofiud: rxid %d to copy %u bytes", rxid, nbytes);
                memcpy(copyRequest.dst, copyRequest.src, nbytes);
                copyRequest.bytesCopied->fetch_add(nbytes);
                timeTrace("ofiud: rxid %d copied %u bytes, coreId %d",
                        rxid, nbytes, Arachne::core.id);
            }
        }
    };

    Arachne::CorePolicy::CoreList cores = Arachne::getCorePolicy()->getCores(0);
    for (int rxid = 1; rxid < int(MAX_RX_QUEUES); rxid++) {
        int coreId = cores[(rxid - 1) % cores.size()];
        receiverThreads.push_back(
                Arachne::createThreadOnCore(coreId, receiverThreadMain, rxid));
    }
}

/**
 * Destroy an OfiUdDriver and free allocated resources.
 */
OfiUdDriver::~OfiUdDriver()
{
    // Wait for the receiver thread to stop.
    receiverThreadStop = true;
    for (Arachne::ThreadId& tid : receiverThreads) {
        Arachne::join(tid);
    }

    // FIXME:
//    if (fi_mr_desc(zeroCopyRegion)) {
//        fi_close(&zeroCopyRegion->fid);
//    }
//    fi_close(&rxcq->fid);
//    fi_close(&txcq->fid);
//    fi_close(&addressVector->fid);
//    fi_close(&endpoint->fid);
//    fi_close(&domain->fid);
//    fi_close(&fabric->fid);
//    fi_freeinfo(info);

    uint32_t totalRxBuffersInNic = 0;
    for (uint32_t i = 0; i < MAX_RX_QUEUES; i++) {
        totalRxBuffersInNic += rxBuffersInNic[i];
    }
    size_t buffersInUse = TOTAL_RX_BUFFERS - rxPool->freeBuffers.size()
            - totalRxBuffersInNic;
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
 * Return a free transmit buffer, wrapped by its corresponding BufferDescriptor.
 * If there are none, block until one is available.
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
            timeTrace("ofiud: TX buffers refilled after %u polls", count);
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
#define MAX_TO_RETRIEVE 8
    fi_cq_entry cqes[MAX_TO_RETRIEVE];
    int numCqes = downCast<int>(fi_cq_read(txcq, cqes, MAX_TO_RETRIEVE));
    if (numCqes <= 0) {
        if (unlikely(numCqes != -FI_EAGAIN)) {
            if (numCqes == -FI_EAVAIL) {
                fi_cq_err_entry err = {};
                FI_CHK_CALL(fi_cq_readerr, txcq, &err, 0);
                DIE("fi_cq_read failed, fi_cq_readerr: %s", fi_cq_strerror(
                        txcq, err.prov_errno, err.err_data, NULL, 0));
            } else {
                DIE("fi_cq_read failed: %s", fi_strerror(-numCqes));
            }
        }
        return;
    } else if (numCqes > 0) {
        timeTrace("ofiud: polling txcq returned %d CQEs", numCqes);
    }

    if (enableSeletiveCompOpt) {
        // Each iteration of the following loop attempts to match one CQE with
        // a transmit buffer posted earlier. Upon the match, we know that any
        // transmit buffer that have been checked so far can be reused as the
        // underlying provider guarantees generating CQEs in a FIFO order.
        for (int i = 0; i < numCqes; i++) {
            BufferDescriptor* signaledCompletion =
                    context_to_bd(cqes[i].op_context);
            bool matchSignal = false;
            while (!txBuffersInNic->empty()) {
                BufferDescriptor* bd = txBuffersInNic->front();
                txBuffersInNic->pop_front();
                txPool->freeBuffers.push_back(bd);
                if (bd == signaledCompletion) {
                    matchSignal = true;
                    break;
                }
            }
            if (!matchSignal) {
                DIE("Couldn't find the send request (SR) just completed");
            }
        }
    } else {
        for (int i = 0; i < numCqes; i++) {
            BufferDescriptor* bd = context_to_bd(cqes[i].op_context);
            txPool->freeBuffers.push_back(bd);
        }
    }
}

/*
 * See docs in the ``Driver'' class.
 */
void
OfiUdDriver::registerMemory(void* base, size_t bytes)
{
    if (mustRegisterLocalMemory) {
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
        static fid_mr NO_MEMORY_REGION = {};
        zeroCopyRegion = &NO_MEMORY_REGION;
        zeroCopyStart = 0;
        zeroCopyEnd = reinterpret_cast<char*>(~0lu);
    }
}

/*
 * See docs in the ``Driver'' class.
 */
void
OfiUdDriver::release()
{
    // Test if we can acquire the lock and return the packets to rxPool;
    // don't block if the lock is currently held by another thread.
    if (!packetsToRelease.empty() && rxPacketsLock.try_lock()) {
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
        rxPacketsLock.unlock();
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
    timeTrace("ofiud: sendPacket invoked");
    uint32_t payloadSize = payload ? payload->size() : 0;
    uint32_t totalLength = headerLen + payloadSize;
    assert(totalLength <= getMaxPacketSize());

    // Grab a free packet buffer.
    BufferDescriptor* bd = getTransmitBuffer();
    bd->packetLength = datagramPrefixSize + totalLength;

    // Leave enough buffer headroom for libfabric. Then copy transport header
    // into packet buffer.
    char* dst = bd->buffer + datagramPrefixSize;
    memcpy(dst, header, headerLen);
    dst += headerLen;

    // Copy payload into packet buffer or apply zero-copy when approapriate.
    iovec io_vec[2];
    io_vec[0]= {
        .iov_base = bd->buffer,
        .iov_len = bd->packetLength
    };
    size_t iov_count = 1;
    void* desc[2] = {fi_mr_desc(bd->memoryRegion)};
    while (payload && !payload->isDone()) {
        // Use zero copy for the *last* chunk of the packet, if it's in the
        // zero copy region and is large enough to justify the overhead of
        // an additional scatter-gather element.
        const char* currentChunk =
                reinterpret_cast<const char*>(payload->getData());
        bool isLastChunk = payload->getLength() == payload->size();
        if (isLastChunk && (payload->getLength() >= 4000)
                && (currentChunk >= zeroCopyStart)
                && (currentChunk + payload->getLength() < zeroCopyEnd)) {
            io_vec[1].iov_base = const_cast<char*>(currentChunk);
            io_vec[1].iov_len = payload->getLength();
            io_vec[0].iov_len -= payload->getLength();
            desc[1] = fi_mr_desc(zeroCopyRegion);
            iov_count = 2;
            break;
        } else {
            memcpy(dst, currentChunk, payload->getLength());
            dst += payload->getLength();
            if (isLastChunk) {
                timeTrace("ofiud: 0-copy TX for last chunk not applicable; "
                        "copied %u bytes", payload->getLength());
            }
        }
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

    // Post the packet buffer to the transmit queue.
    const Address* address = static_cast<const Address*>(addr);
    lastTransmitTime = Cycles::rdtsc();

    // FIXME: hack to do packet balancing on the sender-side; this relies on
    // the FI_NAMED_RX_CTX cap of psm2 when using scalable endpoints. Note
    // that we reserve the first rx queue for relatively small messages in order
    // to improve their latency since the first rxcq will be polled inside the
    // dispatch thread.
    uint32_t remoteRxid = (bd->packetLength < 8192) ? 0 :
            1 + context->dispatch->iteration % (MAX_RX_QUEUES - 1);
    fi_addr_t dstAddr = address->addr[remoteRxid];

    int ret;
    if (bd->packetLength <= info->tx_attr->inject_size) {
        ret = downCast<int>(fi_inject(transmitContext[0], bd->buffer,
                bd->packetLength, dstAddr));
        txPool->freeBuffers.push_back(bd);
    } else {
        ret = downCast<int>(fi_sendv(transmitContext[0], io_vec, desc,
                iov_count, dstAddr, &bd->context));
        if (txBuffersInNic) {
            txBuffersInNic->push_back(bd);
        }
    }
    if (ret) {
        DIE("Error posting transmit packet: %s", fi_strerror(-ret));
    }

    timeTrace("ofiud: sent packet with %u bytes, %u free buffers",
            bd->packetLength, txPool->freeBuffers.size());
    // FIXME: ignore small (how small?) packets so in BasicTransport we can
    // log more meaningful "tx queue idle XXX cyc" message
    if (unlikely(bd->packetLength > 128)) {
        queueEstimator.packetQueued(bd->packetLength, lastTransmitTime,
                txQueueState);
    }
    PerfStats::threadStats.networkOutputBytes += bd->packetLength;
    PerfStats::threadStats.networkOutputPackets++;
}

// FIXME: I think psm2 is such a shitty library that batching with FI_MORE makes
// no difference.
#if 0
/*
 * See docs in the ``Driver'' class.
 */
void
OfiUdDriver::sendPackets(const Driver::Address* addr,
                         const void* headers,
                         uint32_t headerLen,
                         Buffer::Iterator* messageIt,
                         int priority,
                         TransmitQueueState* txQueueState)
{
    const uint32_t messageBytes = messageIt->size();
    timeTrace("ofiud: sendPackets invoked, message bytes %u", messageBytes);

    uint32_t maxPayload = getMaxPacketSize() - headerLen;
    uint32_t numPackets = (messageIt->size() + maxPayload - 1) / maxPayload;

    // Each iteration of the following loop enqueues one outgoing packet to
    // the transmit queue. All send operations except the last one are flagged
    // as FI_MORE to indicate that additional packets will be sent immediately
    // after the current call returns.
    uint32_t bytesSent = 0;
    const char* header = reinterpret_cast<const char*>(headers);
    for (uint32_t i = 0; i < numPackets; i++) {
        // Get a packet buffer.
        uint32_t payloadSize = std::min(maxPayload, messageIt->size());
        BufferDescriptor* bd = getTransmitBuffer();
        bd->packetLength = datagramPrefixSize + headerLen + payloadSize;

        // Copy transport header into packet buffer.
        char* dst = bd->buffer + datagramPrefixSize;
        memcpy(dst, header, headerLen);
        header += headerLen;
        dst += headerLen;

        // Copy payload into packet buffer or apply zero-copy when approapriate.
        iovec io_vec[2];
        io_vec[0]= {
            .iov_base = bd->buffer,
            .iov_len = bd->packetLength
        };
        size_t iov_count = 1;
        void* desc[2] = {fi_mr_desc(bd->memoryRegion)};
        const char* payloadStart =
                reinterpret_cast<const char*>(messageIt->getData());
        if ((messageIt->getLength() >= payloadSize) &&
                (zeroCopyStart <= payloadStart) &&
                (payloadStart + payloadSize <= zeroCopyEnd)) {
            // Add a new scatter-gather entry that points to the payload.
            iov_count = 2;
            io_vec[1].iov_base = const_cast<char*>(payloadStart);
            io_vec[1].iov_len = payloadSize;
            io_vec[0].iov_len -= payloadSize;
            desc[1] = fi_mr_desc(zeroCopyRegion);
            messageIt->advance(payloadSize);
        } else {
            // Copy payload into packet buffer.
            uint32_t bytesToCopy = payloadSize;
            while (bytesToCopy > 0) {
                // The current buffer chunk contains the rest of the packet.
                if (messageIt->getLength() >= bytesToCopy) {
                    memcpy(dst, messageIt->getData(), bytesToCopy);
                    messageIt->advance(bytesToCopy);
                    break;
                }

                memcpy(dst, messageIt->getData(), messageIt->getLength());
                dst += messageIt->getLength();
                bytesToCopy -= messageIt->getLength();
                messageIt->next();
            }
        }

        // Send the packet.
        fi_msg msg = {
            .msg_iov = io_vec,
            .desc = &desc[0],
            .iov_count = iov_count,
            .addr = static_cast<const Address*>(addr)->addr,
            .context = &bd->context,
            .data = 0
        };
        uint64_t flags = (i < numPackets) ?
                (FI_MORE | FI_INJECT_COMPLETE) : FI_INJECT_COMPLETE;
        FI_CHK_CALL(fi_sendmsg, endpoint, &msg, flags);
        timeTrace("ofiud: enqueued one more packet");

        if (txBuffersInNic) {
            txBuffersInNic->push_back(bd);
        }
        bytesSent += bd->packetLength;
    }
    lastTransmitTime = Cycles::rdtsc();
    timeTrace("ofiud: sent %u packets (%u bytes), %u free buffers", numPackets,
            bytesSent, txPool->freeBuffers.size());
    queueEstimator.packetQueued(bytesSent, lastTransmitTime, txQueueState);
    PerfStats::threadStats.networkOutputBytes += bytesSent;
    PerfStats::threadStats.networkOutputPackets += numPackets;
}
#endif

void
OfiUdDriver::receivePacketsImpl(int rxid, uint32_t maxPackets)
{
    static const uint32_t MAX_COMPLETIONS = 16;
    fi_cq_msg_entry wc[MAX_COMPLETIONS];
    maxPackets = std::min(maxPackets, MAX_COMPLETIONS);
    uint64_t pollCqTime = timeTrace("ofiud: polled rxcq %d", rxid);
    int numPackets = downCast<int>(fi_cq_read(rxcq[rxid], wc, maxPackets));
    if (numPackets <= 0) {
        if (unlikely(numPackets != -FI_EAGAIN)) {
            if (numPackets == -FI_EAVAIL) {
                fi_cq_err_entry err = {};
                FI_CHK_CALL(fi_cq_readerr, rxcq[rxid], &err, 0);
                DIE("fi_cq_read failed, fi_cq_readerr: %s", fi_cq_strerror(
                        rxcq[rxid], err.prov_errno, err.err_data, NULL, 0));
            } else {
                DIE("fi_cq_read failed: %s", fi_strerror(-numPackets));
            }
        }
#if TIME_TRACE
        uint64_t now = Cycles::rdtsc();
        if (now - pollCqTime >= Cycles::fromMicroseconds(2)) {
            TimeTrace::record(now, "ofiud: polling rxcq %d took too long!",
                    rxid);
        }
#endif
        TimeTrace::cancelRecord(pollCqTime);
        return;
    }
#if TIME_TRACE
    uint64_t receiveTime = timeTrace("ofiud: received %d packets from rxcq %d",
            numPackets, rxid);
    TimeTrace::cancelRecord(pollCqTime);
#else
    uint64_t receiveTime = Cycles::rdtsc();
#endif

    rxBuffersInNic[rxid] -= downCast<uint32_t>(numPackets);
    if (unlikely(rxBuffersInNic[rxid] == 0)) {
        RAMCLOUD_CLOG(WARNING, "OfiUdDriver: receiver temporarily ran "
                "out of packet buffers; could result in dropped packets");
    }

    // TODO: explain why we acquire the lock here: need to cover refillReceiver
    // which modifies rxPool.
    SpinLock::Guard _(rxPacketsLock);

    // Give the RX queue a chance to replenish.
    refillReceiver(rxid);

    // Each iteration of the following loop processes one incoming packet.
    for (int i = 0; i < numPackets; i++) {
        fi_cq_msg_entry* incoming = &wc[i];
        BufferDescriptor* bd = context_to_bd(incoming->op_context);

        // Convert the raw source address of the packet into fi_addr_t.
        fi_addr_vec srcAddr;
        if (mustIncludeLocalAddress) {
            char* srcAddrBuf = bd->buffer + datagramPrefixSize - addressLength;
            RawAddress rawAddress(srcAddrBuf, addressLength);
            addressMap->insertIfAbsent(&rawAddress, &srcAddr);
        }

        bd->packetLength = downCast<uint32_t>(incoming->len);
        bd->sourceAddress.construct(&srcAddr);
        rxPackets.emplace_back(bd->sourceAddress.get(), this,
                bd->packetLength - datagramPrefixSize,
                bd->buffer + datagramPrefixSize, receiveTime);
        rxPackets.back().delegateCopy = &rxThreadContext[rxid].callback;
        PerfStats::threadStats.networkInputBytes += bd->packetLength;
        PerfStats::threadStats.networkInputPackets++;
    }
    timeTrace("ofiud: receivePackets done");
}

/*
 * See docs in the ``Driver'' class.
 */
void
OfiUdDriver::receivePackets(uint32_t maxPackets,
        std::vector<Received>* receivedPackets)
{
    // Receive "small" packets that are addressed to rxcq[0] in the dispatch
    // thread to improve latency.
    receivePacketsImpl(0, maxPackets);

    if (rxPacketsLock.try_lock()) {
        while (maxPackets-- && !rxPackets.empty()) {
            receivedPackets->emplace_back(std::move(rxPackets.front()));
            rxPackets.pop_front();
        }
        rxPacketsLock.unlock();
    }
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

// See docs in Driver class.
Driver::Address*
OfiUdDriver::newAddress(const ServiceLocator* serviceLocator) {
#if SL_USE_FI_ADDR_STR
    const char* addr = serviceLocator->getOption<const char*>("addr");
//    LOG(NOTICE, "remote addr = %s", addr);
// TODO: how to avoid duplicate insert?
    fi_av_insertsvc(addressVector, addr, NULL, &fi_addr, 0, NULL);
    return new Address(fi_addr);
#else
    std::stringstream sstream(serviceLocator->getOption("addr"));
    std::string byteStr;
    std::vector<uint8_t> bytes;
    while (std::getline(sstream, byteStr, '.')) {
        bytes.push_back(downCast<uint8_t>(stoul(byteStr)));
    }
    RawAddress rawAddress(bytes.data(), addressLength);
    SpinLock::Guard _(rxPacketsLock);
    fi_addr_vec fi_addr;
    addressMap->insertIfAbsent(&rawAddress, &fi_addr);
    return new Address(&fi_addr);
#endif
}

/**
 * Fill up the NIC's receive queue with more packet buffers.
 *
 * \param refillAll
 *     Post as many free receive buffers to the NIC as possible when this
 *     variable is true; otherwise, only post a small batch of buffers at
 *     a time.
 */
void
OfiUdDriver::refillReceiver(int rxid, bool refillAll)
{
    // Always refill in batch to amortize the cost of posting receive buffers.
    static const uint32_t REFILL_BATCH = 8;
    uint32_t maxRefill = std::min(MAX_RX_QUEUE_DEPTH - rxBuffersInNic[rxid],
            downCast<uint32_t>(rxPool->freeBuffers.size()));
    if (maxRefill < REFILL_BATCH) {
        return;
    }

    // Each iteration of the following loop posts one receive buffer to the
    // receive queue. All post operations except the last one are flagged as
    // FI_MORE to indicate that additional buffers will be posted immediately
    // after the current call returns.
    uint32_t refillCount = refillAll ? maxRefill : REFILL_BATCH;
    for (int i = 0; i < refillCount; i++) {
        BufferDescriptor* bd = rxPool->freeBuffers.back();
        rxPool->freeBuffers.pop_back();

        iovec msg_iov = {
            .iov_base = bd->buffer,
            .iov_len = bd->length,
        };
        void* desc = fi_mr_desc(bd->memoryRegion);
        fi_msg msg = {
            .msg_iov = &msg_iov,
            .desc = &desc,
            .iov_count = 1,
            .addr = 0,
            .context = &bd->context,
            .data = 0
        };
        uint64_t flags = (i < refillCount) ? FI_MORE : 0;
        FI_CHK_CALL(fi_recvmsg, receiveContext[rxid], &msg, flags);
    }
    rxBuffersInNic[rxid] += refillCount;
    timeTrace("ofiud: RX queue %d refilled, rxBuffersInNic %u", rxid,
            rxBuffersInNic[rxid]);

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
 * Insert a raw address into the libfabric address vector if it hasn't been
 * inserted before.
 *
 * \param rawAddress
 *      Raw address to insert.
 * \return
 *      Address in fi_addr_t format.
 */
void
OfiUdDriver::AddressMap::insertIfAbsent(RawAddress* rawAddress,
        fi_addr_vec* out)
{
    auto it = map.find(*rawAddress);
    if (it == map.end()) {
        fi_addr_vec& addr_vec = map[*rawAddress];
        fi_addr_t addr;
        for (uint32_t i = 0; i < MAX_RX_QUEUES; i++) {
            fi_av_insert(addressVector, rawAddress->raw, 1, &addr, 0, NULL);
            addr_vec[i] = addr;
            // FIXME: this is not very nice; we are modifying the input rawAddr
            rawAddress->raw[1] += PSM2_MULTI_EP_ADDR_INC;
        }
        *out = addr_vec;
    } else {
        *out = it->second;
    }
}

/**
 * Constructor for BufferPool objects.
 * 
 * \param driver
 *      Driver this buffer pool belongs to.
 * \param bufferSize
 *      Size of each packet buffer, in bytes.
 * \param numBuffers
 *      Number of buffers to allocate in the pool.
 */
OfiUdDriver::BufferPool::BufferPool(OfiUdDriver* driver, uint32_t bufferSize,
        uint32_t numBuffers)
    : bufferMemory(NULL)
    , memoryRegion(NULL)
    , descriptors()
    , freeBuffers()
    , numBuffers(numBuffers)
{
    // Dummy memory region identifer; used by providers that don't require
    // or support memory registration (e.g., psm2).
    static fid_mr NO_MEMORY_REGION = {};

    // Allocate space for the packet buffers (page aligned, full pages).
    size_t bytesToAllocate = ((bufferSize * numBuffers) + 4095) & ~0xfffu;
    bufferMemory = reinterpret_cast<char*>(Memory::xmemalign(HERE, 4096,
            bytesToAllocate));

    if (driver->mustRegisterLocalMemory) {
        int ret = fi_mr_reg(driver->domain, bufferMemory, bytesToAllocate,
                FI_SEND | FI_RECV, 0, 0, 0, &memoryRegion, NULL);
        if (ret) {
            DIE("Couldn't register memory region: %s", fi_strerror(-ret));
        }
    }
    descriptors = reinterpret_cast<BufferDescriptor*>(
            malloc(numBuffers*sizeof(BufferDescriptor)));
    char* buffer = bufferMemory;
    fid_mr* mr = memoryRegion ? memoryRegion : &NO_MEMORY_REGION;
    for (uint32_t i = 0; i < numBuffers; i++) {
        new(&descriptors[i]) BufferDescriptor(buffer, bufferSize, mr);
        freeBuffers.push_back(&descriptors[i]);
        buffer += bufferSize;
    }
}

/**
 * Destructor for BufferPools.
 */
OfiUdDriver::BufferPool::~BufferPool()
{
    if (memoryRegion) {
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
