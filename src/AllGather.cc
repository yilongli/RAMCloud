#include "AllGather.h"
#include "TimeTrace.h"

namespace RAMCloud {

// Change 0 -> 1 in the following line to compile detailed time tracing in
// this transport.
#define TIME_TRACE 1

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

void
AllGather::handleRpc(const WireFormat::AllGather::Request* reqHdr,
        WireFormat::AllGather::Response* respHdr, Service::Rpc* rpc)
{
    SpinLock::Guard _(mutex);

    if (!receivedFrom) {
        receivedFrom.construct(reqHdr->numSenders);
    }

    uint32_t senderId = reqHdr->senderId;
//    LOG(WARNING, "received AllGatherRpc from sender %u", senderId+1);
    if (receivedFrom->test(senderId)) {
        respHdr->common.status = STATUS_MILLISORT_REDUNDANT_DATA;
        return;
    }
    receivedFrom->set(senderId);

    // Chop off the AllGather header and incorporate the data.
    rpc->requestPayload->truncateFront(sizeof(*reqHdr));
    if (merger) {
        timeTrace("AllGather: about to merge payload from rank %u", senderId);
        merger(rpc->requestPayload);
    }
}

bool
AllGather::isReady()
{
    if (broadcast && !broadcast->isReady()) {
        return false;
    }
    if (isReceiver) {
        SpinLock::Guard _(mutex);
        if (!receivedFrom ||
                (receivedFrom->count() < receivedFrom->size())) {
            return false;
        }
    }
    return true;
}

void
AllGather::wait()
{
    while (!isReady()) {}
}

}