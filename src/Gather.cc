#include "Gather.h"
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

bool
FlatGather::isReady()
{
    if (group->rank != root) {
        return sendData->isReady();
    } else {
        SpinLock::Guard _(mutex);
        return int(gatheredFrom->count()) == group->size();
    }
}

void
FlatGather::handleRpc(const WireFormat::FlatGather::Request* reqHdr,
        WireFormat::FlatGather::Response* respHdr, Service::Rpc* rpc)
{
    SpinLock::Guard _(mutex);

    assert(group->rank == 0);
    uint32_t senderId = reqHdr->senderId;
    if (gatheredFrom->test(senderId)) {
        respHdr->common.status = STATUS_MILLISORT_REDUNDANT_DATA;
        return;
    }

    // Chop off the FlatGather header and incorporate the data.
    rpc->requestPayload->truncateFront(sizeof(*reqHdr));
    if (merger) {
        timeTrace("about to merge gathered payload from server %u (rank %u)",
                group->getNode(senderId).indexNumber(), senderId + 1);
        merger(rpc->requestPayload);
    }

    gatheredFrom->set(senderId);
}

void
FlatGather::wait()
{
    while (!isReady()) {}
}

}