#include "AllShuffle.h"
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
AllShuffle::closeSendBuffer(int rank)
{
    if (rank == group->rank) {
        SpinLock::Guard _(mutex);
        merger(&localData);
    } else {
        outstandingRpcs++;
        outgoingRpcs[rank]->send();
    }
}

Buffer*
AllShuffle::getSendBuffer(int rank)
{
    return rank == group->rank ? &localData : &outgoingRpcs[rank]->request;
}

void
AllShuffle::handleRpc(const WireFormat::AllShuffle::Request* reqHdr,
        WireFormat::AllShuffle::Response* respHdr, Service::Rpc* rpc)
{
    SpinLock::Guard _(mutex);

    uint32_t senderId = reqHdr->senderId;
//    LOG(NOTICE, "received AllShuffleRpc from server %u", senderId + 1);
    if (receivedFrom.test(senderId)) {
        respHdr->common.status = STATUS_MILLISORT_REDUNDANT_DATA;
        return;
    }
    receivedFrom.set(senderId);

    // Chop off the AllShuffle header and incorporate the data.
    rpc->requestPayload->truncateFront(sizeof(*reqHdr));
    if (merger) {
        timeTrace("AllShuffle: about to merge payload from server %u (rank %u)",
                group->getNode(senderId).indexNumber(), senderId);
        merger(rpc->requestPayload);
    }
}

void
AllShuffle::wait()
{
    // TODO: what if we want to not send out all the rpcs at the beginning?
    for (auto& rpc : outgoingRpcs) {
        if (rpc) {
            rpc->wait();
            rpc.destroy();
            outstandingRpcs--;
        }
    }

    // We must not rely on Arachne::SpinLock to yield because it only yield upon
    // failure to acquire the lock. If the contender thread is dispatched to the
    // same core as this thread, this thread will keep acquiring the lock and
    // never yield.
    for (bool done = false; !done; Arachne::yield()) {
        SpinLock::Guard _(mutex);
        done = int(receivedFrom.count()) == group->size();
    }
}

}