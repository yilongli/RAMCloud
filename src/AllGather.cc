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
//    LOG(NOTICE, "AllGather: local phase %d, received from server %u, remote "
//            "phase %d", phase.load(),
//            group->getNode(reqHdr->senderId).indexNumber(), reqHdr->phase);

    int msgPhase = reqHdr->phase;
    int msgSender = reqHdr->senderId;
    if (msgPhase < phase) {
        respHdr->common.status = STATUS_MILLISORT_REDUNDANT_DATA;
        return;
    }

    // Copy out the data so that we can send back the reply. Otherwise, peers on
    // both sides will be waiting for sendDataRpc->isReady() to become true.
    Buffer buffer;
    buffer.appendCopy(rpc->requestPayload);
    rpc->sendReply();

    while (phase < msgPhase) {
        // Arachne::yield();
    }
    while (!sendDataRpc->isReady()) {
        // Arachne::wait(condition)??
    }

    // Chop off the AllGather header and incorporate the data.
    buffer.truncateFront(sizeof(*reqHdr));
    if (merger) {
        timeTrace("AllGather: about to merge payload from server %u (rank %u)",
                group->getNode(msgSender).indexNumber(), (uint32_t) msgSender);
        std::tie(data, length) = merger(&buffer);
    }

    // Start the next phase.
    int nextPhase = msgPhase + 1;
    if (nextPhase <= maxPhase) {
        sendDataRpc.construct(context, getPeerId(nextPhase), opId, nextPhase,
                group->rank, length, data);
    }
    phase.store(nextPhase);
}

bool
AllGather::isReady()
{
    return phase > maxPhase;
}

void
AllGather::wait()
{
    while (!isReady()) {}
}

}