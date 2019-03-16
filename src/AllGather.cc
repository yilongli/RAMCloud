#include "AllGather.h"
#include "TimeTrace.h"

namespace RAMCloud {

// Change 0 -> 1 in the following line to compile detailed time tracing in
// this transport.
#define TIME_TRACE 0

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

/**
 * Constructor used by nodes who are senders in the all-gather operation.
 *
 * \tparam Merger
 * \param opId
 * \param context
 * \param group
 * \param numElements
 * \param elements
 * \param merger
 */
AllGather::AllGather(int opId, Context* context, CommunicationGroup* group,
        uint32_t length, void* data, Merger* merger)
    : context(context)
    , completed(false)
    , currentPhase()
    , lastPhase()
    , expanderNode(false)
    , ghostNode(false)
    , group(group)
    , merger(merger)
    , opId(opId)
    , nearestPowerOfTwo()
    , numRecursiveDoublingSteps(0)
    , outstandingRpcs()
{
    int n = group->size();
    if (n == 1) {
        // Special case: only one node in the group; nothing to do.
        completed = true;
        return;
    }

    bool powerOfTwo = true;
    while (n > 1) {
        powerOfTwo = powerOfTwo && (n % 2 == 0);
        n /= 2;
        numRecursiveDoublingSteps++;
    }
    nearestPowerOfTwo = 1 << numRecursiveDoublingSteps;

    // Determine the starting phase of this node. Note: not all nodes start at
    // the same stage.
    if (powerOfTwo) {
        // The number of nodes is a power of two. The plain old recursive
        // doubling algorithm suffices.
        currentPhase = 1;
    } else {
        // Otherwise, we need to extend the recursive doubling algorithm with
        // an initial contraction step and a final expansion step to deal with
        // non-power-of-two group sizes.
        int remainder = group->size() - nearestPowerOfTwo;
        if (group->rank >= nearestPowerOfTwo) {
            // This node is a sender of the initial contraction phase.
            currentPhase = 0;
            ghostNode = true;
        } else if (group->rank < remainder) {
            // This node is a receiver of the initial contraction phase.
            currentPhase = 0;
            expanderNode = true;
        } else {
            // This node can skip the contraction phase and start from the
            // exchange phase directly.
            currentPhase = 1;
        }
    }
    lastPhase = numRecursiveDoublingSteps;
    if (ghostNode || expanderNode) {
        lastPhase++;
    }

    Buffer dataBuf;
    dataBuf.appendExternal(data, length);
    std::vector<ServerId> targets;
    getPeersToSend(currentPhase, &targets);
    for (ServerId target : targets) {
        timeTrace("about to send to server %u, phase %u", target.indexNumber(),
                currentPhase.load());
        outstandingRpcs.emplace_back(context, target, opId, currentPhase,
                group->rank, &dataBuf);
    }
}

/**
 * Obtain all peer nodes that we need to send data to in a given phase.
 *
 * \param phase
 *      Phase in which the peer nodes belong to.
 * \param[out] targets
 *      ServerId's of the peer nodes will be pushed into this vector upon
 *      return.
 */
void
AllGather::getPeersToSend(int phase, std::vector<ServerId>* targets)
{
    if (phase == 0) {
        // Initial contration step: only the larger half of the nodes in the
        // group might need to participate.
        assert(nearestPowerOfTwo < group->size());
        if (ghostNode) {
            targets->push_back(group->getNode(group->rank - nearestPowerOfTwo));
        }
    } else if (phase > numRecursiveDoublingSteps) {
        // Final expansion step: only the smaller half of the node in the
        // group might need to participate.
        assert(nearestPowerOfTwo < group->size());
        if (expanderNode) {
            targets->push_back(group->getNode(group->rank + nearestPowerOfTwo));
        }
    } else {
        assert((1 <= phase) && (phase <= numRecursiveDoublingSteps));
        if (!ghostNode) {
            int p = phase - 1;
            int rank = group->rank >> p;
            int d = (rank % 2) ? -1 : 1;
            targets->push_back(group->getNode(group->rank + d * (1 << p)));
        }
    }
}

void
AllGather::handleRpc(const WireFormat::AllGather::Request* reqHdr,
        WireFormat::AllGather::Response* respHdr, Service::Rpc* rpc)
{
    timeTrace("received from server %u, local phase %u, remote phase %u",
            group->getNode(reqHdr->senderId).indexNumber(), currentPhase.load(),
            reqHdr->phase);
    int msgPhase = reqHdr->phase;
    int msgSender = reqHdr->senderId;
    if (msgPhase < currentPhase) {
        respHdr->common.status = STATUS_MILLISORT_REDUNDANT_DATA;
        return;
    }

    // Copy out the data so that we can send back the reply. Otherwise, peers on
    // both sides will deadlock at `outgoingRpc->wait()`.
    // FIXME: avoid deadlock without extra copy? seems to require the ability to
    // take ownership of the incoming RPC request payload.
    Buffer buffer;
    buffer.appendCopy(rpc->requestPayload);
    rpc->sendReply();

    // If this is a ghost node, advance to the last expansion phase directly
    // because it doesn't participate in the intermediate exchange steps.
    // Otherwise, we must wait for the local node to catch up before merging
    // data from the incoming RPC.
    if (ghostNode) {
        currentPhase = lastPhase;
    }
    while (currentPhase < msgPhase) {
        Arachne::yield();
    }

    // Must wait till the outgoing RPC to finish before incorporating the
    // incoming data. Otherwise, we risk corrupting the content of the outgoing
    // request.
    while (!outstandingRpcs.empty()) {
        outstandingRpcs.front().wait();
        outstandingRpcs.pop_front();
    }

    // Chop off the AllGather header and incorporate the data.
    buffer.truncateFront(sizeof(*reqHdr));
    if (merger) {
        timeTrace("AllGather: about to merge payload from server %u (rank %u)",
                group->getNode(msgSender).indexNumber(), (uint32_t) msgSender);
        if (ghostNode) {
            merger->replace(&buffer);
        } else {
            merger->add(&buffer);
        }
    }

    // Start the next phase unless we have reached the final one.
    int nextPhase = msgPhase + 1;
    if (nextPhase <= lastPhase) {
        std::vector<ServerId> targets;
        getPeersToSend(nextPhase, &targets);
        for (ServerId target : targets) {
            timeTrace("about to send to server %u, phase %u",
                    target.indexNumber(), currentPhase.load());
            outstandingRpcs.emplace_back(context, target, opId, nextPhase,
                    group->rank, merger->getResult());
        }
        currentPhase.store(nextPhase);
    } else {
        completed = true;
    }
}

bool
AllGather::isReady()
{
    if (expanderNode && (currentPhase == lastPhase)) {
        while (!outstandingRpcs.empty()) {
            if (outstandingRpcs.front().isReady()) {
                outstandingRpcs.pop_front();
            } else {
                break;
            }
        }
        if (outstandingRpcs.empty()) {
            completed = true;
        }
    }
    return completed;
}

void
AllGather::wait()
{
    while (!isReady()) {
        Arachne::yield();
    }
}

}