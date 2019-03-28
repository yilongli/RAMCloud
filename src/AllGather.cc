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
    , complete(false)
    , completeTime(0)
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
        complete = true;
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
    std::vector<int> targets;
    getPeersToSend(currentPhase, &targets);
    for (int target : targets) {
        timeTrace("AllGather: sending to server %u, phase %u", target,
                currentPhase.load());
        outstandingRpcs.emplace_back(context, group->getNode(target), opId,
                currentPhase, group->rank, &dataBuf);
    }
}

/**
 * Check if any outgoing RPC has finished.
 *
 * \param waitOnBlock
 *      True means we should yield when an outgoing RPC is not ready and wait
 *      for its completion. False means we should return from this method.
 */
void
AllGather::checkOutgoingRpcs(bool waitOnBlock)
{
    while (!outstandingRpcs.empty()) {
        if (!outstandingRpcs.front().isReady()) {
            if (waitOnBlock) {
                Arachne::yield();
            } else {
                return;
            }
        } else {
            outstandingRpcs.pop_front();
        }
    }
}

/**
 * Obtain all peer nodes that we need to send data to in a given phase.
 *
 * \param phase
 *      Phase in which the peer nodes belong to.
 * \param[out] targets
 *      Ranks of the peer nodes will be pushed into this vector upon return.
 */
void
AllGather::getPeersToSend(int phase, std::vector<int>* targets)
{
    if (phase == 0) {
        // Initial consolidation step: only the larger half of the nodes in the
        // group might need to participate.
        assert(nearestPowerOfTwo < group->size());
        if (ghostNode) {
            targets->push_back(group->rank - nearestPowerOfTwo);
        }
    } else if (phase > numRecursiveDoublingSteps) {
        // Final expansion step: only the smaller half of the node in the
        // group might need to participate.
        assert(nearestPowerOfTwo < group->size());
        if (expanderNode) {
            targets->push_back(group->rank + nearestPowerOfTwo);
        }
    } else {
        assert((1 <= phase) && (phase <= numRecursiveDoublingSteps));
        if (!ghostNode) {
            int p = phase - 1;
            int rank = group->rank >> p;
            int d = (rank % 2) ? -1 : 1;
            targets->push_back(group->rank + d * (1 << p));
        }
    }
}

void
AllGather::handleRpc(const WireFormat::AllGather::Request* reqHdr,
        WireFormat::AllGather::Response* respHdr, Service::Rpc* rpc)
{
    // Chop off the AllGather header and incorporate the data.
    rpc->requestPayload->truncateFront(sizeof(*reqHdr));

    int msgPhase = reqHdr->phase;
    timeTrace("AllGather: received from server %u, size %u, local phase %u, "
            "remote phase %u", reqHdr->senderId, rpc->requestPayload->size(),
            currentPhase.load(), msgPhase);

    // If this is a ghost node, advance to the last expansion phase directly
    // because it doesn't participate in the intermediate exchange steps.
    // Otherwise, we must wait for the local node to catch up before merging
    // data from the incoming RPC.
    if (ghostNode) {
        merger->clear();
        currentPhase = lastPhase;
    }
    while (currentPhase < msgPhase) {
        Arachne::yield();
    }

    // Incorporate the data.
    timeTrace("AllGather: about to merge payload from sender %u",
            reqHdr->senderId);
    merger->append(rpc->requestPayload);
    uint64_t mergeCompleteTime = Cycles::rdtsc();
    timeTrace("AllGather: merge completed");

    // We are done with the request buffer; send back the reply since the other
    // end needs to wait on it eventually.
    rpc->sendReply();

    // Start the next phase unless we have reached the final one.
    int nextPhase = msgPhase + 1;
    if (nextPhase <= lastPhase) {
        std::vector<int> targets;
        getPeersToSend(nextPhase, &targets);
        for (int target : targets) {
            // Start sending RPCs of the next phase.
            timeTrace("AllGather: sending to server %u, phase %u, size %u",
                    target, nextPhase, merger->getResult()->size());
            outstandingRpcs.emplace_back(context, group->getNode(target), opId,
                    nextPhase, group->rank, merger->getResult());
        }

        // Check if some older RPCs have finished and then advance the phase.
        // Note: the order is important to prevent data race on #outstandingRpcs.
        checkOutgoingRpcs();
        currentPhase.store(nextPhase, std::memory_order_release);
        timeTrace("AllGather: increment phase to %d", nextPhase);

        // Special handling for expander nodes: there will be no incoming
        // request so we must mark our completion before we return.
        if (expanderNode && (nextPhase == lastPhase)) {
            timeTrace("AllGather: expander node final stage");
            checkOutgoingRpcs(true);
            completeTime = mergeCompleteTime;
            complete.store(1, std::memory_order_release);
            timeTrace("AllGather: operation completed");
        }
    } else {
        checkOutgoingRpcs(true);
        completeTime = mergeCompleteTime;
        complete.store(1, std::memory_order_release);
        timeTrace("AllGather: operation completed");
    }
}

bool
AllGather::isReady()
{
    return complete.load(std::memory_order_acquire);
}

uint64_t
AllGather::wait()
{
    while (!isReady()) {
        Arachne::yield();
    }
    return completeTime;
}
}