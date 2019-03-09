#include "Broadcast.h"
#include "ClockSynchronizer.h"
#include "MilliSortService.h"
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

// FIXME: removing the following causes(?) an Arachne warning and eventually
// segfault(?)inside arachne when running ClusterPerf::clockSync
std::vector<std::vector<int>> K_NOMIAL_TREE_CHILDREN = {
#include "k_nomial_tree.txt"
};

// TODO: remove this constant.
#define BRANCH_FACTOR 10

/**
 * Prepare to invoke a broadcast operation by constructing a tree broadcast
 * task and schedules it on the rule engine. The caller must then call zero or
 * more times #appendCollectiveOp followed by a #send to actually initiate the
 * broadcast.
 *
 * \param context
 *      Service context.
 * \param group
 *      Communication group that specifies the recipients of the broadcast.
 * \param merger
 *      Used to combine responses from children nodes.
 * \param opId
 *      Identifier of this broadcast operation.
 */
TreeBcast::TreeBcast(Context* context, CommunicationGroup* group, uint32_t opId)
    : context(context)
    , opId(opId ? opId : uint32_t(generateRandom()))
    , group(group)
    , kNomialTree(BRANCH_FACTOR, group->size())
    , localResult()
    , root(group->rank)
    , outgoingResponse()
    , respHdr()
    , responseStorage()
    , outstandingRpcs()
    , payloadResponseHeaderLength(0)
    , payloadRequest()
    , payloadRpc()
    , serviceRpc()
{
    outgoingResponse = responseStorage.construct();
    WireFormat::TreeBcast::Response response;
    bzero(&response, sizeof(response));
    outgoingResponse->appendCopy(&response);
    respHdr = outgoingResponse->getStart<WireFormat::TreeBcast::Response>();
}

/**
 * Constructs a tree broadcast task in response to an incoming tree broadcast
 * request.
 *
 * The visibility of this constructor is set to private because it should only
 * be invoked by the #handleRpc method.
 *
 * \param context
 *      Service context.
 * \param group
 *      Communication group that specifies the recipients of the broadcast.
 * \param header
 *      TreeBcast request header as defined in WireFormat.
 * \param rpc
 *      Incoming broadcast RPC.
 */
TreeBcast::TreeBcast(Context* context,
        const WireFormat::TreeBcast::Request* reqHdr, Service::Rpc* rpc)
    : context(context)
    , opId(reqHdr->opId)
    , group(context->getMilliSortService()->getCommunicationGroup(reqHdr->groupId))
    , kNomialTree(BRANCH_FACTOR, group->size())
    , localResult()
    , root(reqHdr->root)
    , outgoingResponse(rpc->replyPayload)
    , respHdr(rpc->replyPayload->getStart<WireFormat::TreeBcast::Response>())
    , responseStorage()
    , outstandingRpcs()
    , payloadResponseHeaderLength(reqHdr->payloadResponseHeaderLength)
    , payloadRequest()
    , payloadRpc()
    , serviceRpc(rpc)
{
    timeTrace("TreeBcast %u, rank %u, request arrived", opId,
            uint32_t(group->relativeRank(root)));

    // Copy out the payload request (from network packet buffers) to make it
    // contiguous. TreeBcast is not meant for large messages anyway.
    payloadRequest.append(rpc->requestPayload, sizeof(*reqHdr), ~0u);

    // Kick start the broadcast.
    start();
}

/**
 * Return the children of a given parent node in the tree.
 *
 * \param parent
 *      Rank of the parent node in the tree.
 * \param[out] children
 *      Ranks of the children nodes will be pushed here upon return.
 */
void
TreeBcast::KNomialTree::getChildren(int parent, std::vector<int>* children)
{
    // Change the following from 0 to 1 to test with k-ary tree.
#if 0
    for (int i = 0; i < k; i++) {
        int child = parent * k + i + 1;
        if (child < nodes) {
            children->push_back(child);
        }
    }
#else
    // If d is the number of digits in the base-k representation of parent
    // then let delta = k^d.
    int delta = 1;
    int p = parent;
    while (p > 0) {
        p /= k;
        delta *= k;
    }

    // The children of a node in the k-nomial tree will have exactly one more
    // non-zero digit in a more significant position than the parent's most
    // significant digit. For example, in a trinomial tree (i.e., k = 3) of
    // size 27, the children of node 7 (21 in base-3) will be node 16 (121 in
    // base-3) and 25 (221 in base-3).
    while (true) {
        for (int i = 1; i < k; i++) {
            int child = parent + i * delta;
            if (child >= nodes) {
                return;
            }
            children->push_back(child);
        }
        delta *= k;
    }
#endif
}

/**
 * Assuming that #isCompleted has returned true and this method is invoked
 * on the root node of the broadcast operation, return the buffer storing the
 * final result produced from merging the individual responses collected from
 * each node in the communication group. Otherwise, NULL.
 *
 * The interpretation of the content inside the buffer is up to the caller
 * depending on the response format of the RPC being broadcasted and the merge
 * operation being used.
 */
Buffer*
TreeBcast::getResult()
{
    return (serviceRpc == NULL) ? outgoingResponse : NULL;
}

/**
 * Handler for tree broadcast requests. Invoked by the RPC dispatcher when
 * it sees the TREE_BCAST opcode.
 *
 * \pre
 *      The TreeBcast response header has been allocated and initialized.
 * \param context
 *      Service context.
 * \param reqHdr
 *      Header from the incoming RPC request; contains parameters for this
 *      operation.
 * \param rpc
 *      Service RPC object associated with the broadcast RPC.
 */
void
TreeBcast::handleRpc(Context* context,
        const WireFormat::TreeBcast::Request* reqHdr, Service::Rpc* rpc)
{
#if TIME_TRACE
    uint64_t now = Cycles::rdtsc();
    timeTrace("TreeBcast %u, handleRpc invoked, arachne spawn time %u cyc, "
            "service dispatch time %u cyc", reqHdr->opId,
            rpc->dispatchTime - rpc->arriveTime, now - rpc->dispatchTime);
#endif
    TreeBcast treeBcast(context, reqHdr, rpc);
    while (!treeBcast.isReady()) {
        Arachne::yield();
    }
}

/**
 * Helper method to kick start the broadcast operation. It should be invoked
 * after the request message becomes complete. It will the outgoing RPCs,
 * handles the inner RPC locally, and checks to see if the broadcast can be
 * completed when this method returns.
 */
void
TreeBcast::start()
{
    // k-nomial-tree assumes node 0 to be the root. Therefore, to find the
    // children of this node when node 0 is not the root, we need to use the
    // relative rank of this node to compute the children.
    int myRank = group->relativeRank(root);

    // If there is no embedded RPC, we are in benchmark mode. Record the receive
    // time and reply it back to the sender.
    if ((payloadResponseHeaderLength == 0) && (myRank > 0)) {
        uint64_t now = Cycles::rdtsc();
        outgoingResponse->emplaceAppend<uint32_t>(myRank);
        outgoingResponse->emplaceAppend<uint64_t>(context->clockSynchronizer->
                getConverter(group->getNode(root)).toRemoteTsc(now));
    }

    std::vector<int> children;
    kNomialTree.getChildren(myRank, &children);
    for (int child : children) {
        timeTrace("TreeBcast %u, size %u, rank %u, sending RPC to rank %u",
                opId, group->size(), myRank, child);
        outstandingRpcs.emplace_back(context, opId, group, root, child,
                payloadResponseHeaderLength, &payloadRequest);
    }

    // FIXME: can we avoid using RPC to deliver payload request locally? Could
    // save some time on the leaf nodes.
    // Deliver the payload request locally.
    if (payloadResponseHeaderLength > 0) {
        payloadRpc.construct(payloadResponseHeaderLength);
        payloadRpc->request.appendExternal(&payloadRequest);
        ServerId id = group->getNode(group->rank);
        payloadRpc->session = context->serverList->getSession(id);
        payloadRpc->send();
    }
}

bool
TreeBcast::isReady()
{
    // TODO: we really need a more scalable RPC completion mechanism eventually;
    // polling each outstanding RPC in turn sucks.
    for (OutstandingRpcs::iterator it = outstandingRpcs.begin();
            it != outstandingRpcs.end();) {
        TreeBcastRpc* rpc = &(*it);
        if (rpc->isReady()) {
            // Chop off the TreeBcast response header of the reply from
            // child nodes.
            rpc->response->truncateFront(TreeBcastRpc::responseHeaderLength);
            outgoingResponse->append(rpc->response);
            it = outstandingRpcs.erase(it);
        } else {
            it++;
        }
    }

    if (payloadRpc && payloadRpc->isReady()) {
        timeTrace("TreeBcast %u, payload RPC finished");
        payloadRpc->simpleWait(context);
        outgoingResponse->append(payloadRpc->response);
        payloadRpc.destroy();
    }

    return outstandingRpcs.empty() && !payloadRpc;
}

/**
 * Return the buffer that stores the final result produced by merging the
 * individual embedded RPC responses collected from each node in the
 * communication group.
 *
 * The interpretation of the content inside the buffer is up to the caller
 * depending on the response format of the RPC being broadcasted and the merge
 * operation being used.
 */
Buffer*
TreeBcast::wait()
{
    assert(root == group->rank);
    while (!isReady()) {
        Arachne::yield();
    }
    outgoingResponse->truncateFront(TreeBcastRpc::responseHeaderLength);
    return outgoingResponse;
}

}
