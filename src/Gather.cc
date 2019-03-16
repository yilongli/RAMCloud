#include "Gather.h"
#include "TimeTrace.h"

namespace RAMCloud {

// Change 0 -> 1 in the following line to compile detailed time tracing in
// this transport.
#define TIME_TRACE 0

// Change the following from 0 to 1 to test with k-ary tree.
#define FIXED_ARY_TREE 0

// TODO: remove this handcoded constant; this number should be chosen dynamically
// based on the ratio of OWD (i.e., latency) and per-RPC CPU overhead:
// e.g., if OWD + BW_COST at the root node = a * RPC_RX_CPU_COST, we might want
// to have a gather tree whose root's out degree is a.
#define BRANCH_FACTOR 10

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

TreeGather::TreeGather(int opId, Context* context, CommunicationGroup* group,
        int root, uint32_t numBytes, const void* data, Merger* merger)
    : context(context)
    , group(group)
    , merger(merger)
    , opId(opId)
    , root(root)
    , relativeRank(group->relativeRank(root))
    , nodesToGather()
    , kNomialTree(BRANCH_FACTOR, group->size())
    , mutex("TreeGather::mutex")
    , sendData()
{
    // k-nomial-tree assumes node 0 to be the root. Therefore, to find the
    // children of this node when node 0 is not the root, we need to use the
    // relative rank of this node to compute the children.
    std::vector<int> children;
    kNomialTree.getChildren(relativeRank, &children);
    size_t numChildren = children.size();

    // If this is an interior node, wait until we have received data from
    // all its children before sending to its parent; otherwise, send out
    // the data immediately.
    if (numChildren > 0) {
        nodesToGather.construct(children.begin(), children.end());
        // Don't add the data duplicately; the merger should've been initialized
        // with the local data!
//        merger->add(&dataBuf);
    } else {
        int parent = kNomialTree.getParent(relativeRank);
        Buffer dataBuf;
        dataBuf.appendExternal(data, numBytes);
        sendData.construct(context, group->getNode(root + parent), opId,
                group->rank, &dataBuf);
    }
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
TreeGather::KNomialTree::getChildren(int parent, std::vector<int>* children)
{
#if FIXED_ARY_TREE
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

int
TreeGather::KNomialTree::getParent(int child)
{
#if FIXED_ARY_TREE
    return (rank - 1) / k;
#else
    // If d is the number of digits in the base-k representation of parent
    // then let x = k^d.
    int x = 1;
    int r = child;
    int msd = 0;    // most significant digit
    while (r > 0) {
        msd = r % k;
        r /= k;
        x *= k;
    }

    // Clear the most significant digit in the child's base-k representation of
    // its rank.
    return child - msd * x / k;
#endif
}

bool
TreeGather::isReady()
{
    if (group->rank != root) {
        return sendData && sendData->isReady();
    } else {
        Lock _(mutex);
        return nodesToGather->empty();
    }
}

void
TreeGather::handleRpc(const WireFormat::TreeGather::Request* reqHdr,
        WireFormat::TreeGather::Response* respHdr, Service::Rpc* rpc)
{
    Lock _(mutex);

    assert(group->rank == 0);
    uint32_t senderId = reqHdr->senderId;
    if (nodesToGather->find(senderId) == nodesToGather->end()) {
        respHdr->common.status = STATUS_MILLISORT_REDUNDANT_DATA;
        return;
    }
    nodesToGather->erase(senderId);

    // Chop off the TreeGather header and incorporate the data.
    rpc->requestPayload->truncateFront(sizeof(*reqHdr));
    if (merger) {
        timeTrace("TreeGather: about to merge payload from server %u (rank %u)",
                group->getNode(senderId).indexNumber(), senderId);
        merger->add(rpc->requestPayload);
    }

    if (nodesToGather->empty() && (group->rank != root)) {
        int parent = kNomialTree.getParent(relativeRank);
        sendData.construct(context, group->getNode(root + parent), opId,
                group->rank, merger->getResult());
    }
}

void
TreeGather::wait()
{
    while (!isReady()) {
        Arachne::yield();
    }
}
}