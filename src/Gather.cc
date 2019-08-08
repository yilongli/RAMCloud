#include "Gather.h"
#include "TimeTrace.h"

namespace RAMCloud {

// Change 0 -> 1 in the following line to compile detailed time tracing in
// this transport.
#define TIME_TRACE 0

// Change the following to 0 to test with k-ary tree, or 1 to test with
// k-nomial tree.
#define TREE_TYPE 0

// TODO: remove this handcoded constant; can we choose it dynamically?
// A k1-k2 tree is simple (suppose node 0 is the root):
//  1. Node 1 to node (k1 + 1) are children of node 0
//  2. Starting from node (k1 + 2), every k2 nodes is a group where the first
// node is the parent of the rest of the group and a child of node 0.
#define BRANCH_FACTOR_1 10
#define BRANCH_FACTOR_2 8

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
    , gatherTree(BRANCH_FACTOR_1, BRANCH_FACTOR_2, group->size())
    // FIXME: creating a SpinLock with name is quite expensive (a few us!) due
    // to dynamic memory allocation in string creation.
//    , mutex("TreeGather::mutex")
    , mutex()
    , numPayloadsToMerge()
    , sendData()
{
    if (unlikely(group->size() == 1)) {
        DIE("Gather of 1 node is currently broken!!!!");
    }

    // The gather tree assumes node 0 to be the root. Therefore, to find the
    // children of this node when node 0 is not the root, we need to use the
    // relative rank of this node to compute the children.
    std::vector<int> children;
    gatherTree.getChildren(relativeRank, &children);
    size_t numChildren = children.size();
    timeTrace("TreeGather: constructor invoked, opId %d, group size %d, "
            "rank %d, numChildren %u", opId, group->size(), group->rank,
            numChildren);

    // If this is an interior node, wait until we have received data from
    // all its children before sending to its parent; otherwise, send out
    // the data immediately.
    if (numChildren > 0) {
        nodesToGather.construct(children.begin(), children.end());
        numPayloadsToMerge = downCast<int>(numChildren);
        // Don't add the data duplicately; the merger should've been initialized
        // with the local data!
//        merger->add(&dataBuf);
    } else {
        int parent = gatherTree.getParent(relativeRank);
        Buffer dataBuf;
        dataBuf.appendExternal(data, numBytes);
        timeTrace("TreeGather: sending data to parent %u, size %u, opId %d",
                parent, numBytes, opId);
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
TreeGather::GatherTree::getChildren(int parent, std::vector<int>* children)
{
#if TREE_TYPE == 0
    for (int i = 0; i < k1; i++) {
        int child = parent * k1 + i + 1;
        if (child < nodes) {
            children->push_back(child);
        }
    }
#elif TREE_TYPE == 1
    int k = k1;
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
#else
    if (parent == 0) {
        for (int i = 1; i <= k1; i++) {
            if (i >= nodes) {
                return;
            }
            children->push_back(i);
        }
        int child = k1 + 1;
        while (child < nodes) {
            children->push_back(child);
            child += k2;
        }
    } else if (parent > k1) {
        if ((parent - k1 - 1) % k2 == 0) {
            for (int i = 1; i < k2; i++) {
                int child = parent + i;
                if (child >= nodes) {
                    return;
                }
                children->push_back(child);
            }
        }
    }
#endif
}

int
TreeGather::GatherTree::getParent(int child)
{
#if TREE_TYPE == 0
    return (child - 1) / k1;
#elif TREE_TYPE == 1
    int k = k1;
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
#else
    if (child <= k1) {
        return 0;
    } else {
        child -= (k1 + 1);
        if (child % k2 == 0) {
            return 0;
        } else {
            return (k1 + 1) + (child / k2 * k2);
        }
    }
#endif
}

bool
TreeGather::isReady()
{
    if (group->rank != root) {
        return sendData && sendData->isReady();
    } else {
        return numPayloadsToMerge.load(std::memory_order_acquire) == 0;
    }
}

void
TreeGather::handleRpc(const WireFormat::TreeGather::Request* reqHdr,
        WireFormat::TreeGather::Response* respHdr, Service::Rpc* rpc)
{
    // Chop off the TreeGather header.
    uint32_t senderId = reqHdr->senderId;
    rpc->requestPayload->truncateFront(sizeof(*reqHdr));
    timeTrace("TreeGather: received new data, sender %u, size %u, opId %d",
            senderId, rpc->requestPayload->size(), opId);

    // TODO: we can even eliminate this spinlock by chaning nodesToGather to
    // be a atomic bitmask.
    {
        SpinLock::Guard _(mutex);
        if (nodesToGather->erase(senderId) == 0) {
            respHdr->common.status = STATUS_MILLISORT_REDUNDANT_DATA;
            return;
        }
    }

    // Incorporate the payload.
    if (merger) {
        merger->add(rpc->requestPayload);
    }
    bool completed = (numPayloadsToMerge.fetch_sub(1) == 1);

    if (completed && (group->rank != root)) {
        int parent = gatherTree.getParent(relativeRank);
        timeTrace("TreeGather: sending data to parent %u, size %u, opId %d",
                parent, merger->getResult()->size(), opId);
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
