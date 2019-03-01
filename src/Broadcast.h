#ifndef GRANULARCOMPUTING_BROADCAST_H
#define GRANULARCOMPUTING_BROADCAST_H

#include <list>

#include "CommunicationGroup.h"
#include "ServerIdRpcWrapper.h"
#include "Service.h"
#include "WireFormat.h"

namespace RAMCloud {

// TODO: how would fire-and-forget feature in Homa RPC change the interface/impl we have here?

// TODO: how to implement multi-level RPCs (e.g. Bcast of Bcast of Echo, Bcast of Scatter of X)

/**
 * A broadcast operation sends a given RPC request to a group of nodes and
 * collects responses from each one of them. For each broadcast operation, one
 * instance of this class is created on each of the node involved to manage the
 * progress of the broadcast.
 *
 * This class implements the broadcast operation using the k-nomial-tree
 * algorithm.
 *
 * TODO: Limitations
 * 1. Only support static communication group agreed upon globally
 * 2. No fault-tolerance
 * 3. No exactly-once guarantee
 * 4. Fire-and-forget? (i.e. bcast ack right after inner RPC delivered)
 */
class TreeBcast {
  public:
    explicit TreeBcast(Context* context, CommunicationGroup* group);
  PRIVATE:
    explicit TreeBcast(Context* context,
            const WireFormat::TreeBcast::Request* reqHdr, Service::Rpc* rpc);

  public:
    ~TreeBcast() = default;

    // TODO: remove this; use wait() instead?
    Buffer* getResult();
    static void handleRpc(Context* context,
            const WireFormat::TreeBcast::Request* reqHdr, Service::Rpc* rpc);

    /**
     * Constructs the innermost RPC request at the end of the send buffer and
     * kicks start the broadcast.
     *
     * \tparam RpcType
     *      Type of the innermost RPC.
     * \tparam Args
     *      Types of the arguments used to construct the innermost RPC request.
     * \param args
     *      Arguments used to construct the innermost RPC request.
     */
    template <typename RpcType, typename... Args>
    void
    send(Args&&... args)
    {
        // Construct the innermost RPC request and, thus, complete the TreeBcast
        // RPC request.
        RpcType::appendRequest(&payloadRequest, args...);
        payloadResponseHeaderLength = RpcType::responseHeaderLength;

        // Kick start the broadcast.
        start();
    }

    /// For testing only. No embedded RPC; just a block of data.
    void send(const void* data, uint32_t length)
    {
        assert(payloadRequest.size() == 0);
        payloadResponseHeaderLength = 0;
        payloadRequest.appendExternal(data, length);
        respHdr->receiveTime = Cycles::rdtsc();
        start();
    }

    bool isReady();
    Buffer* wait(uint64_t* broadcastTime = NULL);

  PRIVATE:
    void start();

    class TreeBcastRpc : public ServerIdRpcWrapper {
      public:
        explicit TreeBcastRpc(Context* context, CommunicationGroup* group,
                int root, int child, uint32_t payloadResponseHeaderLength,
                Buffer* payloadRequest)
            : ServerIdRpcWrapper(context, group->getNode(root + child),
                sizeof(WireFormat::TreeBcast::Response))
        {
            WireFormat::TreeBcast::Request* reqHdr(
                    allocHeader<WireFormat::TreeBcast>());
            reqHdr->groupId = group->id;
            reqHdr->root = root;
            reqHdr->payloadResponseHeaderLength = payloadResponseHeaderLength;
            request.appendExternal(payloadRequest);
            send();
        }

        /// Return the time (in Cycles::rdtsc ticks at the root node) when the
        /// broadcast message reached the last node in the tree.
        uint64_t
        wait()
        {
            waitAndCheckErrors();
            uint64_t receiveTime =
                    getResponseHeader<WireFormat::TreeBcast>()->receiveTime;
            // Chop off the TreeBcast response header.
            response->truncateFront(responseHeaderLength);
            return receiveTime;
        }

        DISALLOW_COPY_AND_ASSIGN(TreeBcastRpc);
    };

    /**
     * A k-nomial-tree which specifies the communication pattern of the
     * broadcast operation. Each node in the tree is assigned a rank starting
     * from 0.
     */
    struct KNomialTree {
        /// Construct a k-nomial tree of a specific size.
        explicit KNomialTree(int k, int nodes)
            : k(k), nodes(nodes)
        {}

        void getChildren(int rank, std::vector<int>* children);

        /// Fan-out factor of the tree.
        int k;

        /// # nodes in the tree.
        int nodes;
    };

    /// Context of the service that created this task.
    Context* context;

    /// All nodes that are participating in the broadcast operation, including
    /// the initial broadcaster.
    CommunicationGroup* group;

    /// Specifies the broadcast communication pattern.
    KNomialTree kNomialTree;

    /// Buffer used to store the processing result of #payloadRpc.
    Buffer localResult;

    /// Rank of the initial broadcaster within #group.
    const int root;

    /// Buffer used to store the broadcast RPC response. When #serviceRpc is
    /// NULL, it points to #serviceRpc->replyPayload, which is owned by the
    /// RPC/transport layer. Otherwise, it points to #responseStorage and the
    /// buffer is owned by this class.
    Buffer* outgoingResponse;

    /// Corresponding RPC response header of #outgoingResponse.
    WireFormat::TreeBcast::Response* respHdr;

    /// Storage to use for #outgoingResponse on root node. Empty otherwise.
    Tub<Buffer> responseStorage;

    // FIXME: use boost_intrusive_list?
    /// Each outgoing RPC is represented by a RpcWrapper class as opposed to
    /// TreeBcastRpc because all the outgoing RPCs share the same request
    /// message. We don't want the constructor of TreeBcastRpc to duplicate
    /// the work.
    using OutstandingRpcs = std::list<TreeBcastRpc>;

    /// Ongoing RPCs that are sending the broadcast request to all the direct
    /// children of this node.
    OutstandingRpcs outstandingRpcs;

    uint32_t payloadResponseHeaderLength;

    /// Request buffer for #payloadRpc.
    Buffer payloadRequest;

    /// Service RPC object corresponding to the embedded RPC request which is
    /// the payload of the broadcast message. Destroyed once its result is
    /// merged to #response.
    Tub<RpcWrapper> payloadRpc;

    /// Incoming RPC that results in the creation of this object. NULL when this
    /// object is created by invoking the broadcast API directly.
    Service::Rpc* serviceRpc;

    DISALLOW_COPY_AND_ASSIGN(TreeBcast);
};

}

#endif