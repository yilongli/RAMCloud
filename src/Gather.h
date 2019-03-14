#ifndef GRANULARCOMPUTING_GATHER_H
#define GRANULARCOMPUTING_GATHER_H

#include <unordered_set>

#include "Buffer.h"
#include "CommunicationGroup.h"
#include "RpcWrapper.h"
#include "ServerIdRpcWrapper.h"
#include "Service.h"
#include "WireFormat.h"

namespace RAMCloud {

class TreeGather {
  public:

    using RpcType = WireFormat::TreeGather;

    class Merger {
      public:
        virtual void add(Buffer* buffer) = 0;
        virtual Buffer* getResult() = 0;
    };

    // TODO: this is the data-transfer version of the API. Not sure if we need
    // the more generic reduce-like API right now. Actually, unlike bcast, there
    // is no RPC involved because any processing on local data can be simply
    // performed before calling the ctor

    template <typename T>
    explicit TreeGather(int opId, Context* context, CommunicationGroup* group,
            int root, uint32_t numElements, const T* elements, Merger* merger)
        : TreeGather(opId, context, group, root, numElements * sizeof32(T),
                static_cast<const void*>(elements), merger)
    {}

    explicit TreeGather(int opId, Context* context, CommunicationGroup* group,
            int root, uint32_t numBytes, const void* data, Merger* merger);
    ~TreeGather() = default;

    bool isReady();

    void handleRpc(const WireFormat::TreeGather::Request* reqHdr,
            WireFormat::TreeGather::Response* respHdr, Service::Rpc* rpc);

    void wait();

  PRIVATE:
    class TreeGatherRpc : public ServerIdRpcWrapper {
      public:
        explicit TreeGatherRpc(Context* context, ServerId serverId,
                int opId, int senderId, Buffer* data)
            : ServerIdRpcWrapper(context, serverId,
                sizeof(WireFormat::TreeGather::Response))
        {
            WireFormat::TreeGather::Request* reqHdr(
                    allocHeader<WireFormat::TreeGather>(downCast<uint32_t>(opId)));
            reqHdr->senderId = downCast<uint32_t>(senderId);
            request.appendExternal(data);
            send();
        }

        DISALLOW_COPY_AND_ASSIGN(TreeGatherRpc);
    };
    
    /**
     * A k-nomial-tree which specifies the communication pattern of the
     * gather operation. Each node in the tree is assigned a rank starting
     * from 0.
     */
    struct KNomialTree {
        /// Construct a k-nomial tree of a specific size.
        explicit KNomialTree(int k, int nodes)
            : k(k), nodes(nodes)
        {}

        void getChildren(int parent, std::vector<int>* children);
        int getParent(int child);

        /// Fan-out factor of the tree.
        int k;

        /// # nodes in the tree.
        int nodes;
    };

    /// Context of the service that created this task.
    Context* context;

    /// All nodes that are participating in the gather operation.
    CommunicationGroup* group;

    Merger* merger;

    /// Unique identifier of the gather operation.
    int opId;

    /// Rank of the final gatherer within #group.
    int root;


    int relativeRank;

    /// Holds child nodes from which we still expect to receive data. 
    /// Only present on interior nodes (i.e., non-leaf nodes).
    Tub<std::unordered_set<int>> nodesToGather;

    /// Specifies the broadcast communication pattern.
    KNomialTree kNomialTree;

    /// Used to make sure that only one thread at a time attempts to access
    /// #nodesToGather.
    // FIXME: and ensures sequential access of the merger?
    Arachne::SpinLock mutex;

    typedef std::lock_guard<Arachne::SpinLock> Lock;

    /// RPC used to transfer data to the root node.
    Tub<TreeGatherRpc> sendData;

    DISALLOW_COPY_AND_ASSIGN(TreeGather)
};

}

#endif