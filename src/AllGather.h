#ifndef GRANULARCOMPUTING_ALLGATHER_H
#define GRANULARCOMPUTING_ALLGATHER_H

#include <cmath>
#include <list>

#include "Buffer.h"
#include "CommunicationGroup.h"
#include "RpcWrapper.h"
#include "RuleEngine.h"
#include "ServerIdRpcWrapper.h"
#include "Service.h"
#include "WireFormat.h"

namespace RAMCloud {

/// Note: right now, I implement the recursive doubling algorithm with an extension
/// to handle non-power-of-2 nodes by casting the original group to a smaller
/// group with the nearest power-of-two nodes. Alternatively, we might expand
/// the original group to a larger group with the next power-of-two number of nodes?
/// Also, what about the factorization algorithm described in "Generalisation of
/// recursive doubling for AllReduce"? e.g., for a group of 6, we can do an allgather
/// inside groups of 3 and then another inside groups of 2. It seems that an ideal
/// algorithm will have to take all the factors (e.g., message size, latency, group
/// sizes) into consideration and dynamically picking the size of the sub-allgather-group
/// at each stage?
/// BTW, I didn't implement, say, recursive-tripling because the power-of-three
/// numbers are even sparser (i.e., 3, 9, 27, 81, etc.) and the naive strategy
/// of casting to a smaller group won't work well I think.
class AllGather {
  public:

    using RpcType = WireFormat::AllGather;

    // FIXME: thread-unsafe for now (not necessary for recursive doubling
    // algorithm)
    class Merger {
      public:
        /// Append new data to existing data. This method is guaranteed not to
        /// modify existing data.
        virtual void append(Buffer* incomingData) = 0;
        // Quick hack: only used to implement the final expansion step.
        virtual void clear() = 0;
        virtual Buffer* getResult() = 0;
    };

    explicit AllGather(int opId, Context* context, CommunicationGroup* group,
            uint32_t length, void* data, Merger* merger);
    ~AllGather() = default;
    void handleRpc(const WireFormat::AllGather::Request* reqHdr,
            WireFormat::AllGather::Response* respHdr, Service::Rpc* rpc);
    bool isReady();
    void wait();

  PRIVATE:
    class AllGatherRpc : public ServerIdRpcWrapper {
      public:
        explicit AllGatherRpc(Context* context, ServerId serverId,
                int opId, int phase, int senderId, Buffer* data)
            : ServerIdRpcWrapper(context, serverId,
                sizeof(WireFormat::AllGather::Response))
        {
            appendRequest(&request, opId, phase, senderId, data);
            send();
        }

        static void
        appendRequest(Buffer* request, int opId, int phase, int senderId,
                Buffer* data)
        {
            WireFormat::AllGather::Request* reqHdr(
                    allocHeader<WireFormat::AllGather>(request,
                    downCast<uint32_t>(opId)));
            reqHdr->phase = phase;
            reqHdr->senderId = senderId;
            request->appendExternal(data);
        }

        /// \copydoc ServerIdRpcWrapper::waitAndCheckErrors
        void wait()
        {
            waitAndCheckErrors();
            // Chop off the AllGather response header.
            response->truncateFront(responseHeaderLength);
        }

        static const uint32_t responseHeaderLength =
                sizeof(WireFormat::AllGather::Response);

        DISALLOW_COPY_AND_ASSIGN(AllGatherRpc);
    };

    void checkOutgoingRpcs(bool waitOnBlock = false);
    void getPeersToSend(int phase, std::vector<int>* targets);

    Context* context;

    /// True means the all-gather operation is completed on the local node.
    std::atomic<bool> complete;

    /// Current stage of the algorithm. Possible values are:
    ///   0: initial contraction step; only present if the group size is not a
    /// power of two
    ///   1..log2(N): pair-wise exchange steps
    ///   log2(N)+1: final expansion step; only present if the group size is not
    /// a power of two
    /// where N is # nodes in the communication group.
    ///
    /// This is the major synchronization mechanism for #handleRpc:
    /// it guarantees that RPCs from different phases are processed in order.
    /// This above is sufficient to prevent data-race because there is at most
    /// one RPC from each phase in the recursive doubling algorithm.
    std::atomic<int> currentPhase;

    int lastPhase;

    bool expanderNode;

    bool ghostNode;

    CommunicationGroup* group;

    Merger* merger;

    /// Unique identifier of the all-gather operation.
    int opId;

    /// Largest power of two that is smaller than or equal to the group size.
    /// This is the effective group size used in the recursive doubling phases.
    int nearestPowerOfTwo;

    /// Number of pairwise exchange steps need to be performed.
    int numRecursiveDoublingSteps;

    // TODO: intrusive list?
    using OutstandingRpcs = std::list<AllGatherRpc>;

    /// Ongoing RPCs that are sending data to peer nodes.
    OutstandingRpcs outstandingRpcs;

    DISALLOW_COPY_AND_ASSIGN(AllGather)
};

}

#endif