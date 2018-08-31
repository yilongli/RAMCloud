#ifndef GRANULARCOMPUTING_ALLGATHER_H
#define GRANULARCOMPUTING_ALLGATHER_H

#include <cmath>

#include "Buffer.h"
#include "CommunicationGroup.h"
#include "RpcWrapper.h"
#include "RuleEngine.h"
#include "ServerIdRpcWrapper.h"
#include "Service.h"
#include "WireFormat.h"

namespace RAMCloud {

class AllGather {
  public:

    using RpcType = WireFormat::AllGather;

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
    template <typename Merger>
    explicit AllGather(int opId, Context* context, CommunicationGroup* group,
            size_t length, void* data, Merger* merger)
        : context(context)
        , group(group)
        , length(length)
        , data(data)
        , merger(*merger)
        , opId(opId)
        , phase(0)
        , maxPhase(-1)
        , outgoingRpc()
    {
        int n = group->size();
        if (n > 1) {
            maxPhase = int(std::log2(n - 1));
        }

        // FIXME: remove this limitation
        if (!n || (n & (n - 1))) {
            DIE("group size = %d is not a power of 2", n);
        }

        if (phase <= maxPhase) {
            outgoingRpc.construct(context, getPeerId(0), opId, phase,
                    group->rank, length, data);
        }
    }

    ~AllGather() = default;

    ServerId
    getPeerId(int phase)
    {
        int rank = group->rank >> phase;
        int d = (rank % 2) ? -1 : 1;
        return group->getNode(group->rank + d * (1 << phase));
    }

    void handleRpc(const WireFormat::AllGather::Request* reqHdr,
            WireFormat::AllGather::Response* respHdr, Service::Rpc* rpc);
    bool isReady();
    void wait();

  PRIVATE:
    class AllGatherRpc : public ServerIdRpcWrapper {
      public:
        explicit AllGatherRpc(Context* context, ServerId serverId,
                int opId, int phase, int senderId, size_t length,
                const void* data)
            : ServerIdRpcWrapper(context, serverId,
                sizeof(WireFormat::AllGather::Response))
        {
            appendRequest(&request, opId, phase, senderId, length, data);
            send();
        }

        static void
        appendRequest(Buffer* request, int opId, int phase,
                int senderId, size_t length, const void* data)
        {
            WireFormat::AllGather::Request* reqHdr(
                    allocHeader<WireFormat::AllGather>(request,
                    downCast<uint32_t>(opId)));
            reqHdr->phase = phase;
            reqHdr->senderId = senderId;
            request->appendExternal(data, (uint32_t)length);
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

    Context* context;

    CommunicationGroup* group;

    size_t length;

    void* data;

    std::function<std::pair<void*, size_t>(Buffer*)> merger;

    int opId;

    std::atomic<int> phase;

    int maxPhase;

    Tub<AllGatherRpc> outgoingRpc;

    DISALLOW_COPY_AND_ASSIGN(AllGather)
};

}

#endif