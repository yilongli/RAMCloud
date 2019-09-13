/* Copyright (c) 2010-2016 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#ifndef RAMCLOUD_MILLISORTCLIENT_H
#define RAMCLOUD_MILLISORTCLIENT_H

#include "Context.h"
#include "ServerIdRpcWrapper.h"

namespace RAMCloud {

/**
 * Provides methods for invoking RPCs to RAMCloud masters.  The invoking
 * machine is typically another RAMCloud server (either master or backup)
 * or the cluster coordinator; these methods are not normally used by
 * RAMCloud applications. The class contains only static methods, so you
 * shouldn't ever need to instantiate an object.
 */
class MilliSortClient {
  public:
    static void initMilliSort(Context* context, ServerId serverId,
            uint32_t numNodes, uint32_t dataTuplesPerServer,
            uint32_t nodesPerPivotServer, bool fromClient = true);
    static void startMilliSort(Context* context, ServerId serverId,
            int requestId, uint64_t startTime, bool fromClient = true);

  private:
    MilliSortClient();
};

/**
 * Encapsulates the state of a MilliSortClient::initMilliSort request,
 * allowing it to execute asynchronously.
 */
class InitMilliSortRpc : public ServerIdRpcWrapper {
  public:
    InitMilliSortRpc(Context* context, ServerId serverId, uint32_t numNodes,
            uint32_t dataTuplesPerServer, uint32_t nodesPerPivotServer,
            bool fromClient = true);
    ~InitMilliSortRpc() {}

    static void appendRequest(Buffer* request, uint32_t id, uint32_t numNodes,
            uint32_t dataTuplesPerServer, uint32_t nodesPerPivotServer,
            bool fromClient);

    /// \copydoc ServerIdRpcWrapper::waitAndCheckErrors
    WireFormat::InitMilliSort::Response*
    wait()
    {
        waitAndCheckErrors();
        return response->getStart<WireFormat::InitMilliSort::Response>();
    }

    static const uint32_t responseHeaderLength =
            sizeof(WireFormat::InitMilliSort::Response);

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(InitMilliSortRpc);
};

/**
 * Encapsulates the state of a MilliSortClient::startMilliSort request,
 * allowing it to execute asynchronously.
 */
class StartMilliSortRpc : public ServerIdRpcWrapper {
  public:
    StartMilliSortRpc(Context* context, ServerId serverId, int requestId,
            uint64_t startTime = 0, bool fromClient = true);
    ~StartMilliSortRpc() {}

    static void appendRequest(Buffer* request, int requestId,
            uint64_t startTime, bool fromClient);

    /// \copydoc ServerIdRpcWrapper::waitAndCheckErrors
    void wait() {waitAndCheckErrors();}

    static const uint32_t responseHeaderLength =
            sizeof(WireFormat::StartMilliSort::Response);

PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(StartMilliSortRpc);
};

/**
 * Encapsulates the state of a MilliSortClient::startMilliSort request,
 * allowing it to execute asynchronously.
 */
class BenchmarkCollectiveOpRpc : public ServerIdRpcWrapper {
  public:
    BenchmarkCollectiveOpRpc(Context* context, int count, uint32_t opcode,
            uint32_t dataSize, uint64_t masterId = 0, uint64_t startTime = 0);
    ~BenchmarkCollectiveOpRpc() {}
    static void appendRequest(Buffer* request, int count, uint32_t opcode,
            uint32_t dataSize, uint64_t masterId, uint64_t startTime);

    // TODO: document this method and remove the above wait method?
    Buffer*
    wait()
    {
        waitAndCheckErrors();
        response->truncateFront(
                sizeof32(WireFormat::BenchmarkCollectiveOp::Response));
        return response;
    }

    static const uint32_t responseHeaderLength =
            sizeof(WireFormat::BenchmarkCollectiveOp::Response);

PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(BenchmarkCollectiveOpRpc);
};

/**
 * Encapsulates the state of a MilliSortClient::startMilliSort request,
 * allowing it to execute asynchronously.
 */
class SendDataRpc : public ServerIdRpcWrapper {
  public:
    SendDataRpc(Context* context, ServerId serverId, uint32_t dataId,
            uint32_t length, const void* data);
    ~SendDataRpc() {}

    static void appendRequest(Buffer* request, uint32_t dataId, uint32_t length,
            const void* data);
    Buffer* wait();

    static const uint32_t responseHeaderLength =
            sizeof(WireFormat::SendData::Response);

PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(SendDataRpc);
};

class ShufflePushRpc : public ServerIdRpcWrapper {
  public:
    ShufflePushRpc(Context* context, ServerId serverId, int32_t senderId,
            uint32_t dataId, uint32_t totalLength, uint32_t offset,
            Buffer::Iterator* payload);
    ~ShufflePushRpc() {}

    static void appendRequest(Buffer* request, int32_t senderId,
            uint32_t dataId, uint32_t totalLength, uint32_t offset,
            Buffer::Iterator* payload, uint32_t rpcId);

    void wait() { waitAndCheckErrors(); }

    static const uint32_t responseHeaderLength =
            sizeof(WireFormat::ShufflePush::Response);

    // FIXME: Debug only; remove?
    uint32_t rpcId;

PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(ShufflePushRpc);
};



} // namespace RAMCloud

#endif // RAMCLOUD_MILLISORTCLIENT_H
