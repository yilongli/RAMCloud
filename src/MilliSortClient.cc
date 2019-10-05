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

#include "ClientException.h"
#include "MilliSortClient.h"
#include "Util.h"

namespace RAMCloud {

void
MilliSortClient::initMilliSort(Context* context, ServerId serverId,
        uint32_t numNodes, uint32_t dataTuplesPerServer,
        uint32_t nodesPerPivotServer, bool fromClient)
{
    InitMilliSortRpc rpc(context, serverId, numNodes, dataTuplesPerServer,
            nodesPerPivotServer, fromClient);
    rpc.wait();
}

InitMilliSortRpc::InitMilliSortRpc(Context* context, ServerId serverId,
        uint32_t numNodes, uint32_t dataTuplesPerServer,
        uint32_t nodesPerPivotServer, bool flushCache, bool fromClient)
    : ServerIdRpcWrapper(context, serverId,
            sizeof(WireFormat::InitMilliSort::Response))
{
    uint32_t id = Util::wyhash64();
    appendRequest(&request, id, numNodes, dataTuplesPerServer,
            nodesPerPivotServer, flushCache, fromClient);
    send();
}

void
InitMilliSortRpc::appendRequest(Buffer* request, uint32_t id, uint32_t numNodes,
        uint32_t dataTuplesPerServer, uint32_t nodesPerPivotServer,
        bool flushCache, bool fromClient)
{
    WireFormat::InitMilliSort::Request* reqHdr(
            RpcWrapper::allocHeader<WireFormat::InitMilliSort>(request));
    reqHdr->id = id;
    reqHdr->numNodes = numNodes;
    reqHdr->dataTuplesPerServer = dataTuplesPerServer;
    reqHdr->nodesPerPivotServer = nodesPerPivotServer;
    reqHdr->flushCache = flushCache;
    reqHdr->fromClient = fromClient;
}

void
MilliSortClient::startMilliSort(Context* context, ServerId serverId,
        int requestId, uint64_t startTime, bool fromClient)
{
    StartMilliSortRpc rpc(context, serverId, requestId, startTime, fromClient);
    rpc.wait();
}

StartMilliSortRpc::StartMilliSortRpc(Context* context, ServerId serverId,
        int requestId, uint64_t startTime, bool fromClient)
    : ServerIdRpcWrapper(context, serverId,
            sizeof(WireFormat::StartMilliSort::Response))
{
    WireFormat::StartMilliSort::Request* reqHdr(
            allocHeader<WireFormat::StartMilliSort>());
    reqHdr->requestId = requestId;
    reqHdr->startTime = startTime;
    reqHdr->fromClient = fromClient;
    send();
}

void
StartMilliSortRpc::appendRequest(Buffer* request, int requestId,
        uint64_t startTime, bool fromClient)
{
    WireFormat::StartMilliSort::Request* reqHdr(
            RpcWrapper::allocHeader<WireFormat::StartMilliSort>(request));
    reqHdr->requestId = requestId;
    reqHdr->startTime = startTime;
    reqHdr->fromClient = fromClient;
}

BenchmarkCollectiveOpRpc::BenchmarkCollectiveOpRpc(Context* context, int count,
        uint32_t opcode, uint32_t dataSize, uint64_t masterId,
        uint64_t startTime)
    : ServerIdRpcWrapper(context, ServerId(1, 0),
            sizeof(WireFormat::StartMilliSort::Response))
{
    appendRequest(&request, count, opcode, dataSize, masterId, startTime);
    send();
}

void
BenchmarkCollectiveOpRpc::appendRequest(Buffer* request, int count,
        uint32_t opcode, uint32_t dataSize, uint64_t masterId,
        uint64_t startTime)
{
    WireFormat::BenchmarkCollectiveOp::Request* reqHdr(
            allocHeader<WireFormat::BenchmarkCollectiveOp>(request));
    reqHdr->count = count;
    reqHdr->opcode = opcode;
    reqHdr->dataSize = dataSize;
    reqHdr->masterId = masterId;
    reqHdr->startTime = startTime;
}

SendDataRpc::SendDataRpc(Context* context, ServerId serverId, uint32_t dataId,
        uint32_t length, const void* data)
    : ServerIdRpcWrapper(context, serverId,
            sizeof(WireFormat::SendData::Response))
{
    appendRequest(&request, dataId, length, data);
    send();
}

void
SendDataRpc::appendRequest(Buffer* request, uint32_t dataId, uint32_t length,
        const void* data)
{
    WireFormat::SendData::Request* reqHdr(
            allocHeader<WireFormat::SendData>(request));
    reqHdr->dataId = dataId;
    request->appendExternal(data, length);
}

/// \copydoc ServerIdRpcWrapper::waitAndCheckErrors
Buffer*
SendDataRpc::wait()
{
    waitAndCheckErrors();
    response->truncateFront(responseHeaderLength);
    return response;
}

ShufflePushRpc::ShufflePushRpc(Context* context, Transport::SessionRef session,
        int32_t senderId, uint32_t dataId, uint32_t totalLength,
        uint32_t offset, Buffer::Iterator* payload)
    : RpcWrapper(responseHeaderLength)
    , context(context)
    , rpcId(Util::wyhash64())
{
    appendRequest(&request, senderId, dataId, totalLength, offset, payload,
            rpcId);
    this->session = session;
    send();
}

void
ShufflePushRpc::appendRequest(Buffer* request, int32_t senderId,
        uint32_t dataId, uint32_t totalLength, uint32_t offset,
        Buffer::Iterator* payload, uint32_t rpcId)
{
    WireFormat::ShufflePush::Request* reqHdr(
            allocHeader<WireFormat::ShufflePush>(request));
    reqHdr->senderId = senderId;
    reqHdr->dataId = dataId;
    reqHdr->totalLength = totalLength;
    reqHdr->offset = offset;
    reqHdr->len = payload->size();
    reqHdr->rpcId = rpcId;
    while (!payload->isDone()) {
        request->appendExternal(payload->getData(), payload->getLength());
        payload->next();
    }
}

void
ShufflePushRpc::wait()
{
    waitInternal(context->dispatch);
    if (getState() != RpcState::FINISHED) {
        throw TransportException(HERE);
    }

    const WireFormat::ShufflePush::Response* respHdr(
            getResponseHeader<WireFormat::ShufflePush>());

    if (respHdr->common.status != STATUS_OK)
        ClientException::throwException(HERE, respHdr->common.status);
    response->truncateFront(responseHeaderLength);
}

}  // namespace RAMCloud
