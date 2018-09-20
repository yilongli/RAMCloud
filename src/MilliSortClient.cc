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

#include "MilliSortClient.h"

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
        uint32_t nodesPerPivotServer, bool fromClient)
    : ServerIdRpcWrapper(context, serverId,
            sizeof(WireFormat::InitMilliSort::Response))
{
    appendRequest(&request, numNodes, dataTuplesPerServer, nodesPerPivotServer,
            fromClient);
    send();
}

void
InitMilliSortRpc::appendRequest(Buffer* request, uint32_t numNodes,
        uint32_t dataTuplesPerServer, uint32_t nodesPerPivotServer,
        bool fromClient)
{
    WireFormat::InitMilliSort::Request* reqHdr(
            RpcWrapper::allocHeader<WireFormat::InitMilliSort>(request));
    reqHdr->numNodes = numNodes;
    reqHdr->dataTuplesPerServer = dataTuplesPerServer;
    reqHdr->nodesPerPivotServer = nodesPerPivotServer;
    reqHdr->fromClient = fromClient;
}

void
MilliSortClient::startMilliSort(Context* context, ServerId serverId,
        bool fromClient)
{
    StartMilliSortRpc rpc(context, serverId, fromClient);
    rpc.wait();
}

StartMilliSortRpc::StartMilliSortRpc(Context* context, ServerId serverId,
        int requestId, bool fromClient)
    : ServerIdRpcWrapper(context, serverId,
            sizeof(WireFormat::StartMilliSort::Response))
{
    WireFormat::StartMilliSort::Request* reqHdr(
            allocHeader<WireFormat::StartMilliSort>());
    reqHdr->requestId = requestId;
    reqHdr->fromClient = fromClient;
    send();
}

void
StartMilliSortRpc::appendRequest(Buffer* request, int requestId,
        bool fromClient)
{
    WireFormat::StartMilliSort::Request* reqHdr(
            RpcWrapper::allocHeader<WireFormat::StartMilliSort>(request));
    reqHdr->requestId = requestId;
    reqHdr->fromClient = fromClient;
}

BenchmarkCollectiveOpRpc::BenchmarkCollectiveOpRpc(Context* context, int count,
        uint32_t opcode, uint32_t dataSize)
    : ServerIdRpcWrapper(context, ServerId(1, 0),
            sizeof(WireFormat::StartMilliSort::Response))
{
    appendRequest(&request, count, opcode, dataSize, 0, 0);
    send();
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

ShufflePullRpc::ShufflePullRpc(Context* context, ServerId serverId,
        int32_t senderId, uint32_t dataId, uint32_t dataSize, Buffer* response)
    : ServerIdRpcWrapper(context, serverId,
            sizeof(WireFormat::ShufflePull::Response), response)
{
    appendRequest(&request, senderId, dataId, dataSize);
    send();
}

void
ShufflePullRpc::appendRequest(Buffer* request, int32_t senderId,
        uint32_t dataId, uint32_t dataSize)
{
    WireFormat::ShufflePull::Request* reqHdr(
            allocHeader<WireFormat::ShufflePull>(request));
    reqHdr->senderId = senderId;
    reqHdr->dataId = dataId;
    reqHdr->dataSize = dataSize;
}

/// \copydoc ServerIdRpcWrapper::waitAndCheckErrors
Buffer*
ShufflePullRpc::wait()
{
    waitAndCheckErrors();
    response->truncateFront(responseHeaderLength);
    return response;
}

}  // namespace RAMCloud
