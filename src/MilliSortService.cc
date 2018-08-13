/* Copyright (c) 2011-2016 Stanford University
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
#include "MilliSortService.h"
#include "AllGather.h"
#include "AllShuffle.h"
#include "Broadcast.h"
#include "Gather.h"
#include "TimeTrace.h"

namespace RAMCloud {

// Change 0 -> 1 in the following line to compile detailed time tracing in
// this transport.
#define TIME_TRACE 1

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

// Change 0 -> 1 in the following line to log intermediate computation result at
// each stage of the algorithm.
#define LOG_INTERMEDIATE_RESULT 0

/**
 * Construct an MilliSortService.
 *
 * \param context
 *      Overall information about the RAMCloud server or client. The new
 *      service will be registered in this context.
 * \param config
 *      Contains various parameters that configure the operation of
 *      this server.
 */
MilliSortService::MilliSortService(Context* context,
        const ServerConfig* serverConfig)
    : context(context)
    , serverConfig(serverConfig)
    , communicationGroupTable()
    , ongoingMilliSort()
    , startTime()
    , endTime()
    , pivotServerRank(-1)
    , isPivotServer()
    , numDataTuples(-1)
    , numPivotServers(-1)
    , keys()
    , sortedKeys()
    , localPivots()
    , dataBucketBoundaries()
    , world()
    , myPivotServerGroup()
    , allGatherPeersGroup()
    , gatheredPivots()
    , superPivots()
    , pivotBucketBoundaries()
    , sortedGatheredPivots()
    , partialDataBucketBoundaries()
    , numSmallerPivots()
    , numPivotsInTotal()
    , allPivotServers()
    , globalSuperPivots()
{
    context->services[WireFormat::MILLISORT_SERVICE] = this;
}

MilliSortService::~MilliSortService()
{
    context->services[WireFormat::MILLISORT_SERVICE] = NULL;
}

/**
 * Dispatch an RPC to the right handler based on its opcode.
 */
void
MilliSortService::dispatch(WireFormat::Opcode opcode, Rpc* rpc)
{
    switch (opcode) {
        case WireFormat::InitMilliSort::opcode:
            callHandler<WireFormat::InitMilliSort, MilliSortService,
                        &MilliSortService::initMilliSort>(rpc);
            break;
        case WireFormat::StartMilliSort::opcode:
            callHandler<WireFormat::StartMilliSort, MilliSortService,
                        &MilliSortService::startMilliSort>(rpc);
            break;
        case WireFormat::TreeBcast::opcode:
            callHandler<WireFormat::TreeBcast, MilliSortService,
                        &MilliSortService::treeBcast>(rpc);
            break;
        case WireFormat::FlatGather::opcode:
            callHandler<WireFormat::FlatGather, MilliSortService,
                        &MilliSortService::flatGather>(rpc);
            break;
        case WireFormat::AllGather::opcode:
            callHandler<WireFormat::AllGather, MilliSortService,
                        &MilliSortService::allGather>(rpc);
            break;
        case WireFormat::AllShuffle::opcode:
            callHandler<WireFormat::AllShuffle, MilliSortService,
                        &MilliSortService::allShuffle>(rpc);
            break;
        case WireFormat::SendData::opcode:
            callHandler<WireFormat::SendData, MilliSortService,
                        &MilliSortService::sendData>(rpc);
            break;
        default:
            prepareErrorResponse(rpc->replyPayload, 
                    STATUS_UNIMPLEMENTED_REQUEST);
    }
}


/**
 * Top-level server method to handle the INIT_MILLISORT request.
 *
 * \copydetails Service::ping
 */
void
MilliSortService::initMilliSort(const WireFormat::InitMilliSort::Request* reqHdr,
        WireFormat::InitMilliSort::Response* respHdr, Rpc* rpc)
{
    timeTrace("initMilliSort invoked, dataTuplesPerServer %u, "
              "nodesPerPivotServer %u, fromClient %u",
              reqHdr->dataTuplesPerServer, reqHdr->nodesPerPivotServer,
              reqHdr->fromClient);

    // We are busy serving another sorting request.
    if (ongoingMilliSort != NULL) {
        respHdr->common.status = STATUS_SORTING_IN_PROGRESS;
        return;
    }

    initWorld();

    // Initialization request from an external client should only be processed
    // by the root node, which would then broadcast the request to all nodes.
    if (reqHdr->fromClient) {
        if (world->rank > 0) {
            respHdr->common.status = STATUS_MILLISORT_ERROR;
            return;
        }

        // TODO: the following two limitations are due to our impl. of allgather
        int nodesPerPivotServer = reqHdr->nodesPerPivotServer;
        if (world->size() % nodesPerPivotServer != 0) {
            LOG(ERROR, "illegal nodesPerPivotServer value %d: cannot divide "
                    "total number of nodes %d", nodesPerPivotServer,
                    world->size());
            respHdr->common.status = STATUS_MILLISORT_ERROR;
            return;
        }
        int numPivotServers = world->size() / nodesPerPivotServer;
        if ((numPivotServers & (numPivotServers - 1)) != 0) {
            LOG(ERROR, "illegal nodesPerPivotServer value %d; # pivot servers "
                    "= %d is not a power of 2", nodesPerPivotServer,
                    numPivotServers);
            respHdr->common.status = STATUS_MILLISORT_ERROR;
            return;
        }

        TreeBcast treeBcast(context, world.get());
        treeBcast.send<InitMilliSortRpc>(reqHdr->dataTuplesPerServer,
                reqHdr->nodesPerPivotServer, false);
        treeBcast.wait();

        Buffer* result = treeBcast.getResult();
        uint32_t offset = 0;
        while (offset < result->size()) {
            respHdr->numNodesInited +=
                    result->read<WireFormat::InitMilliSort::Response>(
                    &offset)->numNodesInited;
        }
        return;
    }

    numDataTuples = reqHdr->dataTuplesPerServer;
    int nodesPerPivotServer = (int) reqHdr->nodesPerPivotServer;
    pivotServerRank = world->rank / nodesPerPivotServer *
            nodesPerPivotServer;
    isPivotServer = (pivotServerRank == world->rank);
    numPivotServers = world->size() / nodesPerPivotServer +
            (world->size() % nodesPerPivotServer > 0 ? 1 : 0);

    // Construct communication sub-groups.
    communicationGroupTable.clear();
    registerCommunicationGroup(world.get());
    vector<ServerId> nodes;
    for (int i = 0; i < nodesPerPivotServer; i++) {
        if (pivotServerRank + i >= world->size()) {
            break;
        }
        nodes.push_back(world->getNode(pivotServerRank + i));
    }
    myPivotServerGroup.construct(MY_PIVOT_SERVER_GROUP,
            world->rank % nodesPerPivotServer, nodes);
    registerCommunicationGroup(myPivotServerGroup.get());
    if (isPivotServer) {
        nodes.clear();
        for (int i = 0; i < numPivotServers; i++) {
            nodes.push_back(world->getNode(i * nodesPerPivotServer));
        }
        allPivotServers.construct(ALL_PIVOT_SERVERS,
                world->rank / nodesPerPivotServer, nodes);
        registerCommunicationGroup(allPivotServers.get());
    } else {
        allPivotServers.destroy();
    }

    nodes.clear();
    int peerRank = world->rank % nodesPerPivotServer;
    for (int i = 0; i < numPivotServers; i++) {
        nodes.push_back(world->getNode(i * nodesPerPivotServer + peerRank));
    }
    allGatherPeersGroup.construct(ALL_GATHER_PEERS_GROUP,
            world->rank / nodesPerPivotServer, nodes);
//    for (auto nn : nodes) {
//        LOG(WARNING, "peer server %u", nn.indexNumber());
//    }

    // Generate data tuples.
    keys.clear();
    for (int i = 0; i < numDataTuples; i++) {
        // TODO: generate random keys
//        keys.push_back(rand() % 10000);
        keys.push_back(world->rank + i * world->size());
    }
    sortedKeys.clear();

    //
    localPivots.clear();
    dataBucketBoundaries.clear();
    gatheredPivots.clear();
    superPivots.clear();
    pivotBucketBoundaries.clear();
    sortedGatheredPivots.clear();
    partialDataBucketBoundaries.clear();
    numSmallerPivots = 0;
    numPivotsInTotal = 0;
    globalSuperPivots.clear();

    //
    respHdr->numNodesInited = 1;
}

/**
 * Top-level server method to handle the START_MILLISORT request.
 *
 * \copydetails Service::ping
 */
void
MilliSortService::startMilliSort(const WireFormat::StartMilliSort::Request* reqHdr,
        WireFormat::StartMilliSort::Response* respHdr, Rpc* rpc)
{
    startTime = Cycles::rdtsc();
    timeTrace("startMilliSort invoked, requestId %u, fromClient %u",
            uint(reqHdr->requestId), reqHdr->fromClient);

    if (ongoingMilliSort) {
        respHdr->common.status = STATUS_SORTING_IN_PROGRESS;
        return;
    }

    // Start request from external client should only be processed by the root
    // node, which would then broadcast the start request to all nodes.
    if (reqHdr->fromClient) {
        if (world->rank > 0) {
            respHdr->common.status = STATUS_MILLISORT_ERROR;
            return;
        }

        // Start broadcasting the start request.
        TreeBcast startBcast(context, world.get());
        startBcast.send<StartMilliSortRpc>(reqHdr->requestId, false);

        // FIXME: I am throwing away an entire worker thread here; should I use
        // sleep? Arachne?
//        while (!startBcast.isReady()) {
//            yield();
//        }
        startBcast.wait();
        return;
    }

    // Kick start the sorting process.
    ongoingMilliSort = rpc;
    localSortAndPickPivots();
    while (ongoingMilliSort != NULL) {
        // Arachne wait on condition var.
    }
}

void
MilliSortService::inplaceMerge(std::vector<SortKey>& keys,
        size_t sizeOfFirstSortedRange)
{
    auto middle = keys.begin();
    std::advance(middle, sizeOfFirstSortedRange);
    std::inplace_merge(keys.begin(), middle, keys.end());
}

void
MilliSortService::sort(std::vector<SortKey>& keys)
{
    if (!std::is_sorted(keys.begin(), keys.end())) {
        std::sort(keys.begin(), keys.end());
    }
}

void
MilliSortService::partition(std::vector<SortKey>* keys, int numPartitions,
        std::vector<SortKey>* pivots)
{
    assert(pivots->empty());
    // Let P be the number of partitions. Pick P pivots that divide #keys into
    // P partitions as evenly as possible. Since the sizes of any two partitions
    // differ by at most one, we have:
    //      s * k + (s + 1) * (P - k) = numKeys
    // where s is the number of keys in smaller partitions and k is the number
    // of smaller partitions.
    int numKeys = downCast<int>(keys->size());
    int s = numKeys / numPartitions;
    int k = numPartitions * (s + 1) - numKeys;
    int pivotIdx = -1;
    for (int i = 0; i < numPartitions; i++) {
        if (i < k) {
            pivotIdx += s;
        } else {
            pivotIdx += s + 1;
        }
        pivots->push_back((*keys)[pivotIdx]);
    }
}

void
MilliSortService::localSortAndPickPivots()
{
    timeTrace("=== Stage 1: localSortAndPickPivots started");

    {
        CycleCounter<> _(&PerfStats::threadStats.localSortLatency);
        sort(keys);
    }

    partition(&keys, std::min((int)keys.size(), world->size()), &localPivots);
    debugLogKeys("local pivots: ", &localPivots);

    // Merge pivots from slave nodes as they arrive.
    auto merger = [this] (Buffer* pivotsBuffer) {
            CycleCounter<> _(&PerfStats::threadStats.mergePivotsCycles);
            uint32_t offset = 0;
            size_t oldSize = gatheredPivots.size();
            while (offset < pivotsBuffer->size()) {
                gatheredPivots.push_back(*pivotsBuffer->read<SortKey>(&offset));
            }
            inplaceMerge(gatheredPivots, oldSize);
            timeTrace("merged %u pivots", pivotsBuffer->size() / KeySize);
    };

    // Initiate the gather op that collects pivots to the pivot server.
    timeTrace("sorted %u data tuples, picked %u local pivots, about to gather "
            "pivots", keys.size(), localPivots.size());
    {
        CycleCounter<> _(&PerfStats::threadStats.gatherPivotsCycles);
        Tub<FlatGather> gatherPivots;
        invokeCollectiveOp<FlatGather>(gatherPivots, GATHER_PIVOTS, context,
                myPivotServerGroup.get(), 0, (uint32_t) localPivots.size(),
                localPivots.data(), &merger);
        gatherPivots->wait();
        // TODO: the cleanup is not very nice; how to do RAII?
        removeCollectiveOp(GATHER_PIVOTS);

        // FIXME: seems wrong division of responsibility; should I move this
        // into Gather class? Also this assumes a particular implementation of
        // Gather.
        PerfStats::threadStats.gatherPivotsOutputBytes +=
                localPivots.size() * KeySize;
    }

    // Pivot servers will advance to pick super pivots when the gather op
    // completes. Normal nodes will wait to receive data bucket boundaries.
    if (isPivotServer) {
        timeTrace("gathered %u pivots from %u nodes", gatheredPivots.size(),
                uint(myPivotServerGroup->size()));
        pickSuperPivots();
    } else {
        timeTrace("sent %u local pivots", localPivots.size());
    }
}

void
MilliSortService::pickSuperPivots()
{
    timeTrace("=== Stage 2: pickSuperPivots started");

    assert(isPivotServer);
    partition(&gatheredPivots, allPivotServers->size(), &superPivots);
    debugLogKeys("super-pivots: ", &superPivots);

    // Merge super pivots from pivot servers as they arrive.
    auto merger = [this] (Buffer* pivotsBuf) {
            CycleCounter<> _(&PerfStats::threadStats.mergeSuperPivotsCycles);
            uint32_t offset = 0;
            size_t oldSize = globalSuperPivots.size();
            while (offset < pivotsBuf->size()) {
                globalSuperPivots.push_back(*pivotsBuf->read<SortKey>(&offset));
            }
            inplaceMerge(globalSuperPivots, oldSize);
            timeTrace("merged %u super-pivots", pivotsBuf->size() / KeySize);
    };

    // Initiate the gather op that collects super pivots to the root node.
    timeTrace("picked %u super-pivots, about to gather super-pivots",
            superPivots.size());
    {
        CycleCounter<> _(&PerfStats::threadStats.gatherSuperPivotsCycles);
        Tub<FlatGather> gatherSuperPivots;
        invokeCollectiveOp<FlatGather>(gatherSuperPivots, GATHER_SUPER_PIVOTS,
                context, allPivotServers.get(), 0,
                (uint32_t) superPivots.size(), superPivots.data(), &merger);
        // fixme: this is duplicated everywhere
        gatherSuperPivots->wait();
        removeCollectiveOp(GATHER_SUPER_PIVOTS);
    }

    // The root node will advance to pick pivot bucket boundaries when the
    // gather op completes. Other pivot servers will wait to receive pivot
    // bucket boundaries.
    if (world->rank == 0) {
        timeTrace("gathered %u super-pivots from %u pivot servers",
                globalSuperPivots.size(), uint(allPivotServers->size()));
        pickPivotBucketBoundaries();
    } else {
        timeTrace("sent %u super-pivots", superPivots.size());
    }
}

void
MilliSortService::pickPivotBucketBoundaries()
{
    timeTrace("=== Stage 3: pickPivotBucketBoundaries started");
    assert(world->rank == 0);
    partition(&globalSuperPivots, allPivotServers->size(),
            &pivotBucketBoundaries);
    debugLogKeys("pivot bucket boundaries: ", &pivotBucketBoundaries);

    // Broadcast computed pivot bucket boundaries to the other pivot servers
    // (excluding ourselves).
    // TODO: can we unify the API of broadcast and other collective op?
    timeTrace("picked %u pivot bucket boundaries, about to broadcast",
            pivotBucketBoundaries.size());
    {
        CycleCounter<> _(&PerfStats::threadStats.bcastPivotBucketBoundariesCycles);
        TreeBcast bcast(context, allPivotServers.get());
        bcast.send<SendDataRpc>(BROADCAST_PIVOT_BUCKET_BOUNDARIES,
                // TODO: simplify the SendDataRpc api?
                downCast<uint32_t>(KeySize * pivotBucketBoundaries.size()),
                pivotBucketBoundaries.data());
        bcast.wait();
    }

    timeTrace("broadcast to %u pivot servers", uint(allPivotServers->size()));
    pivotBucketSort();
}

void
MilliSortService::pivotBucketSort()
{
    timeTrace("=== Stage 4: pivotBucketSort started");
    assert(isPivotServer);

    // Merge pivots from pivot servers as they arrive.
    auto merger = [this] (Buffer* data) {
        CycleCounter<> _(&PerfStats::threadStats.mergePivotsInBucketSortCycles);
        uint32_t offset = 0;
        size_t oldSize = sortedGatheredPivots.size();
        numSmallerPivots += *data->read<uint32_t>(&offset);
        numPivotsInTotal += *data->read<uint32_t>(&offset);
        while (offset < data->size()) {
            sortedGatheredPivots.push_back(*data->read<SortKey>(&offset));
        }

        // FIXME: this should really be an in-place parallel merge operation
        inplaceMerge(sortedGatheredPivots, oldSize);
        timeTrace("merged %u pivots", (data->size() - 8) / KeySize);
    };

    // Send pivots to their designated pivot servers determined by the pivot
    // bucket boundaries.
    timeTrace("about to shuffle pivots");
    {
        CycleCounter<> _(&PerfStats::threadStats.bucketSortPivotsCycles);
        Tub<AllShuffle> pivotsShuffle;
        invokeCollectiveOp<AllShuffle>(pivotsShuffle, ALLSHUFFLE_PIVOTS,
                context,
                allPivotServers.get(), &merger);
        uint32_t k = 0;
        for (int rank = 0; rank < int(pivotBucketBoundaries.size()); rank++) {
            // Prepare the data to send to node `rank`.
            uint32_t numSmallerPivots = k;
            uint32_t numPivots = downCast<uint32_t>(gatheredPivots.size());

            // The data message starts with two 32-bit numbers: the number of pivots
            // smaller than the current bucket and the total number of pivots this
            // pivot server has. They are used to compute the data bucket boundaries
            // in the next step.
            Buffer* buffer = pivotsShuffle->getSendBuffer(rank);
            buffer->appendCopy(&numSmallerPivots);
            buffer->appendCopy(&numPivots);

            SortKey boundary = pivotBucketBoundaries[rank];
            while ((k < gatheredPivots.size()) &&
                   (gatheredPivots[k] <= boundary)) {
                buffer->appendCopy(&gatheredPivots[k]);
                k++;
            }
            pivotsShuffle->closeSendBuffer(rank);
        }
        pivotsShuffle->wait();
        removeCollectiveOp(ALLSHUFFLE_PIVOTS);
        debugLogKeys("sorted pivots (local portion): ", &sortedGatheredPivots);
    }

    // Pivot servers will advance to pick data bucket boundaries when the
    // shuffle completes.
    timeTrace("finished pivot shuffle");
    pickDataBucketBoundaries();
}

void
MilliSortService::pickDataBucketBoundaries() {
    timeTrace("=== Stage 5: pickDataBucketBoundaries started");

    assert(isPivotServer);
    // TODO: doc. that it's copied from sortAndPartition and modified.
    int s = numPivotsInTotal / world->size();
    int k = world->size() * (s + 1) - numPivotsInTotal;
    int globalIdx = -1;
    for (int i = 0; i < world->size(); i++) {
        if (i < k) {
            globalIdx += s;
        } else {
            globalIdx += s + 1;
        }

        int localIdx = globalIdx - numSmallerPivots;
        if ((0 <= localIdx) && (localIdx < (int) sortedGatheredPivots.size())) {
            partialDataBucketBoundaries.push_back(
                    sortedGatheredPivots[localIdx]);
        }
    }
    debugLogKeys("data bucket boundaries (local portion): ",
            &partialDataBucketBoundaries);

    // Broadcast computed partial data bucket boundaries to the slave nodes.
    timeTrace("picked %u partial data bucket boundaries, about to broadcast to"
              "slave nodes", partialDataBucketBoundaries.size());
    {
//        CycleCounter<> _(&PerfStats::threadStats.?);
        TreeBcast bcast(context, myPivotServerGroup.get());
        bcast.send<SendDataRpc>(BROADCAST_DATA_BUCKET_BOUNDARIES,
                downCast<uint32_t>(
                        KeySize * partialDataBucketBoundaries.size()),
                partialDataBucketBoundaries.data());
        dataBucketBoundaries = partialDataBucketBoundaries;
        // FIXME: there is no need to wait here because the following all-gather
        // modify dataBucketBoundaries rather than partialDataBucketBoundaries.
        bcast.wait();
    }

    timeTrace("broadcast to %u slave nodes", uint(myPivotServerGroup->size()));
    allGatherDataBucketBoundaries();
}

void
MilliSortService::allGatherDataBucketBoundaries()
{
    timeTrace("=== Stage 6: allGatherDataBucketBoundaries started");

    // Merge data bucket boundaries from pivot servers as they arrive.
    auto merger = [this] (Buffer* pivotsBuffer) {
            CycleCounter<> _(&PerfStats::threadStats.allGatherPivotsMergeCycles);
            uint32_t offset = 0;
            size_t oldSize = dataBucketBoundaries.size();
            while (offset < pivotsBuffer->size()) {
                SortKey boundary = *pivotsBuffer->read<SortKey>(&offset);
                dataBucketBoundaries.push_back(boundary);
            }
            inplaceMerge(dataBucketBoundaries, oldSize);
            // TODO: shall we do the merge online? actually, even better, partialDataBucketBoundaries
            // are sorted globally, we know exactly where to insert even they come!
            timeTrace("merged %u data bucket boundaries",
                    pivotsBuffer->size() / KeySize);
        return std::make_pair(dataBucketBoundaries.data(),
                dataBucketBoundaries.size() * KeySize);
    };

    // Gather data bucket boundaries from pivot servers to all nodes.
    timeTrace("about to all-gather data bucket boundaries");
    {
        CycleCounter<> _(&PerfStats::threadStats.allGatherPivotsCycles);
        Tub<AllGather> allGather;
        invokeCollectiveOp<AllGather>(allGather,
                ALLGATHER_DATA_BUCKET_BOUNDARIES, context,
                allGatherPeersGroup.get(),
                dataBucketBoundaries.size() * KeySize,
                dataBucketBoundaries.data(), &merger);
        allGather->wait();
        removeCollectiveOp(ALLGATHER_DATA_BUCKET_BOUNDARIES);
        debugLogKeys("data bucket boundaries: ", &dataBucketBoundaries);
    }

    // All nodes will advance to the final data shuffle when the all-gather
    // completes.
    timeTrace("gathered %u data bucket boundaries",
            dataBucketBoundaries.size());
    dataBucketSort();
}

void
MilliSortService::dataBucketSort()
{
    timeTrace("=== Stage 7: dataBucketSort started");

    // Merge data from other nodes as they arrive.
    auto merger = [this] (Buffer* data) {
        CycleCounter<> _(&PerfStats::threadStats.bucketSortDataMergeCycles);
        uint32_t offset = 0;
        size_t oldSize = sortedKeys.size();
        while (offset < data->size()) {
            sortedKeys.push_back(*data->read<SortKey>(&offset));
        }
        inplaceMerge(sortedKeys, oldSize);
        timeTrace("merged %u keys", data->size() / KeySize);
    };

    // Send data to their designated nodes determined by the data bucket
    // boundaries.
    timeTrace("about to shuffle data tuples");
    {
        CycleCounter<> _(&PerfStats::threadStats.bucketSortDataCycles);
        Tub<AllShuffle> dataShuffle;
        invokeCollectiveOp<AllShuffle>(dataShuffle, ALLSHUFFLE_DATA, context,
                world.get(), &merger);
        uint32_t k = 0;
        for (int rank = 0; rank < int(dataBucketBoundaries.size()); rank++) {
            Buffer* buffer = dataShuffle->getSendBuffer(rank);
            SortKey boundary = dataBucketBoundaries[rank];
            while ((k < keys.size()) && (keys[k] <= boundary)) {
                buffer->appendCopy(&keys[k]);
                k++;
            }
            dataShuffle->closeSendBuffer(rank);
        }
        dataShuffle->wait();
        removeCollectiveOp(ALLSHUFFLE_DATA);
    }

    endTime = Cycles::rdtsc();
    PerfStats::threadStats.millisortTime += endTime - startTime;
    timeTrace("millisort finished");

    ongoingMilliSort.load()->sendReply();
    ongoingMilliSort = NULL;
    debugLogKeys("final result: ", &sortedKeys);
}

void
MilliSortService::debugLogKeys(const char* prefix, vector<SortKey>* keys)
{
#if LOG_INTERMEDIATE_RESULT
    string result;
    for (SortKey& key : *keys) {
        result += std::to_string(key);
        result += " ";
    }
    LOG(NOTICE, "%s%s", prefix, result.c_str());
#endif
}

void
MilliSortService::treeBcast(const WireFormat::TreeBcast::Request* reqHdr,
            WireFormat::TreeBcast::Response* respHdr, Rpc* rpc)
{
    TreeBcast::handleRpc(context, reqHdr, rpc);
}

void
MilliSortService::flatGather(const WireFormat::FlatGather::Request* reqHdr,
            WireFormat::FlatGather::Response* respHdr, Rpc* rpc)
{
    handleCollectiveOpRpc<FlatGather>(reqHdr, respHdr, rpc);
}

void
MilliSortService::allGather(const WireFormat::AllGather::Request* reqHdr,
            WireFormat::AllGather::Response* respHdr, Rpc* rpc)
{
    handleCollectiveOpRpc<AllGather>(reqHdr, respHdr, rpc);
}

void
MilliSortService::allShuffle(const WireFormat::AllShuffle::Request* reqHdr,
            WireFormat::AllShuffle::Response* respHdr, Rpc* rpc)
{
    handleCollectiveOpRpc<AllShuffle>(reqHdr, respHdr, rpc);
}

void
MilliSortService::sendData(const WireFormat::SendData::Request* reqHdr,
            WireFormat::SendData::Response* respHdr, Rpc* rpc)
{
    switch (reqHdr->dataId) {
        // TODO: hmm, right now we handle incoming data differently in Bcast &
        // gather; in gather, we use merger to incorporate incoming data and
        // no need for SendDataRpc, while here we use SendData RPC handler to
        // to incorporate the data. Think about how to make it uniform.
        case BROADCAST_PIVOT_BUCKET_BOUNDARIES: {
            // FIXME: info leak in handler; have to handle differently based on
            // rank
            if (world->rank > 0) {
                Buffer *request = rpc->requestPayload;
                for (uint32_t offset = sizeof(*reqHdr);
                     offset < request->size();) {
                    pivotBucketBoundaries.push_back(
                            *request->read<SortKey>(&offset));
                }
                timeTrace("received %u pivot bucket boundaries",
                        pivotBucketBoundaries.size());

                rpc->sendReply();
                pivotBucketSort();
            }
            break;
        }
        case BROADCAST_DATA_BUCKET_BOUNDARIES: {
            // FIXME: info leak in handler; have to handle differently based on
            // rank
            if (!isPivotServer) {
                Buffer *request = rpc->requestPayload;
                for (uint32_t offset = sizeof(*reqHdr);
                     offset < request->size();) {
                    // Put into dataBucketBoundaries directly.
                    dataBucketBoundaries.push_back(
                            *request->read<SortKey>(&offset));
                }
                timeTrace("received %u partial data bucket boundaries",
                        dataBucketBoundaries.size());
                debugLogKeys("data bucket boundaries (local portion): ",
                        &dataBucketBoundaries);

                rpc->sendReply();
                allGatherDataBucketBoundaries();
            }
            break;
        }
        default:
            assert(false);
    }
}

} // namespace RAMCloud
