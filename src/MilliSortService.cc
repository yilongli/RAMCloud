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

// TODO: justify this number; need to be large enough to saturate the bw
// but not too large that slows down the transport
#define MAX_OUTSTANDING_RPCS 10

#define SCOPED_TIMER(metric) CycleCounter<> _timer_##metric(&PerfStats::threadStats.metric);
#define START_TIMER(metric) CycleCounter<> _timer_##metric(&PerfStats::threadStats.metric);
#define STOP_TIMER(metric) _timer_##metric.stop();

// TODO: the following won't work for network I/O bytes because that counter is accumulated in dispatch thread...
//#define START_COUNTER(metric) uint64_t _old_##metric = PerfStats::threadStats.metric;
//#define STOP_COUNTER(metric, counter) PerfStats::threadStats.counter += PerfStats::threadStats.metric - _old_##metric;

#define ADD_COUNTER(metric, delta) PerfStats::threadStats.metric += delta;
#define SUB_COUNTER(metric, delta) PerfStats::threadStats.metric -= delta;
#define INC_COUNTER(metric) PerfStats::threadStats.metric++;

// Change 0 -> 1 in the following line to use pseudo-random numbers to
// initialize the local keys.
#define USE_PSEUDO_RANDOM 1

// Change 0 -> 1 in the following line to log intermediate computation result at
// each stage of the algorithm.
#define LOG_INTERMEDIATE_RESULT 0

#define LOG_FINAL_RESULT 1

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
    , rand()
    , startTime()
    , pivotServerRank(-1)
    , isPivotServer()
    , numDataTuples(-1)
    , numPivotServers(-1)
    , keys()
    , values()
    , sortedKeys()
    , sortedValues()
    , numSortedItems(0)
    , valueStartIdx()
    , localPivots()
    , dataBucketBoundaries()
    , dataBucketRanges()
    , dataBucketRangesDone(false)
    , mergeSorter()
    , valueRearrangers()
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
        case WireFormat::ShufflePull::opcode:
            callHandler<WireFormat::ShufflePull, MilliSortService,
                        &MilliSortService::shufflePull>(rpc);
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
    if (ongoingMilliSort) {
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
        respHdr->numNodes = respHdr->numNodesInited;
        respHdr->numCoresPerNode = static_cast<int>(
                Arachne::numActiveCores.load());
        respHdr->numPivotsPerNode = respHdr->numNodes;
        respHdr->maxOutstandingRpcs = MAX_OUTSTANDING_RPCS;
        respHdr->keySize = 10;
        respHdr->valueSize = 90;
        return;
    }

    startTime = 0;
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

    // Generate data tuples.
    keys.clear();
    values.clear();
    rand.construct(serverId.indexNumber());
    for (int i = 0; i < numDataTuples; i++) {
#if USE_PSEUDO_RANDOM
        uint64_t key = rand->next();
#else
        uint64_t key = uint64_t(world->rank + i * world->size());
#endif
        keys.emplace_back(key,
                downCast<uint16_t>(serverId.indexNumber()),
                downCast<uint32_t>(i));
        values.emplace_back(key);
    }
    numSortedItems = 0;
    sortedKeys.reserve(numDataTuples * 2);
    sortedKeys.clear();
    PivotKey nullKey(0, 0, 0);
    sortedKeys.insert(sortedKeys.begin(), numDataTuples * 2, nullKey);
    sortedValues.reserve(numDataTuples * 2);
    sortedValues.clear();
    sortedValues.insert(sortedValues.begin(), numDataTuples * 2, 0);
    valueStartIdx.clear();
    valueStartIdx.insert(valueStartIdx.begin(), world->size() + 1, 0);

    //
    localPivots.clear();
    dataBucketBoundaries.clear();
    dataBucketRanges.clear();
    dataBucketRangesDone.store(false);
    mergeSorter.construct(world->size(), numDataTuples * world->size() * 2);
    valueRearrangers.clear();
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
    startTime = startTime > 0 ? startTime : Cycles::rdtsc();
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
        startBcast.wait();
        ADD_COUNTER(millisortTime, Cycles::rdtsc() - startTime);
        return;
    }

    // Kick start the sorting process.
    ADD_COUNTER(millisortIsPivotServer, isPivotServer ? 1 : 0);
    ongoingMilliSort = rpc;
    localSortAndPickPivots();
    // FIXME: a better solution is to wait on condition var.
    while (ongoingMilliSort != NULL) {
        Arachne::yield();
    }
}

void
MilliSortService::inplaceMerge(std::vector<PivotKey>& keys,
        size_t sizeOfFirstSortedRange)
{
    auto middle = keys.begin();
    std::advance(middle, sizeOfFirstSortedRange);
    std::inplace_merge(keys.begin(), middle, keys.end());
}

void
MilliSortService::partition(std::vector<PivotKey>* keys, int numPartitions,
        std::vector<PivotKey>* pivots)
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

/**
 *
 *
 * \param useCurrentCore
 *      True means we are allowed to spawn Arachne threads on the core that
 *      runs the current thread; false, otherwise. Only set to true if there
 *      is no latency-sensitive task to perform.
 */
void
MilliSortService::rearrangeValues(PivotKey* keys, Value* values, int totalItems,
        bool initialData)
{
    uint64_t globalStartTime = Cycles::rdtsc();
    if (initialData) {
        ADD_COUNTER(rearrangeInitValuesStartTime, globalStartTime - startTime);
    } else {
        ADD_COUNTER(rearrangeFinalValuesStartTime, globalStartTime - startTime);
    }

    bool useCurrentCore = !initialData;
    int currentCore = useCurrentCore ? -1 :
                      Arachne::getThreadId().context->coreId;
    Arachne::CorePolicy::CoreList list = Arachne::getCorePolicy()->getCores(0);
    int numWorkers = list.size();
    if (list.find(currentCore) >= 0) {
        numWorkers--;
    }

    if (initialData) {
        ADD_COUNTER(rearrangeInitValuesWorkers, numWorkers);
    } else {
        ADD_COUNTER(rearrangeFinalValuesWorkers, numWorkers);
    }
// FIXME: somehow capture share_ptr by ref. and use it in workerMain cause stack corruption in Arachne?
//  ARACHNE: maxNumCores 8 is greater than the number of available hardware cores 7.Stack overflow detected on 0x26d8f40. Canary = 62678462107746304
#define USE_SHARED_PTR 0
#if USE_SHARED_PTR
    auto readers = std::make_shared<std::atomic<int>>(numWorkers);
#else
    auto readers = new std::atomic<int>(numWorkers);
#endif
    int perWorkerItems = (totalItems + numWorkers - 1) / numWorkers;

    /**
     * Main method of the worker thread. Each worker is responsible to rearrange 
     * values corresponding to a partition of the keys.
     *
     * For instance, if this worker is assigned keys[a..b], this method will
     * ensure that, forall i (a <= i <= b), values[i] holds the corresponding
     * value of keys[i] upon return.
     *
     * \param startIdx
     *      Index of the first key in this worker's key partition.
     * \param maxItems
     *      Max # values assigned to this worker. When the number of values
     *      left is less than this number, this method will recompute the
     *      correct number of values assigned to this worker.
     *      parts that go beyond the last element of the entire value array.
     *      So this worker should rearrange
     *      values corresponding to keys[startIdx...(startIdx+maxItems-1)].
     */
    auto workerMain = [keys, values, totalItems, initialData, globalStartTime,
#if USE_SHARED_PTR
            &readers] (int startIdx, int maxItems) {
#else
            readers] (int startIdx, int maxItems) {
#endif
        uint64_t* cpuTime = initialData ?
                &PerfStats::threadStats.rearrangeInitValuesCycles :
                &PerfStats::threadStats.rearrangeFinalValuesCycles;
        uint64_t* elapsedTime = initialData ?
                &PerfStats::threadStats.rearrangeInitValuesElapsedTime :
                &PerfStats::threadStats.rearrangeFinalValuesElapsedTime;
        uint64_t startTime = Cycles::rdtsc();

        // Allocate an auxilary array to hold the reordered values temporarily.
        int n = std::max(0, std::min(maxItems, totalItems - startIdx));
        Value* array = new Value[n];
        Value* dest = array;
        for (int i = startIdx; i < startIdx + n; i++) {
            std::memcpy(dest, &values[keys[i].index], Value::SIZE);
            keys[i].index = uint32_t(i - startIdx);
            dest++;
        }
        readers->fetch_sub(1);
        (*cpuTime) += (Cycles::rdtsc() - startTime);

        // Wait until all workers have finished reading #values.
        while (readers->load() > 0) {
            Arachne::yield();
        }
        startTime = Cycles::rdtsc();

        // Then copy the values from the auxilary array back to #values.
        std::memcpy(&values[startIdx], array, n * Value::SIZE);
        // TODO: avoid doing manual delete; use unique_ptr around an array? Memory::unique_ptr_free?
        delete[] array;
        (*cpuTime) += (Cycles::rdtsc() - startTime);
        (*elapsedTime) += (Cycles::rdtsc() - globalStartTime);
    };


    // Spin up worker threads on all cores we are allowed to use.
    int start = 0;
    for (uint i = 0; i < list.size(); i++) {
        if (list[i] == currentCore) {
            continue;
        }

        valueRearrangers.push_back(Arachne::createThreadOnCore(list[i],
                workerMain, start, perWorkerItems));
        start += perWorkerItems;
    }

    if (useCurrentCore) {
        for (auto& tid : valueRearrangers) {
            Arachne::join(tid);
        }
        valueRearrangers.clear();
    }
}

void
MilliSortService::localSortAndPickPivots()
{
    timeTrace("=== Stage 1: localSortAndPickPivots started");
    ADD_COUNTER(millisortInitItems, keys.size());
    ADD_COUNTER(millisortInitKeyBytes, keys.size() * PivotKey::KEY_SIZE);
    ADD_COUNTER(millisortInitValueBytes, values.size() * Value::SIZE);

    {
        ADD_COUNTER(localSortStartTime, Cycles::rdtsc() - startTime);
        SCOPED_TIMER(localSortElapsedTime);
        SCOPED_TIMER(localSortCycles);
        ADD_COUNTER(localSortWorkers, 1);
        std::sort(keys.begin(), keys.end());
    }
    // Overlap rearranging initial values with subsequent stages. This only
    // needs to be done before key shuffle.
    rearrangeValues(keys.data(), values.data(), int(keys.size()), true);

    partition(&keys, std::min((int)keys.size(), world->size()), &localPivots);
    debugLogKeys("local pivots: ", &localPivots);

    // Merge pivots from slave nodes as they arrive.
    auto merger = [this] (Buffer* pivotsBuffer) {
        SCOPED_TIMER(gatherPivotsMergeCycles);
        ADD_COUNTER(gatherPivotsInputBytes, pivotsBuffer->size());
        uint32_t offset = 0;
        size_t oldSize = gatheredPivots.size();
        while (offset < pivotsBuffer->size()) {
            gatheredPivots.push_back(*pivotsBuffer->read<PivotKey>(&offset));
        }
        inplaceMerge(gatheredPivots, oldSize);
        timeTrace("merged %u pivots", pivotsBuffer->size() / PivotKey::SIZE);
    };

    // Initiate the gather op that collects pivots to the pivot server.
    timeTrace("sorted %u data tuples, picked %u local pivots, about to gather "
            "pivots", keys.size(), localPivots.size());
    {
        ADD_COUNTER(gatherPivotsStartTime, Cycles::rdtsc() - startTime);
        SCOPED_TIMER(gatherPivotsElapsedTime);
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
        ADD_COUNTER(gatherPivotsOutputBytes,
                localPivots.size() * PivotKey::SIZE);
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
        SCOPED_TIMER(gatherSuperPivotsMergeCycles);
        ADD_COUNTER(gatherSuperPivotsInputBytes, pivotsBuf->size());
        uint32_t offset = 0;
        size_t oldSize = globalSuperPivots.size();
        while (offset < pivotsBuf->size()) {
            globalSuperPivots.push_back(*pivotsBuf->read<PivotKey>(&offset));
        }
        inplaceMerge(globalSuperPivots, oldSize);
        timeTrace("merged %u super-pivots", pivotsBuf->size() / PivotKey::SIZE);
    };

    // Initiate the gather op that collects super pivots to the root node.
    timeTrace("picked %u super-pivots, about to gather super-pivots",
            superPivots.size());
    {
        ADD_COUNTER(gatherSuperPivotsStartTime, Cycles::rdtsc() - startTime);
        SCOPED_TIMER(gatherSuperPivotsElapsedTime);
        Tub<FlatGather> gatherSuperPivots;
        invokeCollectiveOp<FlatGather>(gatherSuperPivots, GATHER_SUPER_PIVOTS,
                context, allPivotServers.get(), 0,
                (uint32_t) superPivots.size(), superPivots.data(), &merger);
        // fixme: this is duplicated everywhere
        gatherSuperPivots->wait();
        removeCollectiveOp(GATHER_SUPER_PIVOTS);

        ADD_COUNTER(gatherSuperPivotsOutputBytes,
                superPivots.size() * PivotKey::SIZE);
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
        ADD_COUNTER(bcastPivotBucketBoundariesStartTime,
                Cycles::rdtsc() - startTime);
        SCOPED_TIMER(bcastPivotBucketBoundariesElapsedTime);
        TreeBcast bcast(context, allPivotServers.get());
        bcast.send<SendDataRpc>(BROADCAST_PIVOT_BUCKET_BOUNDARIES,
                // TODO: simplify the SendDataRpc api?
                downCast<uint32_t>(PivotKey::SIZE * pivotBucketBoundaries.size()),
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
        // TODO: merge here is effectively serialized; shall we parallelize it?
        static SpinLock mutex("pivotShuffle::mutex");
        SpinLock::Guard lock(mutex);
        SCOPED_TIMER(mergePivotsInBucketSortCycles);
        uint32_t offset = 0;
        size_t oldSize = sortedGatheredPivots.size();
        numSmallerPivots += *data->read<uint32_t>(&offset);
        numPivotsInTotal += *data->read<uint32_t>(&offset);
        while (offset < data->size()) {
            sortedGatheredPivots.push_back(*data->read<PivotKey>(&offset));
        }
        inplaceMerge(sortedGatheredPivots, oldSize);
        timeTrace("merged %u pivots", (data->size() - 8) / PivotKey::SIZE);
    };

    // Send pivots to their designated pivot servers determined by the pivot
    // bucket boundaries.
    timeTrace("about to shuffle pivots");
    {
        ADD_COUNTER(bucketSortPivotsStartTime, Cycles::rdtsc() - startTime);
        SCOPED_TIMER(bucketSortPivotsElapsedTime);
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

            PivotKey boundary = pivotBucketBoundaries[rank];
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
                downCast<uint32_t>(PivotKey::SIZE * partialDataBucketBoundaries.size()),
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
            SCOPED_TIMER(allGatherPivotsMergeCycles);
            uint32_t offset = 0;
            size_t oldSize = dataBucketBoundaries.size();
            while (offset < pivotsBuffer->size()) {
                PivotKey boundary = *pivotsBuffer->read<PivotKey>(&offset);
                dataBucketBoundaries.push_back(boundary);
            }
            inplaceMerge(dataBucketBoundaries, oldSize);
            // TODO: shall we do the merge online? actually, even better, partialDataBucketBoundaries
            // are sorted globally, we know exactly where to insert even they come!
            timeTrace("merged %u data bucket boundaries",
                    pivotsBuffer->size() / PivotKey::SIZE);
        return std::make_pair(dataBucketBoundaries.data(),
                dataBucketBoundaries.size() * PivotKey::SIZE);
    };

    // Gather data bucket boundaries from pivot servers to all nodes.
    timeTrace("about to all-gather data bucket boundaries");
    {
        ADD_COUNTER(allGatherPivotsStartTime, Cycles::rdtsc() - startTime);
        SCOPED_TIMER(allGatherPivotsElapsedTime);
        Tub<AllGather> allGather;
        invokeCollectiveOp<AllGather>(allGather,
                ALLGATHER_DATA_BUCKET_BOUNDARIES, context,
                allGatherPeersGroup.get(),
                dataBucketBoundaries.size() * PivotKey::SIZE,
                dataBucketBoundaries.data(), &merger);
        allGather->wait();
        removeCollectiveOp(ALLGATHER_DATA_BUCKET_BOUNDARIES);
        debugLogKeys("data bucket boundaries: ", &dataBucketBoundaries);
    }
    timeTrace("gathered %u data bucket boundaries",
            dataBucketBoundaries.size());

    // Compute data bucket ranges required by the shufflePull handler.
    uint32_t keyIdx = 0;
    for (const PivotKey& rightBound : dataBucketBoundaries) {
        int numKeys = 0;
        int oldKeyIdx = keyIdx;
        while ((keyIdx < keys.size()) && (keys[keyIdx] <= rightBound)) {
            keys[keyIdx].index = static_cast<uint32_t>(numKeys);
            numKeys++;
            keyIdx++;
        }
        dataBucketRanges.emplace_back(oldKeyIdx, numKeys);
    }
    for (auto& tid : valueRearrangers) {
        Arachne::join(tid);
    }
    valueRearrangers.clear();
    dataBucketRangesDone.store(true);

    // All nodes will advance to the final data shuffle when the all-gather
    // completes.
    shuffleKeys();
}

void
MilliSortService::shuffleKeys() {
    timeTrace("=== Stage 7: shuffleKeys started");
    ADD_COUNTER(shuffleKeysStartTime, Cycles::rdtsc() - startTime);
    START_TIMER(shuffleKeysElapsedTime);

    // TODO: move this into startMillisort? not sure it's a good idea to have
    // idle A-threads when it's not necessary.
    mergeSorter->prepareThreads();

    // Merge keys from other nodes into a sorted array as they arrive.
    // TODO: pull-based shuffle is not parallelized by default; shall we try?
    std::atomic<int> sortedItemCnt(0);
    auto keyMerger = [this, &sortedItemCnt] (ServerId fromServer,
            Buffer* keyBuffer)
    {
        SCOPED_TIMER(bucketSortMergeKeyCycles);
        ADD_COUNTER(shuffleKeysInputBytes, keyBuffer->size());
        uint32_t numKeys = keyBuffer->size() / PivotKey::SIZE;
        int start = sortedItemCnt.fetch_add(numKeys);
        valueStartIdx[fromServer.indexNumber()] = start;
        // TODO: would it be better to merge the following two loops into one?
        keyBuffer->copy(0, keyBuffer->size(), &sortedKeys[start]);
        for (int i = start; i < start + int(numKeys); i++) {
            sortedKeys[i].index = uint32_t(i);
        }
        mergeSorter->poll(&sortedKeys[start], numKeys);
        timeTrace("merged %u keys, %u bytes", numKeys, keyBuffer->size());
    };

    timeTrace("about to shuffle keys");
    // FIXME: duplicate code as shuffleValues
    int outstandingRpcs = 0;
    int completedRpcs = 0;
    int sentRpcs = 0;
    Tub<ShufflePullRpc> outgoingRpcs[MAX_OUTSTANDING_RPCS];
    ServerId targetServers[MAX_OUTSTANDING_RPCS];
    std::vector<int> freeTubs;
    for (int i = 0; i < MAX_OUTSTANDING_RPCS; i++) {
        freeTubs.push_back(i);
    }
    while (completedRpcs < world->size()) {
        if ((sentRpcs < world->size()) &&
                (outstandingRpcs < MAX_OUTSTANDING_RPCS)) {
            ServerId target = world->getNode(world->rank + sentRpcs + 1);
            int slot = freeTubs.back();
            freeTubs.pop_back();
            targetServers[slot] = target;
            outgoingRpcs[slot].construct(context, target, world->rank,
                    ALLSHUFFLE_KEY);
            sentRpcs++;
            outstandingRpcs++;
        }

        int slot = 0;
        for (auto& rpc : outgoingRpcs) {
            if (rpc && rpc->isReady()) {
                keyMerger(targetServers[slot], rpc->wait());
                freeTubs.push_back(slot);
                rpc.destroy();
                completedRpcs++;
                outstandingRpcs--;
            }
            slot++;
        }
        Arachne::yield();
    }
    numSortedItems = sortedItemCnt.load();
    STOP_TIMER(shuffleKeysElapsedTime);
    ADD_COUNTER(shuffleKeysSentRpcs, world->size());
    ADD_COUNTER(millisortFinalPivotKeyBytes, numSortedItems * PivotKey::SIZE);

    shuffleValues();
}

void
MilliSortService::shuffleValues()
{
    timeTrace("=== Stage 8: shuffleValues started");
    ADD_COUNTER(shuffleValuesStartTime, Cycles::rdtsc() - startTime);
    START_TIMER(shuffleValuesElapsedTime);

    // TODO: pull-based shuffle is not parallelized by default; shall we try?
    auto valueMerger = [this] (ServerId fromServer, Buffer* valueBuffer) {
        SCOPED_TIMER(bucketSortMergeValueCycles);
        ADD_COUNTER(shuffleValuesInputBytes, valueBuffer->size());
        uint32_t numValues = valueBuffer->size() / Value::SIZE;
        uint32_t serverId = fromServer.indexNumber();
        int start = valueStartIdx[serverId];
        valueBuffer->copy(0, valueBuffer->size(), &sortedValues[start]);
        timeTrace("merged %u values from serverId %u, %u bytes", numValues,
                serverId, valueBuffer->size());
    };

    bool mergeSortCompleted = false;
    auto mergeSortIsReady = [this, &mergeSortCompleted] () {
        if (mergeSortCompleted) {
            return true;
        }
        if (mergeSorter->poll()) {
            return false;
        }

        mergeSortCompleted = true;
        ADD_COUNTER(onlineMergeSortStartTime,
                mergeSorter->startTime - startTime);
        ADD_COUNTER(onlineMergeSortElapsedTime,
                mergeSorter->stopTick - mergeSorter->startTime);
        ADD_COUNTER(onlineMergeSortWorkers, mergeSorter->numWorkers);
        timeTrace("sorted %d final keys", numSortedItems);
        // FIXME: is the following copy necessary?
        auto sortedArray = mergeSorter->getSortedArray();
        memcpy(sortedKeys.data(), sortedArray.data,
                sortedArray.size * PivotKey::SIZE);
        assert(int(sortedArray.size) == numSortedItems);
        timeTrace("copied %d final keys", numSortedItems);
        return true;
    };

    int outstandingRpcs = 0;
    int completedRpcs = 0;
    int sentRpcs = 0;
    Tub<ShufflePullRpc> outgoingRpcs[MAX_OUTSTANDING_RPCS];
    ServerId targetServers[MAX_OUTSTANDING_RPCS];
    std::vector<int> freeTubs;
    for (int i = 0; i < MAX_OUTSTANDING_RPCS; i++) {
        freeTubs.push_back(i);
    }
    while (completedRpcs < world->size()) {
        if ((sentRpcs < world->size()) &&
                (outstandingRpcs < MAX_OUTSTANDING_RPCS)) {
            ServerId target = world->getNode(world->rank + sentRpcs + 1);
            int slot = freeTubs.back();
            freeTubs.pop_back();
            targetServers[slot] = target;
            outgoingRpcs[slot].construct(context, target, world->rank,
                    ALLSHUFFLE_VALUE);
            sentRpcs++;
            outstandingRpcs++;
        }

        int slot = 0;
        for (auto& rpc : outgoingRpcs) {
            if (rpc && rpc->isReady()) {
                valueMerger(targetServers[slot], rpc->wait());
                freeTubs.push_back(slot);
                rpc.destroy();
                completedRpcs++;
                outstandingRpcs--;
            }
            slot++;
        }

        mergeSortIsReady();
        Arachne::yield();
    }
    STOP_TIMER(shuffleValuesElapsedTime);
    ADD_COUNTER(shuffleValuesSentRpcs, sentRpcs);
    ADD_COUNTER(millisortFinalValueBytes, numSortedItems * Value::SIZE);

    // Make sure the online merge of final keys completes before rearranging
    // final values. Normally, the online merge to be hidden completely within
    // value shuffle.
    while (!mergeSortIsReady()) {
        RAMCLOUD_CLOG(WARNING, "Failed to hide mergesort within value shuffle");
    }
    rearrangeValues(sortedKeys.data(), sortedValues.data(), numSortedItems,
            false);

    // FIXME: millisortTime can be measured using CycleCounter; nah, maybe not worth the complexity
    if (world->rank > 0) {
        ADD_COUNTER(millisortTime, Cycles::rdtsc() - startTime);
    }
    ADD_COUNTER(millisortFinalItems, numSortedItems);
    timeTrace("millisort finished");

    ongoingMilliSort.load()->sendReply();
    ongoingMilliSort = NULL;

#if LOG_FINAL_RESULT
    string result;
    for (int i = 0; i < numSortedItems; i++) {
        result += std::to_string(sortedKeys[i].keyAsUint64()) + ":" +
                std::to_string(sortedValues[i].asUint64());
        result += " ";
        if (i % 100 == 0) {
            Arachne::yield();
        }
    }
    Arachne::yield();
    LOG(NOTICE, "final result: %s", result.c_str());
#endif
}

void
MilliSortService::debugLogKeys(const char* prefix, vector<PivotKey>* keys)
{
#if LOG_INTERMEDIATE_RESULT
    string result;
    for (PivotKey& sortKey : *keys) {
        result += std::to_string(sortKey.keyAsUint64());
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
MilliSortService::shufflePull(const WireFormat::ShufflePull::Request* reqHdr,
            WireFormat::ShufflePull::Response* respHdr, Rpc* rpc)
{
    int senderId = reqHdr->senderId;
    switch (reqHdr->dataId) {
        case ALLSHUFFLE_KEY: {
//            timeTrace("received ShufflePull for keys");
            while (!dataBucketRangesDone) {
                Arachne::yield();
            }
            int keyIdx, numKeys;
            std::tie(keyIdx, numKeys) = dataBucketRanges[senderId];
            rpc->replyPayload->appendExternal(&keys[keyIdx], numKeys * PivotKey::SIZE);
            ADD_COUNTER(shuffleKeysOutputBytes, rpc->replyPayload->size());
            INC_COUNTER(shuffleKeysReceivedRpcs);
//            timeTrace("RPC handler ShufflePull finished");
            break;
        }
        case ALLSHUFFLE_VALUE: {
            while (!dataBucketRangesDone) {
                Arachne::yield();
            }
            int valueIdx, numValues;
            std::tie(valueIdx, numValues) = dataBucketRanges[senderId];
            rpc->replyPayload->appendExternal(&values[valueIdx],
                    numValues * Value::SIZE);
            ADD_COUNTER(shuffleValuesOutputBytes, rpc->replyPayload->size());
            INC_COUNTER(shuffleValuesReceivedRpcs);
            break;
        }
        default:
            assert(false);
    }
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
                            *request->read<PivotKey>(&offset));
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
                            *request->read<PivotKey>(&offset));
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
