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

#include "ClockSynchronizer.h"
#include "MilliSortService.h"
#include "AllShuffle.h"
#include "Broadcast.h"
#include "TimeTrace.h"

namespace RAMCloud {

// Change 0 -> 1 in the following line to compile detailed time tracing in
// this transport.
#define TIME_TRACE 0

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
    , printingResult()
    , valueStartIdx()
    , localPivots()
    , rearrangeLocalVals()
    , dataBucketBoundaries()
    , dataBucketRanges()
    , dataBucketRangesDone(false)
    , pullKeyRpcs()
    , pullValueRpcs()
    , mergeSorter()
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
        case WireFormat::TreeGather::opcode:
            callHandler<WireFormat::TreeGather, MilliSortService,
                        &MilliSortService::treeGather>(rpc);
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
        case WireFormat::BenchmarkCollectiveOp::opcode:
            callHandler<WireFormat::BenchmarkCollectiveOp, MilliSortService,
                        &MilliSortService::benchmarkCollectiveOp>(rpc);
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

    if (reqHdr->fromClient || (serverId.indexNumber() > 1)) {
        initWorld(reqHdr->numNodes);
    }

    // Initialization request from an external client should only be processed
    // by the root node, which would then broadcast the request to all nodes.
    if (reqHdr->fromClient) {
        if (world->rank > 0) {
            respHdr->common.status = STATUS_MILLISORT_ERROR;
            return;
        }

        // TODO: can't do broadcast before communication group has been setup
        std::list<InitMilliSortRpc> outgoingRpcs;
        for (int i = 0; i < world->size(); i++) {
            outgoingRpcs.emplace_back(context, world->getNode(i),
                    reqHdr->numNodes, reqHdr->dataTuplesPerServer,
                    reqHdr->nodesPerPivotServer, false);
        }
        while (!outgoingRpcs.empty()) {
            InitMilliSortRpc* initRpc = &outgoingRpcs.front();
            auto initResult = initRpc->wait();
            respHdr->numNodesInited += initResult->numNodesInited;
            outgoingRpcs.pop_front();
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
    for (int i = peerRank; i < world->size(); i += nodesPerPivotServer) {
        nodes.push_back(world->getNode(i));
    }
    allGatherPeersGroup.construct(ALL_GATHER_PEERS_GROUP,
            world->rank / nodesPerPivotServer, nodes);

    // Generate data tuples.
    keys.reset(new PivotKey[numDataTuples]);
    values.reset(new Value[numDataTuples]);
    rand.construct(serverId.indexNumber());
    for (int i = 0; i < numDataTuples; i++) {
#if USE_PSEUDO_RANDOM
        uint64_t key = rand->next();
#else
        uint64_t key = uint64_t(world->rank + i * world->size());
#endif
        new(&keys[i]) PivotKey(key,
                downCast<uint16_t>(serverId.indexNumber()),
                downCast<uint32_t>(i));
        new(&values[i]) Value(key);
    }

    while (printingResult) {
        Arachne::yield();
    }
    numSortedItems = 0;
#define MAX_IMBALANCE_RATIO 2
    sortedKeys.reset(new PivotKey[numDataTuples * MAX_IMBALANCE_RATIO]);
    sortedValues.reset(new Value[numDataTuples * MAX_IMBALANCE_RATIO]);
    valueStartIdx.clear();
    valueStartIdx.insert(valueStartIdx.begin(), world->size(), 0);

    //
    localPivots.clear();
    dataBucketBoundaries.clear();
    dataBucketRanges.clear();
    dataBucketRangesDone.store(false);
    pullKeyRpcs.reset(new Tub<ShufflePullRpc>[world->size()]);
    pullValueRpcs.reset(new Tub<ShufflePullRpc>[world->size()]);
    mergeSorter.construct(world->size(), numDataTuples * world->size() * 2);
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
MilliSortService::partition(PivotKey* keys, int numKeys, int numPartitions,
        std::vector<PivotKey>* pivots)
{
    assert(pivots->empty());
    // Let P be the number of partitions. Pick P pivots that divide #keys into
    // P partitions as evenly as possible. Since the sizes of any two partitions
    // differ by at most one, we have:
    //      s * k + (s + 1) * (P - k) = numKeys
    // where s is the number of keys in smaller partitions and k is the number
    // of smaller partitions.
    int s = numKeys / numPartitions;
    int k = numPartitions * (s + 1) - numKeys;
    int pivotIdx = -1;
    for (int i = 0; i < numPartitions; i++) {
        if (i < k) {
            pivotIdx += s;
        } else {
            pivotIdx += s + 1;
        }
        pivots->push_back(keys[pivotIdx]);
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
MilliSortService::RearrangeValueTask
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
    Value* dest = new Value[totalItems];

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
     * \param n
     *      # values assigned to this worker. So this worker should rearrange
     *      values corresponding to keys[startIdx...(startIdx+numValues-1)].
     */
    auto workerMain = [keys, values, dest, initialData, globalStartTime]
            (int startIdx, int n) {
        uint64_t* cpuTime = initialData ?
                &PerfStats::threadStats.rearrangeInitValuesCycles :
                &PerfStats::threadStats.rearrangeFinalValuesCycles;
        uint64_t* elapsedTime = initialData ?
                &PerfStats::threadStats.rearrangeInitValuesElapsedTime :
                &PerfStats::threadStats.rearrangeFinalValuesElapsedTime;
        uint64_t startTime = Cycles::rdtsc();

        // Allocate an auxilary array to hold the reordered values temporarily.
        Value* nextValue = dest + startIdx;
        for (int i = startIdx; i < startIdx + n; i++) {
            // Note: it's OK to leave the stale value in keys[i].index because
            // after rearrangement, the correspondence between keys and values
            // is trivial (ie. keys[i] -> values[i]).
            std::memcpy(nextValue, &values[keys[i].index], Value::SIZE);
            nextValue++;
        }
        uint64_t endTime = Cycles::rdtsc();
        (*cpuTime) += (endTime - startTime);
        (*elapsedTime) += (endTime - globalStartTime);
    };


    // Spin up worker threads on all cores we are allowed to use.
    std::list<Arachne::ThreadId> workers;
    int perWorkerItems = (totalItems + numWorkers - 1) / numWorkers;
    int start = 0;
    for (uint i = 0; i < list.size(); i++) {
        if (list[i] == currentCore) {
            continue;
        }

        int n = std::max(0, std::min(perWorkerItems, totalItems - start));
        workers.push_back(
                Arachne::createThreadOnCore(list[i], workerMain, start, n));
        start += n;
    }
    return {dest, workers};
}

void
MilliSortService::localSortAndPickPivots()
{
    timeTrace("=== Stage 1: localSortAndPickPivots started");
    ADD_COUNTER(millisortInitItems, numDataTuples);
    ADD_COUNTER(millisortInitKeyBytes, numDataTuples * PivotKey::KEY_SIZE);
    ADD_COUNTER(millisortInitValueBytes, numDataTuples * Value::SIZE);

    {
        ADD_COUNTER(localSortStartTime, Cycles::rdtsc() - startTime);
        SCOPED_TIMER(localSortElapsedTime);
        SCOPED_TIMER(localSortCycles);
        ADD_COUNTER(localSortWorkers, 1);
        std::sort(keys.get(), keys.get() + numDataTuples);
    }
    // Overlap rearranging initial values with subsequent stages. This only
    // needs to be done before key shuffle.
    rearrangeLocalVals =
            rearrangeValues(keys.get(), values.get(), numDataTuples, true);

    partition(keys.get(), numDataTuples, std::min(numDataTuples, world->size()),
            &localPivots);
    timeTrace("sorted %d data tuples, picked %u local pivots, about to gather "
            "pivots", numDataTuples, localPivots.size());
    debugLogKeys("local pivots: ", &localPivots);

    // Initiate the gather op that collects pivots to the pivot server.
    // Merge pivots from slave nodes as they arrive.
    gatheredPivots = localPivots;
    PivotMergeSorter merger(this, &gatheredPivots);
    {
        ADD_COUNTER(gatherPivotsStartTime, Cycles::rdtsc() - startTime);
        SCOPED_TIMER(gatherPivotsElapsedTime);
        Tub<TreeGather> gatherPivots;
        invokeCollectiveOp<TreeGather>(gatherPivots, GATHER_PIVOTS, context,
                myPivotServerGroup.get(), 0, (uint32_t) localPivots.size(),
                localPivots.data(), &merger);
        gatherPivots->wait();
        // TODO: the cleanup is not very nice; how to do RAII?
        removeCollectiveOp(GATHER_PIVOTS);

        // FIXME: seems wrong division of responsibility; should I move this
        // into Gather class? Also this assumes a particular implementation of
        // Gather.
        ADD_COUNTER(gatherPivotsMergeCycles, merger.activeCycles);
        ADD_COUNTER(gatherPivotsInputBytes, merger.bytesReceived);
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
        gatheredPivots.clear();
    }
}

void
MilliSortService::pickSuperPivots()
{
    timeTrace("=== Stage 2: pickSuperPivots started");

    assert(isPivotServer);
    partition(gatheredPivots.data(), int(gatheredPivots.size()),
            allPivotServers->size(), &superPivots);
    timeTrace("picked %u super-pivots, about to gather super-pivots",
            superPivots.size());
    debugLogKeys("super-pivots: ", &superPivots);

    // Initiate the gather op that collects super pivots to the root node.
    // Merge super pivots from pivot servers as they arrive.
    globalSuperPivots = superPivots;
    PivotMergeSorter merger(this, &globalSuperPivots);
    {
        ADD_COUNTER(gatherSuperPivotsStartTime, Cycles::rdtsc() - startTime);
        SCOPED_TIMER(gatherSuperPivotsElapsedTime);
        Tub<TreeGather> gatherSuperPivots;
        invokeCollectiveOp<TreeGather>(gatherSuperPivots, GATHER_SUPER_PIVOTS,
                context, allPivotServers.get(), 0,
                (uint32_t) superPivots.size(), superPivots.data(), &merger);
        // fixme: this is duplicated everywhere
        gatherSuperPivots->wait();
        removeCollectiveOp(GATHER_SUPER_PIVOTS);

        ADD_COUNTER(gatherSuperPivotsMergeCycles, merger.activeCycles);
        ADD_COUNTER(gatherSuperPivotsInputBytes, merger.bytesReceived);
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
        globalSuperPivots.clear();
    }
}

void
MilliSortService::pickPivotBucketBoundaries()
{
    timeTrace("=== Stage 3: pickPivotBucketBoundaries started");
    assert(world->rank == 0);
    partition(globalSuperPivots.data(), int(globalSuperPivots.size()),
            allPivotServers->size(), &pivotBucketBoundaries);
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
//    auto merger = [this] (Buffer* pivotsBuffer) {
//        SCOPED_TIMER(allGatherPivotsMergeCycles);
//        uint32_t offset = 0;
//        size_t oldSize = dataBucketBoundaries.size();
//        while (offset < pivotsBuffer->size()) {
//            PivotKey boundary = *pivotsBuffer->read<PivotKey>(&offset);
//            dataBucketBoundaries.push_back(boundary);
//        }
//        inplaceMerge(dataBucketBoundaries, oldSize);
//        // TODO: shall we do the merge online? actually, even better, partialDataBucketBoundaries
//        // are sorted globally, we know exactly where to insert even they come!
//        timeTrace("merged %u data bucket boundaries",
//                pivotsBuffer->size() / PivotKey::SIZE);
//        return std::make_pair(dataBucketBoundaries.data(),
//                dataBucketBoundaries.size() * PivotKey::SIZE);
//    };
    PivotMerger merger(this, &dataBucketBoundaries);

    // Gather data bucket boundaries from pivot servers to all nodes.
    timeTrace("about to all-gather data bucket boundaries");
    {
        ADD_COUNTER(allGatherPivotsStartTime, Cycles::rdtsc() - startTime);
        SCOPED_TIMER(allGatherPivotsElapsedTime);
        Tub<AllGather> allGather;
        invokeCollectiveOp<AllGather>(allGather,
                ALLGATHER_DATA_BUCKET_BOUNDARIES, context,
                allGatherPeersGroup.get(),
                uint32_t(dataBucketBoundaries.size() * PivotKey::SIZE),
                dataBucketBoundaries.data(), &merger);
        allGather->wait();
        removeCollectiveOp(ALLGATHER_DATA_BUCKET_BOUNDARIES);
        ADD_COUNTER(allGatherPivotsMergeCycles, merger.activeCycles);
        debugLogKeys("data bucket boundaries: ", &dataBucketBoundaries);
    }
    timeTrace("gathered %u data bucket boundaries",
            dataBucketBoundaries.size());

    // Sort the gathered data bucket boundaries.
    std::sort(dataBucketBoundaries.begin(), dataBucketBoundaries.end());

    // Compute data bucket ranges required by the shufflePull handler.
    int keyIdx = 0;
    for (const PivotKey& rightBound : dataBucketBoundaries) {
        int numKeys = 0;
        int oldKeyIdx = keyIdx;
        while ((keyIdx < numDataTuples) && (keys[keyIdx] <= rightBound)) {
            numKeys++;
            keyIdx++;
        }
        dataBucketRanges.emplace_back(oldKeyIdx, numKeys);
    }
    rearrangeLocalVals.wait(&values);
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
    auto keyMerger = [this, &sortedItemCnt] (int serverRank, Buffer* keyBuffer)
    {
        SCOPED_TIMER(shuffleKeysCopyResponseCycles);
        ADD_COUNTER(shuffleKeysInputBytes, keyBuffer->size());
        uint32_t numKeys = keyBuffer->size() / PivotKey::SIZE;
        int start = sortedItemCnt.fetch_add(numKeys);
        valueStartIdx[serverRank] = start;
        keyBuffer->copy(0, keyBuffer->size(), &sortedKeys[start]);
        for (int i = start; i < start + int(numKeys); i++) {
            sortedKeys[i].index = uint32_t(i);
        }
        mergeSorter->poll(&sortedKeys[start], numKeys);
        timeTrace("merged %u keys, %u bytes", numKeys, keyBuffer->size());
    };

    timeTrace("about to shuffle keys");
    invokeShufflePull(pullKeyRpcs.get(), world.get(), MAX_OUTSTANDING_RPCS,
            ALLSHUFFLE_KEY, keyMerger);
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
    auto valueMerger = [this] (int serverId, Buffer* valueBuffer) {
        SCOPED_TIMER(shuffleValuesCopyResponseCycles);
        ADD_COUNTER(shuffleValuesInputBytes, valueBuffer->size());
        uint32_t numValues = valueBuffer->size() / Value::SIZE;
        int start = valueStartIdx[serverId];
        // TODO: eliminate this copy
        valueBuffer->copy(0, valueBuffer->size(), &sortedValues[start]);
        timeTrace("merged %u values from serverId %d, %u bytes", numValues,
                serverId+1, valueBuffer->size());
    };

    Arachne::ThreadId mergeSortPoller = Arachne::createThread([this] () {
        while (mergeSorter->poll()) {
            Arachne::yield();
        }
        assert(int(mergeSorter->getSortedArray().size) == numSortedItems);
        ADD_COUNTER(onlineMergeSortStartTime,
                mergeSorter->startTime - startTime);
        ADD_COUNTER(onlineMergeSortElapsedTime,
                mergeSorter->stopTick - mergeSorter->startTime);
        ADD_COUNTER(onlineMergeSortWorkers, mergeSorter->numWorkers);
        timeTrace("sorted %d final keys", numSortedItems);
        // FIXME: the following copy is not necessary and should be removed!
        auto sortedArray = mergeSorter->getSortedArray();
        memcpy(sortedKeys.get(), sortedArray.data,
                sortedArray.size * PivotKey::SIZE);
        assert(int(sortedArray.size) == numSortedItems);
        timeTrace("copied %d final keys", numSortedItems);
    });

    invokeShufflePull(pullValueRpcs.get(), world.get(), MAX_OUTSTANDING_RPCS,
            ALLSHUFFLE_VALUE, valueMerger);
    STOP_TIMER(shuffleValuesElapsedTime);
    ADD_COUNTER(shuffleValuesSentRpcs, world->size());
    ADD_COUNTER(millisortFinalValueBytes, numSortedItems * Value::SIZE);

    // Make sure the online merge of final keys completes before rearranging
    // final values. Normally, the online merge to be hidden completely within
    // value shuffle.
    Arachne::join(mergeSortPoller);
    rearrangeValues(sortedKeys.get(), sortedValues.get(), numSortedItems, false)
            .wait(&sortedValues);

    // FIXME: millisortTime can be measured using CycleCounter; nah, maybe not worth the complexity
    if (world->rank > 0) {
        ADD_COUNTER(millisortTime, Cycles::rdtsc() - startTime);
    }
    ADD_COUNTER(millisortFinalItems, numSortedItems);
    timeTrace("millisort finished");

    printingResult = true;
    ongoingMilliSort.load()->sendReply();
    ongoingMilliSort = NULL;

#if LOG_FINAL_RESULT
    string result;
    for (int i = 0; i < numSortedItems; i++) {
        result += std::to_string(sortedKeys[i].keyAsUint64());
        result += ":";
        result += std::to_string(sortedValues[i].asUint64());
        result += " ";
        if (i % 100 == 0) {
            Arachne::yield();
        }
    }
    Arachne::yield();
    LOG(NOTICE, "final result (%d items): %s", numSortedItems, result.c_str());
#endif
    printingResult = false;
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
MilliSortService::treeGather(const WireFormat::TreeGather::Request* reqHdr,
            WireFormat::TreeGather::Response* respHdr, Rpc* rpc)
{
    handleCollectiveOpRpc<TreeGather>(reqHdr, respHdr, rpc);
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
        case ALLSHUFFLE_BENCHMARK: {
            if (reqHdr->dataSize <= Transport::MAX_RPC_LEN) {
                rpc->replyPayload->appendExternal(
                        context->masterZeroCopyRegion, reqHdr->dataSize);
            } else {
                respHdr->common.status = STATUS_INVALID_PARAMETER;
            }
            break;
        }
        default:
            assert(false);
    }
}

void
MilliSortService::benchmarkCollectiveOp(
        const WireFormat::BenchmarkCollectiveOp::Request* reqHdr,
        WireFormat::BenchmarkCollectiveOp::Response* respHdr,
        Rpc* rpc)
{
#define WARMUP_COUNT 10
    std::unique_ptr<char[]> data(new char[std::max(1u, reqHdr->dataSize)]);
    if (reqHdr->opcode == WireFormat::SEND_DATA) {
        /* Point-to-point message benchmark */
        ServerId target = world->getNode(world->size() - 1);
        for (int i = 0; i < WARMUP_COUNT + reqHdr->count; i++) {
            uint64_t startTime = Cycles::rdtsc();
            SendDataRpc sendDataRpc(context, target, POINT2POINT_BENCHMARK,
                    reqHdr->dataSize, data.get());
            Buffer* result = sendDataRpc.wait();
            if (i >= WARMUP_COUNT) {
                uint64_t remoteEndTime = *result->getStart<uint64_t>();
                uint64_t endTime = context->clockSynchronizer->getConverter(
                        target).toLocalTsc(remoteEndTime);
                uint64_t oneWayDelayNs =
                        Cycles::toNanoseconds(endTime - startTime);
                rpc->replyPayload->emplaceAppend<uint64_t>(oneWayDelayNs);
            }
        }
    } else if (reqHdr->opcode == WireFormat::BCAST_TREE) {
        /* Broadcast benchmark */
        std::vector<uint32_t> latencyNs(world.get()->size() - 1);
        for (int i = 0; i < WARMUP_COUNT + reqHdr->count; i++) {
            uint64_t startTime = Cycles::rdtsc();
            TreeBcast bcast(context, world.get(), downCast<uint32_t>(i));
            bcast.send(data.get(), reqHdr->dataSize);
            Buffer* result = bcast.wait();
            if (i >= WARMUP_COUNT) {
                uint32_t offset = 0;
                while (offset < result->size()) {
                    uint32_t rank = *result->read<uint32_t>(&offset);
                    uint64_t endTime = *result->read<uint64_t>(&offset);
                    latencyNs[rank - 1] = downCast<uint32_t>(
                            Cycles::toNanoseconds(endTime - startTime));
                }
                rpc->replyPayload->appendCopy(latencyNs.data(),
                        downCast<uint32_t>(latencyNs.size() * 4));
            }
        }
    } else if (reqHdr->opcode == WireFormat::GATHER_TREE) {
        /* Gather benchmark */

        // The master node is responsible for running many iterations of the
        // experiment and recording the completion time of each run. Note that
        // the experiment relies on clock sync. to ensure all participating
        // nodes invokes/enters the collective operation at the same time. To
        // start a run, the master node broadcasts the BenchmarkCollectiveOpRpc
        // to all participating nodes which includes a starting time. Upon the
        // receipt of the benchmark RPC, a node waits until the starting time
        // to invoke the collective op. The completion time is computed on the
        // gather-tree root node and sends back via the response of benchmark
        // RPC. The outer broadcast op that carries the benchmark RPC then
        // return the completion time as result to the master node.
        if (reqHdr->masterId == 0) {
            for (int i = 0; i < WARMUP_COUNT + reqHdr->count; i++) {
                // Start the operation 100 us from now, which should be enough
                // for the broadcast message to reach all the participants.
                uint64_t startTime = Cycles::rdtsc() +
                        Cycles::fromMicroseconds(100);
                TreeBcast bcast(context, world.get());
                bcast.send<BenchmarkCollectiveOpRpc>(i, reqHdr->opcode,
                        reqHdr->dataSize, serverId.getId(), startTime);
                Buffer* result = bcast.wait();

                // Only one BenchmarkCollectiveOpRpc will have non-zero result
                // (i.e., the one sent to the gather-tree root node).
                uint32_t offset = 0;
                uint64_t gatherNs = 0;
                while (offset < result->size()) {
                    result->read<WireFormat::BenchmarkCollectiveOp::Response>(
                            &offset);
                    uint64_t ns = *result->read<uint64_t>(&offset);
                    if (ns > 0) {
                        gatherNs = ns;
                    }
                }
                if (i >= WARMUP_COUNT) {
                    rpc->replyPayload->emplaceAppend<uint64_t>(gatherNs);
                }
            }
        } else {
            class DataMerger : public TreeGather::Merger {
              public:
                explicit DataMerger(bool isRoot)
                    : bytesReceived()
                    , isRoot(isRoot)
                    , localBuf()
                    , mutex("DataMerger::mutex")
                {}

                void add(Buffer* dataBuf)
                {
                    // Quick hack to skip processing received data at the root
                    // node.
                    if (isRoot) {
                        bytesReceived += downCast<int>(dataBuf->size());
                    } else {
                        SpinLock::Guard _(mutex);
                        localBuf.appendCopy(dataBuf);
                    }
                }

                Buffer* getResult() { return &localBuf; }

                std::atomic<int> bytesReceived;

                bool isRoot;

                Buffer localBuf;

                SpinLock mutex;
            };
            DataMerger merger(world->rank == 0);
            merger.localBuf.appendCopy(data.get(), reqHdr->dataSize);

            uint64_t startTime = context->clockSynchronizer->getConverter(
                    ServerId(reqHdr->masterId)).toLocalTsc(reqHdr->startTime);
            if (Cycles::rdtsc() > startTime) {
                LOG(ERROR, "Gather operation started %.2f us late!",
                        Cycles::toSeconds(Cycles::rdtsc() - startTime)*1e6);
            }
            while (Cycles::rdtsc() < startTime) {}
            timeTrace("Gather operation started, run %d", reqHdr->count);

            const int DUMMY_OP_ID = 999;
            Tub<TreeGather> gatherOp;
            invokeCollectiveOp<TreeGather>(gatherOp, DUMMY_OP_ID, context,
                    world.get(), 0, reqHdr->dataSize, data.get(), &merger);
            gatherOp->wait();
            uint64_t endTime = Cycles::rdtsc();
            timeTrace("Gather operation completed");
            // TODO: the cleanup is not very nice; how to do RAII?
            removeCollectiveOp(DUMMY_OP_ID);

            uint64_t gatherNs = 0;
            if (world->rank == 0) {
                gatherNs = Cycles::toNanoseconds(endTime - startTime);
                // Verify if the root has the right amount of data in the end.
                int totalBytes = int(reqHdr->dataSize) + merger.bytesReceived;
                if (totalBytes != world->size() * int(reqHdr->dataSize)) {
                    LOG(ERROR, "Unexpected final data size %d", totalBytes);
                }
            }
            rpc->replyPayload->emplaceAppend<uint64_t>(gatherNs);

        }
    } else if (reqHdr->opcode == WireFormat::ALL_GATHER) {
        /* All-gather benchmark */

        // The master node is responsible for running many iterations of the
        // experiment and recording the completion time of each run. Note that
        // the experiment relies on clock sync. to ensure all participating
        // nodes invokes/enters the collective operation at the same time. To
        // start a run, the master node broadcasts the BenchmarkCollectiveOpRpc
        // to all participating nodes which includes a starting time. Upon the
        // receipt of the benchmark RPC, a node waits until the starting time
        // to invoke the collective op. The completion time is computed on each
        // node independently and sent back via the response of benchmark
        // RPC. The outer broadcast op that carries the benchmark RPC then
        // return the completion times of all nodes to the master node.
        if (reqHdr->masterId == 0) {
            for (int i = 0; i < WARMUP_COUNT + reqHdr->count; i++) {
                // Start the operation 100 us from now, which should be enough
                // for the broadcast message to reach all the participants.
                uint64_t startTime = Cycles::rdtsc() +
                        Cycles::fromMicroseconds(100);
                TreeBcast bcast(context, world.get());
                bcast.send<BenchmarkCollectiveOpRpc>(i, reqHdr->opcode,
                        reqHdr->dataSize, serverId.getId(), startTime);
                Buffer* result = bcast.wait();

                // The completion time for the all-gather operation is the
                // largest completion time on all nodes.
                uint32_t offset = 0;
                uint64_t allGatherNs = 0;
                while (offset < result->size()) {
                    result->read<WireFormat::BenchmarkCollectiveOp::Response>(
                            &offset);
                    uint64_t ns = *result->read<uint64_t>(&offset);
                    if (ns > allGatherNs) {
                        allGatherNs = ns;
                    }
                }
                if (i >= WARMUP_COUNT) {
                    rpc->replyPayload->emplaceAppend<uint64_t>(allGatherNs);
                }
            }
        } else {
            class DataMerger : public AllGather::Merger {
              public:
                explicit DataMerger() : localBuf() {}

                void append(Buffer* dataBuf) { localBuf.appendCopy(dataBuf); }

                void clear() { localBuf.reset(); }

                Buffer* getResult() { return &localBuf; }

                Buffer localBuf;
            };
            DataMerger merger;
            merger.localBuf.appendExternal(data.get(), reqHdr->dataSize);

            uint64_t startTime = context->clockSynchronizer->getConverter(
                    ServerId(reqHdr->masterId)).toLocalTsc(reqHdr->startTime);
            if (Cycles::rdtsc() > startTime) {
                LOG(ERROR, "Gather operation started %.2f us late!",
                        Cycles::toSeconds(Cycles::rdtsc() - startTime)*1e6);
            }
            while (Cycles::rdtsc() < startTime) {}
            timeTrace("AllGather operation started, run %d", reqHdr->count);

            const int DUMMY_OP_ID = 999;
            Tub<AllGather> allGatherOp;
            invokeCollectiveOp<AllGather>(allGatherOp, DUMMY_OP_ID, context,
                    world.get(), reqHdr->dataSize, data.get(), &merger);
            allGatherOp->wait();
            uint64_t endTime = Cycles::rdtsc();
            timeTrace("AllGather operation completed");
            // TODO: the cleanup is not very nice; how to do RAII?
            removeCollectiveOp(DUMMY_OP_ID);

            uint64_t allGatherNs = 0;
            if (world->rank == 0) {
                allGatherNs = Cycles::toNanoseconds(endTime - startTime);
            }
            rpc->replyPayload->emplaceAppend<uint64_t>(allGatherNs);

            // Verify if we have the right amount of data in the end.
            if (merger.localBuf.size() !=
                    uint32_t(world->size()) * reqHdr->dataSize) {
                LOG(ERROR, "Unexpected final data size %u",
                        merger.localBuf.size());
                timeTrace("Unexpected final data size %d",
                        merger.localBuf.size());
            }
        }
    } else if (reqHdr->opcode == WireFormat::ALL_SHUFFLE) {
        /* AllShuffle benchmark */
        if (reqHdr->masterId == 0) {
            for (int i = 0; i < WARMUP_COUNT + reqHdr->count; i++) {
                // Start the operation 100 us from now, which should be enough
                // for the broadcast message to reach all the participants.
                uint64_t startTime = Cycles::rdtsc() +
                        Cycles::fromMicroseconds(100);
                TreeBcast bcast(context, world.get());
                bcast.send<BenchmarkCollectiveOpRpc>(i, reqHdr->opcode,
                        reqHdr->dataSize, serverId.getId(), startTime);
                Buffer* result = bcast.wait();
                uint64_t elapsedMicros =
                        Cycles::toMicroseconds(Cycles::rdtsc() - startTime);
                timeTrace("Broadcast of ALL_SHUFFLE operations completed");

                // Take the completion time of the slowest node as the
                // completion time of the shuffle operation.
                uint32_t offset = 0;
                while (offset < result->size()) {
                    result->read<WireFormat::BenchmarkCollectiveOp::Response>(
                            &offset);
                    uint32_t rank = *result->read<uint32_t>(&offset);
                    uint32_t shuffleNs = *result->read<uint32_t>(&offset);
                    if (i >= WARMUP_COUNT) {
                        // Note: the local elapsed time is always larger than the
                        // the shuffle completion time because the former also
                        // includes the time to send/receive the payload RPC reply
                        // (note: the payload RPC of bcast is currently delivered
                        // over the network even though it's actually a local op)
                        // and the broadcast return time.
                        LOG(DEBUG, "Master node elapsed time %lu us, "
                                "shuffle time %u us", elapsedMicros,
                                shuffleNs / 1000);
                        rpc->replyPayload->emplaceAppend<uint32_t>(rank);
                        rpc->replyPayload->emplaceAppend<uint32_t>(shuffleNs);
                    }
                }
            }
        } else {
            std::atomic<int> bytesReceived(0);
            auto merger = [&bytesReceived] (int _, Buffer* dataBuffer) {
                bytesReceived.fetch_add(dataBuffer->size());
                // Copy the pulled data into contiguous space.
//                dataBuffer->getRange(0, dataBuffer->size());
            };

            std::unique_ptr<Tub<ShufflePullRpc>[]> pullRpcs(
                    new Tub<ShufflePullRpc>[world->size()]);
            uint64_t startTime = context->clockSynchronizer->getConverter(
                    ServerId(reqHdr->masterId)).toLocalTsc(reqHdr->startTime);
            if (Cycles::rdtsc() > startTime) {
                LOG(ERROR, "AllShuffle operation started %.2f us late!",
                        Cycles::toSeconds(Cycles::rdtsc() - startTime)*1e6);
            }
            while (Cycles::rdtsc() < startTime) {}
            timeTrace("ALL_SHUFFLE benchmark started, run %d", reqHdr->count);
            // FIXME: quick hack to get better perf. numbers for small & large
            // messages; for small msgs, we need to send more concurrent rpcs to
            // avoid bottlenecked on network latency; for large msgs, having
            // more than 2 message at a time make the distributed matching
            // appears to deviate from the optimal behavior.
            int maxRpcs = reqHdr->dataSize >= 100*1000 ? 2 : MAX_OUTSTANDING_RPCS;
            invokeShufflePull(pullRpcs.get(), world.get(), maxRpcs,
                    ALLSHUFFLE_BENCHMARK, merger, reqHdr->dataSize);
            uint32_t shuffleNs = downCast<uint32_t>(
                    Cycles::toNanoseconds(Cycles::rdtsc() - startTime));
            timeTrace("ALL_SHUFFLE benchmark run %d completed in %u us, "
                    "received %u bytes", reqHdr->count, shuffleNs / 1000,
                    bytesReceived.load());
            rpc->replyPayload->emplaceAppend<uint32_t>(world->rank);
            rpc->replyPayload->emplaceAppend<uint32_t>(shuffleNs);
            // It could take a while to destroy large ShufflePullRpc's; better
            // send reply first.
            rpc->sendReply();
        }
    } else {
        respHdr->common.status = STATUS_INVALID_PARAMETER;
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
        case POINT2POINT_BENCHMARK: {
            // Return the local time back to the sender so it can compute the
            // one-way delay using synchronized clock.
            rpc->replyPayload->emplaceAppend<uint64_t>(Cycles::rdtsc());
        }
        default:
            assert(false);
    }
}

} // namespace RAMCloud
