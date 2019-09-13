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

#include <emmintrin.h>
#include <immintrin.h>

#include "Arachne/DefaultCorePolicy.h"
#include "ClockSynchronizer.h"
#include "MilliSortService.h"
#include "AllShuffle.h"
#include "Broadcast.h"
#include "TimeTrace.h"

namespace RAMCloud {

// Change 0 -> 1 in the following line to compile detailed time tracing in
// this transport.
#define TIME_TRACE 1

#define TIME_TRACE_SP 1

#if 0
/// Hack: redefine timeTrace to log so that we can use it even when server crashes.
#define timeTrace(format, ...) RAMCLOUD_LOG(NOTICE, format, ##__VA_ARGS__)
#define timeTraceSp(format, ...) RAMCLOUD_LOG(NOTICE, format, ##__VA_ARGS__)
#else

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

    inline void
    timeTraceSp(const char* format,
            uint64_t arg0 = 0, uint64_t arg1 = 0, uint64_t arg2 = 0,
            uint64_t arg3 = 0)
    {
#if TIME_TRACE_SP
        TimeTrace::record(format, uint32_t(arg0), uint32_t(arg1),
                uint32_t(arg2), uint32_t(arg3));
#endif
    }
}

#endif

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

// Change 0 -> 1 in the following line to sort local keys before starting
// millisort.
#define BYPASS_LOCAL_SORT 1

// Change 0 -> 1 in the following line to overlap init. value rearrangement
// with the subsequent partition steps and key shuffle.
#define OVERLAP_INIT_VAL_REARRANGE 1

// Change 0 -> 1 in the following line to overlap merge-sorting incoming keys
// with value shuffle.
#define OVERLAP_MERGE_SORT 1

// Change 0 -> 1 in the following line to log intermediate computation result at
// each stage of the algorithm. Note: must be turned off during benchmarking.
#define LOG_INTERMEDIATE_RESULT 0

#define LOG_FINAL_RESULT 0

// Change 0 -> 1 to enable final result validation.
#define VALIDATE_RESULT 1

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
    , zeroCopyMemoryRegion(const_cast<void*>(context->masterZeroCopyRegion))
    , communicationGroupTable()
    , ongoingMilliSort()
    , rand()
    , startTime()
    , partitionStartTime()
    , pivotServerRank(-1)
    , isPivotServer()
    , numDataTuples(-1)
    , numPivotServers(-1)
    , localKeys((PivotKey*) zeroCopyMemoryRegion)
    , incomingKeys(localKeys + MAX_RECORDS_PER_NODE)
    , sortedKeys()
    , localValues((Value*) incomingKeys + MAX_RECORDS_PER_NODE)
    , localValues0(localValues + MAX_RECORDS_PER_NODE)
    , sortedValues(localValues0 + MAX_RECORDS_PER_NODE)
    , sortedValues0(sortedValues + MAX_RECORDS_PER_NODE)
    , numSortedItems(0)
    , numValuesReceived()
    , printingResult()
    , valueStartIdx()
    , localPivots()
    , rearrangeLocalVals()
    , dataBucketBoundaries()
    , dataBucketRanges()
    , shuffleKeysRxCount()
    , shuffleValsRxCount()
    , shuffleKeysRxFrom()
    , shuffleValsRxFrom()
    , mergeSorter()
    , mergeSorterIsReady()
    , mergeSorterLock("MilliSortService::mergeSorter")
    , pendingMergeReqs()
    , world()
    , myPivotServerGroup()
    , gatheredPivots()
    , superPivots()
    , pivotBucketBoundaries()
    , sortedGatheredPivots()
    , numSmallerPivots()
    , numPivotsInTotal()
    , allPivotServers()
    , bcastDataBucketBoundaries()
    , globalSuperPivots()
    , bcastPivotBucketBoundaries()
{
    context->services[WireFormat::MILLISORT_SERVICE] = this;
    if (uint64_t(localKeys) % 64) {
        DIE("localKeys not aligned to cache line!");
    }
    if (uint64_t(localValues) % 64) {
        DIE("localValues not aligned to cache line!");
    }
    if (uint64_t(localValues0) % 64) {
        DIE("localValues0 not aligned to cache line!");
    }
    if (uint64_t(incomingKeys) % 64) {
        DIE("incomingKeys not aligned to cache line!");
    }
    if (uint64_t(sortedValues) % 64) {
        DIE("sortedalues not aligned to cache line!");
    }
    if (uint64_t(sortedValues0) % 64) {
        DIE("sortedValues0 not aligned to cache line!");
    }
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
        case WireFormat::ShufflePush::opcode:
            callHandler<WireFormat::ShufflePush, MilliSortService,
                        &MilliSortService::shufflePush>(rpc);
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
    timeTrace("initMilliSort invoked, id %u, dataTuplesPerServer %u, "
              "nodesPerPivotServer %u", reqHdr->id, reqHdr->dataTuplesPerServer,
              reqHdr->nodesPerPivotServer);

    // Mutex used to serialize calls of this method.
    static Arachne::SpinLock mutex("MilliSortService::initMilliSort");
    static uint32_t lastInitRpcId;
    static WireFormat::InitMilliSort::Response lastInitRpcResp;

    std::lock_guard<Arachne::SpinLock> _(mutex);

    // Check if we have just finished processing the same init. RPC; if so,
    // just return the same response stored earlier.
    bool duplicateInitReq = (reqHdr->id == lastInitRpcId);
    if (duplicateInitReq) {
        LOG(WARNING, "Skipped duplicate InitMilliSort RPC (id = %u)",
                reqHdr->id);
        *respHdr = lastInitRpcResp;
        return;
    } else {
        lastInitRpcId = reqHdr->id;
        lastInitRpcResp = {};
    }

    // We are busy serving another sorting request.
    if (ongoingMilliSort) {
        LOG(WARNING, "Sorting already in progress!");
        respHdr->common.status = STATUS_SORTING_IN_PROGRESS;
        return;
    }

    // Initialize the world communication group so we know the rank of this
    // server.
    initWorld(reqHdr->numNodes);

    // Initialization request from an external client should only be processed
    // by the root node, which also performs error checking.
    if (reqHdr->fromClient) {
        if ((world->rank > 0) ||
                (numDataTuples * MAX_IMBALANCE_RATIO > MAX_RECORDS_PER_NODE)) {
            respHdr->common.status = STATUS_MILLISORT_ERROR;
            return;
        }
    }

    // Initiate the init. RPCs to its children nodes in the world group;
    // unfortunately, we can't (re-)use the broadcast mechanism because the
    // world group has not been setup on the target nodes.
    std::list<InitMilliSortRpc> outgoingRpcs;
    const int fanout = 4;
    for (int i = 0; i < fanout; i++) {
        int rank = world->rank * fanout + i + 1;
        if (rank >= world->size()) {
            break;
        }
        outgoingRpcs.emplace_back(context, world->getNode(rank),
                reqHdr->numNodes, reqHdr->dataTuplesPerServer,
                reqHdr->nodesPerPivotServer, false);
    }

    // Initialize member fields.
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

    // Generate data tuples.
    rand.construct(serverId.indexNumber());
    for (int i = 0; i < numDataTuples; i++) {
#if USE_PSEUDO_RANDOM
        uint64_t key = rand->next();
#else
        uint64_t key = uint64_t(world->rank + i * world->size());
#endif
        new(&localKeys[i]) PivotKey(key,
                downCast<uint16_t>(serverId.indexNumber()),
                downCast<uint32_t>(i));
        new(&localValues0[i]) Value(key);
    }
#if BYPASS_LOCAL_SORT
    std::sort(localKeys, localKeys + numDataTuples);
#endif
    sortedKeys = NULL;

    while (printingResult) {
        Arachne::yield();
    }
    numSortedItems = 0;
    numValuesReceived = 0;
    valueStartIdx.clear();
    valueStartIdx.resize(world->size(), 0);

    //
    localPivots.clear();
    rearrangeLocalVals.construct(localKeys, localValues0, localValues);
    dataBucketBoundaries.clear();
    dataBucketRanges.clear();
    shuffleKeysRxCount = 0;
    shuffleValsRxCount = 0;
    shuffleKeysRxFrom.construct(world->size());
    shuffleValsRxFrom.construct(world->size());
    mergeSorter.construct(world->size(), numDataTuples * world->size() * 2,
            int(Arachne::getCorePolicy()->getCores(0).size()));
    mergeSorterIsReady = false;
    pendingMergeReqs.clear();
    gatheredPivots.clear();
    superPivots.clear();
    pivotBucketBoundaries.clear();
    sortedGatheredPivots.clear();
    numSmallerPivots = 0;
    numPivotsInTotal = 0;
    if (isPivotServer) {
        bcastDataBucketBoundaries.construct(context, myPivotServerGroup.get());
    }
    globalSuperPivots.clear();
    if (world->rank == 0) {
        bcastPivotBucketBoundaries.construct(context, allPivotServers.get());
    }

    // Flush LLC to avoid generating unrealistic numbers; we are doing
    // memory-to-memory sorting (not cache-to-cache sorting)!
    flushSharedCache();
    timeTrace("initMilliSort: flushed shared cache; ready for benchmark");

    // Wait for its children nodes to finish initialization.
    while (!outgoingRpcs.empty()) {
        InitMilliSortRpc* initRpc = &outgoingRpcs.front();
        if (initRpc->isReady()) {
            auto initResult = initRpc->wait();
            respHdr->numNodesInited += initResult->numNodesInited;
            outgoingRpcs.pop_front();
        }
        Arachne::yield();
    }

    // Fill out the RPC response.
    respHdr->numNodesInited += 1;
    if (reqHdr->fromClient) {
        respHdr->numNodes = respHdr->numNodesInited;
        respHdr->numCoresPerNode = static_cast<int>(
                Arachne::numActiveCores.load());
        respHdr->numPivotsPerNode = respHdr->numNodes;
        respHdr->keySize = PivotKey::KEY_SIZE;
        respHdr->valueSize = Value::SIZE;
    }
    lastInitRpcResp = *respHdr;
    timeTrace("initMilliSort: rank %d initialized %d millisort nodes",
            world->rank, respHdr->numNodesInited);
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
    timeTrace("startMilliSort invoked, requestId %u, fromClient %u",
            uint(reqHdr->requestId), reqHdr->fromClient);

    // Mutex used to serialize calls of this method.
    static Arachne::SpinLock mutex("MilliSortService::startMilliSort");
    static int32_t lastMilliSortId = -1;

    // Start request from external client should only be processed by the root
    // node, which would then broadcast the start request to all nodes.
    if (reqHdr->fromClient) {
        if (world->rank > 0) {
            respHdr->common.status = STATUS_MILLISORT_ERROR;
            return;
        }

        if (mutex.try_lock()) {
            if (reqHdr->requestId == lastMilliSortId) {
                LOG(WARNING, "Skipped duplicate StartMilliSort RPC from client,"
                        " requestId = %d", reqHdr->requestId);
                return;
            }
            mutex.unlock();
        } else {
            // Can't acquire the lock: there must another millisort in progress;
            // retry later.
            respHdr->common.status = STATUS_RETRY;
            return;
        }

        // Set the global time where all nodes should start millisort.
        startTime = Cycles::rdtsc() + Cycles::fromMicroseconds(200);

        // Start broadcasting the start request.
        TreeBcast startBcast(context, world.get());
        startBcast.send<StartMilliSortRpc>(reqHdr->requestId, startTime, false);
        startBcast.wait();
        timeTrace("MilliSort finished at master node, requestId %d",
                reqHdr->requestId);
        return;
    }

    // Acquire the lock to make sure no ongoing millisort is running on this
    // server. Skip millisort if we have received a duplicate RPC request.
    std::lock_guard<Arachne::SpinLock> _(mutex);
    bool duplicateRequest = (reqHdr->requestId == lastMilliSortId);
    if (duplicateRequest) {
        LOG(WARNING, "Skipped duplicate StartMilliSort RPC (requestId = %d)",
                reqHdr->requestId);
        return;
    } else {
        lastMilliSortId = reqHdr->requestId;
    }

    // Wait until the designated start time set by the root node.
    TimeConverter converter =
            context->clockSynchronizer->getConverter(world->getNode(0));
    startTime = converter.isValid() ?
            converter.toLocalTsc(reqHdr->startTime) : Cycles::rdtsc();
    if (Cycles::rdtsc() - startTime > Cycles::fromMicroseconds(1)) {
        LOG(ERROR, "Millisort started %.2f us late!",
                Cycles::toSeconds(Cycles::rdtsc() - startTime)*1e6);
    }
    while (Cycles::rdtsc() < startTime) {
        Arachne::yield();
    }
    timeTrace("MilliSort algorithm started, requestId %d", reqHdr->requestId);

    // Kick start the sorting process.
    ADD_COUNTER(millisortIsPivotServer, isPivotServer ? 1 : 0);
    ongoingMilliSort = rpc;
    localSortAndPickPivots();
    // FIXME: a better solution is to wait on condition var.
    while (ongoingMilliSort != NULL) {
        Arachne::yield();
    }
    timeTrace("MilliSort algorithm stopped, requestId %d", reqHdr->requestId);
}

void
MilliSortService::copyValue(void* dst, const void* src)
{
#if __AVX2__
    static_assert(Value::SIZE == 96, "Illegal Value::SIZE!");
    __m256i ymm1 = _mm256_stream_load_si256((__m256i*)src + 0);
    __m256i ymm2 = _mm256_stream_load_si256((__m256i*)src + 1);
    __m256i ymm3 = _mm256_stream_load_si256((__m256i*)src + 2);
    _mm256_stream_si256((__m256i*)dst + 0, ymm1);
    _mm256_stream_si256((__m256i*)dst + 1, ymm2);
    _mm256_stream_si256((__m256i*)dst + 2, ymm3);
#else
    memcpy(dst, src, Value::SIZE);
#endif
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
void
MilliSortService::rearrangeValues(RearrangeValueTask* task, int totalItems,
        bool initialData)
{
//    LOG(NOTICE, "keys at %p (seq. read), src. values at %p (random read) "
//            "dest. values at %p (seq. write)", keys, src, dest);
    task->startTime = Cycles::rdtsc();
    uint64_t* numWorkers;
    if (initialData) {
        ADD_COUNTER(rearrangeInitValuesStartTime, task->startTime - startTime);
        numWorkers = &PerfStats::threadStats.rearrangeInitValuesWorkers;
    } else {
        ADD_COUNTER(rearrangeFinalValuesStartTime, task->startTime - startTime);
        numWorkers = &PerfStats::threadStats.rearrangeFinalValuesWorkers;
    }

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
    auto workerMain = [task, totalItems, initialData]
    {
        // Unpack arguments.
        PivotKey* keys = task->keys;
        Value* src = task->src;
        Value* dest = task->dst;

        timeTrace("RearrangeValue worker started, coreId %d", Arachne::core.id);
        uint64_t* cpuTime = initialData ?
                &PerfStats::threadStats.rearrangeInitValuesCycles :
                &PerfStats::threadStats.rearrangeFinalValuesCycles;

        // FIXME: how should I choose this number? too large => load imbalance
        // (not all HTs run equally fast: hypertwin of dispatch thread and the
        // hypertwin of the non-Arachne core runs faster); too small => cache
        // thrashing between HTs?? messing up with memory prefetcher?
        const int maxItemsPerTask = 128;
        int workDone = 0;
        uint64_t busyTime = 0;
        while (true) {
            uint64_t now = Cycles::rdtsc();
            int startIdx = task->numSortedValues->fetch_add(maxItemsPerTask);
            int numItems = std::min(maxItemsPerTask, totalItems - startIdx);
            if (numItems <= 0) {
                break;
            }

            int endIdx = startIdx + numItems;
            for (int i = startIdx; i < endIdx; i++) {
                // Note: it's OK to leave the stale value in keys[i].index
                // because after rearrangement, the correspondence between keys
                // and values is trivial (ie. keys[i] -> values[i]).
                copyValue(dest + i, &src[keys[i].index]);
            }

            workDone += numItems;
            busyTime += (Cycles::rdtsc() - now);
            Arachne::yield();
        }
        task->workerDone(Cycles::rdtsc());
        timeTrace("Worker rearranged %d values, busy %lu us, cpu %d, "
                "initialData %u", workDone, Cycles::toMicroseconds(busyTime),
                Arachne::core.id, initialData);
        (*cpuTime) += busyTime;
    };

    // Spin up worker threads on all cores we are allowed to use.
    timeTrace("Spawning workers on coreId %d", Arachne::core.id);
    Arachne::CorePolicy::CoreList coreList =
            Arachne::getCorePolicy()->getCores(0);
    for (uint i = 0; i < coreList.size(); i++) {
        int coreId = coreList[i];
        if (initialData && (coreId == Arachne::core.id)) {
            continue;
        }
        task->workers.push_back(
                Arachne::createThreadOnCore(coreId, workerMain));
        (*numWorkers)++;
    }
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
#if BYPASS_LOCAL_SORT
        // Bypass local sorting until we integrate the ips4o module.
        // Assume ~11 ns/item using 10 physical cores (or 20 lcores).
        uint64_t localSortCostMicros =
                uint64_t(double(numDataTuples) * 11 / 1000.0);
        uint64_t deadline = Cycles::rdtsc() +
                Cycles::fromMicroseconds(localSortCostMicros);
        // Prefetch sorted keys into LLC to simulate the cache state after sort.
        prefetchMemoryBlock<_MM_HINT_T2>(localKeys,
                downCast<uint32_t>(numDataTuples) * PivotKey::SIZE);
        while (Cycles::rdtsc() < deadline) {
            Arachne::yield();
        }
#else
        std::sort(localKeys, localKeys + numDataTuples);
#endif
    }
    timeTrace("sorted %d keys", numDataTuples);

    // Overlap rearranging initial values with subsequent stages. This only
    // needs to be done before key (actually, value) shuffle.
    rearrangeLocalVals->callback = [this] () {
        timeTrace("rearrangeLocalVals completed;");
        std::lock_guard<Arachne::SpinLock> _(mergeSorterLock);
        mergeSorter->prepareThreads();
        mergeSorterIsReady = true;
        timeTrace("mergeSorter ready to run, %u pending requests",
                pendingMergeReqs.size());
        for (auto& request : pendingMergeReqs) {
            mergeSorter->poll(request.first, request.second);
        }
        pendingMergeReqs.clear();
    };
    rearrangeValues(rearrangeLocalVals.get(), numDataTuples, true);

    // FIXME: to avoid interfering with the partition phase, block till finish.
#if !OVERLAP_INIT_VAL_REARRANGE
    uint64_t elapsedTime = rearrangeLocalVals->wait();
    timeTrace("rearranging init. values completed");
    ADD_COUNTER(rearrangeInitValuesElapsedTime, elapsedTime)
#endif

    // Pick local pivots that partitions the local keys evenly.
    partition(localKeys, numDataTuples, std::min(numDataTuples, world->size()),
            &localPivots);
    timeTrace("picked %lu local pivots, about to gather pivots",
            localPivots.size());
    debugLogKeys("local pivots: ", &localPivots);

    // Initiate the gather op that collects pivots to the pivot server.
    // Merge pivots from slave nodes as they arrive.
    partitionStartTime = Cycles::rdtsc();
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
        timeTrace("gathered %lu pivots from %u nodes", gatheredPivots.size(),
                uint(myPivotServerGroup->size()));
        pickSuperPivots();
    } else {
        timeTrace("sent %lu local pivots", localPivots.size());
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
    timeTrace("picked %lu super-pivots, about to gather super-pivots",
            superPivots.size());
    debugLogKeys("super-pivots: ", &superPivots);

    // Initiate the gather op that collects super pivots to the root node.
    // Merge super pivots from pivot servers as they arrive.
    globalSuperPivots = superPivots;
    PivotMergeSorter merger(this, &globalSuperPivots);
    if (allPivotServers->size() > 1) {
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
        timeTrace("gathered %lu super-pivots from %u pivot servers",
                globalSuperPivots.size(), uint(allPivotServers->size()));
        pickPivotBucketBoundaries();
    } else {
        timeTrace("sent %lu super-pivots", superPivots.size());
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
    timeTrace("picked %lu pivot bucket boundaries, about to broadcast",
            pivotBucketBoundaries.size());
    ADD_COUNTER(bcastPivotBucketBoundariesStartTime,
            Cycles::rdtsc() - startTime);
//    SCOPED_TIMER(bcastPivotBucketBoundariesElapsedTime);
    START_TIMER(bcastPivotBucketBoundariesElapsedTime);
    bcastPivotBucketBoundaries->send<SendDataRpc>(
            BROADCAST_PIVOT_BUCKET_BOUNDARIES,
            // TODO: simplify the SendDataRpc api?
            uint32_t(PivotKey::SIZE * pivotBucketBoundaries.size()),
            pivotBucketBoundaries.data());
    // Note: there is no need to wait for the broadcast to finish as we are not
    // going to modify #pivotBucketBoundaries.
//    bcastPivotBucketBoundaries->wait();
    STOP_TIMER(bcastPivotBucketBoundariesElapsedTime);

    timeTrace("broadcast to %d pivot servers", allPivotServers->size());
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
        timeTrace("inplace-merged %lu and %lu pivots", oldSize,
                sortedGatheredPivots.size() - oldSize);
    };

    // Send pivots to their designated pivot servers determined by the pivot
    // bucket boundaries.
    timeTrace("about to shuffle pivots");
    {
        ADD_COUNTER(bucketSortPivotsStartTime, Cycles::rdtsc() - startTime);
        SCOPED_TIMER(bucketSortPivotsElapsedTime);
        Tub<AllShuffle> pivotsShuffle;
        // TODO: this is out-dated; change to invokeShufflePull???
        invokeCollectiveOp<AllShuffle>(pivotsShuffle, ALLSHUFFLE_PIVOTS,
                context, allPivotServers.get(), &merger);
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
            dataBucketBoundaries.push_back(sortedGatheredPivots[localIdx]);
        }
    }
    timeTrace("pivot server picked %lu data bucket boundaries locally",
            dataBucketBoundaries.size());
    debugLogKeys("data bucket boundaries (local portion): ",
            &dataBucketBoundaries);

    // All-gather data bucket boundaries between pivot servers.
    timeTrace("about to all-gather data bucket boundaries");
    {
//        std::vector<PivotKey> local = dataBucketBoundaries;
        PivotMerger merger(this, &dataBucketBoundaries);
        ADD_COUNTER(allGatherPivotsStartTime, Cycles::rdtsc() - startTime);
        SCOPED_TIMER(allGatherPivotsElapsedTime);
        Tub<AllGather> allGather;
        invokeCollectiveOp<AllGather>(allGather,
                ALLGATHER_DATA_BUCKET_BOUNDARIES, context,
                allPivotServers.get(),
                uint32_t(dataBucketBoundaries.size() * PivotKey::SIZE),
                dataBucketBoundaries.data(), &merger);
        allGather->wait();
        removeCollectiveOp(ALLGATHER_DATA_BUCKET_BOUNDARIES);
        ADD_COUNTER(allGatherPivotsMergeCycles, merger.activeCycles);
    }
    timeTrace("gathered %lu data bucket boundaries",
            dataBucketBoundaries.size());

    // Sort the gathered data bucket boundaries.
    std::sort(dataBucketBoundaries.begin(), dataBucketBoundaries.end());
    debugLogKeys("data bucket boundaries: ", &dataBucketBoundaries);

    // Broadcast data bucket boundaries to slave nodes.
    timeTrace("about to broadcast %lu data bucket boundaries to %d slave nodes",
            dataBucketBoundaries.size(), myPivotServerGroup->size());
    bcastDataBucketBoundaries->send<SendDataRpc>(
            BROADCAST_DATA_BUCKET_BOUNDARIES,
            uint32_t(PivotKey::SIZE * dataBucketBoundaries.size()),
            dataBucketBoundaries.data());
    // Note: there is no need to wait for the broadcast to finih as we are not
    // going to modify #dataBucketBoundaries anymore.
//    bcastDataBucketBoundaries->wait();

    // Pivot servers can advance to the final data shuffle immediately while
    // other nodes will have to wait until they receive the data bucket
    // boundaries via broadcast.
    shuffleKeys();
}

void
MilliSortService::shuffleKeys() {
    timeTrace("=== Stage 6: shuffleKeys started");
    ADD_COUNTER(partitionElapsedTime, Cycles::rdtsc() - partitionStartTime);
    ADD_COUNTER(shuffleKeysStartTime, Cycles::rdtsc() - startTime);
    START_TIMER(shuffleKeysElapsedTime);

    // Partition the local keys and values according to the data bucket ranges;
    // this will be used to service the ShufflePull handler.
    // TODO: parallelize this step across all cores
    double keysPerPartition =
            double(numDataTuples) / double(dataBucketBoundaries.size());
    const int step = std::max(1, int(std::sqrt(keysPerPartition)));
    int rangeEnd = 0;
    for (const PivotKey& rightBound : dataBucketBoundaries) {
        int rangeStart = rangeEnd;
        while ((rangeEnd < numDataTuples) && (localKeys[rangeEnd] <= rightBound)) {
            rangeEnd = std::min(rangeEnd + step, numDataTuples);
        }
        while ((rangeEnd > rangeStart) && !(localKeys[rangeEnd-1] <= rightBound)) {
            rangeEnd--;
        }
        dataBucketRanges.emplace_back(rangeStart, rangeEnd - rangeStart);
    }
    timeTrace("partitioned keys to be ready for key shuffle");

    // Push keys to the right node they belong to (keys that should remain in
    // the local node are also sent in the same way to simplify code).
    timeTrace("about to shuffle keys");
    std::vector<Buffer> keysToSend(world->size());
    for (int i = 0; i < world->size(); i++) {
        int rank = (world->rank + i) % world->size();
        int keyIdx, numKeys;
        std::tie(keyIdx, numKeys) = dataBucketRanges[rank];
        keysToSend[i].append(&localKeys[keyIdx], numKeys * PivotKey::SIZE);
        ADD_COUNTER(shuffleKeysOutputBytes, keysToSend[i].size());
    }
    invokeShufflePush(world.get(), ALLSHUFFLE_KEY, &keysToSend);

    // Wait until we have received all keys that belong to us.
    while (shuffleKeysRxCount.load() < world->size()) {
        Arachne::yield();
    }

    // Local value rearrangement must be completed before shuffling values.
#if OVERLAP_INIT_VAL_REARRANGE
    uint64_t now = Cycles::rdtsc();
    uint64_t elapsedTime = rearrangeLocalVals->wait();
    if (rearrangeLocalVals->stopTime < now) {
        uint64_t slackTime = Cycles::rdtsc() - rearrangeLocalVals->stopTime;
        timeTrace("rearrangeLocalVals completed %u us ago, elapsed %u us",
                Cycles::toMicroseconds(slackTime),
                Cycles::toMicroseconds(elapsedTime));
    } else {
        uint64_t delayTime = Cycles::rdtsc() - now;
        timeTrace("rearrangeLocalVals completed %u us late, elapsed %u us",
                Cycles::toMicroseconds(delayTime),
                Cycles::toMicroseconds(elapsedTime));
    }
    ADD_COUNTER(rearrangeInitValuesElapsedTime, elapsedTime)
#endif

    STOP_TIMER(shuffleKeysElapsedTime);
    ADD_COUNTER(millisortFinalPivotKeyBytes, numSortedItems * PivotKey::SIZE);
    timeTrace("shuffle keys completed, received %d keys", numSortedItems);

    shuffleValues();
}

void
MilliSortService::shuffleValues()
{
    timeTrace("=== Stage 7: shuffleValues started");

    Arachne::ThreadId mergeSortPoller = Arachne::createThread([this] () {
        bool done = false;
        while (!done) {
            mergeSorterLock.lock();
            done = !mergeSorter->poll();
            mergeSorterLock.unlock();
            Arachne::yield();
        }
        auto sortedArray = mergeSorter->getSortedArray();
        assert(int(sortedArray.size) == numSortedItems);
        sortedKeys = sortedArray.data;
        ADD_COUNTER(onlineMergeSortStartTime,
                mergeSorter->startTime - startTime);
        ADD_COUNTER(onlineMergeSortElapsedTime,
                mergeSorter->stopTick - mergeSorter->startTime);
        ADD_COUNTER(onlineMergeSortWorkers, mergeSorter->numWorkers);
        timeTrace("sorted %u final keys", sortedArray.size);
    });

    // Ideally, online merge of final keys only need to complete before
    // rearranging final values. However, on rc machines, we don't have many
    // cores and sorting will introduce too much noise for value shuffling
#if !OVERLAP_MERGE_SORT
    Arachne::join(mergeSortPoller);
#endif

    ADD_COUNTER(shuffleValuesStartTime, Cycles::rdtsc() - startTime);
    START_TIMER(shuffleValuesElapsedTime);
    timeTrace("about to shuffle values");
    std::vector<Buffer> valuesToSend(world->size());
    for (int i = 0; i < world->size(); i++) {
        int rank = (world->rank + i) % world->size();
        int valueIdx, numValues;
        std::tie(valueIdx, numValues) = dataBucketRanges[rank];
        valuesToSend[i].append(&localValues[valueIdx], numValues * Value::SIZE);
        ADD_COUNTER(shuffleValuesOutputBytes, valuesToSend[i].size());
    }
    invokeShufflePush(world.get(), ALLSHUFFLE_VALUE, &valuesToSend);

    // Wait until we have received all final values that belong to us.
    while (shuffleValsRxCount.load() < world->size()) {
        Arachne::yield();
    }

    STOP_TIMER(shuffleValuesElapsedTime);
    ADD_COUNTER(millisortFinalValueBytes, numSortedItems * Value::SIZE);
    timeTrace("shuffle values completed");

    // Make sure the online merge of final keys completes before rearranging
    // final values. Normally, the online merge can be hidden completely within
    // value shuffle.
#if OVERLAP_MERGE_SORT
    Arachne::join(mergeSortPoller);
#endif

    timeTrace("about to rearrange final values");
    RearrangeValueTask rearrangeFinalVals(sortedKeys, sortedValues0,
            sortedValues);
    rearrangeValues(&rearrangeFinalVals, numSortedItems, false);
    ADD_COUNTER(rearrangeFinalValuesElapsedTime, rearrangeFinalVals.wait())

    ADD_COUNTER(millisortTime, Cycles::rdtsc() - startTime);
    ADD_COUNTER(millisortFinalItems, numSortedItems);
    timeTrace("millisort finished, numSortedItems %d", numSortedItems);

    // Clean up async. broadcast operations.
    if (bcastPivotBucketBoundaries) {
        bcastPivotBucketBoundaries->wait();
    }
    if (bcastDataBucketBoundaries) {
        bcastDataBucketBoundaries->wait();
    }

    printingResult = true;
    ongoingMilliSort.load()->sendReply();
    ongoingMilliSort = NULL;

#if VALIDATE_RESULT
    uint64_t prevKey = 0;
    bool success = true;
    if (int(mergeSorter->getSortedArray().size) != numSortedItems) {
        LOG(ERROR, "Validation failed: sortedArray.size != numSortedItems "
                "(%lu vs %u)", mergeSorter->getSortedArray().size,
                numSortedItems);
        success = false;
    }
    if (numValuesReceived.load() != numSortedItems) {
        LOG(ERROR, "Validation failed: #keys (%d) not equal to #values (%d)",
                numSortedItems, numValuesReceived.load());
        success = false;
    }
    for (int i = 0; i < numSortedItems; i++) {
        // TODO: how to check if keys are globally sorted?
        uint64_t key = sortedKeys[i].keyAsUint64();
        if (prevKey > key) {
            LOG(ERROR, "Validation failed: unordered keys, index %d", i);
            success = false;
            break;
        }
        if (key != sortedValues[i].asUint64()) {
            LOG(ERROR, "Validation failed: unequal key and value, index %d", i);
            success = false;
            break;
        }
        prevKey = key;
    }
    if (success) {
        LOG(NOTICE, "Validation passed");
    }
#endif

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
    uint64_t checksum = 0;
    uint64_t index = 0;
    for (PivotKey& sortKey : *keys) {
        result += std::to_string(sortKey.keyAsUint64());
        result += " ";
        index++;
        checksum += sortKey.keyAsUint64() * index;
    }
    LOG(NOTICE, "%s (csum = %s) %s", prefix, std::to_string(checksum).c_str(),
            result.c_str());
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

/**
 *
 * \param group
 * \param dataId
 * \param outMessages
 *      Shuffle messages to be sent to all the other nodes in the communication
 *      group, in clockwise order starting from the local node.
 */
void
MilliSortService::invokeShufflePush(CommunicationGroup* group, uint32_t dataId,
        std::vector<Buffer>* outMessages)
{
    assert(group->size() == outMessages->size());
    if (group->size() != outMessages->size()) {
        DIE("wtf??? not enought messages to send?");
    }

    struct ShuffleMessageTracker {
        /// Contains the ongoing RPC used to send a chunk of this message.
        Tub<ShufflePushRpc> outstandingRpc;

        /// Server ID of the message receiver.
        ServerId serverId;

        /// Size of the complete message, in bytes.
        uint32_t totalLength;

        /// Offset of the first byte yet to be transmitted.
        uint32_t offset;

        /// Range of the message yet to be transmitted.
        Buffer::Iterator messageIt;

        /// Time, in rdtsc ticks, when we send out #outstandingRpc.
        uint64_t lastTransmitTime;

        ShuffleMessageTracker(ServerId serverId, Buffer* message)
            : outstandingRpc()
            , serverId(serverId)
            , totalLength(message->size())
            , offset(0)
            , messageIt(message)
            , lastTransmitTime()
        {}

        ~ShuffleMessageTracker() {}

        void
        sendMessageChunk(Context* context, int senderId, uint32_t dataId,
                uint32_t size)
        {
            timeTraceSp("shuffle-push: sending message chunk to server %u, "
                    "offset %u, len %u, left %u", serverId.indexNumber(),
                    offset, size, totalLength - offset);
            assert(!outstandingRpc);
            size = std::min(size, messageIt.size());
            Buffer::Iterator payload(messageIt);
            payload.truncate(size);
            outstandingRpc.construct(context, serverId, senderId, dataId,
                    totalLength, offset, &payload);
            timeTraceSp("shuffle-push: push rpc constructed, rpcId %u",
                    outstandingRpc->rpcId);
            offset += size;
            messageIt.advance(size);
        }
    };

    uint64_t activeCycles = context->dispatch->dispatchActiveCycles;
    uint64_t totalBytesSent = 0;

    int totalMessages = group->size();
    std::unique_ptr<Tub<ShuffleMessageTracker>[]> messageTrackers(
            new Tub<ShuffleMessageTracker>[totalMessages]);
    for (int i = 0; i < totalMessages; i++) {
        Buffer* message = &(*outMessages)[i];
        messageTrackers[i].construct(group->getNode(group->rank + i), message);
        totalBytesSent += message->size();
    }

    // Performance stats.
    uint64_t shuffleStart = Cycles::rdtsc();
    uint32_t outstandingChunks = 0;

#define MAX_PUSH_CHUNK_SIZE 40000

    std::vector<int> candidates;
    int messagesUnfinished = totalMessages;

    // TODO: handle corner case first, send out all empty messages
    for (int i = 0; i < totalMessages; i++) {
        ShuffleMessageTracker* messageTracker = messageTrackers[i].get();
        if (messageTracker->totalLength == 0) {
            messageTracker->sendMessageChunk(context, group->rank, dataId, 0);
            outstandingChunks++;
            if (dataId == ALLSHUFFLE_KEY) {
                INC_COUNTER(shuffleKeysSentRpcs);
            } else if (dataId == ALLSHUFFLE_VALUE) {
                INC_COUNTER(shuffleValuesSentRpcs);
            }
        }
    }

    while (messagesUnfinished > 0) {
        // TODO: we might want to relax the "no outstanding RPC" restrict to
        // allow multiple chunks from the same message in flight; otherwise,
        // we might be limited by the network latency when we have only a few
        // messages left?

        uint32_t maxBytesLeft = 0;
        int index = -1;
        candidates.clear();

        // FIXME: hack? perform our own app-level rate control
        int transmitPendingRpcs = 0;

        // Each iteration of the following loop does one of the two things for
        // each message: (1) if there is no outstanding chunk being sent, check
        // for RPC completion; (2) otherwise, find the message with maximum
        // remaining bytes.
        for (int i = 0; i < totalMessages; i++) {
            // Check RPC completion.
            ShufflePushRpc* rpc = messageTrackers[i]->outstandingRpc.get();
            if (rpc) {
                transmitPendingRpcs += rpc->transmitDone ? 0 : 1;
                if (rpc->isReady()) {
                    // FIXME: RTT seems ridiculously high (e.g. 500 us); how is it
                    // possible? 40KB amounts to only 3.2 us in 100Gbps network,
                    // so 500 us is 156 packets! But we only have 38 servers!
                    // It's impossible for the network queue at the core/ToR
                    // switch to have that much data?!
                    // Perhaps the rx_queue_size = 16 in ofiud? PSM2 becomes
                    // extremely slow when running out of rx buffers?
                    outstandingChunks--;
                    timeTraceSp("shuffle-push: message chunk to server %u acked,"
                            " offset %u, RTT %u us, outstanding chunks %u",
                            messageTrackers[i]->serverId.indexNumber(),
                            messageTrackers[i]->offset,
                            Cycles::toMicroseconds(Cycles::rdtsc() -
                            messageTrackers[i]->lastTransmitTime),
                            outstandingChunks);
                    messageTrackers[i]->outstandingRpc.destroy();
                    if (messageTrackers[i]->messageIt.isDone()) {
                        messagesUnfinished--;
                        timeTraceSp("shuffle-push: message to server %u done, "
                                "%d messages left",
                                messageTrackers[i]->serverId.indexNumber(),
                                messagesUnfinished);
                    }
                } else {
                    // This message has an outstanding chunk being sent;
                    // do not consider it for transmission again until that
                    // chunk is completed.
                    continue;
                }
            }
            candidates.push_back(i);

            // TODO: is there any benefit to use strict LRPT policy over
            // random power-of-two policy? Vice versa?
            // TODO: should I try a random strategy ASAP?

            // Find next message chunk to send.
            uint32_t bytesLeft = messageTrackers[i]->messageIt.size();
            if (bytesLeft > maxBytesLeft) {
                index = downCast<int>(i);
                maxBytesLeft = bytesLeft;
            }
        }

#define MAX_TX_PENDING_RPCS 2

        // Change 1 -> 0 to turn off power-of-two policy (and use deterministic
        // LRPT policy)
#if 0
        const int MAX_CHOICES = 2;
        maxBytesLeft = 0;
        if (transmitPendingRpcs < MAX_TX_PENDING_RPCS) {
            for (int choice = 0; choice < MAX_CHOICES; choice++) {
                int idx = candidates[Util::wyhash64() % candidates.size()];
                uint32_t bytesLeft = messageTrackers[idx]->messageIt.size();
                if (bytesLeft > maxBytesLeft) {
                    maxBytesLeft = bytesLeft;
                    index = idx;
                }
            }
        }
        // TODO: the following buggy version gives better performance???
        // it was power-of-two SRPT by mistake...
//        if (transmitPendingRpcs < MAX_TX_PENDING_RPCS) {
//            uint64_t x0 = Util::wyhash64() % candidates.size();
//            uint64_t x1 = Util::wyhash64() % candidates.size();
////            uint64_t x2 = Util::wyhash64() % candidates.size();
//            uint32_t bytesLeft0 = messageTrackers[candidates[x0]]->messageIt.size();
//            uint32_t bytesLeft1 = messageTrackers[candidates[x1]]->messageIt.size();
////            uint32_t bytesLeft2 = messageTrackers[candidates[x2]]->messageIt.size();
//            if (bytesLeft0 < bytesLeft1) {
//                index = candidates[x0];
//            } else {
//                index = candidates[x1];
//            }
//        }
#endif

        // Send out one more chunk of data, if appropriate.
        if ((transmitPendingRpcs < MAX_TX_PENDING_RPCS) && (index >= 0)) {
            uint64_t now = Cycles::rdtsc();
            messageTrackers[index]->lastTransmitTime = now;
            messageTrackers[index]->sendMessageChunk(context, group->rank,
                    dataId, MAX_PUSH_CHUNK_SIZE);
            outstandingChunks++;

            if (dataId == ALLSHUFFLE_KEY) {
                INC_COUNTER(shuffleKeysSentRpcs);
            } else if (dataId == ALLSHUFFLE_VALUE) {
                INC_COUNTER(shuffleValuesSentRpcs);
            }
        }

        Arachne::yield();
    }

    activeCycles = context->dispatch->dispatchActiveCycles - activeCycles;
    uint64_t elapsed = Cycles::rdtsc() - shuffleStart;
    timeTraceSp("shuffle-push: operation completed, idle %u us, active %u us, "
            "active BW %u Gbps, actual BW %u Gbps",
            Cycles::toMicroseconds(elapsed - activeCycles),
            Cycles::toMicroseconds(activeCycles),
            uint32_t(double(totalBytesSent)*8e-9/Cycles::toSeconds(activeCycles)),
            uint32_t(double(totalBytesSent)*8e-9/Cycles::toSeconds(elapsed)));
}

void
MilliSortService::shufflePush(const WireFormat::ShufflePush::Request* reqHdr,
            WireFormat::ShufflePush::Response* respHdr, Rpc* rpc)
{
    uint32_t senderId = downCast<uint32_t>(reqHdr->senderId);

    // Reply to acknowledge the incoming data immediately; no need to wait
    // after processing the request.
    rpc->sendReply();

    switch (reqHdr->dataId) {
        case ALLSHUFFLE_KEY: {
            INC_COUNTER(shuffleKeysReceivedRpcs);

            // Prevent duplicate RPCs from messing up our memory.
            if (shuffleKeysRxCount == world->size()) {
                LOG(WARNING, "Ignore duplicate message chunk from senderId %u",
                        senderId);
                return;
            }

            // Lock protecting MillisortService::{valueStartIdx, numSortedItems}
            static SpinLock lock("shufflePush::ALLSHUFFLE_KEY");
            uint32_t numKeys = reqHdr->totalLength / PivotKey::SIZE;

            // If this is the first chunk of a shuffle message, reserve space
            // for the entire message.
            if (reqHdr->offset == 0) {
                SpinLock::Guard _(lock);
                if (valueStartIdx[senderId] == 0) {
                    valueStartIdx[senderId] =
                            downCast<uint32_t>(numSortedItems);
                    numSortedItems += numKeys;
                } else {
                    // Duplicate RPC; we've seen this message before.
                    return;
                }
            }

            // Copy the message chunk to its destination.
            const uint32_t startIdx = valueStartIdx[senderId];
            Buffer::Iterator payload(rpc->requestPayload, sizeof(*reqHdr), ~0u);
            char* dst = reinterpret_cast<char*>(&incomingKeys[startIdx]);
            dst += reqHdr->offset;
            while (!payload.isDone()) {
                SCOPED_TIMER(shuffleKeysCopyResponseCycles);
                memcpy(dst, payload.getData(), payload.getLength());
                dst += payload.getLength();
                payload.next();
            }

            // This is the last chunk of the message, which implies that we
            // have received the entire message because our push-based shuffle
            // implementation doesn't send out the next data chunk before the
            // preceding one is finished.
            if (reqHdr->offset + reqHdr->len == reqHdr->totalLength) {
                // Make sure the message is only added to the merge sorter once.
                if (!shuffleKeysRxFrom->at(senderId).exchange(true)) {
                    timeTrace("received %u keys from server %u", numKeys,
                            senderId);

                    // Fix the value indices of incoming keys.
                    for (uint32_t i = startIdx; i < startIdx + numKeys; i++) {
                        incomingKeys[i].index = i;
                    }

                    // Notify the merge sorter about the incoming keys.
                    mergeSorterLock.lock();
                    if (mergeSorterIsReady) {
                        mergeSorter->poll(&incomingKeys[startIdx], numKeys);
                    } else {
                        pendingMergeReqs.emplace_back(&incomingKeys[startIdx],
                                numKeys);
                    }
                    mergeSorterLock.unlock();

                    shuffleKeysRxCount.fetch_add(1);
                    ADD_COUNTER(shuffleKeysInputBytes, reqHdr->totalLength);
                }
            }
            break;
        }
        case ALLSHUFFLE_VALUE: {
            INC_COUNTER(shuffleValuesReceivedRpcs);

            // Prevent duplicate RPCs from messing up our memory.
            if (shuffleValsRxCount == world->size()) {
                LOG(WARNING, "Ignore duplicate message chunk from senderId %u",
                        senderId);
                return;
            }

            // Copy the message chunk to its destination.
            uint32_t startIdx = valueStartIdx[senderId];
            Buffer::Iterator payload(rpc->requestPayload, sizeof(*reqHdr), ~0u);
            char* dst = reinterpret_cast<char*>(&sortedValues0[startIdx]);
            dst += reqHdr->offset;
            while (!payload.isDone()) {
                SCOPED_TIMER(shuffleValuesCopyResponseCycles);
                memcpy(dst, payload.getData(), payload.getLength());
                dst += payload.getLength();
                payload.next();
            }

            // This is the last chunk of the message, which implies that we
            // have received the entire message because our push-based shuffle
            // implementation doesn't send out the next data chunk before the
            // preceding one is finished.
            if (reqHdr->offset + reqHdr->len == reqHdr->totalLength) {
                if (!shuffleValsRxFrom->at(senderId).exchange(true)) {
                    uint32_t numValues = reqHdr->totalLength / Value::SIZE;
                    timeTrace("received %u values from server %u", numValues,
                            senderId);
                    numValuesReceived.fetch_add(numValues);
                    shuffleValsRxCount++;
                    ADD_COUNTER(shuffleValuesInputBytes, reqHdr->totalLength);
                }
            }
            break;
        }
        case ALLSHUFFLE_BENCHMARK: {
            // Do nothing; we are benchmarking the transport layer.
            timeTraceSp("shuffle-push: received message chunk, senderId %d, "
                    "offset %u, len %u", senderId, reqHdr->offset, reqHdr->len);
            if (reqHdr->offset + reqHdr->len == reqHdr->totalLength) {
                timeTraceSp("shuffle-push: received entire message, senderId %d",
                        senderId);
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
    static std::atomic<bool> ongoingBenchmark;
    if (reqHdr->masterId == 0) {
        if (ongoingBenchmark) {
            LOG(ERROR, "Already servicing another BenchmarkCollectiveOp RPC!");
            respHdr->common.status = STATUS_SORTING_IN_PROGRESS;
            return;
        }
        ongoingBenchmark = true;
    }

#define WARMUP_COUNT 20
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

                void getResult(Buffer* out) { out->appendExternal(&localBuf); }

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
            // For the micro-benchmark, we are going to "cheat" by using the
            // internal merge complete time, as opposed to the time when all
            // outgoing RPCs are done.
//            uint64_t endTime = Cycles::rdtsc();
            uint64_t endTime = allGatherOp->wait();
            uint64_t allGatherNs = Cycles::toNanoseconds(endTime - startTime);
            timeTrace("AllGather operation completed in %lu ns", allGatherNs);
            // TODO: the cleanup is not very nice; how to do RAII?
            removeCollectiveOp(DUMMY_OP_ID);
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
            std::vector<Buffer> messages(world->size());
            std::unique_ptr<char[]> space(new char[8000000]);
            Buffer outMessage;
            for (int i = 0; i < world->size(); i++) {
                messages[i].appendExternal(space.get(), reqHdr->dataSize);
            }

            uint64_t startTime = context->clockSynchronizer->getConverter(
                    ServerId(reqHdr->masterId)).toLocalTsc(reqHdr->startTime);
            if (Cycles::rdtsc() > startTime) {
                LOG(ERROR, "AllShuffle operation %d started %.2f us late!",
                        reqHdr->count,
                        Cycles::toSeconds(Cycles::rdtsc() - startTime)*1e6);
            }
            while (Cycles::rdtsc() < startTime) {}
            timeTrace("ALL_SHUFFLE benchmark started, run %d", reqHdr->count);
            invokeShufflePush(world.get(), ALLSHUFFLE_BENCHMARK, &messages);
            uint32_t shuffleNs = downCast<uint32_t>(
                    Cycles::toNanoseconds(Cycles::rdtsc() - startTime));
            timeTrace("ALL_SHUFFLE benchmark run %d completed in %u us, "
                    "sent %u bytes", reqHdr->count, shuffleNs / 1000,
                    reqHdr->dataSize * uint32_t(world->size()));

            rpc->replyPayload->emplaceAppend<uint32_t>(world->rank);
            rpc->replyPayload->emplaceAppend<uint32_t>(shuffleNs);
        }
    } else {
        respHdr->common.status = STATUS_INVALID_PARAMETER;
    }

    ongoingBenchmark = false;
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
                timeTrace("received %lu pivot bucket boundaries",
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
                timeTrace("received %lu data bucket boundaries",
                        dataBucketBoundaries.size());
                debugLogKeys("data bucket boundaries: ", &dataBucketBoundaries);

                rpc->sendReply();
                shuffleKeys();
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
