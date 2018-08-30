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

#ifndef RAMCLOUD_MILLISORTSERVICE_H
#define RAMCLOUD_MILLISORTSERVICE_H

#include "CommunicationGroup.h"
#include "Context.h"
#include "Dispatch.h"
#include "Service.h"
#include "ServerConfig.h"
#include "WorkerManager.h"
#include "simulator/Merge.h"

namespace RAMCloud {

/**
 * This Service supports a variety of requests used for cluster management,
 * such as pings, server controls, and server list management.
 */
class MilliSortService : public Service {
  public:
    explicit MilliSortService(Context* context,
            const ServerConfig* serverConfig);
    ~MilliSortService();
    void dispatch(WireFormat::Opcode opcode, Rpc* rpc);

    CommunicationGroup*
    getCommunicationGroup(int groupId)
    {
        CommGroupTable::iterator it = communicationGroupTable.find(groupId);
        return (it != communicationGroupTable.end()) ? it->second : NULL;
    }

    bool
    registerCommunicationGroup(CommunicationGroup* group)
    {
        if (communicationGroupTable.find(group->id) ==
                communicationGroupTable.end()) {
            communicationGroupTable[group->id] = group;
            return true;
        } else {
            return false;
        }
    }

    void
    initWorld()
    {
        if (!world) {
            // FIXME: Geee, I can't believe this is actually what you need to do
            // to iterate over the server list.
            std::vector<ServerId> allNodes;
            // FIXME: not sure why serverList can have duplicate records.
            std::array<bool, 1000> set = {0};
            bool end = false;
            for (ServerId id = ServerId(0); !end;) {
                id = context->serverList->nextServer(id,
                        ServiceMask({WireFormat::MILLISORT_SERVICE}), &end);
                if (!set[id.indexNumber()]) {
                    allNodes.push_back(id);
//                    LOG(WARNING, "server id %u, gen. %u", id.indexNumber(),
//                            id.generationNumber());
                    set[id.indexNumber()] = true;
                }
            }
            std::sort(allNodes.begin(), allNodes.end(), std::less<ServerId>());
            world.construct(WORLD, serverId.indexNumber() - 1, allNodes);
        }
        registerCommunicationGroup(world.get());
    }

    /**
     * Helper function to initiate a collective operation. It creates and
     * schedules a collective operation task on the rule engine, sets up the
     * continuation code to run upon the completion of the operation, and
     * handles RPCs for this collective operation that arrive early.
     *
     * \tparam T
     *      Concrete type of the collective operation to create.
     * \tparam Callback
     *      Concrete type of the callback.
     * \tparam Args
     *      Types of the arguments passed to the constructor of the collective
     *      operation.
     * \param opId
     *      Unique identifier of the collective operation.
     * \param callback
     *      Continuation code to run upon the completion of the collective
     *      operation.
     * \param args
     *      Arguments to be passed to the constructor of the collective
     *      operation.
     */
    template <typename Op, typename... Args>
    void
    invokeCollectiveOp(Tub<Op>& op, int opId, Args&&... args)
    {
        op.construct(opId, args...);

        // Sync with the dispatch thread before accessing collectiveOpTable.
        // This must be done after constructing the collective op object since
        // the constructor might be expensive.
        Dispatch::Lock _(context->dispatch);

        CollectiveOpRecord* record = getCollectiveOpRecord(opId);
        record->op = op.get();
//        LOG(WARNING, "about to handle early RPCs from op %d, record %p, op %p",
//                opId, record, op.get());
        for (Transport::ServerRpc* serverRpc : record->serverRpcList) {
            context->workerManager->collectiveOpRpcs.push_back(serverRpc);
//            LOG(WARNING, "RPC from collective op %d arrives early!", opId);
        }
        record->serverRpcList.clear();
        // FIXME: can't do it here because pivotsShuffle needs a ptr to op
//        op.wait();
//        collectiveOpTable.erase(opId);
    }

    void
    removeCollectiveOp(int opId)
    {
        Dispatch::Lock _(context->dispatch);
        collectiveOpTable.erase(opId);
    }

    /**
     * Helper function for use in handling RPCs used by collective operations.
     * Record the RPC for future processing if the corresponding collective
     * operation has not been created.
     *
     * \tparam Op
     *      Type of the collective operation (e.g., AllGather, AllShuffle, etc.)
     * \param reqHdr
     *      Header from the incoming RPC request; contains parameters
     *      for this operation.
     * \param[out] respHdr
     *      Header for the response that will be returned to the client.
     *      The caller has pre-allocated the right amount of space in the
     *      response buffer for this type of request, and has zeroed out
     *      its contents (so, for example, status is already zero).
     * \param[out] rpc
     *      Complete information about the remote procedure call; can be
     *      used to read additional information beyond the request header
     *      and/or append additional information to the response buffer.
     * \return
     *      True if the corresponding collective operation has been created
     *      and it has finished processing the RPC.
     */
    template <typename Op>
    void
    handleCollectiveOpRpc(const typename Op::RpcType::Request* reqHdr,
            typename Op::RpcType::Response* respHdr, Rpc* rpc)
    {
        Op* collectiveOp = NULL;
        {
            Dispatch::Lock _(context->dispatch);
            CollectiveOpRecord* record =
                    getCollectiveOpRecord(reqHdr->common.opId);
            collectiveOp = record->getOp<Op>();
        }
        if (collectiveOp == NULL) {
            LOG(ERROR, "unable to find the collective op object, opId %u",
                    reqHdr->common.opId);
        } else {
            collectiveOp->handleRpc(reqHdr, respHdr, rpc);
        }
    }

    /**
     * A pivot consists of the original key of the data tuple plus some
     * metadata.
     *
     * TODO: doc. why metadata; 1. handle duplicate keys gracefully; 2. separate
     * key and value.
     */
    struct PivotKey {
        // TODO: explain the decl. order of the fields (least significant one
        // comes first)

        /// Size of the raw key, in bytes.
        static const uint32_t KEY_SIZE = 10;

        /// Size of the raw key plus metadata.
        static const uint32_t SIZE = KEY_SIZE + 6;

        /// 32-bit index that supports more than 4 billion data tuples on each
        /// server.
        uint32_t index;

        /// 16-bit serverId that supports up to 65536 nodes.
        uint16_t serverId;

        /// Byte representation of the original key that is specialized for
        /// little-endian machines. It starts with the least significant byte
        /// such that
        // TODO:
        char bytes[KEY_SIZE];

        /**
         * Default constructor that does absolutely nothing.
         */
        PivotKey() = default;

        /**
         * Convenient method to build a pivot from an 64-bit signed integer.
         *
         * \param key
         * \param serverId
         * \param index
         */
        PivotKey(uint64_t key, uint16_t serverId, uint32_t index)
            : index(index)
            , serverId(serverId)
            , bytes()
        {
            static_assert(KEY_SIZE >= 8, "Key must be at least 8-byte");
            for (char& byte : bytes) {
                byte = char(key % 256);
                key >>= 8;
            }
        }

        /**
         * Convenient method to build a pivot from a string.
         *
         * \param key
         * \param serverId
         * \param index
         */
        PivotKey(const string& key, uint16_t serverId, uint32_t index)
            : index(index)
            , serverId(serverId)
            , bytes()
        {
            assert(key.size() <= KEY_SIZE);
            int msb = KEY_SIZE - 1;
            for (char ch : key) {
                bytes[msb--] = ch;
            }
        }

        /**
         * Internal helper method that interprets and returns the 10-byte key
         * and the 6-byte metadata together as an 128-bit unsigned integer.
         */
        unsigned __int128
        asUint128() const
        {
            static_assert(sizeof(PivotKey) == 16, "PivotKey must be 128-bit");
            return *((const unsigned __int128*) this);
        }

        /**
         * If this key is constructed from a uint64_t value, return that value;
         * otherwise, undefined. Helper method for debugging only.
         */
        uint64_t
        keyAsUint64()
        {
            return *((uint64_t*) bytes);
        }

        /**
         * If this key is constructed from a string value, return that value;
         * otherwise, undefined. Helper method for debugging only.
         */
        std::string
        keyAsString()
        {
            string str;
            for (char ch : bytes) {
                if (ch > 0) {
                    str += ch;
                }
            }
            std::reverse(str.begin(), str.end());
            return str;
        }

        /// Required by comparison-based sorting algorithms.
        bool
        operator<(const PivotKey& other) const
        {
            return asUint128() < other.asUint128();
        }

        bool
        operator<=(const PivotKey& otherKey) const
        {
            return asUint128() <= otherKey.asUint128();
        }

        bool
        operator==(const PivotKey& otherKey) const
        {
            return asUint128() == otherKey.asUint128();
        }
    };

    struct Value {

        static const uint32_t SIZE = 90;

        char bytes[SIZE];

        /**
         * Default constructor that does absolutely nothing.
         */
        Value() = default;

        Value(uint64_t value)
            : bytes()
        {
            *((uint64_t*) bytes) = value;
        }

        /**
         * If this value is constructed from a uint64_t, return that unsigned
         * integer; otherwise, undefined. Helper method for debugging only.
         */
        uint64_t asUint64() { return *((uint64_t*) bytes); }
    };

  PRIVATE:
    void initMilliSort(const WireFormat::InitMilliSort::Request* reqHdr,
                WireFormat::InitMilliSort::Response* respHdr,
                Rpc* rpc);
    void startMilliSort(const WireFormat::StartMilliSort::Request* reqHdr,
                WireFormat::StartMilliSort::Response* respHdr,
                Rpc* rpc);
    void treeBcast(const WireFormat::TreeBcast::Request* reqHdr,
                WireFormat::TreeBcast::Response* respHdr,
                Rpc* rpc);
    void flatGather(const WireFormat::FlatGather::Request* reqHdr,
                WireFormat::FlatGather::Response* respHdr,
                Rpc* rpc);
    void allGather(const WireFormat::AllGather::Request* reqHdr,
                WireFormat::AllGather::Response* respHdr,
                Rpc* rpc);
    void allShuffle(const WireFormat::AllShuffle::Request* reqHdr,
                WireFormat::AllShuffle::Response* respHdr,
                Rpc* rpc);
    void sendData(const WireFormat::SendData::Request* reqHdr,
                WireFormat::SendData::Response* respHdr,
                Rpc* rpc);
    void shufflePull(const WireFormat::ShufflePull::Request* reqHdr,
                WireFormat::ShufflePull::Response* respHdr,
                Rpc* rpc);

    struct RandomGenerator {
        explicit RandomGenerator(uint64_t seed = 1)
            : state(seed)
        {}

        uint64_t
        next()
        {
            state = (164603309694725029ull * state) % 14738995463583502973ull;
            return state;
        }

        uint64_t state;
    };

    using PivotKeyArray = std::unique_ptr<PivotKey[]>;

    using ValueArray = std::unique_ptr<Value[]>;

    /// Encapsulates the state of method #rearrangeValues, allowing it to finish
    /// asynchronously.
    struct RearrangeValueTask {
        /// C-style array holding the values after rearrangement.
        Value* dest;

        /// Worker threads that rearrange the values.
        std::list<Arachne::ThreadId> workers;

        void
        wait(ValueArray* oldValues)
        {
            for (auto& tid : workers) {
                Arachne::join(tid);
            }
            oldValues->reset(dest);
            dest = NULL;
        }
    };

    /// Shared RAMCloud information.
    Context* context;

    /// This server's ServerConfig, which we export to curious parties.
    /// NULL means we'll reject curious parties.
    const ServerConfig* serverConfig;

    using CommGroupTable = std::unordered_map<int, CommunicationGroup*>;
    CommGroupTable communicationGroupTable;

    enum CollectiveCommunicationOpId {
        GATHER_PIVOTS,
        GATHER_SUPER_PIVOTS,
        BROADCAST_PIVOT_BUCKET_BOUNDARIES,
        ALLSHUFFLE_PIVOTS,
        BROADCAST_DATA_BUCKET_BOUNDARIES,
        ALLGATHER_DATA_BUCKET_BOUNDARIES,
        ALLSHUFFLE_KEY,
        ALLSHUFFLE_VALUE,
    };

    // ----------------------
    // Computation steps
    // ----------------------

    void inplaceMerge(vector<PivotKey>& keys, size_t sizeOfFirstSortedRange);
    void partition(PivotKey* keys, int numKeys, int numPartitions,
            std::vector<PivotKey>* pivots);
    void localSortAndPickPivots();
    RearrangeValueTask rearrangeValues(PivotKey* keys, Value* values,
            int totalItems, bool initialData);
    void pickSuperPivots();
    void pickPivotBucketBoundaries();
    void pivotBucketSort();
    void pickDataBucketBoundaries();
    void allGatherDataBucketBoundaries();
    void shuffleKeys();
    void shuffleValues();
    void debugLogKeys(const char* prefix, vector<PivotKey>* keys);

    enum CommunicationGroupId {
        WORLD                   = 0,
        MY_PIVOT_SERVER_GROUP   = 1,
        ALL_PIVOT_SERVERS       = 2,
        ALL_GATHER_PEERS_GROUP  = 3
    };

    /// MilliSort request in progress. NULL means the service is idle.e
    std::atomic<Service::Rpc*> ongoingMilliSort;

    Tub<RandomGenerator> rand;

    uint64_t startTime;

    /// Rank of the pivot server this node belongs to. -1 means unknown.
    int pivotServerRank;

    // True if this node is a pivot server. Only valid when #pivotServerRank
    /// is not -1.
    bool isPivotServer;

    // -------- Node state --------
    /// # data tuples on each node initially. -1 means unknown.
    int numDataTuples;

    /// # pivot servers. -1 means unknown.
    int numPivotServers;

    /// Keys of the data tuples to be sorted.
    PivotKeyArray keys;

    /// Values of the data tuples to be sorted.
    ValueArray values;

    /// Sorted keys on this node when the sorting completes.
    PivotKeyArray sortedKeys;

    /// Sorted values on this node when the sorting completes.
    ValueArray sortedValues;

    /// # data tuples end up on this node when the sorting completes.
    int numSortedItems;

    // FIXME: this is a bad name; besides, does it have to be class member?
    std::vector<int> valueStartIdx;

    /// Selected keys that evenly divide local #keys on this node into # nodes
    /// partitions.
    std::vector<PivotKey> localPivots;

    /// Represents the task of rearranging local values.
    RearrangeValueTask rearrangeLocalVals;

    /// Data bucket boundaries that determine the final destination of each data
    /// tuple on this node. Same on all nodes.
    /// For example, dataBucketBoundaries = {1, 5, 9} means all data are divided
    /// into 3 buckets: (-Inf, 1], (1, 5], and (5, 9].
    std::vector<PivotKey> dataBucketBoundaries;

    /// Range of each data bucket in #keys. Each range is represented by a pair
    /// of integers: the starting index and # items in the bucket. For example,
    /// dataBucketRanges[i] = (a, b) means the first key in the (i+1)-th data
    /// bucket is keys[a] and there are b keys in total in the bucket.
    std::vector<std::pair<int,int>> dataBucketRanges;

    /// True means #dataBucketRanges has been filled out and, thus, can be
    /// safely accessed by #shufflePull handler. Used to prevent data race on
    /// #dataBucketRanges.
    std::atomic_bool dataBucketRangesDone;

    // TODO: doesn't have to be a class member?
    /// Used to sort keys as they arrive during the final key shuffle stage.
    Tub<Merge<PivotKey>> mergeSorter;

    /// Contains all nodes in the service.
    Tub<CommunicationGroup> world;

    /// Contains the local node, the pivot server it's assigned to (or the local
    /// node itself is a pivot server), and all other nodes that are assigned to
    /// this pivot server.
    Tub<CommunicationGroup> myPivotServerGroup;


    Tub<CommunicationGroup> allGatherPeersGroup;

    // -------- Pivot server state --------
    /// Pivots gathered from this pivot server's slave nodes. Always empty on
    /// normal nodes.
    std::vector<PivotKey> gatheredPivots;

    /// Selected pivots that evenly divide #gatherPivots into # pivot servers
    /// partitions. Different on each pivot server. Always empty on normal
    /// nodes.
    std::vector<PivotKey> superPivots;

    /// Pivot bucket boundaries that determine the destination of each pivot on
    /// this pivot server. Same on all pivot servers. Always empty on normal
    /// nodes.
    std::vector<PivotKey> pivotBucketBoundaries;

    /// After #gatheredPivots on all nodes are sorted globally and spread across
    /// all pivot servers, this variable holds the local portion of this node.
    /// Always empty on normal nodes.
    std::vector<PivotKey> sortedGatheredPivots;

    // TODO: local view of #dataBucketBoundaries
    std::vector<PivotKey> partialDataBucketBoundaries;

    /// # pivots on all nodes that are smaller than #sortedGatherPivots. Only
    /// computed on pivot servers.
    int numSmallerPivots;

    /// # pivots on all nodes. Only computed on pivot servers.
    int numPivotsInTotal;

    /// Contains all pivot servers (including the root) in #nodes. Always empty
    /// on normal nodes.
    Tub<CommunicationGroup> allPivotServers;

    // -------- Root node state --------
    /// Contains super pivots collected from all pivot servers. Always empty on
    /// non-root nodes.
    std::vector<PivotKey> globalSuperPivots;

    friend class TreeBcast;
    friend class Merge;

    DISALLOW_COPY_AND_ASSIGN(MilliSortService);
};


} // end RAMCloud

#endif  // RAMCLOUD_MILLISORTSERVICE_H
