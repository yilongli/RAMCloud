#ifndef GRANULARCOMPUTING_BIGQUERY_H
#define GRANULARCOMPUTING_BIGQUERY_H

#include <vector>
#include "Arachne/Arachne.h"
#include "Buffer.h"

namespace RAMCloud {

struct HashPartitioner {
    /// #records to scan
    size_t numRecords;

    /// #records already claimed by scan workers.
    std::atomic<size_t> numClaimed;

    std::vector<int> coresAvail;

    /// perCoreMsgs[core][target]
    std::vector<Tub<std::vector<Buffer>>> perCoreMsgs;

    const size_t BATCH_SIZE = 1000;

    explicit HashPartitioner(size_t nrecords, size_t nodes,
            std::vector<int>* coresAvail)
            : numRecords(nrecords)
            , numClaimed(0)
            , coresAvail(*coresAvail)
            , perCoreMsgs(coresAvail->size())
    {
        for (auto& tub : perCoreMsgs) {
            tub.construct(nodes);
        }
    }

    ~HashPartitioner() {}

    virtual void doWork(std::vector<Buffer>* outMsgs, size_t recordId) = 0;

    void workerMain(int coreIdx)
    {
        while (true) {
            size_t start = numClaimed.fetch_add(BATCH_SIZE);
            size_t end = std::min(start + BATCH_SIZE, numRecords);
            if (start >= numRecords) {
                break;
            }

            for (size_t i = start; i < end; i++) {
                doWork(perCoreMsgs[coreIdx].get(), i);
            }
        }
    }

    void start(std::vector<Buffer>* finalOutMsgs)
    {
        std::vector<Arachne::ThreadId> workers;
        for (size_t id = 0; id < coresAvail.size(); id++) {
            workers.push_back(Arachne::createThreadOnCore(coresAvail[id],
                    [this] (size_t id) { workerMain(id); }, id));
        }
        for (auto& tid : workers) Arachne::join(tid);

        size_t numNodes = perCoreMsgs[0]->size();
        for (size_t target = 0; target < numNodes; target++) {
            Buffer* outgoing = &finalOutMsgs->at(target);
            for (auto& coreLocalBufs : perCoreMsgs) {
                outgoing->appendExternal(&coreLocalBufs->at(target));
            }
        }
    }
};

template <typename T>
struct Deserializer {

    /// # records deserialized
    size_t numRecords;

    /// # incoming messages already claimed by deserialization workers.
    std::atomic<size_t> numClaimed;

    std::vector<int> coresAvail;

    std::vector<std::vector<char>>* incomingMessages;

    /// perCoreRecords[core][target]
    vector<vector<T>> perCoreRecords;

    explicit Deserializer(std::vector<int>* coresAvail,
            std::vector<std::vector<char>>* incomingMessages)
            : numRecords(0)
            , numClaimed(0)
            , coresAvail(*coresAvail)
            , incomingMessages(incomingMessages)
            , perCoreRecords(coresAvail->size())
    {}

    static inline char*
    nextString(char*& s, uint32_t& len)
    {
        char* old = s;
        len = 0;
        while (s[len]) len++;
        s += len + 1;
        return old;
    }

    static inline uint64_t
    next8BInt(char*& s)
    {
        uint64_t bytes = *reinterpret_cast<uint64_t*>(s);
        s += 8;
        return bytes;
    }

    virtual void process(size_t msgId, std::vector<T>* outRecords) = 0;

    void workerMain(int coreIdx)
    {
        while (true) {
            const static size_t B = 4;
            size_t start = numClaimed.fetch_add(B);
            size_t end = std::min(start + B, incomingMessages->size());
            if (start >= incomingMessages->size()) {
                break;
            }

            for (size_t i = start; i < end; i++) {
                process(i, &perCoreRecords[coreIdx]);
            }
        }
    }

    void start()
    {
        std::vector<Arachne::ThreadId> workers;
        for (size_t id = 0; id < coresAvail.size(); id++) {
            workers.push_back(Arachne::createThreadOnCore(coresAvail[id],
                    [this] (size_t id) { workerMain(id); }, id));
        }
        for (auto& tid : workers) Arachne::join(tid);

        for (auto& vec : perCoreRecords) {
            numRecords += vec.size();
        }
    }
};

/**
 * Given a list of records sorted by their keys, produce a new sorted list of
 * records with only distinct keys. The original list of records are scanned
 * in parallel and the semantics to merge records with the same key is defined
 * in the subclass.
 */
struct Combiner {
    /// # records to scan
    size_t numRecords;

    /// # final records produced by combining records with the same key.
    std::atomic<size_t> numFinalRecords;

    std::vector<int> coresAvail;

    std::vector<bool> scanDone;

    Arachne::SpinLock mutex;

    explicit Combiner(size_t nrecords, std::vector<int>* coresAvail)
        : numRecords(nrecords)
        , numFinalRecords(0)
        , coresAvail(*coresAvail)
        , scanDone(coresAvail->size())
        , mutex()
    {}

    virtual bool equal(size_t idx0, size_t idx1) = 0;
    virtual void doWork(size_t recordId, size_t& lastUniq) = 0;
    virtual void copyRecord(size_t fromIdx, size_t toIdx) = 0;

    void workerMain(int coreIdx)
    {
        size_t avgRecordsPerCore = (numRecords + coresAvail.size() - 1) /
                coresAvail.size();
        size_t start = avgRecordsPerCore * coreIdx;
        size_t end = std::min(numRecords, start + avgRecordsPerCore);

        // Advance `start` if the key of our first record is equal to that
        // of the previous record.
        if (start && equal(start - 1, start)) {
            if (equal(start, end - 1)) {
                // All records in range [start, end) are equal;
                // there is nothing for us to do.
                std::lock_guard<Arachne::SpinLock> _(mutex);
                scanDone[coreIdx] = true;
                return;
            }

            start++;
            while (equal(start - 1, start)) {
                start++;
            }
        }

        // Advance `end` if the key of our last record is equal to that
        // of the next record.
        while ((end < numRecords) && (equal(end - 1, end))) {
            end++;
        }

        // Scan all records in the range of [start, end).
        size_t lastUniq = start;
        for (size_t i = start; i < end; i++) {
            doWork(i, lastUniq);
        }

        // Wait for the worker before us to finish scanning and claim the slots
        // for writing final records; this is to make sure final records remain
        // sorted.
        size_t copyDst = 0;
        while (true) {
            std::lock_guard<Arachne::SpinLock> _(mutex);
            if ((coreIdx == 0) || scanDone[coreIdx - 1]) {
                scanDone[coreIdx] = true;
                size_t more = lastUniq - start + 1;
                copyDst = numFinalRecords.fetch_add(more);
                break;
            }
        }

        // Copy reduced records into their final destination.
        for (size_t i = start; i <= lastUniq; i++) {
            copyRecord(i, copyDst++);
        }
    }

    void run()
    {
        std::vector<Arachne::ThreadId> workers;
        for (size_t id = 0; id < coresAvail.size(); id++) {
            workers.push_back(Arachne::createThreadOnCore(coresAvail[id],
                    [this] (size_t id) { workerMain(id); }, id));
        }
        for (auto& tid : workers) Arachne::join(tid);
    }
};

struct HashCombiner {
    /// # records to scan
    size_t numRecords;

    const size_t numMapShards;

    /// # records already claimed by scan workers.
    std::atomic<size_t> numClaimed;

    /// # final records produced by combining records with the same key.
    std::atomic<size_t> numFinalRecords;

    /// # workers that have finished scanning.
    std::atomic<size_t> scannersLeft;

    std::vector<int> coresAvail;

    std::atomic<size_t> nextShard;

    explicit HashCombiner(size_t nrecords, std::vector<int>* coresAvail)
        : numRecords(nrecords)
        , numMapShards(coresAvail->size() * 20)
        , numClaimed(0)
        , numFinalRecords(0)
        , scannersLeft(coresAvail->size())
        , coresAvail(*coresAvail)
        , nextShard(0)
    {}

    virtual bool insert(size_t recordId) = 0;
    virtual void copyRecords(size_t shardId) = 0;

    void workerMain()
    {
        const static size_t BATCH_SIZE = 1024;
        size_t cnt = 0;
        while (true) {
            size_t start = numClaimed.fetch_add(BATCH_SIZE);
            size_t end = std::min(start + BATCH_SIZE, numRecords);
            if (start >= numRecords) {
                break;
            }

            for (size_t i = start; i < end; i++) {
                if (insert(i)) {
                    cnt++;
                }
            }
        }
        numFinalRecords.fetch_add(cnt);

        scannersLeft--;
        while (scannersLeft > 0) {
            Arachne::yield();
        }

        // Copy reduced records into their final destination.
        while (true) {
            size_t shardId = nextShard.fetch_add(1);
            if (shardId >= numMapShards) {
                break;
            }

            copyRecords(shardId);
        }
    }

    void run()
    {
        std::vector<Arachne::ThreadId> workers;
        for (size_t id = 0; id < coresAvail.size(); id++) {
            workers.push_back(Arachne::createThreadOnCore(coresAvail[id],
                    [this] () { workerMain(); }));
        }
        for (auto& tid : workers) Arachne::join(tid);
    }
};

}

#endif