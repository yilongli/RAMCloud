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

}

#endif