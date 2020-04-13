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

}

#endif