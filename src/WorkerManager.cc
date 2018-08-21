/* Copyright (c) 2011-2017 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER
 * RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF
 * CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include <sched.h>
#include <new>
#include <typeinfo>
#include "BitOps.h"
#include "Cycles.h"
#include "CycleCounter.h"
#include "Initialize.h"
#include "LogProtector.h"
#include "MasterService.h"
#include "MilliSortService.h"
#include "PerfStats.h"
#include "RawMetrics.h"
#include "RpcLevel.h"
#include "ShortMacros.h"
#include "ServerRpcPool.h"
#include "TimeTrace.h"
#include "WireFormat.h"
#include "WorkerManager.h"

// If the following line is uncommented, trace records will be generated that
// allow service times to be computed for all RPCs.
// WARNING: These extra logging calls may (read: will likely) make the system
// unstable. The additional file IO on the dispatch thread will cause service
// gaps that prevent servers from responding to pings quickly enough to prevent
// eviction from the cluster.
// #define LOG_RPCS 1

namespace RAMCloud {
// Uncomment the following line (or specify -D WMTT on the make command line)
// to enable a bunch of time tracing in this module.
// #define WMTT 1

// Provides a shorthand way of invoking TimeTrace::record, compiled in or out
// by the WMTT #ifdef.
void
WorkerManager::timeTrace(const char* format,
        uint32_t arg0, uint32_t arg1, uint32_t arg2, uint32_t arg3)
{
#ifdef WMTT
    TimeTrace::record(format, arg0, arg1, arg2, arg3);
#endif
}

// Change the following from 0 -> 1 to enable log statements for debugging
// distributed deadlocks in worker threads.
#define DEBUG_DEADLOCK 0

/**
 * Default object used to make system calls.
 */
static Syscall defaultSyscall;

/**
 * Used by this class to make all system calls.  In normal production
 * use it points to defaultSyscall; for testing it points to a mock
 * object.
 */
Syscall* WorkerManager::sys = &defaultSyscall;

// Length of time that a worker will actively poll for new work before it puts
// itself to sleep. This period should be much longer than typical RPC
// round-trip times so the worker thread doesn't go to sleep in an ongoing
// conversation with a single client.  It must also be much longer than the
// time it takes to wake up the thread once it has gone to sleep (as of
// September 2011 this time appears to be as much as 50 microseconds).
int WorkerManager::pollMicros = 10000;
// The following constant is used to signal a worker thread that
// it should exit.
#define WORKER_EXIT reinterpret_cast<Transport::ServerRpc*>(1)

/**
 * Construct a WorkerManager.
 *
 * \param context
 *      Overall information about this server.
 * \param maxCores
 *      Number of cores workers can be scheduled on.
 *      TODO:  Ask Henry if this does anything.
 */
WorkerManager::WorkerManager(Context* context, uint32_t maxCores)
    : Dispatch::Poller(context->dispatch, "WorkerManager")
    , context(context)
    , outstandingRpcs()
    , numOutstandingRpcs(0)
    , testingSaveRpcs(0)
    , testRpcs()
    , collectiveOpRpcs()
{ }

/**
 * Transports invoke this method when an incoming RPC is complete and
 * ready for processing.  This method will arrange for the RPC (eventually)
 * to be serviced, and will invoke its #sendReply method once the RPC
 * has been serviced.
 *
 * \param rpc
 *      RPC object containing a fully-formed request that is ready for
 *      service.
 */
void
WorkerManager::handleRpc(Transport::ServerRpc* rpc)
{
    // TODO: also useful for ramcloud?
    rpc->arriveTime = context->dispatch->currentTime;
    // Since this method should only run in the dispatch thread, there is no
    // need to synchronize this state.
    static uint32_t nextRpcId = 1;

    // Find the service for this RPC.
    const WireFormat::RequestCommon* header;
    header = rpc->requestPayload.getStart<WireFormat::RequestCommon>();
    if ((header == NULL) || (header->opcode >= WireFormat::ILLEGAL_RPC_TYPE)) {
#if TESTING
        if (testingSaveRpcs) {
            // Special case for testing.
            testRpcs.push(rpc);
            return;
        }
#endif
        if (header == NULL) {
            LOG(WARNING, "Incoming RPC contains no header (message length %d)",
                    rpc->requestPayload.size());
            Service::prepareErrorResponse(&rpc->replyPayload,
                    STATUS_MESSAGE_TOO_SHORT);
        } else {
            LOG(WARNING, "Incoming RPC contained unknown opcode %d",
                    header->opcode);
            Service::prepareErrorResponse(&rpc->replyPayload,
                    STATUS_UNIMPLEMENTED_REQUEST);
        }
        rpc->sendReply();
        return;
    }

    // Some requests are better handled inside the dispatch thread.
    // For instance, echo requests are so trivial to process that
    // it's not worth passing them to worker threads. Also, handle
    // ping requests inline so that high server load can never cause
    // a server to appear offline.
    if ((header->opcode == WireFormat::ECHO) ||
            (header->opcode == WireFormat::PING)) {
        Service::Rpc serviceRpc(NULL, &rpc->requestPayload,
                &rpc->replyPayload);
#if HOMA_BENCHMARK
        // As of 2017/10, bypassing Service::handleRpc reduces ~400(!) ns
        // for short echo requests.
        if (header->opcode == WireFormat::ECHO) {
            MasterService* master = static_cast<MasterService*>(
                    context->services[WireFormat::MASTER_SERVICE]);
            master->dispatch(WireFormat::ECHO, &serviceRpc);
            rpc->sendReply();
            return;
        }
#endif
        Service::handleRpc(context, &serviceRpc);
        rpc->sendReply();
        return;
    }

    if ((header->opcode >= WireFormat::GATHER_FLAT) &&
            (header->opcode <= WireFormat::ALL_SHUFFLE)) {
        MilliSortService* millisort = context->getMilliSortService();
        uint32_t opId = reinterpret_cast<
                const WireFormat::RequestCommonWithOpId*>(header)->opId;
        Service::CollectiveOpRecord* record =
                millisort->getCollectiveOpRecord(opId);
//        LOG(NOTICE, "%s RPC from op %u, record->op %p",
//                WireFormat::opcodeSymbol(header->opcode), opId, record->op);
        if (record->op == NULL) {
            record->serverRpcList.push_back(rpc);
            return;
        }
    }

//    if (header->opcode == WireFormat::SHUFFLE_PULL) {
//        TimeTrace::record("received shuffle pull request");
//    }

#ifdef LOG_RPCS
    LOG(NOTICE, "Received %s RPC at %u with %u bytes",
            WireFormat::opcodeSymbol(header->opcode),
            reinterpret_cast<uint64_t>(rpc),
            rpc->requestPayload.size());
#endif

    numOutstandingRpcs++;

    // Create a new thread to handle the RPC.
    rpc->id = nextRpcId++;
    rpc->header = header;
    timeTrace("ID %u: Dispatching opcode %d on core %d", rpc->id,
        header->opcode, sched_getcpu());
    Arachne::ThreadId threadId = Arachne::createThread(&WorkerManager::workerMain, this, rpc);
    if (threadId ==
            Arachne::NullThread) {
        // Thread creations can fail randomly due to core deallocation,
        // so first retry a few times.
        for (int i = 0; i < 10; i++) {
            LOG(NOTICE, "Incoming RPC with opcode %d failed to find a core, "
                "reattempt %d", header->opcode, i);
            Arachne::sleep(10000);
            if (Arachne::createThread(&WorkerManager::workerMain,
                this, rpc) != Arachne::NullThread) {
                outstandingRpcs.push_back(rpc);
                return;
            }
        }
        // On failure, send STATUS_RETRY
        LOG(WARNING, "Incoming RPC with opcode %d failed to find a core, "
            "sending RETRY request", header->opcode);
        Service::prepareErrorResponse(&rpc->replyPayload,
                STATUS_RETRY);
        rpc->sendReply();
    } else {
//        RAMCLOUD_LOG(NOTICE, "%s dispatched, init. core %d, core %d, request %p",
//                WireFormat::opcodeSymbol(header->opcode),
//                threadId.context->originalCoreId, threadId.context->coreId,
//                &rpc->requestPayload);
        outstandingRpcs.push_back(rpc);
    }
}

/**
 * Returns true if there are currently no RPCs being serviced, false
 * if at least one RPC is currently being executed by a worker.  If true
 * is returned, it also means that any changes to memory made by any
 * worker threads will be visible to the caller.
 *
 * This method should only be called within the dispatch thread.
 */
bool
WorkerManager::idle()
{
    return (numOutstandingRpcs == 0);
}

/**
 * This method is invoked by Dispatch during its polling loop.  It checks
 * for completion of outstanding RPCs.
 */
int
WorkerManager::poll()
{
    // TODO: kind of a hack for collective op
    for (auto collectiveOpRpc : collectiveOpRpcs) {
        uint32_t delayMicros = static_cast<uint32_t>(
                Cycles::toSeconds(Cycles::rdtsc() -
                collectiveOpRpc->arriveTime) * 1e6);
        auto opcode = collectiveOpRpc->getOpcode();
        if (delayMicros > 2) {
            TimeTrace::record("opcode %u was delayed %u us; collective op not "
                    "ready", opcode, delayMicros);
        }
        handleRpc(collectiveOpRpc);
    }
    collectiveOpRpcs.clear();
    // end of hack

    int foundWork = 0;

    for (int i = downCast<int>(outstandingRpcs.size()) - 1; i >= 0; i--) {
        Transport::ServerRpc* rpc = outstandingRpcs[i];
        if (!rpc->finished.load(std::memory_order_acquire)) {
//            RAMCLOUD_CLOG(NOTICE, "%s not finished, request %p",
//                    WireFormat::opcodeSymbol(&rpc->requestPayload),
//                    &rpc->requestPayload);
            continue;
        }

        foundWork = 1;

        timeTrace("ID %u: dispatch sending response",
                rpc->id, *(rpc->requestPayload.getStart<uint16_t>()));

#ifdef LOG_RPCS
            LOG(NOTICE, "Sending reply for %s at %u with %u bytes",
                    WireFormat::opcodeSymbol(&rpc->requestPayload),
                    reinterpret_cast<uint64_t>(rpc),
                    rpc->replyPayload.size());
#endif
        rpc->sendReply();
        timeTrace("ID %u: reply sent", rpc->id);
        numOutstandingRpcs--;

        // If we are not the last rpc, store the last Rpc here so that pop-back
        // doesn't lose data and we do not iterate here again.
        if (rpc != outstandingRpcs.back())
            outstandingRpcs[i] = outstandingRpcs.back();
        outstandingRpcs.pop_back();
    }
    return foundWork;
}

/**
 * Wait for an RPC request to appear in the testRpcs queue, but give up if
 * it takes too long.  This method is intended only for testing (it only
 * works when there are no registered services).
 *
 * \param timeoutSeconds
 *      If a request doesn't arrive within this many seconds, return NULL.
 *
 * \result
 *      The incoming RPC request, or NULL if nothing arrived within the time
 *      limit.
 */
Transport::ServerRpc*
WorkerManager::waitForRpc(double timeoutSeconds) {
    uint64_t start = Cycles::rdtsc();
    while (true) {
        if (!testRpcs.empty()) {
            Transport::ServerRpc* result = testRpcs.front();
            testRpcs.pop();
            return result;
        }
        if (Cycles::toSeconds(Cycles::rdtsc() - start) > timeoutSeconds) {
            return NULL;
        }
        context->dispatch->poll();
    }
}

/**
 * This is the top-level method for worker threads.  It repeatedly waits for
 * an RPC to be assigned to it, then executes that RPC and communicates its
 * completion back to the dispatch thread.
 *
 * \param serverRpc
 *      The RPC this worker was created to process.
 */
void
WorkerManager::workerMain(Transport::ServerRpc* serverRpc)
{
    // Cycles::rdtsc time that's updated continuously when this thread is idle.
    // Used to keep track of how much time this thread spends doing useful
    // work.
    uint64_t lastIdle = Cycles::rdtsc();

//    RAMCLOUD_LOG(NOTICE, "%s passed to handler, request %p",
//            WireFormat::opcodeSymbol(serverRpc->getOpcode()),
//            &serverRpc->requestPayload);

    try {
       timeTrace("ID %u: Starting processing of opcode %d  on KT %d, "
            "idInCore %d", serverRpc->id, serverRpc->header->opcode,
            Arachne::core.kernelThreadId,
            Arachne::core.loadedContext->idInCore);
        Worker worker(context, serverRpc,
            WireFormat::Opcode(serverRpc->header->opcode));

        serverRpc->epoch = LogProtector::getCurrentEpoch();
        Service::Rpc rpc(&worker, &serverRpc->requestPayload,
                &serverRpc->replyPayload);
        Service::handleRpc(context, &rpc);
//        RAMCLOUD_LOG(NOTICE, "GET_SERVER_ID back from handler, request %p",
//                &serverRpc->requestPayload);

        // Pass the RPC back to the dispatch thread for completion.
        worker.sendReply();

        // Update performance statistics.
        uint64_t current = Cycles::rdtsc();
        PerfStats::threadStats.workerActiveCycles += (current - lastIdle);
        timeTrace("ID %u: took worker time %u", serverRpc->id,
            (uint32_t) Cycles::toNanoseconds(current - lastIdle));
        TEST_LOG("exiting");
    } catch (std::exception& e) {
        LOG(ERROR, "worker: %s", e.what());
        throw; // will likely call std::terminate()
    } catch (...) {
        LOG(ERROR, "worker");
        throw; // will likely call std::terminate()
    }
}

/**
 * Tell the dispatch thread that this worker has finished processing its RPC,
 * so it is safe to start sending the reply.  This method should only be
 * invoked in the worker thread.
 */
void
Worker::sendReply()
{
    if (!replySent) {
        rpc->finished.store(1, std::memory_order_release);
        replySent = true;
    }
}

} // namespace RAMCloud
