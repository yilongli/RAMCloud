/* Copyright (c) 2011-2018 Stanford University
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

// #include <sched.h>

#if (__GNUC__ == 4 && __GNUC_MINOR__ >= 5) || (__GNUC__ > 4)
#include <atomic>
#else
#include <cstdatomic>
#endif
#include <sys/resource.h>
#include <sys/time.h>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <vector>
#include <queue>
#include <boost/program_options.hpp>
#include <byteswap.h>

#include "Common.h"
#include "Cycles.h"
#include "simulator/Merge.h"
#include "MilliSortService.h"

using namespace RAMCloud;

static int numMergeWorkers;
static int numArrays;
static int numArrayItems;

//----------------------------------------------------------------------
// Test functions start here
//----------------------------------------------------------------------


/**
 * Fill the given address with random data.
 *
 * \param dest
 *      Memory block in which to write in random data.
 * \param length
 *      Total number of bytes to write.
 */
void fillRandom(char* dest, uint32_t length)
{
    memset(dest, 'x', length);
    for (uint offset = 0; offset + 8 <= length; offset += 8) {
        *(reinterpret_cast<uint64_t*>(dest + offset)) = generateRandom();
    }
    int bytesLeftToFill = length & 7;
    for (uint offset = length - bytesLeftToFill; offset < length; offset++) {
        *(dest + offset) = downCast<char>(generateRandom() & 127);
    }
}

double memcmp10b(){
    int count = 100000;
    char key1[1024][10], key2[1024][10];
    for (int i = 0; i < 1000; i++) {
        fillRandom(key1[i], 10);
        fillRandom(key2[i], 10);
    }
    
    int countSmallerThan = 0;
    uint64_t start = Cycles::rdtscp();
    for (int i = 0; i < count; i++) {
        if (memcmp(key1[i & 1023], key2[i & 1023], 10) > 0)
            countSmallerThan++;
    }
    uint64_t stop = Cycles::rdtscp();
    
    printf("countSmallerThan: %d\n", countSmallerThan);

    return Cycles::toSeconds(stop - start)/(count);
}

double memcpy16b(){
    int count = 1000000;
    const int size = 16;
    char key1[1024][size], key2[1024][size];
    for (int i = 0; i < 1000; i++) {
        fillRandom(key1[i], size);
        fillRandom(key2[i], size);
    }

    uint64_t start = Cycles::rdtscp();
    for (int i = 0; i < count; i++) {
        memcpy(key1[i & 1023], key2[i & 1023], size);
    }
    uint64_t stop = Cycles::rdtscp();
    
    return Cycles::toSeconds(stop - start)/(count);
}

struct MillisortArrayPtr {
    MillisortItem* data;
    size_t size;
};

struct MergeWorkerContext {
    std::atomic<int> state; // 0: idle, 1: done, 2: working. 3: die.
    
    // Description of Job.
    std::vector<MillisortArrayPtr> arrays;
    MillisortArrayPtr dest;
    
    // Performance statistics
    uint64_t workCycles = 0;
    uint64_t idleTotalCycles = 0;
    uint64_t noWorkCycles = 0;
};

void printData(const MillisortItem* ptr, size_t length, const char* header = "", bool warnIfUnsorted = true) {
    printf("%s ", header);
    uint64_t prev = 0;
    for (size_t i = 0; i < length; i++) {
        uint64_t raw = *((const uint64_t*)&ptr[i]);
        uint64_t swapped = bswap_64(raw);
        if (warnIfUnsorted && i > 0 && prev > swapped) {
            printf(" *");
        }
        prev = swapped;
//        printf(" %22" PRIu64 "", swapped);
        printf(" %3" PRIu64 "", swapped);
    }
    printf(" |");
//    fflush(stdout);
}


void printData(MillisortArrayPtr ptr, const char* header = "", bool warnIfUnsorted = true) {
    printf("%s ", header);
    uint64_t prev = 0;
    for (size_t i = 0; i < ptr.size; i++) {
        uint64_t raw = *((uint64_t*)&ptr.data[i]);
        uint64_t swapped = bswap_64(raw);
        if (warnIfUnsorted && i > 0 && prev > swapped) {
            printf(" *");
        }
        prev = swapped;
//        printf(" %22" PRIu64 "", swapped);
        printf(" %3" PRIu64 "", swapped);
    }
    printf(" |");
//    fflush(stdout);
}

#define VERBOSE 1

// Listing 1
// _end pointer point not to the last element, but one past and never access it.
template< class _Type >
inline void merge_ptr(const _Type* a_start, const _Type* a_end, const _Type* b_start, const _Type* b_end, _Type* dst)
{
//    _Type* initial_dst = dst;
//    const _Type* a_start_init = a_start;
//    const _Type* b_start_init = b_start;
//    size_t a_size = a_end - a_start;
//    size_t b_size = b_end - b_start;
    while (a_start < a_end && b_start < b_end) {
//        bool from_a;
        if (*a_start > *b_start) {
            *dst++ = *b_start++;
//            from_a = true;
        } else {
            *dst++ = *a_start++;	// if elements are equal, then a[] element is output
//            from_a = false;
        }
//        if (initial_dst <= (dst-2) && *(dst-2) > *(dst-1)) {
//            printf("reversed order while merging. merged from a? %s | ", from_a ? "yes" : "no");
//            printData(dst-2, 2); printData(a_start_init, a_size, "A: "); printData(b_start_init, b_size, "B: ");
//            printf("\n");
//        }
//        assert(initial_dst > (dst-2) || !(*(dst-2) > *(dst-1)));
    }
    while (a_start < a_end)	*dst++ = *a_start++;
    while (b_start < b_end)	*dst++ = *b_start++;
}

double mergeModule()
{
    int count = 100;
    uint64_t totalTime = 0;
    int numMasters = numArrays;
    int numItemsPerNode = numArrayItems;

#if USE_ARACHNE
// TODO: shall I make sure # cores available >= numWorkers + 1 (dispatch)?
    int numAttempts = 0;
    Arachne::CorePolicy::CoreList list = Arachne::getCorePolicy()->getCores(0);
    while (list.size() < numMergeWorkers) {
        Arachne::sleep(100 * 1000000);
        numAttempts++;
        if (numAttempts > 10) {
            printf("Failed to get enough cores available after %d attemtps, "
                    "cores avail. %u, numWorkers %d", numAttempts, list.size(),
                    numMergeWorkers);
            exit(1);
        }
        list = Arachne::getCorePolicy()->getCores(0);
    }
    printf("Merge dispatcher running on core %u\n",
            Arachne::getThreadId().context->coreId);
#endif

    for (int expId = 0; expId < count; expId++) {
//        printf("starting experiment %d\n", expId);
        Merge<MilliSortService::PivotKey> merger(numMasters, numItemsPerNode * numMasters * 2, numMergeWorkers);
        MergeTestTools<MilliSortService::PivotKey> testSetup;
        testSetup.initializeInput(numItemsPerNode, numMasters);
        merger.prepareThreads();
        
        // Run merge. First add all input arrays. Then keep polling to finish.
        for (auto array : testSetup.initialArrays) {
            merger.poll(array.data, array.size);
            // We may add intentional delays here to simulate online merge.
        }
        while (merger.poll()) {
#if USE_ARACHNE
            Arachne::yield();
#endif
        }
        uint64_t ticks = merger.stopTick - merger.startTick;
        totalTime += ticks;
        printf("finished experiment %d in %.2f us\n", expId,
                Cycles::toSeconds(ticks)*1e6);

        // verify output
        auto mergedArray = merger.getSortedArray();
        testSetup.verifyOutput(mergedArray.data, mergedArray.size);
        if (expId == count - 1) {
            merger.getPerfStats()->printStats();
        }
    }

    return Cycles::toSeconds(totalTime) / count;
}

// The following struct and table define each performance test in terms of
// a string name and a function that implements the test.
struct TestInfo {
    const char* name;             // Name of the performance test; this is
                                  // what gets typed on the command line to
                                  // run the test.
    double (*func)();             // Function that implements the test;
                                  // returns the time (in seconds) for each
                                  // iteration of that test.
    const char *description;      // Short description of this test (not more
                                  // than about 40 characters, so the entire
                                  // test output fits on a single line).
};
TestInfo tests[] = {
    {"memcmp10b", memcmp10b,
     "memcmp two randomly filled 10b memory buffers."},
    {"mergeModule", mergeModule,
     "merge 50k items using multiple cores using Merge class"},
//    {"sortSka", sortSka,
//     "sort 50k items with ska_sort, which is a single core Radix sort."}, 
};

/**
 * Runs a particular test and prints a one-line result message.
 *
 * \param info
 *      Describes the test to run.
 */
void runTest(TestInfo& info)
{
    double secs = info.func();
    int width = printf("%-23s ", info.name);
    if (secs < 1.0e-06) {
        width += printf("%8.2fns", 1e09*secs);
    } else if (secs < 1.0e-03) {
        width += printf("%8.2fus", 1e06*secs);
    } else if (secs < 1.0) {
        width += printf("%8.2fms", 1e03*secs);
    } else {
        width += printf("%8.2fs", secs);
    }
    printf("%*s %s\n", 26-width, "", info.description);
}

#if USE_ARACHNE
// This is where user code should start running.
void
AppMain(int argc, const char** argv) {
    Cycles::init();
    // Parse command-line options.
    namespace po = boost::program_options;
    vector<string> testNames;
    po::options_description optionsDesc(
            "Usage: Perf [options] testName testName ...\n\n"
            "Runs one or more micro-benchmarks and outputs performance "
            "information.\n\n"
            "Allowed options");
    optionsDesc.add_options()
        ("help", "Print this help message and exit")
        ("mergeWorkers", po::value<int>(&numMergeWorkers)->default_value(6),
                 "Number of merge workers to create")
        ("numArrays", po::value<int>(&numArrays)->default_value(1000),
                 "Number of sorted arrays to merge")
        ("arrayItems", po::value<int>(&numArrayItems)->default_value(50),
                 "Number of items in the sorted array")
        ("testName", po::value<vector<string>>(&testNames),
                "A test is run if its name contains any of the testName"
                "arguments as a substring");

    po::positional_options_description posDesc;
    posDesc.add("testName", -1);
    po::variables_map vm;
    po::store(po::command_line_parser(argc, argv).
          options(optionsDesc).positional(posDesc).run(), vm);
    po::notify(vm);
    if (vm.count("help")) {
        std::cout << optionsDesc << "\n";
        exit(0);
    }

    // CPU_ZERO(&savedAffinities);
    // sched_getaffinity(0, sizeof(savedAffinities), &savedAffinities);
    if (argc == 1) {
        // No test names specified; run all tests.
        foreach (TestInfo& info, tests) {
            runTest(info);
        }
    } else {
        // Run only the tests whose names contain at least one of the
        // command-line arguments as a substring.
        bool foundTest = false;
        foreach (TestInfo& info, tests) {
            for (size_t i = 0; i < testNames.size(); i++) {
                if (strstr(info.name, testNames[i].c_str()) !=  NULL) {
                    foundTest = true;
                    runTest(info);
                    break;
                }
            }
        }
        if (!foundTest) {
            printf("No tests matched given arguments\n");
        }
    }
}

// The following bootstrapping code should be copied verbatim into most Arachne
// applications.
void AppMainWrapper(int argc, const char** argv) {
    AppMain(argc, argv);
    Arachne::shutDown();
}
int
main(int argc, const char** argv) {
    Arachne::Logger::setLogLevel(Arachne::SILENT);
    Arachne::init(&argc, argv);
    Arachne::createThread(&AppMainWrapper, argc, argv);
    Arachne::waitForTermination();
}
#else
int
main(int argc, const char** argv) {
    Cycles::init();
    // Parse command-line options.
    namespace po = boost::program_options;
    vector<string> testNames;
    po::options_description optionsDesc(
            "Usage: Perf [options] testName testName ...\n\n"
            "Runs one or more micro-benchmarks and outputs performance "
            "information.\n\n"
            "Allowed options");
    optionsDesc.add_options()
        ("help", "Print this help message and exit")
        ("mergeWorkers", po::value<int>(&numMergeWorkers)->default_value(6),
                 "Number of merge workers to create")
        ("testName", po::value<vector<string>>(&testNames),
                "A test is run if its name contains any of the testName"
                "arguments as a substring");

    po::positional_options_description posDesc;
    posDesc.add("testName", -1);
    po::variables_map vm;
    po::store(po::command_line_parser(argc, argv).
          options(optionsDesc).positional(posDesc).run(), vm);
    po::notify(vm);
    if (vm.count("help")) {
        std::cout << optionsDesc << "\n";
        exit(0);
    }

    // CPU_ZERO(&savedAffinities);
    // sched_getaffinity(0, sizeof(savedAffinities), &savedAffinities);
    if (argc == 1) {
        // No test names specified; run all tests.
        foreach (TestInfo& info, tests) {
            runTest(info);
        }
    } else {
        // Run only the tests whose names contain at least one of the
        // command-line arguments as a substring.
        bool foundTest = false;
        foreach (TestInfo& info, tests) {
            for (size_t i = 0; i < testNames.size(); i++) {
                if (strstr(info.name, testNames[i].c_str()) !=  NULL) {
                    foundTest = true;
                    runTest(info);
                    break;
                }
            }
        }
        if (!foundTest) {
            printf("No tests matched given arguments\n");
        }
    }
}
#endif
