/******************************************************************************
 * ips4o/thread_pool.hpp
 *
 * In-place Parallel Super Scalar Samplesort (IPS⁴o)
 *
 ******************************************************************************
 * BSD 2-Clause License
 *
 * Copyright © 2017, Michael Axtmann <michael.axtmann@kit.edu>
 * Copyright © 2017, Daniel Ferizovic <daniel.ferizovic@student.kit.edu>
 * Copyright © 2017, Sascha Witt <sascha.witt@kit.edu>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * * Redistributions of source code must retain the above copyright notice, this
 *   list of conditions and the following disclaimer.
 *
 * * Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *****************************************************************************/

#pragma once


#include <algorithm>
#include <functional>
#include <memory>
#include <vector>

#include "Arachne/Arachne.h"
#include "synchronization.hpp"

#include "TimeTrace.h"

//#define timeTrace_ips4o(fmt, ...) RAMCloud::TimeTrace::record(fmt,##__VA_ARGS__)
#define timeTrace_ips4o(fmt, ...) ;

namespace ips4o {

/**
 * A thread pool using std::thread.
 */
    class ArachneThreadPool {
    public:
        using Sync = detail::Sync;

        explicit ArachneThreadPool(int num_threads = ArachneThreadPool::maxNumThreads())
                : impl_(new Impl(num_threads)) {}

        template <class F>
        void operator()(F&& func, int num_threads = std::numeric_limits<int>::max()) {
            num_threads = std::min(num_threads, numThreads());
            if (num_threads > 1)
                impl_.get()->run(std::forward<F>(func), num_threads);
            else
                func(0, 1);
        }

        Sync& sync() { return impl_.get()->sync_; }

        int numThreads() const { return impl_.get()->threads_.size() + 1; }

        static int maxNumThreads() { return Arachne::getCorePolicy()->getCores(0).size(); }

    private:
        struct Impl {
            Sync sync_;
            detail::Barrier pool_barrier_;
            std::vector<Arachne::ThreadId> threads_;
            std::function<void(int, int)> func_;
            int num_threads_;
            bool done_ = false;

            /**
            * Constructor for the std::thread pool.
            */
            Impl(int num_threads)
                    : sync_(std::max(1, num_threads))
                    , pool_barrier_(std::max(1, num_threads))
                    , num_threads_(std::max(1, num_threads))
            {
                threads_.reserve(num_threads_ - 1);
                auto coreList = Arachne::getCorePolicy()->getCores(0);
                if (num_threads_ > coreList.size()) {
                    fprintf(stderr, "Not enough cores! num_threads_ %d, "
                            "cores avail. %u", num_threads_, coreList.size());
                }
                int coreIdx = 0;
                for (int i = 1; i < num_threads_; ++i) {
                    int coreId = coreList.get(coreIdx++);
                    if (coreId == Arachne::core.id) {
                        coreId = coreList.get(coreIdx++);
                    }
                    threads_.push_back(Arachne::createThreadOnCore(
                            coreId, &Impl::main, this, i));
                    timeTrace_ips4o("ips4o: created thread %d on core %d", i,
                            coreId);
                }
                timeTrace_ips4o("ips4o: num_threads_ %u, created %u threads",
                        num_threads_, threads_.size());
            }

            /**
             * Destructor for the std::thread pool.
             */
            ~Impl() {
                done_ = true;
                pool_barrier_.barrier();
                for (auto& t : threads_)
                    Arachne::join(t);
                timeTrace_ips4o("ips4o: all threads joined");
            }

            /**
            * Entry point for parallel execution for the std::thread pool.
            */
            template <class F>
            void run(F&& func, const int num_threads) {
                func_ = func;
                num_threads_ = num_threads;
                sync_.setNumThreads(num_threads);
                timeTrace_ips4o("ips4o: run with %u threads", num_threads);

                pool_barrier_.barrier();
                func_(0, num_threads);
                pool_barrier_.barrier();
            }

            /**
            * Main loop for threads created by the std::thread pool.
            */
            void main(const int my_id) {
                for (;;) {
                    pool_barrier_.barrier();
                    if (done_) break;
                    if (my_id < num_threads_)
                        func_(my_id, num_threads_);
                    pool_barrier_.barrier();
                }
                timeTrace_ips4o("ips4o: thread %d done", my_id);
            }
        };

        std::unique_ptr<Impl> impl_;
    };

    using DefaultThreadPool = ArachneThreadPool;

}  // namespace ips4o