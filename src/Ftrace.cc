/* Copyright (c) 2017 Stanford University
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

#include <fstream>

#include "Ftrace.h"
#include "Syscall.h"

namespace RAMCloud {

// TODO: SET IT TO 0 BY DEFAULT
#ifndef FTRACE
#define FTRACE 1
#endif

static Syscall syscall;

// Path to the directory that holds all ftrace control and output files.
// This directory only presents after the `debugfs` file system is mounted.
static string tracingFs = "/sys/kernel/debug/tracing/";
static string traceFile = tracingFs + "trace";

/// Map from ftrace files to file descriptors.
using FileDescriptors = std::map<string, int>;
static FileDescriptors fds = {};

/**
 * Control the behavior of ftrace by writing to the control files.
 *
 * \param option
 *      The control file to write.
 * \param value
 *      Null-terminated string that contains the content to be written
 *      into the control file.
 */
void
Ftrace::controlSet(const char* option, const char* value)
{
#if FTRACE
    int fd;
    FileDescriptors::iterator it = fds.find(option);
    if (it == fds.end()) {
        fd = syscall.open((tracingFs + option).c_str(), O_WRONLY);
        if (fd < 0) {
            RAMCLOUD_LOG(ERROR, "cannot open %s%s, errno %s",
                    tracingFs.c_str(), option, strerror(errno));
            return;
        }
        fds[option] = fd;
    } else {
        fd = it->second;
    }

    ssize_t ret = syscall.write(fd, value, strlen(value));
    if (ret < 0) {
        RAMCLOUD_LOG(ERROR, "write failed: %s", strerror(errno));
    }
#endif
}

/**
 * Print the collected ftrace to the system log.
 */
void
Ftrace::printToLog()
{
#if FTRACE
    std::ifstream ifstream(traceFile);
    RAMCLOUD_LOG(NOTICE, "Logging collected ftrace:\n");
    string line;
    while (getline(ifstream, line)) {
        line += '\n';
        syscall.write(Logger::get().getLogFile(), line.c_str(), line.length());
    }
#endif
}

// TODO:
void
Ftrace::snapshot() {
#if FTRACE
#define MAX_BUFFER_SIZE 1024
    char buffer[MAX_BUFFER_SIZE];
    std::ifstream ifstream(traceFile);
    ifstream.seekg(-MAX_BUFFER_SIZE, std::ios::end);
    ifstream.read(buffer, MAX_BUFFER_SIZE);

#endif
}

/**
 * Start tracing the calling thread.
 */
void
Ftrace::start() {
#if FTRACE
    // This method can be called from dispath and worker threads and must
    // be synchronized.
    static SpinLock mutex("Ftrace::lock");
    SpinLock::Guard lock(mutex);

    string pid = std::to_string(getpid());
    static bool initialized = false;
    if (!initialized) {
        // Stop writing to the trace buffer.
        controlSet("tracing_on", "0");
        // Clear the trace buffer.
        controlSet("current_tracer", "nop");
        // Trace the first kernel function that is called from the user space.
        controlSet("current_tracer", "function_graph");
        controlSet("max_graph_depth", "1");
        // Use TSC cycle clock for timestamp.
        controlSet("trace_clock", "x86-tsc");
        // Display task/pid and absolute timestamp fields in the trace.
        controlSet("trace_options", "funcgraph-proc");
        controlSet("trace_options", "funcgraph-abstime");
        // Trace the calling thread only.
        controlSet("set_ftrace_pid", "-1");
        controlSet("set_ftrace_pid", pid.c_str());
        // Reenable writing to the trace buffer. This must happen after setting
        // the pid.
        controlSet("tracing_on", "1");
        initialized = true;
    } else {
        controlSet("set_ftrace_pid", pid.c_str());
    }
#endif
}

/**
 * Stop tracing all threads. This method should be called exactly once from the
 * dispatch thread.
 */
void
Ftrace::stopAll() {
#if FTRACE
    // Stop writing to the trace buffer.
    controlSet("tracing_on", "0");
    // Clear pids of traced threads.
    controlSet("set_ftrace_pid", "-1");

    // Close opened file descriptors.
    for (FileDescriptors::value_type& kv : fds) {
        syscall.close(kv.second);
    }
    fds.clear();
#endif
}

} // namespace RAMCloud
