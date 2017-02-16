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

#ifndef RAMCLOUD_FTRACE_H
#define RAMCLOUD_FTRACE_H

#include <fcntl.h>
#include <fstream>
#include <sstream>

#include "Common.h"

namespace RAMCloud {

/**
 * Miscellaneous helper methods useful for interacting with the Linux
 * function tracer: ftrace.
 */
namespace Ftrace {

// TODO: SET IT TO 0 BY DEFAULT
#ifndef FTRACE
#define FTRACE 1
#endif

// Path to the directory that holds all ftrace control and output files.
// This directory only presents after the `debugfs` file system is mounted.
static string tracingFs = "/sys/kernel/debug/tracing/";

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
controlSet(const char* option, const char* value)
{
#if FTRACE
    // Map from ftrace control files to file descriptors.
    using TraceFileDescriptors = std::map<string, int>;
    static TraceFileDescriptors traceFds = {};
    int fd;

    TraceFileDescriptors::iterator it = traceFds.find(option);
    if (it == traceFds.end()) {
        fd = open((tracingFs + option).c_str(), O_WRONLY);
        if (fd < 0) {
            RAMCLOUD_LOG(ERROR, "cannot open %s%s, errno %s",
                    tracingFs.c_str(), option, strerror(errno));
            return;
        }
        traceFds[option] = fd;
    } else {
        fd = it->second;
    }

    ssize_t ret = write(fd, value, strlen(value));
    if (ret < 0) {
        RAMCLOUD_LOG(ERROR, "write failed: %s", strerror(errno));
    }
#endif
}

/**
 * Print the collected ftrace to the system log.
 */
void
printToLog()
{
#if FTRACE
    std::ifstream ifstream((tracingFs + "trace").c_str());
    std::stringstream buffer;
    buffer << ifstream.rdbuf();
    RAMCLOUD_LOG(NOTICE, "Logging collected ftrace:\n%s",
            buffer.str().c_str());
#endif
}

}

} // namespace RAMCloud

#endif // RAMCLOUD_FTRACE_H

