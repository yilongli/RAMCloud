/* Copyright (c) 2012 Stanford University
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

#include "TestUtil.h"

#include "ServerConfig.h"
#include "SegmentManager.h"
#include "ReplicaManager.h"
#include "LogEntryRelocator.h"

namespace RAMCloud {

/**
 * Unit tests for LogEntryRelocator.
 */
class LogEntryRelocatorTest : public ::testing::Test {
  public:
    Context context;
    ServerId serverId;
    ServerList serverList;
    ServerConfig serverConfig;
    ReplicaManager replicaManager;
    SegletAllocator allocator;
    SegmentManager segmentManager;

    LogEntryRelocatorTest()
        : context(),
          serverId(ServerId(57, 0)),
          serverList(&context),
          serverConfig(ServerConfig::forTesting()),
          replicaManager(&context, &serverId, 0, false),
          allocator(&serverConfig),
          segmentManager(&context, &serverConfig, &serverId,
                         allocator, replicaManager)
    {
    }

    DISALLOW_COPY_AND_ASSIGN(LogEntryRelocatorTest);
};

TEST_F(LogEntryRelocatorTest, constructor) {
    LogEntryRelocator r(NULL, 50);
    EXPECT_EQ(static_cast<LogSegment*>(NULL), r.segment);
    EXPECT_EQ(50U, r.maximumLength);
    EXPECT_EQ(-1U, r.offset);
    EXPECT_FALSE(r.outOfSpace);
    EXPECT_FALSE(r.didAppend);
    EXPECT_EQ(0U, r.appendTicks);
}

TEST_F(LogEntryRelocatorTest, append_nullSegment) {
    LogEntryRelocator r(NULL, 50);
    Buffer buffer;
    EXPECT_FALSE(r.append(LOG_ENTRY_TYPE_OBJ, buffer, 0));
    EXPECT_TRUE(r.outOfSpace);
}

TEST_F(LogEntryRelocatorTest, append_tooBig) {
    LogEntryRelocator r(NULL, 1);
    Buffer buffer;
    buffer.append("!", 2);
    EXPECT_THROW(r.append(LOG_ENTRY_TYPE_OBJ, buffer, 0),
        FatalError);
}

TEST_F(LogEntryRelocatorTest, append_alreadyAppended) {
    LogEntryRelocator r(NULL, 50);
    r.didAppend = true;
    Buffer buffer;
    EXPECT_THROW(r.append(LOG_ENTRY_TYPE_OBJ, buffer, 0),
        FatalError);
}

TEST_F(LogEntryRelocatorTest, append) {
    LogSegment* s = segmentManager.allocHeadSegment();
    LogEntryRelocator r(s, 50);
    Buffer buffer;
    buffer.append("!", 2);
    uint32_t bytesBefore = s->statistics.liveBytes;
    uint64_t spaceTimeSumBefore = s->statistics.spaceTimeSum;
    EXPECT_TRUE(r.append(LOG_ENTRY_TYPE_OBJ, buffer, 2));
    EXPECT_TRUE(r.didAppend);
    EXPECT_GT(s->statistics.liveBytes, bytesBefore);
    EXPECT_GT(s->statistics.spaceTimeSum, spaceTimeSumBefore);
}

TEST_F(LogEntryRelocatorTest, getNewReference_noAppend) {
    LogEntryRelocator r(NULL, 50);
    EXPECT_THROW(r.getNewReference(), FatalError);
}

TEST_F(LogEntryRelocatorTest, getNewReference) {
    LogSegment* s = segmentManager.allocHeadSegment();
    LogEntryRelocator r(s, 50);
    Buffer buffer;
    buffer.append("!", 2);
    EXPECT_TRUE(r.append(LOG_ENTRY_TYPE_OBJ, buffer, 0));
    EXPECT_NO_THROW(r.getNewReference());
}

} // namespace RAMCloud
