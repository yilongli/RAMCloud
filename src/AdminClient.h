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

#ifndef RAMCLOUD_ADMINCLIENT_H
#define RAMCLOUD_ADMINCLIENT_H

#include "ServerId.h"
#include "ServerIdRpcWrapper.h"
#include "ServerMetrics.h"
#include "Transport.h"

namespace RAMCloud {

/**
 * This class implements the client-side interface to the ping service.
 * The class contains only static methods, so you shouldn't ever need
 * to instantiate an object.
 */
class AdminClient {
  public:
    static void ping(Context* context, ServerId targetId,
            ServerId callerId = ServerId());
//    static void clockSync(Context* context, ServerId targetId,
//            ServerId callerId = ServerId());
    static uint64_t proxyPing(Context* context, ServerId proxyId,
            ServerId targetId, uint64_t timeoutNanoseconds);
    static void serverControl(Context* context, ServerId serverId,
            WireFormat::ControlOp controlOp, const void* inputData = NULL,
            uint32_t inputLength = 0, Buffer* outputData = NULL);
    static void logMessage(Context* context, ServerId serverId,
            LogLevel level, const char* fmt, ...)
        __attribute__ ((format (gnu_printf, 4, 5)));
    static ServerId getServerId(Context* context,
            Transport::SessionRef session);

  private:
    AdminClient();
};

/**
 * Encapsulates the state of a AdminClient::clockSync request, allowing it to
 * execute asynchronously.
 */
class ClockSyncRpc : public RpcWrapper {
  public:
    ClockSyncRpc(Context* context, Transport::SessionRef session,
            uint64_t baseTsc, ServerId targetId, ServerId callerId,
            uint64_t fastestClientTsc, uint64_t fastestServerTsc);
    ~ClockSyncRpc() {}
    void completed();
    uint64_t wait();
    uint64_t getClientTsc();
    uint64_t getCompletionTime();

    ServerId targetId;
  PRIVATE:
    Context* context;

    /// Cycles::rdtsc ticks when we logically reset the timestamp counter on
    /// this machine.
    uint64_t baseTsc;

    /// Cycles::rdtsc ticks since #baseTsc when we invoke RpcWrapper::send().
    uint64_t startTime;

    /// Cycles::rdtsc ticks since #baseTsc when the RPC completes.
    uint64_t endTime;
    DISALLOW_COPY_AND_ASSIGN(ClockSyncRpc);
};

/**
 * Encapsulates the state of a AdminClient::getServerId request, allowing
 * it to execute asynchronously. This RPC is unusual in that it's a subclass
 * of RpcWrapper; this means that it doesn't retry if there are any problems
 * (this is the correct behavior for its normal usage in verifying server ids).
 */
class GetServerIdRpc : public RpcWrapper {
    public:
    GetServerIdRpc(Context* context, Transport::SessionRef session);
    ~GetServerIdRpc() {}
    ServerId wait();

    PRIVATE:
    // Overall server information.
    Context* context;

    DISALLOW_COPY_AND_ASSIGN(GetServerIdRpc);
};

/**
 * Encapsulates the state of a AdminClient::ping
 * request, allowing it to execute asynchronously.
 */
class PingRpc : public ServerIdRpcWrapper {
    public:
    PingRpc(Context* context, ServerId targetId,
            ServerId callerId = ServerId());
    ~PingRpc() {}
    /// \copydoc ServerIdRpcWrapper::waitAndCheckErrors
    void wait() {waitAndCheckErrors();}
    bool wait(uint64_t timeoutNanoseconds);

    PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(PingRpc);
};

/**
 * Encapsulates the state of a AdminClient::proxyPing
 * request, allowing it to execute asynchronously.
 */
class ProxyPingRpc : public ServerIdRpcWrapper {
    public:
    ProxyPingRpc(Context* context, ServerId proxyId, ServerId targetId,
            uint64_t timeoutNanoseconds);
    ~ProxyPingRpc() {}
    uint64_t wait();

    PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(ProxyPingRpc);
};

/**
 * Encapsulates the state of a AdminClient::serverControl operation,
 * allowing it to execute asynchronously.
 */
class ServerControlRpc : public ServerIdRpcWrapper {
  public:
    ServerControlRpc(Context* context, ServerId serverId,
            WireFormat::ControlOp controlOp, const void* inputData = NULL,
            uint32_t inputLength = 0, Buffer* outputData = NULL);
    ~ServerControlRpc() {}
    void wait();
    bool waitRaw();
  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(ServerControlRpc);
};

} // namespace RAMCloud

#endif // RAMCLOUD_ADMINCLIENT_H
