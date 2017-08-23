/* Copyright (c) 2010-2016 Stanford University
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

#ifndef RAMCLOUD_MTCPTRANSPORT_H
#define RAMCLOUD_MTCPTRANSPORT_H

#include <queue>
#include <mtcp_api.h>
#include <mtcp_epoll.h>

#include "BoostIntrusive.h"
#include "Dispatch.h"
#include "IpAddress.h"
#include "Tub.h"
#include "ServerRpcPool.h"
#include "SessionAlarm.h"
#include "Syscall.h"
#include "Transport.h"

namespace RAMCloud {

/**
 * A simple transport mechanism based on MTCP/IP provided by the kernel.
 * This implementation is unlikely to be fast enough for production use;
 * this class will be used primarily for development and as a baseline
 * for testing.  The goal is to provide an implementation that is about as
 * fast as possible, given its use of kernel-based MTCP/IP.
 */
class MTcpTransport : public Transport {
  public:

    explicit MTcpTransport(Context* context,
            const ServiceLocator* serviceLocator = NULL);
    ~MTcpTransport();
    SessionRef getSession(const ServiceLocator* serviceLocator,
            uint32_t timeoutMs = 0) {
        return new MTcpSession(this, serviceLocator, timeoutMs);
    }
    string getServiceLocator() {
        return locatorString;
    }

    /**
     * Causes MTcpTransport to be invoked during each iteration through
     * the dispatch poller loop.
     */
    class Poller : public Dispatch::Poller {
      public:
        explicit Poller(Context* context, MTcpTransport* t)
            : Dispatch::Poller(context->dispatch, "MTcpTransport::Poller")
            , t(t) { }
        virtual int poll();
      private:
        // Transport on whose behalf this poller operates.
        MTcpTransport* t;
        DISALLOW_COPY_AND_ASSIGN(Poller);
    };

  PRIVATE:
    class Socket;
    class MTcpSession;

    /**
     * Header for request and response messages: precedes the actual data
     * of the message in all transmissions.
     */
    struct Header {
        /// Unique identifier for this RPC: generated on the client, and
        /// returned by the server in responses.  This field makes it
        /// possible for a client to have multiple outstanding RPCs to
        /// the same server.
        uint64_t nonce;

        /// The size in bytes of the payload (which follows immediately).
        /// Must be less than or equal to #MAX_RPC_LEN.
        uint32_t len;
    } __attribute__((packed));

    /**
     * Used to manage the receipt of a message (on either client or server)
     * using an event-based approach.
     */
    class IncomingMessage {
      public:
        IncomingMessage(Buffer* buffer, MTcpSession* session);
        void cancel();
        bool readMessage(int fd);

        Header header;

        /// The number of bytes of header that have been successfully
        /// received so far; 0 means the header has not yet been received;
        /// sizeof(Header) means the header is complete.
        uint32_t headerBytesReceived;

        /// Counts the number of bytes in the message body that have been
        /// received so far.
        uint32_t messageBytesReceived;

        /// The number of bytes of input message that we will actually retain
        /// (normally this is the same as header.len, but it may be less
        /// if header.len is illegally large or if the entire message is being
        /// discarded).
        uint32_t messageLength;

        /// Buffer in which incoming message will be stored (not including
        /// transport-specific header); NULL means we haven't yet started
        /// reading the response, or else the RPC was canceled after we
        /// started reading the response.
        Buffer* buffer;

        /// Session that will find the buffer to use for this message once
        /// the header has arrived (or NULL).
        MTcpSession* session;
      PRIVATE:
        DISALLOW_COPY_AND_ASSIGN(IncomingMessage);
    };

    /**
     * The MTCP implementation of Transport::ServerRpc.
     */
    class MTcpServerRpc : public Transport::ServerRpc {
      public:
        virtual ~MTcpServerRpc()
        {
            RAMCLOUD_TEST_LOG("deleted");
        }
        void sendReply();
        string getClientServiceLocator();

        MTcpServerRpc(Socket* socket, int fd, MTcpTransport* transport)
            : fd(fd), socketId(socket->id), message(&requestPayload, NULL),
            queueEntries(), transport(transport) { }

        int fd;                   /// File descriptor of the socket on
                                  /// which the request was received.
        uint64_t socketId;        /// Uniquely identifies this connection;
                                  /// must match sockets[fd].id.  Allows us
                                  /// to detect if fd has been closed and
                                  /// reused for a different connection.
        IncomingMessage message;  /// Records state of partially-received
                                  /// request.
        IntrusiveListHook queueEntries;
                                  /// Used to link this RPC onto the
                                  /// rpcsWaitingToReply list of the Socket.
        MTcpTransport* transport;  /// The parent MTcpTransport object.

      PRIVATE:
        DISALLOW_COPY_AND_ASSIGN(MTcpServerRpc);
    };

    /**
     * The MTCP implementation of Transport::ClientRpc.
     */
    class MTcpClientRpc {
      public:
        explicit MTcpClientRpc(Buffer* request, Buffer* response,
                RpcNotifier* notifier, uint64_t nonce)
            : request(request)
            , response(response)
            , notifier(notifier)
            , nonce(nonce)
            , sent(false)
            , queueEntries()
        { }

        Buffer* request;          /// Request message for the RPC.
        Buffer* response;         /// Will eventually hold the response message.
        RpcNotifier* notifier;    /// Use this object to report completion.
        uint64_t nonce;           /// Unique identifier for this RPC; used
                                  /// to pair the RPC with its response.
        bool sent;                /// True means the request has been sent
                                  /// and we are waiting for the response;
                                  /// false means this RPC is queued on
                                  /// rpcsWaitingToSend.
        IntrusiveListHook queueEntries;
                                  /// Used to link this RPC onto the
                                  /// rpcsWaitingToSend and
                                  /// rpcsWaitingForResponse lists of session.

      PRIVATE:
        DISALLOW_COPY_AND_ASSIGN(MTcpClientRpc);
    };

    void closeSocket(int fd);
    ssize_t recvCarefully(int fd, char* buffer, size_t length,
            IncomingMessage* message);
    int sendMessage(int fd, uint64_t nonce, Buffer* payload,
            int bytesToSend);

//    /**
//     * An event handler that will accept connections on a socket.
//     */
//    class AcceptHandler : public Dispatch::File {
//      public:
//        AcceptHandler(int fd, MTcpTransport* transport);
//        virtual void handleFileEvent(int events);
//        // Transport that manages this socket.
//        MTcpTransport* transport;
//
//      PRIVATE:
//        DISALLOW_COPY_AND_ASSIGN(AcceptHandler);
//    };

    /**
     * An event handler that moves bytes to and from a server's socket.
     */
    class ServerSocketHandler : public Dispatch::File {
      public:
        ServerSocketHandler(int fd, MTcpTransport* transport, Socket* socket);
        virtual void handleFileEvent(int events);
        // The following variables are just copies of constructor arguments.
        int fd;
        MTcpTransport* transport;
        Socket* socket;

      PRIVATE:
        DISALLOW_COPY_AND_ASSIGN(ServerSocketHandler);
    };

    /**
     * An event handler that moves bytes to and from a client-side sockes.
     */
    class ClientSocketHandler : public Dispatch::File {
      public:
        ClientSocketHandler(int fd, MTcpSession* session);
        virtual void handleFileEvent(int events);
        // The following variables are just copies of constructor arguments.
        int fd;
        MTcpSession* session;

      PRIVATE:
        DISALLOW_COPY_AND_ASSIGN(ClientSocketHandler);
    };

    /**
     * The MTCP implementation of Sessions (stored on a client to manage its
     * interactions with a particular server).
     */
    class MTcpSession : public Session {
      public:
        explicit MTcpSession(MTcpTransport* transport,
                const ServiceLocator* serviceLocator,
                uint32_t timeoutMs = 0);
        ~MTcpSession();
        virtual void abort();
        virtual void cancelRequest(RpcNotifier* notifier);
        Buffer* findRpc(Header* header);
        virtual string getRpcInfo();
        virtual void sendRequest(Buffer* request, Buffer* response,
                RpcNotifier* notifier);
#if TESTING
        explicit MTcpSession(MTcpTransport* transport) :
            Session(""), transport(transport),
            address(), fd(-1), serial(1),
            rpcsWaitingToSend(), bytesLeftToSend(0),
            rpcsWaitingForResponse(), current(NULL),
            message(), clientIoHandler(),
            alarm(transport->context->sessionAlarmTimer, this, 0) { }
#endif
        void close();

        MTcpTransport* transport;  /// Transport that owns this session.
        IpAddress address;        /// Server to which requests will be sent.
        int fd;                   /// File descriptor for the socket that
                                  /// connects to address  -1 means no socket
                                  /// open (the socket was aborted because of
                                  /// an error).
        uint64_t serial;          /// Used to generate nonces for RPCs: starts
                                  /// at 1 and increments for each RPC.

        INTRUSIVE_LIST_TYPEDEF(MTcpClientRpc, queueEntries) ClientRpcList;
        ClientRpcList rpcsWaitingToSend;
                                  /// RPCs whose request messages have not yet
                                  /// been transmitted.  The front RPC on this
                                  /// list is currently being transmitted.
        int bytesLeftToSend;      /// The number of (trailing) bytes in the
                                  /// first RPC on rpcsWaitingToSend that still
                                  /// need to be transmitted, once fd becomes
                                  /// writable again.  -1 or 0 means there
                                  /// are no RPCs waiting to be transmitted.
        ClientRpcList rpcsWaitingForResponse;
                                  /// RPCs whose request messages have been
                                  /// transmitted, but whose responses have
                                  /// not yet been received.
        MTcpClientRpc* current;    /// RPC for which we are currently receiving
                                  /// a response (NULL if none).
        Tub<IncomingMessage> message;
                                  /// Records state of partially-received
                                  /// reply for current.
        Tub<ClientSocketHandler> clientIoHandler;
                                  /// Used to get notified when response data
                                  /// arrives.
        SessionAlarm alarm;       /// Used to detect server timeouts.

      PRIVATE:
        DISALLOW_COPY_AND_ASSIGN(MTcpSession);
    };

    static Syscall* sys;

    /// Shared RAMCloud information.
    Context* context;

    /// Service locator used to open server socket (empty string if this
    /// isn't a server). May differ from what was passed to the constructor
    /// if dynamic ports are used.
    string locatorString;

    /// File descriptor used by servers to listen for connections from
    /// clients.  -1 means this instance is not a server.
    int listenSocket;

    // FIXME: mMTCP thread per-core context
    mctx_t mctx;

    // TODO
    int epollId;

    // TODO
#define EPOLL_EVENT_QUEUE_SIZE 100
    struct mtcp_epoll_event epollEvents[EPOLL_EVENT_QUEUE_SIZE];

//    /// Used to wait for listenSocket to become readable.
//    Tub<AcceptHandler> acceptHandler;

    /// Used to hold information about a file descriptor associated with
    /// a socket, on which RPC requests may arrive.
    class Socket {
      public:
        Socket(int fd, MTcpTransport* transport, sockaddr_in& sin);
        ~Socket();
        MTcpTransport* transport;  /// The parent MTcpTransport object.
        uint64_t id;              /// Unique identifier: no other Socket
                                  /// for this transport instance will use
                                  /// the same value.
        MTcpServerRpc* rpc;        /// Incoming RPC that is in progress for
                                  /// this fd, or NULL if none.
        ServerSocketHandler ioHandler;
                                  /// Used to get notified whenever data
                                  /// arrives on this fd.
        INTRUSIVE_LIST_TYPEDEF(MTcpServerRpc, queueEntries) ServerRpcList;
        ServerRpcList rpcsWaitingToReply;
                                  /// RPCs whose response messages have not yet
                                  /// been transmitted.  The front RPC on this
                                  /// list is currently being transmitted.
        int bytesLeftToSend;      /// The number of (trailing) bytes in the
                                  /// front RPC on rpcsWaitingToReply that still
                                  /// need to be transmitted, once fd becomes
                                  /// writable again.  -1 or 0 means there are
                                  /// no RPCs waiting.
        struct sockaddr_in sin;   /// sockaddr_in of the client host on the
                                  /// other end of the socket. Used to
                                  /// implement #getClientServiceLocator().
      PRIVATE:
        DISALLOW_COPY_AND_ASSIGN(Socket);
    };

    /// Keeps track of all of our open client connections. Entry i has
    /// information about file descriptor i (NULL means no client
    /// is currently connected).
    std::vector<Tub<Socket>> sockets;

    /// Used to assign increasing id values to Sockets.
    uint64_t nextSocketId;

    /// Counts the number of nonzero-size partial messages sent by
    /// sendMessage (for testing only).
    static int messageChunks;

    /// Allows us to get invoked during the dispatch polling loop.
    Poller poller;

    /// Pool allocator for our ServerRpc objects.
    ServerRpcPool<MTcpServerRpc> serverRpcPool;

    /// Pool allocator for MTcpClientRpc objects.
    ObjectPool<MTcpClientRpc> clientRpcPool;

    DISALLOW_COPY_AND_ASSIGN(MTcpTransport);
};

}  // namespace RAMCloud

#endif  // RAMCLOUD_MTCPTRANSPORT_H
