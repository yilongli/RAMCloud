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

#ifndef RAMCLOUD_TCPTRANSPORT_H
#define RAMCLOUD_TCPTRANSPORT_H

#include <queue>

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
 * A simple transport mechanism based on TCP/IP provided by the kernel.
 * This implementation is unlikely to be fast enough for production use;
 * this class will be used primarily for development and as a baseline
 * for testing.  The goal is to provide an implementation that is about as
 * fast as possible, given its use of kernel-based TCP/IP.
 */
class TcpTransport : public Transport {
  public:

    explicit TcpTransport(Context* context,
            const ServiceLocator* serviceLocator = NULL);
    ~TcpTransport();
    SessionRef getSession(const ServiceLocator* serviceLocator,
            uint32_t timeoutMs = 0) {
        return new TcpSession(this, serviceLocator, timeoutMs);
    }
    string getServiceLocator() {
        return locatorString;
    }

  PRIVATE:
    class Socket;
    class TcpSession;

    /**
     * Header for request and response messages: precedes the actual data
     * of the message in all transmissions.
     */
    struct Header {
        /// Unique identifier for this RPC: generated on the client, and
        /// returned by the server in responses.  This field makes it
        /// possible for a client to have multiple outstanding RPCs to
        /// the same server.
        uint64_t rpcId;

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
        IncomingMessage(Buffer* buffer);
        IncomingMessage(TcpSession* session);
        bool tryToReadMessage(int fd);

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
        /// transport-specific header). May be NULL if this is a response
        /// message and the client hasn't yet started reading the response,
        /// or the RPC was canceled after we started reading the response.
        Buffer* buffer;

        /// Used by the client to find the buffer that stores this incoming
        /// response once the header has arrived. NULL if this message is a
        /// request.
        TcpSession* session;
      PRIVATE:
        DISALLOW_COPY_AND_ASSIGN(IncomingMessage);
    };

    /**
     * Holds server-side state for an RPC.
     */
    class TcpServerRpc : public Transport::ServerRpc {
      public:
        virtual ~TcpServerRpc()
        {
            RAMCLOUD_TEST_LOG("deleted");
        }
        void sendReply();
        string getClientServiceLocator();

        // FIXME: rename all transport to t (like Homa & Basic)?
        TcpServerRpc(int fd, TcpTransport* transport)
            : fd(fd)
            , socketId(transport->serverSockets[fd]->id)
            , request(&requestPayload)
            , bytesSent(0)
            , outgoingResponseLinks()
            , transport(transport)
        {}

        // FIXME: what is the difference between the fd of a socket
        // and the id of a socket? I assume socket id identifies the connection
        // so it can change when the same fd is reused for another connection?
        const int fd;             /// File descriptor of the socket on
                                  /// which the request was received.
        const uint64_t socketId;  /// Uniquely identifies this connection;
                                  /// must match sockets[fd].id.  Allows us
                                  /// to detect if fd has been closed and
                                  /// reused for a different connection.
        IncomingMessage request;  /// Records state of partially-received
                                  /// request.

        /// # bytes in the response message (including header) that have been
        /// successfully transmitted.
        uint32_t bytesSent;

        IntrusiveListHook outgoingResponseLinks;
                                  /// Used to link this RPC onto the
                                  /// outgoingResponses list of the Socket.
        TcpTransport* transport;  /// The parent TcpTransport object.

      PRIVATE:
        DISALLOW_COPY_AND_ASSIGN(TcpServerRpc);
    };

    /**
     * One object of this class exists for each outgoing RPC; it is used
     * to track the RPC through to completion.
     */
    class ClientRpc {
      public:
        explicit ClientRpc(Buffer* request, Buffer* response,
                RpcNotifier* notifier, uint64_t rpcId)
            : request(request)
            , response(response)
            , notifier(notifier)
            , rpcId(rpcId)
            , transmitPending(true)
            , bytesSent(0)
            , outgoingRequestLinks()
        { }

        Buffer* request;          /// Request message for the RPC.
        Buffer* response;         /// Will eventually hold the response message.
        RpcNotifier* notifier;    /// Use this object to report completion.
        uint64_t rpcId;           /// Unique identifier for this RPC; used
                                  /// to pair the RPC with its response.

        /// True means that the request message is in the process of being
        /// transmitted (and this object is linked on outgoingRequests).
        bool transmitPending;

        /// # bytes in the request message (including header) that have been
        /// successfully transmitted.
        uint32_t bytesSent;

        /// Used to link this RPC onto the outgoingRequests.
        IntrusiveListHook outgoingRequestLinks;

      PRIVATE:
        DISALLOW_COPY_AND_ASSIGN(ClientRpc);
    };

    static ssize_t recvCarefully(int fd, void* buffer, size_t length);
    static uint32_t sendMessage(int fd, uint64_t rpcId, Buffer* payload,
            uint32_t bytesSent);

    /**
     * An event handler that will accept connections on a socket.
     */
    class AcceptHandler : public Dispatch::File {
      public:
        AcceptHandler(int fd, TcpTransport* transport);
        virtual void handleFileEvent(int events);
      PRIVATE:
        // Transport that manages this socket.
        TcpTransport* transport;
        DISALLOW_COPY_AND_ASSIGN(AcceptHandler);
    };

    /**
     * An event handler that moves bytes to and from a server's socket.
     */
    class ServerSocketHandler : public Dispatch::File {
      public:
        virtual void handleFileEvent(int events);

        /// File descriptor for the server socket.
        int fd;

        /// Server socket associated with this event handler.
        Socket* socket;

        explicit ServerSocketHandler(int fd, Socket* socket)
            : Dispatch::File(socket->transport->context->dispatch, fd,
                    Dispatch::FileEvent::READABLE)
            , fd(fd)
            , socket(socket)
        {}

      PRIVATE:
        DISALLOW_COPY_AND_ASSIGN(ServerSocketHandler);
    };

    /**
     * An event handler that moves bytes to and from a client-side socket.
     */
    class ClientSocketHandler : public Dispatch::File {
      public:
        virtual void handleFileEvent(int events);

        /// File descriptor for the client socket.
        int fd;

        /// Client socket associated with this event handler.
        TcpSession* session;

        explicit ClientSocketHandler(int fd, TcpSession* session)
            : Dispatch::File(session->transport->context->dispatch, fd,
                    Dispatch::FileEvent::READABLE)
            , fd(fd)
            , session(session)
        {}

      PRIVATE:
        DISALLOW_COPY_AND_ASSIGN(ClientSocketHandler);
    };

    typedef std::unordered_map<uint64_t, ClientRpc*> ClientRpcMap;

    /**
     * This class represents the client side of the connection between a
     * particular client and a particular server. Each session can have at
     * most one incoming response at any given time.
     */
    class TcpSession : public Session {
        // FIXME: rename to Session or ClientSocket?
      public:
        explicit TcpSession(TcpTransport* transport,
                const ServiceLocator* serviceLocator,
                uint32_t timeoutMs = 0);
        ~TcpSession();
        virtual void abort();
        virtual void cancelRequest(RpcNotifier* notifier);
        virtual string getRpcInfo();
        virtual void sendRequest(Buffer* request, Buffer* response,
                RpcNotifier* notifier);
#if TESTING
        explicit TcpSession(TcpTransport* transport) :
            Session(""), transport(transport),
            address(), fd(-1), serial(1),
            outgoingRequests(), bytesLeftToSend(0),
            rpcsWaitingForResponse(), incomingResponse(NULL),
            message(), clientIoHandler(),
            alarm(transport->context->sessionAlarmTimer, this, 0) { }
#endif
        void close();

        TcpTransport* transport;  /// Transport that owns this session.
        IpAddress address;        /// Server to which requests will be sent.
        int fd;                   /// File descriptor for the socket that
                                  /// connects to address  -1 means no socket
                                  /// open (the socket was aborted because of
                                  /// an error).
        uint64_t nextRpcId;       /// Used to generate nonces for RPCs: starts
                                  /// at 1 and increments for each RPC.

        INTRUSIVE_LIST_TYPEDEF(ClientRpc, outgoingRequestLinks) ClientRpcList;
        ClientRpcList outgoingRequests;
                                  /// RPCs whose request messages have not yet
                                  /// been transmitted.  The front RPC on this
                                  /// list is currently being transmitted.

        /// Holds RPCs for which we are the client, and for which a
        /// response has not yet been completely received (we have sent
        /// at least part of the request, but not necessarily the entire
        /// request yet). Keys are RPC identifiers.
        ClientRpcMap outgoingRpcs;

        /// Holds the partially-received response for currentRpc.
        Tub<IncomingMessage> currentResponse;

        /// RPC for which we are currently receiving a response. NULL until
        /// the header of currentResponse is complete.
        ClientRpc* currentRpc;

        // FIXME: WHY IS THIS INSTANCE PROTECTED BY THE DISPATCH LOCK?
        Tub<ClientSocketHandler> clientIoHandler;
                                  /// Used to get notified when response data
                                  /// arrives.
        SessionAlarm alarm;       /// Used to detect server timeouts.

      PRIVATE:
        DISALLOW_COPY_AND_ASSIGN(TcpSession);
    };

    /**
     * This class represents the server side of the connection between a
     * particular server and a particular client. Each socket can have at
     * most one incoming request at any given time.
     */
    class Socket {
        // FIXME: RENAME TO ServerSocket
      public:
        Socket(int fd, TcpTransport* transport, sockaddr_in* sin);
        ~Socket();
        TcpTransport* transport;  /// The parent TcpTransport object.
        uint64_t id;              /// Unique identifier: no other Socket
                                  /// for this transport instance will use
                                  /// the same value.
        TcpServerRpc* incomingRpc;/// Incoming RPC that is in progress for
                                  /// this fd, or NULL if none.
        ServerSocketHandler ioHandler;
                                  /// Used to get notified whenever data
                                  /// arrives on this fd.
        INTRUSIVE_LIST_TYPEDEF(TcpServerRpc, outgoingResponseLinks) ServerRpcList;
        ServerRpcList outgoingResponses;
                                  /// RPCs whose response messages have not yet
                                  /// been transmitted.  The front RPC on this
                                  /// list is currently being transmitted.
        struct sockaddr_in sin;   /// sockaddr_in of the client host on the
                                  /// other end of the socket. Used to
                                  /// implement #getClientServiceLocator().
      PRIVATE:
        DISALLOW_COPY_AND_ASSIGN(Socket);
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

    /// Used to wait for listenSocket to become readable.
    Tub<AcceptHandler> acceptHandler;

    /// Keeps track of all of our open client connections. Entry i has
    /// information about file descriptor i (NULL means no client
    /// is currently connected).
    typedef std::unordered_map<int, Tub<Socket>> ServerSockets;
    ServerSockets serverSockets;

    /// Used to assign increasing id values to ServerSockets.
    uint64_t nextServerSocketId;

    /// Counts the number of nonzero-size partial messages sent by
    /// sendMessage (for testing only).
    static int messageChunks;

    /// Pool allocator for our ServerRpc objects.
    ServerRpcPool<TcpServerRpc> serverRpcPool;

    /// Pool allocator for ClientRpc objects.
    ObjectPool<ClientRpc> clientRpcPool;

    DISALLOW_COPY_AND_ASSIGN(TcpTransport);
};

}  // namespace RAMCloud

#endif  // RAMCLOUD_TCPTRANSPORT_H
