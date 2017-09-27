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

#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>

#include "Common.h"
#include "PerfStats.h"
#include "ShortMacros.h"
#include "TcpTransport.h"
#include "WorkerManager.h"

namespace RAMCloud {

int TcpTransport::messageChunks = 0;

/**
 * Default object used to make system calls.
 */
static Syscall defaultSyscall;

/**
 * Used by this class to make all system calls.  In normal production
 * use it points to defaultSyscall; for testing it points to a mock
 * object.
 */
Syscall* TcpTransport::sys = &defaultSyscall;

/**
 * Construct a TcpTransport instance.
 *
 * \param context
 *      Overall information about the RAMCloud server or client.
 * \param serviceLocator
 *      If non-NULL this transport will be used to serve incoming
 *      RPC requests as well as make outgoing requests; this parameter
 *      specifies the (local) address on which to listen for connections.
 *      If NULL this transport will be used only for outgoing requests.
 *
 * \throw TransportException
 *      There was a problem that prevented us from creating the transport.
 */
TcpTransport::TcpTransport(Context* context,
        const ServiceLocator* serviceLocator)
    : context(context)
    , locatorString()
    , listenSocket(-1)
    , acceptHandler()
    , serverSockets()
    , nextServerSocketId(100)
    , serverRpcPool()
    , clientRpcPool()
{
    if (serviceLocator == NULL)
        return;
    IpAddress address(serviceLocator);
    locatorString = serviceLocator->getOriginalString();

    listenSocket = sys->socket(PF_INET, SOCK_STREAM, 0);
    if (listenSocket == -1) {
        LOG(WARNING, "TcpTransport couldn't create listen socket: %s",
                strerror(errno));
        throw TransportException(HERE,
                "TcpTransport couldn't create listen socket", errno);
    }

    int r = sys->fcntl(listenSocket, F_SETFL, O_NONBLOCK);
    if (r != 0) {
        sys->close(listenSocket);
        LOG(WARNING, "TcpTransport couldn't set nonblocking on listen "
                "socket: %s", strerror(errno));
        throw TransportException(HERE,
                "TcpTransport couldn't set nonblocking on listen socket",
                errno);
    }

    int optval = 1;
    if (sys->setsockopt(listenSocket, SOL_SOCKET, SO_REUSEADDR, &optval,
                           sizeof(optval)) != 0) {
        sys->close(listenSocket);
        LOG(WARNING, "TcpTransport couldn't set SO_REUSEADDR on "
                "listen socket: %s", strerror(errno));
        throw TransportException(HERE,
                "TcpTransport couldn't set SO_REUSEADDR on listen socket",
                errno);
    }

    if (sys->bind(listenSocket, &address.address,
            sizeof(address.address)) == -1) {
        sys->close(listenSocket);
        string message = format("TcpTransport couldn't bind to '%s'",
                serviceLocator->getOriginalString().c_str());
        LOG(WARNING, "%s: %s", message.c_str(), strerror(errno));
        throw TransportException(HERE, message, errno);
    }

    if (sys->listen(listenSocket, INT_MAX) == -1) {
        sys->close(listenSocket);
        LOG(WARNING, "TcpTransport couldn't listen on socket: %s",
                strerror(errno));
        throw TransportException(HERE,
                "TcpTransport couldn't listen on socket", errno);
    }

    // Arrange to be notified whenever anyone connects to listenSocket.
    acceptHandler.construct(listenSocket, this);
}

/**
 * Destructor for TcpTransports: close file descriptors and perform
 * any other needed cleanup.
 */
TcpTransport::~TcpTransport()
{
    if (listenSocket >= 0) {
        sys->close(listenSocket);
    }
}

/**
 * Constructor for Sockets.
 */
TcpTransport::Socket::Socket(int fd, TcpTransport* transport, sockaddr_in* sin)
    : transport(transport)
    , id(transport->nextServerSocketId)
    , incomingRpc(NULL)
    , ioHandler(fd, this)
    , outgoingResponses()
//    , bytesLeftToSend(0)
    , sin(*sin)
{
    transport->nextServerSocketId++;
}

/**
 * Destructor for Sockets.
 */
TcpTransport::Socket::~Socket() {
    if (incomingRpc != NULL) {
        transport->serverRpcPool.destroy(incomingRpc);
    }
    while (!outgoingResponses.empty()) {
        TcpServerRpc* rpc = &outgoingResponses.front();
        outgoingResponses.pop_front();
        transport->serverRpcPool.destroy(rpc);
    }

    // No need to erase fd from transport->serverSockets because
    // this destructor must have been triggered by that.
//    LOG(ERROR, "CLOSE SOCKET!!!");
    sys->close(ioHandler.fd);
}


/**
 * Constructor for AcceptHandlers.
 *
 * \param fd
 *      File descriptor for a socket on which the #listen system call has
 *      been invoked.
 * \param transport
 *      Transport that manages this socket.
 */
TcpTransport::AcceptHandler::AcceptHandler(int fd, TcpTransport* transport)
    : Dispatch::File(transport->context->dispatch, fd,
            Dispatch::FileEvent::READABLE)
    , transport(transport)
{}

/**
 * This method is invoked by Dispatch when a listening socket becomes
 * readable; it tries to open a new connection with a client. If that
 * succeeds then we will begin listening on that socket for RPCs.
 *
 * \param events
 *      Indicates whether the socket was readable, writable, or both
 *      (OR-ed combination of Dispatch::FileEvent bits).
 */
void
TcpTransport::AcceptHandler::handleFileEvent(int events)
{
    struct sockaddr_in sin;
    socklen_t socklen = sizeof(sin);

    int acceptedFd = sys->accept(transport->listenSocket,
            reinterpret_cast<sockaddr*>(&sin), &socklen);
    if (acceptedFd < 0) {
        switch (errno) {
            // According to the man page for accept, you're supposed to
            // treat these as retry on Linux.
            case EHOSTDOWN:
            case EHOSTUNREACH:
            case ENETDOWN:
            case ENETUNREACH:
            case ENONET:
            case ENOPROTOOPT:
            case EOPNOTSUPP:
            case EPROTO:
                return;

            // No incoming connections are currently available.
            case EAGAIN:
#if EAGAIN != EWOULDBLOCK
            case EWOULDBLOCK:
#endif
                return;
        }

        // Unexpected error: log a message and then close the socket
        // (so we don't get repeated errors).
        LOG(ERROR, "error in TcpTransport::AcceptHandler accepting "
                "connection for '%s': %s",
                transport->locatorString.c_str(), strerror(errno));
        this->events = 0;
        sys->close(transport->listenSocket);
        transport->listenSocket = -1;
        return;
    }

    // Disable the hideous Nagle algorithm, which will delay sending small
    // messages in some situations (before adding this code in 5/2015, we
    // observed occasional 40ms delays when a server responded to a batch
    // of requests from the same client).
    int flag = 1;
    setsockopt(acceptedFd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

    // At this point we have successfully opened a client connection.
    // Save information about it and create a handler for incoming
    // requests.
    transport->serverSockets[acceptedFd].construct(acceptedFd, transport,
            &sin);
}

/**
 * This method is invoked by Dispatch when a server's connection from a client
 * becomes readable or writable.  It attempts to read incoming messages from
 * the socket.  If a full message is available, a TcpServerRpc object gets
 * queued for service.  It also attempts to write responses to the socket
 * (if there are responses waiting for transmission).
 *
 * \param events
 *      Indicates whether the socket was readable, writable, or both
 *      (OR-ed combination of Dispatch::FileEvent bits).
 */
void
TcpTransport::ServerSocketHandler::handleFileEvent(int events)
{
    // FIXME: The following comment should be deprecated now.
    // The following variables are copies of data from this object;
    // they are needed to safely detect socket closure below.
    TcpTransport* transport = socket->transport;
    Socket* socket = transport->serverSockets[fd].get();
    assert(socket != NULL);
    try {
        if (events & Dispatch::FileEvent::READABLE) {
            if (socket->incomingRpc == NULL) {
                socket->incomingRpc =
                        transport->serverRpcPool.construct(fd, transport);
            }
            if (socket->incomingRpc->request.tryToReadMessage(fd)) {
                // The incoming request is complete; pass it off for servicing.
                transport->context->workerManager->handleRpc(
                        socket->incomingRpc);
                socket->incomingRpc = NULL;
            }
        }
        // FIXME: I don't think the following check is necessary anymore,
        // since the only place to close a server socket is in the catch
        // block of this method.
        // Check to see if this socket got closed due to an error in the
        // read handler; if so, it's neither necessary nor safe to continue
        // in this method. Note: the check for closure must be done without
        // accessing any fields in this object, since the object may have been
        // destroyed and its memory recycled.
//        if (socket != transport->serverSockets[fd].get()) {
//            return;
//        }
        if (events & Dispatch::FileEvent::WRITABLE) {
            while (!socket->outgoingResponses.empty()) {
                TcpServerRpc* rpc = &socket->outgoingResponses.front();
                rpc->bytesSent = TcpTransport::sendMessage(fd,
                        rpc->request.header.rpcId, &rpc->replyPayload,
                        rpc->bytesSent);
                if (rpc->bytesSent <
                        rpc->replyPayload.size() + sizeof32(Header)) {
                    return;
                }
                // The current reply is finished; start the next one, if
                // there is one.
                socket->outgoingResponses.pop_front();
                transport->serverRpcPool.destroy(rpc);
            }
            // Optimization: no more outstanding responses, only notify me
            // when the socket becomes readable.
            this->events = Dispatch::FileEvent::READABLE;
        }
    } catch (TransportException& e) {
        // Close the server socket.
        transport->serverSockets.erase(fd);
    }
}

/**
 * This method is invoked when the socket connecting to a server becomes
 * readable or writable. This method reads or writes the socket as
 * appropriate.
 *
 * \param events
 *      Indicates whether the socket was readable, writable, or both
 *      (OR-ed combination of Dispatch::FileEvent bits).
 */
void
TcpTransport::ClientSocketHandler::handleFileEvent(int events)
{
    try {
        if (events & Dispatch::FileEvent::READABLE) {
            if (session->currentResponse->tryToReadMessage(fd)) {
                // This RPC is finished.
                ClientRpc* rpc = session->currentRpc;
                // FIXME: The only reason the following check may fail is because
                // the rpc might be cancelled?
                if (rpc != NULL) {
                    session->outgoingRpcs.erase(rpc->rpcId);
//                    session->alarm.rpcFinished();
                    rpc->notifier->completed();
                    session->transport->clientRpcPool.destroy(rpc);
                    session->currentRpc = NULL;
                }
                session->currentResponse.construct(session);
            }
        }
        if (events & Dispatch::FileEvent::WRITABLE) {
            while (!session->outgoingRequests.empty()) {
                ClientRpc* rpc = &session->outgoingRequests.front();
                rpc->bytesSent = TcpTransport::sendMessage(
                        session->fd, rpc->rpcId, rpc->request,
                        rpc->bytesSent);
                if (rpc->bytesSent <
                        rpc->request->size() + sizeof32(Header)) {
                    return;
                }
                // The current RPC request is finished; start the next one,
                // if there is one.
                session->outgoingRequests.pop_front();
                rpc->transmitPending = false;
            }
            // Optimization: No more outstanding requests, only notify me
            // when the socket becomes readable.
            this->events = Dispatch::FileEvent::READABLE;
        }
    } catch (TransportException& e) {
        session->abort();
    }
}

/**
 * Transmit an RPC request or response on a socket.  This method uses
 * a non-blocking approach: if the entire message cannot be transmitted,
 * it transmits as many bytes as possible and returns how many bytes in
 * the payload have been successfully transmitted.
 *
 * \param fd
 *      File descriptor to write.
 * \param rpcId
 *      Unique identifier for the RPC; must never have been used
 *      before on this socket.
 * \param payload
 *      Message to transmit on fd; this method adds on a header.
 * \param bytesSent
 *      # bytes in the payload that have been transmitted before
 *      this method call.
 * \return
 *      # bytes in the payload that have been transmitted after
 *      this method returns.
 *
 * \throw TransportException
 *      An I/O error occurred.
 */
uint32_t
TcpTransport::sendMessage(int fd, uint64_t rpcId, Buffer* payload,
        uint32_t bytesSent)
{
    assert(fd >= 0);
    Header header = {rpcId, payload->size()};

    // Use an iovec to send everything in one kernel call: one iov
    // for header, the rest for payload.  Skip parts that have
    // already been sent.
    uint32_t iovecs = 1 + payload->getNumberChunks();
    struct iovec iov[iovecs];
    uint32_t offset;
    size_t iovecIndex;
    if (bytesSent < sizeof(Header)) {
        iov[0].iov_base = reinterpret_cast<char*>(&header) + bytesSent;
        iov[0].iov_len = sizeof(Header) - bytesSent;
        iovecIndex = 1;
        offset = 0;
    } else {
        iovecIndex = 0;
        offset = bytesSent - sizeof32(Header);
    }
    Buffer::Iterator iter(payload, offset, header.len - offset);
    while (!iter.isDone()) {
        iov[iovecIndex].iov_base = const_cast<void*>(iter.getData());
        iov[iovecIndex].iov_len = iter.getLength();
        ++iovecIndex;

        // There's an upper limit on the permissible number of iovecs in
        // one outgoing message. Unfortunately, this limit does not appear
        // to be defined publicly, so we make a guess here. If we hit the
        // limit, stop accumulating chunks for this message: the remaining
        // chunks will get tried in a future invocation of this method.
        if (iovecIndex >= 100) {
            break;
        }
        iter.next();
    }

    struct msghdr msg;
    memset(&msg, 0, sizeof(msg));
    msg.msg_iov = iov;
    msg.msg_iovlen = iovecIndex;

    int r = static_cast<int>(sys->sendmsg(fd, &msg,
            MSG_NOSIGNAL|MSG_DONTWAIT));
    if (r < 0) {
        if ((errno != EAGAIN) && (errno != EWOULDBLOCK)) {
            LOG(WARNING, "TcpTransport sendmsg error: %s", strerror(errno));
//            DIE("Why broken pipe?");
            throw TransportException(HERE, "TcpTransport sendmsg error",
                    errno);
        }
    } else {
        bytesSent += downCast<uint32_t>(r);
        PerfStats::threadStats.networkOutputBytes += r;
#if TESTING
        uint32_t totalLength = sizeof32(header) + header.len;
        if ((r > 0) && (r < totalLength)) {
            messageChunks++;
        }
#endif
    }
    return bytesSent;
}

/**
 * Read bytes from a socket and generate exceptions for errors and
 * end-of-file.
 *
 * \param fd
 *      File descriptor for socket.
 * \param buffer
 *      Store incoming data here.
 * \param length
 *      Maximum number of bytes to read.
 * \return
 *      The number of bytes read.  The recv is done in non-blocking mode;
 *      if there are no bytes available then 0 is returned (0 does *not*
 *      mean end-of-file).
 *
 * \throw TransportException
 *      An I/O error occurred.
 */

ssize_t
TcpTransport::recvCarefully(int fd, void* buffer, size_t length) {
    ssize_t actual = sys->recv(fd, buffer, length, MSG_DONTWAIT);
    if (actual > 0) {
        PerfStats::threadStats.networkInputBytes += actual;
        return actual;
    }
    if (actual == 0) {
        throw TransportException(HERE, "session closed by peer");
    }
    if ((errno == EAGAIN) || (errno == EWOULDBLOCK)) {
        return 0;
    }
    LOG(WARNING, "TcpTransport recv error: %s", strerror(errno));
    throw TransportException(HERE, "TcpTransport recv error", errno);
}

/**
 * Constructor for IncomingMessages that are requests.
 * \param buffer
 *      Buffer in which to place the body of the incoming message;
 *      the caller should ensure that the buffer is empty.
 */
TcpTransport::IncomingMessage::IncomingMessage(Buffer* buffer)
    : header(), headerBytesReceived(0), messageBytesReceived(0),
      messageLength(0), buffer(buffer), session(NULL)
{
}

/**
 * Constructor for IncomingMessages that are responses.
 * \param session
 *      The session through which the RPC request was sent. Used to
 *      find the ClientRpc the response belongs to.
 */
TcpTransport::IncomingMessage::IncomingMessage(TcpSession* session)
    : header(), headerBytesReceived(0), messageBytesReceived(0),
      messageLength(0), buffer(NULL), session(session)
{
}

/**
 * Attempt to read part or all of a message from an open socket.
 *
 * \param fd
 *      File descriptor to use for reading message info.
 * \return
 *      True means the message is complete (it's present in the
 *      buffer provided to the constructor); false means we still need
 *      more data.
 *
 * \throw TransportException
 *      An I/O error occurred.
 */
bool
TcpTransport::IncomingMessage::tryToReadMessage(int fd) {
    // First make sure we have received the header (it may arrive in
    // multiple chunks).
    if (headerBytesReceived < sizeof(Header)) {
        ssize_t len = TcpTransport::recvCarefully(fd,
                reinterpret_cast<char*>(&header) + headerBytesReceived,
                sizeof(header) - headerBytesReceived);
        headerBytesReceived += downCast<uint32_t>(len);
        if (headerBytesReceived < sizeof(Header))
            return false;

        // Header is complete; check for various errors and set up for
        // reading the body.
        messageLength = header.len;
        if (header.len > MAX_RPC_LEN) {
            LOG(WARNING, "TcpTransport received oversize message (%d bytes); "
                    "discarding extra bytes", header.len);
            messageLength = MAX_RPC_LEN;
        }

        if ((buffer == NULL) && (session != NULL)) {
            ClientRpcMap::iterator it =
                    session->outgoingRpcs.find(header.rpcId);
            if (it != session->outgoingRpcs.end()) {
                session->currentRpc = it->second;
                buffer = it->second->response;
            }
        }
        if (buffer == NULL)
            messageLength = 0;
    }

    // We have the header; now receive the message body (it may take several
    // calls to this method before we get all of it).
    if (messageBytesReceived < messageLength) {
        void *dest;
        if (buffer->size() == 0) {
            dest = buffer->alloc(messageLength);
        } else {
            buffer->peek(messageBytesReceived, &dest);
        }
        ssize_t len = TcpTransport::recvCarefully(fd, dest,
                messageLength - messageBytesReceived);
        messageBytesReceived += downCast<uint32_t>(len);
        if (messageBytesReceived < messageLength)
            return false;
    }

    // We have the header and the message body, but we may have to discard
    // extraneous bytes.
    if (messageBytesReceived < header.len) {
        char buffer[4096];
        uint32_t maxLength = header.len - messageBytesReceived;
        if (maxLength > sizeof(buffer))
            maxLength = sizeof(buffer);
        ssize_t len = TcpTransport::recvCarefully(fd, buffer, maxLength);
        messageBytesReceived += downCast<uint32_t>(len);
        if (messageBytesReceived < header.len)
            return false;
    }
    return true;
}

/**
 * Construct a TcpSession object for communication with a given server.
 *
 * \param transport
 *      The transport with which this session is associated.
 * \param serviceLocator
 *      Identifies the server to which RPCs on this session will be sent.
 * \param timeoutMs
 *      If there is an active RPC and we can't get any signs of life out
 *      of the server within this many milliseconds then the session will
 *      be aborted.  0 means we get to pick a reasonable default.
 *
 * \throw TransportException
 *      There was a problem that prevented us from creating the session.
 */
TcpTransport::TcpSession::TcpSession(TcpTransport* transport,
        const ServiceLocator* serviceLocator,
        uint32_t timeoutMs)
    : Session(serviceLocator->getOriginalString())
    , transport(transport)
    , address(serviceLocator)
    , fd(-1), nextRpcId(1)
    , outgoingRequests()
//    , bytesLeftToSend(0)
    , outgoingRpcs()
    , currentResponse()
    , currentRpc(NULL)
    , clientIoHandler()
//    , alarm(transport->context->sessionAlarmTimer, this,
//            (timeoutMs != 0) ? timeoutMs : DEFAULT_TIMEOUT_MS)
{
    fd = sys->socket(PF_INET, SOCK_STREAM, 0);
    if (fd == -1) {
        LOG(WARNING, "TcpTransport couldn't open socket for session: %s",
            strerror(errno));
        throw TransportException(HERE,
                "TcpTransport couldn't open socket for session", errno);
    }

    int r = sys->connect(fd, &address.address, sizeof(address.address));
    if (r == -1) {
        sys->close(fd);
        fd = -1;
        LOG(WARNING, "TcpTransport couldn't connect to %s: %s",
            this->serviceLocator.c_str(), strerror(errno));
        throw TransportException(HERE, format(
                "TcpTransport couldn't connect to %s",
                this->serviceLocator.c_str()), errno);
    }

    // Check to see if we accidentally connected to ourself. This can
    // happen if the target server is on the same machine and has
    // crashed, so that it is no longer using its port. If this
    // happens our local socket (fd) might end up reusing that same port,
    // in which case we will connect to ourselves. If this happens,
    // abort this connection (it will get retried, at which point a
    // different port will get selected).
    struct sockaddr cAddr;
    socklen_t cAddrLen = sizeof(cAddr);
    // Read address information associated with our local socket.
    if (sys->getsockname(fd, &cAddr, &cAddrLen)) {
        sys->close(fd);
        fd = -1;
        LOG(WARNING, "TcpTransport failed to get client socket info");
        throw TransportException(HERE,
                "TcpTransport failed to get client socket info", errno);
    }
    IpAddress sourceIp(&cAddr);
    IpAddress destinationIp(serviceLocator);
    if (sourceIp.toString().compare(destinationIp.toString()) == 0) {
        sys->close(fd);
        fd = -1;
        LOG(WARNING, "TcpTransport connected to itself %s",
                sourceIp.toString().c_str());
        throw TransportException(HERE, format(
                "TcpTransport connected to itself %s",
                sourceIp.toString().c_str()));
    }

    // Disable the hideous Nagle algorithm, which will delay sending small
    // messages in some situations.
    int flag = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

    /// Arrange for notification whenever the server sends us data.
    Dispatch::Lock lock(transport->context->dispatch);
    clientIoHandler.construct(fd, this);
    currentResponse.construct(this);
}

/**
 * Destructor for TcpSession objects.
 */
TcpTransport::TcpSession::~TcpSession()
{
    close();
}

// See documentation for Transport::Session::abort.
void
TcpTransport::TcpSession::abort()
{
    close();
}

// See Transport::Session::cancelRequest for documentation.
void
TcpTransport::TcpSession::cancelRequest(RpcNotifier* notifier)
{
    // Search for an RPC that refers to this notifier; if one is
    // found then remove all state relating to it.
    for (ClientRpcMap::iterator it = outgoingRpcs.begin();
            it != outgoingRpcs.end(); it++) {
        ClientRpc* rpc = it->second;
        if (rpc->notifier == notifier) {
            transport->clientRpcPool.destroy(rpc);
//            alarm.rpcFinished();

            // If we have started reading the response message,
            // cancel that also.
            if (rpc == currentRpc) {
                // Setting messageLength to 0 so that unread bytes in the
                // socket will be properly discarded in #readMessage.
                currentResponse->buffer = NULL;
                currentResponse->messageLength = 0;
                currentRpc = NULL;
            }
            outgoingRpcs.erase(it);
            if (rpc->transmitPending) {
                erase(outgoingRequests, *rpc);
            }
            return;
        }
    }
}

/**
 * Close the socket associated with a session.
 */
void
TcpTransport::TcpSession::close()
{
//    LOG(ERROR, "CLOSE CLIENT SOCKET");
    if (fd >= 0) {
        sys->close(fd);
        fd = -1;
    }
    for (ClientRpcMap::iterator it = outgoingRpcs.begin();
            it != outgoingRpcs.end(); it++) {
        ClientRpc *rpc = it->second;
        rpc->notifier->failed();
        transport->clientRpcPool.destroy(rpc);
    }
    outgoingRpcs.clear();
    outgoingRequests.clear();
    if (clientIoHandler) {
        Dispatch::Lock lock(transport->context->dispatch);
        clientIoHandler.destroy();
    }
}

// See Transport::Session::getRpcInfo for documentation.
string
TcpTransport::TcpSession::getRpcInfo()
{
    const char* separator = "";
    string result;
    for (ClientRpcMap::iterator it = outgoingRpcs.begin();
            it != outgoingRpcs.end(); it++) {
        ClientRpc *rpc = it->second;
        result += separator;
        result += WireFormat::opcodeSymbol(rpc->request);
        separator = ", ";
    }
    if (result.empty())
        result = "no active RPCs";
    result += " to server at ";
    result += serviceLocator;
    return result;
}

// See Transport::Session::sendRequest for documentation.
void
TcpTransport::TcpSession::sendRequest(Buffer* request, Buffer* response,
        RpcNotifier* notifier)
{
    response->reset();
    if (fd == -1) {
        notifier->failed();
        return;
    }
//    alarm.rpcStarted();
    ClientRpc* rpc = transport->clientRpcPool.construct(request, response,
            notifier, nextRpcId);
    nextRpcId++;
    outgoingRpcs[rpc->rpcId] = rpc;
    if (!outgoingRequests.empty()) {
        // Can't transmit this request yet; there are already other
        // requests that haven't yet been sent.
        outgoingRequests.push_back(*rpc);
        return;
    }

    // Try to transmit the request.
    try {
        rpc->bytesSent = TcpTransport::sendMessage(fd, rpc->rpcId,
                request, 0);
    } catch (TransportException& e) {
        abort();
        notifier->failed();
        return;
    }
    if (rpc->bytesSent >= request->size() + sizeof32(Header)) {
        // The whole request was sent immediately (this should be the
        // common case).
        rpc->transmitPending = false;
    } else {
        outgoingRequests.push_back(*rpc);
        clientIoHandler->setEvents(Dispatch::FileEvent::READABLE |
                Dispatch::FileEvent::WRITABLE);
    }
}

// See Transport::ServerRpc::sendReply for documentation.
void
TcpTransport::TcpServerRpc::sendReply()
{
    Socket* socket = transport->serverSockets[fd].get();

    // It's possible that our fd has been closed (or even reused for a
    // new connection); if so, just discard the RPC without sending
    // a response.
    if ((socket != NULL) && (socket->id == socketId)) {
        if (!socket->outgoingResponses.empty()) {
            // Can't transmit the response yet; the socket is backed up.
            socket->outgoingResponses.push_back(*this);
            return;
        }

        // Try to transmit the response.
        bytesSent = TcpTransport::sendMessage(fd, request.header.rpcId,
                &replyPayload, 0);
        if (bytesSent < replyPayload.size() + sizeof32(Header)) {
            socket->outgoingResponses.push_back(*this);
            socket->ioHandler.setEvents(Dispatch::FileEvent::READABLE |
                    Dispatch::FileEvent::WRITABLE);
            return;
        }
    }
//    } catch (TransportException& e) {
//        transport->closeSocket(fd);
//        // FIXME: the idea is to let the ServerSocketHandler::handleFileEvent to close the socket
//        // But perhaps a even better idea is to also let it destroy this ServerRpc
//        // so we don't even need a try-catch block in this method.
//        transport->serverRpcPool.destroy(this);
//        throw e;
//    }

    // The whole response was sent immediately (this should be the
    // common case) or our fd was closed.  Recycle the RPC object.
    transport->serverRpcPool.destroy(this);
}

// See Transport::ServerRpc::getclientServiceLocator for documentation.
string
TcpTransport::TcpServerRpc::getClientServiceLocator()
{
    Socket* socket = transport->serverSockets[fd].get();
    return format("tcp:host=%s,port=%hu", inet_ntoa(socket->sin.sin_addr),
        NTOHS(socket->sin.sin_port));
}

}  // namespace RAMCloud
