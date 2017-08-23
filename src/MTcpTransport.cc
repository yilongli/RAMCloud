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

#include <arpa/inet.h>
#include <ifaddrs.h>
#include <linux/if_packet.h>
#include <rte_ethdev.h>

#include "Common.h"
#include "PerfStats.h"
#include "ShortMacros.h"
#include "MTcpTransport.h"
#include "OptionParser.h"
#include "WorkerManager.h"
#include "MacAddress.h"

namespace RAMCloud {

int MTcpTransport::messageChunks = 0;

/**
 * Default object used to make system calls.
 */
//static Syscall defaultSyscall;

/**
 * Used by this class to make all system calls.  In normal production
 * use it points to defaultSyscall; for testing it points to a mock
 * object.
 */
//Syscall* MTcpTransport::sys = &defaultSyscall;

/**
 * Construct a MTcpTransport instance.
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
MTcpTransport::MTcpTransport(Context* context,
        const ServiceLocator* serviceLocator)
    : context(context)
    , locatorString()
    , listenSocket(-1)
    , mctx()
    , epollId(-1)
    , epollEvents()
//    , acceptHandler()
    , sockets()
    , nextSocketId(100)
    , poller(context, this)
    , serverRpcPool()
    , clientRpcPool()
{
    // FIXME: how to avoid hardcode the config file path?
    if (mtcp_init("/shome/RAMCloud/config/mtcp.conf") == -1) {
        LOG(WARNING, "MTcpTransport couldn't initialize", strerror(errno));
        throw TransportException(HERE, "MTcpTransport couldn't initialize",
                errno);
    }

    // FIXME: which cpu should I use? the same one as dispatch thread? or different one?
    // FIXME: there is a check "0 <= mctx->cpu < num_cpus" in api.c:GetMTCPManager that
    // I do not understand; so right now just use cpu 0 for best chance...
    mctx = mtcp_create_context(0);
    if (NULL == mctx) {
        throw TransportException(HERE,
                "MTcpTransport couldn't create mTCP context");
    }

    epollId = mtcp_epoll_create(mctx, arrayLength(epollEvents));
    if (epollId < 0) {
        throw TransportException(HERE,
                "MTcpTransport couldn't create epoll instance");
    }

    if (serviceLocator == NULL) {
        // The rest of the code is just creating the listener socket.
        // No need to continue for client-side transport.
        return;
    }

    // Get the host IP address based on the DPDK port id of the Ethernet
    // device. The IP address is then combined with the port info to build
    // the complete service locator.
    int portId = context->options->getDpdkPort();
    int numPorts = rte_eth_dev_count();
    if (numPorts <= portId) {
        throw TransportException(HERE, format(
                "Ethernet port %u doesn't exist (%u ports available)",
                portId, numPorts));
    }

    struct ether_addr mac;
    rte_eth_macaddr_get(downCast<uint8_t>(portId), &mac);

    struct ifaddrs* ifaddr;
    if (getifaddrs(&ifaddr) == -1) {
        LOG(WARNING, "getifaddrs failed", strerror(errno));
        throw TransportException(HERE, "getifaddrs failed");
    }

    char* ifa_name = NULL;
    struct ifaddrs* ifa;
    for (ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next) {
        struct sockaddr* sockaddr = ifa->ifa_addr;
        if ((sockaddr != NULL) && (sockaddr->sa_family == AF_PACKET)) {
            struct sockaddr_ll* s = (struct sockaddr_ll*)sockaddr;
            if (std::equal(std::begin(s->sll_addr), std::end(s->sll_addr),
                    std::begin(mac.addr_bytes))) {
                ifa_name = ifa->ifa_name;
                break;
            }
        }
    }
    if (NULL == ifa_name) {
        throw TransportException(HERE,
                format("No interface found with matching MAC address: %s",
                MacAddress(mac.addr_bytes).toString().c_str()));
    }
    for (ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next) {
        struct sockaddr* sockaddr = ifa->ifa_addr;
        if ((sockaddr != NULL) && (sockaddr->sa_family == AF_INET)) {
            if (strcmp(ifa_name, ifa->ifa_name) == 0) {
                break;
            }
        }
    }

    reinterpret_cast<sockaddr_in*>(ifa->ifa_addr)->sin_port =
            NTOHS(serviceLocator->getOption<uint16_t>("port"));
    IpAddress address(ifa->ifa_addr);
    freeifaddrs(ifaddr);

    string tcpAddr = address.toString();
    size_t pos = tcpAddr.find_first_of(':');
    locatorString =
            "mtcp:host="+tcpAddr.substr(0,pos)+",port="+tcpAddr.substr(pos+1);
    LOG(NOTICE, "mTCP service locator: %s", locatorString.c_str());

    // TODO: The file descriptor returned by mtcp lives in a different universe
    // than the OS descriptors right? So we can't reuse the Dispatch::File class right?
    // We need to write our own poller using mTcp epoll?
    listenSocket = mtcp_socket(mctx, PF_INET, SOCK_STREAM, 0);
    if (listenSocket == -1) {
        LOG(WARNING, "MTcpTransport couldn't create listen socket: %s",
                strerror(errno));
        throw TransportException(HERE,
                "MTcpTransport couldn't create listen socket", errno);
    }

    int r = mtcp_setsock_nonblock(mctx, listenSocket);
    if (r != 0) {
        mtcp_close(mctx, listenSocket);
        LOG(WARNING, "MTcpTransport couldn't set nonblocking on listen "
                "socket: %s", strerror(errno));
        throw TransportException(HERE,
                "MTcpTransport couldn't set nonblocking on listen socket",
                errno);
    }

    if (mtcp_bind(mctx, listenSocket, &address.address,
            sizeof(address.address)) == -1) {
        mtcp_close(mctx, listenSocket);
        string message = format("MTcpTransport couldn't bind to '%s'",
                serviceLocator->getOriginalString().c_str());
        LOG(WARNING, "%s: %s", message.c_str(), strerror(errno));
        throw TransportException(HERE, message, errno);
    }

#define MAX_BACKLOG 10000
    if (mtcp_listen(mctx, listenSocket, MAX_BACKLOG) == -1) {
        mtcp_close(mctx, listenSocket);
        LOG(WARNING, "MTcpTransport couldn't listen on socket: %s",
                strerror(errno));
        throw TransportException(HERE,
                "MTcpTransport couldn't listen on socket", errno);
    }

    // Arrange to be notified whenever anyone connects to listenSocket.
    struct mtcp_epoll_event event;
    event.data.sockid = listenSocket;
    // FIXME: Level vs. edge? What about oneshot? Why does Dispatch:556 use oneshot?
    event.events = MTCP_EPOLLIN;
//    event.events = MTCP_EPOLLIN | MTCP_EPOLLONESHOT;
    if (mtcp_epoll_ctl(mctx, epollId, MTCP_EPOLL_CTL_ADD, listenSocket,
            &event) == -1) {
        throw TransportException(HERE,
                "MTcpTransport couldn't set epoll event for listenSocket", errno);
    }
//    acceptHandler.construct(listenSocket, this);
}

/**
 * Destructor for MTcpTransports: close file descriptors and perform
 * any other needed cleanup.
 */
MTcpTransport::~MTcpTransport()
{
    if (listenSocket >= 0) {
        mtcp_close(mctx, listenSocket);
        listenSocket = -1;
    }
    for (unsigned int i = 0; i < sockets.size(); i++) {
        if (sockets[i]) {
            closeSocket(i);
        }
    }
    mtcp_destroy_context(mctx);
    mtcp_destroy();
}

/**
 * This private method is invoked to close the server's end of a
 * connection to a client and cleanup any related state.
 * \param fd
 *      File descriptor for the socket to be closed.
 */
void
MTcpTransport::closeSocket(int fd) {
    sockets[fd].destroy();
    mtcp_close(mctx, fd);
}


/**
 * This method is invoked in the inner polling loop of the dispatcher;
 * it drives the operation of the transport.
 * \return
 *      The return value is 1 if this method found something useful to do,
 *      0 otherwise.
 */
int
MTcpTransport::Poller::poll()
{
    int result = 0;

    while (true) {
        int n = mtcp_epoll_wait(t->mctx, t->epollId, t->epollEvents,
                arrayLength(t->epollEvents), 0);
        for (int i = 0; i < n; i++) {
            int sockid = t->epollEvents[i].data.sockid;
            if (sockid == t->listenSocket) {
                struct sockaddr_in sin;
                socklen_t socklen = sizeof(sin);

                int acceptedFd = mtcp_accept(t->mctx, t->listenSocket,
                        reinterpret_cast<sockaddr*>(&sin), &socklen);
                if (acceptedFd < 0) {
                    switch (errno) {
                        // According to the man page for accept, you're
                        // supposed to treat these as retry on Linux.
                        case EHOSTDOWN:
                        case EHOSTUNREACH:
                        case ENETDOWN:
                        case ENETUNREACH:
                        case ENONET:
                        case ENOPROTOOPT:
                        case EOPNOTSUPP:
                        case EPROTO:
                            continue;

                        // No incoming connections are currently available.
                        case EAGAIN:
#if EAGAIN != EWOULDBLOCK
                        case EWOULDBLOCK:
#endif
                        default:
                            continue;
                    }

                    // Unexpected error: log a message and then close the socket
                    // (so we don't get repeated errors).
                    LOG(ERROR, "error in MTcpTransport::AcceptHandler accepting "
                            "connection for '%s': %s",
                            t->locatorString.c_str(), strerror(errno));
                    // FIXME: HOW TO IMPLEMENT setEvents(0)?
//                    setEvents(0);

                    mtcp_close(t->mctx, t->listenSocket);
                    t->listenSocket = -1;
                    continue;
                }

                // At this point we have successfully opened a client connection.
                // Save information about it and create a handler for incoming
                // requests.
                // FIXME: Does mTCP generates socket id like kernel tcp? How do
                // we know sock fd starts from 0 (so that we can use array as
                // opposed to map to store Sockets? Also eliminate the `new` eventually.
                if (t->sockets.size() <=
                        static_cast<unsigned int>(acceptedFd)) {
                    t->sockets.resize(acceptedFd + 1);
                }
                t->sockets[acceptedFd].construct(acceptedFd, t, sin);
            } else if (t->epollEvents[i].events == MTCP_EPOLLIN) {

            } else if (t->epollEvents[i].events == MTCP_EPOLLOUT) {

            }

        }
    }

    return result;
}

/**
 * Constructor for Sockets.
 */
MTcpTransport::Socket::Socket(int fd, MTcpTransport* transport, sockaddr_in& sin)
    : transport(transport)
    , id(transport->nextSocketId)
    , rpc(NULL)
    , ioHandler(fd, transport, this)
    , rpcsWaitingToReply()
    , bytesLeftToSend(0)
    , sin(sin)
{
    transport->nextSocketId++;
}

/**
 * Destructor for Sockets.
 */
MTcpTransport::Socket::~Socket() {
    if (rpc != NULL) {
        transport->serverRpcPool.destroy(rpc);
    }
    while (!rpcsWaitingToReply.empty()) {
        MTcpServerRpc& rpc = rpcsWaitingToReply.front();
        rpcsWaitingToReply.pop_front();
        transport->serverRpcPool.destroy(&rpc);
    }
}

/**
 * Constructor for ServerSocketHandlers.
 *
 * \param fd
 *      File descriptor for a client socket on which RPC requests may arrive.
 * \param transport
 *      The MTcpTransport that manages this socket.
 * \param socket
 *      Socket object corresponding to fd.
 */
MTcpTransport::ServerSocketHandler::ServerSocketHandler(int fd,
                                                       MTcpTransport* transport,
                                                       Socket* socket)
    : Dispatch::File(transport->context->dispatch, fd,
                     Dispatch::FileEvent::READABLE)
    , fd(fd)
    , transport(transport)
    , socket(socket)
{
    // Empty constructor body.
}

/**
 * This method is invoked by Dispatch when a server's connection from a client
 * becomes readable or writable.  It attempts to read incoming messages from
 * the socket.  If a full message is available, a MTcpServerRpc object gets
 * queued for service.  It also attempts to write responses to the socket
 * (if there are responses waiting for transmission).
 *
 * \param events
 *      Indicates whether the socket was readable, writable, or both
 *      (OR-ed combination of Dispatch::FileEvent bits).
 */
void
MTcpTransport::ServerSocketHandler::handleFileEvent(int events)
{
    // The following variables are copies of data from this object;
    // they are needed to safely detect socket closure below.
    MTcpTransport* transport = this->transport;
    int socketFd = fd;
    Socket* socket = transport->sockets[socketFd].get();
    assert(socket != NULL);
    try {
        if (events & Dispatch::FileEvent::READABLE) {
            if (socket->rpc == NULL) {
                socket->rpc = transport->serverRpcPool.construct(socket,
                                                                 fd, transport);
            }
            if (socket->rpc->message.readMessage(fd)) {
                // The incoming request is complete; pass it off for servicing.
                MTcpServerRpc *rpc = socket->rpc;
                socket->rpc = NULL;
                transport->context->workerManager->handleRpc(rpc);
            }
        }
        // Check to see if this socket got closed due to an error in the
        // read handler; if so, it's neither necessary nor safe to continue
        // in this method. Note: the check for closure must be done without
        // accessing any fields in this object, since the object may have been
        // destroyed and its memory recycled.
        if (socket != transport->sockets[socketFd].get()) {
            return;
        }
        if (events & Dispatch::FileEvent::WRITABLE) {
            while (true) {
                if (socket->rpcsWaitingToReply.empty()) {
                    setEvents(Dispatch::FileEvent::READABLE);
                    break;
                }
                MTcpServerRpc& rpc = socket->rpcsWaitingToReply.front();
                socket->bytesLeftToSend = transport->sendMessage(fd,
                        rpc.message.header.nonce, &rpc.replyPayload,
                        socket->bytesLeftToSend);
                if (socket->bytesLeftToSend != 0) {
                    break;
                }
                // The current reply is finished; start the next one, if
                // there is one.
                socket->rpcsWaitingToReply.pop_front();
                transport->serverRpcPool.destroy(&rpc);
                socket->bytesLeftToSend = -1;
            }
        }
    } catch (TransportException& e) {
        transport->closeSocket(fd);
    }
}

/**
 * Transmit an RPC request or response on a socket.  This method uses
 * a nonblocking approach: if the entire message cannot be transmitted,
 * it transmits as many bytes as possible and returns information about
 * how much more work is still left to do.
 *
 * \param fd
 *      File descriptor to write.
 * \param nonce
 *      Unique identifier for the RPC; must never have been used
 *      before on this socket.
 * \param payload
 *      Message to transmit on fd; this method adds on a header.
 * \param bytesToSend
 *      -1 means the entire message must still be transmitted;
 *      Anything else means that part of the message was transmitted
 *      in a previous call, and the value of this parameter is the
 *      result returned by that call (always greater than 0).
 *
 * \return
 *      The number of (trailing) bytes that could not be transmitted.
 *      0 means the entire message was sent successfully.
 *
 * \throw TransportException
 *      An I/O error occurred.
 */
int
MTcpTransport::sendMessage(int fd, uint64_t nonce, Buffer* payload,
        int bytesToSend)
{
    assert(fd >= 0);

    Header header;
    header.nonce = nonce;
    header.len = payload->size();
    int totalLength = downCast<int>(sizeof(header) + header.len);
    if (bytesToSend < 0) {
        bytesToSend = totalLength;
    }
    int alreadySent = totalLength - bytesToSend;

    // Use an iovec to send everything in one kernel call: one iov
    // for header, the rest for payload.  Skip parts that have
    // already been sent.
    struct iovec iov[payload->getNumberChunks()+1];
    int offset;
    int iovecIndex;
    if (alreadySent < downCast<int>(sizeof(header))) {
        iov[0].iov_base = reinterpret_cast<char*>(&header) + alreadySent;
        iov[0].iov_len = sizeof(header) - alreadySent;
        iovecIndex = 1;
        offset = 0;
    } else {
        iovecIndex = 0;
        offset = alreadySent - downCast<int>(sizeof(header));
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

//    struct msghdr msg;
//    memset(&msg, 0, sizeof(msg));
//    msg.msg_iov = iov;
//    msg.msg_iovlen = iovecIndex;
//
//    int r = downCast<int>(sys->sendmsg(fd, &msg,
//            MSG_NOSIGNAL|MSG_DONTWAIT));
    int r = mtcp_writev(mctx, fd, iov, iovecIndex);
    if (r == bytesToSend) {
        PerfStats::threadStats.networkOutputBytes += r;
        return 0;
    }
#if TESTING
    if ((r > 0) && (r < totalLength)) {
        messageChunks++;
    }
#endif
    if (r == -1) {
        if ((errno != EAGAIN) && (errno != EWOULDBLOCK)) {
            LOG(WARNING, "MTcpTransport sendmsg error: %s", strerror(errno));
            throw TransportException(HERE, "MTcpTransport sendmsg error",
                    errno);
        }
        r = 0;
    }
    PerfStats::threadStats.networkOutputBytes += r;
    return bytesToSend - r;
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
MTcpTransport::recvCarefully(int fd, char* buffer, size_t length,
        IncomingMessage* message) {
    ssize_t actual = mtcp_recv(mctx, fd, buffer, length, MSG_DONTWAIT);
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
    if (message->headerBytesReceived < sizeof(Header)) {
        LOG(WARNING, "MTcpTransport recv error: %s, received header bytes %u",
                strerror(errno), message->headerBytesReceived);
    } else {
        LOG(WARNING, "MTcpTransport recv error: %s, message id %lu, length %u, "
                "received %u", strerror(errno), message->header.nonce,
                message->header.len, message->messageBytesReceived);
    }
    throw TransportException(HERE, "MTcpTransport recv error", errno);
}

/**
 * Constructor for IncomingMessages.
 * \param buffer
 *      If non-NULL, specifies a buffer in which to place the body of
 *      the incoming message; the caller should ensure that the buffer
 *      is empty.  This parameter is used on servers, where the buffer
 *      is known before any part of the message has been received.
 * \param session
 *      If non-NULL, specifies a MTcpSession whose findRpc method should
 *      be invoked once the header for the message has been received.
 *      FindRpc will provide a buffer to use for the body of the message.
 *      This argument is typically used on clients.
 */
MTcpTransport::IncomingMessage::IncomingMessage(Buffer* buffer,
        MTcpSession* session)
    : header(), headerBytesReceived(0), messageBytesReceived(0),
      messageLength(0), buffer(buffer), session(session)
{
}

/**
 * This method is invoked to cancel the receipt of a message in progress.
 * Once this method returns, we will still finish reading the message
 * (don't want to leave unread bytes in the socket), but the contents
 * will be discarded.
 */
void
MTcpTransport::IncomingMessage::cancel() {
    buffer = NULL;
    messageLength = 0;
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
MTcpTransport::IncomingMessage::readMessage(int fd) {
    // First make sure we have received the header (it may arrive in
    // multiple chunks).
    if (headerBytesReceived < sizeof(Header)) {
        ssize_t len = session->transport->recvCarefully(fd,
                reinterpret_cast<char*>(&header) + headerBytesReceived,
                sizeof(header) - headerBytesReceived, this);
        headerBytesReceived += downCast<uint32_t>(len);
        if (headerBytesReceived < sizeof(Header))
            return false;

        // Header is complete; check for various errors and set up for
        // reading the body.
        messageLength = header.len;
        if (header.len > MAX_RPC_LEN) {
            LOG(WARNING, "MTcpTransport received oversize message (%d bytes); "
                    "discarding extra bytes", header.len);
            messageLength = MAX_RPC_LEN;
        }

        if ((buffer == NULL) && (session != NULL)) {
            buffer = session->findRpc(&header);
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
        ssize_t len = session->transport->recvCarefully(fd,
                static_cast<char*>(dest),
                messageLength - messageBytesReceived, this);
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
        ssize_t len = session->transport->recvCarefully(fd, buffer, maxLength,
                this);
        messageBytesReceived += downCast<uint32_t>(len);
        if (messageBytesReceived < header.len)
            return false;
    }
    return true;
}

/**
 * Construct a MTcpSession object for communication with a given server.
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
MTcpTransport::MTcpSession::MTcpSession(MTcpTransport* transport,
        const ServiceLocator* serviceLocator,
        uint32_t timeoutMs)
    : Session(serviceLocator->getOriginalString())
    , transport(transport)
    , address(serviceLocator)
    , fd(-1), serial(1)
    , rpcsWaitingToSend()
    , bytesLeftToSend(0)
    , rpcsWaitingForResponse()
    , current(NULL)
    , message()
    , clientIoHandler()
    , alarm(transport->context->sessionAlarmTimer, this,
            (timeoutMs != 0) ? timeoutMs : DEFAULT_TIMEOUT_MS)
{
    fd = mtcp_socket(transport->mctx, PF_INET, SOCK_STREAM, 0);
    if (fd == -1) {
        LOG(WARNING, "MTcpTransport couldn't open socket for session: %s",
            strerror(errno));
        throw TransportException(HERE,
                "MTcpTransport couldn't open socket for session", errno);
    }

    int r = mtcp_connect(transport->mctx, fd, &address.address,
            sizeof(address.address));
    if (r == -1) {
        mtcp_close(transport->mctx, fd);
        fd = -1;
        LOG(WARNING, "MTcpTransport couldn't connect to %s: %s",
            this->serviceLocator.c_str(), strerror(errno));
        throw TransportException(HERE, format(
                "MTcpTransport couldn't connect to %s",
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
    if (mtcp_getsockname(transport->mctx, fd, &cAddr, &cAddrLen)) {
        mtcp_close(transport->mctx, fd);
        fd = -1;
        LOG(WARNING, "MTcpTransport failed to get client socket info");
        throw TransportException(HERE,
                "MTcpTransport failed to get client socket info", errno);
    }
    IpAddress sourceIp(&cAddr);
    IpAddress destinationIp(serviceLocator);
    if (sourceIp.toString().compare(destinationIp.toString()) == 0) {
        mtcp_close(transport->mctx, fd);
        fd = -1;
        LOG(WARNING, "MTcpTransport connected to itself %s",
                sourceIp.toString().c_str());
        throw TransportException(HERE, format(
                "MTcpTransport connected to itself %s",
                sourceIp.toString().c_str()));
    }

    // Disable the hideous Nagle algorithm, which will delay sending small
    // messages in some situations.
//    int flag = 1;
//    sys->setsockopt(fd, IPPROTO_MTCP, MTCP_NODELAY, &flag, sizeof(flag));

    /// Arrange for notification whenever the server sends us data.
    Dispatch::Lock lock(transport->context->dispatch);
    clientIoHandler.construct(fd, this);
    message.construct(static_cast<Buffer*>(NULL), this);
}

/**
 * Destructor for MTcpSession objects.
 */
MTcpTransport::MTcpSession::~MTcpSession()
{
    close();
}

// See documentation for Transport::Session::abort.
void
MTcpTransport::MTcpSession::abort()
{
    close();
}

// See Transport::Session::cancelRequest for documentation.
void
MTcpTransport::MTcpSession::cancelRequest(RpcNotifier* notifier)
{
    // Search for an RPC that refers to this notifier; if one is
    // found then remove all state relating to it.
    foreach (MTcpClientRpc& rpc, rpcsWaitingForResponse) {
        if (rpc.notifier == notifier) {
            rpcsWaitingForResponse.erase(
                    rpcsWaitingForResponse.iterator_to(rpc));
            transport->clientRpcPool.destroy(&rpc);
            alarm.rpcFinished();

            // If we have started reading the response message,
            // cancel that also.
            if (&rpc == current) {
                message->cancel();
                current = NULL;
            }
            return;
        }
    }
    foreach (MTcpClientRpc& rpc, rpcsWaitingToSend) {
        if (rpc.notifier == notifier) {
            rpcsWaitingToSend.erase(
                    rpcsWaitingToSend.iterator_to(rpc));
            transport->clientRpcPool.destroy(&rpc);
            return;
        }
    }
}

/**
 * Close the socket associated with a session.
 */
void
MTcpTransport::MTcpSession::close()
{
    if (fd >= 0) {
        mtcp_close(transport->mctx, fd);
        fd = -1;
    }
    while (!rpcsWaitingForResponse.empty()) {
        MTcpClientRpc& rpc = rpcsWaitingForResponse.front();
        rpc.notifier->failed();
        rpcsWaitingForResponse.pop_front();
        transport->clientRpcPool.destroy(&rpc);

    }
    while (!rpcsWaitingToSend.empty()) {
        MTcpClientRpc& rpc = rpcsWaitingToSend.front();
        rpc.notifier->failed();
        rpcsWaitingToSend.pop_front();
        transport->clientRpcPool.destroy(&rpc);
    }
    if (clientIoHandler) {
        Dispatch::Lock lock(transport->context->dispatch);
        clientIoHandler.destroy();
    }
}

/**
 * This method is invoked once the header has been received for an RPC response.
 * It uses information in the header to locate the corresponding MTcpClientRpc
 * object, and returns the Buffer to use for the response.
 *
 * \param header
 *      The header from the incoming RPC.
 *
 * \return
 *      If the nonce in the header refers to an active RPC, then the return
 *      value is the reply payload for that RPC.  If no matching RPC can be
 *      found (perhaps the RPC was canceled?) then NULL is returned to indicate
 *      that the input message should be dropped.
 */
Buffer*
MTcpTransport::MTcpSession::findRpc(Header* header) {
    foreach (MTcpClientRpc& rpc, rpcsWaitingForResponse) {
        if (rpc.nonce == header->nonce) {
            current = &rpc;
            return rpc.response;
        }
    }
    return NULL;
}

// See Transport::Session::getRpcInfo for documentation.
string
MTcpTransport::MTcpSession::getRpcInfo()
{
    const char* separator = "";
    string result;
    foreach (MTcpClientRpc& rpc, rpcsWaitingForResponse) {
        result += separator;
        result += WireFormat::opcodeSymbol(rpc.request);
        separator = ", ";
    }
    foreach (MTcpClientRpc& rpc, rpcsWaitingToSend) {
        result += separator;
        result += WireFormat::opcodeSymbol(rpc.request);
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
MTcpTransport::MTcpSession::sendRequest(Buffer* request, Buffer* response,
        RpcNotifier* notifier)
{
    response->reset();
    if (fd == -1) {
        notifier->failed();
        return;
    }
    alarm.rpcStarted();
    MTcpClientRpc* rpc = transport->clientRpcPool.construct(request, response,
            notifier, serial);
    serial++;
    if (!rpcsWaitingToSend.empty()) {
        // Can't transmit this request yet; there are already other
        // requests that haven't yet been sent.
        rpcsWaitingToSend.push_back(*rpc);
        return;
    }

    // Try to transmit the request.
    try {
        bytesLeftToSend = transport->sendMessage(fd, rpc->nonce,
                request, -1);
    } catch (TransportException& e) {
        abort();
        notifier->failed();
        return;
    }
    if (bytesLeftToSend == 0) {
        // The whole request was sent immediately (this should be the
        // common case).
        rpcsWaitingForResponse.push_back(*rpc);
        rpc->sent = true;
    } else {
        rpcsWaitingToSend.push_back(*rpc);
        clientIoHandler->setEvents(Dispatch::FileEvent::READABLE |
                Dispatch::FileEvent::WRITABLE);
    }
}

/**
 * Constructor for ClientSocketHandlers.
 *
 * \param fd
 *      File descriptor for a socket on which the the response for
 *      an RPC will arrive.
 * \param session
 *      The MTcpSession that is controlling this request and its response.
 */
MTcpTransport::ClientSocketHandler::ClientSocketHandler(int fd,
        MTcpSession* session)
    : Dispatch::File(session->transport->context->dispatch, fd,
                     Dispatch::FileEvent::READABLE)
    , fd(fd)
    , session(session)
{
    // Empty constructor body.
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
MTcpTransport::ClientSocketHandler::handleFileEvent(int events)
{
    try {
        if (events & Dispatch::FileEvent::READABLE) {
            if (session->message->readMessage(fd)) {
                // This RPC is finished.
                if (session->current != NULL) {
                    session->rpcsWaitingForResponse.erase(
                            session->rpcsWaitingForResponse.iterator_to(
                            *session->current));
                    session->alarm.rpcFinished();
                    session->current->notifier->completed();
                    session->transport->clientRpcPool.destroy(session->current);
                    session->current = NULL;
                }
                session->message.construct(static_cast<Buffer*>(NULL), session);
            }
        }
        if (events & Dispatch::FileEvent::WRITABLE) {
            while (!session->rpcsWaitingToSend.empty()) {
                MTcpClientRpc& rpc = session->rpcsWaitingToSend.front();
                session->bytesLeftToSend = session->transport->sendMessage(
                        session->fd, rpc.nonce, rpc.request,
                        session->bytesLeftToSend);
                if (session->bytesLeftToSend != 0) {
                    return;
                }
                // The current RPC is finished; start the next one, if
                // there is one.
                session->rpcsWaitingToSend.pop_front();
                session->rpcsWaitingForResponse.push_back(rpc);
                rpc.sent = true;
                session->bytesLeftToSend = -1;
            }
            setEvents(Dispatch::FileEvent::READABLE);
        }
    } catch (TransportException& e) {
        session->abort();
    }
}

// See Transport::ServerRpc::sendReply for documentation.
void
MTcpTransport::MTcpServerRpc::sendReply()
{
    try {
        Socket* socket = transport->sockets[fd].get();

        // It's possible that our fd has been closed (or even reused for a
        // new connection); if so, just discard the RPC without sending
        // a response.
        if ((socket != NULL) && (socket->id == socketId)) {
            if (!socket->rpcsWaitingToReply.empty()) {
                // Can't transmit the response yet; the socket is backed up.
                socket->rpcsWaitingToReply.push_back(*this);
                return;
            }

            // Try to transmit the response.
            socket->bytesLeftToSend = transport->sendMessage(fd,
                    message.header.nonce, &replyPayload, -1);
            if (socket->bytesLeftToSend > 0) {
                socket->rpcsWaitingToReply.push_back(*this);
                socket->ioHandler.setEvents(Dispatch::FileEvent::READABLE |
                        Dispatch::FileEvent::WRITABLE);
                return;
            }
        }
    } catch (TransportException& e) {
        transport->closeSocket(fd);
    }

    // The whole response was sent immediately (this should be the
    // common case).  Recycle the RPC object.
    transport->serverRpcPool.destroy(this);
}

// See Transport::ServerRpc::getclientServiceLocator for documentation.
string
MTcpTransport::MTcpServerRpc::getClientServiceLocator()
{
    Socket* socket = transport->sockets[fd].get();
    return format("tcp:host=%s,port=%hu", inet_ntoa(socket->sin.sin_addr),
        NTOHS(socket->sin.sin_port));
}

}  // namespace RAMCloud
