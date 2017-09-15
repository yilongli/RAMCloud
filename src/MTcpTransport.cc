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
#include "Util.h"

namespace RAMCloud {

int MTcpTransport::messageChunks = 0;

#define MESSAGE_HEADER_LEN static_cast<uint32_t>(sizeof(Header))

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
    , activeFds()
    , epollId(-1)
    , srcip(0)
    , epollEvents()
    , clientSessions()
    , serverSockets()
    , nextServerSocketId(100)
    , poller(context, this)
    , serverRpcPool()
    , clientRpcPool()
{
    uint64_t startTime = Cycles::rdtsc();

    // FIXME: how to avoid hardcode the config file path?
    if (mtcp_init("/shome/RAMCloud/config/mtcp.conf") == -1) {
        LOG(WARNING, "MTcpTransport couldn't initialize", strerror(errno));
        throw TransportException(HERE, "MTcpTransport couldn't initialize",
                errno);
    }

    // FIXME: which cpu should I use? the same one as dispatch thread? or different one?
    // FIXME: there is a check "0 <= mctx->cpu < num_cpus" in api.c:GetMTCPManager that
    // I do not understand; so right now just use cpu 0 for best chance...
    int cpu = 0;
    mctx = mtcp_create_context(cpu);
    // FIXME: Do we have to put dispatch thread and mtcp thread on the same core?
//    Util::pinThreadToCore(cpu);
    if (NULL == mctx) {
        throw TransportException(HERE,
                "MTcpTransport couldn't create mTCP context");
    }

    epollId = mtcp_epoll_create(mctx, arrayLength(epollEvents));
    if (epollId < 0) {
        throw TransportException(HERE,
                "MTcpTransport couldn't create epoll instance");
    }

//    if (serviceLocator == NULL) {
//        // The rest of the code is just creating the listener socket.
//        // No need to continue for client-side transport.
//        return;
//    }

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

    if (serviceLocator == NULL) {
        // The rest of the code is just creating the listener socket.
        // No need to continue for client-side transport.
        srcip = ((struct sockaddr_in*)ifa->ifa_addr)->sin_addr.s_addr;
//        srcip = inet_addr(tcpAddr.substr(0, pos).c_str());
//        LOG(NOTICE, "Local IP: %s", tcpAddr.substr(0, pos).c_str());
        // FIXME: On the coordinator, this takes about 2000 ms and causes
        // EnsureServers to close its client sockets due to transport timeout(?)
        // That is why on the coordintor we saw broken pipe when trying to send
        // out the reply of GET_SERVER_LIST RPC.
        LOG(ERROR, "MTcpTransport ctor takes %.1f ms",
                Cycles::toSeconds(Cycles::rdtsc() - startTime)*1e3);
        return;
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
    listenSocket = mtcp_socket(mctx, AF_INET, SOCK_STREAM, 0);
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

//    struct sockaddr_in saddr;
//    saddr.sin_family = AF_INET;
//    saddr.sin_addr.s_addr = INADDR_ANY;
//    saddr.sin_port = HTONS(stoi(tcpAddr.substr(pos+1)));
//    if (mtcp_bind(mctx, listenSocket, (struct sockaddr *)&saddr,
//            sizeof(struct sockaddr_in)) == -1) {
//    LOG(WARNING, "Binding to %s, listenSocket = %d", address.toString().c_str(), listenSocket);
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
    // FIXME: I think this can be simply level-triggered and not one shot?
//    event.events = READABLE | MTCP_EPOLLONESHOT;
    event.events = READABLE;
    if (mtcp_epoll_ctl(mctx, epollId, MTCP_EPOLL_CTL_ADD, listenSocket,
            &event) == -1) {
        throw TransportException(HERE,
                "MTcpTransport couldn't set epoll event for listenSocket", errno);
    }
    activeFds.insert(listenSocket);

    LOG(ERROR, "MTcpTransport ctor takes %.1f ms",
            Cycles::toSeconds(Cycles::rdtsc() - startTime)*1e3);
}

/**
 * Destructor for MTcpTransports: close file descriptors and perform
 * any other needed cleanup.
 */
MTcpTransport::~MTcpTransport()
{
    if (listenSocket >= 0) {
        mtcp_epoll_ctl(mctx, epollId, MTCP_EPOLL_CTL_DEL, listenSocket, NULL);
        mtcp_close(mctx, listenSocket);
    }
    mtcp_destroy_context(mctx);
    mtcp_destroy();
}

void
MTcpTransport::setEvents(int fd, uint32_t events)
{
    // FIXME: do we have to initialize this event struct properly?
    // FIXME: verify this struct is input only
    struct mtcp_epoll_event event;
    event.data.sockid = fd;
    event.events = events | MTCP_EPOLLONESHOT;
    if (activeFds.find(fd) == activeFds.end()) {
        mtcp_epoll_ctl(mctx, epollId, MTCP_EPOLL_CTL_ADD, fd, &event);
        activeFds.insert(fd);
    } else {
        mtcp_epoll_ctl(mctx, epollId, MTCP_EPOLL_CTL_MOD, fd, &event);
    }
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
    int n = mtcp_epoll_wait(t->mctx, t->epollId, t->epollEvents,
            arrayLength(t->epollEvents), 0);
    for (int i = 0; i < n; i++) {
        int fd = t->epollEvents[i].data.sockid;
        uint32_t events = t->epollEvents[i].events;
//        LOG(WARNING, "there are some events finally: %d", n);
        if (fd == t->listenSocket) {
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
                        continue;
                }

                // Unexpected error: log a message and then close the socket
                // (so we don't get repeated errors).
                LOG(ERROR, "error in MTcpTransport::AcceptHandler accepting "
                        "connection for '%s': %s",
                        t->locatorString.c_str(), strerror(errno));
                // FIXME: WHY SET TO 0? This transport cannot be recovered anyway.
                t->setEvents(fd, 0);
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
            t->serverSockets[acceptedFd].construct(acceptedFd, t, &sin);
            t->setEvents(acceptedFd, READABLE);
            LOG(NOTICE, "Accepted connection from %s",
                    IpAddress((struct sockaddr*)&sin).toString().c_str());
        } else if (t->clientSessions.find(fd) != t->clientSessions.end()) {
            MTcpSession* session = t->clientSessions[fd];
            try {
                if (events & READABLE) {
                    bool messageComplete =
                            session->currentResponse->tryToReadMessage(fd);
                    LOG(WARNING, "client receiving response, fd %d, "
                            "rpcId %lu, length %u, received %u", fd,
                            session->currentResponse->header.rpcId,
                            session->currentResponse->header.len,
                            session->currentResponse->messageBytesReceived);
                    if (messageComplete) {
                        // This RPC is finished.
                        ClientRpc* rpc = session->currentRpc;
                        // FIXME: The only reason the following check may fail is because
                        // the rpc might be cancelled?
                        if (rpc != NULL) {
                            session->outgoingRpcs.erase(rpc->rpcId);
                            session->alarm.rpcFinished();
                            rpc->notifier->completed();
                            session->transport->clientRpcPool.destroy(rpc);
                            session->currentRpc = NULL;
                        }
                        session->currentResponse.construct(session);
                    }
                }
                if (events & WRITABLE) {
                    while (!session->outgoingRequests.empty()) {
                        ClientRpc* rpc = &session->outgoingRequests.front();
                        rpc->bytesSent = t->sendMessage(
                                session->fd, rpc->rpcId, rpc->request,
                                rpc->bytesSent);
                        LOG(WARNING, "client sending request, fd %d, "
                                "rpcId %lu, length %u, sent %u", fd,
                                rpc->rpcId, rpc->request->size(),
                                rpc->bytesSent - MESSAGE_HEADER_LEN);
                        if (rpc->bytesSent <
                                rpc->request->size() + MESSAGE_HEADER_LEN) {
                            goto done;
                        }
                        // The current RPC request is finished; start the next one,
                        // if there is one.
                        session->outgoingRequests.pop_front();
                        rpc->transmitPending = false;
                    }
                    // Optimization: No more outstanding requests, only notify me
                    // when the socket becomes readable.
                    t->setEvents(fd, READABLE);
                    continue;
                }
            } catch (TransportException& e) {
                session->abort();
            }
        } else if (t->serverSockets.find(fd) != t->serverSockets.end()) {
            Socket* socket = t->serverSockets[fd].get();
            MTcpTransport* transport = socket->transport;
            try {
                if (events & READABLE) {
                    if (socket->incomingRpc == NULL) {
                        socket->incomingRpc =
                                transport->serverRpcPool.construct(fd, transport);
                    }
                    IncomingMessage* request = &socket->incomingRpc->request;
                    bool messageComplete = request->tryToReadMessage(fd);
                    if (messageComplete) {
                        // The incoming request is complete; pass it off for servicing.
                        transport->context->workerManager->handleRpc(
                                socket->incomingRpc);
                        socket->incomingRpc = NULL;
                    }
                    // FIXME: change to timeTrace
                    LOG(WARNING, "server receiving request, fd %d, rpcId %lu, "
                            "length %u, received %u", fd,
                            request->header.rpcId, request->header.len,
                            request->messageBytesReceived);
                }
                if (events & WRITABLE) {
                    while (!socket->outgoingResponses.empty()) {
                        ServerRpc* rpc = &socket->outgoingResponses.front();
                        rpc->bytesSent = transport->sendMessage(fd,
                                rpc->request.header.rpcId, &rpc->replyPayload,
                                rpc->bytesSent);
                        LOG(WARNING, "server sending response, fd %d, "
                                "rpcId %lu, length %u, sent %u", fd,
                                rpc->request.header.rpcId,
                                rpc->replyPayload.size(),
                                rpc->bytesSent - MESSAGE_HEADER_LEN);
                        if (rpc->bytesSent <
                                rpc->replyPayload.size() + MESSAGE_HEADER_LEN) {
                            goto done;
                        }
                        // The current reply is finished; start the next one, if
                        // there is one.
                        socket->outgoingResponses.pop_front();
                        transport->serverRpcPool.destroy(rpc);
                    }
                    // Optimization: no more outstanding responses, only notify me
                    // when the socket becomes readable.
                    t->setEvents(fd, READABLE);
                    continue;
                }
            } catch (TransportException& e) {
                // Close the server socket.
                LOG(WARNING, "close server socket %d: %s", fd, e.what());
                transport->serverSockets.erase(fd);
            }
        } else {
            LOG(ERROR, "Unexpected event, fd %d, event bitmask %u", fd, events);
        }

      done:
        t->setEvents(fd, READABLE | WRITABLE);
    }

    return n;
}

/**
 * Constructor for Sockets.
 */
MTcpTransport::Socket::Socket(int fd, MTcpTransport* transport,
        sockaddr_in* sin)
    : transport(transport)
    , fd(fd)
    , id(transport->nextServerSocketId)
    , incomingRpc(NULL)
    , outgoingResponses()
    , sin(*sin)
{
    transport->nextServerSocketId++;
}

/**
 * Destructor for Sockets.
 */
MTcpTransport::Socket::~Socket() {
    if (incomingRpc != NULL) {
        transport->serverRpcPool.destroy(incomingRpc);
    }
    while (!outgoingResponses.empty()) {
        ServerRpc* rpc = &outgoingResponses.front();
        outgoingResponses.pop_front();
        transport->serverRpcPool.destroy(rpc);
    }

    mtcp_epoll_ctl(transport->mctx, transport->epollId,
            MTCP_EPOLL_CTL_DEL, fd, NULL);
    transport->activeFds.erase(fd);
    // No need to erase fd from transport->serverSockets because
    // this destructor must have been triggered by that.
    mtcp_close(transport->mctx, fd);
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
MTcpTransport::sendMessage(int fd, uint64_t rpcId, Buffer* payload,
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
    int iovecIndex;
    if (bytesSent < sizeof(Header)) {
        iov[0].iov_base = reinterpret_cast<char*>(&header) + bytesSent;
        iov[0].iov_len = sizeof(Header) - bytesSent;
        iovecIndex = 1;
        offset = 0;
    } else {
        iovecIndex = 0;
        offset = bytesSent - sizeof(Header);
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

    int r = mtcp_writev(mctx, fd, iov, iovecIndex);
    if (r < 0) {
        if ((errno != EAGAIN) && (errno != EWOULDBLOCK)) {
            LOG(WARNING, "TcpTransport sendmsg error: %s", strerror(errno));
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
MTcpTransport::recvCarefully(int fd, char* buffer, size_t length,
        IncomingMessage* message) {
    ssize_t actual = mtcp_recv(mctx, fd, buffer, length, 0);
    // FIXME: man mtcp_recv says the current implementation only supports 0 or MSG_PEEK
//    ssize_t actual = mtcp_recv(mctx, fd, buffer, length, MSG_DONTWAIT);
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
                "received %u", strerror(errno), message->header.rpcId,
                message->header.len, message->messageBytesReceived);
    }
    throw TransportException(HERE, "MTcpTransport recv error", errno);
}

/**
 * Constructor for IncomingMessages that are requests.
 * \param buffer
 *      Buffer in which to place the body of the incoming message;
 *      the caller should ensure that the buffer is empty.
 */
MTcpTransport::IncomingMessage::IncomingMessage(Buffer* buffer,
        MTcpTransport* t)
    : header(), headerBytesReceived(0), messageBytesReceived(0),
      messageLength(0), buffer(buffer), session(NULL), t(t)
{
}

/**
 * Constructor for IncomingMessages that are responses.
 * \param session
 *      The session through which the RPC request was sent. Used to
 *      find the ClientRpc the response belongs to.
 */
MTcpTransport::IncomingMessage::IncomingMessage(MTcpSession* session)
    : header(), headerBytesReceived(0), messageBytesReceived(0),
      messageLength(0), buffer(NULL), session(session), t(session->transport)
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
MTcpTransport::IncomingMessage::tryToReadMessage(int fd) {
    // First make sure we have received the header (it may arrive in
    // multiple chunks).
    if (headerBytesReceived < sizeof(Header)) {
        ssize_t len = t->recvCarefully(fd,
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
        ssize_t len = t->recvCarefully(fd, static_cast<char*>(dest),
                messageLength - messageBytesReceived, this);
        if (unlikely(len == 0)) {
            LOG(ERROR, "Failed to read anything from a readable socket?");
        }
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
        ssize_t len = t->recvCarefully(fd, buffer, maxLength, this);
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
    , fd(-1), nextRpcId(1)
    , outgoingRequests()
    , outgoingRpcs()
    , currentResponse()
    , currentRpc(NULL)
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

    int w = mtcp_init_rss(transport->mctx, transport->srcip, 1,
                  ((struct sockaddr_in *)&address.address)->sin_addr.s_addr,
                  ((struct sockaddr_in *)&address.address)->sin_port);
    if (w < 0) {
        LOG(WARNING, "mtcp_init_rss failed: %s", strerror(errno));
    }

    // FIXME: without this, mtcp_connect could block and multiple threads could
    // deadlock at TransportManager::openSession
//    mtcp_setsock_nonblock(transport->mctx, fd);

//    struct sockaddr_in saddr;
//    saddr.sin_family = AF_INET;
//    saddr.sin_addr.s_addr = inet_addr("10.1.1.3");
//    saddr.sin_family = HTONS(12247);
//
//    int r = mtcp_connect(transport->mctx, fd, (struct sockaddr *)&saddr,
//            sizeof(struct sockaddr_in));
    int r = mtcp_connect(transport->mctx, fd, &address.address,
            sizeof(address.address));
    if (r == -1) {
        mtcp_close(transport->mctx, fd);
        fd = -1;
        LOG(WARNING, "MTcpTransport couldn't connect to %s: %s",
            this->serviceLocator.c_str(), strerror(errno));
//        DIE("JUST CAN'T CONNECT?!");
        throw TransportException(HERE, format(
                "MTcpTransport couldn't connect to %s",
                this->serviceLocator.c_str()), errno);
    }
    LOG(NOTICE, "Session connect to %s", this->serviceLocator.c_str());

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

    transport->clientSessions[fd] = this;

    // FIXME: WHY LOCK?
    /// Arrange for notification whenever the server sends us data.
    Dispatch::Lock lock(transport->context->dispatch);
    transport->setEvents(fd, READABLE);
    currentResponse.construct(this);
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
    for (ClientRpcMap::iterator it = outgoingRpcs.begin();
            it != outgoingRpcs.end(); it++) {
        ClientRpc* rpc = it->second;
        if (rpc->notifier == notifier) {
            transport->clientRpcPool.destroy(rpc);
            alarm.rpcFinished();

            // If we have started reading the response message,
            // cancel that also.
            if (rpc == currentRpc) {
                // Setting messageLength to 0 so that unread bytes in the
                // socket will be properly discarded in #tryToReadMessage.
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
MTcpTransport::MTcpSession::close()
{
    if (fd >= 0) {
        mtcp_close(transport->mctx, fd);
        fd = -1;
    }
    outgoingRequests.clear();
    for (ClientRpcMap::iterator it = outgoingRpcs.begin();
            it != outgoingRpcs.end(); it++) {
        ClientRpc *rpc = it->second;
        rpc->notifier->failed();
        transport->clientRpcPool.destroy(rpc);
    }
    outgoingRpcs.clear();
    transport->clientSessions.erase(fd);
    mtcp_epoll_ctl(transport->mctx, transport->epollId,
            MTCP_EPOLL_CTL_DEL, fd, NULL);
    transport->activeFds.erase(fd);
}

// See Transport::Session::getRpcInfo for documentation.
string
MTcpTransport::MTcpSession::getRpcInfo()
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
MTcpTransport::MTcpSession::sendRequest(Buffer* request, Buffer* response,
        RpcNotifier* notifier)
{
    response->reset();
    if (fd == -1) {
        notifier->failed();
        return;
    }
    alarm.rpcStarted();
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
        rpc->bytesSent = transport->sendMessage(fd, rpc->rpcId,
                request, 0);
        LOG(WARNING, "client sending request, fd %d, rpcId %lu, "
                "length %u, sent %u", fd, rpc->rpcId,
                rpc->request->size(), rpc->bytesSent - MESSAGE_HEADER_LEN);
    } catch (TransportException& e) {
        abort();
        notifier->failed();
        return;
    }
    if (rpc->bytesSent >= request->size() + MESSAGE_HEADER_LEN) {
        // The whole request was sent immediately (this should be the
        // common case).
        rpc->transmitPending = false;
    } else {
        LOG(WARNING, "session %p", transport->clientSessions[fd]);
        outgoingRequests.push_back(*rpc);
        transport->setEvents(fd, READABLE | WRITABLE);
    }
}

// See Transport::ServerRpc::sendReply for documentation.
void
MTcpTransport::ServerRpc::sendReply()
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
        bytesSent = transport->sendMessage(fd, request.header.rpcId,
                &replyPayload, 0);
        if (bytesSent < replyPayload.size() + MESSAGE_HEADER_LEN) {
            socket->outgoingResponses.push_back(*this);
            transport->setEvents(fd, READABLE | WRITABLE);
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
MTcpTransport::ServerRpc::getClientServiceLocator()
{
    Socket* socket = transport->serverSockets[fd].get();
    return format("tcp:host=%s,port=%hu", inet_ntoa(socket->sin.sin_addr),
        NTOHS(socket->sin.sin_port));
}

}  // namespace RAMCloud
