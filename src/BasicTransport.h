/* Copyright (c) 2015-2017 Stanford University
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

#ifndef RAMCLOUD_BASICTRANSPORT_H
#define RAMCLOUD_BASICTRANSPORT_H

#include <deque>

#include "BoostIntrusive.h"
#include "Buffer.h"
#include "Cycles.h"
#include "Dispatch.h"
#include "Driver.h"
#include "ServerRpcPool.h"
#include "ServiceLocator.h"
#include "Transport.h"

namespace RAMCloud {

/**
 * This class implements a simple transport that uses the Driver mechanism
 * for datagram-based packet delivery.
 */
class BasicTransport : public Transport {
  PRIVATE:
    struct DataHeader;

  public:
    explicit BasicTransport(Context* context, const ServiceLocator* locator,
            Driver* driver, uint64_t clientId);
    ~BasicTransport();

    string getServiceLocator();
    Transport::SessionRef getSession(const ServiceLocator* serviceLocator,
            uint32_t timeoutMs = 0) {
        // Note: DispatchLock not needed here, since this doesn't access
        // any transport or driver state.
        return new Session(this, serviceLocator, timeoutMs);
    }
    void registerMemory(void* base, size_t bytes) {
        driver->registerMemory(base, bytes);
    }

  PRIVATE:
    /**
     * A unique identifier for an RPC.
     */
    struct RpcId {
        uint64_t clientId;           // Uniquely identifies the client for
                                     // this request.
        uint64_t sequence;           // Sequence number for this RPC (unique
                                     // for clientId, monotonically increasing).

        RpcId(uint64_t clientId, uint64_t sequence)
            : clientId(clientId)
            , sequence(sequence)
        {}

        /**
         * Comparison function for RpcIds, for use in std::maps etc.
         */
        bool operator<(RpcId other) const
        {
            return (clientId < other.clientId)
                    || ((clientId == other.clientId)
                    && (sequence < other.sequence));
        }

        /**
         * Equality function for RpcIds, for use in std::unordered_maps etc.
         */
        bool operator==(RpcId other) const
        {
            return ((clientId == other.clientId)
                    && (sequence == other.sequence));
        }

        /**
         * This class computes a hash of an RpcId, so that RpcIds can
         * be used as keys in unordered_maps.
         */
        struct Hasher {
            std::size_t operator()(const RpcId& rpcId) const {
                std::size_t h1 = std::hash<uint64_t>()(rpcId.clientId);
                std::size_t h2 = std::hash<uint64_t>()(rpcId.sequence);
                return h1 ^ (h2 << 1);
            }
        };
    } __attribute__((packed));

    /**
     * This class represents the client side of the connection between a
     * particular client in a particular server. Each session can support
     * multiple outstanding RPCs.
     */
    class Session : public Transport::Session {
      public:
        virtual ~Session();
        void abort();
        void cancelRequest(RpcNotifier* notifier);
        string getRpcInfo();
        virtual void sendRequest(Buffer* request, Buffer* response,
                RpcNotifier* notifier);

      PRIVATE:
        // Transport associated with this session.
        BasicTransport* t;

        // Address of the target server to which RPCs will be sent. This is
        // dynamically allocated and must be freed by the session.
        Driver::Address* serverAddress;

        // True means the abort method has been invoked, sso this session
        // is no longer usable.
        bool aborted;

        Session(BasicTransport* t, const ServiceLocator* locator,
                uint32_t timeoutMs);

        friend class BasicTransport;
        DISALLOW_COPY_AND_ASSIGN(Session);
    };

    /**
     * An object of this class stores the state for a multi-packet
     * message for which at least one packet has been received. It is
     * used both for request messages on the server and for response
     * messages on the client. Not used for single-packet messages.
     */
    class MessageAccumulator {
      public:
        MessageAccumulator(BasicTransport* t, Buffer* buffer);
        ~MessageAccumulator();
        bool addPacket(DataHeader *header, uint32_t length);
        bool appendFragment(DataHeader *header, uint32_t length);
        uint32_t requestRetransmission(BasicTransport *t,
                const Driver::Address* address, RpcId rpcId,
                uint32_t grantOffset, uint8_t whoFrom);

        /// Transport that is managing this object.
        BasicTransport* t;

        // TODO: NOT USING VECTOR BECAUSE EXPANSION COULD CAUSE SIGNIFICANT JITTER
        // DEQUE SUPPORTS O(1) RANDOM ACCESS (BACKED BY FIXED-SIZE ARRAYS), CHEAPER
        // EXPANSION AND BETTER CACHE LOCALITY THAN LINKED LIST.
        using MessageBuffer = std::deque<char*>;
        MessageBuffer* assembledPayloads;

        /// Used to assemble the complete message. It holds all of the
        /// data that has been received for the message so far, up to the
        /// first byte that has not yet been received.
        Buffer* buffer;

        /// Describes a portion of an incoming message.
        struct MessageFragment {
            /// Address of first byte of a DATA packet, as returned by
            /// Driver::steal.
            DataHeader *header;

            /// # of bytes of message data available at payload.
            uint32_t length;

            MessageFragment()
                    : header(NULL), length(0)
            {}
            MessageFragment(DataHeader *header, uint32_t length)
                    : header(header), length(length)
            {}
        };

        /// This map stores information about packets that have been
        /// received but cannot yet be added to buffer because one or
        /// more preceding packets have not yet been received. Each
        /// key is an offset in the message; each value describes the
        /// corresponding fragment, which is a stolen Driver::Received.
        typedef std::map<uint32_t, MessageFragment>FragmentMap;
        FragmentMap fragments;

      PRIVATE:
        DISALLOW_COPY_AND_ASSIGN(MessageAccumulator);
    };

    /**
     * An object of this class stores the state for a scheduled message for
     * for which at least one packet has been received. It is used both for
     * request messages on the server and for response messages on the client.
     * Not used for messages that can be sent unilaterally.
     */
    class ScheduledMessage {
      public:
        ScheduledMessage(RpcId rpcId, MessageAccumulator* accumulator,
                uint32_t unscheduledBytes,
                const Driver::Address* senderAddress, uint32_t totalLength,
                uint8_t whoFrom);
        ~ScheduledMessage();

        /// Holds state of partially-received multi-packet messages.
        const MessageAccumulator* const accumulator;

        /// Offset from the most recent GRANT packet we have sent for this
        /// incoming message, or # unscheduled bytes in this message if we
        /// haven't sent any GRANTs.
        uint32_t grantOffset;

        /// Unique identifier for the RPC this message belongs to.
        RpcId rpcId;

        /// Network address of the message sender.
        const Driver::Address* senderAddress;

        /// Total # bytes in the message.
        const uint32_t totalLength;

        /// Must be either FROM_CLIENT, indicating that this message is the
        /// request from a client, or FROM_SERVER, indicating that this is
        /// the response from a server.
        const uint8_t whoFrom;

      PRIVATE:
        DISALLOW_COPY_AND_ASSIGN(ScheduledMessage);
    };

    // TODO
    class OutgoingMessage {
      public:
        /// Holds the contents of the message.
        Buffer* buffer;

        /// True means this message is the request of a ClientRpc; false means
        /// it is the response of a ServerRpc.
        const bool isRequest;

        /// Where to send the message.
        const Driver::Address* recipient;

        /// Offset within the message of the next byte we should transmit to
        /// the recipient; all preceding bytes have already been sent.
        uint32_t transmitOffset;

        /// The number of bytes in the message that it's OK for us to transmit.
        /// Bytes after this cannot be transmitted until we receive a GRANT for
        /// them.
        uint32_t transmitLimit;

        /// True means this message is among the sender's top outgoing
        /// messages with fewest bytes left (and this object is linked in
        /// t->topOutgoingMessages).
        bool topChoice;

        /// Cycles::rdtsc time of the most recent time that we transmitted
        /// data bytes of the message.
        uint64_t lastTransmitTime;

        /// Used to link this object into t->topOutgoingMessages.
        IntrusiveListHook outgoingMessageLinks;

        // TODO: doc. dynamic throttling
        /// # bytes that can be sent unilaterally.
        uint32_t unscheduledBytes;

        OutgoingMessage(bool isRequest, Buffer* buffer,
                const Driver::Address* recipient)
            : buffer(buffer)
            , isRequest(isRequest)
            , recipient(recipient)
            , transmitOffset(0)
            , transmitLimit(0)
            , topChoice(false)
            , lastTransmitTime(0)
            , outgoingMessageLinks()
            , unscheduledBytes()
        {}

        virtual ~OutgoingMessage() {}

      PRIVATE:
        DISALLOW_COPY_AND_ASSIGN(OutgoingMessage);
    };

    /**
     * One object of this class exists for each outgoing RPC; it is used
     * to track the RPC through to completion.
     */
    class ClientRpc : public OutgoingMessage {
      public:
        /// The ClientSession on which to send/receive the RPC.
        Session* session;

        /// Request message for the RPC.
        Buffer* request;

        /// Will eventually hold the response for the RPC.
        Buffer* response;

        /// Use this object to report completion.
        RpcNotifier* notifier;

        /// Unique identifier for this RPC.
        RpcId rpcId;

        /// The sum of the offset and length fields from the most recent
        /// RESEND we have sent, 0 if no RESEND has been sent for this
        /// RPC. Used to detect unnecessary RESENDs (because the original
        /// data eventually arrives).
        uint32_t resendLimit;

        /// Number of times that the transport timer has fired since we
        /// received any packets from the server.
        uint32_t silentIntervals;

        /// True means that the request message is in the process of being
        /// transmitted (and this object is linked on t->outgoingRequests).
        bool transmitPending;

        /// Holds state of partially-received multi-packet responses.
        Tub<MessageAccumulator> accumulator;

        /// Holds state of response messages that require scheduling.
        Tub<ScheduledMessage> scheduledMessage;

        /// Used to link this object into t->outgoingRequests.
        IntrusiveListHook outgoingRequestLinks;

        ClientRpc(Session* session, uint64_t sequence, Buffer* request,
                Buffer* response, RpcNotifier* notifier)
            : OutgoingMessage(true, request, session->serverAddress)
            , session(session)
            , request(request)
            , response(response)
            , notifier(notifier)
            , rpcId(session->t->clientId, sequence)
            , resendLimit(0)
            , silentIntervals(0)
            , transmitPending(false)
            , accumulator()
            , scheduledMessage()
            , outgoingRequestLinks()
        {}

      PRIVATE:
        DISALLOW_COPY_AND_ASSIGN(ClientRpc);
    };

    /**
     * Holds server-side state for an RPC.
     */
    class ServerRpc : public Transport::ServerRpc, public OutgoingMessage {
      public:
        void sendReply();
        string getClientServiceLocator();

        /// The transport that will be used to deliver the response when
        /// the RPC completes.
        BasicTransport* t;

        /// Uniquely identifies this RPC among all RPCs ever received by
        /// this server. This is the *server's* sequence number; the client's
        /// sequence number is in rpcId.
        uint64_t sequence;

        // TODO: THIS FIELD IS NOW REDUNDANT; REMOVE IT AND USE response->recipient instead?
        /// Where to send the response once the RPC has executed.
        const Driver::Address* clientAddress;

        /// Unique identifier for this RPC.
        RpcId rpcId;

        /// The sum of the offset and length fields from the most recent
        /// RESEND we have sent, 0 if no RESEND has been sent for this
        /// RPC. Used to detect unnecessary RESENDs (because the original
        /// data eventually arrives).
        uint32_t resendLimit;

        /// Number of times that the transport timer has fired since we
        /// received any packets from the client.
        uint32_t silentIntervals;

        /// True means we have received the entire request message, so either
        /// we're processing the request or we're sending the response now.
        bool requestComplete;

        /// True means we have finished processing the request and are trying
        /// to send a response.
        bool sendingResponse;

        /// Holds state of partially-received multi-packet requests.
        Tub<MessageAccumulator> accumulator;

        /// Holds state of request messages that require scheduling.
        Tub<ScheduledMessage> scheduledMessage;

        /// Used to link this object into t->serverTimerList.
        IntrusiveListHook timerLinks;

        /// Used to link this object into t->outgoingResponses.
        IntrusiveListHook outgoingResponseLinks;

        ServerRpc(BasicTransport* transport, uint64_t sequence,
                const Driver::Address* clientAddress, RpcId rpcId)
            : OutgoingMessage(false, &replyPayload, clientAddress)
            , t(transport)
            , sequence(sequence)
            , clientAddress(clientAddress)
            , rpcId(rpcId)
            , resendLimit(0)
            , silentIntervals(0)
            , requestComplete(false)
            , sendingResponse(false)
            , accumulator()
            , scheduledMessage()
            , timerLinks()
            , outgoingResponseLinks()
        {}

        DISALLOW_COPY_AND_ASSIGN(ServerRpc);
    };

    /**
     * This enum defines the opcode field values for packets. See the
     * xxxHeader class definitions below for more information about each
     * kind of packet
     */
    enum PacketOpcode {
        ALL_DATA               = 20,
        DATA                   = 21,
        GRANT                  = 22,
        LOG_TIME_TRACE         = 23,
        RESEND                 = 24,
        ACK                    = 25,
        ABORT                  = 26,
        BOGUS                  = 27,      // Used only in unit tests.
        // If you add a new opcode here, you must also do the following:
        // * Change BOGUS so it is the highest opcode
        // * Add support for the new opcode in opcodeSymbol and headerToString
    };

    /**
     * Describes the wire format for header fields that are common to all
     * packet types.
     */
    struct CommonHeader {
        uint8_t opcode;              // One of the values of PacketOpcode.
        RpcId rpcId;                 // RPC associated with this packet.
        uint8_t flags;               // ORed competition a flag bits; see
                                     // below for definitions.
        CommonHeader(PacketOpcode opcode, RpcId rpcId, uint8_t flags)
            : opcode(opcode), rpcId(rpcId), flags(flags) {}
    } __attribute__((packed));

    // Bit values for CommonHeader flags; not all flags are valid for all
    // opcodes.
    // FROM_CLIENT:              Valid for all opcodes. Non-zero means this
    //                           packet was sent from client to server; zero
    //                           means it was sent from server to client.
    // FROM_SERVER:              Opposite of FROM_CLIENT; provided to make
    //                           code more readable.
    // RETRANSMISSION:           Used only in DATA packets. Nonzero means
    //                           this packet is being sent in response to a
    //                           RESEND request (it has already been sent
    //                           previously).
    // RESTART:                  Used only in RESEND packets: indicates that
    //                           the server has no knowledge of this request,
    //                           so the client should reset its state to
    //                           indicate that everything needs to be resent.
    static const uint8_t FROM_CLIENT =    1;
    static const uint8_t FROM_SERVER =    0;
    static const uint8_t RETRANSMISSION = 2;
    static const uint8_t RESTART =        4;

    /**
     * Describes the wire format for an ALL_DATA packet, which contains an
     * entire request or response message.
     */
    struct AllDataHeader {
        CommonHeader common;         // Common header fields.
        uint16_t messageLength;      // Total # bytes in the message (i.e.,
                                     // the bytes following this header).

        // The remaining packet bytes after the header constitute the
        // entire request or response message.

        explicit AllDataHeader(RpcId rpcId, uint8_t flags,
                uint16_t messageLength)
            : common(PacketOpcode::ALL_DATA, rpcId, flags)
            , messageLength(messageLength) {}
    } __attribute__((packed));

    /**
     * Describes the wire format for a DATA packet, which contains a
     * portion of a request or response message
     */
    struct DataHeader {
        CommonHeader common;         // Common header fields.
        uint32_t totalLength;        // Total # bytes in the message (*not*
                                     // this packet!).
        uint32_t offset;             // Offset within the message of the first
                                     // byte of data in this packet.
        uint32_t unscheduledBytes;   // # unscheduled bytes in the message.

        // The remaining packet bytes after the header constitute message
        // data starting at the given offset.

        DataHeader(RpcId rpcId, uint32_t totalLength, uint32_t offset,
                uint32_t unscheduledBytes, uint8_t flags)
            : common(PacketOpcode::DATA, rpcId, flags)
            , totalLength(totalLength)
            , offset(offset)
            , unscheduledBytes(unscheduledBytes)
        {}
    } __attribute__((packed));

    /**
     * Describes the wire format for GRANT packets. A GRANT is sent by
     * the receiver back to the sender to indicate that it is now safe
     * for the sender to transmit a given range of bytes in the message.
     * This packet type is used for flow control.
     */
    struct GrantHeader {
        CommonHeader common;         // Common header fields.
        uint32_t offset;             // Byte offset within the message; the
                                     // sender should now transmit all data up
                                     // to (but not including) this offset, if
                                     // it hasn't already.

        GrantHeader(RpcId rpcId, uint32_t offset, uint8_t flags)
            : common(PacketOpcode::GRANT, rpcId, flags), offset(offset) {}
    } __attribute__((packed));

    /**
     * Describes the wire format for RESEND packets. A RESEND is sent by
     * the receiver back to the sender when it believes that some of the
     * message data was lost in transmission. The receiver should resend
     * the specified portion of the message, even if it already sent
     * it before.
     */
    struct ResendHeader {
        CommonHeader common;         // Common header fields.
        uint32_t offset;             // Byte offset within the message of the
                                     // first byte of data that should be
                                     // retransmitted.
        uint32_t length;             // Number of bytes of data to retransmit;
                                     // this could specify a range longer than
                                     // the total message size.

        ResendHeader(RpcId rpcId, uint32_t offset, uint32_t length,
                uint8_t flags)
            : common(PacketOpcode::RESEND, rpcId, flags), offset(offset),
              length(length) {}
    } __attribute__((packed));

    /**
     * Describes the wire format for LOG_TIME_TRACE packets. These packets
     * are only used for debugging and performance analysis: the recipient
     * will write its time trace to the log.
     */
    struct LogTimeTraceHeader {
        CommonHeader common;         // Common header fields.

        explicit LogTimeTraceHeader(RpcId rpcId, uint8_t flags)
            : common(PacketOpcode::LOG_TIME_TRACE, rpcId, flags) {}
    } __attribute__((packed));

    /**
     * Describes the wire format for ACK packets. These packets are used
     * to let the recipient know that the sender is still alive; they
     * don't trigger any actions on the receiver except resetting timers.
     */
    struct AckHeader {
        CommonHeader common;         // Common header fields.

        explicit AckHeader(RpcId rpcId, uint8_t flags)
            : common(PacketOpcode::ACK, rpcId, flags) {}
    } __attribute__((packed));

    /**
     * Describes the wire format for ABORT packets. These packets are used
     * to let the server know that the client has cancelled the request.
     */
    struct AbortHeader {
        CommonHeader common;         // Common header fields.

        explicit AbortHeader(RpcId rpcId)
            : common(PacketOpcode::ABORT, rpcId, FROM_CLIENT) {}
    } __attribute__((packed));

    /**
     * Causes BasicTransport to be invoked during each iteration through
     * the dispatch poller loop.
     */
    class Poller : public Dispatch::Poller {
      public:
        explicit Poller(Context* context, BasicTransport* t)
            : Dispatch::Poller(context->dispatch, "BasicTransport(" +
                    t->driver->getServiceLocator() + ")::Poller")
            , t(t) { }
        virtual int poll();
      private:
        // Transport on whose behalf this poller operates.
        BasicTransport* t;
        DISALLOW_COPY_AND_ASSIGN(Poller);
    };

  PRIVATE:
    void checkTimeouts();
    void deleteClientRpc(ClientRpc* clientRpc);
    void deleteServerRpc(ServerRpc* serverRpc);
    uint32_t getRoundTripBytes(const ServiceLocator* locator);
    void handlePacket(Driver::Received* received);
    static string headerToString(const void* header, uint32_t headerLength);
    static string opcodeSymbol(uint8_t opcode);
    uint32_t sendBytes(const Driver::Address* address, RpcId rpcId,
            Buffer* message, uint32_t offset, uint32_t maxBytes,
            uint32_t unscheduedBytes, uint8_t flags, bool partialOK = false);
    template<typename T>
    void sendControlPacket(const Driver::Address* recipient, const T* packet);
    uint32_t tryToTransmitData();
    void updateTopOutgoingMessageSet(OutgoingMessage* candidate,
            bool newMessage);

    /// Shared RAMCloud information.
    Context* context;

    /// The Driver used to send and receive packets.
    Driver* driver;

    /// Service locator string of this transport.
    string locatorString;

    /// Allows us to get invoked during the dispatch polling loop.
    Poller poller;

    /// Maximum # bytes of message data that can fit in one packet.
    uint32_t maxDataPerPacket;

    /// Unique identifier for this client (used to provide unique
    /// identification for RPCs).
    uint64_t clientId;

    /// The sequence number to use in the next outgoing RPC (i.e., one
    /// higher than the highest number ever used in the past).
    uint64_t nextClientSequenceNumber;

    /// The sequence number to use for the next incoming RPC (i.e., one
    /// higher than the highest number ever used in the past).
    uint64_t nextServerSequenceNumber;

    /// Holds incoming packets received from the driver. This is only
    /// used temporarily during the poll method, but it's allocated here
    /// so that we only pay the cost for storage allocation once (don't
    /// want to reallocate space in every call to poll). Always empty,
    /// except when the poll method is executing.
    std::vector<Driver::Received> receivedPackets;

    /// Holds incoming messages we are about to grant. Always empty, except
    /// when the poll method is receiving and processing incoming packets.
    std::vector<ScheduledMessage*> grantRecipients;

    /// Holds message buffers that are consist of payloads that are retained
    /// and assembled by the MessageAccumulator. These retained payloads are
    /// gradually released in the poll method so that the MessageAccumulator
    /// doesn't have to release all its payloads at one shot in the destructor
    /// and cause significant jitter.
    std::vector<MessageAccumulator::MessageBuffer*> messageBuffers;

    /// Pool allocator for our ServerRpc objects.
    ServerRpcPool<ServerRpc> serverRpcPool;

    /// Pool allocator for ClientRpc objects.
    ObjectPool<ClientRpc> clientRpcPool;

    /// Holds RPCs for which we are the client, and for which a
    /// response has not yet been completely received (we have sent
    /// at least part of the request, but not necessarily the entire
    /// request yet). Keys are RPC sequence numbers. Note: as of
    /// 10/2015, maps are faster than unordered_maps if they hold
    /// fewer than about 20 objects.
    typedef std::map<uint64_t, ClientRpc*> ClientRpcMap;
    ClientRpcMap outgoingRpcs;
    // TODO: The way we are uing this map is quite inefficient!!!
    // (e.g. frequently iterating the entire map) especially when there is
    // a large number of outstanding RPCs

    /// Holds RPCs for which we are the client, and for which the
    /// request message has not yet been completely transmitted (once
    /// the last byte of the request has been transmitted for the first
    /// time, the RPC is removed from this map). An RPC may be in both
    /// this list and outgoingRpcs simultaneously.
    INTRUSIVE_LIST_TYPEDEF(ClientRpc, outgoingRequestLinks)
            OutgoingRequestList;
    OutgoingRequestList outgoingRequests;

    /// A relatively small set of the sender's top K outgoing messages with
    /// fewest bytes left. We keep this as a separate set so that the sender
    /// doesn't have to consider all the outgoing messages in the common case.
    /// K is dynamically adjusted based on the workload. The overall goal is
    /// to keep K as small as possible while still ensuring that the sender
    /// doesn't have to look outside this set when picking the next message
    /// to transmit. Every time the sender decides to transmit a message
    /// outside this set, we increment K by one.
    INTRUSIVE_LIST_TYPEDEF(OutgoingMessage, outgoingMessageLinks)
            OutgoingMessageList;
    OutgoingMessageList topOutgoingMessages;

    /// False means we know that no message outside t->topOutgoingMessages
    /// has grants available and there is no need to take the slow path in
    /// #tryToTransmitData.
    bool transmitDataSlowPath;

    /// An RPC is in this map if (a) is one for which we are the server,
    /// (b) at least one byte of the request message has been received, and
    /// (c) the last byte of the response message has not yet been passed
    /// to the driver.  Note: this map could get large if the server gets
    /// backed up, so that there are a large number of RPCs that have been
    /// received but haven't yet been assigned to worker threads.
    typedef std::unordered_map<RpcId, ServerRpc*, RpcId::Hasher> ServerRpcMap;
    ServerRpcMap incomingRpcs;

    /// Holds RPCs for which we are the server, and whose response is
    /// partially transmitted (the response is ready to be sent, but the
    /// last byte has not yet been sent). An RPC may be in both
    /// this list and incomingRpcs simultaneously.
    INTRUSIVE_LIST_TYPEDEF(ServerRpc, outgoingResponseLinks)
            OutgoingResponseList;
    OutgoingResponseList outgoingResponses;

    /// Subset of the objects in incomingRpcs that require monitoring by
    /// the timer. We keep this as a separate list so that the timer doesn't
    /// have to consider RPCs currently being executed (which could be a
    /// very large number if the server is overloaded).
    INTRUSIVE_LIST_TYPEDEF(ServerRpc, timerLinks) ServerTimerList;
    ServerTimerList serverTimerList;

    /// The number of bytes corresponding to a round-trip time between
    /// two machines.  This serves two purposes. First, senders may
    /// transmit this many initial bytes without receiving a GRANT; this
    /// hides the round-trip time for receiving a GRANT, thereby minimizing
    /// latency. Second, the receiver uses this to pace GRANT messages (it
    /// tries to keep at least this many bytes of unreceived data granted
    /// at all times, in order to utilize the full network bandwidth).
    uint32_t roundTripBytes;
    // TODO: need to clean up the use of this field thoroughly

    /// How many bytes to extend the granted range in each new GRANT;
    /// a larger value avoids the overhead of sending and receiving
    /// GRANTS, but it can result in additional buffering in the network.
    uint32_t grantIncrement;

    /// Specifies the interval between calls to checkTimeouts, in units
    /// of rdtsc ticks.
    uint64_t timerInterval;

    /// At this Cycles::rdtsc time we'll start looking for a convenient
    /// opportunity to call checkTimeouts.
    uint64_t nextTimeoutCheck;

    /// At this Cycles::rdtsc time we'll call checkTimeouts even if it's
    /// not convenient. 0 means we haven't yet set a deadline (because we
    /// haven't reached nextTimeoutCheck).
    uint64_t timeoutCheckDeadline;

    /// If either client or server experiences this many timer wakeups without
    /// receiving any packets from the other end, then it will abort the
    /// request.
    uint32_t timeoutIntervals;

    /// If a client experiences this many timer wakeups without receiving
    /// any packets from the server for particular RPC, then it sends a
    /// RESEND request, assuming the response was lost.
    uint32_t pingIntervals;

    //--------------------
    // Performance Monitor
    //--------------------

    /// The beginning of the current monitoring interval, in units of
    /// rdtsc ticks.
    uint64_t lastMeasureTime;

    /// The value of `PerfStats::activeDispatchCycles` at `lastMeasureTime`.
    uint64_t lastDispatchActiveCycles;

    /// The start time of the last call to Dispatch::poll where
    /// `numTimesGrantRunDry` increments.
    uint64_t lastTimeGrantRunDry;

    /// `monitorMillis` in units of rdtsc ticks.
    uint64_t monitorInterval;

    /// Specifies the period over which to log the performance metrics,
    /// in milliseconds.
    uint32_t monitorMillis;

    /// # total packets received and processed in the current interval.
    uint32_t numPacketsReceived;

    /// # data packets received and processed in the current interval.
    uint32_t numDataPacketsReceived;

    /// # total control packets transmitted in the current interval.
    uint32_t numControlPacketsSent;

    /// # data packets transmitted in the current interval.
    uint32_t numDataPacketsSent;

    /// # times, in the current interval, we cannot transmit any message
    /// because we are waiting for GRANTs and the transmit queue has run
    /// dry.
    uint32_t numTimesGrantRunDry;

    /// Total # control bytes transmitted in the current interval.
    uint32_t outputControlBytes;

    /// Total # data bytes (excluding packet headers) transmitted in the
    /// current interval.
    uint32_t outputDataBytes;

    /// Total # retransmitted data bytes (excluding packet headers) in the
    /// current interval.
    uint32_t outputResentBytes;

    /// # performance monitor intervals we have experienced.
    uint64_t perfMonitorIntervals;

    // TODO
    uint64_t processPacketCycles;

    uint64_t timeoutCheckCycles;

    uint64_t transmitDataCycles;

    uint64_t transmitGrantCycles;

    /// # times we have to look outside of t->topOutgoingMessages in order to
    /// find a message to transmit in #tryToTransmitData.
    uint32_t tryToTransmitDataCacheMisses;

    /// Total # idle rdtsc ticks of the NIC's transmit queue in the
    /// current interval.
    uint64_t unusedBandwidth;

    DISALLOW_COPY_AND_ASSIGN(BasicTransport);
};

}  // namespace RAMCloud

#endif // RAMCLOUD_BASICTRANSPORT_H
