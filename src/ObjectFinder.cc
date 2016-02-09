/* Copyright (c) 2010-2016 Stanford University
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

#include "Cycles.h"
#include "Dispatch.h"
#include "IndexKey.h"
#include "ObjectFinder.h"
#include "FailSession.h"

namespace RAMCloud {

/**
 * The implementation of ObjectFinder::TableConfigFetcher that is used for
 * normal execution. This class is not thread-safe; requests to the class
 * must be serialized externally.
 */
class RealTableConfigFetcher : public ObjectFinder::TableConfigFetcher {
  public:
    explicit RealTableConfigFetcher(Context* context)
        : context(context)
        , outstandingRpcs()
    {}

    ~RealTableConfigFetcher() {
        for (auto& kv : outstandingRpcs) {
            delete kv.second;
        }
    }

    /**
     * Attempts to retrieve the configuration information of a table from the
     * coordinator.
     *
     * \param tableId
     *      The id of the table whose tablet configuration is to be retrieved.
     * \return
     *      A reference to the configuration information; NULL means the result
     *      is not yet available.  The caller is responsible to free the
     *      resources when its done.
     */
    ProtoBuf::TableConfig*
    tryGetTableConfig(uint64_t tableId) {
        GetTableConfigRpc* rpc;
        auto iter = outstandingRpcs.find(tableId);
        if (iter == outstandingRpcs.end()) {
            rpc = new GetTableConfigRpc(context, tableId);
            outstandingRpcs.emplace(tableId, rpc);
        } else {
            rpc = iter->second;
        }

        if (context->dispatch->isDispatchThread()) {
            context->dispatch->poll();
        }
        if (!rpc->isReady()) {
            return NULL;
        }

        // When an RPC is ready, acquire its ownership with a unique_ptr and
        // then erase the corresponding entry; if rpc->wait() later throws
        // exception, the RPC object will be deleted properly
        auto readyRpc = std::unique_ptr<GetTableConfigRpc>(rpc);
        outstandingRpcs.erase(tableId);

        ProtoBuf::TableConfig* tableConfig = new ProtoBuf::TableConfig();
        rpc->wait(tableConfig);
        return tableConfig;
    }

  private:
    Context* const context;

    /// A cache of the outstanding RPCs indexed by their requesting table ids.
    std::unordered_map<uint64_t, GetTableConfigRpc*> outstandingRpcs;

    DISALLOW_COPY_AND_ASSIGN(RealTableConfigFetcher);
};

/**
 * Constructor.
 * \param context
 *      Overall information about this client.
 */
ObjectFinder::ObjectFinder(Context* context)
    : context(context)
    , mutex("ObjectFinder")
    , tableMap()
    , tableIndexMap()
    , tableConfigFetcher(new RealTableConfigFetcher(context))
{
}

/**
 * This method deletes all cached information, restoring the object
 * to its original pristine state. It's used primarily to force cached
 * Session objects to be released during RAMCloud shutdown to avoid
 * order-of-destruction problems where a transport could be deleted before
 * all of its sessions.
 */
void ObjectFinder::reset()
{
    SpinLock::Guard _(mutex);
    tableMap.clear();
    tableIndexMap.clear();
}

/**
 * This method is invoked when the caller has reason to believe that
 * the configuration information for particular table is out-of-date.
 * The method deletes all information related to that table; fresh
 * information will be fetched from the coordinator the next time it
 * is needed.
 * \param tableId
 *      The id of the table to be flushed.
 */
void
ObjectFinder::flush(uint64_t tableId)
{
    SpinLock::Guard _(mutex);
    flushImpl(tableId);
}

/**
 * The actual implementation code of flush(); factored out so that other
 * methods in this class can invoke it without acquiring the mutex.
 */
void
ObjectFinder::flushImpl(uint64_t tableId)
{
    TabletKey start {tableId, 0U};
    TabletKey end {tableId, std::numeric_limits<KeyHash>::max()};
    TabletIter lower = tableMap.lower_bound(start);
    TabletIter upper = tableMap.upper_bound(end);
    tableMap.erase(lower, upper);

    IndexletIter indexLower = tableIndexMap.lower_bound
            (std::make_pair(tableId, 0));
    IndexletIter indexUpper = tableIndexMap.upper_bound(
            std::make_pair(tableId, std::numeric_limits<uint8_t>::max()));
    tableIndexMap.erase(indexLower, indexUpper);
}

/**
 * Return a string representation of all the table id's presented
 * at the tableMap at any given moment. Used mainly for testing.
 *
 * \return
 *      A human-readable string describing the contents of tableMap.
 */
string
ObjectFinder::debugString() const
{
    SpinLock::Guard _(mutex);
    std::map<TabletKey, TabletWithLocator>::const_iterator it;
    std::stringstream result;
    for (it = tableMap.begin(); it != tableMap.end(); it++) {
           if (it != tableMap.begin()) {
                result << ", ";
           }
           result << "{{tableId : " << it->first.tableId
                  << ", keyHash : " << it->first.keyHash
                  << "}, "
                  << "{start_key_hash : " << it->second.tablet.startKeyHash
                  << ", end_key_hash : " << it->second.tablet.endKeyHash
                  << ", state : " << it->second.tablet.status
                  << "}}";
    }
    return result.str();
}

/**
 * Find information about the tablet containing a key in a given table.
 *
 * \deprecated
 *      All uses of this method should be converted to use its asynchronous
 *      counterpart tryLookup() eventually.
 *
 * \param tableId
 *      The table containing the desired object
 * \param key
 *      Variable length key that uniquely identifies the object within tableId.
 *      It does not necessarily have to be null terminated like a string.
 * \param keyLength
 *      Size in bytes of the key.
 * \return
 *      Session for communication with the server who holds the tablet.
 * \post
 *      The return value is not a NULL session.
 *
 * \throw TableDoesntExistException
 *      The coordinator has no record of the table.
 */
Transport::SessionRef
ObjectFinder::lookup(uint64_t tableId, const void* key, KeyLength keyLength)
{
    // No lock needed: doesn't access ObjectFinder object.
    KeyHash keyHash = Key::getHash(tableId, key, keyLength);
    return lookup(tableId, keyHash);
}

/**
 * Lookup the master for a key hash in a given table. Useful for
 * looking up a key hash range in the table when you do not have a
 * specific key.
 *
 * \deprecated
 *      All uses of this method should be converted to use its asynchronous
 *      counterpart tryLookup() eventually.
 *
 * \param tableId
 *      The table containing the desired object.
 * \param keyHash
 *      A hash value in the space of key hashes.
 * \return
 *      Session for communication with the server who holds the tablet.
 * \post
 *      The return value is not a NULL session.
 *
 * \throw TableDoesntExistException
 *      The coordinator has no record of the table.
 */
Transport::SessionRef
ObjectFinder::lookup(uint64_t tableId, KeyHash keyHash)
{
    // No lock needed: doesn't access ObjectFinder object.
    Transport::SessionRef session;
    while (true) {
        session = tryLookup(tableId, keyHash);
        if (session) return session;
    }
}

/**
 * Lookup the master for a particular key hash in a given table.
 * Only used internally in lookup() and for testing/debugging routines.
 *
 * \deprecated
 *      All uses of this method should be converted to use its asynchronous
 *      counterpart tryLookupTablet() eventually.
 *
 * \param tableId
 *      The table containing the desired object (return value from a
 *      previous call to getTableId).
 * \param keyHash
 *      A hash value in the space of key hashes.
 * \return
 *      Reference to a tablet with the details of the server that owns
 *      the specified key. This reference may be invalidated by any future
 *      calls to the ObjectFinder.
 *
 * \throw TableDoesntExistException
 *      The coordinator has no record of the table.
 */
TabletWithLocator*
ObjectFinder::lookupTablet(uint64_t tableId, KeyHash keyHash)
{
    // No lock needed: doesn't access ObjectFinder object.
    TabletWithLocator* tabletWithLocator;
    while (true) {
        tabletWithLocator = tryLookupTablet(tableId, keyHash);
        if (tabletWithLocator) return tabletWithLocator;
    }
}

/**
 * Delete the session connecting to the master that owns a particular
 * object, if such a session exists. This method is typically invoked after
 * an unrecoverable error has occurred on the session, so that a new
 * session will be created the next time someone wants to communicate
 * with that master.
 *
 * \param tableId
 *      The table containing the desired object (return value from a
 *      previous call to getTableId).
 * \param keyHash
 *      Hash value corresponding to a particular object in table.
 */
void
ObjectFinder::flushSession(uint64_t tableId, KeyHash keyHash)
{
    SpinLock::Guard _(mutex);
    TabletKey key{tableId, keyHash};
    TabletWithLocator* tabletWithLocator = lookupTabletInCache(&key);
    if (tabletWithLocator != NULL) {
        context->transportManager->flushSession(
                tabletWithLocator->serviceLocator);
        tabletWithLocator->session.reset(NULL);
    }
}

/**
 * Delete the session connecting to the master that owns a particular
 * index entry, if such a session exists. This method is typically invoked
 * after an unrecoverable error has occurred on the session, so that a new
 * session will be created the next time someone wants to communicate
 * with that master.
 *
 * \param tableId
 *      The table containing the desired object.
 * \param indexId
 *      Id of a particular index in tableId.
 * \param key
 *      Blob corresponding to the key.
 * \param keyLength
 *      Length of key.
 */
void
ObjectFinder::flushSession(uint64_t tableId, uint8_t indexId,
                           const void* key, KeyLength keyLength)
{
    SpinLock::Guard _(mutex);
    IndexletWithLocator* indexletWithLocator = lookupIndexletInCache(
            tableId, indexId, key, keyLength);
    if (indexletWithLocator != NULL) {
        context->transportManager->flushSession(
                indexletWithLocator->serviceLocator);
        indexletWithLocator->session.reset(NULL);
    }
}

/**
 * Lookup the master for a particular indexlet in the local cache of
 * configuration information.
 *
 * \param tableId
 *      The table containing the desired object.
 * \param indexId
 *      Id of a particular index in tableId.
 * \param key
 *      Blob corresponding to the key.
 * \param keyLength
 *      Length of key.
 *
 * \return
 *      Reference to Indexlet with the details of the server that owns
 *      the specified key. NULL means there is no information about such
 *      indexlet in the local cache.
 */
IndexletWithLocator*
ObjectFinder::lookupIndexletInCache(uint64_t tableId, uint8_t indexId,
                                    const void* key, KeyLength keyLength)
{
    // No lock needed: locked by the caller of this method
    mutex.assertAcquired();
    TableIdIndexIdPair indexKey {tableId, indexId};
    auto range = tableIndexMap.equal_range(indexKey);
    for (auto iter = range.first; iter != range.second; iter++) {
        IndexletWithLocator& indexletWithLocator = iter->second;
        Indexlet& indexlet = indexletWithLocator.indexlet;
        if (indexlet.firstKey != NULL &&
            IndexKey::keyCompare(key, keyLength,
                                 indexlet.firstKey,
                                 indexlet.firstKeyLength) < 0) {
            continue;
        }
        if (indexlet.firstNotOwnedKey != NULL &&
            IndexKey::keyCompare(key, keyLength,
                                 indexlet.firstNotOwnedKey,
                                 indexlet.firstNotOwnedKeyLength) >= 0)
        {
            continue;
        }
        return &indexletWithLocator;
    }
    return NULL;
}

/**
 * Lookup the master for a particular tablet in the local cache of
 * configuration information.
 *
 * \param key
 *      The tablet containing the desired object.
 * \return
 *      Reference to a tablet with the details of the server that owns
 *      the specified key. NULL means there is no information about such
 *      tablet in the local cache.
 */
TabletWithLocator*
ObjectFinder::lookupTabletInCache(const TabletKey* key)
{
    // No lock needed: locked by the caller of this method
    mutex.assertAcquired();
    TabletIter iter = tableMap.upper_bound(*key);
    if (!tableMap.empty() && iter != tableMap.begin()) {
        TabletWithLocator *tabletWithLocator = &((--iter)->second);
        if (tabletWithLocator->tablet.tableId == key->tableId &&
            tabletWithLocator->tablet.startKeyHash <= key->keyHash &&
            key->keyHash <= tabletWithLocator->tablet.endKeyHash) {

            return tabletWithLocator;
        }
    }
    return NULL;
}

/**
 * Find information about the tablet containing a key in a given table.
 *
 * \param tableId
 *      The table containing the desired object
 * \param key
 *      Variable length key that uniquely identifies the object within tableId.
 *      It does not necessarily have to be null terminated like a string.
 * \param keyLength
 *      Size in bytes of the key.
 * \return
 *      Session for communication with the server who holds the tablet. NULL
 *      session means the result is not available yet.
 *
 * \throw TableDoesntExistException
 *      The coordinator has no record of the table.
 */
Transport::SessionRef
ObjectFinder::tryLookup(uint64_t tableId, const void* key, KeyLength keyLength)
{
    // No lock needed: doesn't access ObjectFinder object.
    KeyHash keyHash = Key::getHash(tableId, key, keyLength);
    return tryLookup(tableId, keyHash);
}

/**
 * Lookup the master for a key hash in a given table. Useful for
 * looking up a key hash range in the table when you do not have a
 * specific key.
 *
 * \param tableId
 *      The table containing the desired object.
 * \param keyHash
 *      A hash value in the space of key hashes.
 * \return
 *      Session for communication with the server who holds the tablet.
 *      NULL session means the result is not available yet.
 *
 * \throw TableDoesntExistException
 *      The coordinator has no record of the table.
 */
Transport::SessionRef
ObjectFinder::tryLookup(uint64_t tableId, KeyHash keyHash)
{
    // No lock needed: doesn't access ObjectFinder object.
    TabletWithLocator* tabletWithLocator = tryLookupTablet(tableId, keyHash);
    if (tabletWithLocator != NULL) {
        if (!tabletWithLocator->session) {
            tabletWithLocator->session = context->transportManager->getSession(
                    tabletWithLocator->serviceLocator);
        }
        return tabletWithLocator->session;
    } else {
        return Transport::SessionRef();
    }
}

/**
 * Attempts to find the master holding the indexlet containing a given key.
 *
 * \param tableId
 *      The table containing the desired object.
 * \param indexId
 *      Id of a particular index in tableId.
 * \param key
 *      Blob corresponding to the key.
 * \param keyLength
 *      Length of key.
 * \param[out] indexletNotFound
 *      True if the coordinator has no record of the index.
 *
 * \return
 *      Session for communication with the server who holds the indexlet.
 *      NULL session means either the indexlet doesn't exist in the system,
 *      when indexletNotFound is set to true, or the result is not available
 *      yet.
 */
Transport::SessionRef
ObjectFinder::tryLookup(uint64_t tableId, uint8_t indexId,
                        const void* key, KeyLength keyLength,
                        bool& indexletNotFound)
{
    // No lock needed: doesn't access ObjectFinder object.
    IndexletWithLocator* indexletWithLocator = tryLookupIndexlet(
            tableId, indexId, key, keyLength, indexletNotFound);
    if (indexletWithLocator != NULL && !indexletNotFound) {
        if (!indexletWithLocator->session) {
            indexletWithLocator->session = context->transportManager->getSession(
                    indexletWithLocator->serviceLocator);
        }
        return indexletWithLocator->session;
    } else {
        return Transport::SessionRef();
    }
}

/**
 * Lookup the indexlet containing the given key.
 *
 * \param tableId
 *      The table containing the desired object.
 * \param indexId
 *      Id of a particular index in tableId.
 * \param key
 *      Blob corresponding to the key.
 * \param keyLength
 *      Length of key.
 * \param[out] indexletNotFound
 *      True if the coordinator has no record of the index.
 *
 * \return
 *      Reference to Indexlet with the details of the server that owns
 *      the specified key. This reference may be invalidated by any future
 *      calls to the ObjectFinder. NULL session means either the indexlet
 *      doesn't exist in the system, when indexletNotFound is set to true,
 *      or the result is not available yet.
 */
IndexletWithLocator*
ObjectFinder::tryLookupIndexlet(uint64_t tableId, uint8_t indexId,
                                const void* key, KeyLength keyLength,
                                bool& indexletNotFound)
{
    SpinLock::Guard _(mutex);
    indexletNotFound = false;
    IndexletWithLocator* indexletWithLocator = lookupIndexletInCache(
            tableId, indexId, key, keyLength);
    if (indexletWithLocator != NULL) {
        // TODO(YilongL): maybe we should check the status of the indexlet
        // to be normal here just like tryLookupTablet() if the recovering
        // information is available
        return indexletWithLocator;
    }

    // We do not have the latest information about the indexlet; try to fetch
    // it from the coordinator but do not block here
    if (!tryUpdateTableConfig(tableId)) {
        return NULL;
    }

    // The response of our last RPC to the coordinator has come back
    indexletWithLocator = lookupIndexletInCache(tableId, indexId,
                                                key, keyLength);
    if (indexletWithLocator == NULL) {
        indexletNotFound = true;
        return NULL;
    } else {
        return indexletWithLocator;
    }
}

/**
 * Lookup the master for a particular key hash in a given table.
 * Only used internally in tryLookup() and for testing/debugging routines.
 *
 * \param tableId
 *      The table containing the desired object (return value from a
 *      previous call to getTableId).
 * \param keyHash
 *      A hash value in the space of key hashes.
 * \return
 *      Reference to a tablet with the details of the server that owns
 *      the specified key. This reference may be invalidated by any future
 *      calls to the ObjectFinder. NULL means the result is not available
 *      yet.
 *
 * \throw TableDoesntExistException
 *      The coordinator has no record of the table.
 */
TabletWithLocator*
ObjectFinder::tryLookupTablet(uint64_t tableId, KeyHash keyHash)
{
    SpinLock::Guard _(mutex);
    // First lookup the tablet in our local cache
    TabletKey key{tableId, keyHash};
    TabletWithLocator* tabletWithLocator = lookupTabletInCache(&key);
    if (tabletWithLocator != NULL) {
        if (tabletWithLocator->tablet.status == Tablet::Status::NORMAL) {
            return tabletWithLocator;
        }

        if (Cycles::toMicroseconds(Cycles::rdtsc() -
                tabletWithLocator->recoveringStart) < 10000) {
            // Not long ago, the tablet was known to be under recovery; better
            // avoid stressing the coordinator even more
            return NULL;
        }
    }

    // We do not have the latest information about the tablet; try to fetch
    // it from the coordinator but do not block here
    if (!tryUpdateTableConfig(tableId)) {
        return NULL;
    }

    // The response of our last RPC to the coordinator has come back; we can
    // finally throw a TableDoesntExistException for sure if needed
    tabletWithLocator = lookupTabletInCache(&key);
    if (tabletWithLocator == NULL) {
        throw TableDoesntExistException(HERE);
    } else if (tabletWithLocator->tablet.status != Tablet::Status::NORMAL) {
        return NULL;
    } else {
        return tabletWithLocator;
    }
}

/**
 * Try to fetch the latest table configuration information from the coordinator and
 * update the local cache accordingly.
 *
 * \param tableId
 *      The id of the table whose configuration information is to be updated.
 * \return
 *      Whether the configuration information of the particular table has been
 *      updated.
 */
bool
ObjectFinder::tryUpdateTableConfig(uint64_t tableId)
{
    // No lock needed: locked by the caller of this method
    mutex.assertAcquired();
    flushImpl(tableId);
    ProtoBuf::TableConfig* tableConfig =
            tableConfigFetcher->tryGetTableConfig(tableId);
    if (tableConfig == NULL) {
        return false;
    }

    for (const auto& tablet : tableConfig->tablet()) {
        Tablet rawTablet(tableId,
                         tablet.start_key_hash(),
                         tablet.end_key_hash(),
                         ServerId(tablet.server_id()),
                         Tablet::Status(tablet.state()),
                         LogPosition(tablet.ctime_log_head_id(),
                                     tablet.ctime_log_head_offset()));

        tableMap.emplace(
                TabletKey{tableId, tablet.start_key_hash()},
                TabletWithLocator(rawTablet, tablet.service_locator()));
    }

    for (const auto& index : tableConfig->index()) {
        for (const auto& indexlet : index.indexlet()) {
            const string &startKey = indexlet.start_key();
            const string &endKey = indexlet.end_key();
            Indexlet rawIndexlet(startKey.c_str(),
                                 static_cast<KeyLength>(startKey.length()),
                                 endKey.c_str(),
                                 static_cast<KeyLength>(endKey.length()));
            IndexletWithLocator indexletWithLocator(
                    rawIndexlet, indexlet.service_locator());

            tableIndexMap.emplace(
                    std::make_pair(tableId, index.index_id()),
                    indexletWithLocator);
        }
    }

    delete tableConfig;
    return true;
}

/**
 * Flush the tablet map and refresh it until we detect that at least one tablet
 * has a state set to something other than normal.
 *
 * Used only by RecoveryMain.c to detect when the failure is detected by the
 * coordinator.
 */
void
ObjectFinder::waitForTabletDown(uint64_t tableId)
{
    RAMCLOUD_TEST_LOG("flushing object map");
    for (;;) {
        SpinLock::Guard _(mutex);
        while (!tryUpdateTableConfig(tableId)) {};
        TabletKey start {tableId, 0U};
        TabletKey end {tableId, std::numeric_limits<KeyHash>::max()};
        TabletIter lower = tableMap.lower_bound(start);
        TabletIter upper = tableMap.upper_bound(end);
        for (; lower != upper; ++lower) {
            const TabletWithLocator& tabletWithLocator = lower->second;
            if (tabletWithLocator.tablet.status != Tablet::Status::NORMAL)
            {
                return;
            }
        }
        usleep(200);
    }
}

/**
 * Flush information for a given table and refresh it until it is non-empty
 * and all of its tablets have normal status.
 *
 * Used for testing to detect when the recovery is complete.
 */
void
ObjectFinder::waitForAllTabletsNormal(uint64_t tableId, uint64_t timeoutNs)
{
    uint64_t start = Cycles::rdtsc();
    RAMCLOUD_TEST_LOG("flushing object map");
    while (Cycles::toNanoseconds(Cycles::rdtsc() - start) < timeoutNs) {
        SpinLock::Guard _(mutex);
        bool allNormal = true;
        while (!tryUpdateTableConfig(tableId)) {};
        TabletKey start {tableId, 0U};
        TabletKey end {tableId, std::numeric_limits<KeyHash>::max()};
        TabletIter lower = tableMap.lower_bound(start);
        TabletIter upper = tableMap.upper_bound(end);
        for (; lower != upper; ++lower) {
            const TabletWithLocator& tabletWithLocator = lower->second;
            if (tabletWithLocator.tablet.status != Tablet::Status::NORMAL) {
                allNormal = false;
                break;
            }
        if (allNormal && tableMap.size() > 0)
            return;
        }
        usleep(10000);
    }
}

} // namespace RAMCloud
