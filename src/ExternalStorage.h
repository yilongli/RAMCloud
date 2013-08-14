/* Copyright (c) 2013 Stanford University
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

#ifndef RAMCLOUD_EXTERNALSTORAGE_H
#define RAMCLOUD_EXTERNALSTORAGE_H

#include "Buffer.h"

namespace RAMCloud {

/**
 * This class defines an interface for storing data durably in a separate
 * key-value store with a hierarchical name space.  An object of this class
 * represents a connection to a particular storage system, such as ZooKeeper.
 * This class is used by the coordinator to store configuration data that
 * must survive coordinator crashes, such as information about cluster
 * membership and tablet locations. This class is also used for leader
 * election (ensuring that there is exactly one server acting as coordinator
 * at a time).
 *
 * The storage provided by this class is expected to be highly durable
 * and available (for example, it might be implemented using a consensus
 * approach with multiple storage servers). No recoverable exceptions or
 * errors should be thrown by this class; the only exceptions thrown should
 * be unrecoverable once such as LostLeadershipException or internal
 * errors that should result in coordinator crashes. The storage system is
 * expected to be totally reliable; the worst that should happen is for
 * calls to this class to block while waiting for the storage system to
 * come back online.
 *
 * The goal of this interface is to support multiple implementations using
 * different storage systems; however, the interface was designed with
 * ZooKeeper in mind as the storage system.
 */
class ExternalStorage {
  PUBLIC:
    /**
     * The following structure holds information about a single object
     * including both its name and its value. Its primary use is
     * for enumerating all of the children of a node.
     */
    struct Object {
        /// Name of the object within its parent (not a full path name).
        /// NULL-terminated. Dynamically-allocated, freed on destruction.
        char* name;

        /// Value of the object (not necessarily NULL-terminated). NULL
        /// means there is no value for this name (it is a pure node).
        /// If non-NULL, will be freed on destruction.
        char* value;

        /// Length of value, in bytes.
        int length;

        Object(char* name, char* value, int length);
        ~Object();

        // The following constructors are provided so that Objects can
        // be used in std::vectors.
        Object()
            : name(NULL)
            , value(NULL)
            , length(0)
        {}
        Object(const Object& other)
            : name(other.name)
            , value(other.value)
            , length(other.length)
        {
            Object& o = const_cast<Object&>(other);
            o.name = NULL;
            o.value = NULL;
            o.length = 0;
        }
        Object& operator=(const Object& other)
        {
            if (this != &other) {
                Object& o = const_cast<Object&>(other);
                std::swap(name, o.name);
                std::swap(value, o.value);
                std::swap(length, o.length);
            }
            return *this;
        }
    };

    /**
     * This exception is thrown if we lose leadership (i.e. some other
     * server decided that we are dead, so it took over as coordinator)
     * or if operations are attempted before we ever became leader.
     * When this exception is thrown, the coordinator must either exit
     * or reset all of its state and do nothing until it becomes leader
     * again.
     */
    struct NotLeaderException : public Exception {
        explicit NotLeaderException(const CodeLocation& where)
            : Exception(where) {}
    };

    /**
     * This enum is passed as an argument to the "set" method as a hint
     * to indicate whether or not the object already exists.
     */
    enum Hint {
        CREATE,                    // A new object is being created.
        UPDATE                     // An existing object is being overwritten.
    };

    ExternalStorage() {}
    virtual ~ExternalStorage() {}

    /**
     * This method is invoked when a server first starts up.  At this point
     * the server is in "backup" mode, under the assumption that some other
     * server acting as leader. This method waits until there is no longer
     * an active leader; at that point it claims leadership and returns.
     * Once this method returns, the caller can begin acting as coordinator.
     *
     *  \param name
     *      Name of an object to use for coordinating leadership: a
     *      NULL-terminated hierarchical path containing one or more path
     *      elements separated by slashes, such as "foo" or "foo/bar".
     *  \param leaderInfo
     *      Information about this server (e.g., service locator that clients
     *      can use to connect), which will be stored in the object given
     *      by "name" once we become leader.
     */
    virtual void becomeLeader(const char* name, const string* leaderInfo) = 0;

    /**
     * Read one object from external storage.
     *
     * \param name
     *      Name of the desired object; NULL-terminated hierarchical path
     *      containing one or more path elements separated by slashes,
     *      such as "foo" or "foo/bar".
     * \param value
     *      The contents of the object are returned in this buffer.
     *
     * \return
     *      If the specified object exists, then true is returned. If there
     *      is no such object, then false is returned and value is empty.
     *
     * \throws LostLeadershipException
     */
    virtual bool get(const char* name, Buffer* value) = 0;

    /**
     * Return information about all of the children of a node.
     *
     * \param name
     *      Name of the node whose children should be enumerated.
     *      NULL-terminated hierarchical path.
     * \param children
     *      This vector will be filled in with one entry for each child
     *      of name. Any previous contents of the vector are discarded.
     */
    virtual void getChildren(const char* name, vector<Object>* children) = 0;

    /**
     * Remove an object, if it exists. If it doesn't exist, do nothing.
     * If name has children, all of the children are removed recursively,
     * followed by name.
     *
     * \param name
     *      Name of the object to remove; NULL-terminated hierarchical path
     *      containing one or more path elements separated by slashes,
     *      such as "foo" or "foo/bar".
     *
     * \throws LostLeadershipException
     */
    virtual void remove(const char* name) = 0;

    /**
     * Set the value of a particular object in external storage. If the
     * object did not previously exist, then it is created; otherwise
     * its existing value is overwritten. If the parent node of the object
     * does not exist, it will be created automatically.
     *
     * \param flavor
     *      Either Hint::CREATE or Hint::UPDATE: indicates whether or not
     *      the object (most likely) already exists. This argument does not
     *      affect the outcome, but if specified incorrectly it may impact
     *      performance on some storage systems (e.g., ZooKeeper has separate
     *      "create" and "update" operations, so we may have to try both to
     *      find one that works).
     * \param name
     *      Name of the desired object; NULL-terminated hierarchical path
     *      containing one or more path elements separated by slashes,
     *      such as "foo" or "foo/bar".
     * \param value
     *      Address of first byte of value to store for this object.
     * \param valueLength
     *      Size of value, in bytes. If less than zero, then value
     *      must be a NULL-terminated string, and the length will be computed
     *      automatically to include all the characters in the string and
     *      the terminating NULL character.
     *
     * \throws LostLeadershipException
     */
    virtual void set(Hint flavor, const char* name, const char* value,
            int valueLength = -1) = 0;

    /**
     * This method is called to modified by the current leader to modified
     * information about it that is kept in external storage (this is the
     * same information that was originally set when becomeLeader was
     * invoked). This information is not immediately propagated to the
     * external server; it will be written to the server during the next
     * operation that renews the leader's lease.
     *
     *  \param leaderInfo
     *      Information about this server (e.g., service locator that clients
     *      can use to connect), which will eventually be stored in the object
     *      specified by the name argument to becomeLeader.
     */
    virtual void setLeaderInfo(const string* leaderInfo) = 0;

    DISALLOW_COPY_AND_ASSIGN(ExternalStorage);
};

} // namespace RAMCloud

#endif // RAMCLOUD_EXTERNALSTORAGE_H
