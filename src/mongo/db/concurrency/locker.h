/**
 *    Copyright (C) 2014 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#pragma once

#include <climits> // For UINT_MAX
#include <vector>

#include "mongo/base/disallow_copying.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/db/concurrency/d_concurrency.h"
#include "mongo/db/concurrency/lock_mgr_new.h"


namespace mongo {
    
    /**
     * Interface for acquiring locks. One of those objects will have to be instantiated for each
     * request (transaction).
     *
     * Lock/unlock methods must always be called from a single thread.
     */
    class Locker {
        MONGO_DISALLOW_COPYING(Locker);
    public:
        virtual ~Locker() {}

        virtual uint64_t getId() const = 0;

        /**
         * This should be the first method invoked for a particular Locker object. It acquires the
         * Global lock in the specified mode and effectively indicates the mode of the operation.
         * This is what the lock modes on the global lock mean:
         *
         * IX - Regular write operation
         * IS - Regular read operation
         * S  - Stops all *write* activity. Used for administrative operations (repl, etc).
         * X  - Stops all activity. Used for administrative operations (repl state changes,
         *          shutdown, etc).
         *
         * This method can be called recursively, but each call to beginTransaction must be
         * accompanied by a call to endTransaction.
         *
         * @param mode Mode in which the global lock should be acquired. Also indicates the intent
         *              of the operation.
         * @param timeoutMs How long to wait for the global lock (and the flush lock, for the MMAP
         *          V1 engine) to be acquired.
         *
         * @return LOCK_OK, if the global lock (and the flush lock, for the MMAP V1 engine) were
         *          acquired within the specified time bound. Otherwise, the respective failure
         *          code and neither lock will be acquired.
         */
        virtual newlm::LockResult lockGlobal(newlm::LockMode mode,
                                             unsigned timeoutMs = UINT_MAX) = 0;

        /**
         * Decrements the reference count on the global lock.  If the reference count on the
         * global lock hits zero, the transaction is over, and unlockAll unlocks all other locks.
         *
         * @return true if this is the last endTransaction call (i.e., the global lock was
         *          released); false if there are still references on the global lock. This value
         *          should not be relied on and is only used for assertion purposes.
         *
         * @return false if the global lock is still held.
         */
        virtual bool unlockAll() = 0;

        /**
         * This is only necessary for the MMAP V1 engine and in particular, the fsyncLock command
         * which needs to first acquire the global lock in X-mode for truncating the journal and
         * then downgrade to S before it blocks.
         *
         * The downgrade is necessary in order to be nice and not block readers while under
         * fsyncLock.
         */
        virtual void downgradeGlobalXtoSForMMAPV1() = 0;

        /**
         * beginWriteUnitOfWork/endWriteUnitOfWork must only be called by WriteUnitOfWork. See
         * comments there for the semantics of units of work.
         */
        virtual void beginWriteUnitOfWork() = 0;
        virtual void endWriteUnitOfWork() = 0;

        virtual bool inAWriteUnitOfWork() const = 0;

        /**
         * Acquires lock on the specified resource in the specified mode and returns the outcome
         * of the operation. See the details for LockResult for more information on what the
         * different results mean.
         *
         * Acquiring the same resource twice increments the reference count of the lock so each
         * call to lock, which doesn't time out (return value LOCK_TIMEOUT) must be matched with a
         * corresponding call to unlock.
         *
         * @param resId Id of the resource to be locked.
         * @param mode Mode in which the resource should be locked. Lock upgrades are allowed.
         * @param timeoutMs How many milliseconds to wait for the lock to be granted, before
         *              returning LOCK_TIMEOUT. This parameter defaults to UINT_MAX, which means
         *              wait infinitely. If 0 is passed, the request will return immediately, if
         *              the request could not be granted right away.
         *
         * @return All LockResults except for LOCK_WAITING, because it blocks.
         */
        virtual newlm::LockResult lock(const newlm::ResourceId& resId,
                                       newlm::LockMode mode,
                                       unsigned timeoutMs = UINT_MAX) = 0;

        /**
         * Releases a lock previously acquired through a lock call. It is an error to try to
         * release lock which has not been previously acquired (invariant violation).
         *
         * @return true if the lock was actually released; false if only the reference count was 
         *              decremented, but the lock is still held.
         */
        virtual bool unlock(const newlm::ResourceId& resId) = 0;

        /**
         * Retrieves the mode in which a lock is held or checks whether the lock held for a
         * particular resource covers the specified mode.
         *
         * For example isLockHeldForMode will return true for MODE_S, if MODE_X is already held,
         * because MODE_X covers MODE_S.
         */
        virtual newlm::LockMode getLockMode(const newlm::ResourceId& resId) const = 0;
        virtual bool isLockHeldForMode(const newlm::ResourceId& resId,
                                       newlm::LockMode mode) const = 0;

        /**
         * LockSnapshot captures the state of all resources that are locked, what modes they're
         * locked in, and how many times they've been locked in that mode.
         */
        struct LockSnapshot {
            // The global lock is handled differently from all other locks.
            newlm::LockMode globalMode;

            // One can acquire the global lock repeatedly.
            unsigned globalRecursiveCount;

            struct OneLock {
                // What lock resource is held?
                newlm::ResourceId resourceId;

                // In what mode is it held?
                newlm::LockMode mode;

                // What's the recursive count of this lock?  Note that we don't store any state
                // about how we got this lock (eg upgrade), just how many times we've locked it
                // in this mode.
                unsigned recursiveCount;
            };

            // The non-global non-flush locks held, sorted by granularity.  That is, locks[i] is
            // coarser or as coarse as locks[i + 1].
            std::vector<OneLock> locks;
        };

        /**
         * Retrieves all locks held by this transaction, and what mode they're held in.
         * Stores these locks in 'stateOut', destroying any previous state.  Unlocks all locks
         * held by this transaction.  This functionality is used for yielding in the MMAPV1
         * storage engine.  MMAPV1 uses voluntary/cooperative lock release and reacquisition
         * in order to allow for interleaving of otherwise conflicting long-running operations.
         *
         * This functionality is also used for releasing locks on databases and collections
         * when cursors are dormant and waiting for a getMore request.
         *
         * Returns true if locks are released.  It is expected that restoreLockState will be called
         * in the future.
         *
         * Returns false if locks are not released.  restoreLockState(...) does not need to be
         * called in this case.
         */
        virtual bool saveLockStateAndUnlock(LockSnapshot* stateOut) = 0;

        /**
         * Re-locks all locks whose state was stored in 'stateToRestore'.
         */
        virtual void restoreLockState(const LockSnapshot& stateToRestore) = 0;

        //
        // These methods are legacy from LockState and will eventually go away or be converted to
        // calls into the Locker methods
        //

        virtual void dump() const = 0;

        virtual BSONObj reportState() = 0;
        virtual void reportState(BSONObjBuilder* b) = 0;

        virtual unsigned recursiveCount() const = 0;

        virtual bool isW() const = 0;
        virtual bool isR() const = 0;
        virtual bool hasAnyReadLock() const = 0; // explicitly r or R

        virtual bool isLocked() const = 0;
        virtual bool isWriteLocked() const = 0;
        virtual bool isWriteLocked(const StringData& ns) const = 0;
        virtual bool isAtLeastReadLocked(const StringData& ns) const = 0;
        virtual bool isRecursive() const = 0;

        virtual void assertWriteLocked(const StringData& ns) const = 0;
        virtual void assertAtLeastReadLocked(const StringData& ns) const = 0;

        /** pending means we are currently trying to get a lock */
        virtual bool hasLockPending() const = 0;

        // ----

        // Those are only used for TempRelease. Eventually they should be removed.
        virtual void enterScopedLock(Lock::ScopedLock* lock) = 0;
        virtual Lock::ScopedLock* getCurrentScopedLock() const = 0;
        virtual void leaveScopedLock(Lock::ScopedLock* lock) = 0;

        virtual void recordLockTime() = 0;
        virtual void resetLockTime() = 0;

        // Used for the replication parallel log op application threads
        virtual void setIsBatchWriter(bool newValue) = 0;
        virtual bool isBatchWriter() const = 0;
        virtual void setLockPendingParallelWriter(bool newValue) = 0;

    protected:
        Locker() { }
    };

} // namespace mongo
