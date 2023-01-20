// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;

namespace FASTER.core
{
    /// <summary>
    /// Manual-enabled (both manual and ephemeral) LockTable interface definition
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    internal interface ILockTable<TKey> : IDisposable
    {
        /// <summary>
        /// Try to acquire a manual lock for the key.
        /// </summary>
        /// <param name="key">The key to lock</param>
        /// <param name="hei">The hash table entry info of the key to lock</param>
        /// <param name="lockType">The lock type to acquire</param>
        /// <returns>True if the lock was acquired; false if lock acquisition failed</returns>
        /// <remarks>There are no variations of this call specific to Shared vs. Exclusive, because this is
        ///     called only from InternalLock, which takes the <paramref name="lockType"/> argument.</remarks>
        public bool TryLockManual(ref TKey key, ref HashEntryInfo hei, LockType lockType);

        /// <summary>
        /// Try to acquire a <paramref name="lockType"/> ephemeral lock for the key. 
        /// </summary>
        /// <param name="key">The key to lock</param>
        /// <param name="hei">The hash table entry info of the key to lock</param>
        /// <param name="lockType">The lock type to acquire--shared or exclusive</param>
        public bool TryLockEphemeral(ref TKey key, ref HashEntryInfo hei, LockType lockType);

        /// <summary>
        /// Try to acquire a shared ephemeral lock for the key. 
        /// </summary>
        /// <param name="key">The key to lock</param>
        /// <param name="hei">The hash table entry info of the key to lock</param>
        public bool TryLockEphemeralShared(ref TKey key, ref HashEntryInfo hei);

        /// <summary>
        /// Try to acquire an exclusive ephemeral lock for the key.
        /// </summary>
        /// <param name="key">The key to lock</param>
        /// <param name="hei">The hash table entry info of the key to lock</param>
        public bool TryLockEphemeralExclusive(ref TKey key, ref HashEntryInfo hei);

        /// <summary>
        /// Release the <paramref name="lockType"/> lock on the key.
        /// </summary>
        /// <param name="key">The key to unlock</param>
        /// <param name="hei">The hash table entry info of the key to lock</param>
        /// <param name="lockType">The lock type to release--shared or exclusive</param>
        public void Unlock(ref TKey key, ref HashEntryInfo hei, LockType lockType);

        /// <summary>
        /// Release a shared lock on the key.
        /// </summary>
        /// <param name="key">The key to unlock</param>
        /// <param name="hei">The hash table entry info of the key to lock</param>
        public void UnlockShared(ref TKey key, ref HashEntryInfo hei);

        /// <summary>
        /// Release an exclusive lock on the key.
        /// </summary>
        /// <param name="key">The key to unlock</param>
        /// <param name="hei">The hash table entry info of the key to lock</param>
        public void UnlockExclusive(ref TKey key, ref HashEntryInfo hei);

        /// <summary>
        /// Return whether the key is S locked
        /// </summary>
        public bool IsLockedShared(ref TKey key, ref HashEntryInfo hei);

        /// <summary>
        /// Return whether the keyrecord is X locked
        /// </summary>
        public bool IsLockedExclusive(ref TKey key, ref HashEntryInfo hei);

        /// <summary>
        /// Return whether an the key is S or X locked
        /// </summary>
        public bool IsLocked(ref TKey key, ref HashEntryInfo hei);

        /// <summary>
        /// Return the Lock state of the key.
        /// </summary>
        public LockState GetLockState(ref TKey key, ref HashEntryInfo hei);
    }
}
