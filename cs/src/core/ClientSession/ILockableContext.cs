// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.core
{
    /// <summary>
    /// Lockable context functions. Useful when doing generic locking across diverse <see cref="LockableUnsafeContext{Key, Value, Input, Output, Context, Functions}"/> and <see cref="LockableContext{Key, Value, Input, Output, Context, Functions}"/> specializations.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    public interface ILockableContext<TKey>
    {
        /// <summary>
        /// Begins a series of lock operations on possibly multiple keys; call before any locks are taken.
        /// </summary>
        void BeginLockable();

        /// <summary>
        /// Ends a series of lock operations on possibly multiple keys; call after all locks are released.
        /// </summary>
        void EndLockable();

        /// <summary>
        /// Lock the key with the specified <paramref name="lockType"/>, waiting until it is acquired
        /// </summary>
        /// <param name="key">The key to lock</param>
        /// <param name="lockType">The type of lock to take</param>
        void Lock(ref TKey key, LockType lockType);

        /// <summary>
        /// Lock the key with the specified <paramref name="lockType"/>, waiting until it is acquired
        /// </summary>
        /// <param name="key">The key to lock</param>
        /// <param name="lockType">The type of lock to take</param>
        void Lock(TKey key, LockType lockType);

        /// <summary>
        /// Lock the key with the specified <paramref name="lockType"/>
        /// </summary>
        /// <param name="key">The key to lock</param>
        /// <param name="lockType">The type of lock to release</param>
        void Unlock(ref TKey key, LockType lockType);

        /// <summary>
        /// Unlock the key with the specified <paramref name="lockType"/>
        /// </summary>
        /// <param name="key">The key to lock</param>
        /// <param name="lockType">The type of lock to release</param>
        void Unlock(TKey key, LockType lockType);

        /// <summary>
        /// Determines if the key is locked. Note this value may be obsolete as soon as it returns.
        /// </summary>
        /// <param name="key">The key to lock</param>
        (bool exclusive, ushort shared) IsLocked(ref TKey key);

        /// <summary>
        /// Determines if the key is locked. Note this value may be obsolete as soon as it returns.
        /// </summary>
        /// <param name="key">The key to lock</param>
        (bool exclusive, ushort shared) IsLocked(TKey key);
    }
}
