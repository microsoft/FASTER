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
        /// If true, then keys must use one of the <see cref="GetLockCode(ref TKey, long)"/> overloads to obtain a code by which groups of keys will be sorted for manual locking, to avoid deadlocks.
        /// </summary>
        /// <remarks>Whether this returns true depends on the <see cref="LockingMode"/> on <see cref="FasterKVSettings{Key, Value}"/>, or passed to the FasterKV constructor.</remarks>
        bool NeedKeyLockCode { get; }

        /// <summary>
        /// Obtain a code by which groups of keys will be sorted for manual locking, to avoid deadlocks.
        /// <param name="key">The key to obtain a code for</param>
        /// <param name="keyHash">The hashcode of the key; created and returned by <see cref="IFasterEqualityComparer{Key}.GetHashCode64(ref Key)"/>.</param>
        /// </summary>
        /// <remarks>If <see cref="NeedKeyLockCode"/> is true, this code is obtained by FASTER on method calls and is used in its locking scheme. 
        ///     In that case the app must ensure that the keys in a group are sorted by this value, to avoid deadlock.</remarks>
        long GetLockCode(TKey key, out long keyHash);

        /// <summary>
        /// Obtain a code by which groups of keys will be sorted for manual locking, to avoid deadlocks.
        /// <param name="key">The key to obtain a code for</param>
        /// <param name="keyHash">The hashcode of the key; created and returned by <see cref="IFasterEqualityComparer{Key}.GetHashCode64(ref Key)"/>.</param>
        /// </summary>
        /// <remarks>If <see cref="NeedKeyLockCode"/> is true, this code is obtained by FASTER on method calls and is used in its locking scheme. 
        ///     In that case the app must ensure that the keys in a group are sorted by this value, to avoid deadlock.</remarks>
        long GetLockCode(ref TKey key, out long keyHash);

        /// <summary>
        /// Obtain a code by which groups of keys will be sorted for manual locking, to avoid deadlocks.
        /// <param name="key">The key to obtain a code for</param>
        /// <param name="keyHash">The hashcode of the key; must be the value returned by <see cref="IFasterEqualityComparer{Key}.GetHashCode64(ref Key)"/>.</param>
        /// </summary>
        /// <remarks>If <see cref="NeedKeyLockCode"/> is true, this code is obtained by FASTER on method calls and is used in its locking scheme. 
        ///     In that case the app must ensure that the keys in a group are sorted by this value, to avoid deadlock.</remarks>
        long GetLockCode(TKey key, long keyHash);

        /// <summary>
        /// Obtain a code by which groups of keys will be sorted for manual locking, to avoid deadlocks.
        /// <param name="key">The key to obtain a code for</param>
        /// <param name="keyHash">The hashcode of the key; must be the value returned by <see cref="IFasterEqualityComparer{Key}.GetHashCode64(ref Key)"/>.</param>
        /// </summary>
        /// <remarks>If <see cref="NeedKeyLockCode"/> is true, this code is obtained by FASTER on method calls and is used in its locking scheme. 
        ///     In that case the app must ensure that the keys in a group are sorted by this value, to avoid deadlock.</remarks>
        long GetLockCode(ref TKey key, long keyHash);

        /// <summary>
        /// Compare two structures that implement ILockableKey.
        /// </summary>
        /// <typeparam name="TLockableKey">The type of the app data struct or class containing key info</typeparam>
        /// <param name="key1">The first key to compare</param>
        /// <param name="key2">The first key to compare</param>
        /// <returns>The result of key1.CompareTo(key2)</returns>
        int CompareLockCodes<TLockableKey>(TLockableKey key1, TLockableKey key2)
            where TLockableKey : ILockableKey;

        /// <summary>
        /// Compare two structures that implement ILockableKey.
        /// </summary>
        /// <typeparam name="TLockableKey">The type of the app data struct or class containing key info</typeparam>
        /// <param name="key1">The first key to compare</param>
        /// <param name="key2">The first key to compare</param>
        /// <returns>The result of key1.CompareTo(key2)</returns>
        int CompareLockCodes<TLockableKey>(ref TLockableKey key1, ref TLockableKey key2)
            where TLockableKey : ILockableKey;

        /// <summary>
        /// Sort an array of app data structures (or classes) by lock code and lock type; these will be passed to Lockable*Session.Lock
        /// </summary>
        /// <typeparam name="TLockableKey">The type of the app data struct or class containing key info</typeparam>
        /// <param name="keys">The array of app key data </param>
        void SortLockCodes<TLockableKey>(TLockableKey[] keys)
            where TLockableKey : ILockableKey;

        /// <summary>
        /// Sort an array of app data structures (or classes) by lock code and lock type; these will be passed to Lockable*Session.Lock
        /// </summary>
        /// <typeparam name="TLockableKey">The type of the app data struct or class containing key info</typeparam>
        /// <param name="keys">The array of app key data </param>
        /// <param name="start">The starting index to sort</param>
        /// <param name="count">The number of keys to sort</param>
        void SortLockCodes<TLockableKey>(TLockableKey[] keys, int start, int count)
            where TLockableKey : ILockableKey;

        /// <summary>
        /// Locks the keys identified in the passed array.
        /// </summary>
        /// <typeparam name="TLockableKey"></typeparam>
        /// <param name="keys">keyCodes to be locked, and whether that locking is shared or exclusive; must be sorted by <see cref="SortLockCodes{TLockableKey}(TLockableKey[])"/>.</param>
        void Lock<TLockableKey>(TLockableKey[] keys)
            where TLockableKey : ILockableKey;

        /// <summary>
        /// Locks the keys identified in the passed array.
        /// </summary>
        /// <typeparam name="TLockableKey"></typeparam>
        /// <param name="keys">keyCodes to be locked, and whether that locking is shared or exclusive; must be sorted by <see cref="SortLockCodes{TLockableKey}(TLockableKey[])"/>.</param>
        /// <param name="start">The starting index to Lock</param>
        /// <param name="count">The number of keys to Lock</param>
        void Lock<TLockableKey>(TLockableKey[] keys, int start, int count)
            where TLockableKey : ILockableKey;

        /// <summary>
        /// Unlocks the keys identified in the passed array.
        /// </summary>
        /// <typeparam name="TLockableKey"></typeparam>
        /// <param name="keys">keyCodes to be unlocked, and whether that unlocking is shared or exclusive; must be sorted by <see cref="SortLockCodes{TLockableKey}(TLockableKey[])"/>.</param>
        void Unlock<TLockableKey>(TLockableKey[] keys)
            where TLockableKey : ILockableKey;

        /// <summary>
        /// Unlocks the keys identified in the passed array.
        /// </summary>
        /// <typeparam name="TLockableKey"></typeparam>
        /// <param name="keys">keyCodes to be unlocked, and whether that unlocking is shared or exclusive; must be sorted by <see cref="SortLockCodes{TLockableKey}(TLockableKey[])"/>.</param>
        /// <param name="start">The starting index to Unlock</param>
        /// <param name="count">The number of keys to Unlock</param>
        void Unlock<TLockableKey>(TLockableKey[] keys, int start, int count)
            where TLockableKey : ILockableKey;
    }
}
