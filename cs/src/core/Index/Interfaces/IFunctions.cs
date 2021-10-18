// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.core
{
    /// <summary>
    /// Callback functions to FASTER
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    /// <typeparam name="Input"></typeparam>
    /// <typeparam name="Output"></typeparam>
    /// <typeparam name="Context"></typeparam>
    public interface IFunctions<Key, Value, Input, Output, Context>
    {
        #region Optional features supported by this implementation
        /// <summary>
        /// Whether this Functions instance supports locking. Iff so, FASTER will call <see cref="Lock(ref RecordInfo, ref Key, ref Value, LockType, ref long)"/> 
        /// and <see cref="Unlock(ref RecordInfo, ref Key, ref Value, LockType, long)"/>.
        /// </summary>
        bool SupportsLocking
#if NETSTANDARD2_1 || NET
            => false;
#else
            { get; }
#endif

        /// <summary>
        /// Whether this Functions instance supports operations on records after they have been successfully appended to the log. For example,
        /// after <see cref="CopyUpdater(ref Key, ref Input, ref Value, ref Value, ref Output, ref RecordInfo, long)"/> copies a list,
        /// <see cref="PostCopyUpdater(ref Key, ref Input, ref Value, ref Value, ref Output, ref RecordInfo, long)"/> can add items to it. 
        /// </summary>
        /// <remarks>Once the record has been appended it is visible to other sessions, so locking will be done per <see cref="SupportsLocking"/></remarks>
        bool SupportsPostOperations
#if NETSTANDARD2_1 || NET
            => false;
#else
            { get; }
#endif
        #endregion Optional features supported by this implementation

        #region Reads
        /// <summary>
        /// Non-concurrent reader. 
        /// </summary>
        /// <param name="key">The key for the record to be read</param>
        /// <param name="input">The user input for computing <paramref name="dst"/> from <paramref name="value"/></param>
        /// <param name="value">The value for the record being read</param>
        /// <param name="dst">The location where <paramref name="value"/> is to be copied</param>
        /// <param name="recordInfo">A reference to the header of the record; may be used by <see cref="Lock(ref RecordInfo, ref Key, ref Value, LockType, ref long)"/></param>
        /// <param name="address">The logical address of the record being read from, or zero if this was a readcache write; used as a RecordId by indexing if nonzero</param>
        /// <returns>True if the value was available, else false (e.g. the value was expired)</returns>
        bool SingleReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref RecordInfo recordInfo, long address);

        /// <summary>
        /// Conncurrent reader
        /// </summary>
        /// <param name="key">The key for the record to be read</param>
        /// <param name="input">The user input for computing <paramref name="dst"/> from <paramref name="value"/></param>
        /// <param name="value">The value for the record being read</param>
        /// <param name="dst">The location where <paramref name="value"/> is to be copied</param>
        /// <param name="recordInfo">A reference to the header of the record; may be used by <see cref="Lock(ref RecordInfo, ref Key, ref Value, LockType, ref long)"/></param>
        /// <param name="address">The logical address of the record being copied to; used as a RecordId by indexing</param>
        /// <returns>True if the value was available, else false (e.g. the value was expired)</returns>
        bool ConcurrentReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref RecordInfo recordInfo, long address);

        /// <summary>
        /// Read completion
        /// </summary>
        /// <param name="key">The key for this record</param>
        /// <param name="input">The user input that was used in the read operation</param>
        /// <param name="output">The result of the read operation; if this is a struct, then it will be a temporary and should be copied to <paramref name="ctx"/></param>
        /// <param name="ctx">The application context passed through the pending operation</param>
        /// <param name="status">The result of the pending operation</param>
        /// <param name="recordMetadata">Metadata for the record; may be used to obtain <see cref="RecordInfo.PreviousAddress"/> when doing iterative reads</param>
        void ReadCompletionCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status, RecordMetadata recordMetadata);
        #endregion reads

        #region Upserts
        /// <summary>
        /// Non-concurrent writer; called on an Upsert that does not find the key so does an insert or finds the key's record in the immutable region so does a read/copy/update (RCU),
        /// or when copying reads fetched from disk to either read cache or tail of log.
        /// </summary>
        /// <param name="key">The key for this record</param>
        /// <param name="input">The user input to be used for computing <paramref name="dst"/></param>
        /// <param name="src">The previous value to be copied/updated</param>
        /// <param name="dst">The destination to be updated; because this is an copy to a new location, there is no previous value there.</param>
        /// <param name="recordInfo">A reference to the header of the record; may be used by <see cref="Lock(ref RecordInfo, ref Key, ref Value, LockType, ref long)"/></param>
        /// <param name="address">The logical address of the record being copied to; used as a RecordId by indexing</param>
        void SingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref RecordInfo recordInfo, long address);

        /// <summary>
        /// Called after a record containing an upsert of a new key has been successfully inserted at the tail of the log.
        /// </summary>
        /// <param name="key">The key for this record</param>
        /// <param name="input">The user input that was used to compute <paramref name="dst"/></param>
        /// <param name="src">The previous value to be copied/updated</param>
        /// <param name="dst">The destination to be updated; because this is an copy to a new location, there is no previous value there.</param>
        /// <param name="recordInfo">A reference to the header of the record; may be used by <see cref="Lock(ref RecordInfo, ref Key, ref Value, LockType, ref long)"/></param>
        /// <param name="address">The logical address of the record being written; used as a RecordId by indexing</param>
        void PostSingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref RecordInfo recordInfo, long address)
#if NETSTANDARD2_1 || NET
        { }
#else
        ;
#endif

        /// <summary>
        /// Concurrent writer; called on an Upsert that finds the record in the mutable range.
        /// </summary>
        /// <param name="key">The key for the record to be written</param>
        /// <param name="input">The user input to be used for computing <paramref name="dst"/></param>
        /// <param name="src">The value to be copied to <paramref name="dst"/></param>
        /// <param name="dst">The location where <paramref name="src"/> is to be copied; because this method is called only for in-place updates, there is a previous value there.</param>
        /// <param name="recordInfo">A reference to the header of the record; may be used by <see cref="Lock(ref RecordInfo, ref Key, ref Value, LockType, ref long)"/></param>
        /// <param name="address">The logical address of the record being copied to; used as a RecordId by indexing"/></param>
        /// <returns>True if the value was written, else false</returns>
        bool ConcurrentWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref RecordInfo recordInfo, long address);

        /// <summary>
        /// Upsert completion
        /// </summary>
        /// <param name="key">The key for this record</param>
        /// <param name="input">The user input that was used to compute the new value</param>
        /// <param name="value">The value passed to Upsert</param>
        /// <param name="ctx">The application context passed through the pending operation</param>
        void UpsertCompletionCallback(ref Key key, ref Input input, ref Value value, Context ctx);
        #endregion Upserts

        #region RMWs
        #region InitialUpdater
        /// <summary>
        /// Whether we need to invoke initial-update for RMW
        /// </summary>
        /// <param name="key">The key for this record</param>
        /// <param name="input">The user input to be used for computing the updated value</param>
        /// <param name="output">The location where the result of the <paramref name="input"/> operation is to be copied</param>
        bool NeedInitialUpdate(ref Key key, ref Input input, ref Output output)
#if NETSTANDARD2_1 || NET
            => true
#endif
            ;

        /// <summary>
        /// Initial update for RMW (insert at the tail of the log).
        /// </summary>
        /// <param name="key">The key for this record</param>
        /// <param name="input">The user input to be used for computing the updated <paramref name="value"/></param>
        /// <param name="value">The destination to be updated; because this is an insert, there is no previous value there.</param>
        /// <param name="output">The location where the result of the <paramref name="input"/> operation on <paramref name="value"/> is to be copied</param>
        /// <param name="recordInfo">A reference to the header of the record; may be used by <see cref="Lock(ref RecordInfo, ref Key, ref Value, LockType, ref long)"/></param>
        /// <param name="address">The logical address of the record being updated; used as a RecordId by indexing</param>
        void InitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, long address);

        /// <summary>
        /// Called after a record containing an initial update for RMW has been successfully inserted at the tail of the log.
        /// </summary>
        /// <param name="key">The key for this record</param>
        /// <param name="input">The user input to be used for computing the updated <paramref name="value"/></param>
        /// <param name="value">The destination to be updated; because this is an insert, there is no previous value there.</param>
        /// <param name="output">The location where the result of the <paramref name="input"/> operation on <paramref name="value"/> is to be copied</param>
        /// <param name="recordInfo">A reference to the header of the record; may be used by <see cref="Lock(ref RecordInfo, ref Key, ref Value, LockType, ref long)"/></param>
        /// <param name="address">The logical address of the record being written; used as a RecordId by indexing</param>
        void PostInitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, long address)
#if NETSTANDARD2_1 || NET
        { }
#else
        ;
#endif
        #endregion InitialUpdater

        #region CopyUpdater
        /// <summary>
        /// Whether we need to invoke copy-update for RMW
        /// </summary>
        /// <param name="key">The key for this record</param>
        /// <param name="input">The user input to be used for computing the updated value</param>
        /// <param name="oldValue">The existing value that would be copied.</param>
        /// <param name="output">The location where the result of the <paramref name="input"/> operation on <paramref name="oldValue"/> is to be copied</param>
        bool NeedCopyUpdate(ref Key key, ref Input input, ref Value oldValue, ref Output output)
#if NETSTANDARD2_1 || NET
            => true
#endif
            ;

        /// <summary>
        /// Copy-update for RMW (RCU (Read-Copy-Update) to the tail of the log)
        /// </summary>
        /// <param name="key">The key for this record</param>
        /// <param name="input">The user input to be used for computing <paramref name="newValue"/> from <paramref name="oldValue"/></param>
        /// <param name="oldValue">The previous value to be copied/updated</param>
        /// <param name="newValue">The destination to be updated; because this is an copy to a new location, there is no previous value there.</param>
        /// <param name="output">The location where <paramref name="newValue"/> is to be copied</param>
        /// <param name="recordInfo">A reference to the header of the record; may be used by <see cref="Lock(ref RecordInfo, ref Key, ref Value, LockType, ref long)"/></param>
        /// <param name="address">The logical address of the record being updated; used as a RecordId by indexing</param>
        void CopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RecordInfo recordInfo, long address);

        /// <summary>
        /// Called after a record containing an RCU (Read-Copy-Update) for RMW has been successfully inserted at the tail of the log.
        /// </summary>
        /// <param name="key">The key for this record</param>
        /// <param name="input">The user input to be used for computing <paramref name="newValue"/> from <paramref name="oldValue"/></param>
        /// <param name="oldValue">The previous value to be copied/updated; may also be disposed here if appropriate</param>
        /// <param name="newValue">The destination to be updated; because this is an copy to a new location, there is no previous value there.</param>
        /// <param name="output">The location where <paramref name="newValue"/> is to be copied</param>
        /// <param name="recordInfo">A reference to the header of the record; may be used by <see cref="Lock(ref RecordInfo, ref Key, ref Value, LockType, ref long)"/></param>
        /// <param name="address">The logical address of the record being copied into; used as a RecordId by indexing</param>
        /// <returns>True if the value was successfully updated, else false (e.g. the value was expired)</returns>
        bool PostCopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RecordInfo recordInfo, long address)
#if NETSTANDARD2_1 || NET
            => true
#endif
        ;
        #endregion CopyUpdater

        #region InPlaceUpdater
        /// <summary>
        /// In-place update for RMW
        /// </summary>
        /// <param name="key">The key for this record</param>
        /// <param name="input">The user input to be used for computing the updated <paramref name="value"/></param>
        /// <param name="value">The destination to be updated; because this is an in-place update, there is a previous value there.</param>
        /// <param name="output">The location where the result of the <paramref name="input"/> operation on <paramref name="value"/> is to be copied</param>
        /// <param name="recordInfo">A reference to the header of the record; may be used by <see cref="Lock(ref RecordInfo, ref Key, ref Value, LockType, ref long)"/></param>
        /// <param name="address">The logical address of the record being updated; used as a RecordId by indexing</param>
        /// <returns>True if the value was successfully updated, else false (e.g. the value was expired)</returns>
        bool InPlaceUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, long address);
        #endregion InPlaceUpdater

        /// <summary>
        /// RMW completion
        /// </summary>
        /// <param name="key">The key for this record</param>
        /// <param name="input">The user input that was used to perform the modification</param>
        /// <param name="output">The result of the RMW operation; if this is a struct, then it will be a temporary and should be copied to <paramref name="ctx"/></param>
        /// <param name="ctx">The application context passed through the pending operation</param>
        /// <param name="status">The result of the pending operation</param>
        /// <param name="recordMetadata">The metadata of the modified or inserted record</param>
        void RMWCompletionCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status, RecordMetadata recordMetadata);
        #endregion RMWs

        #region Deletes
        /// <summary>
        /// Called after a record marking a Delete (with Tombstone set) has been successfully inserted at the tail of the log.
        /// </summary>
        /// <param name="key">The key for the record that was deleted</param>
        /// <param name="recordInfo">A reference to the header of the record; may be used by <see cref="Lock(ref RecordInfo, ref Key, ref Value, LockType, ref long)"/></param>
        /// <param name="address">The logical address of the record that was inserted with its tombstone set; used as a RecordId by indexing</param>
        /// <remarks>This does not have the address of the record that contains the value at 'key'; Delete does not retrieve records below HeadAddress, so
        ///     the last record we have in the 'key' chain may belong to 'key' or may be a collision.</remarks>
        void PostSingleDeleter(ref Key key, ref RecordInfo recordInfo, long address)
#if NETSTANDARD2_1 || NET
        { }
#else
        ;
#endif

        /// <summary>
        /// Concurrent deleter; called on an Delete that finds the record in the mutable range.
        /// </summary>
        /// <param name="key">The key for the record to be deleted</param>
        /// <param name="value">The value for the record being deleted; because this method is called only for in-place updates, there is a previous value there. Usually this is ignored or assigned 'default'.</param>
        /// <param name="recordInfo">A reference to the header of the record; may be used by <see cref="Lock(ref RecordInfo, ref Key, ref Value, LockType, ref long)"/></param>
        /// <param name="address">The logical address of the record being deleted; used as a RecordId by indexing</param>
        /// <remarks>For Object Value types, Dispose() can be called here. If recordInfo.Invalid is true, this is called after the record was allocated and populated, but could not be appended at the end of the log.</remarks>
        /// <returns>True if the value was successfully deleted, else false (e.g. the record was sealed)</returns>
        bool ConcurrentDeleter(ref Key key, ref Value value, ref RecordInfo recordInfo, long address);

        /// <summary>
        /// Delete completion
        /// </summary>
        /// <param name="key">The key for this record</param>
        /// <param name="ctx">The application context passed through the pending operation</param>
        void DeleteCompletionCallback(ref Key key, Context ctx);
        #endregion Deletes

        #region Locking
        /// <summary>
        /// User-provided lock call, defaulting to no-op. A default implementation is available via <see cref="RecordInfo.LockX()"/> and <see cref="RecordInfo.LockS()"/>.
        /// See also <see cref="IntExclusiveLocker"/> to use two bits of an existing int value.
        /// </summary>
        /// <param name="recordInfo">The header for the current record</param>
        /// <param name="key">The key for the current record</param>
        /// <param name="value">The value for the current record</param>
        /// <param name="lockType">The type of lock being taken</param>
        /// <param name="lockContext">Context-specific information; will be passed to <see cref="Unlock(ref RecordInfo, ref Key, ref Value, LockType, long)"/></param>
        /// <remarks>
        /// This is called only for records guaranteed to be in the mutable range.
        /// </remarks>
        void Lock(ref RecordInfo recordInfo, ref Key key, ref Value value, LockType lockType, ref long lockContext);

        /// <summary>
        /// User-provided unlock call, defaulting to no-op. A default exclusive implementation is available via <see cref="RecordInfo.UnlockX()"/> and <see cref="RecordInfo.UnlockS()"/>.
        /// See also <see cref="IntExclusiveLocker"/> to use two bits of an existing int value.
        /// </summary>
        /// <param name="recordInfo">The header for the current record</param>
        /// <param name="key">The key for the current record</param>
        /// <param name="value">The value for the current record</param>
        /// <param name="lockType">The type of lock being released, as passed to <see cref="Lock(ref RecordInfo, ref Key, ref Value, LockType, ref long)"/></param>
        /// <param name="lockContext">The context returned from <see cref="Lock(ref RecordInfo, ref Key, ref Value, LockType, ref long)"/></param>
        /// <remarks>
        /// This is called only for records guaranteed to be in the mutable range.
        /// </remarks>
        /// <returns>
        /// True if no inconsistencies detected. Otherwise, the lock and user's callback are reissued.
        /// Currently this is handled only for <see cref="ConcurrentReader(ref Key, ref Input, ref Value, ref Output, ref RecordInfo, long)"/>.
        /// </returns>
        bool Unlock(ref RecordInfo recordInfo, ref Key key, ref Value value, LockType lockType, long lockContext);
        #endregion Locking

        #region Other
        /// <summary>
        /// Checkpoint completion callback (called per client session)
        /// </summary>
        /// <param name="sessionId">Session ID reporting persistence</param>
        /// <param name="commitPoint">Commit point descriptor</param>
        void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint);
        #endregion Other
    }

    /// <summary>
    /// Callback functions to FASTER (two-param version)
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    public interface IFunctions<Key, Value> : IFunctions<Key, Value, Value, Value, Empty>
    {
    }

    /// <summary>
    /// Callback functions to FASTER (two-param version with context)
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    /// <typeparam name="Context"></typeparam>
    public interface IFunctions<Key, Value, Context> : IFunctions<Key, Value, Value, Value, Context>
    {
    }
}