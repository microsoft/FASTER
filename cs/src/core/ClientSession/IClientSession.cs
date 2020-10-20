// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 0162

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    internal interface IClientSession
    {
        void AtomicSwitch(int version);
    }

#if DEBUG
    /// <summary>
    /// Public interface for a client session. DEBUG-only because clients should prefer concrete types rather than interfaces, for performance reasons;
    /// but this ensures consistency across IClientSession and IAdvancedClientSession.
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    /// <typeparam name="Input"></typeparam>
    /// <typeparam name="Output"></typeparam>
    /// <typeparam name="Context"></typeparam>
    public interface IClientSession<Key, Value, Input, Output, Context>
    {
        /// <summary>
        /// Get session ID
        /// </summary>
        string ID { get; }

        /// <summary>
        /// Read operation
        /// </summary>
        /// <param name="key">The key to look up</param>
        /// <param name="input">Input to help extract the retrieved value into <paramref name="output"/></param>
        /// <param name="output">The location to place the retrieved value</param>
        /// <param name="userContext">User application context passed in case the read goes pending due to IO</param>
        /// <param name="serialNo">The serial number of the operation (used in recovery)</param>
        /// <returns><paramref name="output"/> is populated by the <see cref="IFunctions{Key, Value, Context}"/> implementation</returns>
        public Status Read(ref Key key, ref Input input, ref Output output, Context userContext, long serialNo);

        /// <summary>
        /// Read operation
        /// </summary>
        /// <param name="key">The key to look up</param>
        /// <param name="input">Input to help extract the retrieved value into <paramref name="output"/></param>
        /// <param name="output">The location to place the retrieved value</param>
        /// <param name="userContext">User application context passed in case the read goes pending due to IO</param>
        /// <param name="serialNo">The serial number of the operation (used in recovery)</param>
        /// <returns><paramref name="output"/> is populated by the <see cref="IFunctions{Key, Value, Context}"/> implementation</returns>
        public Status Read(Key key, Input input, out Output output, Context userContext = default, long serialNo = 0);

        /// <summary>
        /// Read operation
        /// </summary>
        /// <param name="key">The key to look up</param>
        /// <param name="output">The location to place the retrieved value</param>
        /// <param name="userContext">User application context passed in case the read goes pending due to IO</param>
        /// <param name="serialNo">The serial number of the operation (used in recovery)</param>
        /// <returns><paramref name="output"/> is populated by the <see cref="IFunctions{Key, Value, Context}"/> implementation</returns>
        public Status Read(ref Key key, ref Output output, Context userContext = default, long serialNo = 0);

        /// <summary>
        /// Read operation
        /// </summary>
        /// <param name="key">The key to look up</param>
        /// <param name="output">The location to place the retrieved value</param>
        /// <param name="userContext">User application context passed in case the read goes pending due to IO</param>
        /// <param name="serialNo">The serial number of the operation (used in recovery)</param>
        /// <returns><paramref name="output"/> is populated by the <see cref="IFunctions{Key, Value, Context}"/> implementation</returns>
        public Status Read(Key key, out Output output, Context userContext = default, long serialNo = 0);

        /// <summary>
        /// Read operation
        /// </summary>
        /// <param name="key">The key to look up</param>
        /// <param name="userContext">User application context passed in case the read goes pending due to IO</param>
        /// <param name="serialNo">The serial number of the operation (used in recovery)</param>
        /// <returns>A tuple of (<see cref="Status"/>, <typeparamref name="Output"/>)</returns>
        public (Status, Output) Read(Key key, Context userContext = default, long serialNo = 0);

        /// <summary>
        /// Read operation that accepts a <paramref name="recordInfo"/> ref argument to start the lookup at instead of starting at the hash table entry for <paramref name="key"/>,
        ///     and is updated with the record header for the found record (which contains previous address in the hash chain for this key; this can
        ///     be used as <paramref name="recordInfo"/> in a subsequent call to iterate all records for <paramref name="key"/>).
        /// </summary>
        /// <param name="key">The key to look up</param>
        /// <param name="input">Input to help extract the retrieved value into <paramref name="output"/></param>
        /// <param name="output">The location to place the retrieved value</param>
        /// <param name="recordInfo">On input contains the address to start at in its <see cref="RecordInfo.PreviousAddress"/>; if this is Constants.kInvalidAddress, the
        ///     search starts with the key as in other forms of Read. On output, receives a copy of the record's header, which can be passed
        ///     in a subsequent call, thereby enumerating all records in a key's hash chain.</param>
        /// <param name="userContext">User application context passed in case the read goes pending due to IO</param>
        /// <param name="serialNo">The serial number of the operation (used in recovery)</param>
        /// <returns><paramref name="output"/> is populated by the <see cref="IFunctions{Key, Value, Context}"/> implementation</returns>
        public Status Read(ref Key key, ref Input input, ref Output output, ref RecordInfo recordInfo, Context userContext = default, long serialNo = 0);

        /// <summary>
        /// Read operation that accepts an <paramref name="address"/> argument to lookup at, instead of a key.
        /// </summary>
        /// <param name="address">The address to look up</param>
        /// <param name="input">Input to help extract the retrieved value into <paramref name="output"/></param>
        /// <param name="output">The location to place the retrieved value</param>
        /// <param name="userContext">User application context passed in case the read goes pending due to IO</param>
        /// <param name="serialNo">The serial number of the operation (used in recovery)</param>
        /// <returns><paramref name="output"/> is populated by the <see cref="IFunctions{Key, Value, Context}"/> implementation; this should store the key if it needs it</returns>
        public Status ReadAtAddress(long address, ref Input input, ref Output output, Context userContext = default, long serialNo = 0);

        /// <summary>
        /// Async read operation. May return uncommitted results; to ensure reading of committed results, complete the read and then call WaitForCommitAsync.
        /// </summary>
        /// <param name="key">The key to look up</param>
        /// <param name="input">Input to help extract the retrieved value into output</param>
        /// <param name="userContext">User application context passed in case the read goes pending due to IO</param>
        /// <param name="serialNo">The serial number of the operation (used in recovery)</param>
        /// <param name="cancellationToken">Token to cancel the operation</param>
        /// <returns>ReadAsyncResult - call <see cref="FasterKV{Key, Value}.ReadAsyncResult{Input, Output, Context}.Complete()"/>
        ///     or <see cref="FasterKV{Key, Value}.ReadAsyncResult{Input, Output, Context}.Complete(out RecordInfo)"/> 
        ///     on the return value to complete the read operation and obtain the result status, the output that is populated by the 
        ///     <see cref="IFunctions{Key, Value, Context}"/> implementation, and optionally a copy of the header for the retrieved record</returns>
        public ValueTask<FasterKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(ref Key key, ref Input input, Context userContext = default, long serialNo = 0, CancellationToken cancellationToken = default);

        /// <summary>
        /// Async read operation. May return uncommitted results; to ensure reading of committed results, complete the read and then call WaitForCommitAsync.
        /// </summary>
        /// <param name="key">The key to look up</param>
        /// <param name="input">Input to help extract the retrieved value into output</param>
        /// <param name="userContext">User application context passed in case the read goes pending due to IO</param>
        /// <param name="serialNo">The serial number of the operation (used in recovery)</param>
        /// <param name="cancellationToken">Token to cancel the operation</param>
        /// <returns>ReadAsyncResult - call <see cref="FasterKV{Key, Value}.ReadAsyncResult{Input, Output, Context}.Complete()"/>
        ///     or <see cref="FasterKV{Key, Value}.ReadAsyncResult{Input, Output, Context}.Complete(out RecordInfo)"/> 
        ///     on the return value to complete the read operation and obtain the result status, the output that is populated by the 
        ///     <see cref="IFunctions{Key, Value, Context}"/> implementation, and optionally a copy of the header for the retrieved record</returns>
        public ValueTask<FasterKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(Key key, Input input, Context userContext = default, long serialNo = 0, CancellationToken cancellationToken = default);

        /// <summary>
        /// Async read operation. May return uncommitted results; to ensure reading of committed results, complete the read and then call WaitForCommitAsync.
        /// </summary>
        /// <param name="key">The key to look up</param>
        /// <param name="userContext">User application context passed in case the read goes pending due to IO</param>
        /// <param name="serialNo">The serial number of the operation (used in recovery)</param>
        /// <param name="token">Token to cancel the operation</param>
        /// <returns>ReadAsyncResult - call <see cref="FasterKV{Key, Value}.ReadAsyncResult{Input, Output, Context}.Complete()"/>
        ///     or <see cref="FasterKV{Key, Value}.ReadAsyncResult{Input, Output, Context}.Complete(out RecordInfo)"/> 
        ///     on the return value to complete the read operation and obtain the result status, the output that is populated by the 
        ///     <see cref="IFunctions{Key, Value, Context}"/> implementation, and optionally a copy of the header for the retrieved record</returns>
        public ValueTask<FasterKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(ref Key key, Context userContext = default, long serialNo = 0, CancellationToken token = default);

        /// <summary>
        /// Async read operation, may return uncommitted result
        /// To ensure reading of committed result, complete the read and then call WaitForCommitAsync.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="context"></param>
        /// <param name="serialNo"></param>
        /// <param name="token"></param>
        /// <returns>ReadAsyncResult - call <see cref="FasterKV{Key, Value}.ReadAsyncResult{Input, Output, Context}.Complete()"/>
        ///     or <see cref="FasterKV{Key, Value}.ReadAsyncResult{Input, Output, Context}.Complete(out RecordInfo)"/> 
        ///     on the return value to complete the read operation and obtain the result status, the output that is populated by the 
        ///     <see cref="IFunctions{Key, Value, Context}"/> implementation, and optionally a copy of the header for the retrieved record</returns>
        public ValueTask<FasterKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(Key key, Context context = default, long serialNo = 0, CancellationToken token = default);

        /// <summary>
        /// Async read operation that accepts a <paramref name="startAddress"/> to start the lookup at instead of starting at the hash table entry for <paramref name="key"/>,
        ///     and returns the <see cref="RecordInfo"/> for the found record (which contains previous address in the hash chain for this key; this can
        ///     be used as <paramref name="startAddress"/> in a subsequent call to iterate all records for <paramref name="key"/>).
        /// </summary>
        /// <param name="key">The key to look up</param>
        /// <param name="input">Input to help extract the retrieved value into output</param>
        /// <param name="startAddress">Start at this address rather than the address in the hash table for <paramref name="key"/>"/></param>
        /// <param name="userContext">User application context passed in case the read goes pending due to IO</param>
        /// <param name="serialNo">The serial number of the operation (used in recovery)</param>
        /// <param name="cancellationToken">Token to cancel the operation</param>
        /// <returns>ReadAsyncResult - call <see cref="FasterKV{Key, Value}.ReadAsyncResult{Input, Output, Context}.Complete()"/>
        ///     or <see cref="FasterKV{Key, Value}.ReadAsyncResult{Input, Output, Context}.Complete(out RecordInfo)"/> 
        ///     on the return value to complete the read operation and obtain the result status, the output that is populated by the 
        ///     <see cref="IFunctions{Key, Value, Context}"/> implementation, and optionally a copy of the header for the retrieved record</returns>
        public ValueTask<FasterKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(ref Key key, ref Input input, long startAddress, Context userContext = default, long serialNo = 0, CancellationToken cancellationToken = default);

        /// <summary>
        /// Async Read operation that accepts an <paramref name="address"/> argument to lookup at, instead of a key.
        /// </summary>
        /// <param name="address">The address to look up</param>
        /// <param name="input">Input to help extract the retrieved value into output</param>
        /// <param name="userContext">User application context passed in case the read goes pending due to IO</param>
        /// <param name="serialNo">The serial number of the operation (used in recovery)</param>
        /// <param name="cancellationToken">Token to cancel the operation</param>
        /// <returns>ReadAsyncResult - call <see cref="FasterKV{Key, Value}.ReadAsyncResult{Input, Output, Context}.Complete()"/>
        ///     or <see cref="FasterKV{Key, Value}.ReadAsyncResult{Input, Output, Context}.Complete(out RecordInfo)"/> 
        ///     on the return value to complete the read operation and obtain the result status, the output that is populated by the 
        ///     <see cref="IFunctions{Key, Value, Context}"/> implementation, and optionally a copy of the header for the retrieved record</returns>
        public ValueTask<FasterKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAtAddressAsync(long address, ref Input input, Context userContext = default, long serialNo = 0, CancellationToken cancellationToken = default);

        /// <summary>
        /// Upsert operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="desiredValue"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        public Status Upsert(ref Key key, ref Value desiredValue, Context userContext = default, long serialNo = 0);

        /// <summary>
        /// RMW operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        public Status RMW(ref Key key, ref Input input, Context userContext = default, long serialNo = 0);

        /// <summary>
        /// RMW operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        public Status RMW(Key key, Input input, Context userContext = default, long serialNo = 0);

        /// <summary>
        /// Async RMW operation
        /// Await operation in session before issuing next one
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="context"></param>
        /// <param name="serialNo"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        public ValueTask<FasterKV<Key, Value>.RmwAsyncResult<Input, Output, Context>> RMWAsync(ref Key key, ref Input input, Context context = default, long serialNo = 0, CancellationToken token = default);

        /// <summary>
        /// Async RMW operation
        /// Await operation in session before issuing next one
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="context"></param>
        /// <param name="serialNo"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        public ValueTask<FasterKV<Key, Value>.RmwAsyncResult<Input, Output, Context>> RMWAsync(Key key, Input input, Context context = default, long serialNo = 0, CancellationToken token = default);

        /// <summary>
        /// Delete operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        public Status Delete(ref Key key, Context userContext, long serialNo);

        /// <summary>
        /// Delete operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        public Status Delete(Key key, Context userContext, long serialNo);

        /// <summary>
        /// Get list of pending requests (for current session)
        /// </summary>
        /// <returns></returns>
        public IEnumerable<long> GetPendingRequests();

        /// <summary>
        /// Refresh session epoch and handle checkpointing phases. Used only
        /// in case of thread-affinitized sessions (async support is disabled).
        /// </summary>
        public void Refresh();

        /// <summary>
        /// Sync complete all outstanding pending operations
        /// Async operations (ReadAsync) must be completed individually
        /// </summary>
        /// <param name="spinWait">Spin-wait for all pending operations on session to complete</param>
        /// <param name="spinWaitForCommit">Extend spin-wait until ongoing commit/checkpoint, if any, completes</param>
        /// <returns></returns>
        public bool CompletePending(bool spinWait = false, bool spinWaitForCommit = false);

        /// <summary>
        /// Complete all outstanding pending operations asynchronously
        /// Async operations (ReadAsync) must be completed individually
        /// </summary>
        /// <returns></returns>
        public ValueTask CompletePendingAsync(bool waitForCommit = false, CancellationToken token = default);

        /// <summary>
        /// Check if at least one request is ready for CompletePending to be called on
        /// Returns completed immediately if there are no outstanding requests
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        public ValueTask ReadyToCompletePendingAsync(CancellationToken token = default);

        /// <summary>
        /// Wait for commit of all operations completed until the current point in session.
        /// Does not itself issue checkpoint/commits.
        /// </summary>
        /// <returns></returns>
        public ValueTask WaitForCommitAsync(CancellationToken token = default);
    }
#endif
}

