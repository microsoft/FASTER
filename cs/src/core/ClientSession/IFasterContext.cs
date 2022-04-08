// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    /// <summary>
    /// Interface for FASTER operations
    /// </summary>
    public interface IFasterContext<Key, Value, Input, Output, Context, StoreFunctions>
        where StoreFunctions : IStoreFunctions<Key, Value>
    {
        /// <summary>
        /// Synchronously complete outstanding pending synchronous operations.
        /// Async operations must be completed individually.
        /// </summary>
        /// <param name="wait">Wait for all pending operations on session to complete</param>
        /// <param name="spinWaitForCommit">Spin-wait until ongoing commit/checkpoint, if any, completes</param>
        /// <returns>True if all pending operations have completed, false otherwise</returns>
        bool CompletePending(bool wait = false, bool spinWaitForCommit = false);

        /// <summary>
        /// Synchronously complete outstanding pending synchronous operations, returning outputs for the completed operations.
        /// Async operations must be completed individually.
        /// </summary>
        /// <param name="completedOutputs">Outputs completed by this operation</param>
        /// <param name="wait">Wait for all pending operations on session to complete</param>
        /// <param name="spinWaitForCommit">Spin-wait until ongoing commit/checkpoint, if any, completes</param>
        /// <returns>True if all pending operations have completed, false otherwise</returns>
        bool CompletePendingWithOutputs(out CompletedOutputIterator<Key, Value, Input, Output, Context> completedOutputs, bool wait = false, bool spinWaitForCommit = false);

        /// <summary>
        /// Read operation
        /// </summary>
        /// <param name="key">The key to look up</param>
        /// <param name="input">Input to help extract the retrieved value into <paramref name="output"/></param>
        /// <param name="output">The location to place the retrieved value</param>
        /// <param name="userContext">User application context passed in case the read goes pending due to IO</param>
        /// <param name="serialNo">The serial number of the operation (used in recovery)</param>
        /// <returns><paramref name="output"/> is populated by the <see cref="IFunctions{Key, Value, Context}"/> implementation</returns>
        Status Read(ref Key key, ref Input input, ref Output output, Context userContext = default, long serialNo = 0);

        /// <summary>
        /// Read operation
        /// </summary>
        /// <param name="key">The key to look up</param>
        /// <param name="input">Input to help extract the retrieved value into <paramref name="output"/></param>
        /// <param name="output">The location to place the retrieved value</param>
        /// <param name="userContext">User application context passed in case the read goes pending due to IO</param>
        /// <param name="serialNo">The serial number of the operation (used in recovery)</param>
        /// <returns><paramref name="output"/> is populated by the <see cref="IFunctions{Key, Value, Context}"/> implementation</returns>
        Status Read(Key key, Input input, out Output output, Context userContext = default, long serialNo = 0);

        /// <summary>
        /// Read operation
        /// </summary>
        /// <param name="key">The key to look up</param>
        /// <param name="output">The location to place the retrieved value</param>
        /// <param name="userContext">User application context passed in case the read goes pending due to IO</param>
        /// <param name="serialNo">The serial number of the operation (used in recovery)</param>
        /// <returns><paramref name="output"/> is populated by the <see cref="IFunctions{Key, Value, Context}"/> implementation</returns>
        Status Read(ref Key key, ref Output output, Context userContext = default, long serialNo = 0);

        /// <summary>
        /// Read operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="output"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        Status Read(Key key, out Output output, Context userContext = default, long serialNo = 0);

        /// <summary>
        /// Read operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        public (Status status, Output output) Read(Key key, Context userContext = default, long serialNo = 0);

        /// <summary>
        /// Read operation that accepts a <paramref name="recordMetadata"/> ref argument to start the lookup at instead of starting at the hash table entry for <paramref name="key"/>,
        ///     and is updated with the address and record header for the found record.
        /// </summary>
        /// <param name="key">The key to look up</param>
        /// <param name="input">Input to help extract the retrieved value into <paramref name="output"/></param>
        /// <param name="output">The location to place the retrieved value</param>
        /// <param name="readOptions">Contains options controlling the Read operation</param>
        /// <param name="recordMetadata">On output, receives:
        ///         <list type="bullet">
        ///             <li>The address of the found record. This may be different from the <paramref name="recordMetadata.RecordInfo.PreviousAddress"/> passed on the call, due to
        ///                 tracing back over hash collisions until we arrive at the key match</li>
        ///             <li>A copy of the record's header in <paramref name="recordMetadata.RecordInfo"/>; <paramref name="recordMetadata.RecordInfo.PreviousAddress"/> can be passed
        ///                 in a subsequent call, thereby enumerating all records in a key's hash chain.</li>
        ///         </list>
        /// </param>
        /// <param name="userContext">User application context passed in case the read goes pending due to IO</param>
        /// <param name="serialNo">The serial number of the operation (used in recovery)</param>
        /// <returns><paramref name="output"/> is populated by the <see cref="IFunctions{Key, Value, Context}"/> implementation</returns>
        Status Read(ref Key key, ref Input input, ref Output output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, Context userContext = default, long serialNo = 0);

        /// <summary>
        /// Read operation that accepts an address to lookup at, instead of a key.
        /// </summary>
        /// <param name="input">Input to help extract the retrieved value into <paramref name="output"/></param>
        /// <param name="output">The location to place the retrieved value</param>
        /// <param name="readOptions">Contains options controlling the Read operation, including the address to read at in StartAddress</param>
        /// <param name="userContext">User application context passed in case the read goes pending due to IO</param>
        /// <param name="serialNo">The serial number of the operation (used in recovery)</param>
        /// <returns><paramref name="output"/> is populated by the <see cref="IFunctions{Key, Value, Context}"/> implementation; this should store the key if it needs it</returns>
        Status ReadAtAddress(ref Input input, ref Output output, ref ReadOptions readOptions, Context userContext = default, long serialNo = 0);

        /// <summary>
        /// Async read operation. May return uncommitted results; to ensure reading of committed results, complete the read and then call WaitForCommitAsync.
        /// </summary>
        /// <param name="key">The key to look up</param>
        /// <param name="input">Input to help extract the retrieved value into output</param>
        /// <param name="userContext">User application context passed in case the read goes pending due to IO</param>
        /// <param name="serialNo">The serial number of the operation (used in recovery)</param>
        /// <param name="cancellationToken">Token to cancel the operation</param>
        /// <returns><see cref="ValueTask"/> wrapping <see cref="FasterKV{Key, Value, StoreFunctions}.ReadAsyncResult{Input, Output, Context}"/></returns>
        /// <remarks>The caller must await the return value to obtain the result, then call one of
        ///     <list type="bullet">
        ///     <item>result.<see cref="FasterKV{Key, Value, StoreFunctions}.ReadAsyncResult{Input, Output, Context}.Complete()"/></item>
        ///     <item>result.<see cref="FasterKV{Key, Value, StoreFunctions}.ReadAsyncResult{Input, Output, Context}.Complete(out RecordMetadata)"/></item>
        ///     </list>
        ///     to complete the read operation and obtain the result status, the output that is populated by the 
        ///     <see cref="IFunctions{Key, Value, Context}"/> implementation, and optionally a copy of the header for the retrieved record</remarks>
        ValueTask<FasterKV<Key, Value, StoreFunctions>.ReadAsyncResult<Input, Output, Context>> ReadAsync(ref Key key, ref Input input, Context userContext = default, long serialNo = 0, CancellationToken cancellationToken = default);

        /// <summary>
        /// Async read operation, may return uncommitted result
        /// To ensure reading of committed result, complete the read and then call WaitForCommitAsync.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="context"></param>
        /// <param name="serialNo"></param>
        /// <param name="token"></param>
        /// <returns><see cref="ValueTask"/> wrapping <see cref="FasterKV{Key, Value, StoreFunctions}.ReadAsyncResult{Input, Output, Context}"/></returns>
        /// <remarks>The caller must await the return value to obtain the result, then call one of
        ///     <list type="bullet">
        ///     <item>result.<see cref="FasterKV{Key, Value, StoreFunctions}.ReadAsyncResult{Input, Output, Context}.Complete()"/></item>
        ///     <item>result.<see cref="FasterKV{Key, Value, StoreFunctions}.ReadAsyncResult{Input, Output, Context}.Complete(out RecordMetadata)"/></item>
        ///     </list>
        ///     to complete the read operation and obtain the result status, the output that is populated by the 
        ///     <see cref="IFunctions{Key, Value, Context}"/> implementation, and optionally a copy of the header for the retrieved record</remarks>
        ValueTask<FasterKV<Key, Value, StoreFunctions>.ReadAsyncResult<Input, Output, Context>> ReadAsync(Key key, Input input, Context context = default, long serialNo = 0, CancellationToken token = default);

        /// <summary>
        /// Async read operation. May return uncommitted results; to ensure reading of committed results, complete the read and then call WaitForCommitAsync.
        /// </summary>
        /// <param name="key">The key to look up</param>
        /// <param name="userContext">User application context passed in case the read goes pending due to IO</param>
        /// <param name="serialNo">The serial number of the operation (used in recovery)</param>
        /// <param name="token">Token to cancel the operation</param>
        /// <returns><see cref="ValueTask"/> wrapping <see cref="FasterKV{Key, Value, StoreFunctions}.ReadAsyncResult{Input, Output, Context}"/></returns>
        /// <remarks>The caller must await the return value to obtain the result, then call one of
        ///     <list type="bullet">
        ///     <item>result.<see cref="FasterKV{Key, Value, StoreFunctions}.ReadAsyncResult{Input, Output, Context}.Complete()"/></item>
        ///     <item>result.<see cref="FasterKV{Key, Value, StoreFunctions}.ReadAsyncResult{Input, Output, Context}.Complete(out RecordMetadata)"/></item>
        ///     </list>
        ///     to complete the read operation and obtain the result status, the output that is populated by the 
        ///     <see cref="IFunctions{Key, Value, Context}"/> implementation, and optionally a copy of the header for the retrieved record</remarks>
        ValueTask<FasterKV<Key, Value, StoreFunctions>.ReadAsyncResult<Input, Output, Context>> ReadAsync(ref Key key, Context userContext = default, long serialNo = 0, CancellationToken token = default);

        /// <summary>
        /// Async read operation, may return uncommitted result
        /// To ensure reading of committed result, complete the read and then call WaitForCommitAsync.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="context"></param>
        /// <param name="serialNo"></param>
        /// <param name="token"></param>
        /// <returns><see cref="ValueTask"/> wrapping <see cref="FasterKV{Key, Value, StoreFunctions}.ReadAsyncResult{Input, Output, Context}"/></returns>
        /// <remarks>The caller must await the return value to obtain the result, then call one of
        ///     <list type="bullet">
        ///     <item>result.<see cref="FasterKV{Key, Value, StoreFunctions}.ReadAsyncResult{Input, Output, Context}.Complete()"/></item>
        ///     <item>result.<see cref="FasterKV{Key, Value, StoreFunctions}.ReadAsyncResult{Input, Output, Context}.Complete(out RecordMetadata)"/></item>
        ///     </list>
        ///     to complete the read operation and obtain the result status, the output that is populated by the 
        ///     <see cref="IFunctions{Key, Value, Context}"/> implementation, and optionally a copy of the header for the retrieved record</remarks>
        ValueTask<FasterKV<Key, Value, StoreFunctions>.ReadAsyncResult<Input, Output, Context>> ReadAsync(Key key, Context context = default, long serialNo = 0, CancellationToken token = default);

        /// <summary>
        /// Async read operation that accepts an address to start the lookup at instead of starting at the hash table entry for <paramref name="key"/>,
        ///     and returns the <see cref="RecordInfo"/> for the found record (which contains previous address in the hash chain for this key; this can
        ///     be used as the <paramref name="readOptions"/> StartAddress in a subsequent call to iterate all records for <paramref name="key"/>).
        /// </summary>
        /// <param name="key">The key to look up</param>
        /// <param name="input">Input to help extract the retrieved value into output</param>
        /// <param name="readOptions">Contains options controlling the Read operation</param>
        /// <param name="userContext">User application context passed in case the read goes pending due to IO</param>
        /// <param name="serialNo">The serial number of the operation (used in recovery)</param>
        /// <param name="cancellationToken">Token to cancel the operation</param>
        /// <returns><see cref="ValueTask"/> wrapping <see cref="FasterKV{Key, Value, StoreFunctions}.ReadAsyncResult{Input, Output, Context}"/></returns>
        /// <remarks>The caller must await the return value to obtain the result, then call one of
        ///     <list type="bullet">
        ///     <item>result.<see cref="FasterKV{Key, Value, StoreFunctions}.ReadAsyncResult{Input, Output, Context}.Complete()"/></item>
        ///     <item>result.<see cref="FasterKV{Key, Value, StoreFunctions}.ReadAsyncResult{Input, Output, Context}.Complete(out RecordMetadata)"/></item>
        ///     </list>
        ///     to complete the read operation and obtain the result status, the output that is populated by the 
        ///     <see cref="IFunctions{Key, Value, Context}"/> implementation, and optionally a copy of the header for the retrieved record
        /// </remarks>
        ValueTask<FasterKV<Key, Value, StoreFunctions>.ReadAsyncResult<Input, Output, Context>> ReadAsync(ref Key key, ref Input input, ref ReadOptions readOptions,
                                                                                                 Context userContext = default, long serialNo = 0, CancellationToken cancellationToken = default);

        /// <summary>
        /// Async Read operation that accepts an address argument to lookup at, instead of a key.
        /// </summary>
        /// <param name="input">Input to help extract the retrieved value into output</param>
        /// <param name="readOptions">Contains options controlling the Read operation, including the address to read at in StartAddress</param>
        /// <param name="userContext">User application context passed in case the read goes pending due to IO</param>
        /// <param name="serialNo">The serial number of the operation (used in recovery)</param>
        /// <param name="cancellationToken">Token to cancel the operation</param>
        /// <returns><see cref="ValueTask"/> wrapping <see cref="FasterKV{Key, Value, StoreFunctions}.ReadAsyncResult{Input, Output, Context}"/></returns>
        /// <remarks>The caller must await the return value to obtain the result, then call one of
        ///     <list type="bullet">
        ///     <item>result.<see cref="FasterKV{Key, Value, StoreFunctions}.ReadAsyncResult{Input, Output, Context}.Complete()"/></item>
        ///     <item>result.<see cref="FasterKV{Key, Value, StoreFunctions}.ReadAsyncResult{Input, Output, Context}.Complete(out RecordMetadata)"/></item>
        ///     </list>
        ///     to complete the read operation and obtain the result status, the output that is populated by the 
        ///     <see cref="IFunctions{Key, Value, Context}"/> implementation, and optionally a copy of the header for the retrieved record</remarks>
        ValueTask<FasterKV<Key, Value, StoreFunctions>.ReadAsyncResult<Input, Output, Context>> ReadAtAddressAsync(ref Input input, ref ReadOptions readOptions,
                                                                                                          Context userContext = default, long serialNo = 0, CancellationToken cancellationToken = default);

        /// <summary>
        /// Upsert operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="desiredValue"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        Status Upsert(ref Key key, ref Value desiredValue, Context userContext = default, long serialNo = 0);

        /// <summary>
        /// Upsert operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="desiredValue"></param>
        /// <param name="output"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        Status Upsert(ref Key key, ref Input input, ref Value desiredValue, ref Output output, Context userContext = default, long serialNo = 0);

        /// <summary>
        /// Upsert operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="desiredValue"></param>
        /// <param name="output"></param>
        /// <param name="recordMetadata"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        Status Upsert(ref Key key, ref Input input, ref Value desiredValue, ref Output output, out RecordMetadata recordMetadata, Context userContext = default, long serialNo = 0);

        /// <summary>
        /// Upsert operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="desiredValue"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        Status Upsert(Key key, Value desiredValue, Context userContext = default, long serialNo = 0);

        /// <summary>
        /// Upsert operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="desiredValue"></param>
        /// <param name="output"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        Status Upsert(Key key, Input input, Value desiredValue, ref Output output, Context userContext = default, long serialNo = 0);

        /// <summary>
        /// Async Upsert operation
        /// Await operation in session before issuing next one
        /// </summary>
        /// <param name="key"></param>
        /// <param name="desiredValue"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <param name="token"></param>
        /// <returns>ValueTask wrapping <see cref="FasterKV{Key, Value, StoreFunctions}.UpsertAsyncResult{Input, Output, Context}"/></returns>
        /// <remarks>The caller must await the return value to obtain the result, then call one of
        ///     <list type="bullet">
        ///     <item>result.<see cref="FasterKV{Key, Value, StoreFunctions}.UpsertAsyncResult{Input, Output, Context}.Complete()"/></item>
        ///     <item>result = await result.<see cref="FasterKV{Key, Value, StoreFunctions}.UpsertAsyncResult{Input, Output, Context}.CompleteAsync(CancellationToken)"/> while result.Status is <see cref="Status.IsPending"/></item>
        ///     </list>
        ///     to complete the Upsert operation. Failure to complete the operation will result in leaked allocations.</remarks>
        ValueTask<FasterKV<Key, Value, StoreFunctions>.UpsertAsyncResult<Input, Output, Context>> UpsertAsync(ref Key key, ref Value desiredValue, Context userContext = default, long serialNo = 0, CancellationToken token = default);

        /// <summary>
        /// Async Upsert operation
        /// Await operation in session before issuing next one
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="desiredValue"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <param name="token"></param>
        /// <returns>ValueTask wrapping <see cref="FasterKV{Key, Value, StoreFunctions}.UpsertAsyncResult{Input, Output, Context}"/></returns>
        /// <remarks>The caller must await the return value to obtain the result, then call one of
        ///     <list type="bullet">
        ///     <item>result.<see cref="FasterKV{Key, Value, StoreFunctions}.UpsertAsyncResult{Input, Output, Context}.Complete()"/></item>
        ///     <item>result = await result.<see cref="FasterKV{Key, Value, StoreFunctions}.UpsertAsyncResult{Input, Output, Context}.CompleteAsync(CancellationToken)"/> while result.Status is <see cref="Status.IsPending"/></item>
        ///     </list>
        ///     to complete the Upsert operation. Failure to complete the operation will result in leaked allocations.</remarks>
        ValueTask<FasterKV<Key, Value, StoreFunctions>.UpsertAsyncResult<Input, Output, Context>> UpsertAsync(ref Key key, ref Input input, ref Value desiredValue, Context userContext = default, long serialNo = 0, CancellationToken token = default);

        /// <summary>
        /// Async Upsert operation
        /// Await operation in session before issuing next one
        /// </summary>
        /// <param name="key"></param>
        /// <param name="desiredValue"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <param name="token"></param>
        /// <returns>ValueTask wrapping the asyncResult of the operation</returns>
        /// <remarks>The caller must await the return value to obtain the result, then call one of
        ///     <list type="bullet">
        ///     <item>result.<see cref="FasterKV{Key, Value, StoreFunctions}.UpsertAsyncResult{Input, Output, Context}.Complete()"/></item>
        ///     <item>result = await result.<see cref="FasterKV{Key, Value, StoreFunctions}.UpsertAsyncResult{Input, Output, Context}.CompleteAsync(CancellationToken)"/> while result.Status is <see cref="Status.IsPending"/></item>
        ///     </list>
        ///     to complete the Upsert operation. Failure to complete the operation will result in leaked allocations.</remarks>
        ValueTask<FasterKV<Key, Value, StoreFunctions>.UpsertAsyncResult<Input, Output, Context>> UpsertAsync(Key key, Value desiredValue, Context userContext = default, long serialNo = 0, CancellationToken token = default);

        /// <summary>
        /// Async Upsert operation
        /// Await operation in session before issuing next one
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="desiredValue"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <param name="token"></param>
        /// <returns>ValueTask wrapping the asyncResult of the operation</returns>
        /// <remarks>The caller must await the return value to obtain the result, then call one of
        ///     <list type="bullet">
        ///     <item>result.<see cref="FasterKV{Key, Value, StoreFunctions}.UpsertAsyncResult{Input, Output, Context}.Complete()"/></item>
        ///     <item>result = await result.<see cref="FasterKV{Key, Value, StoreFunctions}.UpsertAsyncResult{Input, Output, Context}.CompleteAsync(CancellationToken)"/> while result.Status is <see cref="Status.IsPending"/></item>
        ///     </list>
        ///     to complete the Upsert operation. Failure to complete the operation will result in leaked allocations.</remarks>
        ValueTask<FasterKV<Key, Value, StoreFunctions>.UpsertAsyncResult<Input, Output, Context>> UpsertAsync(Key key, Input input, Value desiredValue, Context userContext = default, long serialNo = 0, CancellationToken token = default);

        /// <summary>
        /// RMW operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        Status RMW(ref Key key, ref Input input, ref Output output, Context userContext = default, long serialNo = 0);

        /// <summary>
        /// RMW operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="recordMetadata"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        Status RMW(ref Key key, ref Input input, ref Output output, out RecordMetadata recordMetadata, Context userContext = default, long serialNo = 0);

        /// <summary>
        /// RMW operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        Status RMW(Key key, Input input, out Output output, Context userContext = default, long serialNo = 0);

        /// <summary>
        /// RMW operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        Status RMW(ref Key key, ref Input input, Context userContext = default, long serialNo = 0);

        /// <summary>
        /// RMW operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        Status RMW(Key key, Input input, Context userContext = default, long serialNo = 0);

        /// <summary>
        /// Async RMW operation
        /// Await operation in session before issuing next one
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="context"></param>
        /// <param name="serialNo"></param>
        /// <param name="token"></param>
        /// <remarks>The caller must await the return value to obtain the result, then call one of
        ///     <list type="bullet">
        ///     <item>result.<see cref="FasterKV{Key, Value, StoreFunctions}.RmwAsyncResult{Input, Output, Context}.Complete()"/></item>
        ///     <item>result = await result.<see cref="FasterKV{Key, Value, StoreFunctions}.RmwAsyncResult{Input, Output, Context}.CompleteAsync(CancellationToken)"/> while result.Status is <see cref="Status.IsPending"/></item>
        ///     </list>
        ///     to complete the Upsert operation. Failure to complete the operation will result in leaked allocations.</remarks>
        ValueTask<FasterKV<Key, Value, StoreFunctions>.RmwAsyncResult<Input, Output, Context>> RMWAsync(ref Key key, ref Input input, Context context = default, long serialNo = 0, CancellationToken token = default);

        /// <summary>
        /// Async RMW operation
        /// Await operation in session before issuing next one
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="context"></param>
        /// <param name="serialNo"></param>
        /// <param name="token"></param>
        /// <remarks>The caller must await the return value to obtain the result, then call one of
        ///     <list type="bullet">
        ///     <item>result.<see cref="FasterKV{Key, Value, StoreFunctions}.RmwAsyncResult{Input, Output, Context}.Complete()"/></item>
        ///     <item>result = await result.<see cref="FasterKV{Key, Value, StoreFunctions}.RmwAsyncResult{Input, Output, Context}.CompleteAsync(CancellationToken)"/> while result.Status is <see cref="Status.IsPending"/></item>
        ///     </list>
        ///     to complete the Upsert operation. Failure to complete the operation will result in leaked allocations.</remarks>
        ValueTask<FasterKV<Key, Value, StoreFunctions>.RmwAsyncResult<Input, Output, Context>> RMWAsync(Key key, Input input, Context context = default, long serialNo = 0, CancellationToken token = default);

        /// <summary>
        /// Delete operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        Status Delete(ref Key key, Context userContext = default, long serialNo = 0);

        /// <summary>
        /// Delete operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        Status Delete(Key key, Context userContext = default, long serialNo = 0);

        /// <summary>
        /// Async Delete operation
        /// Await operation in session before issuing next one
        /// </summary>
        /// <param name="key"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <param name="token"></param>
        /// <remarks>The caller must await the return value to obtain the result, then call one of
        ///     <list type="bullet">
        ///     <item>result.<see cref="FasterKV{Key, Value, StoreFunctions}.DeleteAsyncResult{Input, Output, Context}.Complete()"/></item>
        ///     <item>result = await result.<see cref="FasterKV{Key, Value, StoreFunctions}.DeleteAsyncResult{Input, Output, Context}.CompleteAsync(CancellationToken)"/> while result.Status is <see cref="Status.IsPending"/></item>
        ///     </list>
        ///     to complete the Upsert operation. Failure to complete the operation will result in leaked allocations.</remarks>
        ValueTask<FasterKV<Key, Value, StoreFunctions>.DeleteAsyncResult<Input, Output, Context>> DeleteAsync(ref Key key, Context userContext = default, long serialNo = 0, CancellationToken token = default);

        /// <summary>
        /// Async Delete operation
        /// Await operation in session before issuing next one
        /// </summary>
        /// <param name="key"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <param name="token"></param>
        /// <remarks>The caller must await the return value to obtain the result, then call one of
        ///     <list type="bullet">
        ///     <item>result.<see cref="FasterKV{Key, Value, StoreFunctions}.DeleteAsyncResult{Input, Output, Context}.Complete()"/></item>
        ///     <item>result = await result.<see cref="FasterKV{Key, Value, StoreFunctions}.DeleteAsyncResult{Input, Output, Context}.CompleteAsync(CancellationToken)"/> while result.Status is <see cref="Status.IsPending"/></item>
        ///     </list>
        ///     to complete the Upsert operation. Failure to complete the operation will result in leaked allocations.</remarks>
        ValueTask<FasterKV<Key, Value, StoreFunctions>.DeleteAsyncResult<Input, Output, Context>> DeleteAsync(Key key, Context userContext = default, long serialNo = 0, CancellationToken token = default);

        /// <summary>
        /// Refresh session epoch and handle checkpointing phases. Used only
        /// in case of thread-affinitized sessions (async support is disabled).
        /// </summary>
        public void Refresh();
    }
}
