// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    /// <summary>
    /// Thread-independent session interface to FASTER
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    /// <typeparam name="Input"></typeparam>
    /// <typeparam name="Output"></typeparam>
    /// <typeparam name="Context"></typeparam>
    /// <typeparam name="Functions"></typeparam>
    public sealed class ClientSession<Key, Value, Input, Output, Context, Functions> : IClientSession, IDisposable
        where Functions : IFunctions<Key, Value, Input, Output, Context>
    {
        private readonly FasterKV<Key, Value> fht;

        internal readonly bool SupportAsync = false;
        internal readonly FasterKV<Key, Value>.FasterExecutionContext<Input, Output, Context> ctx;
        internal CommitPoint LatestCommitPoint;

        internal readonly Functions functions;
        internal readonly IVariableLengthStruct<Value, Input> variableLengthStruct;
        internal readonly IVariableLengthStruct<Input> inputVariableLengthStruct;

        internal CompletedOutputIterator<Key, Value, Input, Output, Context> completedOutputs;

        internal readonly InternalFasterSession FasterSession;

        internal const string NotAsyncSessionErr = "Session does not support async operations";

        internal ClientSession(
            FasterKV<Key, Value> fht,
            FasterKV<Key, Value>.FasterExecutionContext<Input, Output, Context> ctx,
            Functions functions,
            bool supportAsync,
            SessionVariableLengthStructSettings<Value, Input> sessionVariableLengthStructSettings = null)
        {
            this.fht = fht;
            this.ctx = ctx;
            this.functions = functions;
            SupportAsync = supportAsync;
            LatestCommitPoint = new CommitPoint { UntilSerialNo = -1, ExcludedSerialNos = null };
            FasterSession = new InternalFasterSession(this);

            this.variableLengthStruct = sessionVariableLengthStructSettings?.valueLength;
            if (this.variableLengthStruct == default)
            {
                UpdateVarlen(ref this.variableLengthStruct);

                if ((this.variableLengthStruct == default) && (fht.hlog is VariableLengthBlittableAllocator<Key, Value> allocator))
                {
                    Debug.WriteLine("Warning: Session did not specify Input-specific functions for variable-length values via IVariableLengthStruct<Value, Input>");
                    this.variableLengthStruct = new DefaultVariableLengthStruct<Value, Input>(allocator.ValueLength);
                }
            }
            else
            {
                if (!(fht.hlog is VariableLengthBlittableAllocator<Key, Value>))
                    Debug.WriteLine("Warning: Session param of variableLengthStruct provided for non-varlen allocator");
            }

            this.inputVariableLengthStruct = sessionVariableLengthStructSettings?.inputLength;

            if (inputVariableLengthStruct == default)
            {
                if (typeof(Input) == typeof(SpanByte))
                {
                    inputVariableLengthStruct = new SpanByteVarLenStruct() as IVariableLengthStruct<Input>;
                }
                else if (typeof(Input).IsGenericType && (typeof(Input).GetGenericTypeDefinition() == typeof(Memory<>)) && Utility.IsBlittableType(typeof(Input).GetGenericArguments()[0]))
                {
                    var m = typeof(MemoryVarLenStruct<>).MakeGenericType(typeof(Input).GetGenericArguments());
                    object o = Activator.CreateInstance(m);
                    inputVariableLengthStruct = o as IVariableLengthStruct<Input>;
                }
                else if (typeof(Input).IsGenericType && (typeof(Input).GetGenericTypeDefinition() == typeof(ReadOnlyMemory<>)) && Utility.IsBlittableType(typeof(Input).GetGenericArguments()[0]))
                {
                    var m = typeof(ReadOnlyMemoryVarLenStruct<>).MakeGenericType(typeof(Input).GetGenericArguments());
                    object o = Activator.CreateInstance(m);
                    inputVariableLengthStruct = o as IVariableLengthStruct<Input>;
                }
            }

            // Session runs on a single thread
            if (!supportAsync)
                UnsafeResumeThread();
        }

        private void UpdateVarlen(ref IVariableLengthStruct<Value, Input> variableLengthStruct)
        {
            if (!(fht.hlog is VariableLengthBlittableAllocator<Key, Value>))
                return;

            if (typeof(Value) == typeof(SpanByte) && typeof(Input) == typeof(SpanByte))
            {
                variableLengthStruct = new SpanByteVarLenStructForSpanByteInput() as IVariableLengthStruct<Value, Input>;
            }
            else if (typeof(Value).IsGenericType && (typeof(Value).GetGenericTypeDefinition() == typeof(Memory<>)) && Utility.IsBlittableType(typeof(Value).GetGenericArguments()[0]))
            {
                if (typeof(Input).IsGenericType && (typeof(Input).GetGenericTypeDefinition() == typeof(Memory<>)) && typeof(Input).GetGenericArguments()[0] == typeof(Value).GetGenericArguments()[0])
                {
                    var m = typeof(MemoryVarLenStructForMemoryInput<>).MakeGenericType(typeof(Value).GetGenericArguments());
                    object o = Activator.CreateInstance(m);
                    variableLengthStruct = o as IVariableLengthStruct<Value, Input>;
                }
                else if (typeof(Input).IsGenericType && (typeof(Input).GetGenericTypeDefinition() == typeof(ReadOnlyMemory<>)) && typeof(Input).GetGenericArguments()[0] == typeof(Value).GetGenericArguments()[0])
                {
                    var m = typeof(MemoryVarLenStructForReadOnlyMemoryInput<>).MakeGenericType(typeof(Value).GetGenericArguments());
                    object o = Activator.CreateInstance(m);
                    variableLengthStruct = o as IVariableLengthStruct<Value, Input>;
                }
            }
        }

        /// <summary>
        /// Get session ID
        /// </summary>
        public string ID { get { return ctx.guid; } }

        /// <summary>
        /// Next sequential serial no for session (current serial no + 1)
        /// </summary>
        public long NextSerialNo => ctx.serialNum + 1;

        /// <summary>
        /// Current serial no for session
        /// </summary>
        public long SerialNo => ctx.serialNum;

        /// <summary>
        /// Current version number of the session
        /// </summary>
        public long Version => ctx.version;

        /// <summary>
        /// Dispose session
        /// </summary>
        public void Dispose()
        {
            this.completedOutputs?.Dispose();
            CompletePending(true);
            fht.DisposeClientSession(ID);

            // Session runs on a single thread
            if (!SupportAsync)
                UnsafeSuspendThread();
        }

        /// <summary>
        /// Read operation
        /// </summary>
        /// <param name="key">The key to look up</param>
        /// <param name="input">Input to help extract the retrieved value into <paramref name="output"/></param>
        /// <param name="output">The location to place the retrieved value</param>
        /// <param name="userContext">User application context passed in case the read goes pending due to IO</param>
        /// <param name="serialNo">The serial number of the operation (used in recovery)</param>
        /// <returns><paramref name="output"/> is populated by the <see cref="IFunctions{Key, Value, Context}"/> implementation</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(ref Key key, ref Input input, ref Output output, Context userContext = default, long serialNo = 0)
        {
            if (SupportAsync) UnsafeResumeThread();
            try
            {
                return fht.ContextRead(ref key, ref input, ref output, userContext, FasterSession, serialNo, ctx);
            }
            finally
            {
                if (SupportAsync) UnsafeSuspendThread();
            }
        }

        /// <summary>
        /// Read operation
        /// </summary>
        /// <param name="key">The key to look up</param>
        /// <param name="input">Input to help extract the retrieved value into <paramref name="output"/></param>
        /// <param name="output">The location to place the retrieved value</param>
        /// <param name="userContext">User application context passed in case the read goes pending due to IO</param>
        /// <param name="serialNo">The serial number of the operation (used in recovery)</param>
        /// <returns><paramref name="output"/> is populated by the <see cref="IFunctions{Key, Value, Context}"/> implementation</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(Key key, Input input, out Output output, Context userContext = default, long serialNo = 0)
        {
            output = default;
            return Read(ref key, ref input, ref output, userContext, serialNo);
        }

        /// <summary>
        /// Read operation
        /// </summary>
        /// <param name="key">The key to look up</param>
        /// <param name="output">The location to place the retrieved value</param>
        /// <param name="userContext">User application context passed in case the read goes pending due to IO</param>
        /// <param name="serialNo">The serial number of the operation (used in recovery)</param>
        /// <returns><paramref name="output"/> is populated by the <see cref="IFunctions{Key, Value, Context}"/> implementation</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(ref Key key, ref Output output, Context userContext = default, long serialNo = 0)
        {
            Input input = default;
            return Read(ref key, ref input, ref output, userContext, serialNo);
        }

        /// <summary>
        /// Read operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="output"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(Key key, out Output output, Context userContext = default, long serialNo = 0)
        {
            Input input = default;
            output = default;
            return Read(ref key, ref input, ref output, userContext, serialNo);
        }

        /// <summary>
        /// Read operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (Status status, Output output) Read(Key key, Context userContext = default, long serialNo = 0)
        {
            Input input = default;
            Output output = default;
            return (Read(ref key, ref input, ref output, userContext, serialNo), output);
        }

        /// <summary>
        /// Read operation that accepts a <paramref name="recordMetadata"/> ref argument to start the lookup at instead of starting at the hash table entry for <paramref name="key"/>,
        ///     and is updated with the address and record header for the found record.
        /// </summary>
        /// <param name="key">The key to look up</param>
        /// <param name="input">Input to help extract the retrieved value into <paramref name="output"/></param>
        /// <param name="output">The location to place the retrieved value</param>
        /// <param name="recordMetadata">On input contains the address to start at in <paramref name="recordMetadata.RecordInfo.PreviousAddress"/>; if this is Constants.kInvalidAddress, the
        ///     search starts with the key as in other forms of Read.
        ///     <para>On output, receives:
        ///         <list type="bullet">
        ///             <li>The address of the found record. This may be different from the <paramref name="recordMetadata.RecordInfo.PreviousAddress"/> passed on the call, due to
        ///                 tracing back over hash collisions until we arrive at the key match</li>
        ///             <li>A copy of the record's header in <paramref name="recordMetadata.RecordInfo"/>; <paramref name="recordMetadata.RecordInfo.PreviousAddress"/> can be passed
        ///                 in a subsequent call, thereby enumerating all records in a key's hash chain.</li>
        ///         </list>
        ///     </para>
        /// </param>
        /// <param name="readFlags">Flags for controlling operations within the read, such as ReadCache interaction</param>
        /// <param name="userContext">User application context passed in case the read goes pending due to IO</param>
        /// <param name="serialNo">The serial number of the operation (used in recovery)</param>
        /// <returns><paramref name="output"/> is populated by the <see cref="IFunctions{Key, Value, Context}"/> implementation</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(ref Key key, ref Input input, ref Output output, ref RecordMetadata recordMetadata, ReadFlags readFlags = ReadFlags.None, Context userContext = default, long serialNo = 0)
        {
            if (SupportAsync) UnsafeResumeThread();
            try
            {
                return fht.ContextRead(ref key, ref input, ref output, ref recordMetadata, readFlags, userContext, FasterSession, serialNo, ctx);
            }
            finally
            {
                if (SupportAsync) UnsafeSuspendThread();
            }
        }

        /// <summary>
        /// Read operation that accepts an <paramref name="address"/> argument to lookup at, instead of a key.
        /// </summary>
        /// <param name="address">The address to look up</param>
        /// <param name="input">Input to help extract the retrieved value into <paramref name="output"/></param>
        /// <param name="output">The location to place the retrieved value</param>
        /// <param name="readFlags">Flags for controlling operations within the read, such as ReadCache interaction</param>
        /// <param name="userContext">User application context passed in case the read goes pending due to IO</param>
        /// <param name="serialNo">The serial number of the operation (used in recovery)</param>
        /// <returns><paramref name="output"/> is populated by the <see cref="IFunctions{Key, Value, Context}"/> implementation; this should store the key if it needs it</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status ReadAtAddress(long address, ref Input input, ref Output output, ReadFlags readFlags = ReadFlags.None, Context userContext = default, long serialNo = 0)
        {
            if (SupportAsync) UnsafeResumeThread();
            try
            {
                return fht.ContextReadAtAddress(address, ref input, ref output, readFlags, userContext, FasterSession, serialNo, ctx);
            }
            finally
            {
                if (SupportAsync) UnsafeSuspendThread();
            }
        }

        /// <summary>
        /// Async read operation. May return uncommitted results; to ensure reading of committed results, complete the read and then call WaitForCommitAsync.
        /// </summary>
        /// <param name="key">The key to look up</param>
        /// <param name="input">Input to help extract the retrieved value into output</param>
        /// <param name="userContext">User application context passed in case the read goes pending due to IO</param>
        /// <param name="serialNo">The serial number of the operation (used in recovery)</param>
        /// <param name="cancellationToken">Token to cancel the operation</param>
        /// <returns><see cref="ValueTask"/> wrapping <see cref="FasterKV{Key, Value}.ReadAsyncResult{Input, Output, Context}"/></returns>
        /// <remarks>The caller must await the return value to obtain the result, then call one of
        ///     <list type="bullet">
        ///     <item>result.<see cref="FasterKV{Key, Value}.ReadAsyncResult{Input, Output, Context}.Complete()"/></item>
        ///     <item>result.<see cref="FasterKV{Key, Value}.ReadAsyncResult{Input, Output, Context}.Complete(out RecordMetadata)"/></item>
        ///     </list>
        ///     to complete the read operation and obtain the result status, the output that is populated by the 
        ///     <see cref="IFunctions{Key, Value, Context}"/> implementation, and optionally a copy of the header for the retrieved record</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(ref Key key, ref Input input, Context userContext = default, long serialNo = 0, CancellationToken cancellationToken = default)
        {
            Debug.Assert(SupportAsync, NotAsyncSessionErr);
            return fht.ReadAsync(this.FasterSession, this.ctx, ref key, ref input, Constants.kInvalidAddress, userContext, serialNo, cancellationToken);
        }

        /// <summary>
        /// Async read operation, may return uncommitted result
        /// To ensure reading of committed result, complete the read and then call WaitForCommitAsync.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="context"></param>
        /// <param name="serialNo"></param>
        /// <param name="token"></param>
        /// <returns><see cref="ValueTask"/> wrapping <see cref="FasterKV{Key, Value}.ReadAsyncResult{Input, Output, Context}"/></returns>
        /// <remarks>The caller must await the return value to obtain the result, then call one of
        ///     <list type="bullet">
        ///     <item>result.<see cref="FasterKV{Key, Value}.ReadAsyncResult{Input, Output, Context}.Complete()"/></item>
        ///     <item>result.<see cref="FasterKV{Key, Value}.ReadAsyncResult{Input, Output, Context}.Complete(out RecordMetadata)"/></item>
        ///     </list>
        ///     to complete the read operation and obtain the result status, the output that is populated by the 
        ///     <see cref="IFunctions{Key, Value, Context}"/> implementation, and optionally a copy of the header for the retrieved record</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(Key key, Input input, Context context = default, long serialNo = 0, CancellationToken token = default)
        {
            Debug.Assert(SupportAsync, NotAsyncSessionErr);
            return fht.ReadAsync(this.FasterSession, this.ctx, ref key, ref input, Constants.kInvalidAddress, context, serialNo, token);
        }

        /// <summary>
        /// Async read operation. May return uncommitted results; to ensure reading of committed results, complete the read and then call WaitForCommitAsync.
        /// </summary>
        /// <param name="key">The key to look up</param>
        /// <param name="userContext">User application context passed in case the read goes pending due to IO</param>
        /// <param name="serialNo">The serial number of the operation (used in recovery)</param>
        /// <param name="token">Token to cancel the operation</param>
        /// <returns><see cref="ValueTask"/> wrapping <see cref="FasterKV{Key, Value}.ReadAsyncResult{Input, Output, Context}"/></returns>
        /// <remarks>The caller must await the return value to obtain the result, then call one of
        ///     <list type="bullet">
        ///     <item>result.<see cref="FasterKV{Key, Value}.ReadAsyncResult{Input, Output, Context}.Complete()"/></item>
        ///     <item>result.<see cref="FasterKV{Key, Value}.ReadAsyncResult{Input, Output, Context}.Complete(out RecordMetadata)"/></item>
        ///     </list>
        ///     to complete the read operation and obtain the result status, the output that is populated by the 
        ///     <see cref="IFunctions{Key, Value, Context}"/> implementation, and optionally a copy of the header for the retrieved record</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(ref Key key, Context userContext = default, long serialNo = 0, CancellationToken token = default)
        {
            Debug.Assert(SupportAsync, NotAsyncSessionErr);
            Input input = default;
            return fht.ReadAsync(this.FasterSession, this.ctx, ref key, ref input, Constants.kInvalidAddress, userContext, serialNo, token);
        }

        /// <summary>
        /// Async read operation, may return uncommitted result
        /// To ensure reading of committed result, complete the read and then call WaitForCommitAsync.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="context"></param>
        /// <param name="serialNo"></param>
        /// <param name="token"></param>
        /// <returns><see cref="ValueTask"/> wrapping <see cref="FasterKV{Key, Value}.ReadAsyncResult{Input, Output, Context}"/></returns>
        /// <remarks>The caller must await the return value to obtain the result, then call one of
        ///     <list type="bullet">
        ///     <item>result.<see cref="FasterKV{Key, Value}.ReadAsyncResult{Input, Output, Context}.Complete()"/></item>
        ///     <item>result.<see cref="FasterKV{Key, Value}.ReadAsyncResult{Input, Output, Context}.Complete(out RecordMetadata)"/></item>
        ///     </list>
        ///     to complete the read operation and obtain the result status, the output that is populated by the 
        ///     <see cref="IFunctions{Key, Value, Context}"/> implementation, and optionally a copy of the header for the retrieved record</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(Key key, Context context = default, long serialNo = 0, CancellationToken token = default)
        {
            Debug.Assert(SupportAsync, NotAsyncSessionErr);
            Input input = default;
            return fht.ReadAsync(this.FasterSession, this.ctx, ref key, ref input, Constants.kInvalidAddress, context, serialNo, token);
        }

        /// <summary>
        /// Async read operation that accepts a <paramref name="startAddress"/> to start the lookup at instead of starting at the hash table entry for <paramref name="key"/>,
        ///     and returns the <see cref="RecordInfo"/> for the found record (which contains previous address in the hash chain for this key; this can
        ///     be used as <paramref name="startAddress"/> in a subsequent call to iterate all records for <paramref name="key"/>).
        /// </summary>
        /// <param name="key">The key to look up</param>
        /// <param name="input">Input to help extract the retrieved value into output</param>
        /// <param name="startAddress">Start at this address rather than the address in the hash table for <paramref name="key"/>"/></param>
        /// <param name="readFlags">Flags for controlling operations within the read, such as ReadCache interaction</param>
        /// <param name="userContext">User application context passed in case the read goes pending due to IO</param>
        /// <param name="serialNo">The serial number of the operation (used in recovery)</param>
        /// <param name="cancellationToken">Token to cancel the operation</param>
        /// <returns><see cref="ValueTask"/> wrapping <see cref="FasterKV{Key, Value}.ReadAsyncResult{Input, Output, Context}"/></returns>
        /// <remarks>The caller must await the return value to obtain the result, then call one of
        ///     <list type="bullet">
        ///     <item>result.<see cref="FasterKV{Key, Value}.ReadAsyncResult{Input, Output, Context}.Complete()"/></item>
        ///     <item>result.<see cref="FasterKV{Key, Value}.ReadAsyncResult{Input, Output, Context}.Complete(out RecordMetadata)"/></item>
        ///     </list>
        ///     to complete the read operation and obtain the result status, the output that is populated by the 
        ///     <see cref="IFunctions{Key, Value, Context}"/> implementation, and optionally a copy of the header for the retrieved record
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(ref Key key, ref Input input, long startAddress, ReadFlags readFlags = ReadFlags.None,
                                                                                                 Context userContext = default, long serialNo = 0, CancellationToken cancellationToken = default)
        {
            Debug.Assert(SupportAsync, NotAsyncSessionErr);
            var operationFlags = FasterKV<Key, Value>.PendingContext<Input, Output, Context>.GetOperationFlags(readFlags);
            return fht.ReadAsync(this.FasterSession, this.ctx, ref key, ref input, startAddress, userContext, serialNo, cancellationToken, operationFlags);
        }

        /// <summary>
        /// Async Read operation that accepts an <paramref name="address"/> argument to lookup at, instead of a key.
        /// </summary>
        /// <param name="address">The address to look up</param>
        /// <param name="input">Input to help extract the retrieved value into output</param>
        /// <param name="readFlags">Flags for controlling operations within the read, such as ReadCache interaction</param>
        /// <param name="userContext">User application context passed in case the read goes pending due to IO</param>
        /// <param name="serialNo">The serial number of the operation (used in recovery)</param>
        /// <param name="cancellationToken">Token to cancel the operation</param>
        /// <returns><see cref="ValueTask"/> wrapping <see cref="FasterKV{Key, Value}.ReadAsyncResult{Input, Output, Context}"/></returns>
        /// <remarks>The caller must await the return value to obtain the result, then call one of
        ///     <list type="bullet">
        ///     <item>result.<see cref="FasterKV{Key, Value}.ReadAsyncResult{Input, Output, Context}.Complete()"/></item>
        ///     <item>result.<see cref="FasterKV{Key, Value}.ReadAsyncResult{Input, Output, Context}.Complete(out RecordMetadata)"/></item>
        ///     </list>
        ///     to complete the read operation and obtain the result status, the output that is populated by the 
        ///     <see cref="IFunctions{Key, Value, Context}"/> implementation, and optionally a copy of the header for the retrieved record</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAtAddressAsync(long address, ref Input input, ReadFlags readFlags = ReadFlags.None,
                                                                                                          Context userContext = default, long serialNo = 0, CancellationToken cancellationToken = default)
        {
            Debug.Assert(SupportAsync, NotAsyncSessionErr);
            Key key = default;
            var operationFlags = FasterKV<Key, Value>.PendingContext<Input, Output, Context>.GetOperationFlags(readFlags, noKey: true);
            return fht.ReadAsync(this.FasterSession, this.ctx, ref key, ref input, address, userContext, serialNo, cancellationToken, operationFlags);
        }

        /// <summary>
        /// Upsert operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="desiredValue"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref Key key, ref Value desiredValue, Context userContext = default, long serialNo = 0)
        {
            Input input = default;
            return Upsert(ref key, ref input, ref desiredValue, userContext, serialNo);
        }

        /// <summary>
        /// Upsert operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="desiredValue"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref Key key, ref Input input, ref Value desiredValue, Context userContext = default, long serialNo = 0)
        {
            if (SupportAsync) UnsafeResumeThread();
            try
            {
                return fht.ContextUpsert(ref key, ref input, ref desiredValue, userContext, FasterSession, serialNo, ctx);
            }
            finally
            {
                if (SupportAsync) UnsafeSuspendThread();
            }
        }

        /// <summary>
        /// Upsert operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="desiredValue"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(Key key, Value desiredValue, Context userContext = default, long serialNo = 0)
            => Upsert(ref key, ref desiredValue, userContext, serialNo);

        /// <summary>
        /// Upsert operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="desiredValue"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(Key key, Input input, Value desiredValue, Context userContext = default, long serialNo = 0)
            => Upsert(ref key, ref input, ref desiredValue, userContext, serialNo);

        /// <summary>
        /// Async Upsert operation
        /// Await operation in session before issuing next one
        /// </summary>
        /// <param name="key"></param>
        /// <param name="desiredValue"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <param name="token"></param>
        /// <returns>ValueTask wrapping <see cref="FasterKV{Key, Value}.UpsertAsyncResult{Input, Output, Context}"/></returns>
        /// <remarks>The caller must await the return value to obtain the result, then call one of
        ///     <list type="bullet">
        ///     <item>result.<see cref="FasterKV{Key, Value}.UpsertAsyncResult{Input, Output, Context}.Complete()"/></item>
        ///     <item>result = await result.<see cref="FasterKV{Key, Value}.UpsertAsyncResult{Input, Output, Context}.CompleteAsync(CancellationToken)"/> while result.Status == <see cref="Status.PENDING"/></item>
        ///     </list>
        ///     to complete the Upsert operation. Failure to complete the operation will result in leaked allocations.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value>.UpsertAsyncResult<Input, Output, Context>> UpsertAsync(ref Key key, ref Value desiredValue, Context userContext = default, long serialNo = 0, CancellationToken token = default)
        {
            Input input = default;
            return UpsertAsync(ref key, ref input, ref desiredValue, userContext, serialNo, token);
        }

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
        /// <returns>ValueTask wrapping <see cref="FasterKV{Key, Value}.UpsertAsyncResult{Input, Output, Context}"/></returns>
        /// <remarks>The caller must await the return value to obtain the result, then call one of
        ///     <list type="bullet">
        ///     <item>result.<see cref="FasterKV{Key, Value}.UpsertAsyncResult{Input, Output, Context}.Complete()"/></item>
        ///     <item>result = await result.<see cref="FasterKV{Key, Value}.UpsertAsyncResult{Input, Output, Context}.CompleteAsync(CancellationToken)"/> while result.Status == <see cref="Status.PENDING"/></item>
        ///     </list>
        ///     to complete the Upsert operation. Failure to complete the operation will result in leaked allocations.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value>.UpsertAsyncResult<Input, Output, Context>> UpsertAsync(ref Key key, ref Input input, ref Value desiredValue, Context userContext = default, long serialNo = 0, CancellationToken token = default)
        {
            Debug.Assert(SupportAsync, NotAsyncSessionErr);
            return fht.UpsertAsync(this.FasterSession, this.ctx, ref key, ref input, ref desiredValue, userContext, serialNo, token);
        }

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
        ///     <item>result.<see cref="FasterKV{Key, Value}.UpsertAsyncResult{Input, Output, Context}.Complete()"/></item>
        ///     <item>result = await result.<see cref="FasterKV{Key, Value}.UpsertAsyncResult{Input, Output, Context}.CompleteAsync(CancellationToken)"/> while result.Status == <see cref="Status.PENDING"/></item>
        ///     </list>
        ///     to complete the Upsert operation. Failure to complete the operation will result in leaked allocations.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value>.UpsertAsyncResult<Input, Output, Context>> UpsertAsync(Key key, Value desiredValue, Context userContext = default, long serialNo = 0, CancellationToken token = default)
        {
            return UpsertAsync(ref key, ref desiredValue, userContext, serialNo, token);
        }

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
        ///     <item>result.<see cref="FasterKV{Key, Value}.UpsertAsyncResult{Input, Output, Context}.Complete()"/></item>
        ///     <item>result = await result.<see cref="FasterKV{Key, Value}.UpsertAsyncResult{Input, Output, Context}.CompleteAsync(CancellationToken)"/> while result.Status == <see cref="Status.PENDING"/></item>
        ///     </list>
        ///     to complete the Upsert operation. Failure to complete the operation will result in leaked allocations.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value>.UpsertAsyncResult<Input, Output, Context>> UpsertAsync(Key key, Input input, Value desiredValue, Context userContext = default, long serialNo = 0, CancellationToken token = default)
            => UpsertAsync(ref key, ref input, ref desiredValue, userContext, serialNo, token);

        /// <summary>
        /// RMW operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ref Key key, ref Input input, ref Output output, Context userContext = default, long serialNo = 0) 
            => RMW(ref key, ref input, ref output, out _, userContext, serialNo);

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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ref Key key, ref Input input, ref Output output, out RecordMetadata recordMetadata, Context userContext = default, long serialNo = 0)
        {
            if (SupportAsync) UnsafeResumeThread();
            try
            {
                return fht.ContextRMW(ref key, ref input, ref output, out recordMetadata, userContext, FasterSession, serialNo, ctx);
            }
            finally
            {
                if (SupportAsync) UnsafeSuspendThread();
            }
        }

        /// <summary>
        /// RMW operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(Key key, Input input, out Output output, Context userContext = default, long serialNo = 0)
        {
            output = default;
            return RMW(ref key, ref input, ref output, userContext, serialNo);
        }

        /// <summary>
        /// RMW operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ref Key key, ref Input input, Context userContext = default, long serialNo = 0)
        {
            Output output = default;
            return RMW(ref key, ref input, ref output, userContext, serialNo);
        }

        /// <summary>
        /// RMW operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(Key key, Input input, Context userContext = default, long serialNo = 0)
        {
            Output output = default;
            return RMW(ref key, ref input, ref output, userContext, serialNo);
        }

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
        ///     <item>result.<see cref="FasterKV{Key, Value}.RmwAsyncResult{Input, Output, Context}.Complete()"/></item>
        ///     <item>result = await result.<see cref="FasterKV{Key, Value}.RmwAsyncResult{Input, Output, Context}.CompleteAsync(CancellationToken)"/> while result.Status == <see cref="Status.PENDING"/></item>
        ///     </list>
        ///     to complete the Upsert operation. Failure to complete the operation will result in leaked allocations.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value>.RmwAsyncResult<Input, Output, Context>> RMWAsync(ref Key key, ref Input input, Context context = default, long serialNo = 0, CancellationToken token = default)
        {
            Debug.Assert(SupportAsync, NotAsyncSessionErr);
            return fht.RmwAsync(this.FasterSession, this.ctx, ref key, ref input, context, serialNo, token);
        }

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
        ///     <item>result.<see cref="FasterKV{Key, Value}.RmwAsyncResult{Input, Output, Context}.Complete()"/></item>
        ///     <item>result = await result.<see cref="FasterKV{Key, Value}.RmwAsyncResult{Input, Output, Context}.CompleteAsync(CancellationToken)"/> while result.Status == <see cref="Status.PENDING"/></item>
        ///     </list>
        ///     to complete the Upsert operation. Failure to complete the operation will result in leaked allocations.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value>.RmwAsyncResult<Input, Output, Context>> RMWAsync(Key key, Input input, Context context = default, long serialNo = 0, CancellationToken token = default)
            => RMWAsync(ref key, ref input, context, serialNo, token);

        /// <summary>
        /// Delete operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete(ref Key key, Context userContext = default, long serialNo = 0)
        {
            if (SupportAsync) UnsafeResumeThread();
            try
            {
                return fht.ContextDelete(ref key, userContext, FasterSession, serialNo, ctx);
            }
            finally
            {
                if (SupportAsync) UnsafeSuspendThread();
            }
        }

        /// <summary>
        /// Delete operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete(Key key, Context userContext = default, long serialNo = 0)
            => Delete(ref key, userContext, serialNo);

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
        ///     <item>result.<see cref="FasterKV{Key, Value}.DeleteAsyncResult{Input, Output, Context}.Complete()"/></item>
        ///     <item>result = await result.<see cref="FasterKV{Key, Value}.DeleteAsyncResult{Input, Output, Context}.CompleteAsync(CancellationToken)"/> while result.Status == <see cref="Status.PENDING"/></item>
        ///     </list>
        ///     to complete the Upsert operation. Failure to complete the operation will result in leaked allocations.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value>.DeleteAsyncResult<Input, Output, Context>> DeleteAsync(ref Key key, Context userContext = default, long serialNo = 0, CancellationToken token = default)
        {
            Debug.Assert(SupportAsync, NotAsyncSessionErr);
            return fht.DeleteAsync(this.FasterSession, this.ctx, ref key, userContext, serialNo, token);
        }

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
        ///     <item>result.<see cref="FasterKV{Key, Value}.DeleteAsyncResult{Input, Output, Context}.Complete()"/></item>
        ///     <item>result = await result.<see cref="FasterKV{Key, Value}.DeleteAsyncResult{Input, Output, Context}.CompleteAsync(CancellationToken)"/> while result.Status == <see cref="Status.PENDING"/></item>
        ///     </list>
        ///     to complete the Upsert operation. Failure to complete the operation will result in leaked allocations.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value>.DeleteAsyncResult<Input, Output, Context>> DeleteAsync(Key key, Context userContext = default, long serialNo = 0, CancellationToken token = default)
            => DeleteAsync(ref key, userContext, serialNo, token);

        /// <summary>
        /// Experimental feature
        /// Checks whether specified record is present in memory
        /// (between HeadAddress and tail, or between fromAddress
        /// and tail), including tombstones.
        /// </summary>
        /// <param name="key">Key of the record.</param>
        /// <param name="logicalAddress">Logical address of record, if found</param>
        /// <param name="fromAddress">Look until this address</param>
        /// <returns>Status</returns>
        internal Status ContainsKeyInMemory(ref Key key, out long logicalAddress, long fromAddress = -1)
        {
            if (SupportAsync) UnsafeResumeThread();
            try
            {
                return fht.InternalContainsKeyInMemory(ref key, ctx, FasterSession, out logicalAddress, fromAddress);
            }
            finally
            {
                if (SupportAsync) UnsafeSuspendThread();
            }
        }

        /// <summary>
        /// Get list of pending requests (for current session)
        /// </summary>
        /// <returns></returns>
        public IEnumerable<long> GetPendingRequests()
        {
            foreach (var kvp in ctx.prevCtx?.ioPendingRequests)
                yield return kvp.Value.serialNum;

            foreach (var val in ctx.prevCtx?.retryRequests)
                yield return val.serialNum;

            foreach (var kvp in ctx.ioPendingRequests)
                yield return kvp.Value.serialNum;

            foreach (var val in ctx.retryRequests)
                yield return val.serialNum;
        }

        /// <summary>
        /// Refresh session epoch and handle checkpointing phases. Used only
        /// in case of thread-affinitized sessions (async support is disabled).
        /// </summary>
        public void Refresh()
        {
            if (SupportAsync) UnsafeResumeThread();
            fht.InternalRefresh(ctx, FasterSession);
            if (SupportAsync) UnsafeSuspendThread();
        }

        /// <summary>
        /// Synchronously complete outstanding pending synchronous operations.
        /// Async operations must be completed individually.
        /// </summary>
        /// <param name="wait">Wait for all pending operations on session to complete</param>
        /// <param name="spinWaitForCommit">Spin-wait until ongoing commit/checkpoint, if any, completes</param>
        /// <returns>True if all pending operations have completed, false otherwise</returns>
        public bool CompletePending(bool wait = false, bool spinWaitForCommit = false)
            => CompletePending(false, wait, spinWaitForCommit);

        /// <summary>
        /// Synchronously complete outstanding pending synchronous operations, returning outputs for the completed operations.
        /// Async operations must be completed individually.
        /// </summary>
        /// <param name="completedOutputs">Outputs completed by this operation</param>
        /// <param name="wait">Wait for all pending operations on session to complete</param>
        /// <param name="spinWaitForCommit">Spin-wait until ongoing commit/checkpoint, if any, completes</param>
        /// <returns>True if all pending operations have completed, false otherwise</returns>
        public bool CompletePendingWithOutputs(out CompletedOutputIterator<Key, Value, Input, Output, Context> completedOutputs, bool wait = false, bool spinWaitForCommit = false)
        {
            InitializeCompletedOutputs();
            var result = CompletePending(true, wait, spinWaitForCommit);
            completedOutputs = this.completedOutputs;
            return result;
        }

        void InitializeCompletedOutputs()
        {
            if (this.completedOutputs is null)
                this.completedOutputs = new CompletedOutputIterator<Key, Value, Input, Output, Context>();
            else
                this.completedOutputs.Dispose();
        }

        private bool CompletePending(bool getOutputs, bool wait, bool spinWaitForCommit)
        {
            if (SupportAsync) UnsafeResumeThread();
            try
            {
                var requestedOutputs = getOutputs ? this.completedOutputs : default;
                var result = fht.InternalCompletePending(ctx, FasterSession, wait, requestedOutputs);
                if (spinWaitForCommit)
                {
                    if (wait != true)
                    {
                        throw new FasterException("Can spin-wait for commit (checkpoint completion) only if wait is true");
                    }
                    do
                    {
                        fht.InternalCompletePending(ctx, FasterSession, wait, requestedOutputs);
                        if (fht.InRestPhase())
                        {
                            fht.InternalCompletePending(ctx, FasterSession, wait, requestedOutputs);
                            return true;
                        }
                    } while (wait);
                }
                return result;
            }
            finally
            {
                if (SupportAsync) UnsafeSuspendThread();
            }
        }

        /// <summary>
        /// Complete all pending synchronous FASTER operations.
        /// Async operations must be completed individually.
        /// </summary>
        /// <returns></returns>
        public ValueTask CompletePendingAsync(bool waitForCommit = false, CancellationToken token = default)
            => CompletePendingAsync(false, waitForCommit, token);

        /// <summary>
        /// Complete all pending synchronous FASTER operations, returning outputs for the completed operations.
        /// Async operations must be completed individually.
        /// </summary>
        /// <returns>Outputs completed by this operation</returns>
        public async ValueTask<CompletedOutputIterator<Key, Value, Input, Output, Context>> CompletePendingWithOutputsAsync(bool waitForCommit = false, CancellationToken token = default)
        {
            InitializeCompletedOutputs();
            await CompletePendingAsync(true, waitForCommit, token).ConfigureAwait(false);
            return this.completedOutputs;
        }

        private async ValueTask CompletePendingAsync(bool getOutputs, bool waitForCommit = false, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();

            if (fht.epoch.ThisInstanceProtected())
                throw new NotSupportedException("Async operations not supported over protected epoch");

            // Complete all pending operations on session
            await fht.CompletePendingAsync(this.FasterSession, this.ctx, token, getOutputs ? this.completedOutputs : null).ConfigureAwait(false);

            // Wait for commit if necessary
            if (waitForCommit)
                await WaitForCommitAsync(token).ConfigureAwait(false);
        }

        /// <summary>
        /// Check if at least one synchronous request is ready for CompletePending to be called on
        /// Returns completed immediately if there are no outstanding synchronous requests
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        public async ValueTask ReadyToCompletePendingAsync(CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();

            if (fht.epoch.ThisInstanceProtected())
                throw new NotSupportedException("Async operations not supported over protected epoch");

            await fht.ReadyToCompletePendingAsync(this.ctx, token).ConfigureAwait(false);
        }

        /// <summary>
        /// Wait for commit of all operations completed until the current point in session.
        /// Does not itself issue checkpoint/commits.
        /// </summary>
        /// <returns></returns>
        public async ValueTask WaitForCommitAsync(CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();

            if (!ctx.prevCtx.pendingReads.IsEmpty || !ctx.pendingReads.IsEmpty)
                throw new FasterException("Make sure all async operations issued on this session are awaited and completed first");

            // Complete all pending sync operations on session
            await CompletePendingAsync(token: token).ConfigureAwait(false);

            var task = fht.CheckpointTask;
            CommitPoint localCommitPoint = LatestCommitPoint;
            if (localCommitPoint.UntilSerialNo >= ctx.serialNum && localCommitPoint.ExcludedSerialNos?.Count == 0)
                return;

            while (true)
            {
                await task.WithCancellationAsync(token).ConfigureAwait(false);
                Refresh();

                task = fht.CheckpointTask;
                localCommitPoint = LatestCommitPoint;
                if (localCommitPoint.UntilSerialNo >= ctx.serialNum && localCommitPoint.ExcludedSerialNos?.Count == 0)
                    break;
            }
        }

        /// <summary>
        /// Compact the log until specified address using current session, moving active records to the tail of the log. 
        /// </summary>
        /// <param name="untilAddress">Compact log until this address</param>
        /// <param name="shiftBeginAddress">Whether to shift begin address to untilAddress after compaction. To avoid
        /// data loss on failure, set this to false, and shift begin address only after taking a checkpoint. This
        /// ensures that records written to the tail during compaction are first made stable.</param>
        /// <returns>Address until which compaction was done</returns>
        public long Compact(long untilAddress, bool shiftBeginAddress)
        {
            return Compact(untilAddress, shiftBeginAddress, default(DefaultCompactionFunctions<Key, Value>));
        }

        /// <summary>
        /// Compact the log until specified address using current session, moving active records to the tail of the log.
        /// </summary>
        /// <param name="untilAddress">Compact log until this address</param>
        /// <param name="shiftBeginAddress">Whether to shift begin address to untilAddress after compaction. To avoid
        /// <param name="compactionFunctions">User provided compaction functions (see <see cref="ICompactionFunctions{Key, Value}"/>).</param>
        /// data loss on failure, set this to false, and shift begin address only after taking a checkpoint. This
        /// ensures that records written to the tail during compaction are first made stable.</param>
        /// <returns>Address until which compaction was done</returns>
        public long Compact<CompactionFunctions>(long untilAddress, bool shiftBeginAddress, CompactionFunctions compactionFunctions)
            where CompactionFunctions : ICompactionFunctions<Key, Value>
        {
            if (!SupportAsync)
                throw new FasterException("Do not perform compaction using a threadAffinitized session");
            return fht.Log.Compact<Input, Output, Context, Functions, CompactionFunctions>(functions, compactionFunctions, untilAddress, shiftBeginAddress);
        }

        /// <summary>
        /// Insert key and value with the record info preserved.
        /// Succeed only if logical address of the key isn't greater than foundLogicalAddress; otherwise give up and return.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="desiredValue"></param>
        /// <param name="foundLogicalAddress"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void CopyToTail(ref Key key, ref Input input, ref Value desiredValue, long foundLogicalAddress)
        {
            if (SupportAsync) UnsafeResumeThread();
            try
            {
                fht.InternalCopyToTail(ref key, ref input, ref desiredValue, foundLogicalAddress, FasterSession, ctx, noReadCache: true);
            }
            finally
            {
                if (SupportAsync) UnsafeSuspendThread();
            }
        }

        /// <summary>
        /// Iterator for all (distinct) live key-values stored in FASTER
        /// </summary>
        /// <param name="untilAddress">Report records until this address (tail by default)</param>
        /// <returns>FASTER iterator</returns>
        public IFasterScanIterator<Key, Value> Iterate(long untilAddress = -1)
        {
            if (!SupportAsync)
                throw new FasterException("Do not perform iteration using a threadAffinitized session");

            if (untilAddress == -1)
                untilAddress = fht.Log.TailAddress;

            return new FasterKVIterator<Key, Value, Input, Output, Context, Functions>(fht, functions, untilAddress);
        }

        /// <summary>
        /// Resume session on current thread
        /// Call SuspendThread before any async op
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void UnsafeResumeThread()
        {
            fht.epoch.Resume();
            fht.InternalRefresh(ctx, FasterSession);
        }

        /// <summary>
        /// Suspend session on current thread
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void UnsafeSuspendThread()
        {
            fht.epoch.Suspend();
        }

        void IClientSession.AtomicSwitch(int version)
        {
            fht.AtomicSwitch(ctx, ctx.prevCtx, version, fht._hybridLogCheckpoint.info.checkpointTokens);
        }

        // This is a struct to allow JIT to inline calls (and bypass default interface call mechanism)
        internal readonly struct InternalFasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            private readonly ClientSession<Key, Value, Input, Output, Context, Functions> _clientSession;

            public InternalFasterSession(ClientSession<Key, Value, Input, Output, Context, Functions> clientSession)
            {
                _clientSession = clientSession;
            }

            public bool SupportsPostOperations => _clientSession.functions.SupportsPostOperations;

            public void CheckpointCompletionCallback(string guid, CommitPoint commitPoint)
            {
                _clientSession.functions.CheckpointCompletionCallback(guid, commitPoint);
                _clientSession.LatestCommitPoint = commitPoint;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool ConcurrentReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref RecordInfo recordInfo, long address) 
                => !this.SupportsLocking
                    ? _clientSession.functions.ConcurrentReader(ref key, ref input, ref value, ref dst, ref recordInfo, address)
                    : ConcurrentReaderLock(ref key, ref input, ref value, ref dst, ref recordInfo, address);

            public bool ConcurrentReaderLock(ref Key key, ref Input input, ref Value value, ref Output dst, ref RecordInfo recordInfo, long address)
            {
                bool success = false;
                for (bool retry = true; retry; /* updated in loop */)
                {
                    success = false;
                    long context = 0;
                    this.Lock(ref recordInfo, ref key, ref value, LockType.Shared, ref context);
                    try
                    {
                        success = _clientSession.functions.ConcurrentReader(ref key, ref input, ref value, ref dst, ref recordInfo, address);
                    }
                    finally
                    {
                        retry = !this.Unlock(ref recordInfo, ref key, ref value, LockType.Shared, context);
                    }
                }
                return success;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool ConcurrentWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref RecordInfo recordInfo, long address)
                => !this.SupportsLocking
                    ? ConcurrentWriterNoLock(ref key, ref input, ref src, ref dst, ref recordInfo, address)
                    : ConcurrentWriterLock(ref key, ref input, ref src, ref dst, ref recordInfo, address);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private bool ConcurrentWriterNoLock(ref Key key, ref Input input, ref Value src, ref Value dst, ref RecordInfo recordInfo, long address)
            {
                recordInfo.Version = _clientSession.ctx.version;
                // Note: KeyIndexes do not need notification of in-place updates because the key does not change.
                return _clientSession.functions.ConcurrentWriter(ref key, ref input, ref src, ref dst, ref recordInfo, address);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private bool ConcurrentWriterLock(ref Key key, ref Input input, ref Value src, ref Value dst, ref RecordInfo recordInfo, long address)
            {
                long context = 0;
                this.Lock(ref recordInfo, ref key, ref dst, LockType.Exclusive, ref context);
                try
                {
                    return !recordInfo.Tombstone && ConcurrentWriterNoLock(ref key, ref input, ref src, ref dst, ref recordInfo, address);
                }
                finally
                {
                    this.Unlock(ref recordInfo, ref key, ref dst, LockType.Exclusive, context);
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void PostSingleDeleter(ref Key key, ref RecordInfo recordInfo, long address)
                => _clientSession.functions.PostSingleDeleter(ref key, ref recordInfo, address);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool ConcurrentDeleter(ref Key key, ref Value value, ref RecordInfo recordInfo, long address)
                => (!this.SupportsLocking)
                    ? ConcurrentDeleterNoLock(ref key, ref value, ref recordInfo, address)
                    : ConcurrentDeleterLock(ref key, ref value, ref recordInfo, address);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private bool ConcurrentDeleterNoLock(ref Key key, ref Value value, ref RecordInfo recordInfo, long address)
            {
                recordInfo.Version = _clientSession.ctx.version;
                recordInfo.Tombstone = true;
                return _clientSession.functions.ConcurrentDeleter(ref key, ref value, ref recordInfo, address);
            }

            private bool ConcurrentDeleterLock(ref Key key, ref Value value, ref RecordInfo recordInfo, long address)
            {
                long context = 0;
                this.Lock(ref recordInfo, ref key, ref value, LockType.Exclusive, ref context);
                try
                {
                    return ConcurrentDeleterNoLock(ref key, ref value, ref recordInfo, address);
                }
                finally
                {
                    this.Unlock(ref recordInfo, ref key, ref value, LockType.Exclusive, context);
                }
            }

            public bool NeedInitialUpdate(ref Key key, ref Input input, ref Output output)
                => _clientSession.functions.NeedInitialUpdate(ref key, ref input, ref output);

            public bool NeedCopyUpdate(ref Key key, ref Input input, ref Value oldValue, ref Output output)
                => _clientSession.functions.NeedCopyUpdate(ref key, ref input, ref oldValue, ref output);

            public void CopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RecordInfo recordInfo, long address) 
                => _clientSession.functions.CopyUpdater(ref key, ref input, ref oldValue, ref newValue, ref output, ref recordInfo, address);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool PostCopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RecordInfo recordInfo, long address) => !this.SupportsLocking
                ? PostCopyUpdaterNoLock(ref key, ref input, ref output, ref oldValue, ref newValue, ref recordInfo, address)
                : PostCopyUpdaterLock(ref key, ref input, ref output, ref oldValue, ref newValue, ref recordInfo, address);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private bool PostCopyUpdaterNoLock(ref Key key, ref Input input, ref Output output, ref Value oldValue, ref Value newValue, ref RecordInfo recordInfo, long address)
            {
                recordInfo.Version = _clientSession.ctx.version;
                return _clientSession.functions.PostCopyUpdater(ref key, ref input, ref oldValue, ref newValue, ref output, ref recordInfo, address);
            }

            private bool PostCopyUpdaterLock(ref Key key, ref Input input, ref Output output, ref Value oldValue, ref Value newValue, ref RecordInfo recordInfo, long address)
            {
                long context = 0;
                this.Lock(ref recordInfo, ref key, ref newValue, LockType.Exclusive, ref context);
                try
                {
                    // KeyIndexes do not need notification of in-place updates because the key does not change.
                    return !recordInfo.Tombstone && PostCopyUpdaterNoLock(ref key, ref input, ref output, ref oldValue, ref newValue, ref recordInfo, address);
                }
                finally
                {
                    this.Unlock(ref recordInfo, ref key, ref newValue, LockType.Exclusive, context);
                }
            }

            public void DeleteCompletionCallback(ref Key key, Context ctx) 
                => _clientSession.functions.DeleteCompletionCallback(ref key, ctx);

            public int GetInitialLength(ref Input input) 
                => _clientSession.variableLengthStruct.GetInitialLength(ref input);

            public int GetLength(ref Value t, ref Input input) 
                => _clientSession.variableLengthStruct.GetLength(ref t, ref input);

            public void InitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, long address) 
                => _clientSession.functions.InitialUpdater(ref key, ref input, ref value, ref output, ref recordInfo, address);

            public void PostInitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, long address)
                => _clientSession.functions.PostInitialUpdater(ref key, ref input, ref value, ref output, ref recordInfo, address);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool InPlaceUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, long address)
                => !this.SupportsLocking
                    ? InPlaceUpdaterNoLock(ref key, ref input, ref output, ref value, ref recordInfo, address)
                    : InPlaceUpdaterLock(ref key, ref input, ref output, ref value, ref recordInfo, address);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private bool InPlaceUpdaterNoLock(ref Key key, ref Input input, ref Output output, ref Value value, ref RecordInfo recordInfo, long address)
            {
                recordInfo.Version = _clientSession.ctx.version;
                // Note: KeyIndexes do not need notification of in-place updates because the key does not change.
                return _clientSession.functions.InPlaceUpdater(ref key, ref input, ref value, ref output, ref recordInfo, address);
            }

            private bool InPlaceUpdaterLock(ref Key key, ref Input input, ref Output output, ref Value value, ref RecordInfo recordInfo, long address)
            {
                long context = 0;
                this.Lock(ref recordInfo, ref key, ref value, LockType.Exclusive, ref context);
                try
                {
                    return !recordInfo.Tombstone && InPlaceUpdaterNoLock(ref key, ref input, ref output, ref value, ref recordInfo, address);
                }
                finally
                {
                    this.Unlock(ref recordInfo, ref key, ref value, LockType.Exclusive, context);
                }
            }

            public void ReadCompletionCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status, RecordMetadata recordMetadata) 
                => _clientSession.functions.ReadCompletionCallback(ref key, ref input, ref output, ctx, status, recordMetadata);

            public void RMWCompletionCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status, RecordMetadata recordMetadata) 
                => _clientSession.functions.RMWCompletionCallback(ref key, ref input, ref output, ctx, status, recordMetadata);

            public bool SingleReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref RecordInfo recordInfo, long address) 
                => _clientSession.functions.SingleReader(ref key, ref input, ref value, ref dst, ref recordInfo, address);

            public void SingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref RecordInfo recordInfo, long address) 
                => _clientSession.functions.SingleWriter(ref key, ref input, ref src, ref dst, ref recordInfo, address);

            public void PostSingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref RecordInfo recordInfo, long address) 
                => _clientSession.functions.PostSingleWriter(ref key, ref input, ref src, ref dst, ref recordInfo, address);

            public void UpsertCompletionCallback(ref Key key, ref Input input, ref Value value, Context ctx) 
                => _clientSession.functions.UpsertCompletionCallback(ref key, ref input, ref value, ctx);

            public void UnsafeResumeThread() => _clientSession.UnsafeResumeThread();

            public void UnsafeSuspendThread() => _clientSession.UnsafeSuspendThread();

            public bool SupportsLocking => _clientSession.functions.SupportsLocking;

            public void Lock(ref RecordInfo recordInfo, ref Key key, ref Value value, LockType lockType, ref long lockContext) => _clientSession.functions.Lock(ref recordInfo, ref key, ref value, lockType, ref lockContext);

            public bool Unlock(ref RecordInfo recordInfo, ref Key key, ref Value value, LockType lockType, long lockContext) => _clientSession.functions.Unlock(ref recordInfo, ref key, ref value, lockType, lockContext);

            public IHeapContainer<Input> GetHeapContainer(ref Input input)
            {
                if (_clientSession.inputVariableLengthStruct == default)
                    return new StandardHeapContainer<Input>(ref input);
                return new VarLenHeapContainer<Input>(ref input, _clientSession.inputVariableLengthStruct, _clientSession.fht.hlog.bufferPool);
            }

            public bool CompletePendingWithOutputs(out CompletedOutputIterator<Key, Value, Input, Output, Context> completedOutputs, bool wait = false, bool spinWaitForCommit = false)
                => throw new NotImplementedException();
        }
    }
}
