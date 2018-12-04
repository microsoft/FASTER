// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using System.Threading;

namespace FASTER.core
{
    /// <summary>
    /// Async IO related functions of FASTER
    /// </summary>
    public unsafe partial class FasterKV<Key, Value, Input, Output, Context, Functions> : FasterBase, IFasterKV<Key, Value, Input, Output, Context>
        where Key : IKey<Key>
        where Value : IValue<Value>
        where Input : IMoveToContext<Input>
        where Output : IMoveToContext<Output>
        where Context : IMoveToContext<Context>
        where Functions : IFunctions<Key, Value, Input, Output, Context>

    {
        private void AsyncGetFromDisk(long fromLogical,
                                      int numRecords,
                                      IOCompletionCallback callback,
                                      AsyncIOContext<Key> context,
                                      SectorAlignedMemory result = default(SectorAlignedMemory))
        {
            while (numPendingReads > 120)
            {
                Thread.SpinWait(100);

                // Do not protect if we are not already protected
                // E.g., we are in an IO thread
                if (epoch.IsProtected())
                    epoch.ProtectAndDrain();
            }
            Interlocked.Increment(ref numPendingReads);
            hlog.AsyncReadRecordToMemory(fromLogical, numRecords, callback, context, result);
        }

        private bool RetrievedObjects(byte* record, AsyncIOContext<Key> ctx)
        {
            if (!(hlog.KeyHasObjects() || hlog.ValueHasObjects()))
                return true;

            if (ctx.objBuffer.buffer == null)
            {
                // Issue IO for objects
                long startAddress = -1;
                long numBytes = 0;
                if (hlog.KeyHasObjects())
                {
                    var x = hlog.GetKeyAddressInfo((long)record);
                    numBytes += x->Size;
                    startAddress = x->Address;
                }

                if (hlog.ValueHasObjects())
                {
                    var x = hlog.GetValueAddressInfo((long)record);
                    numBytes += x->Size;
                    if (startAddress == -1)
                        startAddress = x->Address;
                }

                // We are limited to a 2GB size per key-value
                if (numBytes > int.MaxValue)
                    throw new Exception("Size of key-value exceeds max of 2GB: " + numBytes);

                AsyncGetFromDisk(startAddress, (int)numBytes,
                    AsyncGetFromDiskCallback, ctx, ctx.record);
                return false;
            }

            // Parse the key and value objects
            MemoryStream ms = new MemoryStream(ctx.objBuffer.buffer);
            ms.Seek(ctx.objBuffer.offset + ctx.objBuffer.valid_offset, SeekOrigin.Begin);
            hlog.GetKey((long)record).Deserialize(ms);
            hlog.GetValue((long)record).Deserialize(ms);
            ctx.objBuffer.Return();
            return true;
        }


        private void AsyncGetFromDiskCallback(
                    uint errorCode,
                    uint numBytes,
                    NativeOverlapped* overlap)
        {
            if (errorCode != 0)
            {
                Trace.TraceError("OverlappedStream GetQueuedCompletionStatus error: {0}", errorCode);
            }

            var result = (AsyncGetFromDiskResult<AsyncIOContext<Key>>)Overlapped.Unpack(overlap).AsyncResult;
            Interlocked.Decrement(ref numPendingReads);

            var ctx = result.context;

            var record = ctx.record.GetValidPointer();
            int requiredBytes = hlog.GetRecordSize((long)record);
            if (ctx.record.available_bytes >= requiredBytes)
            {
                //We have the complete record.
                if (RetrievedObjects(record, ctx))
                {
                    if (ctx.key.Equals(ref hlog.GetKey((long)record)))
                    {
                        //The keys are same, so I/O is complete
                        // ctx.record = result.record;
                        ctx.callbackQueue.Add(ctx);
                    }
                    else
                    {
                        var oldAddress = ctx.logicalAddress;

                        //keys are not same. I/O is not complete
                        ctx.logicalAddress = ((RecordInfo*)record)->PreviousAddress;
                        if (ctx.logicalAddress != Constants.kInvalidAddress)
                        {

                            // Delete key, value, record
                            if (hlog.KeyHasObjects())
                            {
                                var physicalAddress = (long)ctx.record.GetValidPointer();
                                hlog.GetKey(physicalAddress).Free();
                            }
                            if (hlog.ValueHasObjects())
                            {
                                var physicalAddress = (long)ctx.record.GetValidPointer();
                                hlog.GetValue(physicalAddress).Free();
                            }
                            ctx.record.Return();
                            ctx.record = ctx.objBuffer = default(SectorAlignedMemory);
                            AsyncGetFromDisk(ctx.logicalAddress, requiredBytes, AsyncGetFromDiskCallback, ctx);
                        }
                        else
                        {
                            ctx.callbackQueue.Add(ctx);
                        }
                    }
                }
            }
            else
            {
                ctx.record.Return();
                AsyncGetFromDisk(ctx.logicalAddress, requiredBytes, AsyncGetFromDiskCallback, ctx);
            }

            Overlapped.Free(overlap);
        }
    }
}
