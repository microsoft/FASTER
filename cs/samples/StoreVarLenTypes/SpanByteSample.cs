// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Linq;
using FASTER.core;

namespace StoreVarLenTypes
{
    /// <summary>
    /// This sample shows how our special type called SpanByte can be leverage to use FASTER
    /// with variable-length keys and/or values without a separate object log. SpanBytes can
    /// easily be created using pinned or fixed memory. A SpanByte is basically a sequence of
    /// bytes with a 4-byte integer length header that denotes the size of the payload.
    ///
    /// Underlying SpanByte is the use of "ref struct" as a proxy for pointers to variable-sized 
    /// memory in C# (we call these VariableLengthStructs).
    /// </summary>
    public class SpanByteSample
    {
        // Functions for the push scan iterator.
        internal struct ScanFunctions : IScanIteratorFunctions<SpanByte, SpanByte>
        {
            internal long count;

            public bool OnStart(long beginAddress, long endAddress) => true;

            public bool ConcurrentReader(ref SpanByte key, ref SpanByte value, RecordMetadata recordMetadata, long numberOfRecords, long nextAddress)
                => SingleReader(ref key, ref value, recordMetadata, numberOfRecords, nextAddress);

            public bool SingleReader(ref SpanByte key, ref SpanByte value, RecordMetadata recordMetadata, long numberOfRecords, long nextAddress) 
                => key.ToByteArray()[0] == count++;

            public void OnException(Exception exception, long numberOfRecords) { }

            public void OnStop(bool completed, long numberOfRecords) { }
        }

        public static void Run()
        {
            // VarLen types do not need an object log
            var log = Devices.CreateLogDevice("hlog.log", deleteOnClose: true);

            // Create store
            // For custom varlen (not SpanByte), you need to provide IVariableLengthStructSettings and IFasterEqualityComparer
            var store = new FasterKV<SpanByte, SpanByte>(
                size: 1L << 20,
                logSettings: new LogSettings { LogDevice = log, MemorySizeBits = 15, PageSizeBits = 12 });

            // Create session
            var s = store.For(new CustomSpanByteFunctions()).NewSession<CustomSpanByteFunctions>();

            Random r = new(100);

            // Here, stackalloc implies fixed, so it can be used directly with SpanByte
            // For Span<byte> over heap data (e.g., strings or byte[]), make sure to use
            // fixed before the FASTER Read/Upsert/RMW operation.
            Span<byte> keyMem = stackalloc byte[1000];
            Span<byte> valueMem = stackalloc byte[1000];

            const int numRecords = 200;
            byte i;
            for (i = 0; i < numRecords; i++)
            {
                var keyLen = r.Next(1, 1000);
                var key = keyMem.Slice(0, keyLen);
                key.Fill(i);

                var valLen = r.Next(1, 1000);
                var value = valueMem.Slice(0, valLen);
                value.Fill((byte)valLen);

                // Option 1: Using overload for Span<byte>
                s.Upsert(key, value);
            }

            ScanFunctions scanFunctions = new();
            bool success = store.Log.Scan(ref scanFunctions, store.Log.BeginAddress, store.Log.TailAddress)
                           && scanFunctions.count == numRecords;

            if (!success)
            {
                Console.WriteLine("SpanByteSample: Error on Scan!");
            }
            else
            {
                r = new Random(100);
                for (i = 0; i < numRecords; i++)
                {
                    var keyLen = r.Next(1, 1000);
                    Span<byte> key = keyMem.Slice(0, keyLen);
                    key.Fill(i);

                    var valLen = r.Next(1, 1000);

                    // Option 2: Converting fixed Span<byte> to SpanByte
                    var status = s.Read(SpanByte.FromFixedSpan(key), out byte[] output, userContext: (byte)valLen);

                    var expectedValue = valueMem.Slice(0, valLen);
                    expectedValue.Fill((byte)valLen);

                    if (status.IsPending)
                    {
                        s.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                        using (completedOutputs)
                        {
                            success = completedOutputs.Next();
                            if (success)
                                (status, output) = (completedOutputs.Current.Status, completedOutputs.Current.Output);
                        }
                    }
                    success &= status.Found && output.SequenceEqual(expectedValue.ToArray());
                    if (!success)
                    {
                        Console.WriteLine("SpanByteSample: Error on Read!");
                        break;
                    }
                }
            }

            if (success)
                Console.WriteLine("SpanByteSample: Success!");

            s.Dispose();
            store.Dispose();
            log.Dispose();
        }
    }
}