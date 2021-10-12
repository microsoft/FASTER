// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StoreDiskReadBenchmark
{
    public struct Key : IFasterEqualityComparer<Key>
    {
        public long key;

        public Key(long key)
        {
            this.key = key;
        }
        public long GetHashCode64(ref Key key)
        {
            return Utility.GetHashCode(key.key);
        }
        public bool Equals(ref Key k1, ref Key k2)
        {
            return k1.key == k2.key;
        }
    }

    public struct Value
    {
        public Value(long f1)
        {
            vfield1 = f1;
        }   

        public long vfield1;
    }

    public struct Input
    {
        public long ifield1;
    }

    public class Output
    {
        public Value value;
    }

    /// <summary>
    /// Callback functions for FASTER operations
    /// </summary>
    public sealed class MyFuncs : FunctionsBase<Key, Value, Input, Output, Empty>
    {
        // Read functions
        public override bool SingleReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref RecordInfo recordInfo, long address)
        { if (dst == null) dst = new Output(); dst.value = value; return true; }

        public override bool ConcurrentReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref RecordInfo recordInfo, long address)
        { if (dst == null) dst = new Output(); dst.value = value; return true; }

        // RMW functions
        public override void InitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, long address)
        {
            value.vfield1 = input.ifield1;
        }
        public override void CopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RecordInfo recordInfo, long address)
        {
            newValue.vfield1 = oldValue.vfield1 + input.ifield1;
        }
        public override bool InPlaceUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, long address)
        {
            value.vfield1 += input.ifield1;
            return true;
        }

        // Completion callbacks
        public override void ReadCompletionCallback(ref Key key, ref Input input, ref Output output, Empty ctx, Status status, RecordMetadata recordMetadata)
        {
            if (status != Status.OK || output.value.vfield1 != key.key)
            {
                if (!Program.simultaneousReadWrite)
                    throw new Exception("Wrong value found");
            }
        }
    }
}
