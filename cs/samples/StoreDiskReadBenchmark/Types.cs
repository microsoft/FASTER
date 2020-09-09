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
        public Value(long f1, long f2)
        {
            vfield1 = f1;
            //vfield2 = f2;
        }   

        public long vfield1;
        //public long vfield2;
    }

    public struct Input
    {
        public long ifield1;
        public long ifield2;
    }

    public class Output
    {
        public Value value;
    }

    /// <summary>
    /// Callback functions for FASTER operations
    /// </summary>
    public class MyFuncs : IFunctions<Key, Value, Input, Output, Empty>
    {
        // Read functions
        public void SingleReader(ref Key key, ref Input input, ref Value value, ref Output dst)
        { if (dst == null) dst = new Output(); dst.value = value; }

        public void ConcurrentReader(ref Key key, ref Input input, ref Value value, ref Output dst)
        { if (dst == null) dst = new Output(); dst.value = value; }

        // Write functions
        public void SingleWriter(ref Key key, ref Value src, ref Value dst) => dst = src;
        public bool ConcurrentWriter(ref Key key, ref Value src, ref Value dst) { dst = src; return true; }

        // RMW functions
        public void InitialUpdater(ref Key key, ref Input input, ref Value value)
        {
            value.vfield1 = input.ifield1;
            //value.vfield2 = input.ifield2;
        }
        public void CopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue)
        {
            newValue.vfield1 = oldValue.vfield1 + input.ifield1;
            //newValue.vfield2 = oldValue.vfield2 + input.ifield2;
        }
        public bool InPlaceUpdater(ref Key key, ref Input input, ref Value value)
        {
            value.vfield1 += input.ifield1;
            //value.vfield2 += input.ifield2;
            return true;
        }

        // Completion callbacks
        public void ReadCompletionCallback(ref Key key, ref Input input, ref Output output, Empty ctx, Status status)
        {
            if (status != Status.OK || output.value.vfield1 != key.key) // || output.value.vfield2 != key.key)
            {
                throw new Exception("Wrong value found");
            }
        }

        public void UpsertCompletionCallback(ref Key key, ref Value output, Empty ctx) { }
        public void DeleteCompletionCallback(ref Key key, Empty ctx) { }
        public void RMWCompletionCallback(ref Key key, ref Input output, Empty ctx, Status status) { }
        public void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint) { }
    }

}
