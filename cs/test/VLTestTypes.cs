// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using FASTER.core;
using System.Runtime.CompilerServices;
using System.IO;
using System.Diagnostics;
using NUnit.Framework;

namespace FASTER.test
{
    public struct VLKey : IFasterEqualityComparer<VLKey>, IStructLength<VLKey>
    {
        public long key;

        public long GetHashCode64(ref VLKey key)
        {
            return Utility.GetHashCode(key.key);
        }
        public bool Equals(ref VLKey k1, ref VLKey k2)
        {
            return k1.key == k2.key;
        }

        public int GetLength(ref VLKey t)
        {
            return sizeof(long);
        }

        public int GetAverageLength()
        {
            return sizeof(long);
        }

        public int GetInitialLength<Input>(ref Input input)
        {
            return sizeof(long);
        }
    }

    public unsafe struct VLValue : IStructLength<VLValue>
    {
        public int GetAverageLength()
        {
            return 8;
        }

        public int GetInitialLength<Input>(ref Input input)
        {
            return 8;
        }

        public int GetLength(ref VLValue t)
        {
            return *(int*)Unsafe.AsPointer(ref t);
        }
    }

    public struct VLInput
    {
        public long input;
    }

    public struct VLOutput
    {
        public long output;
    }

    public class VLFunctions : IFunctions<VLKey, VLValue, VLInput, VLOutput, Empty>
    {
        public void RMWCompletionCallback(ref VLKey key, ref VLInput input, Empty ctx, Status status)
        {
            Assert.IsTrue(status == Status.OK);
        }

        public void ReadCompletionCallback(ref VLKey key, ref VLInput input, ref VLOutput output, Empty ctx, Status status)
        {
            Assert.IsTrue(status == Status.OK);
        }

        public void UpsertCompletionCallback(ref VLKey key, ref VLValue output, Empty ctx)
        {
        }

        public void DeleteCompletionCallback(ref VLKey key, Empty ctx)
        {
        }

        public void CheckpointCompletionCallback(Guid sessionId, long serialNum)
        {
            Debug.WriteLine("Session {0} reports persistence until {1}", sessionId, serialNum);
        }

        // Read functions
        public void SingleReader(ref VLKey key, ref VLInput input, ref VLValue value, ref VLOutput dst)
        {
        }

        public void ConcurrentReader(ref VLKey key, ref VLInput input, ref VLValue value, ref VLOutput dst)
        {
        }

        // Upsert functions
        public void SingleWriter(ref VLKey key, ref VLValue src, ref VLValue dst)
        {
            dst = src;
        }

        public void ConcurrentWriter(ref VLKey key, ref VLValue src, ref VLValue dst)
        {
            dst = src;
        }

        // RMW functions
        public void InitialUpdater(ref VLKey key, ref VLInput input, ref VLValue value)
        {
        }

        public void InPlaceUpdater(ref VLKey key, ref VLInput input, ref VLValue value)
        {
        }

        public void CopyUpdater(ref VLKey key, ref VLInput input, ref VLValue oldValue, ref VLValue newValue)
        {
        }
    }
}
