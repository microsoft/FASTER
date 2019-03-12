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
    public struct KeyStruct : IFasterEqualityComparer<KeyStruct>
    {
        public long kfield1;
        public long kfield2;

        public long GetHashCode64(ref KeyStruct key)
        {
            return Utility.GetHashCode(key.kfield1);
        }
        public bool Equals(ref KeyStruct k1, ref KeyStruct k2)
        {
            return k1.kfield1 == k2.kfield1 && k1.kfield2 == k2.kfield2;
        }
    }

    public struct ValueStruct
    {
        public long vfield1;
        public long vfield2;
    }

    public struct InputStruct
    {
        public long ifield1;
        public long ifield2;
    }

    public struct OutputStruct
    {
        public ValueStruct value;
    }

    public class Functions : IFunctions<KeyStruct, ValueStruct, InputStruct, OutputStruct, Empty>
    {
        public void RMWCompletionCallback(ref KeyStruct key, ref InputStruct input, Empty ctx, Status status)
        {
            Assert.IsTrue(status == Status.OK);
        }

        public void ReadCompletionCallback(ref KeyStruct key, ref InputStruct input, ref OutputStruct output, Empty ctx, Status status)
        {
            Assert.IsTrue(status == Status.OK);
            Assert.IsTrue(output.value.vfield1 == key.kfield1);
            Assert.IsTrue(output.value.vfield2 == key.kfield2);
        }

        public void UpsertCompletionCallback(ref KeyStruct key, ref ValueStruct output, Empty ctx)
        {
        }

        public void CheckpointCompletionCallback(Guid sessionId, long serialNum)
        {
            Debug.WriteLine("Session {0} reports persistence until {1}", sessionId, serialNum);
        }

        // Read functions
        public void SingleReader(ref KeyStruct key, ref InputStruct input, ref ValueStruct value, ref OutputStruct dst)
        {
            dst.value = value;
        }

        public void ConcurrentReader(ref KeyStruct key, ref InputStruct input, ref ValueStruct value, ref OutputStruct dst)
        {
            dst.value = value;
        }

        // Upsert functions
        public void SingleWriter(ref KeyStruct key, ref ValueStruct src, ref ValueStruct dst)
        {
            dst = src;
        }

        public void ConcurrentWriter(ref KeyStruct key, ref ValueStruct src, ref ValueStruct dst)
        {
            dst = src;
        }

        // RMW functions
        public void InitialUpdater(ref KeyStruct key, ref InputStruct input, ref ValueStruct value)
        {
            value.vfield1 = input.ifield1;
            value.vfield2 = input.ifield2;
        }

        public void InPlaceUpdater(ref KeyStruct key, ref InputStruct input, ref ValueStruct value)
        {
            value.vfield1 += input.ifield1;
            value.vfield2 += input.ifield2;
        }

        public void CopyUpdater(ref KeyStruct key, ref InputStruct input, ref ValueStruct oldValue, ref ValueStruct newValue)
        {
            newValue.vfield1 = oldValue.vfield1 + input.ifield1;
            newValue.vfield2 = oldValue.vfield2 + input.ifield2;
        }
    }
}
