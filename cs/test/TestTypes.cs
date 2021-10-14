// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Text;
using System.Threading;
using FASTER.core;
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

        public override string ToString()
        {
            return kfield1.ToString();
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

    public struct ContextStruct
    {
        public long cfield1;
        public long cfield2;
    }

    public class Functions : FunctionsWithContext<Empty>
    {
    }

    public class FunctionsWithContext<TContext> : FunctionsBase<KeyStruct, ValueStruct, InputStruct, OutputStruct, TContext>
    {
        public override void RMWCompletionCallback(ref KeyStruct key, ref InputStruct input, ref OutputStruct output, TContext ctx, Status status, RecordMetadata recordMetadata)
        {
            Assert.AreEqual(Status.OK, status);
            Assert.AreEqual(key.kfield1 + input.ifield1, output.value.vfield1);
            Assert.AreEqual(key.kfield2 + input.ifield2, output.value.vfield2);
        }

        public override void ReadCompletionCallback(ref KeyStruct key, ref InputStruct input, ref OutputStruct output, TContext ctx, Status status, RecordMetadata recordMetadata)
        {
            Assert.AreEqual(Status.OK, status);
            Assert.AreEqual(key.kfield1, output.value.vfield1);
            Assert.AreEqual(key.kfield2, output.value.vfield2);
        }

        // Read functions
        public override bool SingleReader(ref KeyStruct key, ref InputStruct input, ref ValueStruct value, ref OutputStruct dst, ref RecordInfo recordInfo, long address)
        {
            dst.value = value;
            return true;
        }

        public override bool ConcurrentReader(ref KeyStruct key, ref InputStruct input, ref ValueStruct value, ref OutputStruct dst, ref RecordInfo recordInfo, long address)
        {
            dst.value = value;
            return true;
        }

        // RMW functions
        public override void InitialUpdater(ref KeyStruct key, ref InputStruct input, ref ValueStruct value, ref OutputStruct output, ref RecordInfo recordInfo, long address)
        {
            value.vfield1 = input.ifield1;
            value.vfield2 = input.ifield2;
            output.value = value;
        }

        public override bool InPlaceUpdater(ref KeyStruct key, ref InputStruct input, ref ValueStruct value, ref OutputStruct output, ref RecordInfo recordInfo, long address)
        {
            value.vfield1 += input.ifield1;
            value.vfield2 += input.ifield2;
            output.value = value;
            return true;
        }

        public override bool NeedCopyUpdate(ref KeyStruct key, ref InputStruct input, ref ValueStruct oldValue, ref OutputStruct output) => true;

        public override void CopyUpdater(ref KeyStruct key, ref InputStruct input, ref ValueStruct oldValue, ref ValueStruct newValue, ref OutputStruct output, ref RecordInfo recordInfo, long address)
        {
            newValue.vfield1 = oldValue.vfield1 + input.ifield1;
            newValue.vfield2 = oldValue.vfield2 + input.ifield2;
            output.value = newValue;
        }
    }

    public class FunctionsCompaction : FunctionsBase<KeyStruct, ValueStruct, InputStruct, OutputStruct, int>
    {
        public override void RMWCompletionCallback(ref KeyStruct key, ref InputStruct input, ref OutputStruct output, int ctx, Status status, RecordMetadata recordMetadata)
        {
            Assert.AreEqual(Status.OK, status);
        }

        public override void ReadCompletionCallback(ref KeyStruct key, ref InputStruct input, ref OutputStruct output, int ctx, Status status, RecordMetadata recordMetadata)
        {
            if (ctx == 0)
            {
                Assert.AreEqual(Status.OK, status);
                Assert.AreEqual(key.kfield1, output.value.vfield1);
                Assert.AreEqual(key.kfield2, output.value.vfield2);
            }
            else
            {
                Assert.AreEqual(Status.NOTFOUND, status);
            }
        }

        // Read functions
        public override bool SingleReader(ref KeyStruct key, ref InputStruct input, ref ValueStruct value, ref OutputStruct dst, ref RecordInfo recordInfo, long address)
        {
            dst.value = value;
            return true;
        }

        public override bool ConcurrentReader(ref KeyStruct key, ref InputStruct input, ref ValueStruct value, ref OutputStruct dst, ref RecordInfo recordInfo, long address)
        {
            dst.value = value;
            return true;
        }

        // RMW functions
        public override void InitialUpdater(ref KeyStruct key, ref InputStruct input, ref ValueStruct value, ref OutputStruct output, ref RecordInfo recordInfo, long address)
        {
            value.vfield1 = input.ifield1;
            value.vfield2 = input.ifield2;
        }

        public override bool InPlaceUpdater(ref KeyStruct key, ref InputStruct input, ref ValueStruct value, ref OutputStruct output, ref RecordInfo recordInfo, long address)
        {
            value.vfield1 += input.ifield1;
            value.vfield2 += input.ifield2;
            return true;
        }

        public override bool NeedCopyUpdate(ref KeyStruct key, ref InputStruct input, ref ValueStruct oldValue, ref OutputStruct output) => true;

        public override void CopyUpdater(ref KeyStruct key, ref InputStruct input, ref ValueStruct oldValue, ref ValueStruct newValue, ref OutputStruct output, ref RecordInfo recordInfo, long address)
        {
            newValue.vfield1 = oldValue.vfield1 + input.ifield1;
            newValue.vfield2 = oldValue.vfield2 + input.ifield2;
        }
    }

    public class FunctionsCopyOnWrite : FunctionsBase<KeyStruct, ValueStruct, InputStruct, OutputStruct, Empty>
    {
        private int _concurrentWriterCallCount;
        private int _inPlaceUpdaterCallCount;

        public int ConcurrentWriterCallCount => _concurrentWriterCallCount;
        public int InPlaceUpdaterCallCount => _inPlaceUpdaterCallCount;

        public override void RMWCompletionCallback(ref KeyStruct key, ref InputStruct input, ref OutputStruct output, Empty ctx, Status status, RecordMetadata recordMetadata)
        {
            Assert.AreEqual(Status.OK, status);
        }

        public override void ReadCompletionCallback(ref KeyStruct key, ref InputStruct input, ref OutputStruct output, Empty ctx, Status status, RecordMetadata recordMetadata)
        {
            Assert.AreEqual(Status.OK, status);
            Assert.AreEqual(key.kfield1, output.value.vfield1);
            Assert.AreEqual(key.kfield2, output.value.vfield2);
        }

        // Read functions
        public override bool SingleReader(ref KeyStruct key, ref InputStruct input, ref ValueStruct value, ref OutputStruct dst, ref RecordInfo recordInfo, long address)
        {
            dst.value = value;
            return true;
        }

        public override bool ConcurrentReader(ref KeyStruct key, ref InputStruct input, ref ValueStruct value, ref OutputStruct dst, ref RecordInfo recordInfo, long address)
        {
            dst.value = value;
            return true;
        }

        // Upsert functions
        public override void SingleWriter(ref KeyStruct key, ref InputStruct input, ref ValueStruct src, ref ValueStruct dst, ref RecordInfo recordInfo, long address) => dst = src;

        public override bool ConcurrentWriter(ref KeyStruct key, ref InputStruct input, ref ValueStruct src, ref ValueStruct dst, ref RecordInfo recordInfo, long address)
        {
            Interlocked.Increment(ref _concurrentWriterCallCount);
            return false;
        }

        // RMW functions
        public override void InitialUpdater(ref KeyStruct key, ref InputStruct input, ref ValueStruct value, ref OutputStruct output, ref RecordInfo recordInfo, long address)
        {
            value.vfield1 = input.ifield1;
            value.vfield2 = input.ifield2;
        }

        public override bool InPlaceUpdater(ref KeyStruct key, ref InputStruct input, ref ValueStruct value, ref OutputStruct output, ref RecordInfo recordInfo, long address)
        {
            Interlocked.Increment(ref _inPlaceUpdaterCallCount);
            return false;
        }

        public override bool NeedCopyUpdate(ref KeyStruct key, ref InputStruct input, ref ValueStruct oldValue, ref OutputStruct output) => true;

        public override void CopyUpdater(ref KeyStruct key, ref InputStruct input, ref ValueStruct oldValue, ref ValueStruct newValue, ref OutputStruct output, ref RecordInfo recordInfo, long address)
        {
            newValue.vfield1 = oldValue.vfield1 + input.ifield1;
            newValue.vfield2 = oldValue.vfield2 + input.ifield2;
        }
    }

    class RMWSimpleFunctions<Key, Value> : SimpleFunctions<Key, Value>
    {
        public RMWSimpleFunctions(Func<Value, Value, Value> merger) : base(merger) { }

        public override void InitialUpdater(ref Key key, ref Value input, ref Value value, ref Value output, ref RecordInfo recordInfo, long address)
        {
            base.InitialUpdater(ref key, ref input, ref value, ref output, ref recordInfo, address);
            output = input;
        }

        /// <inheritdoc/>
        public override void CopyUpdater(ref Key key, ref Value input, ref Value oldValue, ref Value newValue, ref Value output, ref RecordInfo recordInfo, long address)
        {
            base.CopyUpdater(ref key, ref input, ref oldValue, ref newValue, ref output, ref recordInfo, address);
            output = newValue;
        }

        /// <inheritdoc/>
        public override bool InPlaceUpdater(ref Key key, ref Value input, ref Value value, ref Value output, ref RecordInfo recordInfo, long address)
        {
            base.InPlaceUpdater(ref key, ref input, ref value, ref output, ref recordInfo, address);
            output = value; 
            return true;
        }
    }
}
