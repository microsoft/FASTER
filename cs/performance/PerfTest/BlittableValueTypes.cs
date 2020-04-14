// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace FASTER.PerfTest
{
    public interface IBlittableValue   // Needed for RMW
    {
        void SetInitialValue(long value, long mod);

        uint Modified { get; set; }

        void Verify(long value);
    }

    [StructLayout(LayoutKind.Sequential, Size = 8)]
    public struct BlittableValue8 : IBlittableValue
    {
        internal BlittableData data;

        public void SetInitialValue(long value, long mod)
        {
            this.data.Value = (int)value;
            this.data.Modified = (uint)(mod & uint.MaxValue);
        }

        public uint Modified
        {
            get => data.Modified;
            set => data.Modified = value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Verify(long value)
        {
            if (this.data.Value != value)
                throw new ApplicationException($"this.data.Value ({this.data.Value}) != value ({value})");
        }

        public override string ToString() => this.data.ToString();
    }

    [StructLayout(LayoutKind.Sequential, Size = 16)]
    public struct BlittableValue16 : IBlittableValue
    {
        internal BlittableData data;
        internal BlittableData extra1;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetInitialValue(long value, long mod)
        {
            this.data.Value = (int)value;
            this.data.Modified = (uint)(mod & uint.MaxValue);
            if (Globals.Verify)
                extra1 = data;
        }

        public uint Modified
        {
            get => data.Modified;
            set 
            {
                data.Modified = value;
                if (Globals.Verify)
                    extra1.Modified = data.Modified;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Verify(long value)
        {
            if (this.data.Value != value)
                throw new ApplicationException($"this.data.Value ({this.data.Value}) != value ({value})");
            if (extra1 != data)
                throw new ApplicationException("extraN != value");
        }

        public override string ToString() => this.data.ToString();
    }

    [StructLayout(LayoutKind.Sequential, Size = 32)]
    public struct BlittableValue32 : IBlittableValue
    {
        internal BlittableData data;
        internal BlittableData extra1, extra2, extra3;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetInitialValue(long value, long mod)
        {
            this.data.Value = (int)value;
            this.data.Modified = (uint)(mod & uint.MaxValue);
            if (Globals.Verify)
                extra1 = extra2 = extra3 = data;
        }

        public uint Modified
        {
            get => data.Modified;
            set
            {
                data.Modified = value;
                if (Globals.Verify)
                    extra1.Modified = extra2.Modified = extra3.Modified = data.Modified;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Verify(long value)
        {
            if (this.data.Value != value)
                throw new ApplicationException($"this.data.Value ({this.data.Value}) != value ({value})");
            if (extra1 != data || extra2 != data || extra3 != data)
                throw new ApplicationException("extraN != data");
        }

        public override string ToString() => this.data.ToString();
    }

    [StructLayout(LayoutKind.Sequential, Size = 64)]
    public struct BlittableValue64 : IBlittableValue
    {
        internal BlittableData data;
        internal BlittableData extra1, extra2, extra3, extra4, extra5, extra6, extra7;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetInitialValue(long value, long mod)
        {
            this.data.Value = (int)value;
            this.data.Modified = (uint)(mod & uint.MaxValue);
            if (Globals.Verify)
                extra1 = extra2 = extra3 = extra4 = extra5 = extra6 = extra7 = data;
        }

        public uint Modified
        {
            get => data.Modified;
            set
            {
                data.Modified = value;
                if (Globals.Verify)
                    extra1.Modified = extra2.Modified = extra3.Modified = extra4.Modified = extra5.Modified = extra6.Modified = extra7.Modified = data.Modified;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Verify(long value)
        {
            if (this.data.Value != value)
                throw new ApplicationException($"this.data.Value ({this.data.Value}) != value ({value})");
            if (extra1 != data || extra2 != data || extra3 != data || extra4 != data || extra5 != data || extra6 != data || extra7 != data)
                throw new ApplicationException("extraN != data");
        }

        public override string ToString() => this.data.ToString();
    }

    [StructLayout(LayoutKind.Sequential, Size = 128)]
    public struct BlittableValue128 : IBlittableValue
    {
        internal BlittableData data;
        internal BlittableData extra1, extra2, extra3, extra4, extra5, extra6, extra7;
        internal BlittableData extra10, extra11, extra12, extra13, extra14, extra15, extra16, extra17;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetInitialValue(long value, long mod)
        {
            this.data.Value = (int)value;
            this.data.Modified = (uint)(mod & uint.MaxValue);
            if (Globals.Verify)
                extra1 = extra2 = extra3 = extra4 = extra5 = extra6 = extra7
                = extra10 = extra11 = extra12 = extra13 = extra14 = extra15 = extra16 = extra17 = data;
        }

        public uint Modified
        {
            get => data.Modified;
            set
            {
                data.Modified = value;
                if (Globals.Verify)
                    extra1.Modified = extra2.Modified = extra3.Modified = extra4.Modified = extra5.Modified = extra6.Modified = extra7.Modified
                    = extra10.Modified = extra11.Modified = extra12.Modified = extra13.Modified = extra14.Modified = extra15.Modified = extra16.Modified = extra17.Modified = data.Modified;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Verify(long value)
        {
            if (this.data.Value != value)
                throw new ApplicationException("this.data.Value != value");
            if (extra1 != data || extra2 != data || extra3 != data || extra4 != data || extra5 != data || extra6 != data || extra7 != data
                || extra10 != data || extra11 != data || extra12 != data || extra13 != data || extra14 != data || extra15 != data || extra16 != data || extra17 != data)
                throw new ApplicationException($"this.data.Value ({this.data.Value}) != value ({value})");
        }

        public override string ToString() => this.data.ToString();
    }

    [StructLayout(LayoutKind.Sequential, Size = 256)]
    public struct BlittableValue256 : IBlittableValue
    {
        internal BlittableData data;
        internal BlittableData extra1, extra2, extra3, extra4, extra5, extra6, extra7;
        internal BlittableData extra10, extra11, extra12, extra13, extra14, extra15, extra16, extra17;
        internal BlittableData extra20, extra21, extra22, extra23, extra24, extra25, extra26, extra27;
        internal BlittableData extra30, extra31, extra32, extra33, extra34, extra35, extra36, extra37;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetInitialValue(long value, long mod)
        {
            this.data.Value = (int)value;
            this.data.Modified = (uint)(mod & uint.MaxValue);
            if (Globals.Verify)
                extra1 = extra2 = extra3 = extra4 = extra5 = extra6 = extra7
                = extra10 = extra11 = extra12 = extra13 = extra14 = extra15 = extra16 = extra17
                = extra20 = extra21 = extra22 = extra23 = extra24 = extra25 = extra26 = extra27
                = extra30 = extra31 = extra32 = extra33 = extra34 = extra35 = extra36 = extra37 = data;
        }

        public uint Modified
        {
            get => data.Modified;
            set
            {
                data.Modified = value;
                if (Globals.Verify)
                    extra1.Modified = extra2.Modified = extra3.Modified = extra4.Modified = extra5.Modified = extra6.Modified = extra7.Modified
                    = extra10.Modified = extra11.Modified = extra12.Modified = extra13.Modified = extra14.Modified = extra15.Modified = extra16.Modified = extra17.Modified
                    = extra20.Modified = extra21.Modified = extra22.Modified = extra23.Modified = extra24.Modified = extra25.Modified = extra26.Modified = extra27.Modified
                    = extra30.Modified = extra31.Modified = extra32.Modified = extra33.Modified = extra34.Modified = extra35.Modified = extra36.Modified = extra37.Modified = data.Modified;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Verify(long value)
        {
            if (this.data.Value != value)
                throw new ApplicationException($"this.data.Value ({this.data.Value}) != value ({value})");
            if (extra1 != data || extra2 != data || extra3 != data || extra4 != data || extra5 != data || extra6 != data || extra7 != data
                || extra10 != data || extra11 != data || extra12 != data || extra13 != data || extra14 != data || extra15 != data || extra16 != data || extra17 != data
                || extra20 != data || extra21 != data || extra22 != data || extra23 != data || extra24 != data || extra25 != data || extra26 != data || extra27 != data
                || extra30 != data || extra31 != data || extra32 != data || extra33 != data || extra34 != data || extra35 != data || extra36 != data || extra37 != data)
                throw new ApplicationException("extraN != data");
        }

        public override string ToString() => this.data.ToString();
    }

    public struct BlittableOutput<TBlittableValue>
    {
        public TBlittableValue Value { get; set; }

        public override string ToString() => this.Value.ToString();
    }

    class BlittableThreadValueRef<TBlittableValue> : IThreadValueRef<TBlittableValue, BlittableOutput<TBlittableValue>>
        where TBlittableValue : IBlittableValue, new()
    {
        readonly TBlittableValue[] values;

        internal BlittableThreadValueRef(int count)
            => values = Enumerable.Range(0, count).Select(ii => new TBlittableValue()).ToArray();

        public ref TBlittableValue GetRef(int threadIndex) => ref values[threadIndex];

        public BlittableOutput<TBlittableValue> GetOutput(int threadIndex) => new BlittableOutput<TBlittableValue>();

        public void SetInitialValue(ref TBlittableValue valueRef, long value) => valueRef.SetInitialValue(value, 0);

        public void SetUpsertValue(ref TBlittableValue valueRef, long value, long mod) => valueRef.SetInitialValue(value, mod);
    }

    public class BlittableFunctions<TBlittableValue> : IFunctions<Key, TBlittableValue, Input, BlittableOutput<TBlittableValue>, Empty>
        where TBlittableValue : IBlittableValue
    {
        #region Upsert
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ConcurrentWriter(ref Key key, ref TBlittableValue src, ref TBlittableValue dst)
        {
            SingleWriter(ref key, ref src, ref dst);
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SingleWriter(ref Key key, ref TBlittableValue src, ref TBlittableValue dst)
        {
            if (Globals.Verify)
            {
                if (!Globals.IsInitialInsertPhase)
                    dst.Verify(key.key);
                src.Verify(key.key);
            }
            dst = src;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UpsertCompletionCallback(ref Key key, ref TBlittableValue value, Empty ctx)
        {
            if (Globals.Verify)
                value.Verify(key.key);
        }
        #endregion Upsert

        #region Read
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ConcurrentReader(ref Key key, ref Input input, ref TBlittableValue value, ref BlittableOutput<TBlittableValue> dst)
            => SingleReader(ref key, ref input, ref value, ref dst);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SingleReader(ref Key key, ref Input _, ref TBlittableValue value, ref BlittableOutput<TBlittableValue> dst)
        {
            if (Globals.Verify)
                value.Verify(key.key);
            dst.Value = value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ReadCompletionCallback(ref Key key, ref Input _, ref BlittableOutput<TBlittableValue> output, Empty ctx, Status status)
        {
            if (Globals.Verify)
                output.Value.Verify(key.key);
        }
        #endregion Read

        #region RMW
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void InitialUpdater(ref Key key, ref Input input, ref TBlittableValue value) 
            => value.SetInitialValue(key.key, (uint)input.value);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool InPlaceUpdater(ref Key key, ref Input input, ref TBlittableValue value)
        {
            if (Globals.Verify)
                value.Verify(key.key);
            value.Modified += (uint)input.value;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CopyUpdater(ref Key key, ref Input input, ref TBlittableValue oldValue, ref TBlittableValue newValue)
        {
            if (Globals.Verify)
                oldValue.Verify(key.key);
            newValue.SetInitialValue(key.key, oldValue.Modified + (uint)input.value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void RMWCompletionCallback(ref Key key, ref Input input, Empty ctx, Status status)
        { }
        #endregion RMW

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint)
        { }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void DeleteCompletionCallback(ref Key key, Empty ctx)
            => throw new InvalidOperationException("Delete not implemented");
    }
}
