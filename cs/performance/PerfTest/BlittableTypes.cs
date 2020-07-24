// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using Performance.Common;
using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace FASTER.PerfTest
{
    public interface IBlittableType : IKey
    {
        void SetInitialValue(long value, long mod);

        BlittableData Data { get; }

        uint Modified { get; set; }

        bool Verify(long value);
    }

    [StructLayout(LayoutKind.Sequential, Size = 8)]
    public struct BlittableType8 : IBlittableType
    {
        internal BlittableData data;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetInitialValue(long value, long mod)
        {
            this.data.Value = (int)value;
            this.data.Modified = (uint)(mod & uint.MaxValue);
        }

        public BlittableData Data => this.data;

        public long Value => this.data.Value;

        public uint Modified
        {
            get => data.Modified;
            set => data.Modified = value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Verify(long value)
        {
            return this.data.Value == value
                ? true
                : throw new ApplicationException($"this.data.Value ({this.data.Value}) != value ({value})");
        }

        public override string ToString() => this.data.ToString();
    }

    [StructLayout(LayoutKind.Sequential, Size = 16)]
    public struct BlittableType16 : IBlittableType
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

        public BlittableData Data => this.data;

        public long Value => this.data.Value;

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
        public bool Verify(long value)
        {
            if (this.data.Value != value)
                throw new ApplicationException($"this.data.Value ({this.data.Value}) != value ({value})");
            return extra1 != data
                ? throw new ApplicationException("extraN != value")
                : true;
        }

        public override string ToString() => this.data.ToString();
    }

    [StructLayout(LayoutKind.Sequential, Size = 32)]
    public struct BlittableType32 : IBlittableType
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

        public BlittableData Data => this.data;

        public long Value => this.data.Value;

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
        public bool Verify(long value)
        {
            if (this.data.Value != value)
                throw new ApplicationException($"this.data.Value ({this.data.Value}) != value ({value})");
            return extra1 != data || extra2 != data || extra3 != data
                ? throw new ApplicationException("extraN != data")
                : true;
        }

        public override string ToString() => this.data.ToString();
    }

    [StructLayout(LayoutKind.Sequential, Size = 64)]
    public struct BlittableType64 : IBlittableType
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

        public BlittableData Data => this.data;

        public long Value => this.data.Value;

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
        public bool Verify(long value)
        {
            if (this.data.Value != value)
                throw new ApplicationException($"this.data.Value ({this.data.Value}) != value ({value})");
            return extra1 != data || extra2 != data || extra3 != data || extra4 != data || extra5 != data || extra6 != data || extra7 != data
                ? throw new ApplicationException("extraN != data")
                : true;
        }

        public override string ToString() => this.data.ToString();
    }

    [StructLayout(LayoutKind.Sequential, Size = 128)]
    public struct BlittableType128 : IBlittableType
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

        public BlittableData Data => this.data;

        public long Value => this.data.Value;

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
        public bool Verify(long value)
        {
            if (this.data.Value != value)
                throw new ApplicationException("this.data.Value != value");
            return extra1 != data || extra2 != data || extra3 != data || extra4 != data || extra5 != data || extra6 != data || extra7 != data
                || extra10 != data || extra11 != data || extra12 != data || extra13 != data || extra14 != data || extra15 != data || extra16 != data || extra17 != data
                ? throw new ApplicationException($"this.data.Value ({this.data.Value}) != value ({value})")
                : true;
        }

        public override string ToString() => this.data.ToString();
    }

    [StructLayout(LayoutKind.Sequential, Size = 256)]
    public struct BlittableType256 : IBlittableType
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

        public BlittableData Data => this.data;

        public long Value => this.data.Value;

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
        public bool Verify(long value)
        {
            if (this.data.Value != value)
                throw new ApplicationException($"this.data.Value ({this.data.Value}) != value ({value})");
            return extra1 != data || extra2 != data || extra3 != data || extra4 != data || extra5 != data || extra6 != data || extra7 != data
                || extra10 != data || extra11 != data || extra12 != data || extra13 != data || extra14 != data || extra15 != data || extra16 != data || extra17 != data
                || extra20 != data || extra21 != data || extra22 != data || extra23 != data || extra24 != data || extra25 != data || extra26 != data || extra27 != data
                || extra30 != data || extra31 != data || extra32 != data || extra33 != data || extra34 != data || extra35 != data || extra36 != data || extra37 != data
                ? throw new ApplicationException("extraN != data")
                : true;
        }

        public override string ToString() => this.data.ToString();
    }

    public struct BlittableEqualityComparer<TBV> : IFasterEqualityComparer<TBV>
        where TBV : IBlittableType
    {
        public long GetHashCode64(ref TBV k) => k.Data.GetHashCode64();

        public unsafe bool Equals(ref TBV k1, ref TBV k2)
            => k1.Data == k2.Data && (!Globals.Verify || (k1.Verify(k1.Data.Value) && k2.Verify(k2.Data.Value)));
    }

    public struct BlittableOutput<TBlittableType>
    {
        public TBlittableType Value { get; set; }

        public override string ToString() => this.Value.ToString();
    }

    class BlittableValueWrapperFactory<TBlittableType> : IValueWrapperFactory<TBlittableType, BlittableOutput<TBlittableType>, BlittableValueWrapper<TBlittableType>>
        where TBlittableType : IBlittableType, new()
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public BlittableValueWrapper<TBlittableType> GetValueWrapper(int threadIndex) => new BlittableValueWrapper<TBlittableType>();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public BlittableOutput<TBlittableType> GetOutput(int threadIndex) => new BlittableOutput<TBlittableType>();
    }

    class BlittableValueWrapper<TBlittableType> : IValueWrapper<TBlittableType>
        where TBlittableType : IBlittableType, new()
    {
        private TBlittableType value = new TBlittableType();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref TBlittableType GetRef() => ref this.value;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetInitialValue(long value) => this.value.SetInitialValue(value, 0);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetUpsertValue(long value, long mod) => this.value.SetInitialValue(value, mod);
    }

    internal class BlittableKeyManager<TBV> : KeyManagerBase<TBV>, IKeyManager<TBV>
        where TBV : struct, IBlittableType
    {
        TBV[] initKeys;
        TBV[] opKeys;

        internal override void Initialize(ZipfSettings zipfSettings, RandomGenerator rng, int initCount, int opCount)
        {
            this.initKeys = new TBV[initCount];
            for (var ii = 0; ii < initCount; ++ii)
            {
                initKeys[ii] = new TBV();
                initKeys[ii].SetInitialValue(ii, 0);
            }

            // Note on memory usage: BlittableType# is a struct and thus copied by value into opKeys, so will take much more storage
            // than VarLen or Object which are reference/object copies.
            if (!(zipfSettings is null))
            {
                this.opKeys = new Zipf<TBV>().GenerateOpKeys(zipfSettings, initKeys, opCount);
            }
            else
            {
                // Note: Since we pay the storage cost anyway, make it faster by creating a new key rather than reading initKeys.
                this.opKeys = new TBV[opCount];
                for (var ii = 0; ii < opCount; ++ii)
                {
                    opKeys[ii] = new TBV();
                    opKeys[ii].SetInitialValue((long)rng.Generate64((ulong)initCount), 0);
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref TBV GetInitKey(int index) => ref this.initKeys[index];

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref TBV GetOpKey(int index) => ref this.opKeys[index];

        public override void Dispose() { }
    }

    public class BlittableFunctions<TKey, TBlittableType> : IFunctions<TKey, TBlittableType, Input, BlittableOutput<TBlittableType>, Empty>
        where TKey : IKey
        where TBlittableType : IBlittableType
    {
        #region Upsert
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ConcurrentWriter(ref TKey key, ref TBlittableType src, ref TBlittableType dst)
        {
            SingleWriter(ref key, ref src, ref dst);
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SingleWriter(ref TKey key, ref TBlittableType src, ref TBlittableType dst)
        {
            if (Globals.Verify)
            {
                if (!Globals.IsInitialInsertPhase)
                    dst.Verify(key.Value);
                src.Verify(key.Value);
            }
            dst = src;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UpsertCompletionCallback(ref TKey key, ref TBlittableType value, Empty ctx)
        {
            if (Globals.Verify)
                value.Verify(key.Value);
        }
        #endregion Upsert

        #region Read
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ConcurrentReader(ref TKey key, ref Input input, ref TBlittableType value, ref BlittableOutput<TBlittableType> dst)
            => SingleReader(ref key, ref input, ref value, ref dst);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SingleReader(ref TKey key, ref Input _, ref TBlittableType value, ref BlittableOutput<TBlittableType> dst)
        {
            if (Globals.Verify)
                value.Verify(key.Value);
            dst.Value = value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ReadCompletionCallback(ref TKey key, ref Input _, ref BlittableOutput<TBlittableType> output, Empty ctx, Status status)
        {
            if (Globals.Verify)
                output.Value.Verify(key.Value);
        }
        #endregion Read

        #region RMW
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void InitialUpdater(ref TKey key, ref Input input, ref TBlittableType value) 
            => value.SetInitialValue(key.Value, (uint)input.value);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool InPlaceUpdater(ref TKey key, ref Input input, ref TBlittableType value)
        {
            if (Globals.Verify)
                value.Verify(key.Value);
            value.Modified += (uint)input.value;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CopyUpdater(ref TKey key, ref Input input, ref TBlittableType oldValue, ref TBlittableType newValue)
        {
            if (Globals.Verify)
                oldValue.Verify(key.Value);
            newValue.SetInitialValue(key.Value, oldValue.Modified + (uint)input.value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void RMWCompletionCallback(ref TKey key, ref Input input, Empty ctx, Status status)
        { }
        #endregion RMW

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint)
        { }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void DeleteCompletionCallback(ref TKey key, Empty ctx)
            => throw new InvalidOperationException("Delete not implemented");
    }
}
