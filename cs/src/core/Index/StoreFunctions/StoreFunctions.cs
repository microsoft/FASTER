// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;

namespace FASTER.core
{
    /// <summary>
    /// Default compositor of the StoreFunctions components.
    /// </summary>
    public class StoreFunctions<Key, Value, TKeyComparer, TRecordDisposer, TKeyVarLen, TValueVarLen> : IStoreFunctions<Key, Value>
        where TKeyComparer : IFasterEqualityComparer<Key>
        where TRecordDisposer : IRecordDisposer<Key, Value>
        where TKeyVarLen : IVariableLengthStruct<Key>
        where TValueVarLen : IVariableLengthStruct<Value>
    {
        private TKeyComparer keyComparer;
        private TRecordDisposer recordDisposer;
        private TKeyVarLen keyLength;
        private TValueVarLen valueLength;

        /// <summary>
        /// Constructor
        /// </summary>
        public StoreFunctions(TKeyComparer keyComparer, TRecordDisposer recordDisposer, TKeyVarLen keyLen, TValueVarLen valueLen)
        {
            this.keyComparer = keyComparer;
            this.recordDisposer = recordDisposer;
            this.keyLength = keyLen;
            this.valueLength = valueLen;
        }

        #region Record Disposal
        /// <inheritdoc/>
        public bool DisposeOnPageEviction => recordDisposer.DisposeOnPageEviction;

        /// <inheritdoc/>
        public void DisposeRecord(ref Key key, ref Value value, DisposeReason reason) => recordDisposer.DisposeRecord(ref key, ref value, reason);
        #endregion Record Disposal

        #region Key Comparison
        /// <inheritdoc/>
        public long GetKeyHashCode64(ref Key key) => keyComparer.GetHashCode64(ref key);

        /// <inheritdoc/>
        public bool KeyEquals(ref Key key1, ref Key key2) => keyComparer.Equals(ref key1, ref key2);
        #endregion Key Comparison

        #region Input-independent Variable length Keys
        /// <inheritdoc/>
        public bool IsVariableLengthKey => keyLength.IsVariableLength;

        /// <inheritdoc/>
        public int GetKeyLength(ref Key key) => keyLength.GetLength(ref key);

        /// <inheritdoc/>
        public int GetInitialKeyLength() => keyLength.GetInitialLength();

        /// <inheritdoc/>
        public unsafe void SerializeKey(ref Key source, void* destination)
        {
            var length = GetKeyLength(ref source);
            Buffer.MemoryCopy(Unsafe.AsPointer(ref source), destination, length, length);
        }

        /// <inheritdoc/>
        public unsafe ref Key KeyAsRef(void* source) => ref Unsafe.AsRef<Key>(source);

        /// <inheritdoc/>
        public unsafe void InitializeKey(void* source, void* end) { }
        #endregion Input-independent Variable length Keys

        #region Input-independent Variable length Values
        /// <inheritdoc/>
        public bool IsVariableLengthValue => keyLength.IsVariableLength;

        /// <inheritdoc/>
        public int GetValueLength(ref Value value) => valueLength.GetLength(ref value);

        /// <inheritdoc/>
        public int GetInitialValueLength() => valueLength.GetInitialLength();

        /// <inheritdoc/>
        public unsafe void SerializeValue(ref Value source, void* destination)
        {
            var length = GetValueLength(ref source);
            Buffer.MemoryCopy(Unsafe.AsPointer(ref source), destination, length, length);
        }

        /// <inheritdoc/>
        public unsafe ref Value ValueAsRef(void* source) => ref Unsafe.AsRef<Value>(source);

        /// <inheritdoc/>
        public unsafe void InitializeValue(void* source, void* end) { }
        #endregion Input-independent Variable length Values
    }

    internal class VariableLengthKeyRedirector<Key, Value, StoreFunctions> : IVariableLengthStruct<Key>
        where StoreFunctions : IStoreFunctions<Key, Value>
    {
        StoreFunctions storeFunctions;

        internal VariableLengthKeyRedirector(StoreFunctions storeFunctions) => this.storeFunctions = storeFunctions;

        /// <summary> Indicates whether <typeparamref name="Key"/> is a variable-length struct</summary>
        public bool IsVariableLength => storeFunctions.IsVariableLengthKey;

        /// <summary>Default implementation should not be called</summary>
        public int GetLength(ref Key key) => storeFunctions.GetKeyLength(ref key);

        /// <summary>Default implementation should not be called</summary>
        public int GetInitialLength() => storeFunctions.GetInitialKeyLength();

        /// <summary>Default implementation should not be called</summary>
        public unsafe void Serialize(ref Key source, void* destination) => storeFunctions.SerializeKey(ref source, destination);

        /// <summary>Default implementation should not be called</summary>
        public unsafe ref Key AsRef(void* source) => ref storeFunctions.KeyAsRef(source);

        /// <summary>Default implementation should not be called</summary>
        public unsafe void Initialize(void* source, void* end) => storeFunctions.InitializeKey(source, end);
    }

    internal class VariableLengthValueRedirector<Key, Value, StoreFunctions> : IVariableLengthStruct<Value>
        where StoreFunctions : IStoreFunctions<Key, Value>
    {
        StoreFunctions storeFunctions;

        internal VariableLengthValueRedirector(StoreFunctions storeFunctions) => this.storeFunctions = storeFunctions;

        /// <summary> Indicates whether <typeparamref name="Value"/> is a variable-length struct</summary>
        public bool IsVariableLength => storeFunctions.IsVariableLengthValue;

        /// <summary>Default implementation should not be called</summary>
        public int GetLength(ref Value value) => storeFunctions.GetValueLength(ref value);

        /// <summary>Default implementation should not be called</summary>
        public int GetInitialLength() => storeFunctions.GetInitialValueLength();

        /// <summary>Default implementation should not be called</summary>
        public unsafe void Serialize(ref Value source, void* destination) => storeFunctions.SerializeValue(ref source, destination);

        /// <summary>Default implementation should not be called</summary>
        public unsafe ref Value AsRef(void* source) => ref storeFunctions.ValueAsRef(source);

        /// <summary>Default implementation should not be called</summary>
        public unsafe void Initialize(void* source, void* end) => storeFunctions.InitializeValue(source, end);
    }


    /// <summary>
    /// Store functions for specific Key and Value
    /// </summary>
    public class StoreFunctions_Int_Int : StoreFunctions<int, int, IntFasterEqualityComparer, DefaultRecordDisposer<int, int>, DefaultVariableLengthStruct<int>, DefaultVariableLengthStruct<int>>
    {
        /// <summary>Default instance</summary>
        public static readonly StoreFunctions_Int_Int Default = new();

        /// <summary>Constructor</summary>
        public StoreFunctions_Int_Int()
            : base(new IntFasterEqualityComparer(), DefaultRecordDisposer<int, int>.Default, DefaultVariableLengthStruct<int>.Default, DefaultVariableLengthStruct<int>.Default)
        { }
    }

    /// <summary>
    /// Store functions for specific Key and Value
    /// </summary>
    public class StoreFunctions_Long_Long : StoreFunctions<long, long, LongFasterEqualityComparer, DefaultRecordDisposer<long, long>, DefaultVariableLengthStruct<long>, DefaultVariableLengthStruct<long>>
    {
        /// <summary>Default instance</summary>
        public static readonly StoreFunctions_Long_Long Default = new();

        /// <summary>Constructor</summary>
        public StoreFunctions_Long_Long()
            : base(new LongFasterEqualityComparer(), new DefaultRecordDisposer<long, long>(), new DefaultVariableLengthStruct<long>(), new DefaultVariableLengthStruct<long>())
        { }
    }

    /// <summary>
    /// Store functions for specific Key and Value
    /// </summary>
    public class StoreFunctions_SpanByte_SpanByte : StoreFunctions<SpanByte, SpanByte, SpanByteComparer, DefaultRecordDisposer<SpanByte, SpanByte>, SpanByteVarLenStruct, SpanByteVarLenStruct>
    {
        /// <summary>Default instance</summary>
        public static readonly StoreFunctions_Long_Long Default = new();

        /// <summary>Constructor</summary>
        public StoreFunctions_SpanByte_SpanByte()
            : base(new SpanByteComparer(), new DefaultRecordDisposer<SpanByte, SpanByte>(), new SpanByteVarLenStruct(), new SpanByteVarLenStruct())
        { }
    }
}
