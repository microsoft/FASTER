// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Diagnostics.CodeAnalysis;

namespace FASTER.core
{
    /// <summary>
    /// FasterKVSettings extensions, to provide a simpler interface over composition due to the lack of multiple inheritance.
    /// </summary>
    public static class FasterKVFactory
    {
        /// <summary>Creator for <see cref="FasterKV{Key, Value, StoreFunctions, Allocator}"/></summary>
#if NETSTANDARD2_1 || NET
        [return: NotNull]
#endif
        public static FasterKV<Key, Value, StoreFunctions<Key, Value, KeyComparer, DefaultRecordDisposer<Key, Value>, DefaultVariableLengthStruct<Key>, DefaultVariableLengthStruct<Value>>,
                                                          BlittableAllocator<Key, Value, StoreFunctions<Key, Value, KeyComparer, DefaultRecordDisposer<Key, Value>, DefaultVariableLengthStruct<Key>, DefaultVariableLengthStruct<Value>>>> 
            CreateKV<Key, Value, KeyComparer>(this FasterKVSettings<Key, Value> settings, KeyComparer keyComparer)
            where KeyComparer : IFasterEqualityComparer<Key>
            => new(settings, new StoreFunctions<Key, Value, KeyComparer, DefaultRecordDisposer<Key, Value>, DefaultVariableLengthStruct<Key>, DefaultVariableLengthStruct<Value>>(
                                        keyComparer, DefaultRecordDisposer<Key, Value>.Default, DefaultVariableLengthStruct<Key>.Default, DefaultVariableLengthStruct<Value>.Default));

        /// <summary>Creator for <see cref="FasterKV{Key, Value, StoreFunctions, Allocator}"/></summary>
#if NETSTANDARD2_1 || NET
        [return: NotNull]
#endif
        public static FasterKV<Key, Value, StoreFunctions<Key, Value, KeyComparer, Disposer, DefaultVariableLengthStruct<Key>, DefaultVariableLengthStruct<Value>>,
                                                          BlittableAllocator<Key, Value, StoreFunctions<Key, Value, KeyComparer, Disposer, DefaultVariableLengthStruct<Key>, DefaultVariableLengthStruct<Value>>>>
            CreateKV<Key, Value, KeyComparer, Disposer>(this FasterKVSettings<Key, Value> settings, KeyComparer keyComparer, Disposer disposer)
            where KeyComparer : IFasterEqualityComparer<Key>
            where Disposer : IRecordDisposer<Key, Value>
            => new(settings, new StoreFunctions<Key, Value, KeyComparer, Disposer, DefaultVariableLengthStruct<Key>, DefaultVariableLengthStruct<Value>>(
                                        keyComparer, disposer, DefaultVariableLengthStruct<Key>.Default, DefaultVariableLengthStruct<Value>.Default));

        /// <summary>Creator for <see cref="FasterKV{Key, Value, StoreFunctions, Allocator}"/></summary>
#if NETSTANDARD2_1 || NET
        [return: NotNull]
#endif
        public static FasterKV<Key, Value, StoreFunctions<Key, Value, KeyComparer, Disposer, KeyLength, ValueLength>,
                                                          BlittableAllocator<Key, Value, StoreFunctions<Key, Value, KeyComparer, Disposer, KeyLength, ValueLength>>>
            CreateKV<Key, Value, KeyComparer, Disposer, KeyLength, ValueLength>(this FasterKVSettings<Key, Value> settings, KeyComparer keyComparer, Disposer disposer, KeyLength keyLength, ValueLength valueLength)
            where KeyComparer : IFasterEqualityComparer<Key>
            where Disposer : IRecordDisposer<Key, Value>
            where KeyLength : IVariableLengthStruct<Key>
            where ValueLength : IVariableLengthStruct<Value>
            => new(settings, new StoreFunctions<Key, Value, KeyComparer, Disposer, KeyLength, ValueLength>(keyComparer, disposer, keyLength, valueLength));
    }

    /// <summary>
    /// FasterKVSettings extensions, to provide a simpler interface over composition due to the lack of multiple inheritance.
    /// </summary>
    public static class FasterKVFactory_Int_Int
    {
        /// <summary>Creator for <see cref="FasterKV{Key, Value, StoreFunctions, Allocator}"/></summary>
#if NETSTANDARD2_1 || NET
        [return: NotNull]
#endif
        public static FasterKV<int, int, StoreFunctions_Int_Int, BlittableAllocator<int, int, StoreFunctions_Int_Int>> CreateKV(this FasterKVSettings<int, int> settings) => new(settings, new StoreFunctions_Int_Int());
    }

    /// <summary>
    /// FasterKVSettings extensions, to provide a simpler interface over composition due to the lack of multiple inheritance.
    /// </summary>
    public static class FasterKVFactory_Long_Long
    {
        /// <summary>Creator for <see cref="FasterKV{Key, Value, StoreFunctions, Allocator}"/></summary>
#if NETSTANDARD2_1 || NET
        [return: NotNull]
#endif
        public static FasterKV<long, long, StoreFunctions_Long_Long, BlittableAllocator<long, long, StoreFunctions_Long_Long>> CreateKV(this FasterKVSettings<long, long> settings) => new(settings, new StoreFunctions_Long_Long());
    }

    /// <summary>
    /// FasterKVSettings extensions, to provide a simpler interface over composition due to the lack of multiple inheritance.
    /// </summary>
    public static class FasterKVFactory_SpanByte_SpanByte
    {
        /// <summary>Creator for <see cref="FasterKV{Key, Value, StoreFunctions, Allocator}"/></summary>
#if NETSTANDARD2_1 || NET
        [return: NotNull]
#endif
        public static FasterKV<SpanByte, SpanByte, StoreFunctions_SpanByte_SpanByte, VariableLengthBlittableAllocator<SpanByte, SpanByte, StoreFunctions_SpanByte_SpanByte>> CreateKV(this FasterKVSettings<SpanByte, SpanByte> settings)
            => new(settings, new StoreFunctions_SpanByte_SpanByte());
    }
}
