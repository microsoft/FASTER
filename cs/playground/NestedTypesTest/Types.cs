// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

// !BLIT_T && !GENERIC_BLIT_T ==> T is a class or otherwise unblittable type

namespace NestedTypesTest
{
    #region Key

#if BLIT_KEY && GENERIC_BLIT_KEY

    // CompoundGroupKey<Empty, TimeKey<int>>

    /// <summary>
    /// Represents key value for a nested group apply branch.
    /// </summary>
    /// <typeparam name="TOuterKey">Key type for outer branch. Where there is no containing
    /// branch, this is Empty.</typeparam>
    /// <typeparam name="TInnerKey">Key type for nested branch.</typeparam>
    [DataContract]
    [EditorBrowsable(EditorBrowsableState.Never)]
    public struct CompoundGroupKey<TOuterKey, TInnerKey>
    {
        /// <summary>
        /// The value of the inner grouping key.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public TInnerKey _innerGroup;
        /// <summary>
        /// The value of the outer grouping key.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public TOuterKey _outerGroup;
        /// <summary>
        /// A hash code incorporating both key elements.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public int _hashCode;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public TInnerKey InnerGroup => _innerGroup;
        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public TOuterKey OuterGroup => _outerGroup;

        /// <summary>
        /// Provides a string representation of the compound grouping key.
        /// </summary>
        /// <returns>A string representation of the compound grouping key.</returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override string ToString() => new { OuterGroup, InnerGroup }.ToString();

        /// <summary>
        /// Provides a hashcode of the compound grouping key.
        /// </summary>
        /// <returns>A hashcode of the compound grouping key.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override int GetHashCode() => _hashCode;
    }

    /// <summary>
    /// Currently for internal use only - do not use directly.
    /// </summary>
    [DataContract]
    [EditorBrowsable(EditorBrowsableState.Never)]
    public struct TimeKey<TKey>
    {
        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public long timestamp;

        /// <summary>
        /// 
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public TKey key;
    }

#else // key is MyKey
    public
#if BLIT_KEY
        struct
#else
        class
#endif
        MyKey
    {
        public int key;
        public MyKey Clone()
        {
            return this;
        }

        public long GetHashCode64()
        {
            return Utility.GetHashCode(key);
        }

        public bool Equals(MyKey otherKey)
        {
            return key == otherKey.key;
        }
        public void Serialize(Stream toStream)
        {
            new BinaryWriter(toStream).Write(key);
        }

        public void Deserialize(Stream fromStream)
        {
            key = new BinaryReader(fromStream).ReadInt32();
        }
    }
#endif


    #endregion

    #region Value
#if BLIT_VALUE && GENERIC_BLIT_VALUE

    // WrappedState<long>

    /// <summary>
    /// Currently for internal use only - do not use directly.
    /// </summary>
    [DataContract]
    [EditorBrowsable(EditorBrowsableState.Never)]
    public struct WrappedState<TState>
    {
        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public long timestamp;

        /// <summary>
        /// 
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public TState state;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public ulong active;

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public WrappedState<TState> Clone()
        {
            return this;
        }

        //static ISerializer<TState> stateSerializer =
        //    StreamableSerializer.Create<TState>(new SerializerSettings { });

        /// <summary>
        /// 
        /// </summary>
        /// <param name="toStream"></param>
        public void Serialize(Stream toStream)
        {
            var w = new BinaryWriter(toStream);
            w.Write(timestamp);
            w.Write(active);
            //stateSerializer.Serialize(toStream, state);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fromStream"></param>
        public void Deserialize(Stream fromStream)
        {
            var r = new BinaryReader(fromStream);
            timestamp = r.ReadInt64();
            active = r.ReadUInt64();
            //state = stateSerializer.Deserialize(fromStream);
        }
    }
#else // value is MyValue
    public
#if BLIT_VALUE
        struct
#else
        class
#endif
    MyValue
    {
        public int value;
        public MyValue Clone()
        {
            return this;
        }

        public void Serialize(Stream toStream)
        {
            new BinaryWriter(toStream).Write(value);
        }

        public void Deserialize(Stream fromStream)
        {
            value = new BinaryReader(fromStream).ReadInt32();
        }
    }
#endif
    #endregion

    #region Input

#if BLIT_INPUT && GENERIC_BLIT_INPUT

    // WrappedInput<int, long>

    /// <summary>
    /// 
    /// </summary>
    public enum AggregationType : byte
    {
        /// <summary>
        /// 
        /// </summary>
        ACCUMULATE,
        /// <summary>
        /// 
        /// </summary>
        DEACCUMULATE,
        /// <summary>
        /// 
        /// </summary>
        DIFFERENCE
    }

    /// <summary>
    /// Currently for internal use only - do not use directly.
    /// </summary>
    [DataContract]
    [EditorBrowsable(EditorBrowsableState.Never)]
    public struct WrappedInput<TInput, TState>
    {
        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public long timestamp;

        /// <summary>
        /// 
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public TInput input;

        /// <summary>
        /// 
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public TState state;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public ulong active;

        /// <summary>
        /// 
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public AggregationType aggregationType;
    }

#else // input is MyInput
    public
#if BLIT_INPUT
        struct
#else
        class
#endif
 MyInput
    {
    }
#endif
    #endregion

    #region Output
#if BLIT_OUTPUT && GENERIC_BLIT_OUTPUT
    // WrappedState<long>
#else
            public 
#if BLIT_OUTPUT
    struct
#else
    class 
#endif
    MyOutput
    {
        public MyValue value;
    }

#endif
    #endregion

    #region Context

#if BLIT_CONTEXT && GENERIC_BLIT_CONTEXT
#else
    public
#if BLIT_CONTEXT
        struct
#else
        class
#endif
        MyContext
    {
    }
#endif
    #endregion
}
