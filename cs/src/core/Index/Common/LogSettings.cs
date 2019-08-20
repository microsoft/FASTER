// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.


using System;

namespace FASTER.core
{
    /// <summary>
    /// Configuration settings for serializing objects
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    public class SerializerSettings<Key, Value>
    {
        /// <summary>
        /// Key serializer
        /// </summary>
        public Func<IObjectSerializer<Key>> keySerializer;

        /// <summary>
        /// Value serializer
        /// </summary>
        public Func<IObjectSerializer<Value>> valueSerializer;
    }

    /// <summary>
    /// Interface for variable length in-place objects
    /// modeled as structs, in FASTER
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface IVariableLengthStruct<T>
    {
        /// <summary>
        /// Actual length of object
        /// </summary>
        /// <param name="t"></param>
        /// <returns></returns>
        int GetLength(ref T t);

        /// <summary>
        /// Average length of objects, make sure this includes the object
        /// header needed to compute the actual object length
        /// </summary>
        /// <returns></returns>
        int GetAverageLength();

        /// <summary>
        /// Initial length, when populating for RMW from given input
        /// </summary>
        /// <typeparam name="Input"></typeparam>
        /// <param name="input"></param>
        /// <returns></returns>
        int GetInitialLength<Input>(ref Input input);
    }


    /// <summary>
    /// Length specification for fixed size (normal) structs
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public struct FixedLengthStruct<T> : IVariableLengthStruct<T>
    {
        private static readonly int size = Utility.GetSize(default(T));

        /// <summary>
        /// Get average length
        /// </summary>
        /// <returns></returns>
        public int GetAverageLength()
        {
            return size;
        }

        /// <summary>
        /// Get initial length
        /// </summary>
        /// <typeparam name="Input"></typeparam>
        /// <param name="input"></param>
        /// <returns></returns>
        public int GetInitialLength<Input>(ref Input input)
        {
            return size;
        }

        /// <summary>
        /// Get length
        /// </summary>
        /// <param name="t"></param>
        /// <returns></returns>
        public int GetLength(ref T t)
        {
            return size;
        }
    }

    /// <summary>
    /// <see cref="IVariableLengthFunctions{Key, Value, Input}" /> implementation that always support in place editing
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    /// <typeparam name="Input"></typeparam>
    /// <typeparam name="Output"></typeparam>
    /// <typeparam name="Context"></typeparam>
    /// <typeparam name="Functions"></typeparam>
    public class FixedLengthFunctions<Key, Value, Input, Output, Context, Functions> : IVariableLengthFunctions<Key, Value, Input>
        where Key : new()
        where Value : new()
        where Functions : IFunctions<Key, Value, Input, Output, Context>
    {
        private Functions functions;

        /// <summary>
        /// Creates instance of <see cref="FixedLengthFunctions{Key, Value, Input, Output, Context, Functions}"/>
        /// </summary>
        /// <param name="functions"></param>
        public FixedLengthFunctions(Functions functions)
        {
            this.functions = functions;
        }

        /// <summary>
        /// Concurrent writer
        /// </summary>
        /// <param name="key"></param>
        /// <param name="src"></param>
        /// <param name="dst"></param>
        /// <returns>
        /// <code>true</code> - to always allow in place editing
        /// </returns>
        public bool ConcurrentWriter(ref Key key, ref Value src, ref Value dst)
        {
            functions.ConcurrentWriter(ref key, ref src, ref dst);
            return true;
        }

        /// <summary>
        /// In-place update for RMW
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="value"></param>
        /// <returns>
        /// <code>true</code> - to always allow in place editing
        /// </returns>
        public bool InPlaceUpdater(ref Key key, ref Input input, ref Value value)
        {
            functions.InPlaceUpdater(ref key, ref input, ref value);
            return true;
        }
    }


    /// <summary>
    /// Settings for variable length keys and values
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    /// <typeparam name="Input"></typeparam>
    public class VariableLengthStructSettings<Key, Value, Input>
    {
        /// <summary>
        /// Key length
        /// </summary>
        public IVariableLengthStruct<Key> keyLength;

        /// <summary>
        /// Value length
        /// </summary>
        public IVariableLengthStruct<Value> valueLength;

        /// <summary>
        /// Variable length functions
        /// </summary>
        public IVariableLengthFunctions<Key, Value, Input> functions;
    }


    /// <summary>
    /// Configuration settings for hybrid log
    /// </summary>
    public class LogSettings
    {
        /// <summary>
        /// Device used for main hybrid log
        /// </summary>
        public IDevice LogDevice = new NullDevice();

        /// <summary>
        /// Device used for serialized heap objects in hybrid log
        /// </summary>
        public IDevice ObjectLogDevice = new NullDevice();

        /// <summary>
        /// Size of a segment (group of pages), in bits
        /// </summary>
        public int PageSizeBits = 25;

        /// <summary>
        /// Size of a segment (group of pages), in bits
        /// </summary>
        public int SegmentSizeBits = 30;

        /// <summary>
        /// Total size of in-memory part of log, in bits
        /// </summary>
        public int MemorySizeBits = 34;

        /// <summary>
        /// Fraction of log marked as mutable (in-place updates)
        /// </summary>
        public double MutableFraction = 0.9;

        /// <summary>
        /// Copy reads to tail of log
        /// </summary>
        public bool CopyReadsToTail = false;

        /// <summary>
        /// Settings for optional read cache
        /// Overrides the "copy reads to tail" setting
        /// </summary>
        public ReadCacheSettings ReadCacheSettings = null;
    }

    /// <summary>
    /// Configuration settings for hybrid log
    /// </summary>
    public class ReadCacheSettings
    {
        /// <summary>
        /// Size of a segment (group of pages), in bits
        /// </summary>
        public int PageSizeBits = 25;

        /// <summary>
        /// Total size of in-memory part of log, in bits
        /// </summary>
        public int MemorySizeBits = 34;

        /// <summary>
        /// Fraction of log used for second chance copy to tail
        /// </summary>
        public double SecondChanceFraction = 0.9;
    }
}
