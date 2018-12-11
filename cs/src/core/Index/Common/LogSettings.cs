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
    }
}
