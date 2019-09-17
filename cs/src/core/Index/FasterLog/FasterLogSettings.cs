// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 0162

namespace FASTER.core.log
{
    /// <summary>
    /// FASTER Log Settings
    /// </summary>
    public class FasterLogSettings
    {
        /// <summary>
        /// Device used for log
        /// </summary>
        public IDevice LogDevice = new NullDevice();

        /// <summary>
        /// Size of a segment (group of pages), in bits
        /// </summary>
        public int PageSizeBits = 22;

        /// <summary>
        /// Size of a segment (group of pages), in bits
        /// </summary>
        public int SegmentSizeBits = 30;

        /// <summary>
        /// Total size of in-memory part of log, in bits
        /// </summary>
        public int MemorySizeBits = 26;

        internal LogSettings GetLogSettings()
        {
            return new LogSettings
            {
                LogDevice = LogDevice,
                PageSizeBits = PageSizeBits,
                SegmentSizeBits = SegmentSizeBits,
                MemorySizeBits = MemorySizeBits,
                CopyReadsToTail = false,
                MutableFraction = 0,
                ObjectLogDevice = null,
                ReadCacheSettings = null
            };
        }
    }
}
