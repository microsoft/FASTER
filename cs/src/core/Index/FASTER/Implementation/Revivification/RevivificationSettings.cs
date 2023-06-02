// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;

namespace FASTER.core
{
    /// <summary>
    /// Settings for record Revivification
    /// </summary>
    public class RevivificationSettings
    {
        /// <summary>
        /// Indicates whether deleted record space should be reused.
        /// <list type="bullet">
        /// <li>If this is true, then tombstoned records in the hashtable chain are revivified if possible, and a FreeList is maintained if 
        ///     <see cref="FreeListBins"/> is non-null and non-empty.
        /// </li>
        /// <li>If this is false, then tombstoned records in the hashtable chain will not be revivified, and no FreeList is used (regardless 
        ///     of the setting of <see cref="FreeListBins"/>).
        /// </li>
        /// </list>
        /// </summary>
        public bool EnableRevivification = true;

        /// <summary>
        /// Bin definitions for the free list (in addition to any in the hash chains). These must be ordered by <see cref="RevivificationBin.RecordSize"/>.
        /// </summary>
        /// <remarks>
        /// If the Key and Value are both fixed-length datatypes (either blittable or object), this must contain a single bin whose
        /// <see cref="RevivificationBin.RecordSize"/> is ignored. Otherwise, one or both of the Key and Value are variable-length,
        /// and this usually contains multiple bins.
        /// </remarks>
        public RevivificationBin[] FreeListBins;

        /// <summary>
        /// By default, when looking for FreeRecords we search only the bin for the specified size. This allows searching the next-highest bin as well.
        /// </summary>
        public bool SearchNextHighestBin;

        /// <summary>
        /// Use power-of-2 bins with a single oversize bin.
        /// </summary>
        public static PowerOf2BinsRevivificationSettings PowerOf2Bins { get; } = new();

        /// <summary>
        /// Default bin for fixed-length.
        /// </summary>
        public static RevivificationSettings DefaultFixedLength { get; } = new() { FreeListBins = new[] { new RevivificationBin() } };

        /// <summary>
        /// Enable only in-tag-chain revivification; do not use FreeList
        /// </summary>
        public static RevivificationSettings InChainOnly { get; } = new();

        /// <summary>
        /// Turn off all revivification.
        /// </summary>
        public static RevivificationSettings None { get; } = new() { EnableRevivification = false };

        internal void Verify(bool isFixedLength)
        {
            if (!EnableRevivification || FreeListBins?.Length == 0)
                return;
            if (isFixedLength && FreeListBins.Length > 1)
                throw new FasterException($"Only 1 bin may be specified with fixed-length datatypes (blittable or object)");
            foreach (var bin in FreeListBins)
                bin.Verify(isFixedLength);
        }
    }

    /// <summary>
    /// Settings for a Revivification bin
    /// </summary>
    public struct RevivificationBin
    {
        /// <summary>
        /// The maximum size of a record whose size can be stored "inline" in the FreeRecord metadata. This is informational, not a limit;
        /// sizes larger than this are considered "oversize" and require calls to the allocator to determine exact record size, which is slower.
        /// </summary>
        public const int MaxInlineRecordSize = 1 << FreeRecord.kSizeBits;

        /// <summary>
        /// The default number of records per partition.
        /// </summary>
        public const int DefaultRecordsPerPartition = 1024;

        /// <summary>
        /// The maximum size of records in this partition. This should be partitioned for your app. Ignored if this is the single bin
        /// for fixed-length records.
        /// </summary>
        public int RecordSize;

        /// <summary>
        /// To avoid conflicts, we can create the bin with multiple partitions; each thread maps to one of those partition
        /// to start its traversal. Each partition is its own circular buffer.
        /// </summary>
        /// <remarks>
        /// There must be at least one partition.
        /// </remarks>
        public int NumberOfPartitions = Environment.ProcessorCount / 2;

        /// <summary>
        /// The number of records for each partition. This count will be adjusted upward so the partition is cache-line aligned.
        /// </summary>
        /// <remarks>
        /// The first record is not available; its space is used to store the circular buffer read and write pointers
        /// </remarks>
        public int NumberOfRecordsPerPartition = DefaultRecordsPerPartition;

        /// <summary>
        /// The number of partitions to traverse; by default (0), all of them.
        /// </summary>
        public int NumberOfPartitionsToTraverse;

        /// <summary>
        /// Constructor
        /// </summary>
        public RevivificationBin()
        {
        }

        internal void Verify(bool isFixedLength)
        {
            if (!isFixedLength && RecordSize < FreeRecordPool.MinRecordSize)
                throw new FasterException($"Invalid RecordSize {RecordSize}; must be >= {FreeRecordPool.MinRecordSize}");
            if (NumberOfPartitions <= 1)
                throw new FasterException($"Invalid NumberOfPartitions {NumberOfPartitions}; must be > 1");
            if (NumberOfRecordsPerPartition <= FreeRecordBin.MinimumPartitionSize)
                throw new FasterException($"Invalid NumberOfRecordsPerPartition {NumberOfRecordsPerPartition}; must be > {FreeRecordBin.MinimumPartitionSize}");
            if (NumberOfPartitionsToTraverse < 0 || NumberOfPartitionsToTraverse > NumberOfPartitions)
                throw new FasterException($"NumberOfPartitionsToTraverse {NumberOfPartitionsToTraverse} must be >= 0 and must not exceed NumberOfPartitions {NumberOfPartitions}; use 0 for 'all'");
        }
    }

    /// <summary>
    /// Default revivification bin definition: Use power-of-2 bins with a single oversize bin.
    /// </summary>
    public class PowerOf2BinsRevivificationSettings : RevivificationSettings
    {
        /// <summary>
        /// Constructor
        /// </summary>
        public PowerOf2BinsRevivificationSettings() : base()
        {
            List<RevivificationBin> binList = new();
            for (var size = FreeRecordPool.MinRecordSize; size <= RevivificationBin.MaxInlineRecordSize; size *= 2)
                binList.Add(new RevivificationBin ()
                    {
                        RecordSize = size,
                        NumberOfRecordsPerPartition = 64
                    });

            // Use one oversize bin.
            binList.Add(new RevivificationBin()
            {
                RecordSize = int.MaxValue,
                NumberOfRecordsPerPartition = 64
            });
            this.FreeListBins = binList.ToArray();
        }
    }
}
