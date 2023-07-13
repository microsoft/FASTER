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
        public bool SearchNextHigherBin;

        /// <summary>
        /// Use power-of-2 bins with a single oversize bin.
        /// </summary>
        public static PowerOf2BinsRevivificationSettings PowerOf2Bins { get; } = new();

        /// <summary>
        /// Default bin for fixed-length.
        /// </summary>
        public static RevivificationSettings DefaultFixedLength { get; } = new() { FreeListBins = new[] { new RevivificationBin() { RecordSize = RevivificationBin.MaxRecordSize } } };

        /// <summary>
        /// Enable only in-tag-chain revivification; do not use FreeList
        /// </summary>
        public static RevivificationSettings InChainOnly { get; } = new();

        /// <summary>
        /// Turn off all revivification.
        /// </summary>
        public static RevivificationSettings None { get; } = new() { EnableRevivification = false };

        internal void Verify(bool isFixedRecordLength)
        {
            if (!EnableRevivification || FreeListBins?.Length == 0)
                return;
            if (isFixedRecordLength && FreeListBins?.Length > 1)
                throw new FasterException($"Only 1 bin may be specified with fixed-length datatypes (blittable or object)");
            if (FreeListBins is not null)
            { 
                foreach (var bin in FreeListBins)
                    bin.Verify(isFixedRecordLength);
            }
        }

        /// <inheritdoc/>
        public override string ToString() 
            => $"enabled {EnableRevivification}, #bins {FreeListBins?.Length}, searchNextBin {SearchNextHigherBin}";
    }

    /// <summary>
    /// Settings for a Revivification bin
    /// </summary>
    public struct RevivificationBin
    {
        /// <summary>
        /// The minimum size of a record; RecordInfo + int key/value, or any key/value combination below 8 bytes total, as record size is
        /// a multiple of 8.
        /// </summary>
        public const int MinRecordSize = 16;

        /// <summary>
        /// The maximum size of a record; must fit on a single page.
        /// </summary>
        public const int MaxRecordSize = 1 << LogSettings.kMaxPageSizeBits;

        /// <summary>
        /// The maximum size of a record whose size can be stored "inline" in the FreeRecord metadata. This is informational, not a limit;
        /// sizes larger than this are considered "oversize" and require calls to the allocator to determine exact record size, which is slower.
        /// </summary>
        public const int MaxInlineRecordSize = 1 << FreeRecord.kSizeBits;

        /// <summary>
        /// Scan all records in the bin for best fit.
        /// </summary>
        public const int BestFitScanAll = int.MaxValue;

        /// <summary>
        /// Use first-fit instead of best-fit.
        /// </summary>
        public const int UseFirstFit = 0;

        /// <summary>
        /// The default number of records per bin.
        /// </summary>
        public const int DefaultRecordsPerBin = 1024;

        /// <summary>
        /// The maximum size of records in this partition. This should be partitioned for your app. Ignored if this is the single bin
        /// for fixed-length records.
        /// </summary>
        public int RecordSize;

        /// <summary>
        /// The number of records for each partition. This count will be adjusted upward so the partition is cache-line aligned.
        /// </summary>
        /// <remarks>
        /// The first record is not available; its space is used to store the circular buffer read and write pointers
        /// </remarks>
        public int NumberOfRecords = DefaultRecordsPerBin;

        /// <summary>
        /// The maximum number of entries to scan for best fit after finding first fit. Ignored for fixed-length datatypes. 
        /// </summary>
        public int BestFitScanLimit = BestFitScanAll;

        /// <summary>
        /// Constructor
        /// </summary>
        public RevivificationBin()
        {
        }

        internal void Verify(bool isFixedLength)
        {
            if (!isFixedLength && (RecordSize < MinRecordSize || RecordSize > MaxRecordSize))
                throw new FasterException($"Invalid RecordSize {RecordSize}; must be >= {MinRecordSize} and <= {MaxRecordSize}");
            if (NumberOfRecords <= FreeRecordBin.MinRecordsPerBin)
                throw new FasterException($"Invalid NumberOfRecords {NumberOfRecords}; must be > {FreeRecordBin.MinRecordsPerBin}");
        }

        /// <inheritdoc/>
        public override string ToString()
        {
            string scanStr = this.BestFitScanLimit switch
            {
                BestFitScanAll => "ScanAll",
                UseFirstFit => "FirstFit",
                _ => this.BestFitScanLimit.ToString()
            };
            return $"recSize {RecordSize}, numRecs {NumberOfRecords}, scanLimit {scanStr}";
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

            // Start with the 16-byte bin
            for (var size = RevivificationBin.MinRecordSize; size <= RevivificationBin.MaxInlineRecordSize; size *= 2)
                binList.Add(new RevivificationBin ()
                    {
                        RecordSize = size,
                        NumberOfRecords = RevivificationBin.DefaultRecordsPerBin
                    });

            // Use one oversize bin.
            binList.Add(new RevivificationBin()
            {
                RecordSize = RevivificationBin.MaxRecordSize,
                NumberOfRecords = RevivificationBin.DefaultRecordsPerBin
            });
            this.FreeListBins = binList.ToArray();

            this.SearchNextHigherBin = true;
        }
    }
}
