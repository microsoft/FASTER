// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 0162

using System;
using System.Diagnostics;
using System.IO;

namespace FASTER.core
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
        /// Size of a page, in bits
        /// </summary>
        public int PageSizeBits = 22;

        /// <summary>
        /// Total size of in-memory part of log, in bits
        /// Should be at least one page long
        /// Num pages = 2^(MemorySizeBits-PageSizeBits)
        /// </summary>
        public int MemorySizeBits = 23;

        /// <summary>
        /// Size of a segment (group of pages), in bits
        /// This is the granularity of files on disk
        /// </summary>
        public int SegmentSizeBits = 30;

        /// <summary>
        /// Log commit manager
        /// </summary>
        public ILogCommitManager LogCommitManager = null;

        /// <summary>
        /// Use specified directory for storing and retrieving checkpoints
        /// This is a shortcut to providing the following:
        ///   FasterLogSettings.LogCommitManager = new LocalLogCommitManager(LogCommitFile)
        /// </summary>
        public string LogCommitFile = null;

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

    /// <summary>
    /// Recovery info for FASTER Log
    /// </summary>
    internal struct FasterLogRecoveryInfo
    {
        /// <summary>
        /// Begin address
        /// </summary>
        public long BeginAddress;

        /// <summary>
        /// Flushed logical address
        /// </summary>
        public long FlushedUntilAddress;


        /// <summary>
        /// Initialize
        /// </summary>
        public void Initialize()
        {
            BeginAddress = 0;
            FlushedUntilAddress = 0;
        }

        /// <summary>
        /// Initialize from stream
        /// </summary>
        /// <param name="reader"></param>
        public void Initialize(BinaryReader reader)
        {
            BeginAddress = reader.ReadInt64();
            FlushedUntilAddress = reader.ReadInt64();
        }

        /// <summary>
        ///  Recover info from token
        /// </summary>
        /// <param name="logCommitManager"></param>
        /// <returns></returns>
        internal void Recover(ILogCommitManager logCommitManager)
        {
            var metadata = logCommitManager.GetCommitMetadata();
            if (metadata == null)
                throw new Exception("Invalid log commit metadata during recovery");

            Initialize(new BinaryReader(new MemoryStream(metadata)));
        }

        /// <summary>
        /// Reset
        /// </summary>
        public void Reset()
        {
            Initialize();
        }

        /// <summary>
        /// Write info to byte array
        /// </summary>
        public byte[] ToByteArray()
        {
            using (var ms = new MemoryStream())
            {
                using (var writer = new BinaryWriter(ms))
                {
                    writer.Write(BeginAddress);
                    writer.Write(FlushedUntilAddress);
                }
                return ms.ToArray();
            }
        }

        /// <summary>
        /// Print checkpoint info for debugging purposes
        /// </summary>
        public void DebugPrint()
        {
            Debug.WriteLine("******** Log Commit Info ********");

            Debug.WriteLine("BeginAddress: {0}", BeginAddress);
            Debug.WriteLine("FlushedUntilAddress: {0}", FlushedUntilAddress);
        }
    }
}
