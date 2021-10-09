// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 0162

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;

namespace FASTER.core
{
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
        /// Persisted iterators
        /// </summary>
        public Dictionary<string, long> Iterators;

        /// <summary>
        /// User-specified commit cookie
        /// </summary>
        public byte[] Cookie;

        /// <summary>
        /// Initialize
        /// </summary>
        public void Initialize()
        {
            BeginAddress = 0;
            FlushedUntilAddress = 0;
            Iterators = null;
            Cookie = null;
        }

        /// <summary>
        /// Initialize from stream
        /// </summary>
        /// <param name="reader"></param>
        public void Initialize(BinaryReader reader)
        {
            int version;
            long checkSum;
            try
            {
                version = reader.ReadInt32();
                checkSum = reader.ReadInt64();
                BeginAddress = reader.ReadInt64();
                FlushedUntilAddress = reader.ReadInt64();
            }
            catch (Exception e)
            {
                throw new FasterException("Unable to recover from previous commit. Inner exception: " + e.ToString());
            }
            if (version != 0)
                throw new FasterException("Invalid version found during commit recovery");

            if (checkSum != (BeginAddress ^ FlushedUntilAddress))
                throw new FasterException("Invalid checksum found during commit recovery");

            var count = 0;
            try
            {
                count = reader.ReadInt32();
            }
            catch { }

            if (count > 0)
            {
                Iterators = new Dictionary<string, long>();
                for (int i = 0; i < count; i++)
                {
                    Iterators.Add(reader.ReadString(), reader.ReadInt64());
                }
            }
            
            try
            {
                count = reader.ReadInt32();
            }
            catch { }

            if (count > 0)
                Cookie = reader.ReadBytes(count);
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
        public readonly byte[] ToByteArray()
        {
            using MemoryStream ms = new();
            using (BinaryWriter writer = new(ms))
            {
                writer.Write(0); // version
                writer.Write(BeginAddress ^ FlushedUntilAddress); // checksum
                writer.Write(BeginAddress);
                writer.Write(FlushedUntilAddress);
                if (Iterators?.Count > 0)
                {
                    writer.Write(Iterators.Count);
                    foreach (var kvp in Iterators)
                    {
                        writer.Write(kvp.Key);
                        writer.Write(kvp.Value);
                    }
                }
                else
                {
                    writer.Write(0);
                }

                if (Cookie != null)
                {
                    writer.Write(Cookie.Length);
                    writer.Write(Cookie);
                }
                else
                {
                    writer.Write(0);
                }
            }
            return ms.ToArray();
        }

        /// <summary>
        /// Take snapshot of persisted iterators
        /// </summary>
        /// <param name="persistedIterators">Persisted iterators</param>
        public void SnapshotIterators(ConcurrentDictionary<string, FasterLogScanIterator> persistedIterators)
        {
            if (persistedIterators.Count > 0)
            {
                Iterators = new Dictionary<string, long>();

                foreach (var kvp in persistedIterators)
                {
                    Iterators.Add(kvp.Key, kvp.Value.requestedCompletedUntilAddress);
                }
            }
        }

        /// <summary>
        /// Update iterators after persistence
        /// </summary>
        /// <param name="persistedIterators">Persisted iterators</param>
        public void CommitIterators(ConcurrentDictionary<string, FasterLogScanIterator> persistedIterators)
        {
            if (Iterators?.Count > 0)
            {
                foreach (var kvp in Iterators)
                {
                    persistedIterators[kvp.Key].UpdateCompletedUntilAddress(kvp.Value);
                }
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
