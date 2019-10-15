// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 0162

using System;
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
                throw new Exception("Unable to recover from previous commit. Inner exception: " + e.ToString());
            }
            if (version != 0)
                throw new Exception("Invalid version found during commit recovery");

            if (checkSum != (BeginAddress ^ FlushedUntilAddress))
                throw new Exception("Invalid checksum found during commit recovery");
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
                    writer.Write(0); // version
                    writer.Write(BeginAddress ^ FlushedUntilAddress); // checksum
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
