// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System.IO;

namespace FASTER.PerfTest
{
    class LogFiles
    {
        private IDevice log;
        private IDevice objLog;

        internal LogSettings LogSettings { get; }

        internal string CheckpointDir;

        internal LogFiles(bool useObjectLog, TestInputs testInputs)
        {
            // Create files for storing data. We only use one write thread to avoid disk contention.
            // We set deleteOnClose to true, so logs will auto-delete on completion.
            var directory = Path.GetTempPath();
            this.log = Devices.CreateLogDevice(directory + "hlog.log", deleteOnClose: true);
            if (useObjectLog)
                this.objLog = Devices.CreateLogDevice(directory + "hlog.obj.log", deleteOnClose: true);
            this.CheckpointDir = Path.Combine(directory, "PerfTest_chkpt");

            // Define settings for log
            this.LogSettings = new LogSettings
            {
                LogDevice = log, ObjectLogDevice = objLog,
                PageSizeBits = testInputs.LogPageSizeBits,
                SegmentSizeBits = testInputs.LogSegmentSizeBits,
                MemorySizeBits = testInputs.LogMemorySizeBits,
                MutableFraction = testInputs.LogMutableFraction,
                CopyReadsToTail = testInputs.LogCopyReadsToTail
            };
            if (testInputs.UseReadCache)
                this.LogSettings.ReadCacheSettings = new ReadCacheSettings
                { 
                    PageSizeBits = testInputs.ReadCachePageSizeBits,
                    MemorySizeBits = testInputs.ReadCacheMemorySizeBits,
                    SecondChanceFraction = testInputs.ReadCacheSecondChanceFraction
                };
        }

        internal void Close()
        {
            if (!(this.log is null))
            {
                this.log.Close();
                this.log = null;
            }
            if (!(this.objLog is null))
            {
                this.objLog.Close();
                this.objLog = null;
            }
        }
    }
}
