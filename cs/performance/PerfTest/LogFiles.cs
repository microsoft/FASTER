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
        internal string directory;

        internal LogSettings LogSettings { get; }

        internal LogFiles(bool useObjectLog, bool useReadCache)
        {
            // Create files for storing data. We only use one write thread to avoid disk contention.
            // We set deleteOnClose to true, so logs will auto-delete on completion.
            this.directory = Path.GetTempPath();
            this.log = Devices.CreateLogDevice(this.directory + "hlog.log", deleteOnClose: true);
            if (useObjectLog)
                this.objLog = Devices.CreateLogDevice(this.directory + "hlog.obj.log", deleteOnClose: true);

            // Define settings for log
            this.LogSettings = new LogSettings { LogDevice = log, ObjectLogDevice = objLog };
            if (useReadCache)
                this.LogSettings.ReadCacheSettings = new ReadCacheSettings();
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
