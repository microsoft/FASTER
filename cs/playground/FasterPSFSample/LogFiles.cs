using FASTER.core;
using System.IO;

namespace FasterPSFSample
{
    class LogFiles
    {
        private IDevice log;
        private IDevice objLog;

        internal LogSettings LogSettings { get; }

        internal LogFiles(bool useObjectValue, bool useReadCache)
        {
            // Create files for storing data. We only use one write thread to avoid disk contention.
            // We set deleteOnClose to true, so logs will auto-delete on completion.
            this.log = Devices.CreateLogDevice(Path.GetTempPath() + "hlog.log", deleteOnClose: true);
            if (useObjectValue)
                this.objLog = Devices.CreateLogDevice(Path.GetTempPath() + "hlog.obj.log", deleteOnClose: true);

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
