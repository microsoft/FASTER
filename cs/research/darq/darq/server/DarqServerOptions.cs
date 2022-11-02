using FASTER.client;
using FASTER.core;
using FASTER.libdpr;

namespace FASTER.server
{
    /// <summary>
    /// Options when creating DARQ server
    /// </summary>
    public class DarqServerOptions
    {
        /// <summary>
        /// Port to run server on.
        /// </summary>
        public int Port = 3278;

        /// <summary>
        /// IP address to bind server to.
        /// </summary>
        public string Address = "127.0.0.1";
        
        public Worker WorkerId;

        public FasterLogSettings LogSettings;

        public IDarqClusterInfo ClusterInfo;

        public bool Speculative = true;

        public bool ClearOnShutdown = false;

        public bool RunBackgroundThread = true;

        public int commitIntervalMilli = 25;
        public int refreshIntervalMilli = 10;
    }
}