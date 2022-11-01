using FASTER.core;

namespace FASTER.darq
{
    /// <summary>
    /// DARQ Settings
    /// </summary>
    public class DarqSettings
    {
        /// <summary>
        /// Device used for underlying log
        /// </summary>
        public IDevice LogDevice;

        /// <summary>
        /// Size of a page in the underlying log, in bytes. Must be a power of 2.
        /// </summary>
        public long PageSize = 1L << 22;
        
        /// <summary>
        /// Total size of in-memory part of log, in bytes. Must be a power of 2.
        /// Should be at least one page long
        /// Num pages = 2^(MemorySizeBits-PageSizeBits)
        /// </summary>
        public long MemorySize = 1L << 23;
        

        /// <summary>
        /// Size of a segment (group of pages), in bytes. Must be a power of 2.
        /// This is the granularity of files on disk
        /// </summary>
        public long SegmentSize = 1L << 30;

        /// <summary>
        /// Log commit manager - if you want to override the default implementation of commit.
        /// </summary>
        public ILogCommitManager LogCommitManager = null;

        /// <summary>
        /// Use specified directory (path) as base for storing and retrieving underlying log commits. By default,
        /// commits will be stored in a folder named log-commits under this directory. If not provided, 
        /// we use the base path of the log device by default.
        /// </summary>
        public string LogCommitDir = null;
        
        /// <summary>
        /// Type of checksum to add to log
        /// </summary>
        public LogChecksumType LogChecksum = LogChecksumType.None;

        /// <summary>
        /// Fraction of underlying log marked as mutable (uncommitted)
        /// </summary>
        public double MutableFraction = 0;
        
        /// <summary>
        /// When FastCommitMode is enabled, FasterLog will reduce commit critical path latency, but may result in slower
        /// recovery to a commit on restart. Additionally, FastCommitMode is only possible when log checksum is turned
        /// on.
        /// </summary>
        public bool FastCommitMode = false;

        /// <summary>
        /// When DeleteOnClose is true, DARQ will remove all persistent state on shutdown -- useful for testing but
        /// will result in data loss otherwise,
        /// </summary>
        public bool DeleteOnClose = false;

        /// <summary>
        /// Whether this DARQ runs in speculative mode.
        /// </summary>
        public bool Speculative = false;

        /// <summary>
        /// Create default configuration settings for DARQ. You need to create and specify LogDevice 
        /// explicitly with this API.
        /// Use Utility.ParseSize to specify sizes in familiar string notation (e.g., "4k" and "4 MB").
        /// </summary>
        public DarqSettings() { }

        /// <inheritdoc />
        public override string ToString()
        {
            var retStr = $"log memory: {Utility.PrettySize(MemorySize)}; log page: {Utility.PrettySize(PageSize)}; log segment: {Utility.PrettySize(SegmentSize)}";
            retStr += $"; log device: {(LogDevice == null ? "null" : LogDevice.GetType().Name)}";
            retStr += $"; mutable fraction: {MutableFraction}; fast commit mode: {(FastCommitMode ? "yes" : "no")}";
            retStr += $"; delete on close: {(DeleteOnClose ? "yes" : "no")}";
            return retStr;
        }
    }
}