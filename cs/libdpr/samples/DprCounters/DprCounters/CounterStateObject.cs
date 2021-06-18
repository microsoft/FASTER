using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using FASTER.libdpr;

namespace DprCounters
{
    /// <summary>
    /// StateObject that encapsulates a single atomic counter
    /// </summary>
    /// A counter is an example of a simple state object --- one that does not need the fine grained version control
    /// ability exposed by the full StateObject interface for concurrent access performance or (single-node) checkpoint
    /// coordination. We can therefore just extend from SimpleStateObject
    public sealed class CounterStateObject : SimpleStateObject
    {
        private string checkpointDirectory;
        private ConcurrentDictionary<long, long> prevCounters = new();
        public long value;

        /// <summary>
        /// Constructs a new CounterStateObject
        /// </summary>
        /// <param name="checkpointDirectory">directory name to write checkpoints to</param>
        /// <param name="version">
        /// version to start at. If version is not 0, CounterStateObject will attempt to restore
        /// state from corresponding checkpoint
        /// </param>
        public CounterStateObject(string checkpointDirectory, long version)
        {
            
            this.checkpointDirectory = checkpointDirectory;
            if (version != 0)
                RestoreCheckpoint(version);
        }
        
        // With SimpleStateObject, CounterStateObject only needs to implement a single-threaded
        // checkpoint scheme.
        protected override void PerformCheckpoint(long version, Action onPersist)
        {
            // Use a simple naming scheme to associate checkpoints with versions. A more sophisticated scheme may
            // store persistent mappings or use other schemes to do so.
            var fileName = Path.Join(checkpointDirectory, version.ToString());
            var fs = File.Open(fileName, FileMode.OpenOrCreate, FileAccess.Write, FileShare.None);

            // libDPR will ensure that request batches that are protected with VersionScheme.Enter() and
            // VersionScheme.Leave() will not interleave with checkpoint or recovery code. It is therefore safe
            // to read and write values without protection in this function
            prevCounters[version] = value;
            
            // Once the content of the checkpoint is established (we have read a current snapshot of value), it is ok
            // to write to disk asynchronously and allow other operations to continue. In SimpleStateObject, 
            // operations are blocked before PerformCheckpoint return.
            fs.WriteAsync(BitConverter.GetBytes(value), 0, sizeof(long)).ContinueWith(token =>
            {
                if (!token.IsCompletedSuccessfully)
                    Console.WriteLine($"Error {token} during checkpoint");
                // We need to invoke onPersist() to inform DPR when a checkpoint is on disk
                onPersist();
                fs.Dispose();
            });
        }
        
        // With SimpleStateObject, CounterStateObject can just implement a single-threaded blocking recovery function
        protected override void RestoreCheckpoint(long version)
        {
            // RestoreCheckpoint is only called on machines that did not physically go down (otherwise they will simply
            // load the surviving version on restart). libDPR will additionally never request a worker to restore
            // checkpoints earlier than the committed version in the DPR cut. We can therefore rely on a (relatively
            // small) stash of in-memory snapshots to quickly handle this call.
            value = prevCounters[version];
            
            // Remove any cached versions larger than the restored ones because those are rolled back.
            PruneCachedVersions(v => v > version);
        }

        public void PruneCachedVersions(Predicate<long> versionPredicate)
        {
            var matchingKeys = prevCounters.Keys.Where(versionPredicate.Invoke).ToArray();
            foreach (var key in matchingKeys)
                prevCounters.TryRemove(key, out _);
        }
    }
}