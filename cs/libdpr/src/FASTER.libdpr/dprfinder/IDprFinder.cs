using System.Collections.Generic;

namespace FASTER.libdpr
{
    /// <summary>
    /// A DprTableSnapshot is a consistent view of the current DPR cut in a  
    /// </summary>
    public interface IDprStateSnapshot
    {
        /// <summary>
        /// For a given version, returns the largest version number that is recoverable. Method may return arbitrary
        /// number for a worker that is not part of the cluster.
        /// </summary>
        /// <param name="worker">The worker in question</param>
        /// <returns>The largest version number that is recoverable for the given version (may be arbitrary if worker is
        /// not part of the cluster)</returns>
        long SafeVersion(Worker worker);
    }
    
    /// <summary>
    /// A DprFinder is the interface on each Worker/Client to report local checkpoint/recovery and receive guarantees/
    /// rollback requests. This may implement a distributed algorithm underneath or be backed by some other backend
    /// component.
    /// </summary>
    public interface IDprFinder
    {
        /// <summary>
        /// For a given version, returns the largest version number that is recoverable. Method may return arbitrary
        /// number for a worker that is not part of the cluster. This should be equivalent to calling
        /// ReadSnapshot().SafeVersion(worker) for some point in time. 
        /// </summary>
        /// <returns>The largest version number that is recoverable for the given version (may be arbitrary if worker is
        /// not part of the cluster)</returns>
        long SafeVersion(Worker worker);
        
        /// <summary>
        /// Obtains a consistent snapshot of current DPR cut of the system.
        /// </summary>
        /// <returns> a consistent snapshot of current DPR cut of the system </returns>

        IDprStateSnapshot GetStateSnapshot();

        /// <summary>
        /// Returns the current system world-line.
        /// </summary>
        /// <returns>the current system world-line</returns>
        long SystemWorldLine();

        /// <summary>
        /// Returns the max version number known across all workers in the cluster.
        /// </summary>
        /// <returns>the max version number known across all workers in the cluster</returns>
        long GlobalMaxVersion();
        
        /// <summary>
        /// Report a version as locally persistent with the given dependencies.
        ///
        /// It suffices for the dependencies to contain only the largest version number for each worker (e.g. if a
        /// version depends on (w1, 10) and (w1, 11), it suffices to only include (w1, 11), and need not contain
        /// self-dependencies to other versions of the local worker.)
        /// </summary>
        /// <param name="persisted"></param>
        /// <param name="deps"></param>
        void ReportNewPersistentVersion(WorkerVersion persisted, IEnumerable<WorkerVersion> deps);
        
        /// <summary>
        /// Refreshes the local view of the system. This method must be called periodically to receive up-to-date
        /// information about the rest of the cluster.
        /// </summary>
        void Refresh();

        /// <summary>
        /// Reports to the rest of the cluster that this worker has recovered from the failure that resulted in the
        /// given world-line to the given worker-version (i.e., the given version is the latest surviving version). 
        /// </summary>
        /// <param name="worldLine"></param>
        /// <param name="latestRecoveredVersion"></param>
        void ReportRecovery(long worldLine, WorkerVersion latestRecoveredVersion);

        long NewWorker(Worker id);

        void DeleteWorker(Worker id);
    }
}