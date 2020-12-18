using System.Collections.Generic;

namespace FASTER.libdpr
{
    public interface IDprTableSnapshot
    {
        long SafeVersion(Worker worker);
    }
    
    public interface IDprFinder
    {
        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        long SafeVersion(Worker worker);
        
        IDprTableSnapshot ReadSnapshot();

        long SystemWorldLine();

        long GlobalMaxVersion();

        /// <summary>
        /// 
        /// </summary>
        /// <param name="newVersion"></param>
        /// <returns></returns>
        void ReportNewPersistentVersion(WorkerVersion persisted, IEnumerable<WorkerVersion> deps);
        
        /// <summary>
        /// 
        /// </summary>
        /// <returns>system world line</returns>
        void Refresh();

        void Clear();

        void ReportRecovery(long worldLine, WorkerVersion latestRecoveredVersion);
    }
}