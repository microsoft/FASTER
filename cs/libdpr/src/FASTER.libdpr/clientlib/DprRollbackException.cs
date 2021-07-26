using System;

namespace FASTER.libdpr
{
    /// <summary>
    ///     DprRollbackException is thrown when there is a failure in the cluster that results in some loss of uncommitted
    ///     operations.
    /// </summary>
    public class DprRollbackException : Exception
    {
        internal DprRollbackException(IDprStateSnapshot recoveredCut)
        {
            RecoveredCut = recoveredCut;
        }

        /// <summary>
        ///     The exact cutoff for the operations that survived the rollback
        /// </summary>
        public IDprStateSnapshot RecoveredCut { get; }
    }
}