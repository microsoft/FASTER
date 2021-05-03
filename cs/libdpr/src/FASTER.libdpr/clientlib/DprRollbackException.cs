using System;
using FASTER.core;

namespace FASTER.libdpr
{
    /// <summary>
    /// DprRollbackException is thrown when there is a failure in the cluster that results in some loss of uncommitted
    /// operations. 
    /// </summary>
    public class DprRollbackException : Exception
    {
        /// <summary>
        /// The exact cutoff for the operations that survived the rollback
        /// </summary>
        public CommitPoint RollbackPoint { get; }

        internal DprRollbackException(CommitPoint rollbackPoint)
        {
            this.RollbackPoint = rollbackPoint;
        }
    }
}