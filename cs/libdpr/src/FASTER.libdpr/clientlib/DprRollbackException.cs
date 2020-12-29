using System;
using FASTER.core;

namespace FASTER.libdpr
{
    public class DprRollbackException : Exception
    {
        public CommitPoint rollbackPoint;

        public DprRollbackException(CommitPoint rollbackPoint)
        {
            this.rollbackPoint = rollbackPoint;
        }
    }
}