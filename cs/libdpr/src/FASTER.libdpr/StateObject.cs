using System;
using System.Threading;
using System.Threading.Tasks;
using FASTER.core;

namespace FASTER.libdpr
{
    public interface CheckpointToken<OpDescriptor>
    {
        bool Included(OpDescriptor op);
    }
    
    public interface StateObject<Message, OpDescriptor, CheckpointToken>
        where Message : AppendableMessage
        where CheckpointToken : CheckpointToken<OpDescriptor>
    {
        OpDescriptor Operate(ref Message request, ref Message reply);

        CheckpointToken BeginCheckpoint(Action<CheckpointToken> onPersist);

        void RestoreCheckpoint(CheckpointToken token);
    }

    public struct DprOpContext
    {
        // TODO(Tianyu): Place-holder
        private long stuff;
    }

    public interface OpCompletionCallback
    {
        public void Invoke(long serialNum, long version, DprOpContext ctx);
    }

    public interface StateObjectSession<Message, PendingCallback>
        where Message : AppendableMessage
        where PendingCallback : OpCompletionCallback
    {
        long Version();
        
        Status Operate(ref Message request, ref Message reply, long serialNum, DprOpContext ctx);

        bool ClearPending(bool spinWait);
    }

    public interface SessionedStateObject<Message, Token, PendingCallback>
        where Message : AppendableMessage
        where PendingCallback : OpCompletionCallback
    {
        StateObjectSession<Message, PendingCallback> GetSession(Guid sessionId);

        long BeginCheckpoint(Action<(long, Token)> onPersist, long targetVersion = -1);

        long RestoreCheckpoint(Token token);
    }
}