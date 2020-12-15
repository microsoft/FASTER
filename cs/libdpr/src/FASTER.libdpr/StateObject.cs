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

    public interface StateObjectSession<Message>
        where Message : AppendableMessage
    {
        long Version();
        
        // TODO(Tianyu): Add pending-handling?
        Status Operate(ref Message request, ref Message reply, long serialNum);

        bool ClearPending(bool spinWait);
    }

    public interface SessionedStateObject<Message, Token>
        where Message : AppendableMessage
    {
        StateObjectSession<Message> GetSession(Guid sessionId);

        long BeginCheckpoint(Action<(long, Token)> onPersist, long targetVersion = -1);

        long RestoreCheckpoint(Token token);
    }
}