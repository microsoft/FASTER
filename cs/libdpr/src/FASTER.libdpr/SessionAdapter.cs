using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using FASTER.core;

namespace FASTER.libdpr
{
    public class AdapterSession<Message, OpDescriptor, CheckpointToken> : StateObjectSession<Message>
        where Message : AppendableMessage
        where CheckpointToken : CheckpointToken<OpDescriptor>
    {
        private OpDescriptor latest;
        public SortedList<long, CheckpointToken> tokens;

        public long Version()
        {
            throw new System.NotImplementedException();
        }

        public Status Operate(ref Message request, ref Message reply, long serialNum)
        {
            throw new System.NotImplementedException();
        }

        public bool ClearPending(bool spinWait)
        {
            throw new NotImplementedException();
        }
    }
    
    public class SessionedStateObjectAdapter<Underlying, Message, OpDescriptor, CheckpointToken> : SessionedStateObject<Message, CheckpointToken>
        where Message : AppendableMessage
        where CheckpointToken : CheckpointToken<OpDescriptor>
        where Underlying : StateObject<Message, OpDescriptor, CheckpointToken>
    {
        public SortedList<long, CheckpointToken> tokens;
        private ConcurrentDictionary<Guid, AdapterSession<Message, OpDescriptor, CheckpointToken>> sessions;

        public StateObjectSession<Message> GetSession(Guid sessionId)
        {
            return sessions.GetOrAdd(sessionId, guid => new AdapterSession<Message, OpDescriptor, CheckpointToken>());
        }

        public long BeginCheckpoint(Action<(long, CheckpointToken)> onPersist, long targetVersion = -1)
        {
            throw new NotImplementedException();   
        }

        public long RestoreCheckpoint(CheckpointToken token)
        {
            throw new NotImplementedException();
        }
    }
}