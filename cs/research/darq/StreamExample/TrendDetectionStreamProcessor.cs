using System.Diagnostics;
using FASTER.libdpr;

namespace SimpleStream.searchlist
{
    // a fake backend that emulates some big model with hashes and arithmetic
    public class TrendDetectorState
    {
        private Dictionary<string, int> aggregateState = new();

        // Update local state with entry, adding self-message to step for recoverability. Returns whether the entry
        // is considered an anomaly
        public bool Update(DarqMessage inMessage, StepRequestBuilder stepBuilder)
        {
            Debug.Assert(inMessage.GetMessageType() == DarqMessageType.IN);
            var message = inMessage.GetMessageBodyAsString();

            var split = message.Split(":");
            var key = split[0];
            var count = long.Parse(split[1]);
            
            // magic number for starting the hash
            int prevHash;
            if (!aggregateState.TryGetValue(key, out prevHash)) prevHash = 17;
            var newHash = aggregateState[key] = prevHash * 31 + count.GetHashCode();

            stepBuilder.AddSelfMessage($"{key} : {newHash}");
            // Simulate anomaly detection via some simple arithmetics
            return newHash % 256 == 0;
        }

        public void Apply(DarqMessage selfMessage)
        {
            Debug.Assert(selfMessage.GetMessageType() == DarqMessageType.SELF);
            unsafe
            {
                fixed (byte* b = selfMessage.GetMessageBody())
                {
                    var messageSize = *(int*)b;
                    var message = new string((sbyte*)b, sizeof(int), messageSize);
                    var split = message.Split(":");
                    var key = split[0];
                    var hash = int.Parse(split[1]);
                    aggregateState[key] = hash;
                }
            }
        }

        public void Checkpoint(long lsn, StepRequestBuilder stepBuilder)
        {
            stepBuilder.StartCheckpoint(lsn);
            foreach (var entry in aggregateState)
                stepBuilder.AddSelfMessage($"{entry.Key} : {entry.Value}");
        }

    }
    public class TrendDetectionStreamProcessor : IDarqProcessor
    {
        private WorkerId input, output;
        private IDarqProcessorClientCapabilities capabilities;
        private StepRequest reusableRequest;
        // Emulate some persistent insight stored about each key/entry with an integer hash code
        private TrendDetectorState state;
        private int uncheckpointedSize = 0, checkpointThreshold = 1 << 15; 

        
        public TrendDetectionStreamProcessor(WorkerId me, WorkerId output)
        {
            this.input = me;
            this.output = output;
            reusableRequest = new StepRequest(null);
            state = new TrendDetectorState();
        }
        
        public bool ProcessMessage(DarqMessage m)
        {
            switch (m.GetMessageType())
            {
                case DarqMessageType.SELF:
                    state.Apply(m);
                    m.Dispose();
                    return true;
                case DarqMessageType.IN:
                {
                    var builder = new StepRequestBuilder(reusableRequest, input);
                    if (state.Update(m, builder))
                        builder.AddOutMessage(output, m.GetMessageBody());
                    builder.MarkMessageConsumed(m.GetLsn());
                    
                    uncheckpointedSize++;
                    if (++uncheckpointedSize >= checkpointThreshold)
                    {
                        state.Checkpoint(m.GetNextLsn(), builder);
                        uncheckpointedSize = 0;
                    }
                    
                    capabilities.Step(builder.FinishStep());
                    m.Dispose();
                    return true;
                }
                default:
                    throw new NotImplementedException();
            }
        }

        public void OnRestart(IDarqProcessorClientCapabilities capabilities)
        {
            this.capabilities = capabilities;
            state = new TrendDetectorState();
        }
    }
}