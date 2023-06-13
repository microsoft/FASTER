using System.Diagnostics;
using FASTER.libdpr;

namespace SimpleStream.searchlist
{
    // a fake backend that emulates some big model, but using hashes and arithmetic
    public class TrendDetectorState
    {
        private Dictionary<int, int> aggregateState = new();

        // Update local state with entry, adding self-message to step for recoverability. Returns whether the entry
        // is considered an anomaly
        public bool Update(DarqMessage inMessage, StepRequestBuilder stepBuilder)
        {
            Debug.Assert(inMessage.GetMessageType() == DarqMessageType.IN);
            int key;
            long count;
            unsafe
            {
                fixed (byte* b = inMessage.GetMessageBody())
                {
                    key = *(int*)b;
                    count = *(long*)(b + sizeof(int));
                }
            }
            // magic number for starting the hash
            int prevHash;
            if (!aggregateState.TryGetValue(key, out prevHash)) prevHash = 17;
            // Simulate updating the model state with hashing
            var newHash = aggregateState[key] = prevHash * 31 + count.GetHashCode();

            unsafe
            {
                var buffer = stackalloc byte[2 * sizeof(int)];
                *(int*)buffer = key;
                *(int*)(buffer + sizeof(int)) = newHash;
                stepBuilder.AddSelfMessage(new Span<byte>(buffer, 2 * sizeof(int)));
            }

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
                    var key = *(int*) b ;
                    var hash = *(int *) (b + sizeof(int));
                    aggregateState[key] = hash;
                }
            }
        }

        public void Checkpoint(long lsn, StepRequestBuilder stepBuilder)
        {
            stepBuilder.ConsumeUntil(lsn);
            unsafe
            {
                var buffer = stackalloc byte[2 * sizeof(int)];

                foreach (var entry in aggregateState)
                {
                    *(int*)buffer = entry.Key;
                    *(int*)(buffer + sizeof(int)) = entry.Value;
                    stepBuilder.AddSelfMessage(new Span<byte>(buffer, 2 * sizeof(int)));
                }
            }
        }

    }
    public class TrendDetectionStreamProcessor : IDarqProcessor
    {
        private WorkerId input;
        private IDarqProcessorClientCapabilities capabilities;
        private StepRequest reusableRequest;
        // Emulate some persistent insight stored about each key/entry with an integer hash code
        private TrendDetectorState state;
        private int uncheckpointedSize = 0, checkpointThreshold = 1 << 15; 

        
        public TrendDetectionStreamProcessor(WorkerId me)
        {
            this.input = me;
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
                    StepRequestBuilder builder;
                    if (m.GetMessageBody().Length == sizeof(int) && BitConverter.ToInt32(m.GetMessageBody()) == -1)
                    {
                        // Exit upon encountering stop signal
                        m.Dispose();
                        return false;
                    }
                    builder = new StepRequestBuilder(reusableRequest, input);
                    if (state.Update(m, builder))
                    {
                        // Output to stdout
                        Console.WriteLine("Anomaly Detected");
                    }
                    
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