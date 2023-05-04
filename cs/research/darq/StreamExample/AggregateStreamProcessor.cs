using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using FASTER.common;
using FASTER.core;
using FASTER.libdpr;

namespace SimpleStream.searchlist
{
    public class AggregateStreamProcessor : IDarqProcessor
    {
        private WorkerId input, output;
        private IDarqProcessorClientCapabilities capabilities;
        private long currentBatchStartTime = -1;
        public Dictionary<int, long> currentBatchCount;
        private StepRequestBuilder currentRequest;
        private StepRequest reusableRequest;

        public AggregateStreamProcessor(WorkerId me, WorkerId output)
        {
            this.input = me;
            this.output = output;
            currentBatchCount = new Dictionary<int, long>();
            reusableRequest = new StepRequest(null);
            currentRequest = new StepRequestBuilder(reusableRequest, input);
        }
        
        public bool ProcessMessage(DarqMessage m)
        {
            switch (m.GetMessageType())
            {
                case DarqMessageType.IN:
                {
                    int region;
                    long timestamp;
                    var message = m.GetMessageBody();
                    unsafe
                    {
                        fixed (byte* b = m.GetMessageBody())
                        {
                            region = *(int*)b;
                            timestamp = *(long*)(b + sizeof(int));
                        }
                    }

                    if (currentBatchStartTime == -1)
                        currentBatchStartTime = timestamp;
                    
                    if (!currentBatchCount.TryGetValue(region, out var c))
                        currentBatchCount[region] = 1;
                    else
                        currentBatchCount[region] = c + 1;

                    if (timestamp > currentBatchStartTime + SearchListStreamUtils.WindowSizeMilli)
                    {
                        unsafe
                        {
                            var buffer = stackalloc byte[sizeof(int) + sizeof(long)];
                            foreach (var (k, count) in currentBatchCount)
                            {
                                *(int*)buffer = k;
                                *(long*)(buffer + sizeof(int)) = count;
                                currentRequest.AddOutMessage(output,
                                    new Span<byte>(buffer, sizeof(int) + sizeof(long)));
                            }
                        }

                        capabilities.Step(currentRequest.FinishStep());
                        currentRequest = new StepRequestBuilder(reusableRequest, input);
                        currentBatchCount.Clear();
                        currentBatchStartTime = timestamp;
                    }
                    
                    currentRequest.MarkMessageConsumed(m.GetLsn());
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
            currentRequest = new StepRequestBuilder(reusableRequest, input);
            currentBatchCount.Clear();
            currentBatchStartTime = -1;
        }
    }
}