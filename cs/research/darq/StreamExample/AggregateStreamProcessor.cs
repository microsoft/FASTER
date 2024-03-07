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
        private Dictionary<int, long> currentBatchCount;
        private StepRequestBuilder batchedStepBuilder;
        private StepRequest reusableStepRequest;

        public AggregateStreamProcessor(WorkerId me, WorkerId output)
        {
            this.input = me;
            this.output = output;
            currentBatchCount = new Dictionary<int, long>();
            reusableStepRequest = new StepRequest(null);
            batchedStepBuilder = new StepRequestBuilder(reusableStepRequest, input);
        }
        
        public bool ProcessMessage(DarqMessage m)
        {
            switch (m.GetMessageType())
            {
                case DarqMessageType.IN:
                {
                    int region;
                    long timestamp;
                    unsafe
                    {
                        fixed (byte* b = m.GetMessageBody())
                        {
                            region = *(int*)b;
                            // This is a termination message
                            if (region == -1)
                            {
                                // Forward termination signal downstream
                                batchedStepBuilder.MarkMessageConsumed(m.GetLsn());
                                CloseWindow(long.MaxValue);
                                batchedStepBuilder.AddOutMessage(output, BitConverter.GetBytes(-1));
                                capabilities.Step(batchedStepBuilder.FinishStep());
                                m.Dispose();
                                
                                Console.WriteLine("########## Aggregate operator has received termination signal");
                                // Signal for DARQ to break out of the processing loop
                                return false;
                            }
                            timestamp = *(long*)(b + sizeof(int));
                        }
                    }

                    if (currentBatchStartTime == -1)
                        currentBatchStartTime = timestamp;
                    
                    if (!currentBatchCount.TryGetValue(region, out var c))
                        currentBatchCount[region] = 1;
                    else
                        currentBatchCount[region] = c + 1;
                    
                    batchedStepBuilder.MarkMessageConsumed(m.GetLsn());
                    if (timestamp > currentBatchStartTime + SearchListStreamUtils.WindowSizeMilli)
                    {
                        CloseWindow(timestamp);
                        capabilities.Step(batchedStepBuilder.FinishStep());
                        batchedStepBuilder = new StepRequestBuilder(reusableStepRequest, input);
                    }
                    
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
            batchedStepBuilder = new StepRequestBuilder(reusableStepRequest, input);
            currentBatchCount.Clear();
            currentBatchStartTime = -1;
        }

        private void CloseWindow(long timestamp)
        {
            unsafe
            {
                var buffer = stackalloc byte[sizeof(int) + sizeof(long)];
                foreach (var (k, count) in currentBatchCount)
                {
                    *(int*)buffer = k;
                    *(long*)(buffer + sizeof(int)) = count;
                    batchedStepBuilder.AddOutMessage(output,
                        new Span<byte>(buffer, sizeof(int) + sizeof(long)));
                }
            }
            
            Console.WriteLine($"Closed window at timestamp {timestamp}");
            currentBatchCount.Clear();
            currentBatchStartTime = timestamp;
        }
    }
}