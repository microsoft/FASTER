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
        private long largestTimestampInBatch;
        public Dictionary<string, long> currentBatchCount;
        private StepRequestBuilder currentRequest;
        private StepRequest reusableRequest;

        public AggregateStreamProcessor(WorkerId me, WorkerId output)
        {
            this.input = me;
            this.output = output;
            currentBatchCount = new Dictionary<string, long>();
            reusableRequest = new StepRequest(null);
            currentRequest = new StepRequestBuilder(reusableRequest, input);
        }
        
        public bool ProcessMessage(DarqMessage m)
        {
            switch (m.GetMessageType())
            {
                case DarqMessageType.IN:
                {
                    string message;
                    unsafe
                    {
                        fixed (byte* b = m.GetMessageBody)
                        {
                            var messageSize = *(int*)b;
                            message = new string((sbyte*)b, sizeof(int), messageSize);
                        }
                    }
                    

                    var split = message.Split(":");
                    Debug.Assert(split.Length == 3);
                    var term = split[0].Trim();
                    Debug.Assert(term.Equals(SearchListStreamUtils.relevantSearchTerm));
                    var region = split[1].Trim();
                    var timestamp = long.Parse(split[2].Trim());
                    if (currentBatchStartTime == -1)
                        currentBatchStartTime = timestamp;
                    
                    if (!currentBatchCount.TryGetValue(region, out var c))
                        currentBatchCount[region] = 1;
                    else
                        currentBatchCount[region] = c + 1;

                    if (timestamp > currentBatchStartTime + SearchListStreamUtils.WindowSizeMilli)
                    {
                        foreach (var (k, count) in currentBatchCount)
                            currentRequest.AddOutMessage(output, $"{k} : {count} : {largestTimestampInBatch}");
                        capabilities.Step(currentRequest.FinishStep());
                        currentRequest = new StepRequestBuilder(reusableRequest, input);
                        currentBatchCount.Clear();
                        currentBatchStartTime = timestamp;
                    }
                    
                    largestTimestampInBatch = Math.Max(largestTimestampInBatch, timestamp);
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
            largestTimestampInBatch = 0;

        }
    }
}