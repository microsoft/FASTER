using System.Diagnostics;
using FASTER.libdpr;
using Newtonsoft.Json;

namespace SimpleStream.searchlist
{
    public class FilterAndMapStreamProcessor : IDarqProcessor
    {
        private WorkerId input, output;
        private IDarqProcessorClientCapabilities capabilities;
        private StepRequest reusableStepRequest;
        private StepRequestBuilder batchedStepBuilder;
        private int processedCount = 0, batchSize, currentlyBatched = 0;

        public FilterAndMapStreamProcessor(WorkerId me, WorkerId output, int batchSize = 10)
        {
            this.input = me;
            this.output = output;
            reusableStepRequest = new StepRequest(null);
            this.batchSize = batchSize;
            batchedStepBuilder = new StepRequestBuilder(reusableStepRequest, input);
        }

        public bool ProcessMessage(DarqMessage m)
        {
            switch (m.GetMessageType())
            {
                case DarqMessageType.IN:
                {
                    var message = m.GetMessageBody();
                    string messageString;
                    unsafe
                    {
                        fixed (byte* b = message)
                        {
                            var size = *(int*)b;
                            // This is a termination message
                            if (size == -1)
                            {
                                // Forward termination signal downstream
                                batchedStepBuilder.MarkMessageConsumed(m.GetLsn());
                                batchedStepBuilder.AddOutMessage(output, BitConverter.GetBytes(-1));
                                capabilities.Step(batchedStepBuilder.FinishStep());
                                m.Dispose();
                                
                                Console.WriteLine("########## Filter operator has received termination signal");
                                // Signal for DARQ to break out of the processing loop
                                return false;
                            }
                            messageString = new string((sbyte*)b, sizeof(int), size);
                        }
                    }
                    
                    var searchListItem =
                        JsonConvert.DeserializeObject<SearchListJson>(messageString);
                    Debug.Assert(searchListItem != null);
                    batchedStepBuilder.MarkMessageConsumed(m.GetLsn());
                    if (searchListItem.SearchTerm.Contains(SearchListStreamUtils.relevantSearchTerm))
                    {
                        unsafe
                        {
                            var buffer = stackalloc byte[sizeof(int) + sizeof(long)];
                            *(int*)buffer = SearchListStreamUtils.GetRegionCode(searchListItem.IP);
                            *(long*)(buffer + sizeof(int)) = searchListItem.Timestamp;
                            batchedStepBuilder.AddOutMessage(output, new Span<byte>(buffer, sizeof(int) + sizeof(long)));
                        }
                    }

                    if (++processedCount % 10000 == 0)
                        Console.WriteLine($"Filter operator processed {processedCount} messages");

                    if (++currentlyBatched == batchSize)
                    {
                        currentlyBatched = 0;
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
            processedCount = 0;
        }
    }
}