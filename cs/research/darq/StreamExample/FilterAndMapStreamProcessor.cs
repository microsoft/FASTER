using System.Diagnostics;
using FASTER.libdpr;
using Newtonsoft.Json;

namespace SimpleStream.searchlist
{
    public class FilterAndMapStreamProcessor : IDarqProcessor
    {
        private WorkerId input, output;
        private IDarqProcessorClientCapabilities capabilities;
        private StepRequest reusableRequest;

        public FilterAndMapStreamProcessor(WorkerId me, WorkerId output)
        {
            this.input = me;
            this.output = output;
            reusableRequest = new StepRequest(null);
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
                            messageString = new string((sbyte*)b, sizeof(int), size);
                        }
                    }
                    var searchListItem =
                        JsonConvert.DeserializeObject<SearchListJson>(messageString);
                    Debug.Assert(searchListItem != null);
                    var requestBuilder = new StepRequestBuilder(reusableRequest, input);
                    requestBuilder.MarkMessageConsumed(m.GetLsn());
                    if (searchListItem.SearchTerm.Contains(SearchListStreamUtils.relevantSearchTerm))
                    {
                        unsafe
                        {
                            var buffer = stackalloc byte[sizeof(int) + sizeof(long)];
                            *(int*)buffer = SearchListStreamUtils.GetRegionCode(searchListItem.IP);
                            *(long*)(buffer + sizeof(int)) = searchListItem.Timestamp;
                            requestBuilder.AddOutMessage(output, new Span<byte>(buffer, sizeof(int) + sizeof(long)));
                        }
                    }
                    
                    capabilities.Step(requestBuilder.FinishStep());
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
        }
    }
}