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
                    var searchListItem =
                        JsonConvert.DeserializeObject<SearchListJson>(m.GetMessageBodyAsString());
                    Debug.Assert(searchListItem != null);
                    var requestBuilder = new StepRequestBuilder(reusableRequest, input);
                    requestBuilder.MarkMessageConsumed(m.GetLsn());
                    if (searchListItem.SearchTerm.Contains(SearchListStreamUtils.relevantSearchTerm))
                    {
                        var outputMessage =
                            $"{SearchListStreamUtils.relevantSearchTerm} : {SearchListStreamUtils.GetRegionCode(searchListItem.IP)} : {searchListItem.Timestamp}";
                        requestBuilder.AddOutMessage(output, outputMessage);
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