using System.Threading.Tasks;
using FASTER.darq;

namespace FASTER.libdpr
{
    // TODO(Tianyu): Currently not thread-safe, but could be with some work
    public interface IDarqProcessorClientCapabilities
    {
        ValueTask<StepStatus> Step(StepRequest request);

        DprSession StartUsingDprSessionExternally();

        void StopUsingDprSessionExternally();
    }
    
    public interface IDarqProcessor
    {
        public bool ProcessMessage(DarqMessage m);
        
        public void OnRestart(IDarqProcessorClientCapabilities capabilities);
    }
    
    public interface IDarqProcessorClient
    {
        public void StartProcessing<T>(T processor) where T : IDarqProcessor;

        public Task StartProcessingAsync<T>(T processor) where T : IDarqProcessor;

        public void StopProcessing();

        public Task StopProcessingAsync();
    }
}