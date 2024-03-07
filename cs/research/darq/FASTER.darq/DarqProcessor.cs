using FASTER.darq;

namespace FASTER.libdpr
{
    // TODO(Tianyu): Currently not thread-safe, but could be with some work
    /// <summary>
    /// IDarqProcessorClientCapabilities is supplied to DARQ processor implementations to provide access to DARQ features
    /// </summary>
    public interface IDarqProcessorClientCapabilities
    {
        /// <summary>
        /// Performs a step as requested
        /// </summary>
        /// <param name="request"> step request </param>
        /// <returns> status of the step </returns>
        ValueTask<StepStatus> Step(StepRequest request);

        /// <summary>
        /// For use if/when the processor needs to access external speculative state without going through DARQ.
        /// Processor should use the return DprSession to note down any external dependencies.
        /// WARNING: expect DARQ performance to degrade when externally using dpr session
        /// </summary>
        /// <returns> A DPR Session for use when reading/writing external state without DARQ messages </returns>
        DprSession StartUsingDprSessionExternally();

        /// <summary>
        /// Stop using DPR session externally.
        /// </summary>
        void StopUsingDprSessionExternally();
    }
    
    
    /// <summary>
    /// A DARQ Processor is the key abstraction that encapsulates business logic attached to DARQ instances.
    /// </summary>
    public interface IDarqProcessor
    {
        /// <summary>
        /// Process a new message intended for this DARQ instance
        /// </summary>
        /// <param name="m"> the message. Should be explicitly disposed when no longer needed. </param>
        /// <returns> True if the processor should continue receiving messages. False if the processing loop should exit.</returns>
        public bool ProcessMessage(DarqMessage m);
        
        /// <summary>
        /// Invoked when the DARQ processor (re)starts processing, either because it is attached to a DARQ for the first
        /// time or because it has to be restarted due to a failure. In the latter case, the processor should erase or
        /// otherwise repair its in-memory local state, which may no longer be consistent. 
        /// </summary>
        /// <param name="capabilities"> capabilities for use to interact with attached DARQ instance</param>
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