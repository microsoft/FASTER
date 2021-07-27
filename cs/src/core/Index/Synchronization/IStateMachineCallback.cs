namespace FASTER.core
{
    /// <summary>
    /// Encapsulates custom logic to be executed as part of FASTER's state machine logic
    /// </summary>
    public interface IStateMachineCallback
    {
        /// <summary>
        /// Invoked immediately before every state transition.
        /// </summary>
        /// <param name="next"> next system state </param>
        /// <param name="faster"> reference to FASTER K-V </param>
        /// <typeparam name="Key">Key Type</typeparam>
        /// <typeparam name="Value">Value Type</typeparam>
        void BeforeEnteringState<Key, Value>(SystemState next, FasterKV<Key, Value> faster);
    }
}