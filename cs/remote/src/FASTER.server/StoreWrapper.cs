using FASTER.core;

namespace FASTER.server
{
    /// <summary>
    /// Wrapper for store and store-specific information
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    internal class StoreWrapper<Key, Value>
    {
        public readonly FasterKV<Key, Value> store;

        public StoreWrapper(FasterKV<Key, Value> store, bool tryRecover)
        {
            this.store = store;

            if (tryRecover)
            {
                try
                {
                    store.Recover();
                }
                catch
                { }
            }
        }
    }
}