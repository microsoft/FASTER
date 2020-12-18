namespace FASTER.libdpr
{
    public class DprWorkerCallbacks<TToken>
    {
        public void OnVersionStart(long version)
        {
            
        }

        public void OnVersionEnd(long version, TToken token)
        {
            
        }

        public bool GetRollbackToken(out TToken token)
        {
            token = default;
            return false;
        }

        public void OnRollbackComplete(long version)
        {
            
        }
    }
}