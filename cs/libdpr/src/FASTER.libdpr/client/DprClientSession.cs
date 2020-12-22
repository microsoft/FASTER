using FASTER.core;

namespace FASTER.libdpr
{
    public class DprClientSession
    {
        public CommitPoint GetCommitPoint()
        {
            return default;
        }
        
        public long IssueBatch(int batchSize, ref DprBatchRequestHeader header)
        {
            return 0;
        }

        public void ResolveBatch(ref DprBatchResponseHeader reply)
        {
            
        }
    }
}