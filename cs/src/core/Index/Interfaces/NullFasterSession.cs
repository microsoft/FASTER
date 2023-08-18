namespace FASTER.core
{
    struct NullFasterSession : IFasterSession
    {
        public static readonly NullFasterSession Instance = new();

        public void CheckpointCompletionCallback(int sessionID, string sessionName, CommitPoint commitPoint)
        {
        }

        public void UnsafeResumeThread()
        {
        }

        public void UnsafeSuspendThread()
        {
        }
    }
}