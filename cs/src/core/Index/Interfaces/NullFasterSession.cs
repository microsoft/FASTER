namespace FASTER.core
{
    struct NullFasterSession : IFasterSession
    {
        public static readonly NullFasterSession Instance = new NullFasterSession();

        public void CheckpointCompletionCallback(string guid, CommitPoint commitPoint)
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