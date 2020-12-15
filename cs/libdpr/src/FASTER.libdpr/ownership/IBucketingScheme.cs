namespace FASTER.serverless
{
    /// <summary>
    /// A bucket is a group of keys logically related together. Ownership of a bucket is atomic. One bucket can only
    /// be owned by one worker or no worker, and never split. 
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    public interface IBucketingScheme<Key>
    {
        /// <summary></summary>
        /// <param name="key"></param>
        /// <returns>The bucket that the given key belongs to</returns>
        long GetBucket(Key key);
    }
}