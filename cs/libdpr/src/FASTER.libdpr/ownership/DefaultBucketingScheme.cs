using System;
using System.Diagnostics;

namespace FASTER.serverless
{
    /// <summary>
    /// The default bucketing scheme uses lower-order bits from the key's hash code to relate keys. This is unlikely
    /// to perform well as this scheme exploits little application-level locality. 
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    public class DefaultBucketingScheme<Key> : IBucketingScheme<Key>
        where Key : new()
    {
        private int numBits, mask;

        /// <summary>
        /// Construct a new DefaultBucketingScheme
        /// </summary>
        /// <param name="numBits"> number of bits in the hashcode to utilize</param>
        public DefaultBucketingScheme(int numBits = 32)
        {
            Debug.Assert(numBits > 0 && numBits <= 32, "Running out of bits in the hash code!");
            this.numBits = numBits;
            mask = 1 << numBits - 1;
        }

        /// <inheritdoc />
        public long GetBucket(Key key)
        {
            return key.GetHashCode() & mask;
        }
    }
}