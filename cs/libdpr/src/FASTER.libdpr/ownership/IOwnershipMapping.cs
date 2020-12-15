using System.Threading.Tasks;

namespace FASTER.libdpr
{
    /// <summary>
    /// A ownership mapping Store is assumed to be a reliable, persistent K-V store that stores information about
    /// bucket ownership among workers. It is invoked infrequently, so its performance is not important
    /// for the general case of Faster-serverless
    /// </summary>
    /// <typeparam name="WorkerType"></typeparam>
    public interface IOwnershipMapping
    {
        /// <summary>
        /// Find the owner of the given bucket. Information may be out-of-date.
        /// </summary>
        /// <param name="bucket"></param>
        /// <returns>whether the bucket currently has an owner</returns>
        ValueTask<Worker> LookupAsync(long bucket);

        /// <summary>
        /// Try to set owner of the bucket to be the given owner. Ownership cannot be transferred ---
        /// workers may only obtain ownership for unowned buckets.
        /// </summary>
        /// <param name="bucket"></param>
        /// <param name="newOwner"></param>
        /// <returns>true if ownership now belongs to the new owner</returns>
        ValueTask<Worker> ObtainOwnershipAsync(long bucket, Worker newOwner, Worker expectedOwner);

        ValueTask RemoveAsync(long bucket);
    }
}