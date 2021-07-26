using System;
using System.Collections.Concurrent;

namespace FASTER.libdpr
{
    /// <summary>
    ///     A DprClient represents a client machine that maintains its own view into the DPR cluster through a DprFinder.
    ///     A single client can support many (single-threaded) sessions.
    /// </summary>
    public class DprClient
    {
        private readonly IDprFinder dprFinder;
        private long dprViewNumber;
        private readonly ConcurrentDictionary<Guid, DprClientSession> sessions;

        /// <summary>
        ///     Constructs a new DprClient with the given dpr finder backend
        /// </summary>
        /// <param name="dprFinder"> backend dpr finder of the cluster</param>
        public DprClient(IDprFinder dprFinder)
        {
            this.dprFinder = dprFinder;
            sessions = new ConcurrentDictionary<Guid, DprClientSession>();
        }

        /// <summary>
        ///     Refreshes the view of the cluster for this client. This method must be invoked periodically so the client
        ///     can observe new commits and make progress for client guarantees.
        /// </summary>
        public void RefreshDprView()
        {
            dprViewNumber++;
            dprFinder.Refresh();
        }

        internal IDprFinder GetDprFinder()
        {
            return dprFinder;
        }

        internal long GetDprViewNumber()
        {
            return dprViewNumber;
        }

        /// <summary>
        ///     Returns the object representing a DprClientSession with the given guid. If the session does not already
        ///     exist, creates the session with the given guid and commit tracking options. If commit tracking is true,
        ///     the DprClientSession guarantees to eventually commit every operation issued, provided that each state
        ///     object in the cluster eventually checkpoints, and that the client object refreshes periodically. Commit
        ///     tracking should be turned off if the system does not commit operations, otherwise the space used for
        ///     operation tracking can be unbounded.
        /// </summary>
        /// <param name="guid"> Guid of the session</param>
        /// <returns> A Client-unique DprClientSession object with the given guid </returns>
        public DprClientSession GetSession(Guid guid)
        {
            return sessions.GetOrAdd(guid, id => new DprClientSession(id, this));
        }
    }
}