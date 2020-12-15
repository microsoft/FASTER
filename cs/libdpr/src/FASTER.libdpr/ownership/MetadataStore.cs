using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using FASTER.core;

namespace FASTER.libdpr
{
    public class MetadataStore
    {
        private readonly IOwnershipMapping ownershipMapping;
        private ConcurrentDictionary<long, bool> stableLocalKeys, liveLocalKeys;
        private long liveVersionNum;
        // TODO(Tianyu): Add ownership change back in
        // private List<(string, Worker)> bucketsToRemove, backlog;

        // TODO(Tianyu): Maybe limit size of this cache.
        // TODO(Tianyu): Add prefetch?
        private readonly ConcurrentDictionary<long, Worker> cachedRemoteKeys;
        
        public MetadataStore(IOwnershipMapping ownershipMapping)
        {
            this.ownershipMapping = ownershipMapping;
            stableLocalKeys = new ConcurrentDictionary<long, bool>();
            liveLocalKeys = new ConcurrentDictionary<long, bool>();
            liveVersionNum = long.MaxValue;
            cachedRemoteKeys = new ConcurrentDictionary<long, Worker>();
            
            // TODO(Tianyu): Add ownership change back in
            // bucketsToRemove = new List<(string, Worker)>();
            // backlog = new List<(string, Worker)>();
        }

        public bool ValidateLocalOwnership(long bucket)
        {
            return false;
        }
        
        // TODO(Tianyu): Lookups, ownership changes, etc. 
    }
}