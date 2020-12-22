using System;
using System.Collections.Concurrent;

namespace FASTER.libdpr.client
{
    public class DprClient
    {
        private IDprFinder dprFinder;
        private ConcurrentDictionary<Guid, DprClientSession> sessions;
        
        public void Start()
        {
            
        }

        public void End()
        {
            
        }

        public DprClientSession GetSession(Guid guid)
        {
            return default;
        }
    }
    
}