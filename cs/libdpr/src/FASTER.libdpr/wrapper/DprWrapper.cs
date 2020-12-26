using System.Threading;

namespace FASTER.libdpr
{
    public class DprWrapper<TStateObject, TToken>
        where TStateObject : ISimpleStateObject<TToken>
    {
        private TStateObject underlying;
        private DprManager<SimpleStateObjectAdapter<TStateObject, TToken>, TToken> dprManager;
        private ReaderWriterLockSlim opLatch;

        public DprWrapper(TStateObject underlying, IDprFinder dprFinder, Worker me, long checkpointMilli)
        {
            this.underlying = underlying;
            opLatch = new ReaderWriterLockSlim();
            dprManager = new DprManager<SimpleStateObjectAdapter<TStateObject, TToken>, TToken>(
                dprFinder, me, new SimpleStateObjectAdapter<TStateObject, TToken>(underlying, opLatch),
                checkpointMilli);
        }
        
        // TODO(Tianyu): Add in appendable batch abstraction
    }
}