using System.Collections.Concurrent;
using FASTER.common;
using FASTER.core;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace FASTER.server
{
    /// <summary>
    /// FasterServerBase
    /// </summary>
    public abstract class FasterServerBase : IFasterServer
    {
        readonly ConcurrentDictionary<IServerSession, byte> activeSessions;
        readonly ConcurrentDictionary<WireFormat, ISessionProvider> sessionProviders;
        int activeSessionCount;

        readonly string address;
        readonly int port;
        int networkBufferSize;

        /// <summary>
        /// Server Address
        /// </summary>        
        public string Address => address;

        /// <summary>
        /// Server Port
        /// </summary>
        public int Port => port;

        /// <summary>
        /// Server NetworkBufferSize
        /// </summary>        
        public int NetworkBufferSize => networkBufferSize;

        /// <summary>
        /// Check if server has been disposed
        /// </summary>
        public bool Disposed { get; set; }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="address"></param>
        /// <param name="port"></param>
        /// <param name="networkBufferSize"></param>
        public FasterServerBase(string address, int port, int networkBufferSize)
        {
            this.address = address;
            this.port = port;
            this.networkBufferSize = networkBufferSize;
            if (networkBufferSize == default)
                this.networkBufferSize = BufferSizeUtils.ClientBufferSize(new MaxSizeSettings());

            activeSessions = new ConcurrentDictionary<IServerSession, byte>();
            sessionProviders = new ConcurrentDictionary<WireFormat, ISessionProvider>();
            activeSessionCount = 0;
            Disposed = false;
        }

        /// <inheritdoc />
        public void Register(WireFormat wireFormat, ISessionProvider backendProvider)
        {
            if (!sessionProviders.TryAdd(wireFormat, backendProvider))
                throw new FasterException($"Wire format {wireFormat} already registered");
        }

        /// <inheritdoc />
        public void Unregister(WireFormat wireFormat, out ISessionProvider provider)
            => sessionProviders.TryRemove(wireFormat, out provider);

        /// <inheritdoc />
        public ConcurrentDictionary<WireFormat, ISessionProvider> GetSessionProviders() => sessionProviders;

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool AddSession(WireFormat protocol, ref ISessionProvider provider, INetworkSender networkSender, out IServerSession session)
        {
            if (Interlocked.Increment(ref activeSessionCount) <= 0)
            {
                session = null;
                return false;
            }
            session = provider.GetSession(protocol, networkSender);
            return activeSessions.TryAdd(session, default);
        }

        /// <inheritdoc />
        public abstract void Start();

        /// <inheritdoc />
        public virtual void Dispose()
        {
            Disposed = true;
            DisposeActiveSessions();
        }

        internal void DisposeActiveSessions()
        {
            while (true)
            {
                while (activeSessionCount > 0)
                {
                    foreach (var kvp in activeSessions)
                    {
                        var _session = kvp.Key;
                        if (_session != null)
                        {
                            if (activeSessions.TryRemove(_session, out _))
                            {
                                _session.Dispose();
                                Interlocked.Decrement(ref activeSessionCount);
                            }
                        }
                    }
                    Thread.Yield();
                }
                if (Interlocked.CompareExchange(ref activeSessionCount, int.MinValue, 0) == 0)
                    break;
            }
        }

        internal unsafe void DisposeSession(IServerSession _session)
        {
            if (_session != null)
            {
                if (activeSessions.TryRemove(_session, out _))
                {
                    _session.Dispose();
                    Interlocked.Decrement(ref activeSessionCount);
                }
            }
        }
    }
}
