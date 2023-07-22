using System.Collections.Concurrent;
using System.Diagnostics;
using FASTER.client;
using FASTER.common;
using FASTER.darq;
using FASTER.libdpr;

namespace FASTER.server
{
    public class DarqProvider : ISessionProvider
    {
        private Darq backend;
        private ConcurrentQueue<ProducerResponseBuffer> responseQueue;

        internal DarqProvider(Darq backend, ConcurrentQueue<ProducerResponseBuffer> responseQueue)
        {
            this.backend = backend;
            GetMaxSizeSettings = new MaxSizeSettings();
            this.responseQueue = responseQueue;
        }

        public IMessageConsumer GetSession(WireFormat wireFormat, INetworkSender networkSender)
        {
            switch ((DarqProtocolType) wireFormat)
            {
                case DarqProtocolType.DarqSubscribe:
                    return new DarqSubscriptionSession(networkSender, backend);
                case DarqProtocolType.DarqProducer:
                    return new DarqProducerSession(networkSender, backend, responseQueue);
                case DarqProtocolType.DarqProcessor:
                    return new DarqProcessorSession(networkSender, backend);
                default:
                    throw new NotSupportedException();
            }
        }

        public MaxSizeSettings GetMaxSizeSettings { get; }
    }

    public class DarqServer : IDisposable
    {
        private DarqServerOptions options;
        private readonly IFasterServer server;
        private readonly Darq darq;
        private readonly DarqProvider provider;
        private readonly DarqBackgroundWorker backgroundWorker;
        private readonly ManualResetEventSlim terminationStart;
        private readonly CountdownEvent terminationComplete;
        private Thread backgroundThread, refreshThread, responseThread;
        private ConcurrentQueue<ProducerResponseBuffer> responseQueue;

        public DarqServer(DarqServerOptions options)
        {
            this.options = options;
            darq = new Darq(options.me, options.DarqSettings);
            backgroundWorker = new DarqBackgroundWorker(darq, options.ClusterInfo);
            terminationStart = new ManualResetEventSlim();
            terminationComplete = new CountdownEvent(2);
            darq.ConnectToCluster();
            responseQueue = darq.Speculative ? new() : null;
            provider = new DarqProvider(darq, responseQueue);
            server = new FasterServerTcp(options.Address, options.Port);
            // Check that our custom defined wire format is not clashing with anything implemented by FASTER
            Debug.Assert(!Enum.IsDefined(typeof(WireFormat), (WireFormat) (int) DarqProtocolType.DarqSubscribe));
            Debug.Assert(!Enum.IsDefined(typeof(WireFormat), (WireFormat) (int)DarqProtocolType.DarqProcessor));
            Debug.Assert(!Enum.IsDefined(typeof(WireFormat), (WireFormat) (int)DarqProtocolType.DarqProducer));

            server.Register((WireFormat) DarqProtocolType.DarqSubscribe, provider);
            server.Register((WireFormat) DarqProtocolType.DarqProcessor, provider);
            server.Register((WireFormat) DarqProtocolType.DarqProducer, provider);
        }

        public Darq GetDarq() => darq;

        public long BackgroundProcessingLag => backgroundWorker.ProcessingLag;

        public void Start()
        {
            server.Start();
            if (backgroundWorker != default)
            {
                backgroundThread = new Thread(() => backgroundWorker.StartProcessing());
                backgroundThread.Start();
            }

            refreshThread = new Thread(() =>
            {
                while (!terminationStart.IsSet)
                {
                    darq.TryRefreshAndCheckpoint(options.commitIntervalMilli, options.refreshIntervalMilli);
                    // Thread.Sleep(Math.Min(options.commitIntervalMilli, options.refreshIntervalMilli));
                }

                terminationComplete.Signal();
            });
            refreshThread.Start();

            responseThread = new Thread(async () =>
            {
                while (!terminationStart.IsSet && responseQueue != null && !responseQueue.IsEmpty)
                {
                    // TODO(Tianyu): current implementation may have response buffers in the queue with versions
                    // out-of-order, resulting in some responses getting sent later than necessary
                    while (responseQueue.TryPeek(out var response))
                    {
                        if (response.version <= darq.CommittedVersion())
                            // TODO(Tianyu): Figure out how to handle errors
                            response.networkSender.SendResponse(response.buf, 0, response.size, response.Dispose);
                        responseQueue.TryDequeue(out _);
                    }

                    await darq.NextCommit();
                }

                terminationComplete.Signal();
            });
            responseThread.Start();
        }

        public void Dispose()
        {
            terminationStart.Set();
            // TODO(Tianyu): this shutdown process is unsafe and may leave things unsent/unprocessed in the queue
            darq.ForceCheckpoint();
            Thread.Sleep(2000);
            backgroundWorker?.StopProcessingAsync().GetAwaiter().GetResult();
            backgroundWorker?.Dispose();
            server.Dispose();
            terminationComplete.Wait();
            darq.StateObject().Dispose();
            backgroundThread?.Join();
            refreshThread.Join();
        }
    }
}