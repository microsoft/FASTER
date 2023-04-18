using System.Collections.Concurrent;
using FASTER.client;
using FASTER.common;
using FASTER.darq;

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
            switch (wireFormat)
            {
                case WireFormat.DarqSubscribe:
                    return new DarqSubscriptionSession(networkSender, backend);
                case WireFormat.DarqProducer:
                    return new DarqProducerSession(networkSender, backend, responseQueue);
                case WireFormat.DarqProcessor:
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
            server.Register(WireFormat.DarqSubscribe, provider);
            server.Register(WireFormat.DarqProcessor, provider);
            server.Register(WireFormat.DarqProducer, provider);
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
                            response.networkSender.SendResponse(response.buf, 0, response.size, response);
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