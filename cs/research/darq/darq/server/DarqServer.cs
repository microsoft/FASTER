using System;
using System.Net.Sockets;
using System.Threading;
using FASTER.client;
using FASTER.common;
using FASTER.libdpr;

namespace FASTER.server
{
    public class DarqServer : IDisposable
    {
        private DarqServerOptions options;
        private readonly IFasterServer server;
        private readonly DprServer<Darq> darq;
        private readonly DarqProvider provider;
        private readonly DarqBackgroundWorker backgroundWorker;
        private readonly ManualResetEventSlim terminationStart, terminationComplete;
        private Thread backgroundThread, refreshThread;

        public DarqServer(DarqServerOptions options)
        {
            this.options = options;
            darq = new DprServer<Darq>(options.Speculative ? options.ClusterInfo.GetNewDprFinder() : null, options.WorkerId,
                new Darq(options.LogSettings, options.Speculative, options.ClearOnShutdown));
            if (options.RunBackgroundThread)
                backgroundWorker = new DarqBackgroundWorker(darq, options.ClusterInfo);
            terminationStart = new ManualResetEventSlim();
            terminationComplete = new ManualResetEventSlim();
            darq.ConnectToCluster();
            provider = new DarqProvider(darq);
            server = new FasterServerTcp(options.Address, options.Port);
            server.Register(WireFormat.DARQRead, provider);
            server.Register(WireFormat.DARQWrite, provider);
        }

        public DprServer<Darq> GetDarq() => darq;

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
                try
                {
                    while (!terminationStart.IsSet)
                    {
                        darq.TryRefreshAndCheckpoint(options.commitIntervalMilli, options.refreshIntervalMilli);
                        // Thread.Sleep(Math.Min(options.commitIntervalMilli, options.refreshIntervalMilli));
                    }
                }
                catch (SocketException e)
                {
                    // TODO(Tianyu): For benchmark only we are ignoring this exception, as therea re some timing issues
                    // arising from shutdown ordering. This should in general NOT be ignored.
                }
                terminationComplete.Set();
            });
            refreshThread.Start();
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