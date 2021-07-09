using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using FASTER.core;
using NUnit.Framework;

namespace FASTER.libdpr
{
    [TestFixture]
    public class EnhancedDprFinderTest
    {
        private static void CheckClusterState(EnhancedDprFinderBackend backend, long expectedWorldLine,
            Dictionary<Worker, long> expectedPrefix)
        {
            var response = backend.GetPrecomputedResponse();
            try
            {
                response.rwLatch.EnterReadLock();
                var newState = ClusterState.FromBuffer(response.serializedResponse, 0, out var head);
                Assert.AreEqual(expectedWorldLine, newState.currentWorldLine);
                Assert.AreEqual(expectedPrefix, newState.worldLinePrefix);
            }
            finally
            {
                response.rwLatch.ExitReadLock();
            }

        }
        private static void CheckDprCut(EnhancedDprFinderBackend backend, Dictionary<Worker, long> expectedCut)
        {
            var response = backend.GetPrecomputedResponse();
            try
            {
                response.rwLatch.EnterReadLock();
                if (BitConverter.ToInt32(response.serializedResponse, response.recoveryStateEnd) == -1)
                {
                    Assert.IsNull(expectedCut);
                    return;
                }

                var actualCut = new Dictionary<Worker, long>();
                RespUtil.ReadDictionaryFromBytes(response.serializedResponse, response.recoveryStateEnd, actualCut);
                Assert.AreEqual(expectedCut, actualCut);
            }
            finally
            {
                response.rwLatch.ExitReadLock();
            }
        }
        
        [Test]
        public void TestDprFinderBackendSequential()
        {
            var localDevice1 = new LocalMemoryDevice(1 << 20, 1 << 20, 1);
            var localDevice2 = new LocalMemoryDevice(1 << 20, 1 << 20, 1);
            
            var testedBackend = new EnhancedDprFinderBackend(new PingPongDevice(localDevice1, localDevice2));

            var A = new Worker(0);
            var B = new Worker(1);
            var C = new Worker(2);
            
            var addComplete = new CountdownEvent(3);
            testedBackend.AddWorker(A, _ => addComplete.Signal());
            testedBackend.AddWorker(B, _ => addComplete.Signal());
            testedBackend.AddWorker(C, _ => addComplete.Signal());
            testedBackend.Process();
            addComplete.Wait();
            CheckClusterState(testedBackend, 1, new Dictionary<Worker, long>
            {
                {A, 0},
                {B, 0},
                {C, 0}
            });
            CheckDprCut(testedBackend, new Dictionary<Worker, long>
            {
                {A, 0},
                {B, 0},
                {C, 0}
            });
            
            var A1 = new WorkerVersion(A, 1);
            var B1 = new WorkerVersion(B, 1);
            var A2 = new WorkerVersion(A, 2);
            var B2 = new WorkerVersion(B, 2);
            var C2 = new WorkerVersion(C, 2);
            
            testedBackend.NewCheckpoint(1, A1, Enumerable.Empty<WorkerVersion>());
            testedBackend.Process();
            CheckDprCut(testedBackend, new Dictionary<Worker, long>
            {
                {A, 1},
                {B, 0},
                {C, 0}
            });
            
            testedBackend.NewCheckpoint(1, B1, new[] {A1});
            testedBackend.Process();
            CheckDprCut(testedBackend, new Dictionary<Worker, long>
            {
                {A, 1},
                {B, 1},
                {C, 0}
            });
            
            testedBackend.NewCheckpoint(1, A2, new []{ A1, B2 });
            testedBackend.Process();
            CheckDprCut(testedBackend, new Dictionary<Worker, long>
            {
                {A, 1},
                {B, 1},
                {C, 0}
            });
            
            testedBackend.NewCheckpoint(1, B2, new []{ B1, C2 });
            testedBackend.Process();
            CheckDprCut(testedBackend, new Dictionary<Worker, long>
            {
                {A, 1},
                {B, 1},
                {C, 0}
            });
            
            testedBackend.NewCheckpoint(1, C2, new []{ A2 });
            testedBackend.Process();
            CheckDprCut(testedBackend, new Dictionary<Worker, long>
            {
                {A, 2},
                {B, 2},
                {C, 2}
            });
            
            localDevice1.Dispose();
            localDevice2.Dispose();
        }

        [Test]
        public void TestDprFinderBackendRestart()
        {
            var localDevice1 = new LocalMemoryDevice(1 << 20, 1 << 20, 1);
            var localDevice2 = new LocalMemoryDevice(1 << 20, 1 << 20, 1);
            
            var testedBackend = new EnhancedDprFinderBackend(new PingPongDevice(localDevice1, localDevice2));
        
            var A = new Worker(0);
            var B = new Worker(1);
            var C = new Worker(2);
            
            var addComplete = new CountdownEvent(3);
            testedBackend.AddWorker(A, _ => addComplete.Signal());
            testedBackend.AddWorker(B, _ => addComplete.Signal());
            testedBackend.AddWorker(C, _ => addComplete.Signal());
            testedBackend.Process();
            addComplete.Wait();
            CheckClusterState(testedBackend, 1, new Dictionary<Worker, long>
            {
                {A, 0},
                {B, 0},
                {C, 0}
            });
            CheckDprCut(testedBackend, new Dictionary<Worker, long>
            {
                {A, 0},
                {B, 0},
                {C, 0}
            });
            
            
            var A1 = new WorkerVersion(A, 1);
            var B1 = new WorkerVersion(B, 1);
            var A2 = new WorkerVersion(A, 2);
            var B2 = new WorkerVersion(B, 2);
            var C2 = new WorkerVersion(C, 2);

            testedBackend.NewCheckpoint(1, A1, Enumerable.Empty<WorkerVersion>());
            testedBackend.NewCheckpoint(1, B1, new[] {A1});
            testedBackend.NewCheckpoint(1, A2, new []{ A1, B2 });
            testedBackend.NewCheckpoint(1, B2, new []{ B1, C2 });
            testedBackend.Process();
            CheckDprCut(testedBackend, new Dictionary<Worker, long>
            {
                {A, 1},
                {B, 1},
                {C, 0}
            });
            
            // Get a new test backend to simulate restart from disk
            testedBackend = new EnhancedDprFinderBackend(new PingPongDevice(localDevice1, localDevice2));
            CheckClusterState(testedBackend, 1, new Dictionary<Worker, long>
            {
                {A, 0},
                {B, 0},
                {C, 0}
            });
            // Cut should be temporarily unavailable during recovery
            CheckDprCut(testedBackend, null);
            
            // Simulate resending of graph
            testedBackend.NewCheckpoint(1, A2, new []{ B2 });
            testedBackend.MarkWorkerAccountedFor(A, 2);
            testedBackend.NewCheckpoint(1, B2, new []{ C2 });
            testedBackend.MarkWorkerAccountedFor(B, 2);
            testedBackend.MarkWorkerAccountedFor(C, 0);

            // We should reach the same cut when dpr finder recovery is complete
            CheckDprCut(testedBackend, new Dictionary<Worker, long>
            {
                {A, 1},
                {B, 1},
                {C, 0}
            });
            
            testedBackend.NewCheckpoint(1, C2, new []{ A2 });
            testedBackend.Process();
            CheckDprCut(testedBackend, new Dictionary<Worker, long>
            {
                {A, 2},
                {B, 2},
                {C, 2}
            });
            
            localDevice1.Dispose();
            localDevice2.Dispose();
        }
    }
}