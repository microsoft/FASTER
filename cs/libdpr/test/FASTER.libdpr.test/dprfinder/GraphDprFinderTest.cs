using System.Collections.Generic;
using System.Linq;
using FASTER.core;
using NUnit.Framework;

namespace FASTER.libdpr
{
    [TestFixture]
    public class GraphDprFinderTest
    {
        private static void CheckPersistedDprCut(GraphDprFinderBackend backend, Dictionary<Worker, long> expectedCut)
        {
            var (bytes, size) = backend.GetPersistentState();
            var deserializedState =
                size == 0 ? new GraphDprFinderBackend.State() : new GraphDprFinderBackend.State(bytes, 0);
            Assert.AreEqual(expectedCut, deserializedState.GetCurrentCut());
        }
        
        [Test]
        public void TestDprFinderBackendSequential()
        {
            var localDevice1 = new LocalMemoryDevice(1 << 20, 1 << 20, 1);
            var localDevice2 = new LocalMemoryDevice(1 << 20, 1 << 20, 1);
            
            var testedBackend = new GraphDprFinderBackend(new PingPongDevice(localDevice1, localDevice2));

            var A = new Worker(0);
            var B = new Worker(1);
            var C = new Worker(2);
            testedBackend.AddWorker(A, null);
            testedBackend.AddWorker(B, null);
            testedBackend.AddWorker(C, null);
            testedBackend.PersistState();
            var A1 = new WorkerVersion(A, 1);
            var B1 = new WorkerVersion(B, 1);
            var A2 = new WorkerVersion(A, 2);
            var B2 = new WorkerVersion(B, 2);
            var C2 = new WorkerVersion(C, 2);
            
            testedBackend.NewCheckpoint(A1, Enumerable.Empty<WorkerVersion>());
            testedBackend.TryFindDprCut();
            // Should be empty before persistence
            CheckPersistedDprCut(testedBackend, new Dictionary<Worker, long>{
                {A, 0},
                {B, 0},
                {C, 0}});
            testedBackend.PersistState();
            CheckPersistedDprCut(testedBackend, new Dictionary<Worker, long>
            {
                {A, 1},
                {B, 0},
                {C, 0}
            });
            
            testedBackend.NewCheckpoint(B1, new[] {A1});
            testedBackend.TryFindDprCut();
            testedBackend.PersistState();
            CheckPersistedDprCut(testedBackend, new Dictionary<Worker, long>
            {
                {A, 1},
                {B, 1},
                {C, 0}
            });
            
            testedBackend.NewCheckpoint(A2, new []{ A1, B2 });
            testedBackend.TryFindDprCut();
            testedBackend.PersistState();
            CheckPersistedDprCut(testedBackend, new Dictionary<Worker, long>
            {
                {A, 1},
                {B, 1},
                {C, 0}
            });
            
            testedBackend.NewCheckpoint(B2, new []{ B1, C2 });
            testedBackend.TryFindDprCut();
            testedBackend.PersistState();
            CheckPersistedDprCut(testedBackend, new Dictionary<Worker, long>
            {
                {A, 1},
                {B, 1},
                {C, 0}
            });
            
            testedBackend.NewCheckpoint(C2, new []{ A2 });
            testedBackend.TryFindDprCut();
            testedBackend.PersistState();
            CheckPersistedDprCut(testedBackend, new Dictionary<Worker, long>
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


            var testedBackend = new GraphDprFinderBackend(new PingPongDevice(localDevice1, localDevice2));

            var A = new Worker(0);
            var B = new Worker(1);
            var C = new Worker(2);
            testedBackend.AddWorker(A, null);
            testedBackend.AddWorker(B, null);
            testedBackend.AddWorker(C, null);
            testedBackend.PersistState();
            var A1 = new WorkerVersion(A, 1);
            var B1 = new WorkerVersion(B, 1);
            var A2 = new WorkerVersion(A, 2);
            var B2 = new WorkerVersion(B, 2);
            var C2 = new WorkerVersion(C, 2);
            var A3 = new WorkerVersion(A, 3);
            var B3 = new WorkerVersion(B, 3);
            var C3 = new WorkerVersion(C, 3);
            
            testedBackend.NewCheckpoint(A1, Enumerable.Empty<WorkerVersion>());
            testedBackend.TryFindDprCut();
            // Should be empty before persistence
            CheckPersistedDprCut(testedBackend, new Dictionary<Worker, long>{
                {A, 0},
                {B, 0},
                {C, 0}});
            testedBackend.PersistState();
            CheckPersistedDprCut(testedBackend, new Dictionary<Worker, long>
            {
                {A, 1},
                {B, 0},
                {C, 0}
            });
            
            testedBackend.NewCheckpoint(B1, new[] { A1 });
            testedBackend.TryFindDprCut();
            testedBackend.PersistState();
            CheckPersistedDprCut(testedBackend, new Dictionary<Worker, long>
            {
                {A, 1},
                {B, 1},
                {C, 0}
            });
            
            testedBackend.NewCheckpoint(A2, new []{ A1, B2 });
            testedBackend.TryFindDprCut();
            testedBackend.PersistState();
            CheckPersistedDprCut(testedBackend, new Dictionary<Worker, long>
            {
                {A, 1},
                {B, 1},
                {C, 0}
            });
            
            testedBackend.NewCheckpoint(B2, new []{ B1, C2 });
            testedBackend.TryFindDprCut();
            testedBackend.PersistState();
            CheckPersistedDprCut(testedBackend, new Dictionary<Worker, long>
            {
                {A, 1},
                {B, 1},
                {C, 0}
            });
            
            // Get a new test backend to simulate restart from disk
            testedBackend = new GraphDprFinderBackend(new PingPongDevice(localDevice1, localDevice2));
                
            // At first, some information is lost and we can't make progress
            testedBackend.NewCheckpoint(C2, new []{ A2 });
            testedBackend.TryFindDprCut();
            testedBackend.PersistState();
            CheckPersistedDprCut(testedBackend, new Dictionary<Worker, long>
            {
                {A, 1},
                {B, 1},
                {C, 1}
            });
            
            testedBackend.NewCheckpoint(A3, new []{ A2, C3 });
            testedBackend.TryFindDprCut();
            testedBackend.PersistState();
            CheckPersistedDprCut(testedBackend, new Dictionary<Worker, long>
            {
                {A, 1},
                {B, 1},
                {C, 1}
            });
            
            // Eventually, become normal
            testedBackend.NewCheckpoint(B3, new []{ B2, C3 });
            testedBackend.TryFindDprCut();
            testedBackend.PersistState();
            CheckPersistedDprCut(testedBackend, new Dictionary<Worker, long>
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