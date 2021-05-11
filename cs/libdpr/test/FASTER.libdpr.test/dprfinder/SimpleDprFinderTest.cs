using System.Collections.Generic;
using System.Linq;
using FASTER.core;
using NUnit.Framework;

namespace FASTER.libdpr
{
    [TestFixture]
    public class SimpleDprFinderTest
    {

        private static void CheckPersistedDprCut(SimpleDprFinderBackend backend, Dictionary<Worker, long> expectedCut)
        {
            var (bytes, size) = backend.GetPersistentState();
            var deserializedState =
                size == 0 ? new SimpleDprFinderBackend.State() : new SimpleDprFinderBackend.State(bytes, 0);
            Assert.AreEqual(expectedCut, deserializedState.GetCurrentCut());
        }
        
        [Test]
        public void TestDprFinderBackendSequential()
        {
            var localDevice1 = new LocalMemoryDevice(1 << 20, 1 << 20, 1);
            var localDevice2 = new LocalMemoryDevice(1 << 20, 1 << 20, 1);
            
            var testedBackend = new SimpleDprFinderBackend(new PingPongDevice(localDevice1, localDevice2));

            var A = new Worker(0);
            var B = new Worker(1);
            var C = new Worker(2);
            var A1 = new WorkerVersion(A, 1);
            var B1 = new WorkerVersion(B, 1);
            var A2 = new WorkerVersion(A, 2);
            var B2 = new WorkerVersion(B, 2);
            var C2 = new WorkerVersion(C, 2);
            
            testedBackend.NewCheckpoint(A1, Enumerable.Empty<WorkerVersion>());
            testedBackend.TryFindDprCut();
            // Should be empty before persistence
            CheckPersistedDprCut(testedBackend, new Dictionary<Worker, long>());
            testedBackend.PersistState();
            CheckPersistedDprCut(testedBackend, new Dictionary<Worker, long>
            {
                {A, 1}
            });
            
            testedBackend.NewCheckpoint(B1, new[] {A1});
            testedBackend.TryFindDprCut();
            testedBackend.PersistState();
            CheckPersistedDprCut(testedBackend, new Dictionary<Worker, long>
            {
                {A, 1},
                {B, 1}
            });
            
            testedBackend.NewCheckpoint(A2, new []{ B2 });
            testedBackend.TryFindDprCut();
            testedBackend.PersistState();
            CheckPersistedDprCut(testedBackend, new Dictionary<Worker, long>
            {
                {A, 1},
                {B, 1}
            });
            
            testedBackend.NewCheckpoint(B2, new []{ C2 });
            testedBackend.TryFindDprCut();
            testedBackend.PersistState();
            CheckPersistedDprCut(testedBackend, new Dictionary<Worker, long>
            {
                {A, 1},
                {B, 1}
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
        }

         [Test]
        public void TestDprFinderBackendRestart()
        {
            var localDevice1 = new LocalMemoryDevice(1 << 20, 1 << 20, 1);
            var localDevice2 = new LocalMemoryDevice(1 << 20, 1 << 20, 1);


            var testedBackend = new SimpleDprFinderBackend(new PingPongDevice(localDevice1, localDevice2));

            var A = new Worker(0);
            var B = new Worker(1);
            var C = new Worker(2);
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
            CheckPersistedDprCut(testedBackend, new Dictionary<Worker, long>());
            testedBackend.PersistState();
            CheckPersistedDprCut(testedBackend, new Dictionary<Worker, long>
            {
                {A, 1}
            });
            
            testedBackend.NewCheckpoint(B1, new[] {A1});
            testedBackend.TryFindDprCut();
            testedBackend.PersistState();
            CheckPersistedDprCut(testedBackend, new Dictionary<Worker, long>
            {
                {A, 1},
                {B, 1}
            });
            
            testedBackend.NewCheckpoint(A2, new []{ B2 });
            testedBackend.TryFindDprCut();
            testedBackend.PersistState();
            CheckPersistedDprCut(testedBackend, new Dictionary<Worker, long>
            {
                {A, 1},
                {B, 1}
            });
            
            testedBackend.NewCheckpoint(B2, new []{ C2 });
            testedBackend.TryFindDprCut();
            testedBackend.PersistState();
            CheckPersistedDprCut(testedBackend, new Dictionary<Worker, long>
            {
                {A, 1},
                {B, 1}
            });
            
            // Get a new test backend to simulate restart from disk
            testedBackend = new SimpleDprFinderBackend(new PingPongDevice(localDevice1, localDevice2));
                
            // At first, some information is lost and we can't make progress
            testedBackend.NewCheckpoint(C2, new []{ A2 });
            testedBackend.TryFindDprCut();
            testedBackend.PersistState();
            CheckPersistedDprCut(testedBackend, new Dictionary<Worker, long>
            {
                {A, 1},
                {B, 1}
            });
            
            testedBackend.NewCheckpoint(A3, new []{ C3 });
            testedBackend.TryFindDprCut();
            testedBackend.PersistState();
            CheckPersistedDprCut(testedBackend, new Dictionary<Worker, long>
            {
                {A, 1},
                {B, 1}
            });
            
            // Eventually, become normal
            testedBackend.NewCheckpoint(B3, new []{ C3 });
            testedBackend.TryFindDprCut();
            testedBackend.PersistState();
            CheckPersistedDprCut(testedBackend, new Dictionary<Worker, long>
            {
                {A, 2},
                {B, 2},
                {C, 2}
            });
        }
    }
}