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
            addComplete.Wait();
            CheckClusterState(testedBackend, 0, new Dictionary<Worker, long>
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
            
            testedBackend.NewCheckpoint(0, A1, Enumerable.Empty<WorkerVersion>());
            testedBackend.TryFindDprCut();
            CheckDprCut(testedBackend, new Dictionary<Worker, long>
            {
                {A, 1},
                {B, 0},
                {C, 0}
            });
            
            testedBackend.NewCheckpoint(0, B1, new[] {A1});
            testedBackend.TryFindDprCut();
            CheckDprCut(testedBackend, new Dictionary<Worker, long>
            {
                {A, 1},
                {B, 1},
                {C, 0}
            });
            
            testedBackend.NewCheckpoint(0, A2, new []{ B2 });
            testedBackend.TryFindDprCut();
            CheckDprCut(testedBackend, new Dictionary<Worker, long>
            {
                {A, 1},
                {B, 1},
                {C, 0}
            });
            
            testedBackend.NewCheckpoint(0, B2, new []{ C2 });
            testedBackend.TryFindDprCut();
            CheckDprCut(testedBackend, new Dictionary<Worker, long>
            {
                {A, 1},
                {B, 1},
                {C, 0}
            });
            
            testedBackend.NewCheckpoint(0, C2, new []{ A2 });
            testedBackend.TryFindDprCut();
            CheckDprCut(testedBackend, new Dictionary<Worker, long>
            {
                {A, 2},
                {B, 2},
                {C, 2}
            });
            
            localDevice1.Dispose();
            localDevice2.Dispose();
        }

        // [Test]
        // public void TestDprFinderBackendRestart()
        // {
        //     var localDevice1 = new LocalMemoryDevice(1 << 20, 1 << 20, 1);
        //     var localDevice2 = new LocalMemoryDevice(1 << 20, 1 << 20, 1);
        //
        //
        //     var testedBackend = new GraphDprFinderBackend(new PingPongDevice(localDevice1, localDevice2));
        //
        //     var A = new Worker(0);
        //     var B = new Worker(1);
        //     var C = new Worker(2);
        //     testedBackend.AddWorker(A, null);
        //     testedBackend.AddWorker(B, null);
        //     testedBackend.AddWorker(C, null);
        //     testedBackend.PersistState();
        //     var A1 = new WorkerVersion(A, 1);
        //     var B1 = new WorkerVersion(B, 1);
        //     var A2 = new WorkerVersion(A, 2);
        //     var B2 = new WorkerVersion(B, 2);
        //     var C2 = new WorkerVersion(C, 2);
        //     var A3 = new WorkerVersion(A, 3);
        //     var B3 = new WorkerVersion(B, 3);
        //     var C3 = new WorkerVersion(C, 3);
        //     
        //     testedBackend.NewCheckpoint(A1, Enumerable.Empty<WorkerVersion>());
        //     testedBackend.TryFindDprCut();
        //     // Should be empty before persistence
        //     CheckDprCut(testedBackend, new Dictionary<Worker, long>{
        //         {A, 0},
        //         {B, 0},
        //         {C, 0}});
        //     testedBackend.PersistState();
        //     CheckDprCut(testedBackend, new Dictionary<Worker, long>
        //     {
        //         {A, 1},
        //         {B, 0},
        //         {C, 0}
        //     });
        //     
        //     testedBackend.NewCheckpoint(B1, new[] {A1});
        //     testedBackend.TryFindDprCut();
        //     testedBackend.PersistState();
        //     CheckDprCut(testedBackend, new Dictionary<Worker, long>
        //     {
        //         {A, 1},
        //         {B, 1},
        //         {C, 0}
        //     });
        //     
        //     testedBackend.NewCheckpoint(A2, new []{ B2 });
        //     testedBackend.TryFindDprCut();
        //     testedBackend.PersistState();
        //     CheckDprCut(testedBackend, new Dictionary<Worker, long>
        //     {
        //         {A, 1},
        //         {B, 1},
        //         {C, 0}
        //     });
        //     
        //     testedBackend.NewCheckpoint(B2, new []{ C2 });
        //     testedBackend.TryFindDprCut();
        //     testedBackend.PersistState();
        //     CheckDprCut(testedBackend, new Dictionary<Worker, long>
        //     {
        //         {A, 1},
        //         {B, 1},
        //         {C, 0}
        //     });
        //     
        //     // Get a new test backend to simulate restart from disk
        //     testedBackend = new GraphDprFinderBackend(new PingPongDevice(localDevice1, localDevice2));
        //         
        //     // At first, some information is lost and we can't make progress
        //     testedBackend.NewCheckpoint(C2, new []{ A2 });
        //     testedBackend.TryFindDprCut();
        //     testedBackend.PersistState();
        //     CheckDprCut(testedBackend, new Dictionary<Worker, long>
        //     {
        //         {A, 1},
        //         {B, 1},
        //         {C, 1}
        //     });
        //     
        //     testedBackend.NewCheckpoint(A3, new []{ C3 });
        //     testedBackend.TryFindDprCut();
        //     testedBackend.PersistState();
        //     CheckDprCut(testedBackend, new Dictionary<Worker, long>
        //     {
        //         {A, 1},
        //         {B, 1},
        //         {C, 1}
        //     });
        //     
        //     // Eventually, become normal
        //     testedBackend.NewCheckpoint(B3, new []{ C3 });
        //     testedBackend.TryFindDprCut();
        //     testedBackend.PersistState();
        //     CheckDprCut(testedBackend, new Dictionary<Worker, long>
        //     {
        //         {A, 2},
        //         {B, 2},
        //         {C, 2}
        //     });
        //     localDevice1.Dispose();
        //     localDevice2.Dispose();
        // }
    }
}