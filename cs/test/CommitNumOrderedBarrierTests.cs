using System;
using System.Collections.Generic;
using System.Threading;
using FASTER.core;
using NUnit.Framework;

namespace FASTER.test
{
    [TestFixture]
    internal class CommitNumOrderedBarrierTests
    {
        [Test]
        [Category("FasterLog")]
        [Category("Smoke")]
        public void TestNormalOperation()
        {
            var tested = new CommitNumOrderedBarrier(0);
            var otherThreadProgress = new ManualResetEventSlim();
            var otherThread = new Thread(() =>
            {
                // Should not be allowed through until main thread drops
                tested.WaitForTurn(2, 3);
                // signal when we are through
                otherThreadProgress.Set();
                tested.SignalFinish();
            });
            otherThread.Start();
            Thread.Sleep(10);
            Assert.IsFalse(otherThreadProgress.IsSet);
            
            // Should be allowed through
            tested.WaitForTurn(0, 2);
            
            Thread.Sleep(10);
            Assert.IsFalse(otherThreadProgress.IsSet);
            
            tested.SignalFinish();
            Thread.Sleep(10);
            Assert.IsTrue(otherThreadProgress.IsSet);
            otherThread.Join();
        }
        
        [Test]
        [Category("FasterLog")]
        [Category("Smoke")]
        public void TestRecoveryStart()
        {
            var tested = new CommitNumOrderedBarrier(10);
            var otherThreadProgress = new ManualResetEventSlim();
            var otherThread = new Thread(() =>
            {
                // Should not be allowed through until main thread drops
                tested.WaitForTurn(12, 13);
                // signal when we are through
                otherThreadProgress.Set();
                tested.SignalFinish();
            });
            otherThread.Start();
            Thread.Sleep(10);
            Assert.IsFalse(otherThreadProgress.IsSet);
            
            // Should be allowed through
            tested.WaitForTurn(10, 12);
            
            Thread.Sleep(10);
            Assert.IsFalse(otherThreadProgress.IsSet);
            
            tested.SignalFinish();
            Thread.Sleep(10);
            Assert.IsTrue(otherThreadProgress.IsSet);
            otherThread.Join();
        }

        private void TestThread(ref long sharedVariable, CommitNumOrderedBarrier tested, List<(long, long)> requests)
        {
            foreach (var (prevCommitNum, commitNum) in requests)
            {
                tested.WaitForTurn(prevCommitNum, commitNum);
                // protected region should always be exclusive to this thread
                Assert.AreEqual(Interlocked.CompareExchange(ref sharedVariable, commitNum, prevCommitNum), prevCommitNum);
                tested.SignalFinish();
            }
        }
        
        [Test]
        [Category("FasterLog")]
        [Category("Smoke")]
        public void LargeConcurrentTest()
        {
            const int numThreads = 8;
            const int numTries = 100000;
            var tested = new CommitNumOrderedBarrier(0);
            var random = new Random();
            var requests = new List<(long, long)>();
            
            for (var i = 0; i < numTries; i++)
            {
                var last = requests.Count == 0 ? 0 : requests[^1].Item2;
                requests.Add((last, last + 1));
            }
            
            var threadLocalRequests = new List<List<(long, long)>>();
            for (var i = 0; i < numThreads; i++)
                threadLocalRequests.Add(new List<(long, long)>());
            foreach (var entry in requests)
            {
                var threadId = random.Next(numThreads);
                threadLocalRequests[threadId].Add(entry);
            }

            long sharedVariable = 0;
            var threads = new List<Thread>();
            foreach (var t in threadLocalRequests)
            {
                var thread = new Thread(() => TestThread(ref sharedVariable, tested, t));
                threads.Add(thread);
                thread.Start();
            }

            foreach (var t in threads)
                t.Join();
        }
    }
}