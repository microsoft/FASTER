using System;
using System.Collections.Generic;
using System.Threading;
using FASTER.core;
using NUnit.Framework;

namespace FASTER.test
{
    [TestFixture]
    internal class SimpleVersionSchemeTest
    {
        [Test]
        [Category("FasterLog")]
        public void SimpleTest()
        {
            var tested = new SimpleVersionScheme();
            var protectedVal = 0;
            var v = tested.Enter();
            
            Assert.AreEqual(1, v);
            tested.TryAdvanceVersion((_, _) => protectedVal = 1);
            Thread.Sleep(10);
            // because of ongoing protection, nothing should happen yet
            tested.Leave();
            // As soon as protection is dropped, action should be done.
            Assert.AreEqual(1, protectedVal);
            
            // Next thread sees new version
            v = tested.Enter();
            Assert.AreEqual(v, 2);
            tested.Leave();
        }

        [Test]
        [Category("FasterLog")]
        public void SingleThreadTest()
        {
            var tested = new SimpleVersionScheme();
            var protectedVal = 0;
            
            var v = tested.Enter();
            Assert.AreEqual(1, v);
            tested.Leave();
            
            tested.TryAdvanceVersion((_, _) => protectedVal = 1);
            Assert.AreEqual(1, protectedVal);
            
            tested.TryAdvanceVersion((_, _) => protectedVal = 2, 4);
            Assert.AreEqual(2, protectedVal);
            
            v = tested.Enter();
            Assert.AreEqual(4, v);
            tested.Leave();
        }
        
        [Test]
        [Category("FasterLog")]
        public void LargeConcurrentTest()
        {
            var tested = new SimpleVersionScheme();
            var protectedVal = 1L;
            var termination = new ManualResetEventSlim();

            var workerThreads = new List<Thread>();
            // Force lots of interleavings by having many threads
            for (var i = 0; i < 2 * Environment.ProcessorCount; i++)
            {
                var t = new Thread(() =>
                {
                    while (!termination.IsSet)
                    {
                        var v = tested.Enter();
                        Assert.AreEqual(v, Interlocked.Read(ref protectedVal));
                        tested.Leave();
                    }
                });
                workerThreads.Add(t);
                t.Start();
            }

            for (var i = 0; i < 100000; i++)
            {
                tested.TryAdvanceVersion((vOld, vNew) =>
                {
                    Assert.AreEqual(vOld, Interlocked.Read(ref protectedVal));
                    // Flip sign to simulate critical section processing
                    protectedVal = -vOld;
                    Thread.Yield();
                    protectedVal = vNew;
                });
            }
            termination.Set();

            foreach (var t in workerThreads)
                t.Join();
        }
    }
}