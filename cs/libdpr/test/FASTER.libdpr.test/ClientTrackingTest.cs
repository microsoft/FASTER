using System;
using System.Collections.Generic;
using NUnit.Framework;

namespace FASTER.libdpr
{
    [TestFixture]
    public class ClientTrackingTest
    {
        private SimpleTestDprFinderBackend fakeFinder;
        private DprClient client;

        private Dictionary<Worker, TestStateStore> GetTestCluster(int size)
        {
            fakeFinder = new SimpleTestDprFinderBackend(size);
            client = new DprClient(new SimpleTestDprFinder(new Worker(1) , fakeFinder));
            var result = new Dictionary<Worker, TestStateStore>();
            for (var i = 0; i < size; i++)
            {
                var worker = new Worker(i);
                result.Add(worker, new TestStateStore(worker, new SimpleTestDprFinder(worker, fakeFinder)));
            }

            return result;
        }

        [Test]
        public void TestSingleClientSingleServer()
        {
            var versionTracker = new ClientVersionTracker();
            var cluster = GetTestCluster(1);
            var tested = new TestClientObject(client.GetSession(Guid.NewGuid()), cluster, 0);
            for (var i = 0; i < 10; i++)
            {
                var id = tested.IssueNewOp(0);
                versionTracker.Add(id);
                var version = tested.ResolveOp(id);
                versionTracker.Resolve(id, new WorkerVersion(new Worker(0), version));
            }

            tested.session.TryGetCurrentCut(out var cut);
            versionTracker.HandleCommit(cut);
            var commitPoint = versionTracker.GetCommitPoint();
            Assert.AreEqual(0, commitPoint.UntilSerialNo);
            
            cluster[new Worker(0)].dprServer.TryRefreshAndCheckpoint(10, 10);
            client.RefreshDprView();
            
            tested.session.TryGetCurrentCut(out cut);
            versionTracker.HandleCommit(cut);
            commitPoint = versionTracker.GetCommitPoint();
            Assert.AreEqual(10, commitPoint.UntilSerialNo);
            Assert.AreEqual(0, commitPoint.ExcludedSerialNos.Count);

            var ops = cluster[new Worker(0)].stateObject.GetOpsPersisted();
            for (var i = 0; i < 10; i++)
                Assert.IsTrue(ops.Remove(ValueTuple.Create(0, i)));
            Assert.IsEmpty(ops);
        }
        
        [Test]
        public void TestSingleClientMultiServer()
        {
            var versionTracker = new ClientVersionTracker();
            var cluster = GetTestCluster(3);
            var tested = new TestClientObject(client.GetSession(Guid.NewGuid()), cluster, 0);
            
            var id = tested.IssueNewOp(0);
            var version = tested.ResolveOp(id);
            versionTracker.Resolve(id, new WorkerVersion(new Worker(0), version));

            
            id = tested.IssueNewOp(1);
            version = tested.ResolveOp(id);
            versionTracker.Resolve(id, new WorkerVersion(new Worker(1), version));

            
            id = tested.IssueNewOp(2);
            version = tested.ResolveOp(id);
            versionTracker.Resolve(id, new WorkerVersion(new Worker(2), version));
            
            tested.session.TryGetCurrentCut(out var cut);
            versionTracker.HandleCommit(cut);
            var commitPoint = versionTracker.GetCommitPoint();
            Assert.AreEqual(0, commitPoint.UntilSerialNo);
            
            cluster[new Worker(0)].dprServer.TryRefreshAndCheckpoint(10, 10);
            cluster[new Worker(2)].dprServer.TryRefreshAndCheckpoint(10, 10);
            client.RefreshDprView();
            Assert.LessOrEqual(commitPoint.UntilSerialNo, 1);
            Assert.AreEqual(0, commitPoint.ExcludedSerialNos.Count);
            
            cluster[new Worker(1)].dprServer.TryRefreshAndCheckpoint(10, 10);
            client.RefreshDprView();
            tested.session.TryGetCurrentCut(out cut);
            versionTracker.HandleCommit(cut);
            commitPoint = versionTracker.GetCommitPoint();
            Assert.AreEqual(3, commitPoint.UntilSerialNo);
            Assert.AreEqual(0, commitPoint.ExcludedSerialNos.Count);

            var opsPersisted = new HashSet<(int, int)>();
            opsPersisted.UnionWith(cluster[new Worker(0)].stateObject.GetOpsPersisted());
            opsPersisted.UnionWith(cluster[new Worker(1)].stateObject.GetOpsPersisted());
            opsPersisted.UnionWith(cluster[new Worker(2)].stateObject.GetOpsPersisted());
            
            Assert.IsTrue(opsPersisted.Remove(ValueTuple.Create(0, 0)));
            Assert.IsTrue(opsPersisted.Remove(ValueTuple.Create(0, 1)));
            Assert.IsTrue(opsPersisted.Remove(ValueTuple.Create(0, 2)));
            Assert.IsEmpty(opsPersisted);
        }
        
        [Test]
        public void TestRelaxedDpr()
        {
            var versionTracker = new ClientVersionTracker();

            var cluster = GetTestCluster(3);
            var tested = new TestClientObject(client.GetSession(Guid.NewGuid()), cluster, 0);
            
            var id = tested.IssueNewOp(0);
            var version = tested.ResolveOp(id);
            versionTracker.Resolve(id, new WorkerVersion(new Worker(0), version));
            
            cluster[new Worker(0)].dprServer.TryRefreshAndCheckpoint(10, 10);

            var id1 = tested.IssueNewOp(0);
            var id2 = tested.IssueNewOp(1); 
            var id3 = tested.IssueNewOp(2);

            version = tested.ResolveOp(id1);
            versionTracker.Resolve(id1, new WorkerVersion(new Worker(0), version));
            
            version = tested.ResolveOp(id2);
            versionTracker.Resolve(id2, new WorkerVersion(new Worker(1), version));
            
            version = tested.ResolveOp(id3);
            versionTracker.Resolve(id3, new WorkerVersion(new Worker(2), version));

            cluster[new Worker(1)].dprServer.TryRefreshAndCheckpoint(10, 10);
            cluster[new Worker(2)].dprServer.TryRefreshAndCheckpoint(10, 10);
            
            client.RefreshDprView();
            
            tested.session.TryGetCurrentCut(out var cut);
            versionTracker.HandleCommit(cut);
            var commitPoint = versionTracker.GetCommitPoint();
            Assert.AreEqual(4, commitPoint.UntilSerialNo);
            Assert.AreEqual(1, commitPoint.ExcludedSerialNos.Count);
            Assert.Contains(id1, commitPoint.ExcludedSerialNos);

            var opsPersisted = new HashSet<(int, int)>();
            opsPersisted.UnionWith(cluster[new Worker(0)].stateObject.GetOpsPersisted());
            opsPersisted.UnionWith(cluster[new Worker(1)].stateObject.GetOpsPersisted());
            opsPersisted.UnionWith(cluster[new Worker(2)].stateObject.GetOpsPersisted());
            
            Assert.IsTrue(opsPersisted.Remove(ValueTuple.Create(0, 0)));
            Assert.IsTrue(opsPersisted.Remove(ValueTuple.Create(0, 2)));
            Assert.IsTrue(opsPersisted.Remove(ValueTuple.Create(0, 3)));
            Assert.IsEmpty(opsPersisted);
        }
    }
}