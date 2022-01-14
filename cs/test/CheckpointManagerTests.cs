using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Threading.Tasks;
using FASTER.core;
using FASTER.devices;
using FASTER.test.recovery;
using NUnit.Framework;

namespace FASTER.test
{
    public class CheckpointManagerTests
    {
        private Random random = new Random();

        [Test]
        [Category("CheckpointRestore")]
        [Category("Smoke")]
        public async Task CheckpointManagerPurgeCheck([Values] DeviceMode deviceMode)
        {
            ICheckpointManager checkpointManager;
            if (deviceMode == DeviceMode.Local)
            {
                checkpointManager = new DeviceLogCommitCheckpointManager(
                    new LocalStorageNamedDeviceFactory(),
                    new DefaultCheckpointNamingScheme(TestUtils.MethodTestDir +
                                                      "/checkpoints/")); // PurgeAll deletes this directory
            }
            else
            {
                TestUtils.IgnoreIfNotRunningAzureTests();
                checkpointManager = new DeviceLogCommitCheckpointManager(
                    new AzureStorageNamedDeviceFactory(TestUtils.AzureEmulatedStorageString),
                    new DefaultCheckpointNamingScheme(
                        $"{TestUtils.AzureTestContainer}/{TestUtils.AzureTestDirectory}"));
            }

            var path = TestUtils.MethodTestDir + "/";
            using var log = Devices.CreateLogDevice(path + "hlog.log", deleteOnClose: true);
            TestUtils.RecreateDirectory(path);

            using var fht = new FasterKV<long, long>
            (1 << 10,
                logSettings: new LogSettings
                {
                    LogDevice = log, MutableFraction = 1, PageSizeBits = 10, MemorySizeBits = 20,
                    ReadCacheSettings = null
                },
                checkpointSettings: new CheckpointSettings { CheckpointManager = checkpointManager }
            );
            using var s = fht.NewSession(new SimpleFunctions<long, long>());

            var logCheckpoints = new Dictionary<Guid, int>();
            var indexCheckpoints = new Dictionary<Guid, int>();
            var fullCheckpoints = new Dictionary<Guid, int>();

            for (var i = 0; i < 10; i++)
            {
                // Do some dummy update
                s.Upsert(0, random.Next());

                var checkpointType = random.Next(5);
                Guid result = default;
                switch (checkpointType)
                {
                    case 0:
                        fht.TakeHybridLogCheckpoint(out result, CheckpointType.FoldOver);
                        logCheckpoints.Add(result, 0);
                        break;
                    case 1:
                        fht.TakeHybridLogCheckpoint(out result, CheckpointType.Snapshot);
                        logCheckpoints.Add(result, 0);
                        break;
                    case 2:
                        fht.TakeIndexCheckpoint(out result);
                        indexCheckpoints.Add(result, 0);
                        break;
                    case 3:
                        fht.TakeFullCheckpoint(out result, CheckpointType.FoldOver);
                        fullCheckpoints.Add(result, 0);
                        break;
                    case 4:
                        fht.TakeFullCheckpoint(out result, CheckpointType.Snapshot);
                        fullCheckpoints.Add(result, 0);
                        break;
                    default:
                        Assert.True(false);
                        break;
                }

                await fht.CompleteCheckpointAsync();
            }

            Assert.AreEqual(checkpointManager.GetLogCheckpointTokens().ToDictionary(guid => guid, _ => 0),
                logCheckpoints.Union(fullCheckpoints).ToDictionary(e => e.Key, e => e.Value));
            Assert.AreEqual(checkpointManager.GetIndexCheckpointTokens().ToDictionary(guid => guid, _ => 0),
                indexCheckpoints.Union(fullCheckpoints).ToDictionary(e => e.Key, e => e.Value));

            if (logCheckpoints.Count != 0)
            {
                var guid = logCheckpoints.First().Key;
                checkpointManager.Purge(guid);
                logCheckpoints.Remove(guid);
                Assert.AreEqual(checkpointManager.GetLogCheckpointTokens().ToDictionary(guid => guid, _ => 0),
                    logCheckpoints.Union(fullCheckpoints).ToDictionary(e => e.Key, e => e.Value));
                Assert.AreEqual(checkpointManager.GetIndexCheckpointTokens().ToDictionary(guid => guid, _ => 0),
                    indexCheckpoints.Union(fullCheckpoints).ToDictionary(e => e.Key, e => e.Value));
            }
            
            if (indexCheckpoints.Count != 0)
            {
                var guid = indexCheckpoints.First().Key;
                checkpointManager.Purge(guid);
                indexCheckpoints.Remove(guid);
                Assert.AreEqual(checkpointManager.GetLogCheckpointTokens().ToDictionary(guid => guid, _ => 0),
                    logCheckpoints.Union(fullCheckpoints).ToDictionary(e => e.Key, e => e.Value));
                Assert.AreEqual(checkpointManager.GetIndexCheckpointTokens().ToDictionary(guid => guid, _ => 0),
                    indexCheckpoints.Union(fullCheckpoints).ToDictionary(e => e.Key, e => e.Value));
            }
            
            
            if (fullCheckpoints.Count != 0)
            {
                var guid = fullCheckpoints.First().Key;
                checkpointManager.Purge(guid);
                fullCheckpoints.Remove(guid);
                Assert.AreEqual(checkpointManager.GetLogCheckpointTokens().ToDictionary(guid => guid, _ => 0),
                    logCheckpoints.Union(fullCheckpoints).ToDictionary(e => e.Key, e => e.Value));
                Assert.AreEqual(checkpointManager.GetIndexCheckpointTokens().ToDictionary(guid => guid, _ => 0),
                    indexCheckpoints.Union(fullCheckpoints).ToDictionary(e => e.Key, e => e.Value));
            }

            checkpointManager.PurgeAll();
            Assert.IsEmpty(checkpointManager.GetLogCheckpointTokens());
            Assert.IsEmpty(checkpointManager.GetIndexCheckpointTokens());

            checkpointManager.Dispose();
        }
    }
}