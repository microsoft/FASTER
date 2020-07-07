// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using Performance.Common;
using System;
using System.Diagnostics;

namespace FASTER.PerfTest
{
    internal abstract class KeyManager<TKey> : IDisposable
    {
        internal abstract void Initialize(ZipfSettings zipfSettings, RandomGenerator rng, int initCount, int opCount);

        internal abstract ref TKey GetInitKey(int index);

        internal abstract ref TKey GetOpKey(int index);

        private TestResult prevTestResult;

        internal void CreateKeys(TestResult testResult)
        {
            // Just to make the test complete a little faster, don't rebuild if we don't have to.
            // This is not part of the timed test.
            if (!(prevTestResult is null)
                    && prevTestResult.Inputs.InitKeyCount == testResult.Inputs.InitKeyCount
                    && prevTestResult.Inputs.OperationKeyCount == testResult.Inputs.OperationKeyCount
                    && prevTestResult.Inputs.DistributionInfo == testResult.Inputs.DistributionInfo)
            {
                Console.WriteLine("Reusing keys from prior run");
                return;
            }

            var sw = new Stopwatch();
            sw.Start();

            prevTestResult = null;

            var rng = new RandomGenerator(testResult.Inputs.DistributionSeed);
            var zipfSettings = testResult.Inputs.Distribution == Distribution.Uniform
                ? null
                : new ZipfSettings
                    {
                        Theta = testResult.Inputs.DistributionParameter,
                        Rng = rng,
                        Shuffle = testResult.Inputs.Distribution == Distribution.ZipfShuffled,
                        Verbose = Globals.Verbose
                    };
            this.Initialize(zipfSettings, rng, testResult.Inputs.InitKeyCount, testResult.Inputs.OperationKeyCount);

            prevTestResult = testResult;

            sw.Stop();
            var workingSetMB = (ulong)Process.GetCurrentProcess().WorkingSet64 / 1048576;
            Console.WriteLine($"Initialization: Time to generate {testResult.Inputs.InitKeyCount:N0} keys" +
                              $" and {testResult.Inputs.OperationKeyCount:N0} operation keys in {testResult.Inputs.Distribution} distribution:" +
                              $" {sw.ElapsedMilliseconds / 1000.0:N3} sec; working set {workingSetMB:N0}MB");
        }

        public abstract void Dispose();
    }
}
