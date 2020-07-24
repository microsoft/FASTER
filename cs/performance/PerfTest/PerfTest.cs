// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.PerfTest
{
    partial class PerfTest
    {
        static internal bool prompt = false;
        static readonly TestResult defaultTestResult = new TestResult();
        static TestParameters testParams;
        static string testFilename;
        static string resultsFilename;
        static string compareFirstFilename, compareSecondFilename;
        static ResultComparisonMode comparisonMode = ResultComparisonMode.None;
        static readonly List<string> mergeResultsFileSpecs = new List<string>();
        static bool intersectResults;

        static void Main(string[] argv)
        {
            if (!ParseArgs(argv))
                return;

            if (comparisonMode != ResultComparisonMode.None)
            {
                TestResultComparisons.Compare(compareFirstFilename, compareSecondFilename, comparisonMode, resultsFilename);
                return;
            }
            if (mergeResultsFileSpecs.Count > 0)
            {
                TestResults.Merge(mergeResultsFileSpecs.ToArray(), intersectResults, resultsFilename);
                return;
            }

            ThreadPool.SetMinThreads(2 * Environment.ProcessorCount, 2 * Environment.ProcessorCount);
            TaskScheduler.UnobservedTaskException += (object sender, UnobservedTaskExceptionEventArgs e) =>
            {
                Console.WriteLine($"Unobserved task exception: {e.Exception}");
                e.SetObserved();
            };

            ExecuteTestRuns();
        }

        static void ExecuteTestRuns()
        {
            var results = new TestResults { Results = new TestResult[0] };
            if (!(testParams is null))
                testParams.Override(parseResult);
            var testRuns = (testParams is null ? new[] { new TestRun(parseResult) } : testParams.GetParamSweeps().Select(sweep => new TestRun(sweep))).ToArray();

            // This overall time includes overhead for allocating and distributing the keys, 
            // which has to be done per-test-run.
            var sw = new Stopwatch();
            sw.Start();

            int testNum = 0;
            foreach (var testRun in testRuns)
            {
                Console.WriteLine($"Test {++testNum} of {testRuns.Length}");

                // If running from a testfile, print command line for investigating testfile failures
                if (!(testParams is null))
                    Console.WriteLine(testRun.TestResult.Inputs);

                ExecuteTestRun(testRun);

                testRun.Finish();
                results.Add(testRun.TestResult);
            }

            sw.Stop();

            if (results.Results.Length == 0)
            {
                Console.WriteLine("No tests were run");
                return;
            }

            Console.WriteLine($"Completed {results.Results.Length} test run(s) in {TimeSpan.FromMilliseconds(sw.ElapsedMilliseconds)}");
            if (!string.IsNullOrEmpty(resultsFilename))
            {
                results.Write(resultsFilename);
                Console.WriteLine($"Results written to {resultsFilename}");
            }

            if (prompt)
            {
                Console.WriteLine("Press <ENTER> to end");
                Console.ReadLine();
            }
        }

        private static bool ExecuteTestRun(TestRun testRun)
        {
            Globals.KeySize = testRun.TestResult.Inputs.KeySize;
            Globals.ValueSize = testRun.TestResult.Inputs.ValueSize;

            if (testRun.TestResult.Inputs.UseVarLenKey)
            {
                if (testRun.TestResult.Inputs.UseVarLenValue)
                {
                    var testInstance = new TestInstance<VarLenType, VarLenKeyManager, VarLenType, VarLenOutput, VarLenValueWrapperFactory, VarLenValueWrapper>(
                        testRun, new VarLenKeyManager(), new VarLenType.EqualityComparer(), new VarLenValueWrapperFactory(testRun.TestResult.Inputs.ThreadCount));
                    return testInstance.Run<VarLenFunctions<VarLenType>>(null,
                                            new VariableLengthStructSettings<VarLenType, VarLenType>
                                            {
                                                keyLength = new VarLenTypeLength(),
                                                valueLength = new VarLenTypeLength()
                                            });
                }
                if (testRun.TestResult.Inputs.UseObjectValue)
                {
                    var testInstance = new TestInstance<VarLenType, VarLenKeyManager, ObjectType, ObjectTypeOutput, ObjectValueWrapperFactory, ObjectValueWrapper>(
                        testRun, new VarLenKeyManager(), new VarLenType.EqualityComparer(), new ObjectValueWrapperFactory());
                    return testInstance.Run<ObjectTypeFunctions<VarLenType>>(new SerializerSettings<VarLenType, ObjectType>
                                            {
                                                valueSerializer = () => new ObjectTypeSerializer(isKey: false)
                                            },
                                            new VariableLengthStructSettings<VarLenType, ObjectType>
                                            {
                                                keyLength = new VarLenTypeLength()
                                            });
                }

                // Value is Blittable
                bool run_VarLen_Key_BV_Value<TBV>() where TBV : IBlittableType, new()
                {
                    var testInstance = new TestInstance<VarLenType, VarLenKeyManager, TBV, BlittableOutput<TBV>, BlittableValueWrapperFactory<TBV>, BlittableValueWrapper<TBV>>(
                        testRun, new VarLenKeyManager(), new VarLenType.EqualityComparer(), new BlittableValueWrapperFactory<TBV>());

                    return testInstance.Run<BlittableFunctions<VarLenType, TBV>>(null,
                                            new VariableLengthStructSettings<VarLenType, TBV> { keyLength = new VarLenTypeLength() });
                }

                return Globals.ValueSize switch
                {
                    8 => run_VarLen_Key_BV_Value<BlittableType8>(),
                    16 => run_VarLen_Key_BV_Value<BlittableType16>(),
                    32 => run_VarLen_Key_BV_Value<BlittableType32>(),
                    64 => run_VarLen_Key_BV_Value<BlittableType64>(),
                    128 => run_VarLen_Key_BV_Value<BlittableType128>(),
                    256 => run_VarLen_Key_BV_Value<BlittableType256>(),
                    _ => throw new InvalidOperationException($"Unexpected Blittable data size: {Globals.ValueSize}")
                };
            }
            
            if (testRun.TestResult.Inputs.UseObjectKey)
            {
                if (testRun.TestResult.Inputs.UseVarLenValue)
                {
                    var testInstance = new TestInstance<ObjectType, ObjectKeyManager, VarLenType, VarLenOutput, VarLenValueWrapperFactory, VarLenValueWrapper>(
                        testRun, new ObjectKeyManager(), new ObjectType.EqualityComparer(), new VarLenValueWrapperFactory(testRun.TestResult.Inputs.ThreadCount));
                    return testInstance.Run<VarLenFunctions<ObjectType>>(new SerializerSettings<ObjectType, VarLenType>
                                            {
                                                keySerializer = () => new ObjectTypeSerializer(isKey: true)
                                            },
                                            new VariableLengthStructSettings<ObjectType, VarLenType>
                                            {
                                                valueLength = new VarLenTypeLength()
                                            });
                }
                if (testRun.TestResult.Inputs.UseObjectValue)
                {
                    var testInstance = new TestInstance<ObjectType, ObjectKeyManager, ObjectType, ObjectTypeOutput, ObjectValueWrapperFactory, ObjectValueWrapper>(
                        testRun, new ObjectKeyManager(), new ObjectType.EqualityComparer(), new ObjectValueWrapperFactory());
                    return testInstance.Run<ObjectTypeFunctions<ObjectType>>(new SerializerSettings<ObjectType, ObjectType>
                                            {
                                                keySerializer = () => new ObjectTypeSerializer(isKey: true),
                                                valueSerializer = () => new ObjectTypeSerializer(isKey: false)
                                            },
                                            null);
                }

                // Value is Blittable
                bool run_Object_Key_BV_Value<TBV>() where TBV : IBlittableType, new()
                {
                    var testInstance = new TestInstance<ObjectType, ObjectKeyManager, TBV, BlittableOutput<TBV>, BlittableValueWrapperFactory<TBV>, BlittableValueWrapper<TBV>>(
                        testRun, new ObjectKeyManager(), new ObjectType.EqualityComparer(), new BlittableValueWrapperFactory<TBV>());
                    return testInstance.Run<BlittableFunctions<ObjectType, TBV>>(
                                            new SerializerSettings<ObjectType, TBV> { keySerializer = () => new ObjectTypeSerializer(isKey: true) }, null);
                }

                return Globals.ValueSize switch
                {
                    8 => run_Object_Key_BV_Value<BlittableType8>(),
                    16 => run_Object_Key_BV_Value<BlittableType16>(),
                    32 => run_Object_Key_BV_Value<BlittableType32>(),
                    64 => run_Object_Key_BV_Value<BlittableType64>(),
                    128 => run_Object_Key_BV_Value<BlittableType128>(),
                    256 => run_Object_Key_BV_Value<BlittableType256>(),
                    _ => throw new InvalidOperationException($"Unexpected Blittable data size: {Globals.ValueSize}")
                };
            }

            // Key is Blittable

            if (testRun.TestResult.Inputs.UseVarLenValue)
            {
                bool run_BV_Key_VarLen_Value<TBV>() where TBV : struct, IBlittableType
                {
                    var testInstance = new TestInstance<TBV, BlittableKeyManager<TBV>, VarLenType, VarLenOutput, VarLenValueWrapperFactory, VarLenValueWrapper>(
                        testRun, new BlittableKeyManager<TBV>(), new BlittableEqualityComparer<TBV>(), new VarLenValueWrapperFactory(testRun.TestResult.Inputs.ThreadCount));
                    return testInstance.Run<VarLenFunctions<TBV>>(null,
                                            new VariableLengthStructSettings<TBV, VarLenType> { valueLength = new VarLenTypeLength() });
                }

                return Globals.KeySize switch
                {
                    8 => run_BV_Key_VarLen_Value<BlittableType8>(),
                    16 => run_BV_Key_VarLen_Value<BlittableType16>(),
                    32 => run_BV_Key_VarLen_Value<BlittableType32>(),
                    64 => run_BV_Key_VarLen_Value<BlittableType64>(),
                    128 => run_BV_Key_VarLen_Value<BlittableType128>(),
                    256 => run_BV_Key_VarLen_Value<BlittableType256>(),
                    _ => throw new InvalidOperationException($"Unexpected Blittable data size: {Globals.KeySize}")
                };
            }

            if (testRun.TestResult.Inputs.UseObjectValue)
            {
                bool run_BV_Key_Object_Value<TBV>() where TBV : struct, IBlittableType
                {
                    var testInstance = new TestInstance<TBV, BlittableKeyManager<TBV>, ObjectType, ObjectTypeOutput, ObjectValueWrapperFactory, ObjectValueWrapper>(
                        testRun, new BlittableKeyManager<TBV>(), new BlittableEqualityComparer<TBV>(), new ObjectValueWrapperFactory());
                    return testInstance.Run<ObjectTypeFunctions<TBV>>(new SerializerSettings<TBV, ObjectType>
                                            {
                                                valueSerializer = () => new ObjectTypeSerializer(isKey: false)
                                            },
                                            null);
                }

                return Globals.KeySize switch
                {
                    8 => run_BV_Key_Object_Value<BlittableType8>(),
                    16 => run_BV_Key_Object_Value<BlittableType16>(),
                    32 => run_BV_Key_Object_Value<BlittableType32>(),
                    64 => run_BV_Key_Object_Value<BlittableType64>(),
                    128 => run_BV_Key_Object_Value<BlittableType128>(),
                    256 => run_BV_Key_Object_Value<BlittableType256>(),
                    _ => throw new InvalidOperationException($"Unexpected Blittable data size: {Globals.KeySize}")
                };
            }

            // Key and value are Blittable

            bool run_BV_Key_BV_Value<TBVKey, TBVValue>() where TBVKey : struct, IBlittableType where TBVValue : IBlittableType, new()
            {
                var testInstance = new TestInstance<TBVKey, BlittableKeyManager<TBVKey>, TBVValue, BlittableOutput<TBVValue>, BlittableValueWrapperFactory<TBVValue>, BlittableValueWrapper<TBVValue>>(
                   testRun, new BlittableKeyManager<TBVKey>(), new BlittableEqualityComparer<TBVKey>(), new BlittableValueWrapperFactory<TBVValue>());
                return testInstance.Run<BlittableFunctions<TBVKey, TBVValue>>(null, null);
            }

            return Globals.KeySize switch
            {
                8 => Globals.ValueSize switch
                    { 
                        8 => run_BV_Key_BV_Value<BlittableType8, BlittableType8>(),
                        16 => run_BV_Key_BV_Value<BlittableType8, BlittableType16>(),
                        32 => run_BV_Key_BV_Value<BlittableType8, BlittableType32>(),
                        64 => run_BV_Key_BV_Value<BlittableType8, BlittableType64>(),
                        128 => run_BV_Key_BV_Value<BlittableType8, BlittableType128>(),
                        256 => run_BV_Key_BV_Value<BlittableType8, BlittableType256>(),
                        _ => throw new InvalidOperationException($"Unexpected Blittable data size: {Globals.ValueSize}")
                    },
                16 => Globals.ValueSize switch
                    {
                        8 => run_BV_Key_BV_Value<BlittableType16, BlittableType8>(),
                        16 => run_BV_Key_BV_Value<BlittableType16, BlittableType16>(),
                        32 => run_BV_Key_BV_Value<BlittableType16, BlittableType32>(),
                        64 => run_BV_Key_BV_Value<BlittableType16, BlittableType64>(),
                        128 => run_BV_Key_BV_Value<BlittableType16, BlittableType128>(),
                        256 => run_BV_Key_BV_Value<BlittableType16, BlittableType256>(),
                        _ => throw new InvalidOperationException($"Unexpected Blittable data size: {Globals.ValueSize}")
                    },
                32 => Globals.ValueSize switch
                    {
                        8 => run_BV_Key_BV_Value<BlittableType32, BlittableType8>(),
                        16 => run_BV_Key_BV_Value<BlittableType32, BlittableType16>(),
                        32 => run_BV_Key_BV_Value<BlittableType32, BlittableType32>(),
                        64 => run_BV_Key_BV_Value<BlittableType32, BlittableType64>(),
                        128 => run_BV_Key_BV_Value<BlittableType32, BlittableType128>(),
                        256 => run_BV_Key_BV_Value<BlittableType32, BlittableType256>(),
                        _ => throw new InvalidOperationException($"Unexpected Blittable data size: {Globals.ValueSize}")
                    },
                64 => Globals.ValueSize switch
                    {
                        8 => run_BV_Key_BV_Value<BlittableType64, BlittableType8>(),
                        16 => run_BV_Key_BV_Value<BlittableType64, BlittableType16>(),
                        32 => run_BV_Key_BV_Value<BlittableType64, BlittableType32>(),
                        64 => run_BV_Key_BV_Value<BlittableType64, BlittableType64>(),
                        128 => run_BV_Key_BV_Value<BlittableType64, BlittableType128>(),
                        256 => run_BV_Key_BV_Value<BlittableType64, BlittableType256>(),
                        _ => throw new InvalidOperationException($"Unexpected Blittable data size: {Globals.ValueSize}")
                    },
                128 => Globals.ValueSize switch
                    {
                        8 => run_BV_Key_BV_Value<BlittableType128, BlittableType8>(),
                        16 => run_BV_Key_BV_Value<BlittableType128, BlittableType16>(),
                        32 => run_BV_Key_BV_Value<BlittableType128, BlittableType32>(),
                        64 => run_BV_Key_BV_Value<BlittableType128, BlittableType64>(),
                        128 => run_BV_Key_BV_Value<BlittableType128, BlittableType128>(),
                        256 => run_BV_Key_BV_Value<BlittableType128, BlittableType256>(),
                        _ => throw new InvalidOperationException($"Unexpected Blittable data size: {Globals.ValueSize}")
                    },
                256 => Globals.ValueSize switch
                    {
                        8 => run_BV_Key_BV_Value<BlittableType256, BlittableType8>(),
                        16 => run_BV_Key_BV_Value<BlittableType256, BlittableType16>(),
                        32 => run_BV_Key_BV_Value<BlittableType256, BlittableType32>(),
                        64 => run_BV_Key_BV_Value<BlittableType256, BlittableType64>(),
                        128 => run_BV_Key_BV_Value<BlittableType256, BlittableType128>(),
                        256 => run_BV_Key_BV_Value<BlittableType256, BlittableType256>(),
                        _ => throw new InvalidOperationException($"Unexpected Blittable data size: {Globals.ValueSize}")
                    },
                _ => throw new InvalidOperationException($"Unexpected Blittable data size: {Globals.KeySize}")
            };
        }
    }
}
