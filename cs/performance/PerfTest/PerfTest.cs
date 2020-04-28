// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace FASTER.PerfTest
{
    partial class PerfTest
    {
        static internal bool verbose = false;
        static internal bool prompt = false;
        static readonly TestResult defaultTestResult = new TestResult();
        static TestParameters testParams;
        static string testFilename;
        static string resultsFilename;
        static string compareFirstFilename, compareSecondFilename;
        static ResultComparisonMode comparisonMode = ResultComparisonMode.None;
        static readonly List<string> mergeResultsFilespecs = new List<string>();
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
            if (mergeResultsFilespecs.Count > 0)
            {
                TestResults.Merge(mergeResultsFilespecs.ToArray(), intersectResults, resultsFilename);
                return;
            }
            ExecuteTestRuns();
        }

        static void ExecuteTestRuns()
        {
            var results = new TestResults();
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
                var testInstance = new TestInstance<VarLenValue>(testRun, new VarLenKeyManager(verbose), new VarLenValue.EqualityComparer());
                if (testRun.TestResult.Inputs.UseVarLenValue)
                {
                    return testInstance.Run<VarLenValue, VarLenOutput, VarLenFunctions<VarLenValue>>(null,
                                            new VariableLengthStructSettings<VarLenValue, VarLenValue>
                                            {
                                                keyLength = new VarLenValueLength(),
                                                valueLength = new VarLenValueLength()
                                            },
                                            new VarLenThreadValueRef(testRun.TestResult.Inputs.ThreadCount));
                }
                if (testRun.TestResult.Inputs.UseObjectValue)
                {
                    return testInstance.Run<ObjectValue, ObjectValueOutput, ObjectValueFunctions<VarLenValue>>(new SerializerSettings<VarLenValue, ObjectValue>
                                            {
                                                valueSerializer = () => new ObjectValueSerializer(isKey: false)
                                            },
                                            new VariableLengthStructSettings<VarLenValue, ObjectValue>
                                            {
                                                keyLength = new VarLenValueLength()
                                            },
                                            new ObjectThreadValueRef(testRun.TestResult.Inputs.ThreadCount));
                }

                // Value is Blittable
                bool run_VarLen_Key_BV_Value<TBV>() where TBV : IBlittableValue, new()
                    => testInstance.Run<TBV, BlittableOutput<TBV>, BlittableFunctions<VarLenValue, TBV>>
                        (null, new VariableLengthStructSettings<VarLenValue, TBV> { keyLength = new VarLenValueLength() },
                        new BlittableThreadValueRef<TBV>(testRun.TestResult.Inputs.ThreadCount));

                return Globals.ValueSize switch
                {
                    8 => run_VarLen_Key_BV_Value<BlittableValue8>(),
                    16 => run_VarLen_Key_BV_Value<BlittableValue16>(),
                    32 => run_VarLen_Key_BV_Value<BlittableValue32>(),
                    64 => run_VarLen_Key_BV_Value<BlittableValue64>(),
                    128 => run_VarLen_Key_BV_Value<BlittableValue128>(),
                    256 => run_VarLen_Key_BV_Value<BlittableValue256>(),
                    _ => throw new InvalidOperationException($"Unexpected Blittable data size: {Globals.ValueSize}")
                };
            }
            
            if (testRun.TestResult.Inputs.UseObjectKey)
            {
                var testInstance = new TestInstance<ObjectValue>(testRun, new ObjectKeyManager(verbose), new ObjectValue.EqualityComparer());
                if (testRun.TestResult.Inputs.UseVarLenValue)
                {
                    return testInstance.Run<VarLenValue, VarLenOutput, VarLenFunctions<ObjectValue>>(new SerializerSettings<ObjectValue, VarLenValue>
                                            {
                                                keySerializer = () => new ObjectValueSerializer(isKey: true)
                                            },
                                            new VariableLengthStructSettings<ObjectValue, VarLenValue>
                                            {
                                                valueLength = new VarLenValueLength()
                                            },
                                            new VarLenThreadValueRef(testRun.TestResult.Inputs.ThreadCount));
                }
                if (testRun.TestResult.Inputs.UseObjectValue)
                {
                    return testInstance.Run<ObjectValue, ObjectValueOutput, ObjectValueFunctions<ObjectValue>>(new SerializerSettings<ObjectValue, ObjectValue>
                                            {
                                                keySerializer = () => new ObjectValueSerializer(isKey: true),
                                                valueSerializer = () => new ObjectValueSerializer(isKey: false)
                                            },
                                            null,
                                            new ObjectThreadValueRef(testRun.TestResult.Inputs.ThreadCount));
                }

                // Value is Blittable
                bool run_Object_Key_BV_Value<TBV>() where TBV : IBlittableValue, new()
                    => testInstance.Run<TBV, BlittableOutput<TBV>, BlittableFunctions<ObjectValue, TBV>>(
                        new SerializerSettings<ObjectValue, TBV> { keySerializer = () => new ObjectValueSerializer(isKey: true) }, null,
                        new BlittableThreadValueRef<TBV>(testRun.TestResult.Inputs.ThreadCount));

                return Globals.ValueSize switch
                {
                    8 => run_Object_Key_BV_Value<BlittableValue8>(),
                    16 => run_Object_Key_BV_Value<BlittableValue16>(),
                    32 => run_Object_Key_BV_Value<BlittableValue32>(),
                    64 => run_Object_Key_BV_Value<BlittableValue64>(),
                    128 => run_Object_Key_BV_Value<BlittableValue128>(),
                    256 => run_Object_Key_BV_Value<BlittableValue256>(),
                    _ => throw new InvalidOperationException($"Unexpected Blittable data size: {Globals.ValueSize}")
                };
            }

            // Key is Blittable

            if (testRun.TestResult.Inputs.UseVarLenValue)
            {
                bool run_BV_Key_VarLen_Value<TBV>() where TBV : struct, IBlittableValue 
                    => new TestInstance<TBV>(testRun, new BlittableKeyManager<TBV>(verbose), new BlittableEqualityComparer<TBV>())
                            .Run<VarLenValue, VarLenOutput, VarLenFunctions<TBV>>(
                                null, new VariableLengthStructSettings<TBV, VarLenValue> { valueLength = new VarLenValueLength() },
                                new VarLenThreadValueRef(testRun.TestResult.Inputs.ThreadCount));

                return Globals.KeySize switch
                {
                    8 => run_BV_Key_VarLen_Value<BlittableValue8>(),
                    16 => run_BV_Key_VarLen_Value<BlittableValue16>(),
                    32 => run_BV_Key_VarLen_Value<BlittableValue32>(),
                    64 => run_BV_Key_VarLen_Value<BlittableValue64>(),
                    128 => run_BV_Key_VarLen_Value<BlittableValue128>(),
                    256 => run_BV_Key_VarLen_Value<BlittableValue256>(),
                    _ => throw new InvalidOperationException($"Unexpected Blittable data size: {Globals.KeySize}")
                };
            }

            if (testRun.TestResult.Inputs.UseObjectValue)
            {
                bool run_BV_Key_Object_Value<TBV>() where TBV : struct, IBlittableValue
                    => new TestInstance<TBV>(testRun, new BlittableKeyManager<TBV>(verbose), new BlittableEqualityComparer<TBV>())
                            .Run<VarLenValue, VarLenOutput, VarLenFunctions<TBV>>(
                                null, new VariableLengthStructSettings<TBV, VarLenValue> { valueLength = new VarLenValueLength() },
                                new VarLenThreadValueRef(testRun.TestResult.Inputs.ThreadCount));

                return Globals.KeySize switch
                {
                    8 => run_BV_Key_Object_Value<BlittableValue8>(),
                    16 => run_BV_Key_Object_Value<BlittableValue16>(),
                    32 => run_BV_Key_Object_Value<BlittableValue32>(),
                    64 => run_BV_Key_Object_Value<BlittableValue64>(),
                    128 => run_BV_Key_Object_Value<BlittableValue128>(),
                    256 => run_BV_Key_Object_Value<BlittableValue256>(),
                    _ => throw new InvalidOperationException($"Unexpected Blittable data size: {Globals.KeySize}")
                };
            }

            // Key and value are Blittable

            bool run_BV_Key_BV_Value<TBVKey, TBVValue>() where TBVKey : struct, IBlittableValue where TBVValue : IBlittableValue, new()
                => new TestInstance<TBVKey>(testRun, new BlittableKeyManager<TBVKey>(verbose), new BlittableEqualityComparer<TBVKey>())
                            .Run<TBVValue, BlittableOutput<TBVValue>, BlittableFunctions<TBVKey, TBVValue>>(
                                null, null, new BlittableThreadValueRef<TBVValue>(testRun.TestResult.Inputs.ThreadCount));

            return Globals.KeySize switch
            {
                8 => Globals.ValueSize switch
                    { 
                        8 => run_BV_Key_BV_Value<BlittableValue8, BlittableValue8>(),
                        16 => run_BV_Key_BV_Value<BlittableValue8, BlittableValue16>(),
                        32 => run_BV_Key_BV_Value<BlittableValue8, BlittableValue32>(),
                        64 => run_BV_Key_BV_Value<BlittableValue8, BlittableValue64>(),
                        128 => run_BV_Key_BV_Value<BlittableValue8, BlittableValue128>(),
                        256 => run_BV_Key_BV_Value<BlittableValue8, BlittableValue256>(),
                        _ => throw new InvalidOperationException($"Unexpected Blittable data size: {Globals.ValueSize}")
                    },
                16 => Globals.ValueSize switch
                    {
                        8 => run_BV_Key_BV_Value<BlittableValue16, BlittableValue8>(),
                        16 => run_BV_Key_BV_Value<BlittableValue16, BlittableValue16>(),
                        32 => run_BV_Key_BV_Value<BlittableValue16, BlittableValue32>(),
                        64 => run_BV_Key_BV_Value<BlittableValue16, BlittableValue64>(),
                        128 => run_BV_Key_BV_Value<BlittableValue16, BlittableValue128>(),
                        256 => run_BV_Key_BV_Value<BlittableValue16, BlittableValue256>(),
                        _ => throw new InvalidOperationException($"Unexpected Blittable data size: {Globals.ValueSize}")
                    },
                32 => Globals.ValueSize switch
                    {
                        8 => run_BV_Key_BV_Value<BlittableValue32, BlittableValue8>(),
                        16 => run_BV_Key_BV_Value<BlittableValue32, BlittableValue16>(),
                        32 => run_BV_Key_BV_Value<BlittableValue32, BlittableValue32>(),
                        64 => run_BV_Key_BV_Value<BlittableValue32, BlittableValue64>(),
                        128 => run_BV_Key_BV_Value<BlittableValue32, BlittableValue128>(),
                        256 => run_BV_Key_BV_Value<BlittableValue32, BlittableValue256>(),
                        _ => throw new InvalidOperationException($"Unexpected Blittable data size: {Globals.ValueSize}")
                    },
                64 => Globals.ValueSize switch
                    {
                        8 => run_BV_Key_BV_Value<BlittableValue64, BlittableValue8>(),
                        16 => run_BV_Key_BV_Value<BlittableValue64, BlittableValue16>(),
                        32 => run_BV_Key_BV_Value<BlittableValue64, BlittableValue32>(),
                        64 => run_BV_Key_BV_Value<BlittableValue64, BlittableValue64>(),
                        128 => run_BV_Key_BV_Value<BlittableValue64, BlittableValue128>(),
                        256 => run_BV_Key_BV_Value<BlittableValue64, BlittableValue256>(),
                        _ => throw new InvalidOperationException($"Unexpected Blittable data size: {Globals.ValueSize}")
                    },
                128 => Globals.ValueSize switch
                    {
                        8 => run_BV_Key_BV_Value<BlittableValue128, BlittableValue8>(),
                        16 => run_BV_Key_BV_Value<BlittableValue128, BlittableValue16>(),
                        32 => run_BV_Key_BV_Value<BlittableValue128, BlittableValue32>(),
                        64 => run_BV_Key_BV_Value<BlittableValue128, BlittableValue64>(),
                        128 => run_BV_Key_BV_Value<BlittableValue128, BlittableValue128>(),
                        256 => run_BV_Key_BV_Value<BlittableValue128, BlittableValue256>(),
                        _ => throw new InvalidOperationException($"Unexpected Blittable data size: {Globals.ValueSize}")
                    },
                256 => Globals.ValueSize switch
                    {
                        8 => run_BV_Key_BV_Value<BlittableValue256, BlittableValue8>(),
                        16 => run_BV_Key_BV_Value<BlittableValue256, BlittableValue16>(),
                        32 => run_BV_Key_BV_Value<BlittableValue256, BlittableValue32>(),
                        64 => run_BV_Key_BV_Value<BlittableValue256, BlittableValue64>(),
                        128 => run_BV_Key_BV_Value<BlittableValue256, BlittableValue128>(),
                        256 => run_BV_Key_BV_Value<BlittableValue256, BlittableValue256>(),
                        _ => throw new InvalidOperationException($"Unexpected Blittable data size: {Globals.ValueSize}")
                    },
                _ => throw new InvalidOperationException($"Unexpected Blittable data size: {Globals.KeySize}")
            };
        }
    }
}
