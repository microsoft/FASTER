// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;

namespace FASTER.PerfTest
{
    partial class PerfTest
    {
        const string UpsertsArg = "--upserts";
        const string DataArg = "--data";
        const string ReadMultArg = "--readmult";
        const string ReadThreadsArg = "--readthreads";
        const string ItersArg = "--iters";
        const string UseVarLenArg = "--useVarLen";
        const string UseObjArg = "--useObj";
        const string NoRcArg = "--noRC";
        const string LogArg = "--log";
        const string TestFileArg = "--testfile";
        const string ResultsFileArg = "--resultsfile";
        const string CompareResultsExactArg = "--compareResultsExact";
        const string CompareResultsSequenceArg = "--compareResultsSequence";
        const string VerboseArg = "-v";
        const string PromptArg = "-p";

        private static TestResult parseResult = new TestResult();

        private static bool ParseArgs(string[] argv)
        {
            bool Usage(string message = null)
            {
                Console.WriteLine();
                if (!string.IsNullOrEmpty(message))
                {
                    Console.WriteLine("====== Invalid Argument(s) ======");
                    Console.WriteLine(message);
                    Console.WriteLine();
                }
                Console.WriteLine($"Usage: Specify parameters for one or more test runs, from either a {TestFileArg} or from command-line arguments.");
                Console.WriteLine("       Parameter values default to the fastest configuration (use read cache, flush-only log, no object values).");
                Console.WriteLine();
                Console.WriteLine("  Test parameters from a file:");
                Console.WriteLine($"    {TestFileArg} <fname>: The name of a JSON file containing {nameof(TestParameters)}, such as in the 'testfiles' subdirectory.");
                Console.WriteLine();
                Console.WriteLine($"  Test parameters from individual command-line arguments (overrides those specified in {TestFileArg} (if any)):");
                Console.WriteLine($"    {UpsertsArg} <num>: The number of Upserts for keys in 0..({UpsertsArg})-1; default is {defaultTestResult.UpsertCount}");
                Console.WriteLine($"    {DataArg} <bytes>: How many bytes of per CacheValue; default {CacheGlobals.MinDataSize} (minimum; sizeof(long))");
                Console.WriteLine($"    {ReadMultArg} <mult>: How many multiples of {UpsertsArg} (default: {defaultTestResult.ReadMultiple}) to read after insert and log flush");
                Console.WriteLine($"    {ReadThreadsArg} <threads>: Number of read threads (default: {defaultTestResult.ReadThreadCount}); {ReadMultArg} * {UpsertsArg} are read per thread");
                Console.WriteLine($"    {UseVarLenArg} [value]: Use variable instead of fixed-length blittable Values; default is {defaultTestResult.UseVarLenValue}");
                Console.WriteLine($"    {UseObjArg} [value]: Use objects instead of blittable Value; default is {defaultTestResult.UseObjectValue}");
                Console.WriteLine($"    {NoRcArg} [value]: Do not use ReadCache; default is {!defaultTestResult.UseReadCache}");
                Console.WriteLine($"    {LogArg} <mode>: The disposition of the log after Upserts; default is {defaultTestResult.LogMode}");
                Console.WriteLine($"        {LogMode.Flush.ToString()}: Copy entire log to disk, but retain tail of log in memory");
                Console.WriteLine($"        {LogMode.FlushAndEvict.ToString()}: Move entire log to disk and eliminate data from memory as");
                Console.WriteLine("             well. This will serve workload entirely from disk using read cache if enabled.");
                Console.WriteLine("             This will *allow* future updates to the store.");
                Console.WriteLine($"        {LogMode.DisposeFromMemory.ToString()}: move entire log to disk and eliminate data from memory as");
                Console.WriteLine("             well. This will serve workload entirely from disk using read cache if enabled.");
                Console.WriteLine("             This will *prevent* future updates to the store.");
                Console.WriteLine($"    {ItersArg} <iters>: Number of iterations of the test; default = {defaultTestResult.IterationCount}");
                Console.WriteLine();
                Console.WriteLine($"  To compare result files (both options compare two JSON files containing {nameof(TestResults)}, where the difference is reported");
                Console.WriteLine("             as 'second minus first', so if second does more operations per second, the difference is positive. If either of these");
                Console.WriteLine("             is specified, no other non-Common parameters are allowed):");
                Console.WriteLine($"    {CompareResultsExactArg} fnFirst fnSecond: Compare only {nameof(TestResults)} where the parameter specifications match exactly.");
                Console.WriteLine("             Useful for comparing code change impact.");
                Console.WriteLine($"    {CompareResultsSequenceArg} fnFirst fnSecond: Compare all {nameof(TestResults)} in sequence (to the length of the shorter sequence).");
                Console.WriteLine("             Useful for comparing test-parameter change impact.");
                Console.WriteLine();
                Console.WriteLine("  Common parameters:");
                Console.WriteLine($"    {ResultsFileArg} fname: The name of file to receive JSON text containing either:");
                Console.WriteLine($"        {nameof(TestResultComparisons)}, if {CompareResultsExactArg} or {CompareResultsSequenceArg} was specified");
                Console.WriteLine($"        {nameof(TestResults)} otherwise");
                Console.WriteLine($"    {VerboseArg}: Verbose; print status messages; default = {verbose}");
                Console.WriteLine($"    {PromptArg}: Print a message and wait before exiting; default = {prompt}");
                Console.WriteLine();
                return false;
            }

            for (var ii = 0; ii < argv.Length; ++ii)
            {
                var arg = argv[ii];

                bool hasValue(out string value, bool required = true)
                {
                    if (ii >= argv.Length - 1)
                    {
                        value = null;
                        if (required)
                            Usage($"{arg} requires an argument");
                        return false;
                    }
                    value = argv[++ii];
                    return true;
                }

                //
                // Test-parameter arguments
                //
                if (string.Compare(arg, UpsertsArg, ignoreCase: true) == 0)
                {
                    if (!hasValue(out string value) || !int.TryParse(value, out var count))
                        return Usage($"{arg} requires a valid int value");
                    parseResult.UpsertCount = count;
                    TestParameters.CommandLineOverrides |= TestParameterFlags.Upsert;
                    continue;
                }
                if (string.Compare(arg, DataArg, ignoreCase: true) == 0)
                {
                    if (!hasValue(out string value) || !int.TryParse(value, out var count))
                        return Usage($"{arg} requires a valid int value");
                    parseResult.DataSize = count;
                    TestParameters.CommandLineOverrides |= TestParameterFlags.DataSize;
                    continue;
                }
                if (string.Compare(arg, ReadMultArg, ignoreCase: true) == 0)
                {
                    if (!hasValue(out string value) || !int.TryParse(value, out var count))
                        return Usage($"{arg} requires a valid int value");
                    parseResult.ReadMultiple = count;
                    TestParameters.CommandLineOverrides |= TestParameterFlags.ReadMultiple;
                    continue;
                }
                if (string.Compare(arg, ReadThreadsArg, ignoreCase: true) == 0)
                {
                    if (!hasValue(out string value) || !int.TryParse(value, out var count))
                        return Usage($"{arg} requires a valid int value");
                    parseResult.ReadThreadCount = count;
                    TestParameters.CommandLineOverrides |= TestParameterFlags.ReadThreadCount;
                    continue;
                }
                if (string.Compare(arg, UseVarLenArg, ignoreCase: true) == 0)
                {
                    var wanted = true;
                    if (hasValue(out string value, required:false) && !bool.TryParse(value, out wanted))
                        return Usage($"If {arg} has a value it must be a valid bool");
                    parseResult.UseVarLenValue = wanted;
                    TestParameters.CommandLineOverrides |= TestParameterFlags.UseVarLenValue;
                    continue;
                }
                if (string.Compare(arg, UseObjArg, ignoreCase: true) == 0)
                {
                    var wanted = true;
                    if (hasValue(out string value, required: false) && !bool.TryParse(value, out wanted))
                        return Usage($"If {arg} has a value it must be a valid bool");
                    parseResult.UseObjectValue = wanted;
                    TestParameters.CommandLineOverrides |= TestParameterFlags.UseObjectValue;
                    continue;
                }
                if (string.Compare(arg, NoRcArg, ignoreCase: true) == 0)
                {
                    var wanted = true;
                    if (hasValue(out string value, required: false) && !bool.TryParse(value, out wanted))
                        return Usage($"If {arg} has a value it must be a valid bool");
                    parseResult.UseReadCache = wanted;
                    TestParameters.CommandLineOverrides |= TestParameterFlags.UseReadCache;
                    continue;
                }
                if (string.Compare(arg, LogArg, ignoreCase: true) == 0)
                {
                    if (!hasValue(out string value) || !Enum.TryParse(value, out LogMode mode))
                        return Usage($"{arg} requires a valid LogMode value");
                    parseResult.LogMode = mode;
                    TestParameters.CommandLineOverrides |= TestParameterFlags.LogMode;
                    continue;
                }
                if (string.Compare(arg, ItersArg, ignoreCase: true) == 0)
                {
                    if (!hasValue(out string value) || !int.TryParse(value, out var count))
                        return Usage($"{arg} requires a valid iteration count");
                    parseResult.IterationCount = count;
                    TestParameters.CommandLineOverrides |= TestParameterFlags.IterationCount;
                    continue;
                }

                //
                // File-related arguments
                //
                if (string.Compare(arg, TestFileArg, ignoreCase: true) == 0)
                {
                    if (!hasValue(out testFilename))
                        return Usage($"{arg} requires a filename");
                    continue;
                }
                if (string.Compare(arg, ResultsFileArg, ignoreCase: true) == 0)
                {
                    if (!hasValue(out resultsFilename))
                        return Usage($"{arg} requires a filename");
                    continue;
                }
                if (string.Compare(arg, CompareResultsExactArg, ignoreCase: true) == 0
                    || string.Compare(arg, CompareResultsSequenceArg, ignoreCase: true) == 0)
                {
                    if (!hasValue(out compareFirstFilename) || !hasValue(out compareSecondFilename))
                        return Usage($"{arg} requires a first and second filename");
                    comparisonMode = string.Compare(arg, CompareResultsExactArg, ignoreCase: true) == 0 ? ResultComparisonMode.Exact : ResultComparisonMode.Sequence;
                    continue;
                }
                if (string.Compare(arg, VerboseArg, ignoreCase: true) == 0)
                {
                    verbose = true;
                    continue;
                }
                if (string.Compare(arg, PromptArg, ignoreCase: true) == 0)
                {
                    prompt = true;
                    continue;
                }
                if (string.Compare(arg, "--help", ignoreCase: true) == 0 || arg == "/?" || arg == "-?")
                {
                    return Usage();
                }
                return Usage($"Unknown argument: {arg}");
            }

            if (!parseResult.Verify())
                return false;

            if (!(testFilename is null))
                testParams = TestParameters.Read(testFilename);
            return true;
        }
    }
}
