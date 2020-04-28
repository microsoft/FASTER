// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using Performance.Common;
using System;

namespace FASTER.PerfTest
{
    partial class PerfTest
    {
        internal const string HashSizeArg = "--hash";
        internal const string NumaArg = "--numa";
        internal const string DistArg = "--dist";
        internal const string DistParamArg = "--distparam";
        internal const string DistSeedArg = "--distseed";
        internal const string ThreadsArg = "--threads";
        internal const string InitKeysArg = "--initkeys";
        internal const string OpKeysArg = "--opkeys";
        internal const string UpsertsArg = "--upserts";
        internal const string ReadsArg = "--reads";
        internal const string RMWsArg = "--rmws";
        internal const string MixOpsArg = "--mixOps";
        internal const string KeySizeArg = "--keySize";
        internal const string ValueSizeArg = "--valueSize";
        internal const string UseVarLenKeysArg = "--varLenKeys";
        internal const string UseVarLenValuesArg = "--varLenValues";
        internal const string UseObjKeysArg = "--objKeys";
        internal const string UseObjValuesArg = "--objValues";
        internal const string UseRcArg = "--useRC";
        internal const string LogArg = "--log";
        internal const string ItersArg = "--iters";
        const string TestFileArg = "--testfile";
        const string ResultsFileArg = "--resultsfile";
        const string CompareResultsExactArg = "--compareResultsExact";
        const string CompareResultsSequenceArg = "--compareResultsSequence";
        internal const string MergeResultsArg = "--mergeResults";
        internal const string IntersectResultsArg = "--intersectResults";
        internal const string VerifyArg = "--verify";
        const string VerboseArg = "-v";
        const string PromptArg = "-p";

        private static readonly TestResult parseResult = new TestResult();

        private static bool ParseArgs(string[] argv)
        {
            static bool Usage(string message = null)
            {
                Console.WriteLine();
                Console.WriteLine($"Usage: Specify parameters for one or more test runs, from either a {TestFileArg} or from command-line arguments.");
                Console.WriteLine("       Parameter values default to the fastest configuration (use read cache, flush-only log, no object values).");
                Console.WriteLine();
                Console.WriteLine("  Test parameters from a file:");
                Console.WriteLine($"    {TestFileArg} <fname>: The name of a JSON file containing {nameof(TestParameters)}, such as in the 'testfiles' subdirectory.");
                Console.WriteLine();
                Console.WriteLine($"  Test parameters from individual command-line arguments (overrides those specified in {TestFileArg} (if any)):");
                Console.WriteLine($"    {HashSizeArg} <size>: The size of the FasterKV hash table; 1 << <size>. Default is {defaultTestResult.Inputs.HashSizeShift}");
                Console.WriteLine($"    {NumaArg} <mode>: Which Numa mode to use; default is {defaultTestResult.Inputs.NumaMode}");
                Console.WriteLine($"        {NumaMode.None}: Do not affinitize");
                Console.WriteLine($"        {NumaMode.RoundRobin}: Round-robin affinitization by thread ordinal");
                Console.WriteLine($"        {NumaMode.Sharded2}: Sharded affinitization across 2 groups");
                Console.WriteLine($"        {NumaMode.Sharded4}: Sharded affinitization across 4 groups");
                Console.WriteLine($"    Distribution of data among the key values: ");
                Console.WriteLine($"        {DistArg} <mode>: The distribution of the data among the keys; default is {defaultTestResult.Inputs.Distribution}");
                Console.WriteLine($"            {Distribution.Uniform}: Uniformly random distribution of keys");
                Console.WriteLine($"            {Distribution.ZipfSmooth}: Smooth curve (most localized keys)");
                Console.WriteLine($"            {Distribution.ZipfShuffled}: Shuffle keys after curve generation");
                Console.WriteLine($"        {DistParamArg} <double value>: A parameter for the distribution of the keys; default is {defaultTestResult.Inputs.DistributionParameter}");
                Console.WriteLine($"            Currently used for Zipf theta and must be > 0.0 and != 1.0 (note: Benchmark (YCSB) uses 0.99; higher values are more skewed).");
                Console.WriteLine($"        {DistSeedArg} <seed>: The distribution rng seed; 0 means create one based on current timestamp. Default is {defaultTestResult.Inputs.DistributionSeed}");
                Console.WriteLine($"    {ThreadsArg} <threads>: Number of threads for initialization and operations; default is {defaultTestResult.Inputs.ThreadCount}");
                Console.WriteLine($"        On initialization (the initial Upserts (which are Inserts) to populate the store), threads divide the full operation count.");
                Console.WriteLine($"        On subsequent operations (Upsert, Read, and RMW), each thread runs the full operation counts.");
                Console.WriteLine($"    Key and Operation counts. Shortcut: use an 'm' suffix (e.g. 20m) to mean million.");
                Console.WriteLine($"        {InitKeysArg} <num>: The number of Keys loaded into the Faster hash table during initialization; default is {defaultTestResult.Inputs.InitKeyCount}");
                Console.WriteLine($"        {OpKeysArg} <num>: The number of Keys to be selected from during operations; default is {defaultTestResult.Inputs.InitKeyCount}");
                Console.WriteLine($"            The opKey indices are limited to 0..{InitKeysArg}; they are distributed according to {DistArg}");
                Console.WriteLine($"        {UpsertsArg} <count>: The number of Upsert operations to run after the initial upserts are done; default is {defaultTestResult.Inputs.UpsertCount}");
                Console.WriteLine($"        {ReadsArg} <count>: The number of Read operations to run after the initial upserts are done; default is {defaultTestResult.Inputs.ReadCount}");
                Console.WriteLine($"        {RMWsArg} <count>: The number of RMW operations to run after the initial upserts are done; default is {defaultTestResult.Inputs.RMWCount}");
                Console.WriteLine($"    {MixOpsArg} [value]: Mix upsert, read, and rmw operations, rather than one after the other; default is {defaultTestResult.Inputs.MixOperations}");
                Console.WriteLine($"        If true, the sum of {UpsertsArg}, {ReadsArg}, and {RMWsArg} operations are run, intermixed, on each thread specified.");
                Console.WriteLine($"    {KeySizeArg} <bytes>: How many bytes per Key; must be in [{string.Join(", ", Globals.ValidDataSizes)}], Default is {defaultTestResult.Inputs.KeySize}");
                Console.WriteLine($"    {ValueSizeArg} <bytes>: How many bytes per Value; must be in [{string.Join(", ", Globals.ValidDataSizes)}], Default is {defaultTestResult.Inputs.ValueSize}");
                Console.WriteLine($"    {UseVarLenKeysArg} [value]: Use variable instead of fixed-length blittable Keys; default is {defaultTestResult.Inputs.UseVarLenKey}");
                Console.WriteLine($"    {UseVarLenValuesArg} [value]: Use variable instead of fixed-length blittable Values; default is {defaultTestResult.Inputs.UseVarLenValue}");
                Console.WriteLine($"    {UseObjValuesArg} [value]: Use objects instead of blittable Keys; default is {defaultTestResult.Inputs.UseObjectKey}");
                Console.WriteLine($"    {UseObjValuesArg} [value]: Use objects instead of blittable Values; default is {defaultTestResult.Inputs.UseObjectValue}");
                Console.WriteLine($"    {UseRcArg} [value]: Use ReadCache; default is {defaultTestResult.Inputs.UseReadCache}");
                Console.WriteLine($"    {LogArg} <mode>: The disposition of the log after initial Inserts; default is {defaultTestResult.Inputs.LogMode}");
                Console.WriteLine($"        {LogMode.None}: Do not flush log");
                Console.WriteLine($"        {LogMode.Flush}: Copy entire log to disk, but retain tail of log in memory");
                Console.WriteLine($"        {LogMode.FlushAndEvict}: Move entire log to disk and eliminate data from memory as well. This will serve workload");
                Console.WriteLine("              entirely from disk using read cache if enabled. This will *allow* future updates to the store.");
                Console.WriteLine($"        {LogMode.DisposeFromMemory}: move entire log to disk and eliminate data from memory as well. This will serve workload");
                Console.WriteLine("              entirely from disk using read cache if enabled. This will *prevent* future updates to the store.");
                Console.WriteLine($"    {ItersArg} <iters>: Number of iterations of the test; default = {defaultTestResult.Inputs.IterationCount}");
                Console.WriteLine();
                Console.WriteLine($"  To compare result files (both options compare two JSON files containing {nameof(TestResults)}, where the difference is reported");
                Console.WriteLine("             as 'second minus first', so if second does more operations per second, the difference is positive. If either of these");
                Console.WriteLine("             is specified, no other non-Common parameters are allowed):");
                Console.WriteLine($"    {CompareResultsExactArg} <fnFirst> <fnSecond>: Compare only {nameof(TestResults)} where the parameter specifications match exactly.");
                Console.WriteLine("             Useful for comparing code change impact.");
                Console.WriteLine($"    {CompareResultsSequenceArg} <fnFirst> <fnSecond>: Compare all {nameof(TestResults)} in sequence (to the length of the shorter sequence).");
                Console.WriteLine("             Useful for comparing test-parameter change impact.");
                Console.WriteLine();
                Console.WriteLine($"  To merge multiple result files (one or more JSON files containing {nameof(TestResults)}, e.g. files obtained at different");
                Console.WriteLine("             times to mitigate the possibility of background tasks affecting results):");
                Console.WriteLine($"    {MergeResultsArg} <filespec1..filespecN>: Wildcards are supported; at least two files must be returned by the combination of filespecs.");
                Console.WriteLine("             Results for identical parameter configurations are merged; other results are appended from the filespec1 first, then filespec2.");
                Console.WriteLine("             Note that if wildcards are used, files will be returned in filesystem order; use appropriate naming if a particular order is needed.");
                Console.WriteLine($"    {IntersectResultsArg} <filespec1..filespecN>: Same as {MergeResultsArg}, but only matching results are preserved.");
                Console.WriteLine();
                Console.WriteLine("  Common parameters:");
                Console.WriteLine($"    {ResultsFileArg} fname: The name of file to receive JSON text containing either:");
                Console.WriteLine($"        {nameof(TestResultComparisons)}, if {CompareResultsExactArg} or {CompareResultsSequenceArg} was specified");
                Console.WriteLine($"        {nameof(TestResults)} otherwise");
                Console.WriteLine($"    {VerboseArg}: Verbose; print status messages; default = {verbose}");
                Console.WriteLine($"    {PromptArg}: Print a message and wait before exiting; default = {prompt}");
                Console.WriteLine();

                if (!string.IsNullOrEmpty(message))
                {
                    Console.WriteLine("====== Invalid Argument(s) ======");
                    Console.WriteLine(message);
                }
                return false;
            }

            for (var ii = 0; ii < argv.Length; ++ii)
            {
                var arg = argv[ii];

                bool hasValue(out string value, bool required = true)
                {
                    if (ii >= argv.Length - 1 || argv[ii + 1].StartsWith("-"))
                    {
                        value = null;
                        if (required)
                            Usage($"{arg} requires an argument");
                        return false;
                    }
                    value = argv[++ii];
                    return true;
                }

                bool hasIntValue(out int num)
                {
                    num = 0;
                    if (!hasValue(out string value))
                        return false;
                    var mult = value.EndsWith("m", StringComparison.OrdinalIgnoreCase) ? 1_000_000 : 1;
                    if (mult != 1)
                        value = value[0..^1];
                    if (int.TryParse(value, out num))
                    {
                        num *= mult;
                        return true;
                    }
                    return Usage($"{arg} requires a valid int value");
                }

                bool hasDoubleValue(out double num)
                {
                    num = 0;
                    return hasValue(out string value) && double.TryParse(value, out num)
                            ? true
                            : Usage($"{arg} requires a valid floating-point value");
                }

                bool hasBoolValue(out bool wanted)
                {
                    wanted = true;
                    return hasValue(out string value, required: false) && !bool.TryParse(value, out wanted)
                        ? Usage($"If {arg} has a value it must be a valid bool")
                        : true;
                }

                bool hasEnumValue<T>(out T member) where T : struct
                {
                    member = default;
                    return hasValue(out string value) && Enum.TryParse<T>(value, true, out member)
                        ? true
                        : Usage($"{arg} requires a valid LogMode value");
                }

                //
                // Test-parameter arguments
                //
                if (string.Compare(arg, HashSizeArg, ignoreCase: true) == 0)
                {
                    if (!hasIntValue(out var value))
                        return false;
                    parseResult.Inputs.HashSizeShift = value;
                    TestParameters.CommandLineOverrides |= TestParameterFlags.HashSize;
                    continue;
                }
                if (string.Compare(arg, NumaArg, ignoreCase: true) == 0)
                {
                    if (!hasEnumValue(out NumaMode mode))
                        return Usage($"{arg} requires a valid NumaMode value");
                    parseResult.Inputs.NumaMode = mode;
                    TestParameters.CommandLineOverrides |= TestParameterFlags.NumaMode;
                    continue;
                }
                if (string.Compare(arg, DistArg, ignoreCase: true) == 0)
                {
                    if (!hasEnumValue(out Distribution mode))
                        return Usage($"{arg} requires a valid DistributionMode value");
                    parseResult.Inputs.Distribution = mode;
                    TestParameters.CommandLineOverrides |= TestParameterFlags.Distribution;
                    continue;
                }
                if (string.Compare(arg, DistParamArg, ignoreCase: true) == 0)
                {
                    if (!hasDoubleValue(out var value))
                        return false;
                    parseResult.Inputs.DistributionParameter = value;
                    TestParameters.CommandLineOverrides |= TestParameterFlags.DistributionParameter;
                    continue;
                }
                if (string.Compare(arg, DistSeedArg, ignoreCase: true) == 0)
                {
                    if (!hasIntValue(out var value))
                        return false;
                    parseResult.Inputs.DistributionSeed = value;
                    TestParameters.CommandLineOverrides |= TestParameterFlags.DistributionSeed;
                    continue;
                }
                if (string.Compare(arg, ThreadsArg, ignoreCase: true) == 0)
                {
                    if (!hasIntValue(out var value))
                        return false;
                    parseResult.Inputs.ThreadCount = value;
                    TestParameters.CommandLineOverrides |= TestParameterFlags.ThreadCount;
                    continue;
                }
                if (string.Compare(arg, InitKeysArg, ignoreCase: true) == 0)
                {
                    if (!hasIntValue(out var value))
                        return false;
                    parseResult.Inputs.InitKeyCount = value;
                    TestParameters.CommandLineOverrides |= TestParameterFlags.InitKeyCount;
                    continue;
                }
                if (string.Compare(arg, OpKeysArg, ignoreCase: true) == 0)
                {
                    if (!hasIntValue(out var value))
                        return false;
                    parseResult.Inputs.OperationKeyCount = value;
                    TestParameters.CommandLineOverrides |= TestParameterFlags.OpKeyCount;
                    continue;
                }
                if (string.Compare(arg, UpsertsArg, ignoreCase: true) == 0)
                {
                    if (!hasIntValue(out var value))
                        return false;
                    parseResult.Inputs.UpsertCount = value;
                    TestParameters.CommandLineOverrides |= TestParameterFlags.UpsertCount;
                    continue;
                }
                if (string.Compare(arg, ReadsArg, ignoreCase: true) == 0)
                {
                    if (!hasIntValue(out var value))
                        return false;
                    parseResult.Inputs.ReadCount = value;
                    TestParameters.CommandLineOverrides |= TestParameterFlags.ReadCount;
                    continue;
                }
                if (string.Compare(arg, RMWsArg, ignoreCase: true) == 0)
                {
                    if (!hasIntValue(out var value))
                        return false;
                    parseResult.Inputs.RMWCount = value;
                    TestParameters.CommandLineOverrides |= TestParameterFlags.RmwCount;
                    continue;
                }
                if (string.Compare(arg, MixOpsArg, ignoreCase: true) == 0)
                {
                    if (!hasBoolValue(out var wanted))
                        return false;
                    parseResult.Inputs.MixOperations = wanted;
                    TestParameters.CommandLineOverrides |= TestParameterFlags.MixOperations;
                    continue;
                }
                if (string.Compare(arg, KeySizeArg, ignoreCase: true) == 0)
                {
                    if (!hasIntValue(out var value))
                        return false;
                    parseResult.Inputs.KeySize = value;
                    TestParameters.CommandLineOverrides |= TestParameterFlags.KeySize;
                    continue;
                }
                if (string.Compare(arg, ValueSizeArg, ignoreCase: true) == 0)
                {
                    if (!hasIntValue(out var value))
                        return false;
                    parseResult.Inputs.ValueSize = value;
                    TestParameters.CommandLineOverrides |= TestParameterFlags.ValueSize;
                    continue;
                }
                if (string.Compare(arg, UseVarLenKeysArg, ignoreCase: true) == 0)
                {
                    if (!hasBoolValue(out var wanted))
                        return false;
                    parseResult.Inputs.UseVarLenKey = wanted;
                    TestParameters.CommandLineOverrides |= TestParameterFlags.UseVarLenKey;
                    continue;
                }
                if (string.Compare(arg, UseVarLenValuesArg, ignoreCase: true) == 0)
                {
                    if (!hasBoolValue(out var wanted))
                        return false;
                    parseResult.Inputs.UseVarLenValue = wanted;
                    TestParameters.CommandLineOverrides |= TestParameterFlags.UseVarLenValue;
                    continue;
                }
                if (string.Compare(arg, UseObjKeysArg, ignoreCase: true) == 0)
                {
                    if (!hasBoolValue(out var wanted))
                        return false;
                    parseResult.Inputs.UseObjectKey = wanted;
                    TestParameters.CommandLineOverrides |= TestParameterFlags.UseObjectKey;
                    continue;
                }
                if (string.Compare(arg, UseObjValuesArg, ignoreCase: true) == 0)
                {
                    if (!hasBoolValue(out var wanted))
                        return false;
                    parseResult.Inputs.UseObjectValue = wanted;
                    TestParameters.CommandLineOverrides |= TestParameterFlags.UseObjectValue;
                    continue;
                }
                if (string.Compare(arg, UseRcArg, ignoreCase: true) == 0)
                {
                    if (!hasBoolValue(out var wanted))
                        return false;
                    parseResult.Inputs.UseReadCache = wanted;
                    TestParameters.CommandLineOverrides |= TestParameterFlags.UseReadCache;
                    continue;
                }
                if (string.Compare(arg, LogArg, ignoreCase: true) == 0)
                {
                    if (!hasEnumValue(out LogMode mode))
                        return Usage($"{arg} requires a valid LogMode value");
                    parseResult.Inputs.LogMode = mode;
                    TestParameters.CommandLineOverrides |= TestParameterFlags.LogMode;
                    continue;
                }
                if (string.Compare(arg, ItersArg, ignoreCase: true) == 0)
                {
                    if (!hasIntValue(out var value))
                        return false;
                    parseResult.Inputs.IterationCount = value;
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
                if (string.Compare(arg, MergeResultsArg, ignoreCase: true) == 0
                    || string.Compare(arg, IntersectResultsArg, ignoreCase: true) == 0)
                {
                    while (hasValue(out var filespec, required: false))
                        mergeResultsFilespecs.Add(filespec);
                    if (mergeResultsFilespecs.Count == 0)
                        return Usage($"{arg} requires at least one filespec");
                    intersectResults = string.Compare(arg, IntersectResultsArg, ignoreCase: true) == 0;
                    continue;
                }
                if (string.Compare(arg, VerifyArg, ignoreCase: true) == 0)
                {
                    if (!hasBoolValue(out var wanted))
                        return false;
                    Globals.Verify = wanted;
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

            bool needResultsFile = comparisonMode != ResultComparisonMode.None || mergeResultsFilespecs.Count > 0;
            if (needResultsFile && resultsFilename is null)
                return Usage($"Comparing or merging files requires {ResultsFileArg}");

            if (!parseResult.Inputs.Verify(needResultsFile || !(testFilename is null)))
                return false;

            if (!(testFilename is null))
                testParams = TestParameters.Read(testFilename);
            return true;
        }
    }
}
