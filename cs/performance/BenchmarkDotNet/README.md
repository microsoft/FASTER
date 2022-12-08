## BenchmarkDotNet tests

[BenchmarkDotNet](https://benchmarkdotnet.org) is a micro-benchmarking suite that supports annotations on functions, somewhat similar to unit tests.

To run:
- Build Release
- Execute `bin/Release/[platform]/BenchmarkDotNetTests.exe -f *LightEpoch*` to run the LightEpoch tests. See [BenchmarkDotNet](https://benchmarkdotnet.org) and related documentation for more details.

Currently we have two very simple benchmarks; more will be added later.
- LightEpochTests: Currently runs a microbenchmark of epoch Acquire/Release.
- SyncVsAsyncTests: Compares sync vs. async API performance.

BenchmarkDotNet does not support cross-run comparisons; currently, just save the output to a file and diff. A script will be written in the future to automate these comparisons.

### Adding a Test
Use one of the existing tests as an example, and add an entry in this doc describing it briefly.
