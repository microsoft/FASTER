// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;

namespace BenchmarkDotNetTests
{
    public class StructInliningTests
    {
        public interface ITestInterface
        {
            public long Read();
        }

        public class TestClass : ITestInterface
        {
            public long Read() => 42;
        }

        public struct TestStruct : ITestInterface
        {
            public readonly long Read() => 101;
        }

        static void TestInterface(ITestInterface impl)
        {
            long total = 0;
            for (int i = 0; i < 1000; i++)
                total += impl.Read();
        }

        static void TestGeneric<TImpl>(TImpl impl) where TImpl : ITestInterface
        {
            long total = 0;
            for (int i = 0; i < 1000; i++)
                total += impl.Read();
        }

        #pragma warning disable CA1822 // Mark members as static; Benchmarks must be instance members

        [Benchmark(Baseline = true)]
        public void StructInterface()  // Will not inline because it is an interface; also has boxing overhead 
        {
            TestInterface(new TestStruct());
        }

        [Benchmark]
        public void ClassInterface()   // will not inline because it is an interface
        {
            TestInterface(new TestClass());
        }

        [Benchmark]
        public void StructGeneric()     // will inline because it is a known struct type
        {
            TestGeneric(new TestStruct());
        }

        [Benchmark]
        public void ClassGeneric()     // will not inline even though the type is known, because it is a class (not struct) type
        {
            TestGeneric(new TestClass());
        }
    }
}
