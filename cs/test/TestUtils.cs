// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using NUnit.Framework;
using System;
using System.Diagnostics;
using System.IO;
using FASTER.core;
using FASTER.devices;

namespace FASTER.test
{
    internal static class TestUtils
    {
        internal static void DeleteDirectory(string path)
        {

            try
            {
                foreach (string directory in Directory.GetDirectories(path))
                    DeleteDirectory(directory);
            }
            catch (DirectoryNotFoundException)
            {
                // Ignore this; some tests call this before the test run to make sure there are no leftovers (e.g. from a debug session).
            }

            // Exceptions may happen due to a handle briefly remaining held after Dispose().
            try
            {
                Directory.Delete(path, true);
            }
            catch (Exception ex) when (ex is IOException ||
                                       ex is UnauthorizedAccessException)
            {
                try
                {
                    Directory.Delete(path, true);
                }
                catch { }
            }
        }

        // Used to test the various devices by using the same test with VALUES parameter
        // Cannot use LocalStorageDevice from non-Windows OS platform
        public enum DeviceType
        {
#if WINDOWS
            LSD,
            EmulatedAzure,
#endif
            MLSD,
            LocalMemory
        }


        internal static IDevice CreateTestDevice(DeviceType testDeviceType, string filename, int latencyMs = 20)  // latencyMs works only for DeviceType = LocalMemory
        {
            IDevice device = null;

            switch (testDeviceType)
            {
#if WINDOWS
                case DeviceType.LSD:
#pragma warning disable CA1416 // Validate platform compatibility -- we're under #if WINDOWS
                    device = new LocalStorageDevice(filename, false, deleteOnClose: true, true, -1, false, false);
#pragma warning restore CA1416 // Validate platform compatibility
                    break;
                case DeviceType.EmulatedAzure:
                    device = new AzureStorageDevice(AzureEmulatedStorageString, AzureTestContainer, AzureTestDirectory, "fasterlogblob", deleteOnClose: true);
                    break;
#endif
                case DeviceType.MLSD:
                    device = new ManagedLocalStorageDevice(filename, deleteOnClose: true);
                    break;
                // Emulated higher latency storage device - takes a disk latency arg (latencyMs) and emulates an IDevice using main memory, serving data at specified latency
                case DeviceType.LocalMemory:  
                    device = new LocalMemoryDevice(1L << 26, 1L << 22, 2, latencyMs: latencyMs);  // 64 MB (1L << 26) is enough for our test cases
                    break;
            }

            return device;
        }

        private static string ConvertedClassName(bool forAzure = false)
        {
            // Make this all under one root folder named {prefix}, which is the base namespace name. All UT namespaces using this must start with this prefix.
            const string prefix = "FASTER.test";
            Debug.Assert(TestContext.CurrentContext.Test.ClassName.StartsWith($"{prefix}."));
            var suffix = TestContext.CurrentContext.Test.ClassName.Substring(prefix.Length + 1);
            return forAzure ? suffix : $"{prefix}/{suffix}";
        }

        internal static string ClassTestDir => Path.Combine(TestContext.CurrentContext.TestDirectory, ConvertedClassName());

        internal static string MethodTestDir => Path.Combine(ClassTestDir, TestContext.CurrentContext.Test.MethodName);

        internal static string AzureTestContainer
        {
            get
            {
                var container = ConvertedClassName(forAzure: true).Replace('.', '-').ToLower();
                Microsoft.Azure.Storage.NameValidator.ValidateContainerName(container);
                return container;
            }
        }

        internal static string AzureTestDirectory => TestContext.CurrentContext.Test.MethodName;

        internal const string AzureEmulatedStorageString = "UseDevelopmentStorage=true;";
    }
}
