// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using NUnit.Framework;
using System;
using System.Diagnostics;
using System.IO;
using FASTER.core;
using FASTER.devices;
using System.Threading;
using System.Runtime.InteropServices;

namespace FASTER.test
{
    internal static class TestUtils
    {
        /// <summary>
        /// Delete a directory recursively
        /// </summary>
        /// <param name="path">The folder to delete</param>
        /// <param name="wait">If true, loop on exceptions that are retryable, and verify the directory no longer exists. Generally true on SetUp, false on TearDown</param>
        internal static void DeleteDirectory(string path, bool wait = false)
        {
            if (!Directory.Exists(path))
                return;

            foreach (string directory in Directory.GetDirectories(path))
                DeleteDirectory(directory, wait);

            bool retry = true;
            while (retry)
            {
                // Exceptions may happen due to a handle briefly remaining held after Dispose().
                retry = false;
                try
                {
                    Directory.Delete(path, true);
                }
                catch (Exception ex) when (ex is IOException ||
                                           ex is UnauthorizedAccessException)
                {
                    if (!wait)
                    {
                        try { Directory.Delete(path, true); }
                        catch { }
                        return;
                    }
                    retry = true;
                }
            }
            
            if (!wait)
                return;

            while (Directory.Exists(path))
                Thread.Yield();
        }

        /// <summary>
        /// Create a clean new directory, removing a previous one if needed.
        /// </summary>
        /// <param name="path"></param>
        internal static void RecreateDirectory(string path)
        {
            if (Directory.Exists(path))
                DeleteDirectory(path);

            // Don't catch; if this fails, so should the test
            Directory.CreateDirectory(path);
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
            bool preallocateFile = false;
            long capacity = -1; // Capacity unspecified
            bool recoverDevice = false;
            bool useIoCompletionPort = false;
            bool disableFileBuffering = true;

            bool deleteOnClose = false;


            switch (testDeviceType)
            {
#if WINDOWS
                case DeviceType.LSD:
#if NETSTANDARD || NET
                    if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))    // avoids CA1416 // Validate platform compatibility
#endif
                        device = new LocalStorageDevice(filename, preallocateFile, deleteOnClose, disableFileBuffering, capacity, recoverDevice, useIoCompletionPort);
                    break;
                case DeviceType.EmulatedAzure:
                    device = new AzureStorageDevice(AzureEmulatedStorageString, AzureTestContainer, AzureTestDirectory, "fasterlogblob", deleteOnClose: true);
                    break;
#endif
                case DeviceType.MLSD:
                    device = new ManagedLocalStorageDevice(filename, preallocateFile, deleteOnClose, capacity, recoverDevice);
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

        internal static string MethodTestDir => Path.Combine(TestContext.CurrentContext.TestDirectory, $"{ConvertedClassName()}_{TestContext.CurrentContext.Test.MethodName}");

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
