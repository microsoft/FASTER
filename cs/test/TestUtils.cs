// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FASTER.core;
using NUnit.Framework;
using FASTER.devices;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.Storage;

namespace FASTER.test
{
    internal static class TestUtils
    {
        internal static void DeleteDirectory(string path)
        {
            foreach (string directory in Directory.GetDirectories(path))
            {
                DeleteDirectory(directory);
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
            MLSD
            //LocalMemory
        }


        internal static IDevice CreateTestDevice(DeviceType testDeviceType, string filename, int latencyMs = 20)  // latencyMs works only for DeviceType = LocalMemory
        {
            IDevice device = null;

            switch (testDeviceType)
            {
#if WINDOWS
                case DeviceType.LSD:
                    device = new LocalStorageDevice(filename, true, deleteOnClose: true, true, -1, false, false);
                    break;
                case DeviceType.EmulatedAzure:
                    string EMULATED_STORAGE_STRING = "UseDevelopmentStorage=true;";
                    string TEST_CONTAINER = "test";
                    device = new AzureStorageDevice(EMULATED_STORAGE_STRING, $"{TEST_CONTAINER}", "AzureStorageDeviceLogDir", "fasterlogblob", deleteOnClose: true);
                    break;
#endif
                case DeviceType.MLSD:
                    device = new ManagedLocalStorageDevice(filename, deleteOnClose: true);
                    break;
/* Best to not have every test run this - have specific tests that are for latency testing - difficult to have stable tests in such mixed environment.
                // Emulated higher latency storage device - takes a disk latency arg (latencyMs) and emulates an IDevice using main memory, serving data at specified latency
                case DeviceType.LocalMemory:  
                    device = new LocalMemoryDevice(1L << 30, 1L << 25, 2, latencyMs: latencyMs);
                    break;
*/
            }

            return device;
        }
    }
}
