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

        //*#*# To do *#*#*
        // Get Emulator Device working
        // Get Local Memory Device working

        //** Used to test the various devices by using the same test with VALUES parameter
        public enum DeviceType
        {
            LSD,
            MLSD
            //EmulatedAzure,
            //LocalMemory
        }


        internal static IDevice CreateTestDevice(DeviceType testDeviceType, string filename, int latencyMs = 20)  // latencyMs works only for DeviceType = LocalMemory
        {
            IDevice device = null;

            switch (testDeviceType)
            {
                case DeviceType.LSD:
                    device = new LocalStorageDevice(filename, true, deleteOnClose: true, true, -1, false, false);
                    break;
                case DeviceType.MLSD:
                    device = new ManagedLocalStorageDevice(filename, deleteOnClose: true);
                    break;
                    /*  For now, just start with two and can add more devices 
                                    case DeviceType.EmulatedAzure:
                                        string EMULATED_STORAGE_STRING = "UseDevelopmentStorage=true;";
                                        string TEST_CONTAINER = "test";
                                        device = new AzureStorageDevice(EMULATED_STORAGE_STRING, $"{TEST_CONTAINER}", "AzureStorageDevicerLogTest", "fasterlog.log", deleteOnClose: true); 
                                        break;
                                    case DeviceType.LocalMemory:
                                        device = new LocalMemoryDevice(1L << 30, 1L << 25, 2, latencyMs: latencyMs);
                                        break;
                    */
            }

            return device;

        }


    }
}
