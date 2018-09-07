// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using Microsoft.Win32.SafeHandles;
using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    public class LocalStorageDevice : IDevice
    {
        /// <summary>
        /// File information
        /// </summary>
        private readonly string filename;
        private readonly SafeFileHandle logHandle;
        private readonly bool enablePrivileges;
        private readonly bool useIoCompletionPort;

        /// <summary>
        /// Device Information obtained from Native32 methods
        /// </summary>
        private readonly uint lpSectorsPerCluster;
        private readonly uint lpBytesPerSector;
        private readonly uint lpNumberOfFreeClusters;
        private readonly uint lpTotalNumberOfClusters;
        private readonly IntPtr ioCompletionPort;

        public LocalStorageDevice(string filename, bool enablePrivileges = false, 
            bool useIoCompletionPort = false, bool unbuffered = false, bool deleteOnClose = false)
        {
            this.filename = filename;
            this.enablePrivileges = enablePrivileges;
            this.useIoCompletionPort = useIoCompletionPort;

            if (enablePrivileges)
            {
                Native32.EnableProcessPrivileges();
            }

            Native32.GetDiskFreeSpace(filename.Substring(0, 3),
                                        out lpSectorsPerCluster,
                                        out lpBytesPerSector,
                                        out lpNumberOfFreeClusters,
                                        out lpTotalNumberOfClusters);

            uint fileAccess = Native32.GENERIC_READ | Native32.GENERIC_WRITE;
            uint fileShare = unchecked(((uint)FileShare.ReadWrite & ~(uint)FileShare.Inheritable));
            uint fileCreation = unchecked((uint)FileMode.OpenOrCreate);
            uint fileFlags = Native32.FILE_FLAG_OVERLAPPED;

            if (unbuffered)
                fileFlags = fileFlags | Native32.FILE_FLAG_NO_BUFFERING;
        
            if (deleteOnClose)
                fileFlags = fileFlags | Native32.FILE_FLAG_DELETE_ON_CLOSE;

            logHandle = Native32.CreateFileW(filename, 
                                             fileAccess,
                                             fileShare, 
                                             IntPtr.Zero, 
                                             fileCreation, 
                                             fileFlags, 
                                             IntPtr.Zero);

            if (enablePrivileges)
            {
                Native32.EnableVolumePrivileges(ref filename, logHandle);
            }

            if (useIoCompletionPort)
            {
                ioCompletionPort = Native32.CreateIoCompletionPort(
                    logHandle,
                    IntPtr.Zero,
                    (uint)logHandle.DangerousGetHandle().ToInt64(),
                    0);
            }

            try
            {
                ThreadPool.BindHandle(logHandle);
            } 
            catch(Exception e)
            {
                throw new Exception("Error binding log handle for " + filename + ": " + e.ToString());
            }
        }

        /// <summary>
        /// Sets file size to the specified value -- DOES NOT reset file seek pointer to original location
        /// </summary>
        /// <param name="size"></param>
        /// <returns></returns>
        public bool SetFileSize(long size)
        {
            if (enablePrivileges)
                return Native32.SetFileSize(logHandle, size);
            else
            {
                int lodist = (int)size;
                int hidist = (int)(size >> 32);
                Native32.SetFilePointer(logHandle, lodist, ref hidist, Native32.EMoveMethod.Begin);
                if (!Native32.SetEndOfFile(logHandle)) return false;
                return true;
            }
        }

        public string GetFileName()
        {
            return filename;
        }

        public uint GetSectorSize()
        {
            return lpBytesPerSector;
        }

        public void Close()
        {
            Native32.CloseHandle(logHandle);
        }

        public unsafe void ReadAsync(ulong sourceAddress,
                                     IntPtr destinationAddress,
                                     uint readLength,
                                     IAsyncResult asyncResult)
        {
            Overlapped ov = new Overlapped
            {
                AsyncResult = asyncResult,
                OffsetLow = unchecked((int)(sourceAddress & 0xFFFFFFFF)),
                OffsetHigh = unchecked((int)((sourceAddress >> 32) & 0xFFFFFFFF))
            };

            NativeOverlapped* ovNative = ov.UnsafePack(null, IntPtr.Zero);

            /* Invoking the Native method ReadFile provided by Kernel32.dll
             * library. Returns false, if request failed or accepted for async 
             * operation. Returns true, if success synchronously.
             */
            uint bytesRead = default(uint);
            bool result = Native32.ReadFile(logHandle,
                                destinationAddress,
                                readLength,
                                out bytesRead,
                                ovNative);

            if (!result)
            {
                int error = Marshal.GetLastWin32Error();
                
                /* Just handle the case when it is not ERROR_IO_PENDING
                 * If ERROR_IO_PENDING, then it is accepted for async execution
                 */ 
                if (error != Native32.ERROR_IO_PENDING)
                {
                    Overlapped.Unpack(ovNative);
                    Overlapped.Free(ovNative);
                    throw new Exception("Error reading from log file: " + error);
                }
            }
            else
            {
                //executed synchronously, so process callback
                //callback(0, bytesRead, ovNative);
            }
        }

        public unsafe void ReadAsync(ulong sourceAddress, 
                                     IntPtr destinationAddress, 
                                     uint readLength, 
                                     IOCompletionCallback callback, 
                                     IAsyncResult asyncResult)
        {
            //Debug.WriteLine("sourceAddress: {0}, destinationAddress: {1}, readLength: {2}"
            //    , sourceAddress, (ulong)destinationAddress, readLength);

            if (readLength != 512)
            {

            }
            Overlapped ov = new Overlapped(0, 0, IntPtr.Zero, asyncResult);
            NativeOverlapped* ovNative = ov.UnsafePack(callback, IntPtr.Zero);
            ovNative->OffsetLow = unchecked((int)((ulong)sourceAddress & 0xFFFFFFFF));
            ovNative->OffsetHigh = unchecked((int)(((ulong)sourceAddress >> 32) & 0xFFFFFFFF));

            uint bytesRead = default(uint);
            bool result = Native32.ReadFile(logHandle, 
                                            destinationAddress, 
                                            readLength,
                                            out bytesRead, 
                                            ovNative);

            if (!result)
            {
                int error = Marshal.GetLastWin32Error();
                if (error != Native32.ERROR_IO_PENDING)
                {
                    Overlapped.Unpack(ovNative);
                    Overlapped.Free(ovNative);

                    // NOTE: alignedDestinationAddress needs to be freed by whoever catches the exception
                    throw new Exception("Error reading from log file: " + error);
                }
            }
            else
            {
                // On synchronous completion, issue callback directly
                callback(0, bytesRead, ovNative);
            }
        }

        public unsafe void WriteAsync(IntPtr sourceAddress, 
                                      ulong destinationAddress, 
                                      uint numBytesToWrite, 
                                      IOCompletionCallback callback, 
                                      IAsyncResult asyncResult)
        {
            Overlapped ov = new Overlapped(0, 0, IntPtr.Zero, asyncResult);
            NativeOverlapped* ovNative = ov.UnsafePack(callback, IntPtr.Zero);
            ovNative->OffsetLow = unchecked((int)(destinationAddress & 0xFFFFFFFF));
            ovNative->OffsetHigh = unchecked((int)((destinationAddress >> 32) & 0xFFFFFFFF));


            /* Invoking the Native method WriteFile provided by Kernel32.dll
            * library. Returns false, if request failed or accepted for async 
            * operation. Returns true, if success synchronously.
            */
            uint bytesWritten = default(uint);
            bool result = Native32.WriteFile(logHandle,
                                    sourceAddress,
                                    numBytesToWrite,
                                    out bytesWritten,
                                    ovNative);

            if (!result)
            {
                int error = Marshal.GetLastWin32Error();
                /* Just handle the case when it is not ERROR_IO_PENDING
                 * If ERROR_IO_PENDING, then it is accepted for async execution
                 */
                if (error != Native32.ERROR_IO_PENDING)
                {
                    Overlapped.Unpack(ovNative);
                    Overlapped.Free(ovNative);
                    throw new Exception("Error writing to log file: " + error);
                }
            }
            else
            {
                //executed synchronously, so process callback
                callback(0, bytesWritten, ovNative);
            }
        }

        public void DeleteAddressRange(long fromAddress, long toAddress)
        {
        }
    }
}
