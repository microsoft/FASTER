// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.benchmark
{
    using System;
    using System.Runtime.InteropServices;
    using System.Security;

    /// <summary>
    /// Methods to perform high-resolution low-overhead timing
    /// </summary>
    public static class HiResTimer
    {
        private const string lib = "kernel32.dll";
        [DllImport(lib)]
        [SuppressUnmanagedCodeSecurity]
        public static extern int QueryPerformanceCounter(ref Int64 count);

        [DllImport(lib)]
        [SuppressUnmanagedCodeSecurity]
        private static extern void GetSystemTimeAsFileTime(out long filetime);

        [DllImport("readtsc.dll")]
        [SuppressUnmanagedCodeSecurity]
        public static extern ulong Rdtsc();

        public static long Freq;

        public static long EstimateCPUFrequency()
        {
            long oldCps = 0, cps = 0;
            ulong startC, endC;
            long accuracy = 500; // wait for consecutive measurements to get within 300 clock cycles

            int i = 0;
            while (i < 5)
            {
                GetSystemTimeAsFileTime(out long startT);
                startC = Rdtsc();

                while (true)
                {
                    GetSystemTimeAsFileTime(out long endT);
                    endC = Rdtsc();

                    if (endT - startT >= 10000000)
                    {
                        cps = (long)(10000000 * (endC - startC) / (double)(endT - startT));
                        break;
                    }
                }


                if ((oldCps > (cps - accuracy)) && (oldCps < (cps + accuracy)))
                {
                    Freq = cps;
                    return cps;
                }
                oldCps = cps;
                i++;
            }
            Freq = cps;
            return cps;
        }
    }
}
