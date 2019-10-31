// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.Threading;

namespace FASTER.core
{
    class ErrorList
    {
        private readonly List<long> errorList;

        public ErrorList() => errorList = new List<long>();

        public void Add(long address)
        {
            lock (errorList)
                errorList.Add(address);
        }

        public uint CheckAndWait(long oldFlushedUntilAddress, long currentFlushedUntilAddress)
        {
            bool done = false;
            uint errorCode = 0;
            while (!done)
            {
                done = true;
                lock (errorList)
                {
                    for (int i = 0; i < errorList.Count; i++)
                    {
                        if (errorList[i] >= oldFlushedUntilAddress && errorList[i] < currentFlushedUntilAddress)
                        {
                            errorCode = 1;
                        }
                        else if (errorList[i] < oldFlushedUntilAddress)
                        {
                            done = false; // spin barrier for other threads during exception
                            Thread.Yield();
                        }
                    }
                }
            }
            return errorCode;
        }

        public void RemoveUntil(long currentFlushedUntilAddress)
        {
            lock (errorList)
            {
                for (int i = 0; i < errorList.Count; i++)
                {
                    if (errorList[i] < currentFlushedUntilAddress)
                    {
                        errorList.RemoveAt(i);
                    }
                }
            }

        }
        public int Count => errorList.Count;
    }
}
