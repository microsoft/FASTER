// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.Threading;

namespace FASTER.core
{
    class ErrorList
    {
        private readonly List<(long address, uint errorCode)> errorList;

        public ErrorList() => errorList = new();

        public void Add(long address, uint errorCode)
        {
            lock (errorList)
                errorList.Add((address, errorCode));
        }

        public uint CheckAndWait(long oldFlushedUntilAddress, long currentFlushedUntilAddress)
        {
            bool done = false;
            uint errorCode = 0;
            uint numIterations = 0;
            while (!done)
            {
                numIterations++;
                done = true;
                lock (errorList)
                {
                    for (int i = 0; i < errorList.Count; i++)
                    {
                        if (errorList[i].address >= oldFlushedUntilAddress && errorList[i].address < currentFlushedUntilAddress)
                        {
                            errorCode = errorList[i].errorCode;
                        }
                        else if (errorList[i].address < oldFlushedUntilAddress)
                        {
                            done = false; // spin barrier for other threads during exception
                            Thread.Yield();
                        }
                    }
                }

                if (numIterations >= 1000000)
                    throw new FasterException("Stuck inside CheckAndWait");
            }
            return errorCode;
        }

        public void RemoveUntil(long currentFlushedUntilAddress)
        {
            lock (errorList)
            {
                for (int i = 0; i < errorList.Count; i++)
                {
                    if (errorList[i].address < currentFlushedUntilAddress)
                    {
                        errorList.RemoveAt(i);
                    }
                }
            }

        }
        public int Count => errorList.Count;
    }
}
