// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Threading;

namespace FASTER.core
{
    class PendingFlushList
    {
        const int maxSize = 8;
        const int maxRetries = 10;
        public PageAsyncFlushResult<Empty>[] list;

        public PendingFlushList()
        {
            list = new PageAsyncFlushResult<Empty>[maxSize];
        }

        public bool Add(PageAsyncFlushResult<Empty> t)
        {
            int retries = 0;
            do
            {
                for (int i = 0; i < maxSize; i++)
                {
                    if (list[i] == default)
                    {
                        if (Interlocked.CompareExchange(ref list[i], t, default) == default)
                        {
                            return true;
                        }
                    }
                }
            } while (retries++ < maxRetries);
            return false;
        }

        /// <summary>
        /// Remove item from flush list with from-address equal to the specified address
        /// </summary>
        public bool RemoveNextAdjacent(long address, out PageAsyncFlushResult<Empty> request)
        {
            for (int i=0; i<maxSize; i++)
            {
                request = list[i];
                if (request?.fromAddress == address)
                {
                    if (Interlocked.CompareExchange(ref list[i], null, request) == request)
                    {
                        return true;
                    }
                }
            }
            request = null;
            return false;
        }

        /// <summary>
        /// Remove item from flush list with until-address equal to the specified address
        /// </summary>
        public bool RemovePreviousAdjacent(long address, out PageAsyncFlushResult<Empty> request)
        {
            for (int i = 0; i < maxSize; i++)
            {
                request = list[i];
                if (request?.untilAddress == address)
                {
                    if (Interlocked.CompareExchange(ref list[i], null, request) == request)
                    {
                        return true;
                    }
                }
            }
            request = null;
            return false;
        }
    }
}