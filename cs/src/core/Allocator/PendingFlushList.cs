// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
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

        public void Add(PageAsyncFlushResult<Empty> t)
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
                            return;
                        }
                    }
                }
            } while (retries++ < maxRetries);
            throw new Exception("Unable to add item to list");
        }

        public bool RemoveAdjacent(long address, out PageAsyncFlushResult<Empty> request)
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
    }

    class CommitInfoWrapper
    {
        public CommitInfo commitInfo;

        public CommitInfoWrapper(CommitInfo commitInfo)
        {
            this.commitInfo = commitInfo;
        }
    }

    class PendingCommitList
    {
        const int maxSize = 256;
        const int maxRetries = 10;
        public CommitInfoWrapper[] list;

        public PendingCommitList()
        {
            list = new CommitInfoWrapper[maxSize];
        }

        public void Add(CommitInfoWrapper t)
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
                            return;
                        }
                    }
                }
            } while (retries++ < maxRetries);
            throw new Exception("Unable to add item to list");
        }

        public bool RemoveAdjacent(long address, out CommitInfoWrapper request)
        {
            request = null;
            while (true)
            {
                int found_i = -1;

                for (int i = 0; i < maxSize; i++)
                {
                    var _request = list[i];
                    if (_request?.commitInfo.FromAddress <= address)
                    {
                        if (found_i >= 0)
                        {
                            if (_request.commitInfo.FromAddress < request.commitInfo.FromAddress)
                            {
                                request = _request;
                                found_i = i;
                            }
                        }
                        else
                        {
                            request = _request;
                            found_i = i;
                        }
                    }
                }

                if (found_i == -1) break;
                if (Interlocked.CompareExchange(ref list[found_i], null, request) == request)
                {
                    return true;
                }
            }
            request = null;
            return false;
        }
    }
}
