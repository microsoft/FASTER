// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;

namespace FASTER.core
{
    class ErrorList
    {
        private List<CommitInfo> errorList;

        public ErrorList() => errorList = new();

        public void Add(CommitInfo info)
        {
            lock (errorList)
                errorList.Add(info);
        }

        public CommitInfo PopEarliestError()
        {
            lock (errorList)
            {
                var result = new CommitInfo {FromAddress = long.MaxValue};
                var index = -1;
                for (var i = 0; i < errorList.Count; i++)
                {
                    if (errorList[i].FromAddress < result.FromAddress)
                    {
                        result = errorList[i];
                        index = i;
                    }
                }
                errorList.RemoveAt(index);

                return result;
            }
        }

        public void ClearError()
        {
            lock (errorList)
                errorList.Clear();
        }

        public void TruncateUntil(long untilAddress)
        {
            lock (errorList)
                errorList = errorList.FindAll(info => info.UntilAddress > untilAddress);
        }

        public bool Empty => errorList.Count == 0;
    }
}