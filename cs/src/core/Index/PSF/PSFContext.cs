// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.core
{
    /// <summary>
    /// Context for operations on the secondary FasterKV instance.
    /// </summary>
    public class PSFContext
    {
        // // TODO Hack because we can't get the functions object from the session, so pass this as context (also hacked to make it a class)
        internal IPSFFunctions Functions;
    }
}
