// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;

namespace FasterPSFSample
{
    public class NoSerializer : BinaryObjectSerializer<BlittableOrders>
    {
        public override void Deserialize(ref BlittableOrders obj) 
            => throw new NotImplementedException("NoSerializer should not be instantiated");

        public override void Serialize(ref BlittableOrders obj) 
            => throw new NotImplementedException("NoSerializer should not be instantiated");
    }
}
