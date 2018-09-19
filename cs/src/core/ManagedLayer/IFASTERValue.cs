// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.IO;

namespace FASTER.core
{
    public interface IFasterValue<TValue>
    {
        TValue Clone();
        void Deserialize(Stream fromStream);
        void Serialize(Stream toStream);
    }
}
