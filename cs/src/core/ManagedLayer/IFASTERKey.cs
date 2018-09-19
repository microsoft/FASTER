// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.IO;

namespace FASTER.core
{
    public interface IFasterKey<TKey>
    {
        TKey Clone();
        void Deserialize(Stream fromStream);
        bool Equals(TKey other);
        long GetHashCode64();
        void Serialize(Stream toStream);
    }
}
