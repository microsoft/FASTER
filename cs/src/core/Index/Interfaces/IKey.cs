// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.IO;

namespace FASTER.core
{
    public interface IKey<T>
    {
        int GetLength();
        void ShallowCopy(ref T dst);

        long GetHashCode64();
        bool Equals(ref T k2);

        bool HasObjectsToSerialize();
        void Serialize(Stream toStream);
        void Deserialize(Stream fromStream);
    }
}