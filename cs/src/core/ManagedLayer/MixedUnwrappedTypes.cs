// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 1591

using System;
using System.IO;

namespace FASTER.core
{
    public
#if BLIT_KEY
    struct
#else
    class
#endif
        MixedKey : IFasterKey<MixedKey>
    {
        public long key;

        public MixedKey Clone()
        {
            return this;
        }
        public new long GetHashCode()
        {
            return Utility.GetHashCode(key);
        }
        public long GetHashCode64()
        {
            return Utility.GetHashCode(key);
        }

        public bool Equals(MixedKey other)
        {
            return key == other.key;
        }
        public void Serialize(Stream toStream)
        {
            new BinaryWriter(toStream).Write(key);
        }

        public void Deserialize(Stream fromStream)
        {
            key = new BinaryReader(fromStream).ReadInt32();
        }
    }
    public
#if BLIT_VALUE
    struct
#else
    class
#endif
 MixedValue : IFasterValue<MixedValue>
    {
        public long value;
        public MixedValue Clone()
        {
            return this;
        }
        public void Serialize(Stream toStream)
        {
            new BinaryWriter(toStream).Write(value);
        }

        public void Deserialize(Stream fromStream)
        {
            value = new BinaryReader(fromStream).ReadInt32();
        }
    }
    public
#if BLIT_INPUT
    struct
#else
    class
#endif
 MixedInput
    {
    }
    public
#if BLIT_OUTPUT
    struct
#else
    class
#endif
 MixedOutput
    {
    }
    public
#if BLIT_CONTEXT
    struct
#else
    class
#endif
 MixedContext
    {
    }
}
