// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using FASTER.common;

namespace FASTER.client
{
    /// <summary>
    /// Client-side serializer for blittable types
    /// </summary>
    /// <typeparam name="Key">Key</typeparam>
    /// <typeparam name="Value">Value</typeparam>
    /// <typeparam name="Input">Input</typeparam>
    /// <typeparam name="Output">Output</typeparam>
    public unsafe struct FixedLenSerializer<Key, Value, Input, Output>  : IClientSerializer<Key, Value, Input, Output>
        where Key : unmanaged
        where Value : unmanaged
        where Input : unmanaged
        where Output : unmanaged
    {
        static FixedLenSerializer()
        {
            if (!IsBlittable<Key>() || !IsBlittable<Value>() || !IsBlittable<Input>() || !IsBlittable<Output>())
                throw new Exception("Cannot use BlittableParameterSerializer with non-blittable types - specify serializer explicitly");
        }

        /// <inheritdoc />
        public bool Write(ref Key k, ref byte* dst, int length)
        {
            if (length < Unsafe.SizeOf<Key>()) return false;
            Unsafe.AsRef<Key>(dst) = k;
            dst += Unsafe.SizeOf<Key>();
            return true;
        }

        /// <inheritdoc />
        public bool Write(ref Value v, ref byte* dst, int length)
        {
            if (length < Unsafe.SizeOf<Value>()) return false;
            Unsafe.AsRef<Value>(dst) = v;
            dst += Unsafe.SizeOf<Value>();
            return true;
        }

        /// <inheritdoc />
        public bool Write(ref Input i, ref byte* dst, int length)
        {
            if (length < Unsafe.SizeOf<Input>()) return false;
            Unsafe.AsRef<Input>(dst) = i;
            dst += Unsafe.SizeOf<Input>();
            return true;
        }

        /// <inheritdoc />
        public Key ReadKey(ref byte* src)
        {
            var _src = src;
            src += Unsafe.SizeOf<Key>();
            return Unsafe.AsRef<Key>(_src);
        }

        /// <inheritdoc />
        public Value ReadValue(ref byte* src)
        {
            var _src = src;
            src += Unsafe.SizeOf<Value>();
            return Unsafe.AsRef<Value>(_src);
        }

        /// <inheritdoc />
        public Output ReadOutput(ref byte* src)
        {
            var _src = src;
            src += Unsafe.SizeOf<Output>();
            return Unsafe.AsRef<Output>(_src);
        }

        /// <summary>
        /// Is type blittable
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        private static bool IsBlittable<T>()
        {
            if (default(T) == null)
                return false;

            try
            {
                var tmp = new T[1];
                var h = GCHandle.Alloc(tmp, GCHandleType.Pinned);
                h.Free();
            }
            catch
            {
                return false;
            }
            return true;
        }
    }
}