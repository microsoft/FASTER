// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using FASTER.common;

namespace FASTER.common
{
    /// <summary>
    /// Serializer for blittable types
    /// </summary>
    /// <typeparam name="Key">Key</typeparam>
    /// <typeparam name="Value">Value</typeparam>
    /// <typeparam name="Input">Input</typeparam>
    /// <typeparam name="Output">Output</typeparam>
    public unsafe struct BlittableParameterSerializer<Key, Value, Input, Output> 
        : IServerSerializer<Key, Value, Input, Output>, IClientSerializer<Key, Value, Input, Output>
    {
        static BlittableParameterSerializer()
        {
            if (!IsBlittable<Key>() || !IsBlittable<Value>() || !IsBlittable<Input>() || !IsBlittable<Output>())
                throw new Exception("Cannot use BlittableParameterSerializer with non-blittable types - specify serializer explicitly");
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref Key ReadKeyByRef(ref byte* src)
        {
            var _src = (void*)src;
            src += Unsafe.SizeOf<Key>();
            return ref Unsafe.AsRef<Key>(_src);
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref Value ReadValueByRef(ref byte* src)
        {
            var _src = (void*)src;
            src += Unsafe.SizeOf<Value>();
            return ref Unsafe.AsRef<Value>(_src);
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref Input ReadInputByRef(ref byte* src)
        {
            var _src = (void*)src;
            src += Unsafe.SizeOf<Input>();
            return ref Unsafe.AsRef<Input>(_src);
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
        public bool Write(ref Output o, ref byte* dst, int length)
        {
            if (length < Unsafe.SizeOf<Output>()) return false;
            Unsafe.AsRef<Output>(dst) = o;
            dst += Unsafe.SizeOf<Output>();
            return true;
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

        /// <inheritdoc />
        public Output ReadOutput(ref byte* src)
        {
            var _src = src;
            src += Unsafe.SizeOf<Output>();
            return Unsafe.AsRef<Output>(_src);
        }

        /// <inheritdoc />
        public ref Output AsRefOutput(byte* src, int length)
        {
            return ref Unsafe.AsRef<Output>(src);
        }

        /// <inheritdoc />
        public void SkipOutput(ref byte* src)
        {
            src += Unsafe.SizeOf<Output>();
        }

        /// <inheritdoc />
        public int GetLength(ref Output o) => Unsafe.SizeOf<Output>();
    }
}