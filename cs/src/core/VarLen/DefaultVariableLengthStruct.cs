// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;

namespace FASTER.core
{
    /// <summary>
    /// Default no-op implementation of <see cref="IVariableLengthStruct{T}"/>, for use with non-variable length <typeparamref name="T"/>.
    /// </summary>
    public class DefaultVariableLengthStruct<T> : IVariableLengthStruct<T>
    {
        /// <summary>Default implementation should not be called</summary>
        public static readonly DefaultVariableLengthStruct<T> Default = new();

        /// <summary>
        /// Indicates whether T is a variable-length struct
        /// </summary>
        public bool IsVariableLength => false;

        /// <summary>Default implementation should not be called</summary>
        public int GetLength(ref T t) => throw new NotImplementedException();

        /// <summary>Default implementation should not be called</summary>
        public int GetInitialLength() => throw new NotImplementedException();

        /// <summary>Default implementation should not be called</summary>
        public unsafe void Serialize(ref T source, void* destination) => throw new NotImplementedException();

        /// <summary>Default implementation should not be called</summary>
        public unsafe ref T AsRef(void* source) => throw new NotImplementedException();

        /// <summary>Default implementation should not be called</summary>
        public unsafe void Initialize(void* source, void* end) => throw new NotImplementedException();
    }
}
