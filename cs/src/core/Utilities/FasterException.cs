// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Runtime.Serialization;

namespace FASTER.core
{
    /// <summary>
    /// FASTER exception base type
    /// </summary>
    public class FasterException : Exception
    {
        internal FasterException()
        {
        }

        internal FasterException(string message) : base(message)
        {
        }

        internal FasterException(string message, Exception innerException) : base(message, innerException)
        {
        }

        internal FasterException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}