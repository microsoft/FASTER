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
        /// <summary>
        /// Throw FASTER exception
        /// </summary>
        public FasterException()
        {
        }

        /// <summary>
        /// Throw FASTER exception
        /// </summary>
        /// <param name="message"></param>
        public FasterException(string message) : base(message)
        {
        }

        /// <summary>
        /// Throw FASTER exception
        /// </summary>
        /// <param name="message"></param>
        /// <param name="innerException"></param>
        public FasterException(string message, Exception innerException) : base(message, innerException)
        {
        }

        /// <summary>
        /// Throw FASTER exception
        /// </summary>
        /// <param name="info"></param>
        /// <param name="context"></param>
        public FasterException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }

    /// <summary>
    /// FASTER IO exception type
    /// </summary>
    public class FasterIOException : FasterException
    {
        /// <summary>
        /// Throw FASTER exception
        /// </summary>
        /// <param name="message"></param>
        /// <param name="innerException"></param>
        public FasterIOException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}