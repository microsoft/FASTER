// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Runtime.Serialization;

namespace FASTER.core
{
    /// <summary>
    /// FASTER PSF exception base type
    /// </summary>
    public class PSFException : FasterException
    {
        internal PSFException() { }

        internal PSFException(string message) : base(message) { }

        internal PSFException(string message, Exception innerException) : base(message, innerException) { }
    }

    /// <summary>
    /// FASTER PSF argument exception type
    /// </summary>
    public class PSFArgumentException : PSFException
    {
        internal PSFArgumentException() { }

        internal PSFArgumentException(string message) : base(message) { }

        internal PSFArgumentException(string message, Exception innerException) : base(message, innerException) { }
    }

    /// <summary>
    /// FASTER PSF argument exception type
    /// </summary>
    public class PSFInvalidOperationException : PSFException
    {
        internal PSFInvalidOperationException() { }

        internal PSFInvalidOperationException(string message) : base(message) { }

        internal PSFInvalidOperationException(string message, Exception innerException) : base(message, innerException) { }
    }

    /// <summary>
    /// FASTER PSF argument exception type
    /// </summary>
    public class PSFInternalErrorException : PSFException
    {
        internal PSFInternalErrorException() { }

        internal PSFInternalErrorException(string message) : base($"Internal Error: {message}") { }

        internal PSFInternalErrorException(string message, Exception innerException) : base($"Internal Error: {message}", innerException) { }
    }
}
