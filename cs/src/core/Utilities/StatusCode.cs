// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;

namespace FASTER.core
{
    /// <summary>
    /// Return status code for FASTER operations
    /// </summary>
    [Flags]
    public enum StatusCode : byte
    {
        // These are the basic codes that correspond to the old Status values, but *do not* compare to these directly; use the IsXxx functions.
        #region Basic status codes
        /// <summary>
        /// General success indicator. By itself it means an in-place update:
        /// <item>Upsert ConcurrentWriter: OK</item>
        /// <item>RMW InPlaceUpdater: OK</item>
        /// <item>Delete ConcurrentDeleter: OK</item>
        /// In combination with advanced enum values it can provide more information; see those values for more information.
        /// </summary>
        OK = 0x00,

        /// <summary>
        /// The key for the operation was not found. For Read, that is all that is returned for an unfound key. For other operations, see
        /// the advanced enum values for more detailed information.
        /// </summary>
        NotFound = 0x01,

        /// <summary>
        /// The Read or RMW operation went pending for I/O. This is not combined with advanced enum values; however, the application should
        /// use this to issue CompletePending operations, and then can apply knowledge of this to the advanced enum values to know whether,
        /// for example, a <see cref="CopyAppend"/> was a copy of a record from the ReadOnly in-memory region or from Storage.
        /// </summary>
        Pending = 0x02,

        /// <summary>
        /// An error occurred. This is not combined with advanced enum values.
        /// </summary>
        Error = 0x04,
        #endregion

        // These are the advanced codes for additional info such as "did we CopyToTail?" or detailed info like "how exactly did this operation achieve its OK status?"
        #region Advanced status codes
        /// <summary>
        /// No advanced bit set.
        /// </summary>
        #pragma warning disable CA1069 // Enums values should not be duplicated; we do not compare to this value--it is just a way to set "no advanced bit"
        None = 0x00,
#pragma warning restore CA1069 // Enums values should not be duplicated

        /// <summary>
        /// Indicates that a previously non-existent key was appended to the log tail. This is combined with basic codes:
        /// <list type="bullet">
        /// <item>Upsert SingleWriter: OK | Append</item>
        /// <item>RMW InitialUpdater: NotFound | Append</item>
        /// <item>Delete SingleDeleter: OK | Append</item>
        /// </list>
        /// </summary>
        NewAppend = 0x10,

        /// <summary>
        /// Indicates that an existing key was appended to the log tail. This is combined with basic codes:
        /// <list type="bullet">
        /// <item>RMW CopyUpdater: OK | CopyAppend</item>
        /// <item>Read CopyToTail: OK | CopyAppend</item>
        /// </list>
        /// Note that these may be obtained from either non-Pending or Pending operations.
        /// </summary>
        CopyAppend = 0x20,

        /// <summary>
        /// This bit is reserved for future use
        /// </summary>
        Reserved = 0x40
        #endregion
    }
}
