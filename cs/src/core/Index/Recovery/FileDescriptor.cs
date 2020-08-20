// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.core
{
    /// <summary>
    /// Complete specification of file with path, for local and cloud files
    /// </summary>
    public struct FileDescriptor
    {
        /// <summary>
        /// Relative directory name or path
        /// </summary>
        public string directoryName;
        /// <summary>
        /// Actual file or blob name
        /// </summary>
        public string fileName;

        /// <summary>
        /// Create FileInfo instance
        /// </summary>
        /// <param name="directoryName"></param>
        /// <param name="fileName"></param>
        public FileDescriptor(string directoryName, string fileName)
        {
            this.directoryName = directoryName;
            this.fileName = fileName;
        }
    }
}