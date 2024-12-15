// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

/// <summary>
/// Header for message batch (Little Endian server)
/// [4 byte seqNo][1 byte protocol][3 byte numMessages]
/// </summary>
var BatchHeader = new Object();
BatchHeader.Size = 8;
BatchHeader.NumMessages = 0;