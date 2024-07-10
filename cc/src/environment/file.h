// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#ifdef _WIN32
#include "file_windows.h"
#else
#include "file_linux.h"
#endif

namespace FASTER {
namespace environment {

    std::string NormalizePath(const std::string& path) {
    // Append a separator to the end of 'path' if not already present
    static std::string sep{ FASTER::environment::kPathSeparator };
    std::string new_path{ path };
    if (path.size() < sep.size() || path.compare(path.size() - sep.size(), path.size(), sep) != 0) {
        new_path += sep;
    }
    return new_path;
    }

}
}