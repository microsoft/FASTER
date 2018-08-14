// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <string>

#ifdef _WIN32
#define NOMINMAX
#define _WINSOCKAPI_
#include <Windows.h>
#else
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#endif

namespace FASTER {
namespace benchmark {

/// Basic wrapper around synchronous file read.
class File {
 public:
  File(const std::string& filename) {
#ifdef _WIN32
    file_handle_ = ::CreateFileA(filename.c_str(), GENERIC_READ, FILE_SHARE_READ, nullptr,
                                 OPEN_EXISTING, FILE_FLAG_NO_BUFFERING, nullptr);
#else
    fd_ = ::open(filename.c_str(), O_RDONLY | O_DIRECT, S_IRUSR);
#endif
  }

  ~File() {
#ifdef _WIN32
    ::CloseHandle(file_handle_);
#else
    ::close(fd_);
#endif
  }

  size_t Read(void* buf, size_t count, uint64_t offset) {
#ifdef _WIN32
    DWORD bytes_read { 0 };
    ::ReadFile(file_handle_, buf, static_cast<DWORD>(count), &bytes_read, nullptr);
    return bytes_read;
#else
    return ::pread(fd_, buf, count, offset);
#endif
  }

 private:
#ifdef _WIN32
  HANDLE file_handle_;
#else
  int fd_;
#endif
};

}
} // namespace FASTER::benchmark
