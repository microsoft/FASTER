// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <atomic>
#include <cstdint>
#include <string>
#include <libaio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "../core/async.h"
#include "../core/status.h"
#include "file_common.h"

namespace FASTER {
namespace environment {

constexpr const char* kPathSeparator = "/";

/// The File class encapsulates the OS file handle.
class File {
 protected:
  File()
    : fd_{ -1 }
    , device_alignment_{ 0 }
    , filename_{}
    , owner_{ false }
#ifdef IO_STATISTICS
    , write_count_{ 0 }
    , bytes_written_ { 0 }
    , read_count_{ 0 }
    , bytes_read_{ 0 }
#endif
  {
  }

  File(const std::string& filename)
    : fd_{ -1 }
    , device_alignment_{ 0 }
    , filename_{ filename }
    , owner_{ false }
#ifdef IO_STATISTICS
    , write_count_{ 0 }
    , bytes_written_ { 0 }
    , read_count_{ 0 }
    , bytes_read_{ 0 }
#endif
  {
  }

  /// Move constructor.
  File(File&& other)
    : fd_{ other.fd_ }
    , device_alignment_{ other.device_alignment_ }
    , filename_{ std::move(other.filename_) }
    , owner_{ other.owner_ }
#ifdef IO_STATISTICS
    , write_count_{ other.write_count_ }
    , bytes_written_ { other.bytes_written_ }
    , read_count_{ other.read_count_ }
    , bytes_read_{ other.bytes_read_ }
#endif
  {
    other.owner_ = false;
  }

  ~File() {
    if(owner_) {
      core::Status s = Close();
    }
  }

  /// Move assignment operator.
  File& operator=(File&& other) {
    fd_ = other.fd_;
    device_alignment_ = other.device_alignment_;
    filename_ = std::move(other.filename_);
    owner_ = other.owner_;
#ifdef IO_STATISTICS
    write_count_ = other.write_count_;
    bytes_written_ = other.bytes_written_;
    read_count_ = other.read_count_;
    bytes_read_ = other.bytes_read_;
#endif
    other.owner_ = false;
    return *this;
  }

 protected:
  core::Status Open(int flags, FileCreateDisposition create_disposition, bool* exists = nullptr);

 public:
  core::Status Close();
  core::Status Delete();

  uint64_t size() const {
    struct stat stat_buffer;
    int result = ::fstat(fd_, &stat_buffer);
    return (result == 0) ? stat_buffer.st_size : 0;
  }

  size_t device_alignment() const {
    return device_alignment_;
  }

  const std::string& filename() const {
    return filename_;
  }

#ifdef IO_STATISTICS
  uint64_t write_count() const {
    return write_count_.load();
  }
  uint64_t bytes_written() const {
    return bytes_written_.load();
  }
  uint64_t read_count() const {
    return read_count_.load();
  }
  uint64_t bytes_read() const {
    return bytes_read_.load();
  }
#endif

 private:
  core::Status GetDeviceAlignment();
  static int GetCreateDisposition(FileCreateDisposition create_disposition);

 protected:
  int fd_;

 private:
  size_t device_alignment_;
  std::string filename_;
  bool owner_;

#ifdef IO_STATISTICS
 protected:
  std::atomic<uint64_t> write_count_;
  std::atomic<uint64_t> bytes_written_;
  std::atomic<uint64_t> read_count_;
  std::atomic<uint64_t> bytes_read_;
#endif
};

class QueueFile;

/// The QueueIoHandler class encapsulates completions for async file I/O, where the completions
/// are put on the AIO completion queue.
class QueueIoHandler {
 public:
  typedef QueueFile async_file_t;

 private:
  constexpr static int kMaxEvents = 128;

 public:
  QueueIoHandler()
    : io_object_{ 0 } {
  }
  QueueIoHandler(size_t max_threads)
    : io_object_{ 0 } {
    int result = ::io_setup(kMaxEvents, &io_object_);
    assert(result >= 0);
  }

  /// Move constructor
  QueueIoHandler(QueueIoHandler&& other) {
    io_object_ = other.io_object_;
    other.io_object_ = 0;
  }

  ~QueueIoHandler() {
    if(io_object_ != 0)
      ::io_destroy(io_object_);
  }

  /// Invoked whenever a Linux AIO completes.
  static void IoCompletionCallback(io_context_t ctx, struct iocb* iocb, long res, long res2);

  struct IoCallbackContext {
    IoCallbackContext(FileOperationType operation, int fd, size_t offset, uint32_t length,
                      uint8_t* buffer, core::IAsyncContext* context_, core::AsyncIOCallback callback_)
      : caller_context{ context_ }
      , callback{ callback_ } {
      if(FileOperationType::Read == operation) {
        ::io_prep_pread(&this->parent_iocb, fd, buffer, length, offset);
      } else {
        ::io_prep_pwrite(&this->parent_iocb, fd, buffer, length, offset);
      }
      ::io_set_callback(&this->parent_iocb, IoCompletionCallback);
    }

    // WARNING: "parent_iocb" must be the first field in AioCallbackContext. This class is a C-style
    // subclass of "struct iocb".

    /// The iocb structure for Linux AIO.
    struct iocb parent_iocb;

    /// Caller callback context.
    core::IAsyncContext* caller_context;

    /// The caller's asynchronous callback function
    core::AsyncIOCallback callback;
  };

  inline io_context_t io_object() const {
    return io_object_;
  }

  /// Try to execute the next IO completion on the queue, if any.
  bool TryComplete();

 private:
  /// The Linux AIO context used for IO completions.
  io_context_t io_object_;
};

/// The QueueFile class encapsulates asynchronous reads and writes, using the specified AIO
/// context.
class QueueFile : public File {
 public:
  QueueFile()
    : File()
    , io_object_{ nullptr } {
  }
  QueueFile(const std::string& filename)
    : File(filename)
    , io_object_{ nullptr } {
  }
  /// Move constructor
  QueueFile(QueueFile&& other)
    : File(std::move(other))
    , io_object_{ other.io_object_ } {
  }
  /// Move assignment operator.
  QueueFile& operator=(QueueFile&& other) {
    File::operator=(std::move(other));
    io_object_ = other.io_object_;
    return *this;
  }

  core::Status Open(FileCreateDisposition create_disposition, const FileOptions& options,
              QueueIoHandler* handler, bool* exists = nullptr);

  core::Status Read(size_t offset, uint32_t length, uint8_t* buffer,
                    core::IAsyncContext& context, core::AsyncIOCallback callback) const;
  core::Status Write(size_t offset, uint32_t length, const uint8_t* buffer,
                     core::IAsyncContext& context, core::AsyncIOCallback callback);

 private:
  core::Status ScheduleOperation(FileOperationType operationType, uint8_t* buffer, size_t offset,
                           uint32_t length, core::IAsyncContext& context, core::AsyncIOCallback callback);

  io_context_t io_object_;
};

}
} // namespace FASTER::environment
