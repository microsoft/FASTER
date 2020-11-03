// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include <atomic>
#include <string>
#include <experimental/filesystem>

#ifdef _WIN32

#include <concurrent_unordered_map.h>
template <typename K, typename V>
using concurrent_unordered_map = concurrency::concurrent_unordered_map<K, V>;

#include <concurrent_queue.h>
template <typename T>
using concurrent_queue = concurrency::concurrent_queue<T>;

#else

#include "tbb/concurrent_unordered_map.h"
template <typename K, typename V>
using concurrent_unordered_map = tbb::concurrent_unordered_map<K, V>;

#include "tbb/concurrent_queue.h"
template <typename T>
using concurrent_queue = tbb::concurrent_queue<T>;

#endif

#include "file_system_disk.h"

#include "core/thread.h"
#include "core/address.h"
#include "core/gc_state.h"
#include "core/persistent_memory_malloc.h"

#include "common/log.h"

#include "environment/file.h"

using namespace FASTER::core;
using namespace FASTER::environment;

namespace FASTER {
namespace device {

/// Forward declaration of StorageFile.
template <class H, class R>
class StorageFile;
template <class H, class R>
class StorageDevice;

/// A File on local SSD.
template <class H, class R>
class LocalFile {
 public:
  /// Typedef for convenience. Determines the mechanism used to handle async IOs.
  typedef H handler_t;
  typedef typename handler_t::async_file_t inner_file_t;

  /// Constructor for LocalFile.
  ///
  /// \param fileName
  ///    Name of the file to the created.
  /// \param ioHandler
  ///    Pointer to handler over which this file will schedule it's async
  ///    IO operations.
  /// \param size
  ///    The total size of the file.
  /// \param unBuffered
  ///    If true, the underlying operating system will not buffer writes to
  ///    this created file.
  /// \param deleteOnClose
  ///    If true, the underlying operating system will delete this file once
  ///    closed.
  LocalFile(const std::string& fileName, handler_t* ioHandler,
            uint64_t size=Address::kMaxOffset, bool unBuffered=true,
            bool deleteOnClose=false)
    : status{ }
    , pages{ nullptr }
    , flushedOffset{ Address(0) }
    , size{ size }
    , file{ fileName }
    , flags{ unBuffered, deleteOnClose }
    , ioHandler{ ioHandler }
    , numPages{ 0 }
  {}

  /// Default constructor.
  LocalFile()
    : status{ }
    , pages{ nullptr }
    , flushedOffset{ Address(0) }
    , size{ }
    , file{ }
    , flags{ }
    , ioHandler{ }
    , numPages{ 0 }
  {}

  /// Move constructor. Required by StorageDevice.
  LocalFile(LocalFile&& from)
    : status{ }
    , pages{ std::move(from.pages) }
    , flushedOffset{ from.flushedOffset.load() }
    , size{ std::move(from.size) }
    , file{ std::move(from.file) }
    , flags{ std::move(from.flags) }
    , ioHandler{ std::move(from.ioHandler) }
    , numPages{ std::move(from.numPages) }
  {
    FlushCloseStatus l = from.status.load();
    status.store(l.flush, l.close);
  }

  /// Move assignment constructor.
  LocalFile& operator=(LocalFile&& from) {
    FlushCloseStatus l = from.status.load();
    this->status.store(l.flush, l.close);
    this->pages = std::move(from.pages);
    this->flushedOffset.store(from.flushedOffset.load());
    this->size = std::move(from.size);
    this->file = std::move(from.file);
    this->flags = std::move(from.flags);
    this->ioHandler = std::move(from.ioHandler);
    this->numPages = std::move(from.numPages);

    return *this;
  }

  // Delete copy and copy-assign constructors.
  LocalFile(const LocalFile& from) = delete;
  LocalFile& operator=(const LocalFile& from) = delete;

  /// Opens the file.
  ///
  /// \return
  ///    Status::Ok on success.
  Status Open() {
    // Set the status of the file as InProgress, Open. InProgress because
    // any writes received are also sent to Azure blob store. Open because
    // the file will service writes and reads.
    status.store(FlushStatus::InProgress, CloseStatus::Open);

    // Allocate the page status array.
    numPages = size / StorageDevice<H, R>::maxRemoteIO;
    if (size % Address::kMaxOffset != 0) numPages++;
    pages = new FullPageStatus[numPages];

    return file.Open(FileCreateDisposition::OpenOrCreate,
                     flags, ioHandler, nullptr);
  }

  /// Closes the file.
  ///
  /// \return
  ///    Status::Ok on success.
  Status Close() {
    delete(pages);
    return file.Close();
  }

  /// Deletes the file.
  ///
  /// \return
  ///    Status::Ok on success.
  Status Delete() {
    return file.Delete();
  }

  /// Reads a contiguous sequence of bytes from the file.
  /// The return value indicates whether the read completed or is in progress
  /// (asynchronous).
  ///
  /// \param source
  ///    The starting address in the file to read from.
  /// \param dest
  ///    The address to read the sequence of bytes into.
  /// \param length
  ///    The number of bytes to be read starting from 'source'.
  /// \param callback
  ///    If the read happens asynchronously, the callback to execute once
  ///    it completes.
  /// \param context
  ///    If the read happens asynchronously, the context required by the
  ///    callback.
  ///
  /// \return
  ///    Status::Ok if the read completed. Status::Pending if it went
  ///    asynchronous.
  Status ReadAsync(uint64_t source, void* dest, uint32_t length,
                   AsyncIOCallback callback, IAsyncContext& context) const
  {
    return file.Read(source, length, reinterpret_cast<uint8_t*>(dest),
                     context, callback);
  }

  /// Writes a contiguous sequence of bytes to the file.
  ///
  /// \param source
  ///    The address to write the sequence of bytes from.
  /// \param dest
  ///    The address in the file to write to.
  /// \param length
  ///    The number of bytes to write starting from dest.
  /// \param callback
  ///    If the write happens asynchronously, the callback to execute once
  ///    it completes.
  /// \param context
  ///    If the write happens asynchronously, the context required by the
  ///    callback.
  ///
  /// \return
  ///    Status::Ok if the write completed. Status::Pending if it went
  ///    asynchronous.
  Status WriteAsync(const void* source, uint64_t dest,
                    uint32_t length, AsyncIOCallback callback,
                    IAsyncContext& context)
  {
    return file.Write(dest, length, reinterpret_cast<const uint8_t*>(source),
                      context, callback);
  }

  /// Returns the alignment of the underlying local hardware/storage device.
  size_t alignment() const {
    return 4096;
  }

  /// Performs a monotonic atomic update on a variable. This method is
  /// a static method.
  ///
  /// \param variable
  ///    Reference to the variable to be updated.
  /// \param new_value
  ///    The value that the variable should be updated to.
  /// \param old_value
  ///    The value of the variable before the update. This parameter is
  ///    set by this function.
  ///
  /// \return
  ///    True if the update was performed. False otherwise.
  static inline bool MonotonicUpdate(AtomicAddress& variable,
                                     Address new_value,
                                     Address& old_value)
  {
    old_value = variable.load();
    while(old_value < new_value) {
      if(variable.compare_exchange_strong(old_value, new_value)) return true;
    }

    return false;
  }

  /// Callback invoked when a page belonging to this file has flushed to
  /// Azure blob storage.
  ///
  /// \param local
  ///    Pointer to the LocalFile that the page belongs to.
  /// \param page
  ///    Page within the file that was flushed.
  /// \param until
  ///    The address within the page (relative to the file) upto which
  ///    the flush completed.
  static void FlushLocal(LocalFile* local, uint64_t page, Address until) {
    // Return if there is an overflow on the number of pages.
    if (page >= local->numPages) return;

    // Update the address upto which the page has flushed.
    local->pages[page].LastFlushedUntilAddress.store(until);

    // Get the address upto which the local file (that the page belongs to)
    // has flushed. Also, lookup the page that this address belongs to, and
    // get the address upto which the page has flushed.
    Address currFlushedUntil = local->flushedOffset.load();
    uint64_t idx = currFlushedUntil.control() >> Address::kOffsetBits;
    if (idx >= local->numPages) return;
    Address pageFlushedUntil = local->pages[idx].LastFlushedUntilAddress.load();

    bool update = false;

    // If the page that that currFlushedUntil falls on has been flushed,
    // then update currFlushedUntil to the next page. Continue doing so
    // for every successive page that has flushed.
    while (pageFlushedUntil > currFlushedUntil) {
      currFlushedUntil = pageFlushedUntil;
      update = true;
      idx++;
      if (idx >= local->numPages) break;
      pageFlushedUntil = local->pages[idx].LastFlushedUntilAddress.load();
    }

    // Update the address upto which the local file has been flushed
    // if necessary.
    if (update) {
      Address old;
      MonotonicUpdate(local->flushedOffset, currFlushedUntil, old);
    }
  }

 private:
  /// Atomic status word indicating whether this file has been flushed and
  /// closed. Flushed => all contents of this file are also on Azure Blob
  /// Storage. Closed => no threads are reading from this file.
  AtomicFlushCloseStatus status;

  /// The status of each page on this file. Required to determine when all
  /// this file's contents have been flushed to blob storage.
  FullPageStatus* pages;

  /// The file offset upto which the contents of this file have been flushed
  /// to Azure blob storage.
  AtomicAddress flushedOffset;

  /// The total amount of data this file will hold.
  uint64_t size;

  /// Underlying file that stores the data and interacts with a thread pool
  /// for asynchronous IO operations.
  inner_file_t file;

  /// Flags that the file will be opened with. Determines whether the file is
  /// unbuffered etc.
  FileOptions flags;

  /// Pointer to thread pool on which asynchronous IO operations will be
  /// scheduled. Callbacks will be executed on this threadpool.
  handler_t* ioHandler;

  /// The number of pages held by this file.
  uint64_t numPages;

  /// StorageFile can access all private members of this class.
  friend class StorageFile<handler_t, R>;
};

/// A File abstraction that spans both, local SSD and Azure Blobs.
template <class H, class R>
class StorageFile {
 public:
  /// Typedef for convenience. Determines the mechanism to handle async IO completions.
  typedef H handler_t;

  /// Typedef for convenience. Determines the DFS layer.
  typedef R remote_t;
  typedef typename remote_t::Task task_t;

  /// Constructor for StorageFile.
  ///
  /// \param fileName
  ///    Name of the file to be created. All files created on local disk
  ///    will have this string as a prefix on their names.
  /// \param ioHandler
  ///    Local threadpool on which writes to local disk/SSD will
  ///    be scheduled.
  /// \param epoch
  ///    Pointer to an Epoch manager. Required for file deletion.
  /// \param pendingTasks
  ///    Queue onto which async tasks issued to the remote layer need to
  ///    be appended. Required because TryComplete() is a method on the
  ///    storage device rather than a storage file.
  /// \param id
  ///    16-bit identifier for the server. Required to identify the correct
  ///    file on the remote tier to send reads and writes to.
  /// \param dfs
  ///    A connection string so that we can communicate with the remote layer.
  /// \param numFiles
  ///    The total number of files to split the log across on local disk.
  /// \param distance
  ///    Parameter determining how aggressive eviction should be. Lower is
  ///    more aggressive.
  /// \param bits
  ///    Logarithm base 2 of the size of each local file the log is split
  ///    across.
  StorageFile(std::string fileName, handler_t* ioHandler, LightEpoch* epoch,
              concurrent_queue<task_t>* pendingTasks,
              uint16_t id, const std::string& dfs,
              uint64_t numFiles=16, uint64_t distance=8, uint64_t bits=9)
    : localFiles()
    , safeRemoteOffset(0)
    , remote(utility::conversions::to_string_t(dfs).c_str(), pendingTasks, id)
    , remoteOffset(0)
    , remoteFlushed(0)
    , tailFile(0)
    , baseName(fileName)
    , ioHandler(ioHandler)
    , epoch(epoch)
    , kFileBits(bits)
    , kFileSize((uint64_t)1 << kFileBits)
    , kNumFiles(numFiles)
    , kDistance(distance * kFileSize)
    , kLocalSize(kNumFiles * kFileSize)
  {}

  /// Default constructor.
  StorageFile()
    : localFiles()
    , safeRemoteOffset(0)
    , remote()
    , remoteOffset(0)
    , remoteFlushed(0)
    , tailFile(0)
    , baseName()
    , ioHandler(nullptr)
    , epoch(nullptr)
    , kFileBits(9)
    , kFileSize((uint64_t)1 << kFileBits)
    , kNumFiles(16)
    , kDistance((uint64_t)8 * kFileSize)
    , kLocalSize(kNumFiles * kFileSize)
  {}

  /// Move constructor.
  StorageFile(StorageFile&& from)
    : localFiles(std::move(from.localFiles))
    , safeRemoteOffset(from.safeRemoteOffset.load())
    , remote(std::move(from.remote))
    , remoteOffset(from.remoteOffset.load())
    , remoteFlushed(from.remoteFlushed.load())
    , tailFile(0)
    , baseName(std::move(from.baseName))
    , ioHandler(std::move(from.ioHandler))
    , epoch(std::move(from.epoch))
    , kFileBits(std::move(from.kFileBits))
    , kFileSize(std::move(from.kFileSize))
    , kNumFiles(std::move(from.kNumFiles))
    , kDistance(std::move(from.kDistance))
    , kLocalSize(std::move(from.kLocalSize))
  {}

  /// Move-assign constructor.
  StorageFile& operator=(StorageFile&& from) {
    this->localFiles = std::move(from.localFiles);
    this->safeRemoteOffset.store(from.safeRemoteOffset.load());
    this->remote = std::move(from.remote);
    this->remoteOffset.store(from.remoteOffset.load());
    this->remoteFlushed.store(from.remoteFlushed.load());
    this->tailFile = std::move(from.tailFile);
    this->baseName = std::move(from.baseName);
    this->ioHandler = std::move(from.ioHandler);
    this->epoch = std::move(from.epoch);
    this->kFileBits = std::move(from.kFileBits);
    this->kFileSize = std::move(from.kFileSize);
    this->kNumFiles = std::move(from.kNumFiles);
    this->kDistance = std::move(from.kDistance);
    this->kLocalSize = std::move(from.kLocalSize);

    return *this;
  }

  // Disallow copy and copy-assign constructors.
  StorageFile(const StorageFile&) = delete;
  StorageFile& operator=(const StorageFile&) = delete;

  /// Opens the file.
  ///
  /// \return
  ///    Status::Ok on success.
  Status Open() {
    std::string m = std::string("Opening HybridLog Storage file. ") +
                    "The log will span %lu %lu GB files on local storage, " +
                    "with the rest flushed to the remote tier. " +
                    "Files will be evicted to the remote tier when there is " +
                    "more than %lu GB on local storage";
    logMessage(Lvl::INFO, m.c_str(), kNumFiles, kFileSize >> 30,
               kDistance >> 30);

    // First open the tail file.
    std::string name = baseName + std::to_string(tailFile.control());
    LocalFile<handler_t, remote_t>* tail =
                new LocalFile<handler_t, remote_t>(name, ioHandler, kFileSize);
    if (tail->Open() != Status::Ok) {
      logMessage(Lvl::ERR, "Failed to open local file %s", name.c_str());
      return Status::Aborted;
    }

    localFiles[tailFile.control()] = tail;

    // Next, open the remote file and return.
    return remote.Open();
  }

  /// Closes the file.
  ///
  /// \return
  ///    Status::Ok on success.
  Status Close() {
    // First close all local files. If any fail, return an error.
    for (auto& file : localFiles) {
      if (file.second->Close() != Status::Ok) {
        logMessage(Lvl::ERR, "Failed to close local file %s",
                   baseName + std::to_string(file.first) + ".log");
        return Status::Aborted;
      }

      // Delete all LocalFile instances.
      if (file.second->Delete() != Status::Ok) {
        logMessage(Lvl::ERR, "Failed to delete local file %s",
                   baseName + std::to_string(file.first) + ".log");
        return Status::Aborted;
      }

      delete(file.second);
    }

    // Remove all local file handles.
    localFiles.clear();

    // Next, close the remote file and return.
    return remote.Close();
  }

  /// Deletes the file.
  ///
  /// \return
  ///    Status::Ok on success.
  Status Delete() {
    // First delete all local files. If any fail, return an error.
    for (auto& file : localFiles) {
      if (file.second->Delete() != Status::Ok) {
        logMessage(Lvl::ERR, "Failed to delete local file %s",
                   baseName + std::to_string(file->first) + ".log");
        return Status::Aborted;
      }
    }

    // Next, delete the remote file and return.
    return remote.Delete();
  }

  /// Reads a contiguous sequence of bytes from the file.
  /// The return value indicates whether the read completed or is in progress
  /// (asynchronous).
  ///
  /// \param source
  ///    The starting address in the file to read from.
  /// \param dest
  ///    The address to read the sequence of bytes into.
  /// \param length
  ///    The number of bytes to be read starting from 'source'.
  /// \param callback
  ///    If the read happens asynchronously, the callback to execute once
  ///    it completes.
  /// \param context
  ///    If the read happens asynchronously, the context required by the
  ///    callback.
  ///
  /// \return
  ///    Status::Ok if the read completed. Status::Pending if it went
  ///    asynchronous.
  Status ReadAsync(uint64_t source, void* dest, uint32_t length,
                   AsyncIOCallback callback, IAsyncContext& context)
  {
    // If the source address belongs to this FASTER instance's logical
    // address address space and is greater than the remote offset, then
    // lookup the local file (on local SSD). The former condition is
    // satisfied if bit 49 to 64 of the source address is equal to zero.
    if (((source >> 48) == 0) && (Address(source) >= remoteOffset.load())) {
      // Find which file the address resides on, and issue an async
      // read to it.
      uint64_t file = source & ~(kFileSize - 1);
      auto handle = localFiles.find(file);
      return handle->second->ReadAsync(source & (kFileSize - 1), dest, length,
                                       callback, context);
    } else {
      // The record was either migrated over from a different machine, or
      // was evicted from the local file to Azure blob store. Lookup the
      // remote file to find it.
      return remote.ReadAsync(source, length, reinterpret_cast<uint8_t*>(dest),
                              callback, context);
    }
  }

  /// Writes a contiguous sequence of bytes to the file.
  ///
  /// \param source
  ///    The address to write the sequence of bytes from.
  /// \param dest
  ///    The address in the file to write to.
  /// \param length
  ///    The number of bytes to write starting from dest.
  /// \param callback
  ///    If the write happens asynchronously, the callback to execute once
  ///    it completes.
  /// \param context
  ///    If the write happens asynchronously, the context required by the
  ///    callback.
  ///
  /// \return
  ///    Status::Ok if the write completed. Status::Pending if it went
  ///    asynchronous.
  Status WriteAsync(const void* source, uint64_t dest, uint32_t length,
                    AsyncIOCallback callback, IAsyncContext& context)
  {
    // Identify the file this write has to go to. Writes are assumed to
    // never straddle a file boundary.
    Address file = Address((uint64_t)(dest >> kFileBits) << kFileBits);

    while (true) {
      // First check if the file being written to overflows the current
      // tailFile. If it does, then try to allocate a new tail file.
      Address tail = tailFile.load();
      if (file <= tail) break;

      // Check if there is only one file between the tail file and size
      // limit. If so, then shift the remote offset, and make room on
      // the local SSD. Return to the caller after doing so.
      if (tail.control() + (kFileSize << (uint64_t)1) >=
          safeRemoteOffset.load().control() + kLocalSize)
      {
        auto m = std::string("Aborting write because space consumed locally ") +
                 "(%lu GB) is close to exceeding configured limit (%lu GB)";
        logMessage(Lvl::INFO, m.c_str(),
                   tail.control() - safeRemoteOffset.load().control(),
                   kLocalSize >> 30);
        ShiftRemoteOffset(Address(tail.control() + kFileSize));
        return Status::Aborted;
      }

      // Bump up the tailFile, and shift the remote offset. If the update
      // failed, then retry the above checks because another thread might
      // have moved the tail file forward.
      Address newTail = Address(tail.control() + kFileSize);
      if (!tailFile.compare_exchange_strong(tail, newTail)) continue;

      LocalFile<handler_t, remote_t>* local =
          new LocalFile<handler_t, remote_t>(baseName +
                                    std::to_string(newTail.control()) + ".log",
                                    ioHandler, kFileSize);
      if (local->Open() != Status::Ok) {
        logMessage(Lvl::ERR,
                   "Write required opening of new file %s which failed",
                   baseName + std::to_string(newTail.control()) + ".log");
        return Status::Aborted;
      }

      localFiles.insert(std::make_pair(newTail.control(), local));
      ShiftRemoteOffset(newTail);
      break;
    }

    // Get the file handle. Might need to wait because a thread might have
    // moved the tail, but not created a new file handle yet.
    auto handle = localFiles.find(file.control());
    while (handle == localFiles.end()) handle = localFiles.find(file.control());

    // Issue the async write to the local file.
    Status lRes = handle->second->WriteAsync(source, dest & (kFileSize - 1),
                                             length, callback, context);
    if (lRes != Status::Ok) {
      logMessage(Lvl::ERR,
                 "Failed to write %lu B to address %lu within file %s",
                 length, dest, baseName + std::to_string(handle->first) +
                 ".log");
      return Status::Aborted;
    }

    /// Depending on the remote tier's max IO size for writes, this
    /// operation might need to be broken up.
    uint32_t remainingBytes = length;
    uint64_t currentDest = dest;
    uint8_t* currentSource = const_cast<uint8_t*>(
                reinterpret_cast<const uint8_t*>(source));
    uint32_t maxRemoteIO = StorageDevice<H, R>::maxRemoteIO;

    while (remainingBytes > maxRemoteIO) {
      // Create a context for the async write to the remote file. Identify
      // the page within the local file that this write is for.
      uint64_t pageInFile = (currentDest & (kFileSize - 1)) / maxRemoteIO;
      uint64_t untilAddrs = (currentDest & (kFileSize - 1)) + maxRemoteIO;

      // The data to be written will have to be copied onto the heap, because
      // it could be evicted from the HybridLog's circular buffer.
      uint8_t* copy = new uint8_t[maxRemoteIO];
      memcpy(copy, currentSource, maxRemoteIO);
      FlushContext fContext = FlushContext(this, file, pageInFile, untilAddrs,
                                           currentDest, copy);

      // Issue the async write to Azure blob storage.
      Status lRes = remote.WriteAsync(copy, maxRemoteIO, currentDest,
                                      Flush, fContext);
      if (lRes != Status::Ok) {
        logMessage(Lvl::ERR,
                   "Failed to write %lu B to address %lu on the remote store",
                   maxRemoteIO, currentDest);
        return Status::Aborted;
      }

      remainingBytes -= maxRemoteIO;
      currentDest += maxRemoteIO;
      currentSource += maxRemoteIO;
    }

    // Issue any remaining bytes to the remote tier.
    uint64_t pageInFile = (currentDest & (kFileSize - 1)) / maxRemoteIO;
    uint64_t untilAddrs = (currentDest & (kFileSize - 1)) + remainingBytes;

    uint8_t* copy = new uint8_t[remainingBytes];
    memcpy(copy, currentSource, remainingBytes);
    FlushContext fContext = FlushContext(this, file, pageInFile, untilAddrs,
                                         currentDest, copy);

    return remote.WriteAsync(copy, remainingBytes, currentDest, Flush,
                             fContext);
  }

  /// Returns the file's alignment.
  size_t alignment() const {
    return 4096;
  }

  /// Truncates the log below a particular offset. XXX Currently does nothing.
  void Truncate(uint64_t newBeginOffset, GcState::truncate_callback_t cb) {
    if (cb) cb(newBeginOffset);
  }

 private:
  /// Updates the remoteFlushed address.
  void UpdateRemoteFlushed() {
    // Read the current value of remoteFlushed.
    Address currRemoteFlushed = remoteFlushed.load();

    bool update = false;

    // Lookup the file that currRemoteFlushed falls inside. If the
    // file is marked flushed, then increment currRemoteFlushed by the
    // size of the file. Stop once you've reached a file that has not
    // been flushed yet.
    do {
      auto handle = localFiles.find(currRemoteFlushed.control());
      if (handle == localFiles.end()) break;

      FlushCloseStatus fStatus = handle->second->status.load();
      if (fStatus.flush == FlushStatus::Flushed) {
        currRemoteFlushed += handle->second->size;
        update = true;
        continue;
      } else break;
    } while (true);

    // Update the remoteFlushed address if needed.
    if (update) {
      Address old;
      LocalFile<handler_t, remote_t>::MonotonicUpdate(remoteFlushed,
                                            currRemoteFlushed, old);
    }
  }

  /// Context with all state required for when a flush to Azure Blob storage
  /// completes.
  class FlushContext : public IAsyncContext {
   public:
    /// Constructor for FlushContext.
    ///
    /// \param storageFile
    ///    Pointer to the StorageFile that issued the flush to remote
    ///    storage.
    /// \param local
    ///    Identifier for the local file (on SSD) that also holds the
    ///    data being flushed.
    /// \param page
    ///    Page within the local file that is being flushed.
    /// \param untilAddress
    ///    Address relative to the local file upto which the flush was
    ///    issued. The flush is assumed to be for data in the range
    ///    [pStart, untilAddress), where pStart is the starting address
    ///    of the page.
    /// \param data
    ///    Pointer to the data that is being flushed.
    /// \param dest
    ///    Logical address being flushed to.
    FlushContext(StorageFile<handler_t, remote_t>* storageFile, Address local,
                 uint64_t page, Address untilAddress, uint64_t dest, void* data)
      : storageFile(storageFile)
      , local(local)
      , page(page)
      , untilAddress(untilAddress)
      , dest(dest)
      , data(data)
    {}

   protected:
    /// Copies the state from this context to the heap. Memory
    /// allocation is performed internally. Usefull for when an
    /// operation goes asynchronous.
    Status DeepCopy_Internal(IAsyncContext*& context) {
      return IAsyncContext::DeepCopy_Internal(*this, context);
    }

   public:
    /// Pointer to the StorageFile that issued the flush to Azure
    /// blob storage.
    StorageFile<handler_t, remote_t>* storageFile;

    /// Address offset of the local file (on SSD) that holds the page being
    /// flushed to Azure blob storage.
    Address local;

    /// The page within the local file being flushed to Azure blob
    /// storage.
    uint64_t page;

    /// The address upto which the flush was issued. Note that this
    /// address is not relative to the page being flushed. It is the
    /// address relative to the file this page belongs to. Think of
    /// the flush being on the address range [pageStart, untilAddress).
    Address untilAddress;

    /// Logical address being flushed to. Required for cases when blob
    /// store is overloaded and we need to reissue this flush.
    uint64_t dest;

    /// Pointer to the data for which the write was issued. This should
    /// be freed by the callback that uses the context.
    void* data;
  };

  /// Callback for when a page is successfully flushed to Azure Blob storage.
  ///
  /// \param ctxt
  ///    Pointer to context with all required state.
  /// \param result
  ///    Placeholder to conform to FASTER's cc::AsyncIOCallback.
  /// \param len
  ///    Placeholder to conform to FASTER's cc::AsyncIOCallback.
  static void Flush(IAsyncContext* ctxt, Status result, size_t len) {
    CallbackContext<FlushContext> context(ctxt);

    // If the original flush failed, then reissue to DFS.
    if (result == Status::Aborted) {
      logMessage(Lvl::DEBUG, "Reissuing flush of %lu B at address %lu to DFS",
                 len, context->dest);

      auto f = FlushContext(context->storageFile, context->local, context->page,
                            context->untilAddress, context->dest,
                            context->data);
      context->storageFile->remote.WriteAsync(
                            reinterpret_cast<uint8_t*>(context->data), (uint32_t) len,
                            context->dest, Flush, f);
      return;
    }

    auto handle =
      context->storageFile->localFiles.find(context->local.control());
    if (handle == context->storageFile->localFiles.end()) return;

    // First, update metadata on the local file.
    LocalFile<handler_t, remote_t>* local = handle->second;
    LocalFile<handler_t, remote_t>::FlushLocal(local, context->page,
                                               context->untilAddress);

    // If the local file has flushed fully, mark it as Flushed.
    if (local->flushedOffset.load() == local->size) {
      FlushCloseStatus oldS = local->status.load();
      FlushCloseStatus newS = FlushCloseStatus(FlushStatus::Flushed,
                                               oldS.close);
      while (local->status.compare_exchange_weak(oldS, newS));
    }

    delete [] reinterpret_cast<uint8_t*>(context->data);

    // Next, try to update the remoteFlushed address on StorageFile.
    context->storageFile->UpdateRemoteFlushed();
  }

  /// Context with all state required to evict a bunch of files from
  /// local SSD. Used by EvictFiles() which is in-turn invoked by the
  /// Epoch manager.
  class FileEvictionContext : public IAsyncContext {
   public:
    /// Constructor.
    ///
    /// \param storageFile
    ///    Pointer to the StorageFile from which local files need to
    ///    be evicted.
    /// \param newSafeRemoteOffset
    ///    New value of safeRemoteOffset that should be set on the
    ///    the StorageFile.
    FileEvictionContext(StorageFile<handler_t, remote_t>* storageFile,
                        Address newSafeRemoteOffset)
      : storageFile(storageFile)
      , newSafeRemoteOffset(newSafeRemoteOffset)
    {}

   protected:
    /// Method that copies an instance of this class from the stack
    /// to the heap. Required when the callback using this context
    /// goes asynch on the epoch manager.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) final {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

   public:
    /// Pointer to the StorageFile containing files to be evicted
    /// from local SSD.
    StorageFile<handler_t, remote_t>* storageFile;

    /// The value that safeRemoteOffset should be monotonically updated
    /// to once the callback completes.
    Address newSafeRemoteOffset;
  };

  /// Evicts files from local SSD. Basically a callback for the Epoch
  /// manager for when all threads have observed a particular remoteOffset.
  ///
  /// \param ctxt
  ///    Context with all necessary state.
  static void EvictFiles(IAsyncContext* ctxt) {
    CallbackContext<FileEvictionContext> context(ctxt);

    // Try to update the safeRemoteOffset.
    Address oldSafeRemoteOffset;
    if (!LocalFile<handler_t, remote_t>::MonotonicUpdate(context->storageFile->safeRemoteOffset,
                                                         context->newSafeRemoteOffset,
                                                         oldSafeRemoteOffset))
    {
      return;
    }

    // If the update went through, then mark corresponding files as
    // closed, and delete those that are also flushed.
    for (Address file = oldSafeRemoteOffset;
         file < context->newSafeRemoteOffset;
         file += context->storageFile->kFileSize)
    {
      auto handle = context->storageFile->localFiles.find(file.control());
      if (handle == context->storageFile->localFiles.end()) return;

      FlushCloseStatus oldS = handle->second->status.load();
      FlushCloseStatus newS(oldS.flush, CloseStatus::Closed);

      // Mark the file closed.
      while (handle->second->status.compare_exchange_weak(oldS, newS));

      // If the file was also flushed, then delete it. Technically, this
      // check is not needed as the safeRemoteOffset will never move past
      // remoteFlushed.
      if (oldS.flush == FlushStatus::Flushed) {
        handle->second->Close();
        handle->second->Delete();
        delete(handle->second);

        // Since remoteOffset has moved, it is ok to remove the file
        // from the concurrent hashmap.
        context->storageFile->localFiles.unsafe_erase(file.control());
      }
    }
  }

  /// Shifts the remoteOffset.
  ///
  /// \param tailFile
  ///    Address of the new tail.
  void ShiftRemoteOffset(Address tailFile) {
    // Snapshot the remote offset and remote flushed addresses.
    Address currRemoteOffset = remoteOffset.load();
    Address currRemoteFlushed = remoteFlushed.load();

    // Corner case wherin the StorageFile has not grown large enough yet.
    if (tailFile < kDistance) return;

    // The desired remote offset. Always at a fixed distance from the tail.
    Address desiredRemoteOffset = tailFile - kDistance;

    // The desired remote offset cannot be more that remote flushed.
    // Otherwise, Blob store will receive reads for data that has not
    // been flushed to it yet.
    if (desiredRemoteOffset > currRemoteFlushed) {
      desiredRemoteOffset = currRemoteFlushed;
    }

    // Update the remote offset to the desired remote offset, and then
    // bump the epoch. When the epoch becomes safe, files with data
    // between safeRemoteOffset and remoteOffset will get deleted.
    Address oldRemoteOffset;
    if (LocalFile<handler_t, remote_t>::MonotonicUpdate(remoteOffset,
                                              desiredRemoteOffset,
                                              oldRemoteOffset))
    {
      FileEvictionContext ctxt(this, desiredRemoteOffset);
      IAsyncContext *copy = nullptr;
      ctxt.DeepCopy(copy);
      epoch->BumpCurrentEpoch(EvictFiles, copy);
    }
  }

 public:
  /// Concurrent hashmap holding all LocalFiles that are currently
  /// on local SSD. The hash map is indexed by a file identifier.
  concurrent_unordered_map<uint64_t, LocalFile<handler_t, remote_t>*> localFiles;

  /// Safe remoteOffset that has been seen by every thread. All records
  /// below this can be safely deleted from local SSD because they are
  /// on Blob storage, and are not being accessed by any thread.
  AtomicAddress safeRemoteOffset;

 private:
  /// The remote file consisting of the part of the log stored on
  /// Azure blob storage.
  remote_t remote;

  /// Logical address below which records will have to be looked up
  /// from the remote file. This needs to be atomic because it can
  /// be updated on calling WriteAsync() while threads are still
  /// reading it from ReadAsync().
  AtomicAddress remoteOffset;

  /// Logical address upto which all data has been durably flushed to the
  /// remote file (Blob storage).
  AtomicAddress remoteFlushed;

  /// Current local file to which writes are being performed. This need not
  /// be atomic since all updates will be performed to it by WriteAsync()
  /// only which never gets invoked concurrently.
  AtomicAddress tailFile;

  /// The file prefix on every local file created by this class.
  std::string baseName;

  /// The IO handler that local files will schedule their IO operations on.
  handler_t* ioHandler;

  /// Pointer to Epoch manager. Required when shifting remoteOffset.
  LightEpoch* epoch;

  /// The number of bits corresponding to the size of each file on local SSD.
  uint64_t kFileBits;

  /// The size of each file on local SSD.
  uint64_t kFileSize;

  /// The total number of files that can be stored on local SSD.
  /// The HybridLog is basically split across all of these files.
  uint64_t kNumFiles;

  /// The distance between "remoteOffset" and "tailFile".
  /// StorageFile will allways maintain this distance.
  uint64_t kDistance;

  /// The total number of bytes that can be stored on local SSD.
  uint64_t kLocalSize;
};

/// A Simple wrapper over file creation. StorageDevice maintains state
/// allowing for file and directory (for checkpoints) creation under a root
/// path, and asynchronous IO operations on these created files.
template <class H, class R>
class StorageDevice {
 public:
  /// Typedef for convenience. Determines mechanism used to handle IO completions.
  typedef H handler_t;

  /// Typedef for convenience. Determines remote storage tier.
  typedef R remote_t;

  /// Typedef for convenience. Determines storage tier task.
  typedef typename remote_t::Task task_t;

  /// The following typedef is needed because FASTER only works with a file
  /// type called 'file_t'. I think the idea is to be able to plug in different
  /// storage implementations without changing any top level FASTER code in
  /// cc/faster.h.
  typedef StorageFile<handler_t, remote_t> log_file_t;

  /// File type for checkpoints etc. All stored on the local machine for now.
  typedef FileSystemFile<handler_t> file_t;

  /// Constructor for StorageDevice.
  ///
  /// Initializes (opens) a file that will hold the portion of the HybridLog
  /// below the safe read only offset.
  ///
  /// \param root
  ///    The root path under which all files will be created.
  /// \param lEpoch
  ///    Pointer to the epoch manager.
  /// \param id
  ///    16-bit identifier for the server. Required to identify the correct
  ///    file on the remote tier to send reads and writes to.
  /// \param remote
  ///    A connection string so that we can communicate with the remote layer.
  /// \param unBuffered
  ///    If set to true, writes to files will not be buffered by the underlying
  ///    operating system.
  /// \param deleteOnClose
  ///    If set to true, files created by the device will be deleted by the
  ///    underlying operating system once closed.
  StorageDevice(const std::string& root, LightEpoch& lEpoch,
                const std::string& remote, uint16_t id=1,
                bool unBuffered=false, bool deleteOnClose=false)
    : localRoot{ FormatPath(root) }
    , epoch{ &lEpoch }
    , unBuffered{ unBuffered }
    , deleteOnClose{ deleteOnClose }
    , ioHandler{ 8 }
    , hLog{ localRoot, &ioHandler, &lEpoch, &pendingTasks, id, remote,
            1024, 302, 30 }
  {
    if (hLog.Open() != Status::Ok) {
      logMessage(Lvl::ERR,
                 "Failed to open hybrid log file on Storage device");
      throw std::runtime_error("Failed to open file for hybrid log");
    }
  }

  /// Constructor for unit testing.
  ///
  /// \param lEpoch
  ///    Pointer to the epoch manager.
  StorageDevice(LightEpoch& lEpoch)
    : localRoot{ "C:\\faster\\" }
    , epoch{ &lEpoch }
    , unBuffered{ false }
    , deleteOnClose{ false }
    , ioHandler{ 8 }
    , hLog{ localRoot, &ioHandler, &lEpoch, &pendingTasks, 1,
            "UseDevelopmentStorage=true;" }
  {
    if (hLog.Open() != Status::Ok) {
      logMessage(Lvl::ERR,
                 "Failed to open hybrid log file on Storage device");
      throw std::runtime_error("Failed to open file for hybrid log");
    }
  }

  /// Move constructor. Required when initializing FASTER.
  ///
  /// \param from
  ///    The StorageDevice that has to be moved into the new object.
  StorageDevice(StorageDevice&& from)
    : localRoot{ std::move(from.localRoot) }
    , epoch{ std::move(from.epoch) }
    , unBuffered{ std::move(from.unBuffered) }
    , deleteOnClose{ std::move(from.deleteOnClose) }
    , ioHandler{ std::move(from.ioHandler) }
    , pendingTasks{ std::move(from.pendingTasks) }
    , hLog{ std::move(from.hLog) }
  {}

  /// Disallow copy and assignment constructors.
  StorageDevice(const StorageDevice&) = delete;
  StorageDevice& operator=(const StorageDevice&) = delete;

  /// This function returns the size of a sector on the underlying hardware.
  /// FASTER's log is aligned to this size to prevent extra data copies during
  /// async IO operations.
  uint32_t sector_size() const {
    return static_cast<uint32_t>(4096);
  }

  /// Returns a reference to the file holding the HybridLog (const version).
  const log_file_t& log() const {
    return hLog;
  }

  /// Returns a reference to the file holding the HybridLog (non-const version).
  log_file_t& log() {
    return hLog;
  }

  /// Returns the path of an index checkpoint relative to the root.
  std::string relative_index_checkpoint_path(const Guid& token) const {
    std::string path = "index-checkpoints";
    path += kPathSeparator;
    path += token.ToString();
    path += kPathSeparator;

    return path;
  }

  /// Returns the full path of an index checkpoint.
  std::string index_checkpoint_path(const Guid& token) const {
    return localRoot + relative_index_checkpoint_path(token);
  }

  /// Returns the path of a cpr checkpoint relative to the root.
  std::string relative_cpr_checkpoint_path(const Guid& token) const {
    std::string path = "cpr-checkpoints";
    path += kPathSeparator;
    path += token.ToString();
    path += kPathSeparator;

    return path;
  }

  /// Returns the full path of a cpr checkpoint.
  std::string cpr_checkpoint_path(const Guid& token) const {
    return localRoot + relative_cpr_checkpoint_path(token);
  }

  /// Creates a new directory to hold an index checkpoint. Any pre-existing
  /// directories are deleted.
  void CreateIndexCheckpointDirectory(const Guid& token) {
    std::string dir = index_checkpoint_path(token);
    std::experimental::filesystem::path path{ dir };
    std::experimental::filesystem::remove_all(path);
    std::experimental::filesystem::create_directories(path);
  }

  /// Creates a new directory to hold a cpr checkpoint. Any pre-existing
  /// directories are deleted.
  void CreateCprCheckpointDirectory(const Guid& token) {
    std::string dir = cpr_checkpoint_path(token);
    std::experimental::filesystem::path path{ dir };
    std::experimental::filesystem::remove_all(path);
    std::experimental::filesystem::create_directories(path);
  }

  /// Creates and returns a new file under the root directory.
  ///
  /// \param fileName
  ///    The new file's name.
  file_t NewFile(std::string fileName) {
    return file_t(localRoot + fileName, environment::FileOptions{});
  }

  /// Makes progress on any IO operations to local disk/SSD.
  bool TryComplete() {
    for (auto i = 0; i < pendingTasks.unsafe_size(); i++) {
      // Try to read a task from the pending queue.
      task_t task;
      if (!pendingTasks.try_pop(task)) break;

      // If the task has not completed yet, then enqueue it again.
      if (!task.isReady()) pendingTasks.push(task);
    }

    return ioHandler.TryComplete();
  }

  /// Complete all pending IO operations. Useful for testing.
  void CompletePending() {
    task_t task;
    while (pendingTasks.try_pop(task)) task.wait();
  }

  /// Returns a reference to the handler processing async IOs.
  handler_t& handler() {
    return ioHandler;
  }

  /// The size of an IO that will be issued to the remote layer.
  static const uint64_t maxRemoteIO = std::min(remote_t::maxWriteBytes(),
                                               Address::kMaxOffset + 1);

 private:
  /// Formats a given filesystem path.
  static std::string FormatPath(std::string path) {
    // Append a separator to the end of 'path' if not already present. Required
    // because the *_checkpoint_path() functions need to return a valid path to
    // an index/cpr checkpoint directory.
    //
    // The .empty() check is to prevent undefined behavior.
    if (path.empty() || path.back() != kPathSeparator[0]) {
      path += kPathSeparator;
    }

    return path;
  }

  /// Root path under which files and directories will be created.
  std::string localRoot;

  /// Pointer to the epoch manager. Required when records/pages/files are
  /// flushed from local to remote storage.
  LightEpoch* epoch;

  /// Flag indicating whether files created should be buffered by the underlying
  /// operating system. If true, then all created files will be unbuffered.
  bool unBuffered;

  /// Flag indicating whether created files should be deleted by the underlying
  /// operating system when closed. If true, then a file will be deleted once
  /// it is closed.
  bool deleteOnClose;

  /// A thread pool on which IO operations on all created files will be
  /// scheduled. Callbacks all for async IO operations execute here.
  handler_t ioHandler;

  /// List of asynchronous IOs to DFS that were successfully
  /// kicked off, but have not completed yet.
  concurrent_queue<task_t> pendingTasks;

  /// The file holding the HybridLog below the safe read only offset.
  log_file_t hLog;
};

} // namespace device
} // namespace FASTER
