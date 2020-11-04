// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include <memory>
#include <algorithm>

#ifdef _WIN32

#include <concurrent_queue.h>
template <typename T>
using concurrent_queue = concurrency::concurrent_queue<T>;

using dfs_conn_t = wchar_t;

#else

#include "tbb/concurrent_queue.h"
template <typename T>
using concurrent_queue = tbb::concurrent_queue<T>;

using dfs_conn_t = char;

#endif

#include <was/storage_account.h>
#include <was/blob.h>

#include <pplx/pplxtasks.h>
#include <cpprest/rawptrstream.h>

#include "core/thread.h"
#include "core/status.h"
#include "common/log.h"

using namespace azure::storage;
using namespace concurrency::streams;

using namespace FASTER::core;

namespace FASTER {
namespace device {

/// A "File" that is stored as a bunch of Azure page blobs on Azure
/// Blob Storage.
class BlobFile {
 public:
  /// Forward declaration for constructor.
  class Task;

  /// Constructor for BlobFile.
  ///
  /// \param connection
  ///    String with credentials required to access Azure storage.
  ///    By default this class communicates with a local Azure storage
  ///    emulator that ships with the SDK.
  ///    Example string:
  ///    "DefaultEndpointsProtocol=https;AccountName=your_storage_account;AccountKey=your_storage_account_key"
  /// \param queue
  ///    Queue on which to enqueue asynchronous IOs.
  /// \param id
  ///    Identifier for the file. All blobs will be stored under a Blob
  ///    container whose name is FASTERid. For example, if id is 6, then the
  ///    container's name will be FASTER6.
  /// \param blobSize
  ///    The maximum size a single blob can grow to. The BlobFile is
  ///    basically a "file" made up of multiple blobs. When a blob becomes
  ///    full, a new one is created. By default, the maximum blob size is
  ///    8 Tera Bytes.
  /// \param deleteOnClose
  ///    If true, the "file" is deleted once closed.
  BlobFile(const dfs_conn_t* connection,
           concurrent_queue<Task>* queue,
           uint16_t id=1, uint64_t blobSize=((uint64_t)1 << 40),
           bool deleteOnClose=true)
    : conn(utility::conversions::to_string_t(connection))
    , account()
    , client()
    , id(std::to_string((unsigned long)id))
    , blobMask(blobSize - 1)
    , pendingTasks(queue)
    , deleteOnClose(deleteOnClose)
    , alignment(512)
  {}

  /// Constructor for testing.
  ///
  /// \param queue
  ///    Queue on which to enqueue asynchronous IOs.
  BlobFile(concurrent_queue<Task>& queue)
    : conn(utility::conversions::to_string_t("UseDevelopmentStorage=true;"))
    , account()
    , client()
    , id(std::to_string((unsigned long)1))
    , blobMask(((uint64_t)1 << 31) - 1)
    , pendingTasks(&queue)
    , deleteOnClose(true)
    , alignment(512)
  {}

  /// Default constructor.
  BlobFile()
    : conn(utility::conversions::to_string_t("UseDevelopmentStorage=true;"))
    , account()
    , client()
    , id(std::to_string((unsigned long)1))
    , blobMask(((uint64_t)1 << 31) - 1)
    , pendingTasks(nullptr)
    , deleteOnClose(true)
    , alignment(512)
  {}

  /// Move constructor.
  BlobFile(BlobFile&& from)
    : conn(std::move(from.conn))
    , account(std::move(from.account))
    , client(std::move(from.client))
    , id(std::move(from.id))
    , blobMask(std::move(from.blobMask))
    , pendingTasks(std::move(from.pendingTasks))
    , deleteOnClose(std::move(from.deleteOnClose))
    , alignment(std::move(from.alignment))
  {}

  /// Move assignment operator.
  BlobFile& operator=(BlobFile&& from) {
    this->conn = std::move(from.conn);
    this->account = std::move(from.account);
    this->client = std::move(from.client);
    this->id = std::move(from.id);
    this->blobMask = std::move(from.blobMask);
    this->pendingTasks = std::move(from.pendingTasks);
    this->deleteOnClose = std::move(from.deleteOnClose);
    this->alignment = std::move(from.alignment);

    return *this;
  }

  // Disallow copy and assignment constructors.
  BlobFile(const BlobFile&) = delete;
  BlobFile& operator=(const BlobFile&) = delete;

  /// The maximum number of bytes that can be written through
  /// a single call to `WriteAsync()`. Azure page blobs support 4 MiB max.
  static constexpr uint32_t maxWriteBytes() {
    return 4 * 1024 * 1024;
  }

  /// Opens the file.
  ///
  /// Performs all initialization and setup on blob storage. After calling
  /// this function, data/records can be read from and written to this file.
  ///
  /// \return
  ///   Status::Ok on success. If the file already exists on Blob storage,
  ///   then Status::Aborted.
  Status Open() {
    try {
      logMessage(Lvl::INFO,
                 "Opening remote tier for server with id %s", id.c_str());

      // First, connect to Azure blob storage.
      account = cloud_storage_account::parse(conn);
      client = account.create_cloud_blob_client();

      // Next, create a container for the file.
      cloud_blob_container container =
          client.get_container_reference(
              utility::conversions::to_string_t("sofaster" + id));

      // Create the container if it does not exist already.
      if (!container.exists()) {
        container.create();
        logMessage(Lvl::INFO, "Created blob container sofaster%s", id.c_str());
      } else {
        logMessage(Lvl::INFO, "Reusing existing blob container sofaster%s",
                   id.c_str());
      }

      // Create enough blobs within the container to hold FASTER's entire
      // logical address space. Save references to these blobs for future
      // read/write operations. Blob store restricts me to 1 TB.
      uint64_t limit = ((uint64_t) 1) << 40;
      uint64_t maxBlobs = std::min(limit,
                                   ((uint64_t) 1 << lBits)) / (blobMask + 1);
      logMessage(Lvl::INFO,
                 "Creating %lu blobs under container sofaster%s", maxBlobs,
                 id.c_str());
      for (uint64_t blob = 0; blob < maxBlobs; blob++) {
        cloud_page_blob pBlob =
            container.get_page_blob_reference(
                utility::conversions::to_string_t(std::to_string((long)blob)));
        pBlob.create(blobMask + 1);
        logMessage(Lvl::INFO, "Created blob %lu of size %lu GB",
                   blob, (blobMask + 1) / (1 << 30));
      }

      return Status::Ok;
    } catch (const storage_exception& e) {
      // Azure can throw exceptions if arguments are invalid.
      logMessage(Lvl::ERR, "Received exception from blob store: %s",
                 e.what());

      // Print out an extended error message too.
      storage_extended_error err = e.result().extended_error();
      if (!err.message().empty()) {
        logMessage(Lvl::ERR, "Extended error message: %s",
                   err.message().c_str());
      }

      return Status::Aborted;
    }
  }

  /// Closes the file.
  ///
  /// If the file was initialized with deleteOnClose set to true, then all
  /// underlying blobs are deleted from blob storage.
  ///
  /// \return
  ///    Status::Ok on success.
  Status Close() {
    // Delete file/blobs if needed.
    if (deleteOnClose) return Delete();
    return Status::Ok;
  }

  /// Deletes the file by deleting all underlying blobs from blob storage.
  ///
  /// \return
  ///    Status::Ok on success.
  Status Delete() {
    try {
      // Delete the container and all blobs within.
      cloud_blob_container container =
          client.get_container_reference(
              utility::conversions::to_string_t("sofaster" + id));
      container.delete_container();
    } catch (const storage_exception& e) {
      // Azure can throw exceptions if arguments are invalid.
      logMessage(Lvl::ERR, "Failed to delete remote tier: %s",
                 e.what());

      // Print out an extended error message too.
      storage_extended_error err = e.result().extended_error();
      if (!err.message().empty()) {
        logMessage(Lvl::ERR, "Extended error message: %s",
                   err.message().c_str());
      }

      return Status::Aborted;
    }

    return Status::Ok;
  }

  /// Returns the alignment of data on Blob storage.
  size_t Alignment() const {
    return alignment;
  }

  /// Continuation for an asynchronous read/write issued to Azure Blob Storage.
  class Task {
   public:
    /// Constructor for Task.
    ///
    /// \param task
    ///    Handle returned by Azure SDK on issuing the asynchronous
    ///    operation to blob storage.
    /// \param fContext
    ///    Context required to process the continuation when it completes.
    /// \param length
    ///    Length of the read/write that was issued to Azure Blob Storage.
    /// \param callback
    ///    Upper level completion callback passed in by FASTER.
    Task(pplx::task<void> task, IAsyncContext& fContext,
         uint64_t length, AsyncIOCallback callback)
      : inner_task(task)
      , context(nullptr)
      , length(length)
      , callback(callback)
    {
      // Make a copy of the passed in context on the heap.
      fContext.DeepCopy(context);
    }

    // Default constructor.
    Task()
      : inner_task()
      , context()
      , length()
      , callback()
    {}

    /// Move constructor.
    Task(Task&& from)
      : inner_task(from.inner_task)
      , context(std::move(from.context))
      , length(std::move(from.length))
      , callback(std::move(from.callback))
    {}

    /// Copy constructor.
    Task(const Task& from)
      : inner_task()
      , context()
      , length()
      , callback()
    {
      inner_task = from.inner_task;
      context = from.context;
      length = from.length;
      callback = from.callback;
    }

    // Copy and assign constructor.
    Task& operator=(const Task& from) {
      if (this != &from) {
        this->inner_task = from.inner_task;
        this->context = from.context;
        this->length = from.length;
        this->callback = from.callback;
      }

      return *this;
    }

    /// Blocks until the Task has completed.
    void wait() {
      while (!isReady());
    }

    /// Checks if the Task has completed.
    ///
    /// \return
    ///    True if the Task completed. False otherwise.
    bool isReady() {
      // If the task has not completed yet, return false.
      if (!inner_task.is_done()) return false;

      // If the task has completed, then just invoke the callback
      // and return. Need to call get() to catch exceptions (if any).
      Status status = Status::Ok;
      try {
        inner_task.get();
      } catch (const storage_exception& e) {
        logMessage(Lvl::DEBUG,
                   "Blob store threw error '%s'. Marking IO as Aborted",
                   e.what());
        status = Status::Aborted;
      }

      callback(context, status, length);
      return true;
    }

   private:
    // Inner task returned by the Azure SDK.
    pplx::task<void> inner_task;

    // Context with all state required to process a task once it completes.
    IAsyncContext* context;

    // Length (in bytes) of the IO that was issued to Azure.
    uint64_t length;

    // Callback passed in by FASTER. Called after the task completes and has
    // been processed.
    AsyncIOCallback callback;
  };

  /// Kicks off an asynchronous read to blob storage. The result of the
  /// operation can be obtained by making a call to CompletePending().
  ///
  /// \param source
  ///    The logical address to read from. If this logical address belongs
  ///    to the local FASTER instance, then the 16 MSB bits should be set to
  ///    0. Otherwise, these 16 bits should identify the FASTER instance to
  ///    read from.
  /// \param len
  ///    The number of bytes to be read (starting from source).
  /// \param dest
  ///    Pointer to memory location into which the data must be read.
  /// \param callback
  ///    Callback to be invoked once the read completes.
  /// \param fContext
  ///    Context that should be passed into the above callback.
  ///
  /// \return
  ///    Status::Pending if the read operation was kicked off successfully.
  ///    Status::Aborted if a read on a migrated record failed because the
  ///    corresponding FASTER instance could not be found.
  Status ReadAsync(uint64_t source, uint64_t len, uint8_t* dest,
                   AsyncIOCallback callback, IAsyncContext& fContext)
  {
    // First, check if the read is for an address that does not belong to
    // this machine's logical address space. If it is not, then the record
    // was most likely migrated to this machine, and will have to be read
    // from a different container on blob storage.
    auto id = this->id;
    if ((source >> lBits) != 0) { 
      id = std::to_string(source >> lBits);
      source = source & Address::kMaxAddress;
    }

    try {
      // Next, get a reference to the page blob the source address resides on.
      // Dividing the address by the size of a blob gives us an identifier for
      // the blob to be looked up.
      uint64_t blobNum = source / (blobMask + 1);
      cloud_blob_container container =
          client.get_container_reference(
              utility::conversions::to_string_t("sofaster" + id));
      cloud_page_blob blob =
          container.get_page_blob_reference(
              utility::conversions::to_string_t(std::to_string((long)blobNum)));

      // Next, get the page number within the blob. Using the blobMask
      // gives us a byte offset within the blob. This then needs to be
      // aligned to a page boundary.
      uint64_t page = (source & blobMask) & ~(alignment - 1);

      // Finally, get the total number of pages to be read from blob store.
      // Since we can perform reads only at page boundaries, some extra bytes
      // will have to be read (The maximum number of extra bytes will be 2*511
      // which is about 1 KB.
      uint64_t nPages = (len & (alignment - 1)) == 0 ?
                         len / alignment : (len / alignment) + 1;

      ostream stream = rawptr_stream<uint8_t>::open_ostream(dest, len);

      // Kick off the async read and add it to the list of inprogress tasks.
      pendingTasks->push(
        Task(blob.download_range_to_stream_async(stream,
        page, nPages * alignment), fContext, len, callback)
      );
    } catch (const storage_exception& e) {
      // Azure can throw exceptions if arguments are invalid.
      logMessage(Lvl::ERR,
                 "Failed to read %lu bytes at address %lu from dfs: %s",
                 len, source, e.what());

      // Print out an extended error message too.
      storage_extended_error err = e.result().extended_error();
      if (!err.message().empty()) {
        logMessage(Lvl::ERR, "Extended error message: %s",
                   err.message().c_str());
      }

      return Status::Aborted;
    }

    return Status::Ok;
  }

  /// Kicks off an asynchronous write operation to blob storage. The
  /// result of the operation can be obtained by making a call to
  /// CompletePending().
  ///
  /// \param source
  ///    Pointer to the sequence of bytes to be written. The caller
  ///    should make sure that this pointer stays valid for the duration
  ///    of the entire operation.
  /// \param len
  ///    The number of bytes (starting from source) to be written.
  /// \param dest
  ///    The destination logical address (on the HybridLog) to be written to.
  ///    The caller should make sure that dest is a multiple of 512.
  /// \param callback
  ///    Callback to be invoked once the write to blob storage completes.
  /// \param fContext
  ///    Context to be passed into the above callback.
  ///
  /// \return
  ///    Status::Pending if the write was kicked off successfully.
  ///    Status::Aborted if the write was too large (> 4 MB) or if dest was
  ///    not a multiple of 512.
  Status WriteAsync(const uint8_t* source, uint32_t len, uint64_t dest,
                    AsyncIOCallback callback, IAsyncContext& fContext)
  {
    // Reject writes that are larger than 4 MB (Blob storage limitation),
    // and writes to addresses that are not aligned to 512 Bytes.
    if (len > (uint64_t)(4 << 20) || (dest & (alignment - 1)) != 0) {
      logMessage(Lvl::ERR,
                 "Bad alignment when writing %lu B to address %lu on dfs",
                 len, dest);
      return Status::Aborted;
    }

    try {
      // First, get a reference to the blob the destination address falls in.
      // Dividing the destination address by the size of a blob gives us an
      // identifier for the blob to lookup.
      uint64_t blobNum = dest / (blobMask + 1);
      cloud_blob_container container =
          client.get_container_reference(
              utility::conversions::to_string_t("sofaster" + this->id));
      cloud_page_blob blob =
          container.get_page_blob_reference(
              utility::conversions::to_string_t(std::to_string((long)blobNum)));


      // Next, get the page number within the blob. Using the blobMask
      // gives us a byte offset within the blob. This then needs to be
      // aligned to a page boundary.
      uint64_t page = (dest & blobMask) & ~(alignment - 1);

      // Finally, create a stream from the supplied pointer that will be
      // used to perform the asynchronous write to blob storage.
      istream stream = rawptr_stream<uint8_t>::open_istream(source, len);

      // Issue the asynchronous task and then add it to the list of
      // pending tasks.
      pendingTasks->push(
        Task(blob.upload_pages_async(stream, page,
          utility::conversions::to_string_t("")), fContext,
          len, callback)
      );
    } catch (const storage_exception& e) {
      // Azure can throw exceptions if arguments are invalid.
      logMessage(Lvl::ERR,
                 "Failed to write %lu bytes to address %lu on dfs: %s",
                 len, dest, e.what());

      // Print out an extended error message too.
      storage_extended_error err = e.result().extended_error();
      if (!err.message().empty()) {
        logMessage(Lvl::ERR, "Extended error message: %s",
                   err.message().c_str());
      }

      return Status::Aborted;
    }

    return Status::Ok;
  }

 private:
  /// The number of bits making up a FASTER logical address.
  static constexpr uint64_t lBits = 48;

  /// A connection string with credentials required to connect to an Azure
  /// storage account. Required during initialization.
  utility::string_t conn;

  /// Represents an Azure storage account. Required during initialization.
  /// This account can be used to get interfaces to Azure blob, queue, and
  /// table services. An entire FASTER cluster is currently associated with
  /// a single storage account.
  cloud_storage_account account;

  /// An interface to Azure blob storage. Required to access blobs across the
  /// entire FASTER cluster.
  cloud_blob_client client;

  /// Identifier for this FASTER instance. A FASTER instance's HybridLog is
  /// stored inside a unique container on Blob Storage. This member helps
  /// identify the container. Required during initialization.
  std::string id;

  /// Bitmask used to identify the offset of a logical address within a blob.
  /// Given a logical address 'l' that falls inside a FASTER instance's 48
  /// bit address space and a blob 'b', 'l' & 'b' gives the offset within 'b'
  /// that should be looked up to read 'l'.
  uint64_t blobMask;

  /// List of asynchronous reads to blob storage that were successfully
  /// kicked off, but have not completed yet.
  concurrent_queue<Task>* pendingTasks;

  /// If true, all blobs are deleted on Azure blob store when this "file"
  /// is closed.
  bool deleteOnClose;

  /// The blob storage alignment boundary. All reads and writes to blob
  /// storage must be for addresses that are a multiple of this value.
  /// Additionally, the number of bytes read or written must also be a
  /// multiple of this value.
  uint64_t alignment;
};

} // namespace device
} // namespace FASTER
