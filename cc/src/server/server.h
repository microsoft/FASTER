// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <thread>

#include "admin.h"
#include "worker.h"
#include "dispatch.h"

#include "migration.h"

#include "environment/file.h"

#include "core/faster.h"
#include "core/light_epoch.h"

/// Uncomment the below line or pass in as a compile-time argument to enable
/// the blob store layer with SoFASTER.
/// #define USE_AZURE 1
#ifndef USE_AZURE
#include "device/file_system_disk.h"
#else
#include "device/azure.h"
#include "device/storage.h"
#endif

namespace SoFASTER {
namespace server {

/// This class defines a SoFASTER server.
///
/// Template arguments:
///    K: The type on the key for each record stored on the server.
///    V: The type on the value of upserts and rmws received from clients.
///    R: The type on the records stored by the server.
///    T: the type on the transport layer used by the server.
template<class K, class V, class R, class T>
class Server {
 public:
  // Defines the type on the IO handler used by FASTER to handle asynchronous
  // reads and writes issued to records that lie below the head of the log.
  typedef FASTER::environment::QueueIoHandler handler_t;

  // Defines the type on the disk used by FASTER to store the log below the
  // head address.
#ifndef USE_AZURE
  typedef FASTER::device::FileSystemDisk<handler_t, 1073741824ull> disk_t;
#else
  typedef FASTER::device::StorageDevice<handler_t,
                                        FASTER::device::BlobFile> disk_t;
#endif

  // Defines the type on FASTER.
  typedef FASTER::core::FasterKv<K, R, disk_t> faster_t;

  // Defines the type on the worker that will run on each SoFASTER thread.
  typedef Worker<faster_t, K, V, R, T> worker_t;

  // Defines the type on the epoch manager. Required by worker_t.
  typedef FASTER::core::LightEpoch epoch_t;

  // For convenience. Defines the type on Server (this class).
  typedef Server<K, V, R, T> server_t;

  /// Constructs a multi-threaded server capable of using the FASTER key-value
  /// store to serve remote clients.
  ///
  /// \param nThreads
  ///    The number of threads that the server should create to serve requests.
  /// \param hashTableEntries
  ///    The number of hash table entries the key-value store must be
  ///    configured with.
  /// \param logSize
  ///    The size of the key-value store's hybrid log.
  /// \param path
  ///    The path on local disk under which the hybrid log must be stored.
  /// \param basePort
  ///    The base port on which workers listen for incoming connections. Worker
  ///    i listens to basePort + i.
  /// \param lIP
  ///    The IP address the workers must listen to for connections.
  /// \param adminPort
  ///    The port on which the server will receive admin related RPCs such
  ///    as migrate() requests etc.
  /// \param samples
  ///    The number of samples (in bytes) to move to the target during migration
  /// \param id
  ///    A 16 bit, non-zero, positive identifier for the server.
  /// \param dfs
  ///    A connection string that will be used to establish a connection,
  ///    send and receive data with a DFS storage layer.
  /// \param mutableFrn
  ///    The fraction of Faster's in-memory log that should be mutable.
  Server(size_t nThreads, uint64_t hashTableEntries, uint64_t logSize,
         const std::string& path, uint16_t basePort, std::string& lIP,
         uint16_t adminPort, uint32_t samples, uint16_t id, std::string& dfs,
         double mutableFrn, bool indirection, bool strictPending, int nSplits,
         bool ownershipLock)
   : store(hashTableEntries, logSize,
           path + FASTER::environment::kPathSeparator + "hlog", id,
           dfs, mutableFrn)
   , nThreads(nThreads)
   , basePort(basePort)
   , ip(lIP)
   , adminPort(adminPort)
   , threads()
   , workers()
   , sampleLimit(samples)
   , id(id)
   , path(path)
  {
    if (!indirection) issueIO = true;
    if (strictPending) strict = true;
    splits = nSplits;
    if (ownershipLock) lock = true;
  }

  /// Destroys a Server.
  ~Server() {}

  /// Disallow copy and copy-assign constructors.
  Server(const Server& from) = delete;
  Server& operator=(const Server& from) = delete;

  /// Starts up the server. On calling this function, workers will be created,
  /// and will start servicing requests. A boolean flag determines whether the
  /// server should open sessions to a target machine on init. `pinOffset`
  /// determines the number of cores by which a worker's affinity must
  /// be offset. This is required for cases where we need cores dedicated to
  /// handle SoftIRQs.
  void run(bool connTarget, uint32_t pinOffset) {
    // Pre-construct all instance of Worker.
    for (size_t i = 0; i < nThreads; i++) {
      workers.emplace_back(new worker_t(&store, basePort + i, ip, sampleLimit,
                                        path));
    }

    // Startup worker threads and wait for them to complete.
    for (size_t i = 0; i < nThreads; i++) {
      threads.emplace_back(&runThread, this, (i == 0), connTarget, pinOffset);
    }

    for (size_t i = 0; i < nThreads; i++) {
      threads[i].join();
    }

    // Delete all instances of Worker.
    for (size_t i = 0; i < nThreads; i++) {
      delete workers[i];
    }
  }

 private:
  /// Run on each worker thread.
  ///
  /// \param server
  ///    Pointer to the instance of Server this thread belongs to.
  /// \param adminF
  ///    Flag indicating whether this thread should also run an admin.
  /// \param connTarget
  ///    Flag indicating whether this worker should open a connection
  ///    to a target machine on init.
  /// \param pinOffset
  ///    Number of cores by which this thread's affinity must be offset.
  static void runThread(server_t* server, bool adminF, bool connTarget,
                        uint32_t pinOffset)
  {
    // Thread::id() starts from 1. Since we need an index into an array,
    // subtract one from it.
    auto id = Thread::id() - 1;

    // Pin the thread and initialize the worker.
    pinThread(id + pinOffset);
    server->workers[id]->init(connTarget);

    // If we're running an admin on this thread, then initialize that too.
    typedef Admin<faster_t, K, V, R, T> admin_t;
    admin_t* admin = nullptr;
    if (adminF) admin = new admin_t(&server->workers[0], &server->store,
                                    server->adminPort, server->ip);

    while (true) {
      server->workers[id]->poll();
      if (admin) admin->poll();
    }
  }

  /// Pins the calling thread to the passed in core.
  static void pinThread(size_t coreId) {
    cpu_set_t mask;
    CPU_ZERO(&mask);

    CPU_SET(coreId, &mask);
    sched_setaffinity(0, sizeof(mask), &mask);
  }

  /// The FASTER key-value store. Workers will issue received operations
  /// against this storage system.
  faster_t store;

  /// The number of worker threads that the server should run.
  size_t nThreads;

  /// The base port that workers listen for incoming connections on. Worker
  /// i listens on port basePort + i.
  uint16_t basePort;

  /// The ip address that workers listen to for incoming connections.
  std::string ip;

  /// The port on which the admin will listen for control RPCs (like migrate).
  uint16_t adminPort;

  /// The list of threads created by the server.
  std::vector<std::thread> threads;

  /// The list of workers created by the server.
  std::vector<worker_t*> workers;

  /// The maximum size of the sampled set in bytes that can be moved over to
  /// the target during migration.
  uint32_t sampleLimit;

  /// A 16 bit identifier for the server. Used to store data on DFS.
  uint16_t id;

  /// Filesystem path under which the log and sessions metadata will be stored.
  std::string path;
};

} // end namespace server.
} // end namespace SoFASTER.
