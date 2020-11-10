// Copyright (c) University of Utah. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include "dispatch.h"
#include "migration.h"
#include "session/server.h"

#include "core/guid.h"
#include "core/light_epoch.h"
#include "environment/file.h"
#include "device/file_system_disk.h"

#include "client/sofaster.h"

#include <boost/thread/locks.hpp>
#include <boost/thread/shared_mutex.hpp>

namespace SoFASTER {
namespace server {

/// If set to true, acquire an ownership lock before processing a batch of
/// requests. Used to benchmark views. Ideally should be inside dispatch,
/// but no clean way of doing so.
bool lock = false;
boost::shared_mutex _ownership;

/// Worker takes in the following template arguments:
///    F: The type on the FASTER instance used by Worker to serve reads,
///       upserts, and read-modify-writes.
///    K: The type on the key of each record. Required to correctly unpack
///       RPCs received from a client.
///    V: The type on the value of rmws and upserts. Required to correctly
///       unpack RPCs received from a client.
///    T: The type on the transport layer that will be used to receive RPCs
///       and send out responses.
template<class F, class K, class V, class R, class T>
class Worker {
 public:
  // For convenience. Defines the type on the FASTER key value store.
  typedef F faster_t;

  // For convenience. Defines the type on the transport layer (ex: Infrc).
  typedef T transport_t;

  // For convenience. Defines the type on the connection exposed
  // by the transport layer.
  typedef typename T::Connection connection_t;

  // For convenience. Defines the type on the connection handler exposed
  // by the transport layer.
  typedef typename T::ConnectHandler conn_poller_t;

  // For convenience. Defines the type on the RPC poller exposed by the
  // transport layer.
  typedef typename T::Poller rpc_poller_t;

  // For convenience. Defines the type on the async IO handler that will be
  // by the filesystem that stores all metadata.
  typedef FASTER::environment::QueueIoHandler handler_t;

  // For convenience. Defines the type on the filesystem used by the sessions
  // layer to durably store all metadata. Only one segment per file.
  typedef FASTER::device::FileSystemDisk<handler_t, 1073741824ull> disk_t;

  // For convenience. Defines the type on the sessions layer that requests will
  // be received on.
  typedef session::ServerSession<transport_t, disk_t, faster_t> session_t;

  // For convenience. Defines the type on the migration object used by this
  // class to make progress on data transfers to a target server.
  typedef Migration<faster_t, K, V, transport_t> migration_t;

  // Typedef on client library. Required for when we want to benchmark server
  // side hash validation.
  typedef Sofaster<K, V, T> sofaster_t;

  /// Constructs a Worker.
  ///
  /// \param faster
  ///    Pointer to an instance of FASTER that is capable of servicing reads,
  ///    upserts, and read-modify-writes.
  /// \param lPort
  ///    The network port on which this worker will listen for incoming
  ///    sessions.
  /// \param lIP
  ///    The IP address the this worker must listen to for connections.
  /// \param samples
  ///    The number of samples (in bytes) to move to the target during migration
  /// \param view
  ///    The view number this worker should operate in.
  Worker(faster_t* faster, uint16_t lPort, std::string& lIP,
         uint32_t samples, std::string& path, uint64_t view=0)
   : faster(faster)
   , guid()
   , incomingSessions()
   , currentView(view)
   , migration(nullptr)
   , deleteMigration(false)
   , connPoller()
   , rpcPoller()
   , transport()
   , epoch(1)
   , metadisk(path + FASTER::environment::kPathSeparator + std::to_string(lPort),
              epoch)
   , sampleLimit(samples)
   , ranges()
  {
    connPoller = std::move(conn_poller_t(lPort, lIP, &transport));
    rpcPoller = std::move(rpc_poller_t(&transport));

    // If we're benchmarking server hash validation, then fill up the workers
    // mappings based on the number of configured splits.
    if (splits > 0) {
      uint64_t split = (~0UL) / splits;
      uint64_t lHash = 0UL;
      uint64_t rHash = lHash + split;

      for (auto i = 0; i < splits; i++) {
        ranges.emplace(
          std::piecewise_construct,
          std::forward_as_tuple(KeyHash(lHash), KeyHash(rHash)),
          std::forward_as_tuple(0)
        );

        lHash = rHash + 1;
        rHash = rHash + split;
      }
    }
  }

  /// Destroys a Worker.
  ~Worker() {
    faster->StopSession();
    if (migration) delete migration;
  }

  /// Disallow copy and copy-assign constructors.
  Worker(const Worker& from) = delete;
  Worker& operator=(const Worker& from) = delete;

  /// Starts/Opens a session to FASTER. A flag indicates whether we should
  /// pre-initialize a migration to a target machine.
  void init(bool connTarget) {
    guid = faster->StartSession();

    // Add a migration if indicated by the flag. Hash range will be set
    // correctly once the actual migration begins.
    std::string targetIP("192.168.0.2");
    if (connTarget) addMigration(targetIP, 22790 + Thread::id() - 1,
                                 KeyHash(0UL), KeyHash(0UL));
  }

  /// Checks for and completes any pending work.
  void poll() {
    // First, poll for an incoming connection. If a connection was received,
    // then create a new incoming session on the worker.
    connection_t* conn = connPoller.poll();
    if (conn) {
      incomingSessions.emplace_back(currentView, metadisk, faster, conn);
    }

    // Next, try to receive incoming RPCs. The RPC poller will take care of
    // enqueuing any received RPC on the appropriate session/connection.
    rpcPoller.poll();

    // Refresh the epoch before processing requests on each session.
    faster->Refresh();
    faster->CompletePending();

    // Next, for every incoming session, make progress on all received RPCs.
    auto it = incomingSessions.begin();
    while (it != incomingSessions.end()) {
      // Delete disconnected/closed sessions on which there aren't any pendings.
      if (it->status != session::Status::INITIALIZING &&
          it->status != session::Status::OPEN)
      {
        if (it->nPendings == 0) {
          logMessage(Lvl::DEBUG, "Deleting disconnected session %s",
                     it->sessionUID.ToString().c_str());
          it = incomingSessions.erase(it);
        } else {
          it++;
        }

        continue;
      }

      try {
        DispatchContext<session_t> context(&(*it), currentView,
                                           reinterpret_cast<void*>(&ranges));

        // If we're validating lock based ownership, then acquire a lock before
        // processing a batch. This should not be an issue wrt fairness since
        // we're always taking a shared lock and benchmarking under saturation.
        if (!lock) {
          it->poll(dispatch<K, V, R, session_t>,
                   reinterpret_cast<void*>(&context));
        } else {
          boost::shared_lock<boost::shared_mutex> lock(_ownership);
          it->poll(dispatch<K, V, R, session_t>,
                   reinterpret_cast<void*>(&context));
        }
      } catch (std::runtime_error&) {
        logMessage(Lvl::DEBUG, "Detected disconnected session %s",
                   it->sessionUID.ToString().c_str());
      }

      it++;
    }

    // Check if the migration needs to be deleted. If so, wait until all
    // responses are received and then delete.
    if (deleteMigration) {
      if (migration->completeRecv()) {
        delete migration;

        migration = nullptr;
        deleteMigration = false;
      }
    }

    // Try to make progress on migration if one exists and is not going
    // to be deleted.
    if (migration && !deleteMigration) migration->poll();
  }

  /// Adds a new migration to the worker. This will open up a session to
  /// a thread on the target -- identified by an IP address (\param ip) and
  /// port number (\param port) -- over which data will be transferred. Once
  /// added, this worker will regularly make progress on the migration.
  void addMigration(std::string& ip, uint16_t port, KeyHash lHash,
                    KeyHash rHash)
  {
    // Looks like there already is an in-progress migration? This should
    // not happen unless we pre-initialized the migration during init.
    if (migration) {
      migration->setHashRange(lHash, rHash);
      return;
    }

    migration = new migration_t(connPoller, &rpcPoller, ip,
                                std::to_string(port), faster,
                                lHash, rHash, sampleLimit);
  }

  /// Deletes the currently running migration (if one existed to begin with).
  void delMigration() {
    // No migration to begin with! Just return for now.
    if (!migration) return;

    deleteMigration = true;
  }

  /// Updates the workers view to the passed in view number.
  void changeView(uint64_t newView) {
    if (newView > currentView) currentView = newView;
  }

 private:
  /// Pointer to an instance of FASTER that will be used to service reads,
  /// upserts and read-modify-writes.
  faster_t* faster;

  /// Unique identifier for the worker. Received on opening a session to
  /// the FASTER key-value store.
  FASTER::core::Guid guid;

  /// Vector of incoming sessions that RPCs (requests) will be received on.
  std::vector<session_t> incomingSessions;

  /// The cluster view number that this worker is currently operating in.
  uint64_t currentView;

 public:
  /// A pointer to an in-progress migration (if one exists).
  migration_t* migration;

 private:
  /// Boolean indicating whether the migration has been completed and should
  /// be deleted from this worker thread.
  bool deleteMigration;

  /// Connection poller. All incoming client connections will be received here.
  conn_poller_t connPoller;

  /// RPC poller. All incoming RPC requests will be received and enqueued on
  /// the appropriate connection here.
  rpc_poller_t rpcPoller;

  /// Transport layer used by this worker.
  transport_t transport;

  /// Dummy epoch manager. This is passed in to the constructor of a file
  /// system that will be used to store all sessions related metadata.
  FASTER::core::LightEpoch epoch;

  /// The disk/filesystem under which all session related metadata must be
  /// durably stored.
  disk_t metadisk;

  /// The maximum size of the sampled set in bytes that can be moved over to
  /// the target during migration.
  uint32_t sampleLimit;

  /// Maps a hash range to a number. Used to add in overhead of hash range
  /// checks on the server for benchmarking.
  std::map<typename sofaster_t::HashRange,
           size_t,
           typename sofaster_t::CompareRanges> ranges;
};

} // end namespace server
} // end namespace SoFASTER
