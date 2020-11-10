// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include <chrono>
#include <vector>
#include <random>
#include <fstream>
#include <iostream>
#include <algorithm>

#include "type.h"
#include "util.h"

#include "core/async.h"
#include "core/status.h"
#include "core/auto_ptr.h"
#include "client/sofaster.h"

#include "boost/program_options.hpp"

using namespace std;
using namespace SoFASTER;
using namespace FASTER::core;
using namespace boost::program_options;

/// The number of keys to be filled by this client into SoFASTER (initialized
/// to 250 Million), and an array to store them.
uint64_t initKCount = 250 * 1000 * 1000;
aligned_unique_ptr_t<uint64_t> fillKeys;

/// The number of txns (requests) that this client must issue to SoFASTER (
/// initialized to 1 Billion), and an array to store keys for them.
uint64_t txnsCount = 1000 * 1000 * 1000;
aligned_unique_ptr_t<uint64_t> txnsKeys;

/// The number of transmit buffers that will be sent to SoFASTER concurrently.
/// Initialized to 1, but reset when parsing command line options.
uint64_t pipelineDepth = 1;

/// A boolean flag indicating whether we should periodically sample thrpt.
/// Initialized to false, but reset when parsing command line options.
bool sample = false;

/// Length of experiment in seconds. Initialized to 360, reset when parsing
/// command line options below.
uint64_t exptTimeSec = 360;

/// Measurement interval in milliseconds. Initialized to 100, reset when parsing
/// command line options below.
uint64_t measureInMs = 100;

/// The list of servers the hash space is initially divided across.
/// Initialized to empty, reset when parsing command line options below.
std::vector<std::string> servers;

/// Port that worker 0 on a server listens to. Worker i listens on a port
/// number offset by i.
uint16_t serverBasePort = 22790;

/// The number of client threads running the workload. Initialized to 32, reset
/// when parsing command line options below.
uint64_t threads = 32;

/// The percentage of operations that will be reads. Initialized to 0, reset
/// when parsing command line options below.
uint32_t readPct = 0;

/// Flag indicating whether we should generate workload keys using a PRNG
/// (true) or whether we should use pre-generated ones (false).
bool generateKeys = false;

/// Number of cores by which a thread must be offset when pinned. Thread `i`
/// will be pinned to core `i` + pinOffset. This is required for TCP where
/// a few cores on socket 0 need to be dedicated for SoftIRQs.
uint32_t pinOffset = 0;

/// Number of ways server 1's hash range must be split. Used for benchmarking
/// SoFASTER's view based ownership validation.
int numSplits = 0;

/// Flag indicating whether a migration will be issued as part of the
/// experiment. Required so that client threads can create and initialize
/// sessions correctly.
bool migrate = false;

/// If migrate is true, the percentage of server 1's hash range that will
/// be migrated. Required so that client threads can create and initialize
/// sessions correctly.
uint32_t migrPct = 10;

/// If we're compacting server 1 as part of the experiment, the log address
/// upto which we will do so in bytes.
uint64_t compactUntil = 1 * 1024 * 1024 * 1024;

/// YCSB workload to run. Defaults to 100% read-modify-write. Reset when
/// parsing command line options.
std::string workload = "YCSB-F";

/// An index into the next chunk of keys that a thread can pick up and issue
/// requests against, and the number of keys in each chunk. The index is into
/// "fillKeys" while loading data, and "txnsKeys" while running the benchmark.
std::atomic<uint64_t> idx(0);
constexpr uint64_t kChunkSize = 100000;

/// Cache aligned and padded struct to avoid false sharing between threads.
struct alignas(64) Measurement {
  std::atomic<uint64_t> inner;

  uint64_t _padding[7];
};
static_assert(sizeof(Measurement) == 64, "Measurements not cache padded.");

/// Total number of operations serviced successfully by each thread so far.
Measurement completed[96];
Measurement completeP[96];
Measurement pendTotal[96];

/// Total number of operations serviced successfully by each thread for the
/// source server so far.
Measurement sComplete[96];

/// Total number of operations serviced successfully by each thread for the
/// target server so far.
Measurement tComplete[96];

/// Total number of operations gone pending on each thread so far.
Measurement nPendings[96];

/// Boolean flag used to indicate to client threads that the experiment is over.
std::atomic<bool> done(false);

/// Uncomment the below line or pass in as an argument to make to compile the
/// client with an Infiniband transport instead of Tcp.
/// #define ENABLE_INFINIBAND 1

/// Based on the transport layer, set the number of bytes worth of requests that
/// will be packed into each RPC. Might br reset when parsing command line
/// options.
#ifndef ENABLE_INFINIBAND
#include "network/tcp.h"
typedef transport::TcpTransport transport_t;
uint32_t transmitBytes = 32768;
#else
#include "network/infrc.h"
typedef transport::InfrcTransport transport_t;
uint32_t transmitBytes = 1024;
#endif

/// By default we use 8 Byte values with atomic operations. If "VALUE_SIZE" is
/// passed in at compile time and is not equal to 8, then use fine-grained
/// record locks (ValueLocked<>).
/// #define VALUE_SIZE 100
#ifndef VALUE_SIZE
typedef Value value_t;
#else
#if VALUE_SIZE <= 8
typedef Value value_t;
#else
typedef ValueLocked<VALUE_SIZE> value_t;
#endif
#endif

/// For convenience. Defines the type on the key.
typedef Key key_t_;

/// For convenience. Defines the type on the client library.
typedef Sofaster<key_t_, value_t, transport_t> sofaster_t;

/// Tracks the number of keys that were actually filled into the cluster.
Measurement filled;

/// Loads data into a remote SoFASTER instance. Each thread that invokes this
/// function picks up a chunk of keys and upserts them into SoFASTER until
/// there aren't any chunks left to pick up.
void init(size_t threadId) {
  // Pin the thread and retrieve it's handle to sofaster for now.
  pinThread(threadId + pinOffset);
  sofaster_t sofaster(1, threadId, 4096);

  // If we're going to issue a migration, do not fill data into the last server
  // because it will be the target of the migration.
  int nServers = servers.size();
  if (migrate) nServers -= 1;

  // Add hash ranges to the library's metadata map.
  uint64_t split = (~0UL) / nServers;
  uint64_t lHash = 0UL;
  uint64_t rHash = lHash + split;

  for (auto i = 1; i <= nServers; i++) {
    sofaster.addHashRange(lHash, rHash, servers[i - 1],
                          std::to_string(serverBasePort + threadId));
    lHash = rHash + 1;
    rHash = rHash + split;
  }

  // As long as there are keys to insert, fetch a chunk of them from the global
  // list. For each key in the fetched chunk, issue an upsert to the server.
  uint64_t kChunkSize = 3200;
  for (uint64_t chunkIdx = idx.fetch_add(kChunkSize); chunkIdx < initKCount;
       chunkIdx = idx.fetch_add(kChunkSize))
  {
    for (uint64_t i = chunkIdx; i < chunkIdx + kChunkSize; i++) {
      // Allocate contexts for the Upsert.
      key_t_ key(i);
      value_t v(41);

      if (!generateKeys) key = Key(fillKeys.get()[i]);

      sofaster_t::UpsertContext context(key, v);
      auto callback = [](FASTER::core::IAsyncContext* ctxt,
                       FASTER::core::Status result)
      {
        // Need to construct this class so that the heap allocated
        // context (ctxt) is freed correctly.
        CallbackContext<sofaster_t::UpsertContext> context(ctxt);
        assert(result == FASTER::core::Status::Ok);
        filled.inner++;
      };

      sofaster.upsert(context, callback);
    }
  }

  // Flush out all remaining requests enqueued on all sessions.
  sofaster.clearSessions();
}

/// Warms up the SoFASTER cluster before issuing the main workload. Each
/// thread issues chunks of operations from the workload file. Returns
/// once this file has been iterated through once.
void warmup(size_t threadId) {
  // Pin the thread and create a handle to sofaster.
  pinThread(threadId + pinOffset);
  sofaster_t sofaster(pipelineDepth, threadId, transmitBytes);

  // If we're going to issue a migration, do not warmup the last server
  // because it will be the target of the migration.
  int nServers = servers.size();
  if (migrate) nServers -= 1;

  // Add hash ranges to the library's metadata map.
  uint64_t split = (~0UL) / nServers;
  uint64_t lHash = 0UL;
  uint64_t rHash = lHash + split;

  for (auto i = 1; i <= nServers; i++) {
    sofaster.addHashRange(lHash, rHash, servers[i - 1],
                          std::to_string(serverBasePort + threadId));
    lHash = rHash + 1;
    rHash = rHash + split;
  }

  // Random number generator for YCSB based workload. Required to determine
  // which key we're issuing a request for when not using workload files.
  std::random_device rd;
  std::mt19937 kRg(rd());

  // As long as there are keys left, fetch a chunk of them from the global
  // list. For each key in the fetched chunk, issue a rmw to the server.
  uint64_t kChunkSize = 3200;
  for (uint64_t chunkIdx = idx.fetch_add(kChunkSize); chunkIdx < txnsCount;
       chunkIdx = idx.fetch_add(kChunkSize))
  {
    for (uint64_t i = chunkIdx; i < chunkIdx + kChunkSize; i++) {
      // Allocate contexts for the operation.
      key_t_ key(0);
      value_t v(41);

      if (generateKeys) {
        key = Key(kRg() % initKCount);
      } else {
        key = Key(txnsKeys.get()[i]);
      }

      sofaster_t::RmwContext context(key, v);
      auto callback = [](FASTER::core::IAsyncContext* ctxt,
                       FASTER::core::Status result)
      {
        // Need to construct this class so that the heap allocated
        // context (ctxt) is freed correctly.
        CallbackContext<sofaster_t::RmwContext> context(ctxt);
        assert(result == FASTER::core::Status::Ok ||
               result == FASTER::core::Status::Pending);
      };

      sofaster.rmw(context, callback);
    }
  }

  // Flush out all remaining requests enqueued on all sessions.
  sofaster.clearSessions();
}

thread_local uint64_t responses[8];             // # resps processed by thread.
thread_local uint64_t responseP[8];             // # resps processed by thread
                                                // that went pending before.
std::atomic<int> nReady(0);                     // Num threads ready to start.

// Random number generator to determine if it is time for a read or an
// upsert for all workloads except YCSB-F.
thread_local std::random_device rd;
thread_local std::mt19937 rng(rd());

/// Based on the configured workload, issues the appropriate request.
void issue(sofaster_t& sofaster, key_t_& k, value_t& v) {
  // YCSB-F. This means that we can just issue a read-modify-write and return.
  if (workload == "YCSB-F") {
    sofaster_t::RmwContext context(k, v);
    auto callback = [](FASTER::core::IAsyncContext* ctxt,
                       FASTER::core::Status result)
    {
      // Need to construct this class so that the heap allocated
      // context (ctxt) is freed correctly.
      CallbackContext<sofaster_t::RmwContext> context(ctxt);
      assert(result == FASTER::core::Status::Ok ||
             result == FASTER::core::Status::Pending);
      if (result == FASTER::core::Status::Pending) responseP[0]++;
      responses[0]++;
    };

    sofaster.rmw(context, callback);
    return;
  }

  // If we're here then we need to figure out if it is time for a read or an
  // an upsert request.
  if (rng() % 100 >= readPct) {
    sofaster_t::UpsertContext context(k, v);
    auto callback = [](FASTER::core::IAsyncContext* ctxt,
                       FASTER::core::Status result)
    {
      // Need to construct this class so that the heap allocated
      // context (ctxt) is freed correctly.
      CallbackContext<sofaster_t::UpsertContext> context(ctxt);
      assert(result == FASTER::core::Status::Ok ||
             result == FASTER::core::Status::Pending);
      if (result == FASTER::core::Status::Pending) responseP[0]++;
      responses[0]++;
    };

    sofaster.upsert(context, callback);
  } else {
    sofaster_t::ReadContext context(k);
    auto callback = [](FASTER::core::IAsyncContext* ctxt,
                       FASTER::core::Status result)
    {
      // Need to construct this class so that the heap allocated
      // context (ctxt) is freed correctly.
      CallbackContext<sofaster_t::ReadContext> context(ctxt);
      assert(result == FASTER::core::Status::Ok ||
             result == FASTER::core::Status::Pending);
      if (result == FASTER::core::Status::Pending) responseP[0]++;
      responses[0]++;
    };

    sofaster.read(context, callback);
  }
}

/// Flag set by main() indicating that thread 0 must issue a migration.
std::atomic<bool> doMigrate(false);

/// Flag set by main() indicating that thread 0 must issue a compaction.
std::atomic<bool> doCompact(false);

/// Executes the workload. The list of transactions to be issued to SoFASTER
/// is partitioned across threads. Each thread issues transactions from it's
/// partition (and wraps around if necessary) until the end of the experiment.
void exec(size_t threadId) {
  // Pin the thread, and retrieve it's handle to sofaster.
  pinThread(threadId + pinOffset);
  sofaster_t sofaster(pipelineDepth, threadId, transmitBytes);

  // If we're going to issue a migration, do not include the last server
  // because it will be the target of the migration. Once the migration
  // is issued, the view change will include the server.
  int nServers = servers.size();
  if (migrate) nServers -= 1;

  // Add hash ranges to the library's metadata map.
  uint64_t split = (~0UL) / nServers;
  uint64_t lHash = 0UL;
  uint64_t rHash = lHash + split;

  for (auto i = 1; i <= nServers && numSplits == 0; i++) {
    sofaster.addHashRange(lHash, rHash, servers[i - 1],
                          std::to_string(serverBasePort + threadId));
    lHash = rHash + 1;
    rHash = rHash + split;
  }

  // If we're benchmarking view validation, break up server 1's hash range
  // based on the number of splits we're benchmarking for.
  if (numSplits > 0) {
    split = (~0UL) / numSplits;
    lHash = 0UL;
    rHash = lHash + split;

    sofaster.addSession(servers[0], std::to_string(serverBasePort + threadId));

    for (auto i = 0; i < numSplits; i++) {
      sofaster.addHashRange(lHash, rHash, "", "", 0);
      lHash = rHash + 1;
      rHash = rHash + split;
    }
  }

  // If we're going to issue a migration, add sessions to the last server.
  // This is required for the view change mechanism to work.
  if (migrate) {
    pct = migrPct;
    sofaster.addSession(servers[servers.size() - 1],
                          std::to_string(serverBasePort + threadId));
  }

  if (!threadId) latency = true;

  // Random number generator for YCSB based workload. Required to determine
  // which key we're issuing a request for when not using workload files.
  std::random_device rd;
  std::mt19937 kRg(rd());

  // As long as there are keys to issue against, fetch a chunk of them from
  // the global list. For each key in the fetched chunk, issue an rmw to
  // the server. We pre-partition chunks between threads to avoid contention.
  uint64_t chunkStart = (txnsCount / threads) * threadId;
  uint64_t chunkStop_ = chunkStart + (txnsCount / threads);

  nReady++;

  uint64_t chunkIdx = chunkStart;
  while (!done) {
    for (uint64_t i = chunkIdx; i < chunkIdx + kChunkSize; i++) {
      if (i >= chunkStop_) break;

      key_t_ key(0);
      value_t m(4);

      if (generateKeys) {
        key = Key(kRg() % initKCount);
      } else {
        key = Key(txnsKeys.get()[i]);
      }

      issue(sofaster, key, m);

      // We're going to take measurements every 5000 operations.
      if (responses[0] < 5000) continue;

      // Issue a migration from the first server to the last if required.
      if (!threadId && doMigrate) {
        uint64_t l = 0;
        uint64_t r = (split / 100) * migrPct;
        logMessage(Lvl::INFO,
          "Migrating hash range [%lu, %lu] from %s to %s",
          l, r, servers[0].c_str(), servers[servers.size() - 1].c_str());
        bool e = sofaster.migrate(l, r, servers[0], 50000,
                   servers[servers.size() - 1], serverBasePort);
        if (!e) {
          logMessage(Lvl::ERROR,
            "Failed to issue migration of range [%lu, %lu] from %s to %s",
            l, r, servers[0].c_str(), servers[servers.size() - 1].c_str());
        }

        doMigrate = false;
      }

      // Compact the first server's log if required.
      if (!threadId && doCompact) {
        logMessage(Lvl::INFO,
          "Compacting %s upto address %lu",
          servers[0].c_str(), compactUntil);
        bool e = sofaster.compact(compactUntil, servers[0]);
        if (!e) {
          logMessage(Lvl::ERROR,
            "Failed to compact %s upto address %lu",
            servers[0].c_str(), compactUntil);
        }

        doCompact = false;
      }

      auto order = std::memory_order_relaxed;
      completed[threadId].inner.fetch_add(responses[0], order);
      completeP[threadId].inner.fetch_add(responseP[0], order);
      responses[0] = 0;
      responseP[0] = 0;

      pendTotal[threadId].inner.fetch_add(pendingT[0], order);
      nPendings[threadId].inner.store(pending[0], order);

      sComplete[threadId].inner.fetch_add(sofaster.sessions[0].nResponses,
                                          order);
      sofaster.sessions[0].nResponses = 0;

      if (sofaster.sessions.size() == 2) {
        tComplete[threadId].inner.fetch_add(sofaster.sessions[1].nResponses,
                                            order);
        sofaster.sessions[1].nResponses = 0;
      }
    }

    chunkIdx += kChunkSize;
    if (chunkIdx >= chunkStop_) chunkIdx = chunkStart;
  }

  auto order = std::memory_order_relaxed;
  completed[threadId].inner.fetch_add(responses[0], order);
  completeP[threadId].inner.fetch_add(responseP[0], order);
  pendTotal[threadId].inner.fetch_add(pendingT[0], order);
  sComplete[threadId].inner.fetch_add(sofaster.sessions[0].nResponses, order);
  nPendings[threadId].inner.store(pending[0], order);

  if (sofaster.sessions.size() == 2) {
    tComplete[threadId].inner.fetch_add(sofaster.sessions[1].nResponses, order);
  }
}

int main(int argc, char* argv[]) {
  // The set of supported command line options.
  options_description desc("Allowed options");
  desc.add_options()
    ("help", "Produce a help message and exit")
    ("threads", value<size_t>()->default_value(32),
       "Number of client threads to be spawned")
    ("nKeys", value<uint64_t>()->default_value(250 * 1000 * 1000),
       "Number of keys to be loaded into SoFASTER")
    ("nTxns", value<uint64_t>()->default_value(1000 * 1000 * 1000),
       "Number of requests/transactions to be issued to SoFASTER")
    ("pipelining", value<uint64_t>()->default_value(2),
       "The maximum number of in-flight transmits at any point")
    ("loadFile", value<std::string>()->default_value("load_ycsb"),
       "File containing keys to be loaded into SoFASTER")
    ("txnsFile", value<std::string>()->default_value("run_ycsb"),
       "File containing keys against which requests should be issued")
    ("sample", value<bool>()->default_value(false),
       "Flag indicating whether we should periodically sample throughput")
    ("exptTimeSec", value<uint64_t>()->default_value(120),
       "Duration of the experiment in seconds")
    ("measureInMs", value<uint64_t>()->default_value(1),
       "Interval in milliseconds at which to collect throughput samples")
    ("servers", value<std::vector<std::string>>()->multitoken(),
       "Server IP addresses to connect to and split the hash range across")
    ("serverBasePort", value<uint16_t>()->default_value(22790),
       "Port that worker 0 on a server listens to. Worker i is offset by i")
    ("fillServers", value<bool>()->default_value(true),
       "If true, fills data into servers in the cluster")
    ("fillAndExit", value<bool>()->default_value(false),
       "If true, fills data into servers in the cluster and then exits")
    ("transmitBytes", value<uint32_t>()->default_value(0),
       "Number of bytes worth of requests packed into each RPC")
    ("workload", value<std::string>()->default_value("YCSB-F"),
       "Type of YCSB workload to run")
    ("generateKeys", value<bool>()->default_value(false),
       "If true, generate keys using a prng")
    ("pinOffset", value<uint32_t>()->default_value(0),
       "Number of cores by which a threads affinity should be offset.")
    ("migrate", value<bool>()->default_value(false),
       "If true, initiates a migration during the experiment.")
    ("migrPct", value<uint32_t>()->default_value(10),
       "Percentage of hash range that will be migrated if migrate is true")
    ("migrTimeSec", value<uint64_t>()->default_value(60),
       "Time from start at which migration will be issued if migrate is true")
    ("numSplits", value<int>()->default_value(0),
       "Number of ways server 1's hash range must be split when benchmarking views")
    ("compact", value<bool>()->default_value(false),
       "If true, initiates a compaction during the experiment.")
    ("compactUntilGB", value<uint64_t>()->default_value(1),
       "Address upto which we will compact if compact is true")
    ("compTimeSec", value<uint64_t>()->default_value(120),
       "Time from start at which compaction will be issued if compact is true")
  ;

  // Parse command line options into a variable map.
  variables_map vm;
  store(parse_command_line(argc, argv, desc), vm);
  notify(vm);

  if (vm.count("help")) {
    std::cout << desc << std::endl;
    return 1;
  }

  // Retrieve the parsed options from the variable map.
  threads = vm["threads"].as<size_t>();
  initKCount = vm["nKeys"].as<uint64_t>();
  txnsCount = vm["nTxns"].as<uint64_t>();
  pipelineDepth = vm["pipelining"].as<uint64_t>();
  std::string loadFile = vm["loadFile"].as<std::string>();
  std::string txnsFile = vm["txnsFile"].as<std::string>();
  sample = vm["sample"].as<bool>();
  exptTimeSec = vm["exptTimeSec"].as<uint64_t>();
  measureInMs = vm["measureInMs"].as<uint64_t>();
  servers = vm["servers"].as<std::vector<std::string>>();
  serverBasePort = vm["serverBasePort"].as<uint16_t>();
  bool fillServers = vm["fillServers"].as<bool>();
  bool fillAndExit = vm["fillAndExit"].as<bool>();
  auto t = vm["transmitBytes"].as<uint32_t>();
  workload = vm["workload"].as<std::string>();
  generateKeys = vm["generateKeys"].as<bool>();
  pinOffset = vm["pinOffset"].as<uint32_t>();
  migrate = vm["migrate"].as<bool>();
  migrPct = vm["migrPct"].as<uint32_t>();
  uint64_t migrTimeSec = vm["migrTimeSec"].as<uint64_t>();
  numSplits = vm["numSplits"].as<int>();
  bool compact = vm["compact"].as<bool>();
  compactUntil = vm["compactUntilGB"].as<uint64_t>() * 1024 * 1024 * 1024;
  uint64_t compTimeSec = vm["compTimeSec"].as<uint64_t>();

  if (workload == "YCSB-A") {
    readPct = 50;
  } else if (workload == "YCSB-B" || workload == "YCSB-D") {
    readPct = 95;
  } else if (workload == "YCSB-C") {
    readPct = 100;
  } else if (workload == "YCSB-F") {
    readPct = 0;
  } else {
    logMessage(Lvl::ERROR, "Unrecognized workload");
    exit(-1);
  }

  if (migrate && servers.size() < 2) {
    logMessage(Lvl::ERROR,
      "Need atleast two servers to issue migration. Only %lu servers provided",
      servers.size());
    exit(-1);
  }

  if (migrate && migrPct > 100) {
    logMessage(Lvl::ERROR,
      "Asked to migrate %lu\%. Cannot migrate more than 100\% of hash range",
      migrPct);
    exit(-1);
  }

  if (migrate && migrTimeSec > exptTimeSec) {
    logMessage(Lvl::ERROR,
      "Asked to migrate at %lu sec which is more than expt duration of %lu sec",
      migrTimeSec, exptTimeSec);
    exit(-1);
  }

  if (t > 0) transmitBytes = t;
  transmitBytes = std::min(transmitBytes, transport_t::bufferSize());

  logMessage(Lvl::INFO,
             (std::string("Running client with %lu threads. Each thread will") +
             " run %s with %lu keys and %lu transactions for %lu seconds." +
             " Requests will be issued in batches of %lu B with a pipeline" +
             " of size %lu. Number of splits for view bench set to %d").c_str(),
             threads,
             workload.c_str(),
             initKCount,
             txnsCount,
             exptTimeSec,
             transmitBytes,
             pipelineDepth,
             numSplits);

  if (!generateKeys) {
    logMessage(Lvl::INFO, "Loading workload data from %s and %s into memory",
               loadFile.c_str(), txnsFile.c_str());
    load(loadFile, initKCount, fillKeys, txnsFile, txnsCount, txnsKeys);
  } else {
    logMessage(Lvl::INFO, "Generating workload keys and requests from PRNG");
  }

  if (fillServers || fillAndExit) {
    logMessage(Lvl::INFO, "Filling data into the server");
    std::vector<std::thread> fThreads;
    for (size_t i = 0; i < threads; i++) {
      fThreads.emplace_back(init, i);
    }

    for (size_t i = 0; i < threads; i++) {
      fThreads[i].join();
    }
    logMessage(Lvl::INFO, "Filled %lu keys into the server",
               filled.inner.load());
  }

  if (fillAndExit) return 0;

  // Warmup the cluster before issuing the workload.
  idx.store(0);
  logMessage(Lvl::INFO, "Warming up the cluster");
  std::vector<std::thread> wThreads;
  for (size_t i = 0; i < threads; i++) {
    wThreads.emplace_back(warmup, i);
  }

  for (size_t i = 0; i < threads; i++) {
    wThreads[i].join();
  }

  // Reset state for the main benchmark.
  idx.store(0);
  fillKeys.reset();

  // To benchmark the datapath, issue a migration & return immediately.

  logMessage(Lvl::INFO, "Running %s benchmark", workload.c_str());
  std::vector<std::thread> rThreads;
  for (size_t i = 0; i < threads; i++) {
    rThreads.emplace_back(exec, i);
  }

  auto interval = milliseconds(measureInMs); // Interval at which we sample.
  auto time = high_resolution_clock::now();  // Current time.
  auto stop = time + seconds(exptTimeSec);   // Time at which we should stop.
  auto migr = time + seconds(migrTimeSec);   // Time at which we should
                                             // issue a migration.
  auto comp = time + seconds(compTimeSec);   // Time at which we should
                                             // issue a compaction.

  uint64_t prev = 0;
  std::vector<double> thrpt;

  uint64_t sPre = 0;
  std::vector<double> sThrp;

  uint64_t tPre = 0;
  std::vector<double> tThrp;

  std::vector<uint64_t> nPend;

  bool doneMigrate = false;

  while (nReady < threads);

  auto s = high_resolution_clock::now();
  while (time < stop) {
    auto start = high_resolution_clock::now();
    this_thread::sleep_for(interval);
    time = high_resolution_clock::now();

    if (migrate && time > migr && !doneMigrate) {
      doMigrate = true;
      doneMigrate = true;
    }

    if (compact && time > comp) {
      doCompact = true;
      compact = false;
    }

    auto ord = std::memory_order_relaxed;
    if (sample) {
      uint64_t total = 0;
      auto d = (time - start).count();
      for (auto i = 0; i < threads; i++) total += completed[i].inner.load(ord);
      thrpt.emplace_back(((double) (total - prev)) * (1e9 / d));
      prev = total;

      total = 0;
      for (auto i = 0; i < threads; i++) total += sComplete[i].inner.load(ord);
      sThrp.emplace_back(((double) (total - sPre)) * (1e9 / d));
      sPre = total;

      total = 0;
      for (auto i = 0; i < threads; i++) total += tComplete[i].inner.load(ord);
      tThrp.emplace_back(((double) (total - tPre)) * (1e9 / d));
      tPre = total;

      total = 0;
      for (auto i = 0; i < threads; i++) total += nPendings[i].inner.load(ord);
      nPend.emplace_back(total);
    }
  }
  auto e = high_resolution_clock::now();

  done = true;
  logMessage(Lvl::INFO, "Completed Experiment");

  // Print out the total throughput as measured by this client.
  uint64_t total = 0;
  auto d = (e - s).count();
  auto ord = std::memory_order_relaxed;
  for (auto i = 0; i < threads; i++) total += completed[i].inner.load(ord);
  logMessage(Lvl::INFO, "Average Throughput: %.3f Mops/sec",
             ((double) total) / (d * 1e-3));

  uint64_t pendT = 0;
  for (auto i = 0; i < threads; i++) pendT += pendTotal[i].inner.load(ord);
  uint64_t pendC = 0;
  for (auto i = 0; i < threads; i++) pendC += completeP[i].inner.load(ord);
  logMessage(Lvl::INFO,
            (std::string("Completed: %lu (%lu of these went pending), ") +
            "Total Pending: %lu, Hit rate %.3f").c_str(),
             total, pendC, pendT, 1 - ((double) pendC) / total);

  // If required, then print out all samples that were collected during
  // the experiment by this thread to a file called "samples.data".
  if (sample) {
    ofstream outputF;
    outputF.open("./samples.data");
    outputF << "TimeSec ThroughputMops Pending Source Target" << "\n";
    for (size_t i = 0; i < std::min(thrpt.size(), nPend.size()); i++) {
      outputF << ((double) (i * measureInMs)) / 1000 << " " <<
                 thrpt[i] / 1e6 << " " << nPend[i] << " " <<
                 sThrp[i] / 1e6 << " " << tThrp[i] / 1e6 << "\n";
    }
    outputF.close();
  }

  logMessage(Lvl::INFO, "Waiting for threads to exit");
  for (size_t i = 0; i < threads; i++) {
    rThreads[i].join();
  }

  return 0;
}
