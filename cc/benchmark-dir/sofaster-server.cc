// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include <iostream>

#include "type.h"
#include "server/server.h"

#include "boost/program_options.hpp"

using namespace SoFASTER;
using namespace boost::program_options;

/// Uncomment the below line or pass in as an argument to make to compile the
/// server with an Infiniband transport instead of Tcp.
/// #define ENABLE_INFINIBAND 1
#ifndef ENABLE_INFINIBAND
#include "network/tcp.h"
typedef transport::TcpTransport transport_t;
#else
#include "network/infrc.h"
typedef transport::InfrcTransport transport_t;
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

/// For convenience. Defines the type on the server.
typedef server::Server<Key, Value, Profile<Value>, transport_t> server_t;

int main(int argc, char* argv[]) {
  // The set of supported command line options.
  options_description desc("Allowed options");
  desc.add_options()
    ("help", "Produce a help message and exit")
    ("threads", value<size_t>()->default_value(32),
       "Number of server threads to be spawned")
    ("htSizeM", value<uint64_t>()->default_value(128),
       "The number of entries to be created on the hash table in millions")
    ("logSizeGB", value<uint64_t>()->default_value(64),
       "The size of the in-memory log in GB")
    ("logDisk", value<std::string>()->default_value("~/storage"),
       "The path under which the log should be stored on disk")
    ("basePort", value<uint16_t>()->default_value(22790),
       (std::string("Base port on which server listens for connections. ") +
        std::string("Thread 'i' listens on port 'basePort + i', where 'i' ") +
        std::string("starts from 0")).c_str())
    ("adminPort", value<uint16_t>()->default_value(50000),
       "Port on which server listens for control RPCs")
    ("ipAddr", value<std::string>()->default_value("10.0.0.1"),
       "The IP address the server will listen to for inbound connections")
    ("samplesK", value<uint32_t>()->default_value(128),
       "The maximum size of the sampled set during migration")
    ("connTarget", value<bool>()->default_value(false),
       "If true, open sessions to the target at boot up")
    ("id", value<uint16_t>()->default_value(1),
       "16 bit identifier for the server. This must be non-zero and positive")
    ("dfs", value<std::string>()->default_value("UseDevelopmentStorage=true;"),
       "Connection string for the DFS layer. Defaulted to Azure's emulator")
    ("mutableFraction", value<double>()->default_value(0.9),
       "Fraction of the in-memory region that must be mutable")
    ("pinOffset", value<uint32_t>()->default_value(0),
       "Number of cores by which a worker thread's affinity must be offset")
    ("indirection", value<bool>()->default_value(true),
       "If false, issues IOs instead of indirection records during scale out")
    ("strictPending", value<bool>()->default_value(false),
       "If true, asks the client to retry if there are too many waiting reqs")
    ("numSplits", value<int>()->default_value(0),
       "Number of ways this server's hash range is split for view bench")
    ("lock", value<bool>()->default_value(false),
       "If true, acquires ownership lock before request processing")
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
  size_t threads = vm["threads"].as<size_t>();
  uint64_t htSize = vm["htSizeM"].as<uint64_t>() << 20;
  uint64_t logSize = vm["logSizeGB"].as<uint64_t>() << 30;
  std::string logDisk = vm["logDisk"].as<std::string>();
  uint16_t basePort = vm["basePort"].as<uint16_t>();
  uint16_t adminPort = vm["adminPort"].as<uint16_t>();
  std::string ip = vm["ipAddr"].as<std::string>();
  uint32_t samples = vm["samplesK"].as<uint32_t>() << 10;
  bool connTarget = vm["connTarget"].as<bool>();
  uint16_t id = vm["id"].as<uint16_t>();
  std::string dfs = vm["dfs"].as<std::string>();
  double mFrn = vm["mutableFraction"].as<double>();
  uint32_t pinOffset = vm["pinOffset"].as<uint32_t>();
  bool indirection = vm["indirection"].as<bool>();
  bool strictPending = vm["strictPending"].as<bool>();
  int numSplits = vm["numSplits"].as<int>();
  bool lock = vm["lock"].as<bool>();

  samples = std::min(samples, 128U << 10);

  // Run the server.
  server_t server(threads, htSize, logSize, logDisk, basePort, ip,
                  adminPort, samples, id, dfs, mFrn, indirection,
                  strictPending, numSplits, lock);
  logMessage(Lvl::INFO,
             (std::string("Running server with %lu worker threads, ") +
              std::string("%lu M hash buckets, %lu GB hybrid log stored ") +
              std::string("at %s, with a mutable fraction of %.2f, ") +
              std::string("and sampled set of %u KB. Indirection ") +
              std::string("records set to %s, Strict pending set ") +
              std::string("to %s. Splits for view bench set to %d, ") +
              std::string("ownership lock set to %s")).c_str(),
              threads,
              vm["htSizeM"].as<uint64_t>(),
              vm["logSizeGB"].as<uint64_t>(),
              mFrn,
              logDisk.c_str(),
              vm["samplesK"].as<uint32_t>(),
              indirection ? "true" : "false",
              strictPending ? "true": "false",
              numSplits,
              lock ? "true" : "false");
  server.run(connTarget, pinOffset);

  return 0;
}
