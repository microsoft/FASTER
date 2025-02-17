// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include <cstdio>
#include <cinttypes>

#include "../src/core/checkpoint_state.h"

using namespace FASTER::core;

int main(int argc, char* argv[])
{
  if (argc != 2) {
    fprintf(stderr, "Usage: ./read_log_metadata <metadata-file>\n\n");
    return 0;
  }

  const uint32_t log_metadata_size = sizeof(LogMetadata);
  uint8_t buffer[log_metadata_size];

  FILE* f = fopen(argv[1], "r");
  if (fread(buffer, log_metadata_size, 1, f) < log_metadata_size) {
    fprintf(stderr, "WARNING: fread read less bytes\n");
  }

  auto log_metadata = reinterpret_cast<LogMetadata*>(buffer);

  printf("Log Metadata\n");
  printf("===================================\n");
  printf("Use snapshot file  : %s \n", log_metadata->use_snapshot_file ? "TRUE" : "FALSE");
  printf("Version            : %u\n", log_metadata->version);
  printf("Number of Threads  : %u\n", log_metadata->num_threads.load());
  printf("Flushed address    : %15" PRIu64 " (page: %8u, offset: %8u)\n", log_metadata->flushed_address.control(),
          log_metadata->flushed_address.page(), log_metadata->flushed_address.offset());
  printf("Final address      : %15" PRIu64 " (page: %8u, offset: %8u)\n", log_metadata->final_address.control(),
          log_metadata->final_address.page(), log_metadata->final_address.offset());

  printf("Monotonic Serial Numbers [size: %" PRIu64 "]\n", Thread::kMaxNumThreads);
  for (size_t idx = 0; idx < Thread::kMaxNumThreads; ++idx) {
    if (log_metadata->monotonic_serial_nums[idx] != 0) {
      printf("\t\t[%" PRIu64 "] %" PRIu64 "\n", idx, log_metadata->monotonic_serial_nums[idx]);
    }
  }
  printf("Guids [size: %" PRIu64 "]\n", Thread::kMaxNumThreads);
  for (size_t idx = 0; idx < Thread::kMaxNumThreads; ++idx) {
    if (!Guid::IsNull(log_metadata->guids[idx])) {
      printf("\t\t[%" PRIu64 "] %s\n", idx, log_metadata->guids[idx].ToString().c_str());
    }
  }

  return 0;
}