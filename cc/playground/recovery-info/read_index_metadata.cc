// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include <cstdio>

#include "../src/core/checkpoint_state.h"

using namespace FASTER::core;

int main(int argc, char* argv[])
{
  if (argc != 2) {
    fprintf(stderr, "Usage: ./read_index_metadata <metadata-file>\n\n");
    return 0;
  }

  const uint32_t index_metadata_size = sizeof(IndexMetadata);
  uint8_t buffer[index_metadata_size];

  FILE* f = fopen(argv[1], "r");
  if (fread(buffer, index_metadata_size, 1, f) < index_metadata_size) {
    fprintf(stderr, "WARNING: fread read less bytes\n");
  }

  auto index_metadata = reinterpret_cast<IndexMetadata*>(buffer);

  printf("Index Metadata\n");
  printf("===================================\n");
  printf("Version                 : %u\n", index_metadata->version);
  printf("Table Size              : %lu\n", index_metadata->table_size);
  printf("Num hash table bytes    : %lu\n", index_metadata->num_ht_bytes);
  printf("Num ovf bucket bytes    : %lu\n", index_metadata->num_ofb_bytes);
  printf("Overflow bucket tail    : %15lu (page: %8lu, offset: %8u)\n", index_metadata->ofb_count.control(),
          index_metadata->ofb_count.page(), index_metadata->ofb_count.offset());
  printf("Log begin address       : %15lu (page: %8u, offset: %8u)\n", index_metadata->log_begin_address.control(),
          index_metadata->log_begin_address.page(), index_metadata->log_begin_address.offset());
  printf("Checkpoint start address: %15lu (page: %8u, offset: %8u)\n", index_metadata->checkpoint_start_address.control(),
          index_metadata->checkpoint_start_address.page(), index_metadata->checkpoint_start_address.offset());

  return 0;
}