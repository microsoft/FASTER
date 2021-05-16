// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include <vector>
#include <fstream>
#include <iostream>
#include <algorithm>
#include <unordered_map>

#include "../benchmark-dir/file.h"

#include "core/auto_ptr.h"
#include "core/utility.h"

#include "common/log.h"

using namespace std;
using namespace SoFASTER;
using namespace FASTER::core;

/// Loads YCSB workload keys from file into memory.
///
/// \param fill
///    File path containing keys to be inserted into the server.
/// \param kInitCount
///    The number of keys in the fill file.
/// \param fillKeys
///    Pointer into which the keys to be inserted will be loaded. This
///    function performs all necessary allocations.
/// \param txns
///    File path containing keys against which requests must be issued.
/// \param kTxnsCount
///    The number of keys in the txns file.
/// \param txnsKeys
///    Pointer into which the keys to be issued will be loaded. This
///    function performs all necessary allocations.
void load(const std::string& fill,
          const uint64_t kInitCount,
          aligned_unique_ptr_t<uint64_t>& fillKeys,
          const std::string& txns,
          const uint64_t kTxnsCount,
          aligned_unique_ptr_t<uint64_t>& txnsKeys)
{
  // The unit (in bytes) at which data will be loaded from the passed in files
  // into main memory.
  constexpr size_t kFileChunkSize = 128 * 1024;

  // Allocate a sector aligned (512 Byte) region of memory of size
  // "kFileChunkSize". Data will be loaded from disk into this region.
  auto chunkGuard = alloc_aligned<uint64_t>(512, kFileChunkSize);
  uint64_t* chunk = chunkGuard.get();

  // Open the file containing all keys that the server should be populated
  // with. Also, allocate a cache-aligned array to store these keys.
  FASTER::benchmark::File initFile(fill);
  fillKeys = alloc_aligned<uint64_t>(64, kInitCount * sizeof(uint64_t));

  // Read the init file one chunk at a time. Iterate through each chunk's
  // contents 8 bytes at a time. Each of these 8 byte sequences is a key
  // to be inserted into the server for the experiment.
  uint64_t count = 0;  // The number of keys read from the file so far.
  uint64_t offset = 0; // File offset that we're currently reading from.
  while (true) {
    uint64_t size = initFile.Read(chunk, kFileChunkSize, offset);

    for (uint64_t idx = 0; idx < (size / sizeof(uint64_t)); idx++) {
      fillKeys.get()[count++] = chunk[idx];
    }

    // Read lesser than expected meaning we've reached the end of the file.
    if (size != kFileChunkSize) break;

    offset += kFileChunkSize;
  }

  if (count != kInitCount) {
    logMessage(Lvl::ERR, "Expected %lu keys, found %lu", kInitCount, count);
    exit(1);
  }

  // No transactions to be loaded, just return.
  if (!kTxnsCount) return;

  // Open the file containing all the keys that requests must be issued against.
  // Also, allocate a cache-aligned array to store these keys.
  FASTER::benchmark::File txnsFile(txns);
  txnsKeys = alloc_aligned<uint64_t>(64, kTxnsCount * sizeof(uint64_t));

  // Read the txns file one chunk at a time. Each chunk contains 8 byte keys
  // against which requests must be issued to the server.
  count = 0;
  offset = 0;
  while (true) {
    uint64_t size = txnsFile.Read(chunk, kFileChunkSize, offset);

    for (uint64_t idx = 0; idx < (size / sizeof(uint64_t)); idx++) {
      txnsKeys.get()[count++] = chunk[idx];
    }

    // Read lesser than expected meaning we've reached the end of the file.
    if (size != kFileChunkSize) break;

    offset += kFileChunkSize;
  }

  if (count != kTxnsCount) {
    logMessage(Lvl::ERR, "Expected %lu txns, found %lu", kTxnsCount, count);
    exit(1);
  }
}

/// Prints out the top "k" most frequent keys in a passed in list.
void loadDist(aligned_unique_ptr_t<uint64_t>& txnsKeys,
              const uint64_t kTxnsCount)
{
  std::unordered_map<uint64_t, uint64_t> frequencies;

  // First, calculate frequencies of keys in the first half of the hash space.
  for (auto idx = 0; idx < kTxnsCount; idx++) {
    if (Utility::GetHashCode(txnsKeys.get()[idx]) > ~0UL / 2) continue;

    frequencies[txnsKeys.get()[idx]] += 1;
  }

  // Next, push the keys and their frequencies into a vector, and sort the
  // vector by frequency.
  std::vector<std::pair<uint64_t, uint64_t>> sortedList;
  for (auto it : frequencies) sortedList.push_back(it);

  auto Comp = [](std::pair<uint64_t, uint64_t> l,
                 std::pair<uint64_t, uint64_t> r)
  {
    return l.second < r.second;
  };

  std::sort(sortedList.begin(), sortedList.end(), Comp);

  double sum = 0;
  double frc = 0.05;
  std::vector<uint64_t> samples;
  for (auto idx = sortedList.size(); idx > 0; idx--) {
    sum += sortedList[idx].second;

    if (sum >= ((double) kTxnsCount) * frc) {
      samples.push_back(sortedList.size() - idx);
      frc += 0.05;
    }
  }

  ofstream outputF;
  outputF.open("./load_dist.data");
  outputF << "Load Size" << "\n";
  for (auto i = 0; i < samples.size(); i++) {
    outputF << 0.05 * (i + 1) << " " << ((double) samples[i]) / kTxnsCount <<
               "\n";
  }
  outputF.close();
}

/// Pins the calling thread to the passed in core.
void pinThread(size_t coreId) {
  cpu_set_t mask;
  CPU_ZERO(&mask);

  CPU_SET(coreId, &mask);
  sched_setaffinity(0, sizeof(mask), &mask);
}
