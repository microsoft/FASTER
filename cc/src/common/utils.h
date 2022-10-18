// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <cstdio>
#include <stdint.h>
#include <sys/resource.h>

#include "log.h"

// kB, MB, GB literal helpers
constexpr uint64_t operator""_KiB(unsigned long long const x) {
  return static_cast<uint64_t>(x * (1 << 10));
}
constexpr uint64_t operator""_MiB(unsigned long long const x) {
  return static_cast<uint64_t>(x * (1 << 20));
}
constexpr uint64_t operator""_GiB(unsigned long long const x) {
  return static_cast<uint64_t>(x * (1 << 30));
}

bool set_stack_size(rlim_t new_size) {
  struct rlimit rl;

  int ret = getrlimit(RLIMIT_STACK, &rl);
  if (ret != 0) {
    log_error("Could not retrieve current stack size [ret=%d]", ret);
    return false;
  }
  log_info("Curr stack size: %lu bytes", rl.rlim_cur);

  if (rl.rlim_cur >= new_size) {
    log_info("Requested stack size [%lu] not larger than current. Skipping...", new_size);
    return true;
  }

  rl.rlim_cur = new_size;
  ret = setrlimit(RLIMIT_STACK, &rl);
  if (ret != 0) {
    log_error("Could not set stack size to %lu bytes [ret=%d]", new_size, ret);
    return false;
  }
  log_info("New  stack size: %lu bytes", rl.rlim_cur);
  return true;
}
