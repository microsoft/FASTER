// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <cstdio>
#include <experimental/filesystem>
#include <stdint.h>
#include <sys/resource.h>

#include "environment/file.h"
#include "common/log.h"

void CreateNewLogDir(const std::string& root) {
  if (root.empty()) {
    log_warn("Setting root directory to local/current one");
    return;
  }
  std::experimental::filesystem::remove_all(root);

  bool success = std::experimental::filesystem::create_directories(root);
  if (!success) {
    log_error("Could not create directory @ %s", root.c_str());
    throw std::runtime_error{ "Could not create directory "};
  }
}

void CreateNewLogDir(const std::string& root, std::string& hot_log_fp, std::string& cold_log_fp) {
  CreateNewLogDir(root);

  std::string local_root{ root };
  std::string separator{ FASTER::environment::kPathSeparator };
  if (local_root.back() != separator.back()) {
    local_root += separator;
  }
  hot_log_fp = local_root + "hot_";
  cold_log_fp = local_root + "cold_";
}

void RemoveDir(const std::string& root) {
  // throw exception on OS error(s)
  std::experimental::filesystem::remove_all(root);
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
