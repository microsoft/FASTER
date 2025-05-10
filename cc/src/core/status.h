// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once
#include <cstdint>

namespace FASTER {
namespace core {

enum class Status : uint8_t {
  Ok = 0,
  Pending = 1,
  NotFound = 2,
  OutOfMemory = 3,
  IOError = 4,
  Corruption = 5,
  Aborted = 6,
};
inline const char* StatusStr(Status s) {
  static const char* STATUS_STR[] = {
    "Ok", "Pending", "NotFound", "OutOfMemory",
    "IOError", "Corruption", "Aborted",
  };
  return STATUS_STR[static_cast<uint8_t>(s)];
}

enum class InternalStatus : uint8_t {
  Ok,
  RETRY_NOW,
  RETRY_LATER,
  RECORD_ON_DISK,
  SUCCESS_UNMARK,
  CPR_SHIFT_DETECTED
};

}
} // namespace FASTER::core
