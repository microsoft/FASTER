// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <stdio.h>
#include <stdarg.h>

#include <chrono>

/// Defines the type of log messages supported by the system.
enum class Lvl {
  DEBUG = 0,
  INFO  = 1,
  ERR = 2,
};

/// By default, only print ERROR log messages.
#define LEVEL Lvl::INFO

/// Macro to add in the file and function name, and line number.
#ifdef _WIN32
#define logMessage(l, f, ...) logMsg(l, __LINE__, __func__, __FILE__, f, __VA_ARGS__)
#else
#define logMessage(l, f, a...) logMsg(l, __LINE__, __func__, __FILE__, f, ##a)
#endif

/// Prints out a message with the current timestamp and code location.
inline void logMsg(Lvl level, int line, const char* func,
                   const char* file, const char* fmt, ...)
{
  // Do not print messages below the current level.
  if (level < static_cast<Lvl>(LEVEL)) return;

  const auto now = std::chrono::high_resolution_clock::now().time_since_epoch();

  va_list argptr;
  va_start(argptr, fmt);

  char buffer[1024];
  vsnprintf(buffer, 1024, fmt, argptr);

  const char *l;
  switch (level) {
  case Lvl::DEBUG:
    l = "DEBUG";
    break;
  case Lvl::INFO:
    l = "INFO";
    break;
  default:
    l = "ERROR";
    break;
  }

  const auto s = std::chrono::duration_cast<std::chrono::seconds>(now);
  const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(now) - s;
  fprintf(stderr, "[%010lld.%09lld]::%s::%s:%s:%d:: %s\n",
          s.count(), ns.count(), l, file, func, line, buffer);

  va_end(argptr);
}
