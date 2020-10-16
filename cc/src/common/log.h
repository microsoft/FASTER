// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <time.h>
#include <stdio.h>
#include <stdarg.h>

/// Defines the type of log messages supported by the system.
enum class Lvl {
  DEBUG = 0,
  INFO  = 1,
  ERROR = 2,
};

/// By default, only print ERROR log messages.
#define LEVEL Lvl::ERROR

/// Macro to add in the file and function name, and line number.
#define logMessage(l, f, a...) logMsg(l, __LINE__, __func__, __FILE__, f, ##a)

/// Prints out a message with the current timestamp and code location.
inline void logMsg(Lvl level, int line, const char* func,
                   const char* file, const char* fmt, ...)
{
  // Do not print messages below the current level.
  if (level < static_cast<Lvl>(LEVEL)) return;

  struct timespec now;
  clock_gettime(CLOCK_REALTIME, &now);

  va_list argptr;
  va_start(argptr, fmt);

  char buffer[1024];
  vsnprintf(buffer, 1024, fmt, argptr);

  std::string l;
  switch (level) {
  case Lvl::DEBUG:
    l = std::string("DEBUG");
    break;
  case Lvl::INFO:
    l = std::string("INFO");
    break;
  default:
    l = std::string("ERROR");
    break;
  }

  fprintf(stderr, "[%010lu.%09lu]::%s::%s:%s:%d:: %s\n",
          now.tv_sec, now.tv_nsec,
          l.c_str(), file, func, line, buffer);

  va_end(argptr);
}
