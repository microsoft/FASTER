// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <time.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>

#include <chrono>

#include "../core/thread.h"

/// Defines the type of log messages supported by the system.
enum class Lvl {
  DEBUG = 0,
  INFO  = 1,
  WARN  = 2,
  ERROR = 3
};

/// By default, only print ERROR log messages.
#define LEVEL Lvl::DEBUG

/// Macro to add in the file and function name, and line number.
#ifdef _WIN32
#define __FILENAME__ (strrchr(__FILE__, '\\') ? strrchr(__FILE__, '\\') + 1 : __FILE__)
#define logMessage(l, f, ...) log_msg(l, __LINE__, __func__, __FILENAME__, f, __VA_ARGS__)
#else
#define __FILENAME__ (strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__)
#define logMessage(l, f, a...) log_msg(l, __LINE__, __func__, __FILENAME__, f, ##a)
#endif

#ifdef NDEBUG
#define log_debug(f, a...) do {} while(0)
#define log_info(f, a...) do {} while(0)
#else
#define log_debug(f, a...) logMessage(Lvl::DEBUG, f, ##a)
#define log_info(f, a...) logMessage(Lvl::INFO , f, ##a)
#endif

#define log_warn(f, a...) logMessage(Lvl::WARN, f, ##a)
#define log_error(f, a...) logMessage(Lvl::ERROR, f, ##a)

typedef std::chrono::high_resolution_clock log_clock_t;

static std::chrono::time_point<log_clock_t> start = log_clock_t::now();

/// Prints out a message with the current timestamp and code location.
inline void log_msg(Lvl level, int line, const char* func,
                   const char* file, const char* fmt, ...) {
  // Do not print messages below the current level.
  if (level < static_cast<Lvl>(LEVEL)) return;

  auto now = log_clock_t::now();

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
  case Lvl::WARN:
    l = std::string("WARN");
    break;
  case Lvl::ERROR:
    l = std::string("ERROR");
    break;
  default:
    fprintf(stderr, "Invalid logging level: %d\n", static_cast<int>(level));
    throw std::runtime_error{ "Invalid logging level "};
  }

  fprintf(stderr, "[%04lu.%09lu]{%u}::%s::%s:%s:%d: %s\n",
          (unsigned long) std::chrono::duration_cast<std::chrono::seconds>(now - start).count(),
          (unsigned long) std::chrono::duration_cast<std::chrono::nanoseconds>(now - start).count(),
          FASTER::core::Thread::id(), l.c_str(), file, func, line, buffer);

  va_end(argptr);
}
