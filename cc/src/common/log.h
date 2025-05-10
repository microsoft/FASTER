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
enum class Lvl : uint8_t {
  DEBUG = 0,
  INFO  = 1,
  WARN  = 2,
  ERR = 3,
  REPORT = 4,

  NUM
};

static const char* LvlStr[] = {
  "DEBUG", "INFO", "WARN", "ERROR", ""
};

#ifdef NDEBUG
// Release build: Print WARN (and above) log messages.
#define LEVEL Lvl::WARN
#else
// Debug build: Print all log messages
#define LEVEL Lvl::DEBUG
#endif

/// Macro to add in the file and function name, and line number.
#ifdef _WIN32
#define __FILENAME__ (strrchr(__FILE__, '\\') ? strrchr(__FILE__, '\\') + 1 : __FILE__)
#define logMessage(l, f, ...) log_msg(l, __LINE__, __func__, __FILENAME__, f, __VA_ARGS__)

// Define warn/error/report macros
#define log_warn(f, ...) logMessage(Lvl::WARN, f, __VA_ARGS__)
#define log_error(f, ...) logMessage(Lvl::ERR, f, __VA_ARGS__)
#define log_rep(f, ...) logMessage(Lvl::REPORT, f, __VA_ARGS__)

#else
#define __FILENAME__ (strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__)
#define logMessage(l, f, a...) log_msg(l, __LINE__, __func__, __FILENAME__, f, ##a)

// Define warn/error/report macros
#define log_warn(f, a...) logMessage(Lvl::WARN, f, ##a)
#define log_error(f, a...) logMessage(Lvl::ERR, f, ##a)
#define log_rep(f, a...) logMessage(Lvl::REPORT, f, ##a)

#endif

/// Disable debug/info macros for release mode
#ifdef NDEBUG
#define log_debug(f, ...) do {} while(0)
#define log_info(f, ...) do {} while(0)
#else

#ifdef _WIN32
#define log_debug(f, ...) logMessage(Lvl::DEBUG, f, __VA_ARGS__)
#define log_info(f, ...) logMessage(Lvl::INFO , f, __VA_ARGS__)
#else
#define log_debug(f, a...) logMessage(Lvl::DEBUG, f, ##a)
#define log_info(f, a...) logMessage(Lvl::INFO , f, ##a)
#endif

#endif


typedef std::chrono::high_resolution_clock log_clock_t;

static std::chrono::time_point<log_clock_t> start = log_clock_t::now();

/// Prints out a message with the current timestamp and code location.
inline void log_msg(Lvl level, int line, const char* func,
                   const char* file, const char* fmt, ...) {
  // Do not print messages below the current level.
  if (level < static_cast<Lvl>(LEVEL)) return;
  if (level >= static_cast<Lvl>(Lvl::NUM)) return;

  auto now = log_clock_t::now();

  va_list argptr;
  va_start(argptr, fmt);

  char buffer[1024];
  vsnprintf(buffer, 1024, fmt, argptr);

  std::string l = LvlStr[static_cast<uint8_t>(level)];
  fprintf(stderr, "[%04lu.%05lu]{%2u}::%s::%s:%d: %s\n",
          (unsigned long) std::chrono::duration_cast<std::chrono::seconds>(now - start).count(),
          (unsigned long) std::chrono::duration_cast<std::chrono::nanoseconds>(now - start).count(),
          FASTER::core::Thread::id(), l.c_str(), file, line, buffer);

  va_end(argptr);
}
