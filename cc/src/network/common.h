// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

/// Convenience macro. Throws an exception if the argument is *NOT* zero.
#define EXCEPT_NZ(x, s)                    \
  do {                                     \
    if (x) throw std::runtime_error(s);    \
  } while (0)

/// Convenience macro. Throws an exception if the argument is zero.
#define EXCEPT_Z(x, s)                     \
  do {                                     \
    if (!x) throw std::runtime_error(s);   \
  } while (0)
