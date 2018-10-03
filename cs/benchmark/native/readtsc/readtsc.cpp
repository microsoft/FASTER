// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include <intrin.h>
#pragma intrinsic(__rdtsc)
extern "C"
__declspec(dllexport) unsigned __int64 __stdcall Rdtsc()
{
	return __rdtsc();
}
