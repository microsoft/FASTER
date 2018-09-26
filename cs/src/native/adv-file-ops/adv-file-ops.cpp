// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include <windows.h>
#include <string>
#include <sstream>
#include <iostream>
#include <iomanip>

std::string FormatWin32AndHRESULT(DWORD win32_result) {
  std::stringstream ss;
  ss << "Win32(" << win32_result << ") HRESULT("
     << std::showbase << std::uppercase << std::setfill('0') << std::hex
     << HRESULT_FROM_WIN32(win32_result) << ")";
  return ss.str();
}

extern "C"
__declspec(dllexport) bool DoEnableProcessPrivileges() {
	HANDLE token;

	TOKEN_PRIVILEGES token_privileges;
	token_privileges.PrivilegeCount = 1;
	token_privileges.Privileges[0].Attributes = SE_PRIVILEGE_ENABLED;

	if (!LookupPrivilegeValue(0, SE_MANAGE_VOLUME_NAME,
		&token_privileges.Privileges[0].Luid)) return false;
	if (!OpenProcessToken(GetCurrentProcess(), TOKEN_ADJUST_PRIVILEGES, &token)) return false;
	if (!AdjustTokenPrivileges(token, 0, (PTOKEN_PRIVILEGES)&token_privileges, 0, 0, 0)) return false;
	if (GetLastError() != ERROR_SUCCESS) return false;

	::CloseHandle(token);

	return true;
}

extern "C"
__declspec(dllexport) bool DoEnableVolumePrivileges(std::string& filename, HANDLE file_handle)
{
	std::string volume_string = "\\\\.\\" + filename.substr(0, 2);
	HANDLE volume_handle = ::CreateFile(volume_string.c_str(), 0, 0, nullptr, OPEN_EXISTING,
		FILE_ATTRIBUTE_NORMAL, nullptr);
	if (INVALID_HANDLE_VALUE == volume_handle) {
		return false;
	}

	MARK_HANDLE_INFO mhi;
	mhi.UsnSourceInfo = USN_SOURCE_DATA_MANAGEMENT;
	mhi.VolumeHandle = volume_handle;
	mhi.HandleInfo = MARK_HANDLE_PROTECT_CLUSTERS;

	DWORD bytes_returned = 0;
	BOOL result = DeviceIoControl(file_handle, FSCTL_MARK_HANDLE, &mhi, sizeof(MARK_HANDLE_INFO), nullptr,
		0, &bytes_returned, nullptr);

	if (!result) {
		return false;
	}

	::CloseHandle(volume_handle);
	return true;
}


extern "C"
__declspec(dllexport) bool DoSetFileSize(HANDLE file_handle, int64_t file_size)
{
	LARGE_INTEGER li;
	li.QuadPart = file_size;

	BOOL result = ::SetFilePointerEx(file_handle, li, NULL, FILE_BEGIN);
	if (!result) {
		return false;
	}

	// Set a fixed file length
	result = ::SetEndOfFile(file_handle);
	if (!result) {
		return false;
	}

	result = ::SetFileValidData(file_handle, file_size);
	if (!result) {
		return false;
	}
	return true;
}

extern "C"
__declspec(dllexport) bool DoCreateAndSetFileSize(std::string& filename, int64_t file_size)
{
	BOOL result = ::DoEnableProcessPrivileges();
	if (!result) {
		return false;
	}

	DWORD desired_access = GENERIC_READ | GENERIC_WRITE;
	DWORD const flags = FILE_FLAG_RANDOM_ACCESS | FILE_FLAG_NO_BUFFERING;
	DWORD create_disposition = CREATE_ALWAYS;
	DWORD shared_mode = FILE_SHARE_READ;

	// Create our test file
	HANDLE file_handle = ::CreateFile(filename.c_str(), desired_access, shared_mode, NULL,
		create_disposition, flags, NULL);
	if (INVALID_HANDLE_VALUE == file_handle) {
		return false;
	}

	result = ::DoEnableVolumePrivileges(filename, file_handle);
	if (!result) {
		return false;
	}

	result = ::DoSetFileSize(file_handle, file_size);
	if (!result) {
		return false;
	}

	::CloseHandle(file_handle);

	return true;
}


