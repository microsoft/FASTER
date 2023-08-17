#include "native_device.h"
#include <string>

extern "C" {
	__declspec(dllexport) NativeDevice* NativeDevice_Create(const char* file, bool enablePrivileges, bool unbuffered, bool delete_on_close) {
		return new NativeDevice(std::string(file), enablePrivileges, unbuffered, delete_on_close);
	}

	__declspec(dllexport) void NativeDevice_Destroy(NativeDevice* device) {
		delete device;
	}

	__declspec(dllexport) uint32_t NativeDevice_sector_size(NativeDevice* device) {
		return device->sector_size();
	}

	__declspec(dllexport) FASTER::core::Status NativeDevice_ReadAsync(NativeDevice* device, uint64_t source, void* dest, uint32_t length, FASTER::core::AsyncIOCallback callback, FASTER::core::IAsyncContext& context) {
		return device->ReadAsync(source, dest, length, callback, context);
	}

	__declspec(dllexport) FASTER::core::Status NativeDevice_WriteAsync(NativeDevice* device, const void* source, uint64_t dest, uint32_t length, FASTER::core::AsyncIOCallback callback, FASTER::core::IAsyncContext& context) {
		return device->WriteAsync(source, dest, length, callback, context);
	}

	__declspec(dllexport) void NativeDevice_CreateDir(NativeDevice* device, const char* dir) {
		device->CreateDir(std::string(dir));
	}

	__declspec(dllexport) bool NativeDevice_TryComplete(NativeDevice* device) {
		return device->TryComplete();
	}
}