#define _SILENCE_EXPERIMENTAL_FILESYSTEM_DEPRECATION_WARNING 1;
#include "file_system_disk.h"  

class NativeDevice {
public:
    typedef FASTER::environment::QueueIoHandler handler_t;
    typedef FASTER::device::FileSystemSegmentedFile<handler_t, 1073741824L> log_file_t;

private:

public:
    NativeDevice(const std::string& file,
        bool enablePrivileges = false,
        bool unbuffered = true,
        bool delete_on_close = false)
        : handler_{ 16 /*max threads*/ }
        , epoch_ { }
        , default_file_options_{ unbuffered, delete_on_close }
        , log_{ file, default_file_options_, &epoch_ } {
        FASTER::core::Status result = log_.Open(&handler_);
        assert(result == FASTER::core::Status::Ok);
    }

    ~NativeDevice() {
		FASTER::core::Status result = log_.Close();
		assert(result == FASTER::core::Status::Ok);
	}

    /// Methods required by the (implicit) disk interface.
    uint32_t sector_size() const {
        return static_cast<uint32_t>(log_.alignment());
    }

    FASTER::core::Status ReadAsync(uint64_t source, void* dest, uint32_t length, FASTER::core::AsyncIOCallback callback,
        FASTER::core::IAsyncContext& context) const {
        return log_.ReadAsync(source, dest, length, callback, context);
    }

    FASTER::core::Status WriteAsync(const void* source, uint64_t dest, uint32_t length,
        FASTER::core::AsyncIOCallback callback, FASTER::core::IAsyncContext& context) {
        return log_.WriteAsync(source, dest, length, callback, context);
    }

    const log_file_t& log() const {
        return log_;
    }
    log_file_t& log() {
        return log_;
    }

    void CreateDir(const std::string& dir) {
        std::experimental::filesystem::path path{ dir };
        try {
            std::experimental::filesystem::remove_all(path);
        }
        catch (std::experimental::filesystem::filesystem_error&) {
            // Ignore; throws when path doesn't exist yet.
        }
        std::experimental::filesystem::create_directories(path);
    }

    /// Implementation-specific accessor.
    handler_t& handler() {
        return handler_;
    }

    bool TryComplete() {
        return handler_.TryComplete();
    }

private:
    FASTER::core::LightEpoch epoch_;
    handler_t handler_;
    FASTER::environment::FileOptions default_file_options_;

    /// Store the data
    log_file_t log_;
};