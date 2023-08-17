#define _SILENCE_EXPERIMENTAL_FILESYSTEM_DEPRECATION_WARNING 1;
#include "file_system_disk.h"  

class NativeDevice {
public:
    typedef FASTER::environment::QueueIoHandler handler_t;
    typedef FASTER::device::FileSystemSegmentedFile<handler_t, 1073741824L> log_file_t;

private:
    class AsyncIoContext : public FASTER::core::IAsyncContext {
    public:
        AsyncIoContext(void* context_, FASTER::core::AsyncIOCallback callback_)
            : context{ context_ },
            callback{ callback_ } {
        }

        /// The deep-copy constructor
        AsyncIoContext(AsyncIoContext& other)
            : context{ other.context},
            callback{ other.callback } {
        }

    protected:
        FASTER::core::Status DeepCopy_Internal(IAsyncContext*& context_copy) final {
            return IAsyncContext::DeepCopy_Internal(*this, context_copy);
        }

    public:
        void* context;
        FASTER::core::AsyncIOCallback callback;
    };

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

    FASTER::core::Status ReadAsync(uint64_t source, void* dest, uint32_t length, FASTER::core::AsyncIOCallback callback, void* context) const {
        AsyncIoContext io_context{ context, callback };
        auto callback_ = [](FASTER::core::IAsyncContext* ctxt, FASTER::core::Status result, size_t bytes_transferred) {
            FASTER::core::CallbackContext<AsyncIoContext> context{ ctxt };
            context->callback((FASTER::core::IAsyncContext*)context->context, result, bytes_transferred);
            };
        return log_.ReadAsync(source, dest, length, callback_, io_context);
    }

    FASTER::core::Status WriteAsync(const void* source, uint64_t dest, uint32_t length, FASTER::core::AsyncIOCallback callback, void* context) {
        AsyncIoContext io_context{ context, callback };
        auto callback_ = [](FASTER::core::IAsyncContext* ctxt, FASTER::core::Status result, size_t bytes_transferred) {
            FASTER::core::CallbackContext<AsyncIoContext> context{ ctxt };
            context->callback((FASTER::core::IAsyncContext*)context->context, result, bytes_transferred);
            };
        return log_.WriteAsync(source, dest, length, callback_, io_context);
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