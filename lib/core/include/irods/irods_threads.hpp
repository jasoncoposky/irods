#ifndef IRODS_THREADS_HPP
#define IRODS_THREADS_HPP

#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>

/// =-=-=-=-=-=-=-
/// @brief C / C++ wrapper for client side threads
struct thread_context {
    std::thread*             reconnThr{};
    std::mutex*              lock{};
    std::condition_variable* cond{};

    thread_context()
        : reconnThr{nullptr}
    {
        auto lk = std::make_unique<std::mutex>();
        auto cv = std::make_unique<std::condition_variable>();
        lock = lk.release();
        cond = cv.release();
    }

    ~thread_context() noexcept
    {
        if (reconnThr) {
            if (reconnThr->joinable()) {
                try {
                    reconnThr->join();
                }
                catch (...) {
                }
            }
            delete reconnThr;
            reconnThr = nullptr;
        }
        delete lock;
        lock = nullptr;
        delete cond;
        cond = nullptr;
    }

    thread_context(const thread_context&) = delete;
    auto operator=(const thread_context&) -> thread_context& = delete;
};

#endif // IRODS_THREADS_HPP



