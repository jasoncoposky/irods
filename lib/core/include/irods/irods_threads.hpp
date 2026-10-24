#ifndef IRODS_THREADS_HPP
#define IRODS_THREADS_HPP

#include <condition_variable>
#include <mutex>
#include <thread>

/// =-=-=-=-=-=-=-
/// @brief C / C++ wrapper for client side threads
struct thread_context {
    std::thread*             reconnThr;
    std::mutex*              lock;
    std::condition_variable* cond;
};

#endif // IRODS_THREADS_HPP



