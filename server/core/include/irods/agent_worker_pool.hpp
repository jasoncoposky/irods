#ifndef IRODS_AGENT_WORKER_POOL_HPP
#define IRODS_AGENT_WORKER_POOL_HPP

/// \file

#include <ctime>
#include <functional>
#include <sys/types.h>
#include <vector>

namespace irods::server
{
    /// \brief Sends an open client socket file descriptor and session creation timestamp over a
    ///        UNIX domain socket using `SCM_RIGHTS` ancillary data.
    ///
    /// \param[in] _unix_sock_fd The UNIX domain socket (`AF_UNIX`) endpoint.
    /// \param[in] _client_fd    The accepted client socket descriptor to pass (or `< 0` for shutdown sentinel).
    /// \param[in] _created_at   The timestamp when the connection was accepted.
    /// \return 0 on success, or a negative iRODS error code on failure.
    auto send_session_fd(int _unix_sock_fd, int _client_fd, std::time_t _created_at) noexcept -> int;

    /// \brief Receives a client socket file descriptor and session creation timestamp over a
    ///        UNIX domain socket using `SCM_RIGHTS` ancillary data.
    ///
    /// \param[in]  _unix_sock_fd The UNIX domain socket (`AF_UNIX`) endpoint.
    /// \param[out] _client_fd    Receives the transferred file descriptor (or `-1` on EOF / shutdown).
    /// \param[out] _created_at   Receives the session creation timestamp.
    /// \return 0 on success, `DISCONN_STATUS` on clean EOF/shutdown, or a negative iRODS error code.
    auto recv_session_fd(int _unix_sock_fd, int& _client_fd, std::time_t& _created_at) noexcept -> int;

    /// \brief Pre-forked warm `irodsAgent` worker pool using UNIX domain socket (`SCM_RIGHTS`) FD passing.
    ///
    /// Eliminates per-connection `fork()` + plugin initialization + catalog connection overhead by
    /// maintaining a pool of pre-forked worker processes that accept client descriptors over a
    /// `socketpair(AF_UNIX, SOCK_STREAM, 0)`, sanitize session state between clients via
    /// `sanitize_agent_session`, and automatically recycle after `max_sessions_per_agent` sessions.
    class agent_worker_pool
    {
    public:
        using session_handler_fn = std::function<int(int /*client_fd*/, std::time_t /*created_at*/, bool /*is_last_session*/)>;

        struct config
        {
            int pool_size{0};
            int max_sessions_per_agent{1000};
            int close_fd_in_child{-1};
        };

        explicit agent_worker_pool(config _cfg, session_handler_fn _handler);
        ~agent_worker_pool() noexcept;

        agent_worker_pool(const agent_worker_pool&) = delete;
        auto operator=(const agent_worker_pool&) -> agent_worker_pool& = delete;

        /// \brief Pre-forks `pool_size` worker processes.
        ///
        /// If called in a forked child worker, runs the worker loop and calls `_exit(0)` when finished.
        /// In the parent (acceptor) process, returns 0 once all workers are spawned.
        auto start() -> int;

        /// \brief Dispatches an accepted client socket descriptor to an available worker in the pool.
        ///
        /// Automatically replenishes any worker that has exited after reaching `max_sessions_per_agent`.
        /// \return 0 if dispatched to a warm worker, or `< 0` if the pool is disabled or unavailable.
        auto dispatch_client(int _client_fd, std::time_t _created_at) -> int;

        /// \brief Gracefully stops all workers in the pool and reaps their processes.
        void stop() noexcept;

        [[nodiscard]] auto enabled() const noexcept -> bool
        {
            return cfg_.pool_size > 0;
        }

        [[nodiscard]] auto active_worker_count() const noexcept -> std::size_t;

    private:
        struct worker_slot
        {
            pid_t pid{-1};
            int parent_sock{-1};
            int sessions_dispatched{0};
            bool busy{false};
        };

        auto spawn_worker_in_slot(worker_slot& _slot) -> int;
        void run_worker_loop(int _worker_sock) noexcept;

        config cfg_;
        session_handler_fn handler_;
        std::vector<worker_slot> workers_;
        std::size_t next_worker_idx_{0};
        bool running_{false};
    };

} // namespace irods::server

#endif // IRODS_AGENT_WORKER_POOL_HPP
