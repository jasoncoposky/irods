#include "irods/agent_worker_pool.hpp"

#include "irods/irods_logger.hpp"
#include "irods/rodsDef.h"
#include "irods/rodsErrorTable.h"
#include "irods/rsApiHandler.hpp"
#include "irods/sockComm.h"

#include <poll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <utility>

namespace irods::server
{
    namespace
    {
        using log_af = irods::experimental::log::agent_factory;
        using log_agent = irods::experimental::log::agent;

        struct fd_message_payload
        {
            std::int64_t created_at{0};
            std::int32_t shutdown_flag{0};
        };
    } // anonymous namespace

    auto send_session_fd(int _unix_sock_fd, int _client_fd, std::time_t _created_at) noexcept -> int
    {
        if (_unix_sock_fd < 0) {
            return SYS_INVALID_INPUT_PARAM;
        }

        fd_message_payload payload{};
        payload.created_at = static_cast<std::int64_t>(_created_at);
        payload.shutdown_flag = (_client_fd < 0) ? 1 : 0;

        struct iovec iov{};
        iov.iov_base = &payload;
        iov.iov_len = sizeof(payload);

        struct msghdr msg{};
        msg.msg_iov = &iov;
        msg.msg_iovlen = 1;

        alignas(struct cmsghdr) char cmsg_buf[CMSG_SPACE(sizeof(int))]{};
        if (_client_fd >= 0) {
            msg.msg_control = cmsg_buf;
            msg.msg_controllen = sizeof(cmsg_buf);

            struct cmsghdr* cmsg = CMSG_FIRSTHDR(&msg);
            cmsg->cmsg_level = SOL_SOCKET;
            cmsg->cmsg_type = SCM_RIGHTS;
            cmsg->cmsg_len = CMSG_LEN(sizeof(int));
            std::memcpy(CMSG_DATA(cmsg), &_client_fd, sizeof(int));
        }

        while (true) {
            const ssize_t sent = ::sendmsg(_unix_sock_fd, &msg, MSG_NOSIGNAL);
            if (sent < 0) {
                if (errno == EINTR) {
                    continue;
                }
                return SYS_SOCK_WRITE_ERR - errno;
            }
            break;
        }

        return 0;
    }

    auto recv_session_fd(int _unix_sock_fd, int& _client_fd, std::time_t& _created_at) noexcept -> int
    {
        _client_fd = -1;
        _created_at = 0;

        if (_unix_sock_fd < 0) {
            return SYS_INVALID_INPUT_PARAM;
        }

        fd_message_payload payload{};
        struct iovec iov{};
        iov.iov_base = &payload;
        iov.iov_len = sizeof(payload);

        alignas(struct cmsghdr) char cmsg_buf[CMSG_SPACE(sizeof(int))]{};
        struct msghdr msg{};
        msg.msg_iov = &iov;
        msg.msg_iovlen = 1;
        msg.msg_control = cmsg_buf;
        msg.msg_controllen = sizeof(cmsg_buf);

        ssize_t nread = 0;
        while (true) {
            nread = ::recvmsg(_unix_sock_fd, &msg, 0);
            if (nread < 0) {
                if (errno == EINTR) {
                    continue;
                }
                return SYS_SOCK_READ_ERR - errno;
            }
            break;
        }

        if (nread == 0) {
            return DISCONN_STATUS;
        }

        _created_at = static_cast<std::time_t>(payload.created_at);

        if (payload.shutdown_flag != 0) {
            return DISCONN_STATUS;
        }

        struct cmsghdr* cmsg = CMSG_FIRSTHDR(&msg);
        if ((msg.msg_flags & MSG_CTRUNC) || !cmsg || cmsg->cmsg_level != SOL_SOCKET ||
            cmsg->cmsg_type != SCM_RIGHTS || cmsg->cmsg_len != CMSG_LEN(sizeof(int)))
        {
            return SYS_SOCK_READ_ERR;
        }

        std::memcpy(&_client_fd, CMSG_DATA(cmsg), sizeof(int));
        return 0;
    }

    agent_worker_pool::agent_worker_pool(config _cfg, session_handler_fn _handler)
        : cfg_{_cfg}
        , handler_{std::move(_handler)}
    {
        if (cfg_.max_sessions_per_agent <= 0) {
            cfg_.max_sessions_per_agent = 1000;
        }
    }

    agent_worker_pool::~agent_worker_pool() noexcept
    {
        stop();
    }

    auto agent_worker_pool::start() -> int
    {
        if (cfg_.pool_size <= 0) {
            return 0;
        }

        workers_.resize(static_cast<std::size_t>(cfg_.pool_size));
        for (auto& slot : workers_) {
            if (const int ec = spawn_worker_in_slot(slot); ec < 0) {
                return ec;
            }
        }

        running_ = true;
        return 0;
    }

    auto agent_worker_pool::spawn_worker_in_slot(worker_slot& _slot) -> int
    {
        if (_slot.parent_sock >= 0) {
            ::close(_slot.parent_sock);
            _slot.parent_sock = -1;
        }

        int sv[2]{-1, -1};
        if (::socketpair(AF_UNIX, SOCK_STREAM, 0, sv) < 0) {
            return SYS_SOCK_OPEN_ERR - errno;
        }

        const pid_t pid = ::fork();
        if (pid < 0) {
            const int saved_err = errno;
            ::close(sv[0]);
            ::close(sv[1]);
            return SYS_FORK_ERROR - saved_err;
        }

        if (pid == 0) {
            // Child worker process: close listening FD, parent end, and all other worker parent sockets.
            if (cfg_.close_fd_in_child >= 0) {
                ::close(cfg_.close_fd_in_child);
            }
            ::close(sv[0]);
            for (auto& other : workers_) {
                if (other.parent_sock >= 0) {
                    ::close(other.parent_sock);
                    other.parent_sock = -1;
                }
            }
            run_worker_loop(sv[1]);
            ::close(sv[1]);
            ::_exit(0);
        }

        // Parent (acceptor) process:
        ::close(sv[1]);
        _slot.pid = pid;
        _slot.parent_sock = sv[0];
        _slot.sessions_dispatched = 0;
        _slot.busy = false;
        return 0;
    }

    void agent_worker_pool::run_worker_loop(int _worker_sock) noexcept
    {
        int sessions_handled = 0;
        while (sessions_handled < cfg_.max_sessions_per_agent) {
            int client_fd = -1;
            std::time_t created_at = 0;
            const int recv_ec = recv_session_fd(_worker_sock, client_fd, created_at);
            if (recv_ec < 0 || client_fd < 0) {
                break;
            }

            ++sessions_handled;
            const bool is_last_session = (sessions_handled >= cfg_.max_sessions_per_agent);

            try {
                if (handler_) {
                    handler_(client_fd, created_at, is_last_session);
                }
                else {
                    ::close(client_fd);
                }
            }
            catch (...) {
                ::close(client_fd);
                break;
            }

            // Send 1-byte completion/ready ACK back to parent so parent knows worker is healthy/idle.
            const std::uint8_t ack = is_last_session ? 0 : 1;
            (void) ::send(_worker_sock, &ack, sizeof(ack), MSG_NOSIGNAL);
        }
    }

    auto agent_worker_pool::dispatch_client(int _client_fd, std::time_t _created_at) -> int
    {
        if (!running_ || workers_.empty() || _client_fd < 0) {
            return SYS_INVALID_INPUT_PARAM;
        }

        const std::size_t n = workers_.size();
        for (std::size_t attempt = 0; attempt < n * 2; ++attempt) {
            auto& slot = workers_[next_worker_idx_ % n];
            next_worker_idx_ = (next_worker_idx_ + 1) % n;

            if (slot.pid <= 0 || slot.parent_sock < 0) {
                if (spawn_worker_in_slot(slot) < 0) {
                    continue;
                }
            }

            if (slot.busy) {
                if (attempt >= n) {
                    struct pollfd pfd{};
                    pfd.fd = slot.parent_sock;
                    pfd.events = POLLIN;
                    (void) ::poll(&pfd, 1, 50);
                }

                std::uint8_t ack = 0;
                ssize_t nread = 0;
                while (true) {
                    nread = ::recv(slot.parent_sock, &ack, sizeof(ack), MSG_DONTWAIT);
                    if (nread < 0 && errno == EINTR) {
                        continue;
                    }
                    break;
                }

                if (nread == 1 && ack == 1 && slot.sessions_dispatched < cfg_.max_sessions_per_agent) {
                    slot.busy = false;
                }
                else if (nread < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                    // Worker is still actively serving a client session; try next worker slot.
                    continue;
                }
                else {
                    // Worker completed its final session (ack == 0) or closed its socket / exited.
                    if (slot.pid > 0) {
                        int status = 0;
                        (void) ::waitpid(slot.pid, &status, WNOHANG);
                        slot.pid = -1;
                    }
                    if (spawn_worker_in_slot(slot) < 0) {
                        continue;
                    }
                }
            }
            else {
                std::uint8_t peek_byte = 0;
                const ssize_t peek_res =
                    ::recv(slot.parent_sock, &peek_byte, sizeof(peek_byte), MSG_PEEK | MSG_DONTWAIT);
                int status = 0;
                const pid_t exited = (slot.pid > 0) ? ::waitpid(slot.pid, &status, WNOHANG) : -1;
                if (peek_res == 0 || exited == slot.pid || slot.sessions_dispatched >= cfg_.max_sessions_per_agent) {
                    slot.pid = -1;
                    if (spawn_worker_in_slot(slot) < 0) {
                        continue;
                    }
                }
            }

            if (send_session_fd(slot.parent_sock, _client_fd, _created_at) == 0) {
                ++slot.sessions_dispatched;
                slot.busy = true;
                return 0;
            }

            // If sendmsg failed (e.g. broken pipe), respawn the slot and retry.
            if (slot.pid > 0) {
                int status = 0;
                (void) ::waitpid(slot.pid, &status, WNOHANG);
                slot.pid = -1;
            }
            (void) spawn_worker_in_slot(slot);
        }

        return SYS_SOCK_WRITE_ERR;
    }

    void agent_worker_pool::stop() noexcept
    {
        if (!running_ && workers_.empty()) {
            return;
        }

        for (auto& slot : workers_) {
            if (slot.parent_sock >= 0) {
                (void) send_session_fd(slot.parent_sock, -1, 0);
                ::close(slot.parent_sock);
                slot.parent_sock = -1;
            }
        }

        for (auto& slot : workers_) {
            if (slot.pid > 0) {
                int status = 0;
                while (::waitpid(slot.pid, &status, 0) < 0 && errno == EINTR) {
                }
                slot.pid = -1;
            }
        }

        workers_.clear();
        running_ = false;
    }

    auto agent_worker_pool::active_worker_count() const noexcept -> std::size_t
    {
        std::size_t count = 0;
        for (const auto& slot : workers_) {
            if (slot.pid > 0 && slot.parent_sock >= 0) {
                ++count;
            }
        }
        return count;
    }

} // namespace irods::server
