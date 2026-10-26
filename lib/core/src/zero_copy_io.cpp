#ifndef _GNU_SOURCE
#  define _GNU_SOURCE
#endif

#include "irods/zero_copy_io.hpp"

#include "irods/irods_at_scope_exit.hpp"
#include "irods/rodsErrorTable.h"

#include <fcntl.h>
#include <sys/sendfile.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <vector>

namespace irods::io
{
    namespace
    {
        constexpr std::int64_t max_kernel_chunk_size = 0x40000000LL; // 1 GiB
        constexpr std::int64_t fallback_buffer_size = 1024LL * 1024LL; // 1 MiB

        auto write_all_fd(int _fd, const char* _buf, std::size_t _len) noexcept -> int
        {
            std::size_t total_written = 0;
            while (total_written < _len) {
                const ssize_t nw = ::write(_fd, _buf + total_written, _len - total_written);
                if (nw < 0) {
                    if (errno == EINTR) {
                        continue;
                    }
                    return UNIX_FILE_WRITE_ERR - errno;
                }
                if (nw == 0) {
                    return SYS_COPY_LEN_ERR;
                }
                total_written += static_cast<std::size_t>(nw);
            }
            return 0;
        }

        auto userspace_copy_loop(int _in_fd, int _out_fd, std::int64_t _remaining) noexcept -> std::int64_t
        {
            errno = 0;
            if (_remaining <= 0) {
                return 0;
            }

            try {
                const auto buf_sz = static_cast<std::size_t>(std::min<std::int64_t>(_remaining, fallback_buffer_size));
                std::vector<char> buf(buf_sz);

                std::int64_t copied = 0;
                while (copied < _remaining) {
                    const auto to_read =
                        static_cast<std::size_t>(std::min<std::int64_t>(_remaining - copied, buf.size()));
                    const ssize_t nr = ::read(_in_fd, buf.data(), to_read);
                    if (nr < 0) {
                        if (errno == EINTR) {
                            continue;
                        }
                        return UNIX_FILE_READ_ERR - errno;
                    }
                    if (nr == 0) {
                        break;
                    }

                    if (const int ec = write_all_fd(_out_fd, buf.data(), static_cast<std::size_t>(nr)); ec < 0) {
                        return ec;
                    }
                    copied += nr;
                }
                return copied;
            }
            catch (...) {
                return SYS_MALLOC_ERR;
            }
        }
    } // anonymous namespace

    auto copy_file_descriptors(int _in_fd, int _out_fd, std::int64_t _count) noexcept -> std::int64_t
    {
        if (_in_fd < 0 || _out_fd < 0 || _count < 0) {
            return SYS_INVALID_INPUT_PARAM;
        }
        if (_count == 0) {
            return 0;
        }

        std::int64_t total_copied = 0;

        // 1. Attempt kernel copy_file_range(2) (supports in-filesystem CoW/reflink & server-side copy).
        while (total_copied < _count) {
            const auto chunk = static_cast<std::size_t>(std::min<std::int64_t>(_count - total_copied, max_kernel_chunk_size));
            const ssize_t n = ::copy_file_range(_in_fd, nullptr, _out_fd, nullptr, chunk, 0);
            if (n < 0) {
                if (errno == EINTR) {
                    continue;
                }
                if (errno == EXDEV || errno == ENOSYS || errno == EOPNOTSUPP || errno == EINVAL ||
                    errno == EBADF || errno == EPERM)
                {
                    break;
                }
                return UNIX_FILE_WRITE_ERR - errno;
            }
            if (n == 0) {
                return total_copied;
            }
            total_copied += n;
        }

        // 2. Fallback to kernel sendfile(2) across different filesystems/mounts.
        while (total_copied < _count) {
            const auto chunk = static_cast<std::size_t>(std::min<std::int64_t>(_count - total_copied, max_kernel_chunk_size));
            const ssize_t n = ::sendfile(_out_fd, _in_fd, nullptr, chunk);
            if (n < 0) {
                if (errno == EINTR) {
                    continue;
                }
                if (errno == EINVAL || errno == ENOSYS || errno == EOPNOTSUPP || errno == EXDEV) {
                    break;
                }
                return UNIX_FILE_WRITE_ERR - errno;
            }
            if (n == 0) {
                return total_copied;
            }
            total_copied += n;
        }

        // 3. Fallback to user-space read/write loop if kernel zero-copy is unsupported for these FDs.
        if (total_copied < _count) {
            const std::int64_t rem = userspace_copy_loop(_in_fd, _out_fd, _count - total_copied);
            if (rem < 0) {
                return rem;
            }
            total_copied += rem;
        }

        return total_copied;
    }

    auto sendfile_to_socket(int _sock_fd, int _file_fd, std::int64_t _count) noexcept -> std::int64_t
    {
        if (_sock_fd < 0 || _file_fd < 0 || _count < 0) {
            return SYS_INVALID_INPUT_PARAM;
        }
        if (_count == 0) {
            return 0;
        }

        std::int64_t total_sent = 0;

        while (total_sent < _count) {
            const auto chunk = static_cast<std::size_t>(std::min<std::int64_t>(_count - total_sent, max_kernel_chunk_size));
            const ssize_t n = ::sendfile(_sock_fd, _file_fd, nullptr, chunk);
            if (n < 0) {
                if (errno == EINTR) {
                    continue;
                }
                if (errno == EINVAL || errno == ENOSYS || errno == EOPNOTSUPP || errno == ENOTSOCK) {
                    break;
                }
                return SYS_SOCK_WRITE_ERR - errno;
            }
            if (n == 0) {
                return total_sent;
            }
            total_sent += n;
        }

        if (total_sent < _count) {
            const std::int64_t rem = userspace_copy_loop(_file_fd, _sock_fd, _count - total_sent);
            if (rem < 0) {
                return rem;
            }
            total_sent += rem;
        }

        return total_sent;
    }

    auto splice_from_socket(int _sock_fd, int _file_fd, std::int64_t _count) noexcept -> std::int64_t
    {
        if (_sock_fd < 0 || _file_fd < 0 || _count < 0) {
            return SYS_INVALID_INPUT_PARAM;
        }
        if (_count == 0) {
            return 0;
        }

        int pfd[2]{-1, -1};
        if (::pipe2(pfd, O_CLOEXEC) < 0) {
            return userspace_copy_loop(_sock_fd, _file_fd, _count);
        }

        irods::at_scope_exit close_pipe{[&pfd] {
            if (pfd[0] >= 0) {
                ::close(pfd[0]);
            }
            if (pfd[1] >= 0) {
                ::close(pfd[1]);
            }
        }};

        std::int64_t total_written = 0;
        bool fallback_to_userspace = false;

        while (total_written < _count && !fallback_to_userspace) {
            const auto chunk = static_cast<std::size_t>(
                std::min<std::int64_t>(_count - total_written, fallback_buffer_size));

            const ssize_t spliced_in =
                ::splice(_sock_fd, nullptr, pfd[1], nullptr, chunk, SPLICE_F_MOVE | SPLICE_F_MORE);
            if (spliced_in < 0) {
                if (errno == EINTR) {
                    continue;
                }
                if (errno == EINVAL || errno == ENOSYS || errno == EOPNOTSUPP) {
                    fallback_to_userspace = true;
                    break;
                }
                return SYS_SOCK_READ_ERR - errno;
            }
            if (spliced_in == 0) {
                break;
            }

            ssize_t in_pipe = spliced_in;
            while (in_pipe > 0) {
                const ssize_t spliced_out = ::splice(
                    pfd[0], nullptr, _file_fd, nullptr, static_cast<std::size_t>(in_pipe), SPLICE_F_MOVE | SPLICE_F_MORE);
                if (spliced_out < 0) {
                    if (errno == EINTR) {
                        continue;
                    }
                    if (errno == EINVAL || errno == ENOSYS || errno == EOPNOTSUPP) {
                        // Drain remaining bytes currently inside the pipe into _file_fd via userspace.
                        const std::int64_t drained = userspace_copy_loop(pfd[0], _file_fd, in_pipe);
                        if (drained < 0) {
                            return drained;
                        }
                        if (drained != in_pipe) {
                            return SYS_COPY_LEN_ERR;
                        }
                        total_written += in_pipe;
                        in_pipe = 0;
                        fallback_to_userspace = true;
                        break;
                    }
                    return UNIX_FILE_WRITE_ERR - errno;
                }
                if (spliced_out == 0) {
                    return SYS_COPY_LEN_ERR;
                }
                in_pipe -= spliced_out;
                total_written += spliced_out;
            }
        }

        if (fallback_to_userspace && total_written < _count) {
            const std::int64_t rem = userspace_copy_loop(_sock_fd, _file_fd, _count - total_written);
            if (rem < 0) {
                return rem;
            }
            total_written += rem;
        }

        return total_written;
    }

} // namespace irods::io
