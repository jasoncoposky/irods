#ifndef IRODS_ZERO_COPY_IO_HPP
#define IRODS_ZERO_COPY_IO_HPP

/// \file

#include <cstdint>

namespace irods::io
{
    /// \brief Copies `_count` bytes from `_in_fd` to `_out_fd` using Linux kernel zero-copy primitives.
    ///
    /// Attempts `copy_file_range(2)` first (enabling filesystem reflink/CoW or server-side copy),
    /// then `sendfile(2)` (page-cache to descriptor within the kernel), and finally falls back to
    /// an `EINTR`-safe user-space buffer loop if the descriptor pair or filesystem does not support
    /// kernel zero-copy.
    ///
    /// \param[in] _in_fd  Source file descriptor opened for reading.
    /// \param[in] _out_fd Destination file descriptor opened for writing.
    /// \param[in] _count  Number of bytes to copy.
    /// \return Total number of bytes copied on success, or a negative iRODS error code on failure.
    auto copy_file_descriptors(int _in_fd, int _out_fd, std::int64_t _count) noexcept -> std::int64_t;

    /// \brief Streams `_count` bytes from file descriptor `_file_fd` to socket `_sock_fd` using `sendfile(2)`.
    ///
    /// Automatically falls back to user-space `read`/`write` if the underlying filesystem or socket
    /// descriptor does not support `sendfile(2)`.
    ///
    /// \param[in] _sock_fd Connected destination socket descriptor.
    /// \param[in] _file_fd Source file descriptor opened for reading.
    /// \param[in] _count   Number of bytes to transfer.
    /// \return Total number of bytes sent on success, or a negative iRODS error code on failure.
    auto sendfile_to_socket(int _sock_fd, int _file_fd, std::int64_t _count) noexcept -> std::int64_t;

    /// \brief Streams `_count` bytes from socket `_sock_fd` to file descriptor `_file_fd` using `splice(2)`.
    ///
    /// Moves pages from the socket receive buffer through an in-kernel pipe into `_file_fd` without
    /// copying payload bytes into user space. Automatically falls back to user-space `read`/`write`
    /// if `splice(2)` is unsupported for either descriptor.
    ///
    /// \param[in] _sock_fd Connected source socket descriptor.
    /// \param[in] _file_fd Destination file descriptor opened for writing.
    /// \param[in] _count   Number of bytes to transfer.
    /// \return Total number of bytes written on success, or a negative iRODS error code on failure.
    auto splice_from_socket(int _sock_fd, int _file_fd, std::int64_t _count) noexcept -> std::int64_t;

} // namespace irods::io

namespace irods::experimental::io
{
    namespace zero_copy = ::irods::io;
} // namespace irods::experimental::io

#endif // IRODS_ZERO_COPY_IO_HPP
