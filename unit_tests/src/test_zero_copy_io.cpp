#include <catch2/catch_all.hpp>

#include "irods/irods_at_scope_exit.hpp"
#include "irods/rodsErrorTable.h"
#include "irods/zero_copy_io.hpp"

#include <fcntl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <cstdint>
#include <cstring>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

namespace fs = std::filesystem;

namespace
{
    auto create_temp_file_with_content(const fs::path& _path, const std::vector<char>& _data) -> void
    {
        const int fd = ::open(_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
        REQUIRE(fd >= 0);
        irods::at_scope_exit close_fd{[fd] { ::close(fd); }};

        std::size_t written = 0;
        while (written < _data.size()) {
            const ssize_t nw = ::write(fd, _data.data() + written, _data.size() - written);
            REQUIRE(nw > 0);
            written += static_cast<std::size_t>(nw);
        }
    }

    auto read_entire_file(const fs::path& _path) -> std::vector<char>
    {
        struct stat st{};
        REQUIRE(::stat(_path.c_str(), &st) == 0);

        std::vector<char> out(static_cast<std::size_t>(st.st_size));
        const int fd = ::open(_path.c_str(), O_RDONLY);
        REQUIRE(fd >= 0);
        irods::at_scope_exit close_fd{[fd] { ::close(fd); }};

        std::size_t total_read = 0;
        while (total_read < out.size()) {
            const ssize_t nr = ::read(fd, out.data() + total_read, out.size() - total_read);
            REQUIRE(nr > 0);
            total_read += static_cast<std::size_t>(nr);
        }
        return out;
    }
} // anonymous namespace

TEST_CASE("Zero-copy copy_file_descriptors copies empty, small, and multi-megabyte files", "[zero_copy][io]")
{
    const fs::path tmp_dir = fs::temp_directory_path() / ("irods_zero_copy_test_" + std::to_string(::getpid()));
    fs::create_directories(tmp_dir);
    irods::at_scope_exit cleanup_dir{[&tmp_dir] {
        std::error_code ec;
        fs::remove_all(tmp_dir, ec);
    }};

    SECTION("Invalid arguments return SYS_INVALID_INPUT_PARAM")
    {
        CHECK(irods::io::copy_file_descriptors(-1, 1, 10) == SYS_INVALID_INPUT_PARAM);
        CHECK(irods::io::copy_file_descriptors(1, -1, 10) == SYS_INVALID_INPUT_PARAM);
        CHECK(irods::io::copy_file_descriptors(1, 1, -1) == SYS_INVALID_INPUT_PARAM);

        CHECK(irods::io::sendfile_to_socket(-1, 1, 10) == SYS_INVALID_INPUT_PARAM);
        CHECK(irods::io::sendfile_to_socket(1, -1, 10) == SYS_INVALID_INPUT_PARAM);
        CHECK(irods::io::sendfile_to_socket(1, 1, -1) == SYS_INVALID_INPUT_PARAM);
        CHECK(irods::io::sendfile_to_socket(1, 1, 0) == 0);

        CHECK(irods::io::splice_from_socket(-1, 1, 10) == SYS_INVALID_INPUT_PARAM);
        CHECK(irods::io::splice_from_socket(1, -1, 10) == SYS_INVALID_INPUT_PARAM);
        CHECK(irods::io::splice_from_socket(1, 1, -1) == SYS_INVALID_INPUT_PARAM);
        CHECK(irods::io::splice_from_socket(1, 1, 0) == 0);
    }

    SECTION("Empty file (0 bytes)")
    {
        const fs::path src = tmp_dir / "empty_src.bin";
        const fs::path dst = tmp_dir / "empty_dst.bin";
        create_temp_file_with_content(src, {});

        const int in_fd = ::open(src.c_str(), O_RDONLY);
        const int out_fd = ::open(dst.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
        REQUIRE(in_fd >= 0);
        REQUIRE(out_fd >= 0);
        irods::at_scope_exit close_fds{[in_fd, out_fd] {
            ::close(in_fd);
            ::close(out_fd);
        }};

        CHECK(irods::io::copy_file_descriptors(in_fd, out_fd, 0) == 0);
        CHECK(read_entire_file(dst).empty());
    }

    SECTION("Multi-megabyte deterministic binary payload (3.5 MiB)")
    {
        constexpr std::size_t payload_size = 3 * 1024 * 1024 + 512 * 1024 + 137;
        std::vector<char> expected(payload_size);
        for (std::size_t i = 0; i < payload_size; ++i) {
            expected[i] = static_cast<char>((i * 131U + 17U) & 0xFFU);
        }

        const fs::path src = tmp_dir / "large_src.bin";
        const fs::path dst = tmp_dir / "large_dst.bin";
        create_temp_file_with_content(src, expected);

        const int in_fd = ::open(src.c_str(), O_RDONLY);
        const int out_fd = ::open(dst.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
        REQUIRE(in_fd >= 0);
        REQUIRE(out_fd >= 0);
        irods::at_scope_exit close_fds{[in_fd, out_fd] {
            ::close(in_fd);
            ::close(out_fd);
        }};

        const std::int64_t copied =
            irods::io::copy_file_descriptors(in_fd, out_fd, static_cast<std::int64_t>(payload_size));
        REQUIRE(copied == static_cast<std::int64_t>(payload_size));

        const auto actual = read_entire_file(dst);
        REQUIRE(actual.size() == expected.size());
        CHECK(actual == expected);
    }

    SECTION("Pipe descriptor pair exercises automatic userspace fallback cleanly")
    {
        int pfd[2]{-1, -1};
        REQUIRE(::pipe(pfd) == 0);
        irods::at_scope_exit close_pipe{[&pfd] {
            if (pfd[0] >= 0) {
                ::close(pfd[0]);
            }
            if (pfd[1] >= 0) {
                ::close(pfd[1]);
            }
        }};

        const std::string msg = "zero-copy fallback verification across pipe descriptor";
        REQUIRE(::write(pfd[1], msg.data(), msg.size()) == static_cast<ssize_t>(msg.size()));
        ::close(pfd[1]);
        pfd[1] = -1;

        const fs::path dst = tmp_dir / "pipe_dst.bin";
        const int out_fd = ::open(dst.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
        REQUIRE(out_fd >= 0);
        irods::at_scope_exit close_out{[out_fd] { ::close(out_fd); }};

        const std::int64_t copied =
            irods::io::copy_file_descriptors(pfd[0], out_fd, static_cast<std::int64_t>(msg.size()));
        REQUIRE(copied == static_cast<std::int64_t>(msg.size()));

        const auto actual = read_entire_file(dst);
        CHECK(std::string(actual.begin(), actual.end()) == msg);
    }
}

TEST_CASE("Zero-copy sendfile_to_socket and splice_from_socket stream over socketpair", "[zero_copy][io]")
{
    const fs::path tmp_dir = fs::temp_directory_path() / ("irods_zero_copy_sock_" + std::to_string(::getpid()));
    fs::create_directories(tmp_dir);
    irods::at_scope_exit cleanup_dir{[&tmp_dir] {
        std::error_code ec;
        fs::remove_all(tmp_dir, ec);
    }};

    constexpr std::size_t payload_size = 2 * 1024 * 1024 + 777;
    std::vector<char> expected(payload_size);
    for (std::size_t i = 0; i < payload_size; ++i) {
        expected[i] = static_cast<char>((i * 73U + 29U) & 0xFFU);
    }

    const fs::path src = tmp_dir / "net_src.bin";
    const fs::path dst = tmp_dir / "net_dst.bin";
    create_temp_file_with_content(src, expected);

    int sv[2]{-1, -1};
    REQUIRE(::socketpair(AF_UNIX, SOCK_STREAM, 0, sv) == 0);
    irods::at_scope_exit close_sv{[&sv] {
        if (sv[0] >= 0) {
            ::close(sv[0]);
        }
        if (sv[1] >= 0) {
            ::close(sv[1]);
        }
    }};

    std::int64_t bytes_sent = -1;
    std::thread sender{[&] {
        const int in_fd = ::open(src.c_str(), O_RDONLY);
        if (in_fd >= 0) {
            bytes_sent = irods::io::sendfile_to_socket(sv[0], in_fd, static_cast<std::int64_t>(payload_size));
            ::close(in_fd);
        }
        ::shutdown(sv[0], SHUT_WR);
    }};

    const int out_fd = ::open(dst.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
    REQUIRE(out_fd >= 0);
    const std::int64_t bytes_spliced =
        irods::io::splice_from_socket(sv[1], out_fd, static_cast<std::int64_t>(payload_size));
    ::close(out_fd);

    sender.join();

    REQUIRE(bytes_sent == static_cast<std::int64_t>(payload_size));
    REQUIRE(bytes_spliced == static_cast<std::int64_t>(payload_size));

    const auto actual = read_entire_file(dst);
    REQUIRE(actual.size() == expected.size());
    CHECK(actual == expected);
}
