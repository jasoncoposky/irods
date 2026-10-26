#include <catch2/catch_all.hpp>

#include "irods/rcConnect.h"
#include "irods/region.h"
#include "irods/rodsErrorTable.h"
#include "irods/irods_threads.hpp"

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdlib>
#include <cstring>

// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST_CASE("set_session_signature_client_side")
{
    RcComm comm{};

    constexpr auto sig_size = sizeof(RcComm::session_signature);
    REQUIRE(std::strncmp(comm.session_signature, "", sig_size) == 0);

    SECTION("input buffer satisfies requirements")
    {
        std::array<char, 16> buf{}; // NOLINT(cppcoreguidelines-avoid-magic-numbers, readability-magic-numbers)
        std::for_each(std::begin(buf), std::end(buf), [i = 0](auto& _v) mutable { _v = i++; });

        CHECK(set_session_signature_client_side(&comm, buf.data(), buf.size()) == 0);
        CHECK(std::strncmp(comm.session_signature, "000102030405060708090a0b0c0d0e0f", sig_size) == 0);
    }

    SECTION("returns error when input buffer is too small")
    {
        std::array<char, 15> buf{}; // NOLINT(cppcoreguidelines-avoid-magic-numbers, readability-magic-numbers)
        CHECK(set_session_signature_client_side(&comm, buf.data(), buf.size()) == SYS_INVALID_INPUT_PARAM);
        CHECK(std::strncmp(comm.session_signature, "", sig_size) == 0);
    }

    SECTION("returns error on null pointers")
    {
        std::array<char, 16> buf{}; // NOLINT(cppcoreguidelines-avoid-magic-numbers, readability-magic-numbers)

        CHECK(set_session_signature_client_side(nullptr, buf.data(), buf.size()) == SYS_INVALID_INPUT_PARAM);
        CHECK(std::strncmp(comm.session_signature, "", sig_size) == 0);

        CHECK(set_session_signature_client_side(&comm, nullptr, buf.size()) == SYS_INVALID_INPUT_PARAM);
        CHECK(std::strncmp(comm.session_signature, "", sig_size) == 0);
    }
}

TEST_CASE("rcDisconnect")
{
    SECTION("cleanRcComm and freeRcComm are idempotent and safe")
    {
        auto* otherconn = new rcComm_t{};
        otherconn->thread_ctx = new thread_context{};

        CHECK(cleanRcComm(otherconn) == 1);
        CHECK(cleanRcComm(otherconn) == 0);
        CHECK(freeRcComm(otherconn) == 0);
        CHECK(freeRcComm(nullptr) == 0);
    }
}

TEST_CASE("RAII RcComm and thread_context lifecycle")
{
    SECTION("stack-allocated RcComm and cleanRcComm clean up owned members")
    {
        RcComm comm{};
        comm.thread_ctx = new thread_context{};
        REQUIRE(comm.thread_ctx->lock != nullptr);
        REQUIRE(comm.thread_ctx->cond != nullptr);
        REQUIRE(comm.thread_ctx->reconnThr == nullptr);
        comm.svrVersion = static_cast<version_t*>(std::calloc(1, sizeof(version_t)));
        REQUIRE(comm.svrVersion != nullptr);
        CHECK(cleanRcComm(&comm) == 1);
        CHECK(comm.thread_ctx == nullptr);
        CHECK(comm.svrVersion == nullptr);
    }

    SECTION("heap-allocated rcComm_t via new and freeRcComm cleans up cleanly")
    {
        auto* conn = new rcComm_t{};
        conn->thread_ctx = new thread_context{};
        REQUIRE(conn->thread_ctx->lock != nullptr);
        REQUIRE(conn->thread_ctx->cond != nullptr);
        CHECK(freeRcComm(conn) == 0);
    }

    SECTION("irods::client_connection_handle manages rcComm_t pointer")
    {
        irods::client_connection_handle handle{nullptr};
        CHECK(!handle);
    }
}

TEST_CASE("std::pmr Region arena allocator")
{
    Region* r = make_region(0, nullptr);
    REQUIRE(r != nullptr);
    CHECK(region_size(r) == 0);

    auto* p1 = static_cast<unsigned char*>(region_alloc(r, 64));
    REQUIRE(p1 != nullptr);
    CHECK(reinterpret_cast<std::uintptr_t>(p1) % REGION_ALIGNMENT == 0);
    CHECK(IN_REGION(p1, r));
    CHECK(REGION_DESC(p1)->region == r);
    CHECK(REGION_DESC(p1)->size >= 64);
    CHECK(DELETED(p1) == 0);
    for (std::size_t i = 0; i < 64; ++i) {
        CHECK(p1[i] == 0);
    }

    SET_DELETE(p1);
    CHECK(DELETED(p1) == 1);

    const std::size_t size_after_p1 = region_size(r);
    CHECK(size_after_p1 == SIZE(p1));

    // Large allocation (> DEFAULT_BLOCK_SIZE) and multi-chunk growth
    auto* p2 = static_cast<unsigned char*>(region_alloc(r, DEFAULT_BLOCK_SIZE * 4));
    REQUIRE(p2 != nullptr);
    CHECK(reinterpret_cast<std::uintptr_t>(p2) % REGION_ALIGNMENT == 0);
    CHECK(IN_REGION(p2, r));
    CHECK(SIZE(p2) >= DEFAULT_BLOCK_SIZE * 4);
    CHECK(region_size(r) == size_after_p1 + SIZE(p2));

    region_free(r);
}
