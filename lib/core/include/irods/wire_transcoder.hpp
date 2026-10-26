#ifndef IRODS_WIRE_TRANSCODER_HPP
#define IRODS_WIRE_TRANSCODER_HPP

#include "irods/rodsDef.h"
#include "irods/packStruct.h"

#include "buffer.hpp"

#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>
#include <utility>
#include <variant>

namespace irods
{
    template <typename E>
    struct unexpected
    {
        E error_value;
    };

    template <typename E>
    unexpected(E) -> unexpected<E>;

    /// Lightweight C++20 value-or-error result type for boundary transcoding.
    template <typename T, typename E = int>
    class expected
    {
    public:
        expected(T _val)
            : storage_(std::in_place_index<0>, std::move(_val))
        {
        }

        template <typename U>
        expected(unexpected<U> _err)
            : storage_(std::in_place_index<1>, static_cast<E>(_err.error_value))
        {
        }

        [[nodiscard]] constexpr auto has_value() const noexcept -> bool
        {
            return storage_.index() == 0;
        }

        [[nodiscard]] constexpr explicit operator bool() const noexcept
        {
            return has_value();
        }

        [[nodiscard]] auto value() & -> T&
        {
            return std::get<0>(storage_);
        }

        [[nodiscard]] auto value() const& -> const T&
        {
            return std::get<0>(storage_);
        }

        [[nodiscard]] auto value() && -> T&&
        {
            return std::get<0>(std::move(storage_));
        }

        [[nodiscard]] auto operator*() & -> T&
        {
            return std::get<0>(storage_);
        }

        [[nodiscard]] auto operator*() const& -> const T&
        {
            return std::get<0>(storage_);
        }

        [[nodiscard]] auto operator->() -> T*
        {
            return &std::get<0>(storage_);
        }

        [[nodiscard]] auto operator->() const -> const T*
        {
            return &std::get<0>(storage_);
        }

        [[nodiscard]] auto error() const noexcept -> E
        {
            return std::get<1>(storage_);
        }

    private:
        std::variant<T, E> storage_;
    };

    namespace wire
    {
        template <typename T, typename E = int>
        using expected = irods::expected<T, E>;

        /// Version-aware protocol negotiation.
        /// Upgrades to LITE3_PROT when both client and server peers are >= rods6.0.0
        /// unless explicitly pinned to XML_PROT or JSON_PROT.
        [[nodiscard]] auto negotiate_protocol(std::string_view _client_rel_version,
                                              std::string_view _server_rel_version,
                                              irodsProt_t _requested_prot = NATIVE_PROT,
                                              bool _explicit_env_override = false) noexcept -> irodsProt_t;

        /// Computes the exact used byte length of a Lite3 B-Tree buffer starting at `_base`.
        [[nodiscard]] auto compute_lite3_buffer_size(const std::uint8_t* _base,
                                                     std::size_t _max_bound = 64 * 1024 * 1024) noexcept -> std::size_t;

        /// Serializes an arbitrary iRODS C struct into a canonical lite3cpp::Buffer
        /// using the schema in RodsPackTable / `_pack_table` / API pack table.
        [[nodiscard]] auto struct_to_lite3(const void* _in_struct,
                                           std::string_view _pack_instruct_name,
                                           const packInstruct_t* _pack_table = nullptr,
                                           int _pack_flag = 0) -> expected<lite3cpp::Buffer, int>;

        /// Deserializes a canonical lite3cpp::Buffer into a newly allocated iRODS C struct
        /// compatible with free/clear routines and legacy API handlers.
        [[nodiscard]] auto lite3_to_struct(const lite3cpp::Buffer& _buf,
                                           void** _out_struct,
                                           std::string_view _pack_instruct_name,
                                           const packInstruct_t* _pack_table = nullptr) -> int;

        /// Ingress: Converts wire bytes from any protocol (LITE3_PROT, JSON_PROT, XML_PROT, NATIVE_PROT)
        /// into a canonical lite3cpp::Buffer.
        [[nodiscard]] auto decode_to_lite3(std::span<const std::byte> _wire_bytes,
                                           std::string_view _pack_instruct_name,
                                           irodsProt_t _wire_prot,
                                           std::string_view _peer_version = {},
                                           const packInstruct_t* _pack_table = nullptr) -> expected<lite3cpp::Buffer, int>;

        /// Egress: Converts a canonical lite3cpp::Buffer into wire bytes for the peer's protocol.
        [[nodiscard]] auto encode_from_lite3(const lite3cpp::Buffer& _msg_buf,
                                             std::string_view _pack_instruct_name,
                                             irodsProt_t _wire_prot,
                                             std::string_view _peer_version = {},
                                             const packInstruct_t* _pack_table = nullptr) -> expected<bytesBuf_t*, int>;
    } // namespace wire
} // namespace irods

#endif // IRODS_WIRE_TRANSCODER_HPP
