#ifndef IRODS_PROTOCOL_LITE3_STRUCTS_HPP
#define IRODS_PROTOCOL_LITE3_STRUCTS_HPP

#include "irods/wire_transcoder.hpp"
#include "irods/dataObjInpOut.h"
#include "irods/dataObjCopy.h"
#include "irods/execCmd.h"
#include "irods/modAVUMetadata.h"
#include "irods/objInfo.h"
#include "irods/objStat.h"
#include "irods/rcGlobalExtern.h"
#include "irods/rcMisc.h"
#include "irods/rodsErrorTable.h"
#include "irods/rodsPackInstruct.h"

#include "buffer.hpp"
#include "value.hpp"

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace irods::protocol
{
    namespace detail
    {
        inline auto get_str_or(const lite3cpp::Buffer& _buf,
                               std::size_t _ofs,
                               std::string_view _key,
                               std::string_view _default = {}) noexcept -> std::string_view
        {
            if (_buf.get_type(_ofs, _key) == lite3cpp::Type::String) {
                return _buf.get_str(_ofs, _key);
            }
            return _default;
        }

        inline auto get_i64_or(const lite3cpp::Buffer& _buf,
                               std::size_t _ofs,
                               std::string_view _key,
                               std::int64_t _default = 0) noexcept -> std::int64_t
        {
            if (_buf.get_type(_ofs, _key) == lite3cpp::Type::Int64) {
                return _buf.get_i64(_ofs, _key);
            }
            return _default;
        }

        inline auto ensure_sub_obj(lite3cpp::Buffer& _buf, std::size_t _ofs, std::string_view _key) -> std::size_t
        {
            const auto existing_type = _buf.get_type(_ofs, _key);
            if (existing_type == lite3cpp::Type::Object) {
                return _buf.get_obj(_ofs, _key);
            }
            if (existing_type != lite3cpp::Type::Invalid) {
                // Force allocation of a fresh 96-byte node when replacing a non-Object (e.g., Type::Null) entry
                _buf.set_bool(_ofs, _key, false);
            }
            return _buf.set_obj(_ofs, _key);
        }

        inline void copy_lite3_array(const lite3cpp::Buffer& _src,
                                     std::size_t _src_arr,
                                     lite3cpp::Buffer& _dst,
                                     std::size_t _dst_arr);

        inline void copy_lite3_object(const lite3cpp::Buffer& _src,
                                      std::size_t _src_ofs,
                                      lite3cpp::Buffer& _dst,
                                      std::size_t _dst_ofs)
        {
            if (_src.size() < lite3cpp::config::node_size || _src_ofs + lite3cpp::config::node_size > _src.size()) {
                return;
            }
            for (auto it = _src.begin(_src_ofs); it != _src.end(_src_ofs); ++it) {
                const std::string_view key = it->key;
                switch (it->value_type) {
                case lite3cpp::Type::Null:
                    _dst.set_null(_dst_ofs, key);
                    break;
                case lite3cpp::Type::Bool:
                    _dst.set_bool(_dst_ofs, key, _src.get_bool(_src_ofs, key));
                    break;
                case lite3cpp::Type::Int64:
                    _dst.set_i64(_dst_ofs, key, _src.get_i64(_src_ofs, key));
                    break;
                case lite3cpp::Type::Float64:
                    _dst.set_f64(_dst_ofs, key, _src.get_f64(_src_ofs, key));
                    break;
                case lite3cpp::Type::String:
                    _dst.set_str(_dst_ofs, key, _src.get_str(_src_ofs, key));
                    break;
                case lite3cpp::Type::Bytes:
                    _dst.set_bytes(_dst_ofs, key, _src.get_bytes(_src_ofs, key));
                    break;
                case lite3cpp::Type::Object: {
                    const std::size_t child_src = _src.get_obj(_src_ofs, key);
                    const std::size_t child_dst = _dst.set_obj(_dst_ofs, key);
                    copy_lite3_object(_src, child_src, _dst, child_dst);
                    break;
                }
                case lite3cpp::Type::Array: {
                    const std::size_t child_src = _src.get_arr(_src_ofs, key);
                    const std::size_t child_dst = _dst.set_arr(_dst_ofs, key);
                    copy_lite3_array(_src, child_src, _dst, child_dst);
                    break;
                }
                default:
                    break;
                }
            }
        }

        inline void copy_lite3_array(const lite3cpp::Buffer& _src,
                                     std::size_t _src_arr,
                                     lite3cpp::Buffer& _dst,
                                     std::size_t _dst_arr)
        {
            if (_src.size() < lite3cpp::config::node_size || _src_arr + lite3cpp::config::node_size > _src.size()) {
                return;
            }
            const lite3cpp::NodeView arr_node(
                reinterpret_cast<const lite3cpp::PackedNodeLayout*>(_src.data() + _src_arr));
            const std::uint32_t len = arr_node.size();
            for (std::uint32_t idx = 0; idx < len; ++idx) {
                switch (_src.arr_get_type(_src_arr, idx)) {
                case lite3cpp::Type::Null:
                    _dst.arr_append_null(_dst_arr);
                    break;
                case lite3cpp::Type::Bool:
                    _dst.arr_append_bool(_dst_arr, _src.arr_get_bool(_src_arr, idx));
                    break;
                case lite3cpp::Type::Int64:
                    _dst.arr_append_i64(_dst_arr, _src.arr_get_i64(_src_arr, idx));
                    break;
                case lite3cpp::Type::Float64:
                    _dst.arr_append_f64(_dst_arr, _src.arr_get_f64(_src_arr, idx));
                    break;
                case lite3cpp::Type::String:
                    _dst.arr_append_str(_dst_arr, _src.arr_get_str(_src_arr, idx));
                    break;
                case lite3cpp::Type::Bytes:
                    _dst.arr_append_bytes(_dst_arr, _src.arr_get_bytes(_src_arr, idx));
                    break;
                case lite3cpp::Type::Object: {
                    const std::size_t child_src = _src.arr_get_obj(_src_arr, idx);
                    const std::size_t child_dst = _dst.arr_append_obj(_dst_arr);
                    copy_lite3_object(_src, child_src, _dst, child_dst);
                    break;
                }
                case lite3cpp::Type::Array: {
                    const std::size_t child_src = _src.arr_get_arr(_src_arr, idx);
                    const std::size_t child_dst = _dst.arr_append_arr(_dst_arr);
                    copy_lite3_array(_src, child_src, _dst, child_dst);
                    break;
                }
                default:
                    break;
                }
            }
        }

        inline auto extract_sub_object(const lite3cpp::Buffer& _src, std::size_t _src_ofs) -> lite3cpp::Buffer
        {
            if (_src_ofs == 0) {
                return _src;
            }
            lite3cpp::Buffer out;
            out.init_object();
            copy_lite3_object(_src, _src_ofs, out, 0);
            return out;
        }
    } // namespace detail

    /// \brief Zero-copy view over a `KeyValPair_PI` sub-object inside a `lite3cpp::Buffer`.
    ///
    /// Performs O(log K) B-Tree lookups and in-place mutations without `strdup` or `malloc`.
    class KeyValPairView
    {
    public:
        KeyValPairView(lite3cpp::Buffer* _buf, std::size_t _kvp_ofs) noexcept
            : buf_{_buf}
            , kvp_ofs_{_kvp_ofs}
        {
        }

        class EntryProxy
        {
        public:
            EntryProxy(lite3cpp::Buffer* _buf, std::size_t _kvp_ofs, std::string_view _key) noexcept
                : buf_{_buf}
                , kvp_ofs_{_kvp_ofs}
                , key_{_key}
            {
            }

            auto operator=(std::string_view _val) -> EntryProxy&
            {
                KeyValPairView{buf_, kvp_ofs_}.set(key_, _val);
                return *this;
            }

            auto operator=(const char* _val) -> EntryProxy&
            {
                KeyValPairView{buf_, kvp_ofs_}.set(key_, _val ? std::string_view{_val} : std::string_view{});
                return *this;
            }

            explicit operator std::string_view() const noexcept
            {
                return KeyValPairView{buf_, kvp_ofs_}.get(key_);
            }

            auto value() const noexcept -> std::string_view
            {
                return KeyValPairView{buf_, kvp_ofs_}.get(key_);
            }

            auto operator==(std::string_view _rhs) const noexcept -> bool
            {
                return KeyValPairView{buf_, kvp_ofs_}.get(key_) == _rhs;
            }

            auto operator==(const char* _rhs) const noexcept -> bool
            {
                return KeyValPairView{buf_, kvp_ofs_}.get(key_) ==
                       (_rhs ? std::string_view{_rhs} : std::string_view{});
            }

        private:
            lite3cpp::Buffer* buf_;
            std::size_t kvp_ofs_;
            std::string_view key_;
        };

        [[nodiscard]] auto get(std::string_view _key) const noexcept -> std::string_view
        {
            if (!buf_) {
                return {};
            }
            const auto kv_ofs = find_kv_ofs();
            if (kv_ofs != npos && buf_->get_type(kv_ofs, _key) == lite3cpp::Type::String) {
                return buf_->get_str(kv_ofs, _key);
            }
            return {};
        }

        [[nodiscard]] auto c_str(std::string_view _key) const noexcept -> const char*
        {
            if (!buf_) {
                return nullptr;
            }
            const auto kv_ofs = find_kv_ofs();
            if (kv_ofs != npos && buf_->get_type(kv_ofs, _key) == lite3cpp::Type::String) {
                return buf_->get_str(kv_ofs, _key).data();
            }
            return nullptr;
        }

        void set(std::string_view _key, std::string_view _val)
        {
            if (!buf_) {
                return;
            }
            const auto kv_ofs = ensure_kv_ofs();
            buf_->set_str(kv_ofs, _key, _val);
        }

        [[nodiscard]] auto contains(std::string_view _key) const noexcept -> bool
        {
            if (!buf_) {
                return false;
            }
            const auto kv_ofs = find_kv_ofs();
            return kv_ofs != npos && buf_->get_type(kv_ofs, _key) == lite3cpp::Type::String;
        }

        auto erase(std::string_view _key) -> bool
        {
            if (!buf_) {
                return false;
            }
            const auto kv_ofs = find_kv_ofs();
            if (kv_ofs == npos || buf_->get_type(kv_ofs, _key) != lite3cpp::Type::String) {
                return false;
            }
            buf_->set_null(kv_ofs, _key);
            return true;
        }

        void clear()
        {
            if (!buf_) {
                return;
            }
            const auto kv_ofs = find_kv_ofs();
            if (kv_ofs != npos) {
                std::vector<std::string> keys;
                for (auto it = buf_->begin(kv_ofs); it != buf_->end(kv_ofs); ++it) {
                    if (it->value_type == lite3cpp::Type::String) {
                        keys.emplace_back(it->key);
                    }
                }
                for (const auto& k : keys) {
                    buf_->set_null(kv_ofs, k);
                }
            }
            buf_->set_i64(kvp_ofs_, "ssLen", 0);
        }

        [[nodiscard]] auto size() const noexcept -> std::size_t
        {
            if (!buf_) {
                return 0;
            }
            const auto kv_ofs = find_kv_ofs();
            if (kv_ofs == npos) {
                return 0;
            }
            std::size_t count = 0;
            for (auto it = buf_->begin(kv_ofs); it != buf_->end(kv_ofs); ++it) {
                if (it->value_type == lite3cpp::Type::String) {
                    ++count;
                }
            }
            return count;
        }

        [[nodiscard]] auto empty() const noexcept -> bool
        {
            return size() == 0;
        }

        auto operator[](std::string_view _key) noexcept -> EntryProxy
        {
            return EntryProxy{buf_, kvp_ofs_, _key};
        }

        [[nodiscard]] auto operator[](std::string_view _key) const noexcept -> std::string_view
        {
            return get(_key);
        }

        [[nodiscard]] auto buffer() noexcept -> lite3cpp::Buffer*
        {
            return buf_;
        }

        [[nodiscard]] auto buffer() const noexcept -> const lite3cpp::Buffer*
        {
            return buf_;
        }

        [[nodiscard]] auto offset() const noexcept -> std::size_t
        {
            return kvp_ofs_;
        }

    private:
        static constexpr std::size_t npos = static_cast<std::size_t>(-1);

        auto find_kv_ofs() const noexcept -> std::size_t
        {
            if (!buf_) {
                return npos;
            }
            if (buf_->get_type(kvp_ofs_, "kv") == lite3cpp::Type::Object) {
                return buf_->get_obj(kvp_ofs_, "kv");
            }
            return const_cast<KeyValPairView*>(this)->sync_legacy_arrays_to_kv();
        }

        auto ensure_kv_ofs() -> std::size_t
        {
            const auto existing = find_kv_ofs();
            if (existing != npos) {
                return existing;
            }
            return buf_->set_obj(kvp_ofs_, "kv");
        }

        auto sync_legacy_arrays_to_kv() noexcept -> std::size_t
        {
            if (!buf_ || buf_->get_type(kvp_ofs_, "ssLen") != lite3cpp::Type::Int64) {
                return npos;
            }
            const auto ss_len = buf_->get_i64(kvp_ofs_, "ssLen");
            if (ss_len <= 0 ||
                buf_->get_type(kvp_ofs_, "keyWord") != lite3cpp::Type::Array ||
                buf_->get_type(kvp_ofs_, "svalue") != lite3cpp::Type::Array)
            {
                return npos;
            }
            const std::size_t kw_arr = buf_->get_arr(kvp_ofs_, "keyWord");
            const std::size_t val_arr = buf_->get_arr(kvp_ofs_, "svalue");
            const std::size_t kv_ofs = buf_->set_obj(kvp_ofs_, "kv");
            for (std::int64_t i = 0; i < ss_len; ++i) {
                const auto idx = static_cast<std::uint32_t>(i);
                if (buf_->arr_get_type(kw_arr, idx) == lite3cpp::Type::String) {
                    const std::string key_str{buf_->arr_get_str(kw_arr, idx)};
                    const std::string val_str =
                        (buf_->arr_get_type(val_arr, idx) == lite3cpp::Type::String)
                            ? std::string{buf_->arr_get_str(val_arr, idx)}
                            : std::string{};
                    buf_->set_str(kv_ofs, key_str, val_str);
                }
            }
            return kv_ofs;
        }

        lite3cpp::Buffer* buf_{nullptr};
        std::size_t kvp_ofs_{0};
    };

    /// \brief Base C++20 facade over a `lite3cpp::Buffer` (either owning or referencing a sub-object offset).
    template <typename Tag, typename LegacyT>
    class struct_buffer
    {
    public:
        using legacy_type = LegacyT;

        struct_buffer()
            : owned_{lite3cpp::Buffer{}}
            , ref_buf_{&*owned_}
            , ofs_{0}
        {
            owned_->init_object();
        }

        explicit struct_buffer(lite3cpp::Buffer _buf, std::size_t _root_ofs = 0) noexcept
            : owned_{std::move(_buf)}
            , ref_buf_{&*owned_}
            , ofs_{_root_ofs}
        {
        }

        struct_buffer(lite3cpp::Buffer* _ref_buf, std::size_t _sub_ofs) noexcept
            : owned_{std::nullopt}
            , ref_buf_{_ref_buf}
            , ofs_{_sub_ofs}
        {
        }

        struct_buffer(const struct_buffer& _other)
            : owned_{detail::extract_sub_object(_other.buffer(), _other.ofs_)}
            , ref_buf_{&*owned_}
            , ofs_{0}
        {
        }

        struct_buffer(struct_buffer&& _other) noexcept
            : owned_{std::move(_other.owned_)}
            , ref_buf_{owned_ ? &*owned_ : _other.ref_buf_}
            , ofs_{_other.ofs_}
        {
        }

        auto operator=(const struct_buffer& _other) -> struct_buffer&
        {
            if (this != &_other) {
                owned_ = detail::extract_sub_object(_other.buffer(), _other.ofs_);
                ref_buf_ = &*owned_;
                ofs_ = 0;
            }
            return *this;
        }

        auto operator=(struct_buffer&& _other) noexcept -> struct_buffer&
        {
            if (this != &_other) {
                owned_ = std::move(_other.owned_);
                ref_buf_ = owned_ ? &*owned_ : _other.ref_buf_;
                ofs_ = _other.ofs_;
            }
            return *this;
        }

        ~struct_buffer() = default;

        [[nodiscard]] auto buffer() noexcept -> lite3cpp::Buffer&
        {
            return *ref_buf_;
        }

        [[nodiscard]] auto buffer() const noexcept -> const lite3cpp::Buffer&
        {
            return *ref_buf_;
        }

        [[nodiscard]] auto offset() const noexcept -> std::size_t
        {
            return ofs_;
        }

        [[nodiscard]] auto operator[](std::string_view _key) -> lite3cpp::Value
        {
            return lite3cpp::Value(ref_buf_, ofs_)[_key];
        }

        [[nodiscard]] static auto from_legacy(const legacy_type& _legacy) -> irods::expected<Tag, int>
        {
            auto res = irods::wire::struct_to_lite3(&_legacy, Tag::pi_name, RodsPackTable, 0);
            if (!res) {
                return irods::unexpected{res.error()};
            }
            return Tag{std::move(*res), 0};
        }

        [[nodiscard]] auto to_legacy() const -> irods::expected<legacy_type*, int>
        {
            void* raw_out = nullptr;
            const int ec = (ofs_ == 0)
                               ? irods::wire::lite3_to_struct(buffer(), &raw_out, Tag::pi_name, RodsPackTable)
                               : irods::wire::lite3_to_struct(
                                     detail::extract_sub_object(buffer(), ofs_), &raw_out, Tag::pi_name, RodsPackTable);
            if (ec < 0) {
                return irods::unexpected{ec};
            }
            return static_cast<legacy_type*>(raw_out);
        }

        auto to_legacy(legacy_type& _out) const -> int
        {
            auto res = to_legacy();
            if (!res) {
                return res.error();
            }
            _out = **res;
            std::free(*res);
            return 0;
        }

        [[nodiscard]] auto encode(irodsProt_t _prot = LITE3_PROT,
                                  std::string_view _peer_version = "rods6.0.0") const
            -> irods::expected<std::vector<std::byte>, int>
        {
            auto bbuf_res = encode_bbuf(_prot, _peer_version);
            if (!bbuf_res) {
                return irods::unexpected{bbuf_res.error()};
            }
            bytesBuf_t* bbuf = *bbuf_res;
            const auto* begin = reinterpret_cast<const std::byte*>(bbuf->buf);
            std::vector<std::byte> out(begin, begin + static_cast<std::size_t>(bbuf->len));
            freeBBuf(bbuf);
            return out;
        }

        [[nodiscard]] static auto decode(std::span<const std::byte> _wire_bytes,
                                         irodsProt_t _prot = LITE3_PROT,
                                         std::string_view _peer_version = "rods6.0.0") -> irods::expected<Tag, int>
        {
            auto res = irods::wire::decode_to_lite3(_wire_bytes, Tag::pi_name, _prot, _peer_version, RodsPackTable);
            if (!res) {
                return irods::unexpected{res.error()};
            }
            return Tag{std::move(*res), 0};
        }

        [[nodiscard]] static auto decode_bbuf(const bytesBuf_t& _bbuf,
                                              irodsProt_t _prot = LITE3_PROT,
                                              std::string_view _peer_version = "rods6.0.0") -> irods::expected<Tag, int>
        {
            if (!_bbuf.buf || _bbuf.len <= 0) {
                return irods::unexpected{SYS_INVALID_INPUT_PARAM};
            }
            const std::span<const std::byte> span{
                reinterpret_cast<const std::byte*>(_bbuf.buf), static_cast<std::size_t>(_bbuf.len)};
            return decode(span, _prot, _peer_version);
        }

        [[nodiscard]] auto encode_bbuf(irodsProt_t _prot = LITE3_PROT,
                                       std::string_view _peer_version = "rods6.0.0") const
            -> irods::expected<bytesBuf_t*, int>
        {
            if (ofs_ == 0) {
                return irods::wire::encode_from_lite3(buffer(), Tag::pi_name, _prot, _peer_version, RodsPackTable);
            }
            const auto root_buf = detail::extract_sub_object(buffer(), ofs_);
            return irods::wire::encode_from_lite3(root_buf, Tag::pi_name, _prot, _peer_version, RodsPackTable);
        }

    protected:
        std::optional<lite3cpp::Buffer> owned_;
        lite3cpp::Buffer* ref_buf_{nullptr};
        std::size_t ofs_{0};
    };

    /// \brief Standalone owning `KeyValPair` (`keyValPair_t`) backed by a `lite3cpp::Buffer`.
    class KeyValPair : public struct_buffer<KeyValPair, ::keyValPair_t>
    {
    public:
        static constexpr std::string_view pi_name = "KeyValPair_PI";

        using struct_buffer<KeyValPair, ::keyValPair_t>::struct_buffer;

        [[nodiscard]] auto view() noexcept -> KeyValPairView
        {
            return KeyValPairView{&buffer(), offset()};
        }

        [[nodiscard]] auto view() const noexcept -> KeyValPairView
        {
            return KeyValPairView{const_cast<lite3cpp::Buffer*>(&buffer()), offset()};
        }

        operator KeyValPairView() noexcept
        {
            return view();
        }

        operator KeyValPairView() const noexcept
        {
            return view();
        }

        [[nodiscard]] auto get(std::string_view _key) const noexcept -> std::string_view
        {
            return view().get(_key);
        }

        [[nodiscard]] auto c_str(std::string_view _key) const noexcept -> const char*
        {
            return view().c_str(_key);
        }

        void set(std::string_view _key, std::string_view _val)
        {
            view().set(_key, _val);
        }

        [[nodiscard]] auto contains(std::string_view _key) const noexcept -> bool
        {
            return view().contains(_key);
        }

        auto erase(std::string_view _key) -> bool
        {
            return view().erase(_key);
        }

        void clear()
        {
            view().clear();
        }

        [[nodiscard]] auto size() const noexcept -> std::size_t
        {
            return view().size();
        }

        [[nodiscard]] auto empty() const noexcept -> bool
        {
            return view().empty();
        }

        auto operator[](std::string_view _key) noexcept -> KeyValPairView::EntryProxy
        {
            return view()[_key];
        }

        [[nodiscard]] auto operator[](std::string_view _key) const noexcept -> std::string_view
        {
            return view().get(_key);
        }
    };

    /// \brief Zero-copy view over a `SpecColl_PI` sub-object inside a `lite3cpp::Buffer`.
    class SpecCollView
    {
    public:
        SpecCollView(lite3cpp::Buffer* _buf, std::size_t _ofs) noexcept
            : buf_{_buf}
            , ofs_{_ofs}
        {
        }

        [[nodiscard]] auto coll_class() const noexcept -> int
        {
            return buf_ ? static_cast<int>(detail::get_i64_or(*buf_, ofs_, "collClass")) : 0;
        }
        void coll_class(int _val)
        {
            if (buf_) {
                buf_->set_i64(ofs_, "collClass", _val);
            }
        }

        [[nodiscard]] auto type() const noexcept -> int
        {
            return buf_ ? static_cast<int>(detail::get_i64_or(*buf_, ofs_, "type")) : 0;
        }
        void type(int _val)
        {
            if (buf_) {
                buf_->set_i64(ofs_, "type", _val);
            }
        }

        [[nodiscard]] auto collection() const noexcept -> std::string_view
        {
            return buf_ ? detail::get_str_or(*buf_, ofs_, "collection") : std::string_view{};
        }
        void collection(std::string_view _val)
        {
            if (buf_) {
                buf_->set_str(ofs_, "collection", _val);
            }
        }

        [[nodiscard]] auto obj_path() const noexcept -> std::string_view
        {
            return buf_ ? detail::get_str_or(*buf_, ofs_, "objPath") : std::string_view{};
        }
        void obj_path(std::string_view _val)
        {
            if (buf_) {
                buf_->set_str(ofs_, "objPath", _val);
            }
        }

        [[nodiscard]] auto resource() const noexcept -> std::string_view
        {
            return buf_ ? detail::get_str_or(*buf_, ofs_, "resource") : std::string_view{};
        }
        void resource(std::string_view _val)
        {
            if (buf_) {
                buf_->set_str(ofs_, "resource", _val);
            }
        }

        [[nodiscard]] auto resc_hier() const noexcept -> std::string_view
        {
            return buf_ ? detail::get_str_or(*buf_, ofs_, "rescHier") : std::string_view{};
        }
        void resc_hier(std::string_view _val)
        {
            if (buf_) {
                buf_->set_str(ofs_, "rescHier", _val);
            }
        }

        [[nodiscard]] auto phy_path() const noexcept -> std::string_view
        {
            return buf_ ? detail::get_str_or(*buf_, ofs_, "phyPath") : std::string_view{};
        }
        void phy_path(std::string_view _val)
        {
            if (buf_) {
                buf_->set_str(ofs_, "phyPath", _val);
            }
        }

        [[nodiscard]] auto cache_dir() const noexcept -> std::string_view
        {
            return buf_ ? detail::get_str_or(*buf_, ofs_, "cacheDir") : std::string_view{};
        }
        void cache_dir(std::string_view _val)
        {
            if (buf_) {
                buf_->set_str(ofs_, "cacheDir", _val);
            }
        }

        [[nodiscard]] auto cache_dirty() const noexcept -> int
        {
            return buf_ ? static_cast<int>(detail::get_i64_or(*buf_, ofs_, "cacheDirty")) : 0;
        }
        void cache_dirty(int _val)
        {
            if (buf_) {
                buf_->set_i64(ofs_, "cacheDirty", _val);
            }
        }

        [[nodiscard]] auto repl_num() const noexcept -> int
        {
            return buf_ ? static_cast<int>(detail::get_i64_or(*buf_, ofs_, "replNum")) : 0;
        }
        void repl_num(int _val)
        {
            if (buf_) {
                buf_->set_i64(ofs_, "replNum", _val);
            }
        }

    private:
        lite3cpp::Buffer* buf_{nullptr};
        std::size_t ofs_{0};
    };

    /// \brief Standalone owning `SpecColl` (`specColl_t`) backed by a `lite3cpp::Buffer`.
    class SpecColl : public struct_buffer<SpecColl, ::specColl_t>
    {
    public:
        static constexpr std::string_view pi_name = "SpecColl_PI";

        using struct_buffer<SpecColl, ::specColl_t>::struct_buffer;

        [[nodiscard]] auto view() noexcept -> SpecCollView
        {
            return SpecCollView{&buffer(), offset()};
        }

        [[nodiscard]] auto view() const noexcept -> SpecCollView
        {
            return SpecCollView{const_cast<lite3cpp::Buffer*>(&buffer()), offset()};
        }
    };

    /// \brief `DataObjInp_PI` (`dataObjInp_t`) facade over a `lite3cpp::Buffer`.
    class DataObjInp : public struct_buffer<DataObjInp, ::dataObjInp_t>
    {
    public:
        static constexpr std::string_view pi_name = "DataObjInp_PI";

        using struct_buffer<DataObjInp, ::dataObjInp_t>::struct_buffer;

        [[nodiscard]] auto obj_path() const noexcept -> std::string_view
        {
            return detail::get_str_or(buffer(), offset(), "objPath");
        }
        void obj_path(std::string_view _val)
        {
            buffer().set_str(offset(), "objPath", _val);
        }

        [[nodiscard]] auto create_mode() const noexcept -> int
        {
            return static_cast<int>(detail::get_i64_or(buffer(), offset(), "createMode"));
        }
        void create_mode(int _val)
        {
            buffer().set_i64(offset(), "createMode", _val);
        }

        [[nodiscard]] auto open_flags() const noexcept -> int
        {
            return static_cast<int>(detail::get_i64_or(buffer(), offset(), "openFlags"));
        }
        void open_flags(int _val)
        {
            buffer().set_i64(offset(), "openFlags", _val);
        }

        [[nodiscard]] auto offset_val() const noexcept -> rodsLong_t
        {
            return static_cast<rodsLong_t>(detail::get_i64_or(buffer(), offset(), "offset"));
        }
        void offset_val(rodsLong_t _val)
        {
            buffer().set_i64(offset(), "offset", _val);
        }

        [[nodiscard]] auto data_size() const noexcept -> rodsLong_t
        {
            return static_cast<rodsLong_t>(detail::get_i64_or(buffer(), offset(), "dataSize"));
        }
        void data_size(rodsLong_t _val)
        {
            buffer().set_i64(offset(), "dataSize", _val);
        }

        [[nodiscard]] auto num_threads() const noexcept -> int
        {
            return static_cast<int>(detail::get_i64_or(buffer(), offset(), "numThreads"));
        }
        void num_threads(int _val)
        {
            buffer().set_i64(offset(), "numThreads", _val);
        }

        [[nodiscard]] auto opr_type() const noexcept -> int
        {
            return static_cast<int>(detail::get_i64_or(buffer(), offset(), "oprType"));
        }
        void opr_type(int _val)
        {
            buffer().set_i64(offset(), "oprType", _val);
        }

        [[nodiscard]] auto cond_input() -> KeyValPairView
        {
            const auto kvp_ofs = detail::ensure_sub_obj(buffer(), offset(), "KeyValPair_PI");
            return KeyValPairView{&buffer(), kvp_ofs};
        }

        [[nodiscard]] auto cond_input() const noexcept -> KeyValPairView
        {
            if (buffer().get_type(offset(), "KeyValPair_PI") == lite3cpp::Type::Object) {
                return KeyValPairView{
                    const_cast<lite3cpp::Buffer*>(&buffer()), buffer().get_obj(offset(), "KeyValPair_PI")};
            }
            return KeyValPairView{nullptr, 0};
        }

        [[nodiscard]] auto has_spec_coll() const noexcept -> bool
        {
            return buffer().get_type(offset(), "SpecColl_PI") == lite3cpp::Type::Object;
        }

        [[nodiscard]] auto spec_coll() -> SpecCollView
        {
            const auto sc_ofs = detail::ensure_sub_obj(buffer(), offset(), "SpecColl_PI");
            return SpecCollView{&buffer(), sc_ofs};
        }

        void clear_spec_coll()
        {
            buffer().set_null(offset(), "SpecColl_PI");
        }
    };

    /// \brief `OpenedDataObjInp_PI` (`openedDataObjInp_t`) facade over a `lite3cpp::Buffer`.
    class OpenedDataObjInp : public struct_buffer<OpenedDataObjInp, ::openedDataObjInp_t>
    {
    public:
        static constexpr std::string_view pi_name = "OpenedDataObjInp_PI";

        using struct_buffer<OpenedDataObjInp, ::openedDataObjInp_t>::struct_buffer;

        [[nodiscard]] auto l1desc_inx() const noexcept -> int
        {
            return static_cast<int>(detail::get_i64_or(buffer(), offset(), "l1descInx"));
        }
        void l1desc_inx(int _val)
        {
            buffer().set_i64(offset(), "l1descInx", _val);
        }

        [[nodiscard]] auto len() const noexcept -> int
        {
            return static_cast<int>(detail::get_i64_or(buffer(), offset(), "len"));
        }
        void len(int _val)
        {
            buffer().set_i64(offset(), "len", _val);
        }

        [[nodiscard]] auto whence() const noexcept -> int
        {
            return static_cast<int>(detail::get_i64_or(buffer(), offset(), "whence"));
        }
        void whence(int _val)
        {
            buffer().set_i64(offset(), "whence", _val);
        }

        [[nodiscard]] auto opr_type() const noexcept -> int
        {
            return static_cast<int>(detail::get_i64_or(buffer(), offset(), "oprType"));
        }
        void opr_type(int _val)
        {
            buffer().set_i64(offset(), "oprType", _val);
        }

        [[nodiscard]] auto offset_val() const noexcept -> rodsLong_t
        {
            return static_cast<rodsLong_t>(detail::get_i64_or(buffer(), offset(), "offset"));
        }
        void offset_val(rodsLong_t _val)
        {
            buffer().set_i64(offset(), "offset", _val);
        }

        [[nodiscard]] auto bytes_written() const noexcept -> rodsLong_t
        {
            return static_cast<rodsLong_t>(detail::get_i64_or(buffer(), offset(), "bytesWritten"));
        }
        void bytes_written(rodsLong_t _val)
        {
            buffer().set_i64(offset(), "bytesWritten", _val);
        }

        [[nodiscard]] auto cond_input() -> KeyValPairView
        {
            const auto kvp_ofs = detail::ensure_sub_obj(buffer(), offset(), "KeyValPair_PI");
            return KeyValPairView{&buffer(), kvp_ofs};
        }

        [[nodiscard]] auto cond_input() const noexcept -> KeyValPairView
        {
            if (buffer().get_type(offset(), "KeyValPair_PI") == lite3cpp::Type::Object) {
                return KeyValPairView{
                    const_cast<lite3cpp::Buffer*>(&buffer()), buffer().get_obj(offset(), "KeyValPair_PI")};
            }
            return KeyValPairView{nullptr, 0};
        }
    };

    /// \brief `DataObjCopyInp_PI` (`dataObjCopyInp_t`) facade over a `lite3cpp::Buffer`.
    class DataObjCopyInp : public struct_buffer<DataObjCopyInp, ::dataObjCopyInp_t>
    {
    public:
        static constexpr std::string_view pi_name = "DataObjCopyInp_PI";

        using struct_buffer<DataObjCopyInp, ::dataObjCopyInp_t>::struct_buffer;

        [[nodiscard]] auto src() -> DataObjInp
        {
            const auto src_ofs = detail::ensure_sub_obj(buffer(), offset(), "DataObjInp_PI");
            return DataObjInp{&buffer(), src_ofs};
        }

        [[nodiscard]] auto src() const noexcept -> DataObjInp
        {
            if (buffer().get_type(offset(), "DataObjInp_PI") == lite3cpp::Type::Object) {
                return DataObjInp{
                    const_cast<lite3cpp::Buffer*>(&buffer()), buffer().get_obj(offset(), "DataObjInp_PI")};
            }
            return DataObjInp{};
        }

        [[nodiscard]] auto dest() -> DataObjInp
        {
            const auto dst_ofs = detail::ensure_sub_obj(buffer(), offset(), "DataObjInp_PI#1");
            return DataObjInp{&buffer(), dst_ofs};
        }

        [[nodiscard]] auto dest() const noexcept -> DataObjInp
        {
            if (buffer().get_type(offset(), "DataObjInp_PI#1") == lite3cpp::Type::Object) {
                return DataObjInp{
                    const_cast<lite3cpp::Buffer*>(&buffer()), buffer().get_obj(offset(), "DataObjInp_PI#1")};
            }
            return DataObjInp{};
        }
    };

    /// \brief `CollInpNew_PI` (`collInp_t`) facade over a `lite3cpp::Buffer`.
    class CollInp : public struct_buffer<CollInp, ::collInp_t>
    {
    public:
        static constexpr std::string_view pi_name = "CollInpNew_PI";

        using struct_buffer<CollInp, ::collInp_t>::struct_buffer;

        [[nodiscard]] auto coll_name() const noexcept -> std::string_view
        {
            return detail::get_str_or(buffer(), offset(), "collName");
        }
        void coll_name(std::string_view _val)
        {
            buffer().set_str(offset(), "collName", _val);
        }

        [[nodiscard]] auto flags() const noexcept -> int
        {
            return static_cast<int>(detail::get_i64_or(buffer(), offset(), "flags"));
        }
        void flags(int _val)
        {
            buffer().set_i64(offset(), "flags", _val);
        }

        [[nodiscard]] auto opr_type() const noexcept -> int
        {
            return static_cast<int>(detail::get_i64_or(buffer(), offset(), "oprType"));
        }
        void opr_type(int _val)
        {
            buffer().set_i64(offset(), "oprType", _val);
        }

        [[nodiscard]] auto cond_input() -> KeyValPairView
        {
            const auto kvp_ofs = detail::ensure_sub_obj(buffer(), offset(), "KeyValPair_PI");
            return KeyValPairView{&buffer(), kvp_ofs};
        }

        [[nodiscard]] auto cond_input() const noexcept -> KeyValPairView
        {
            if (buffer().get_type(offset(), "KeyValPair_PI") == lite3cpp::Type::Object) {
                return KeyValPairView{
                    const_cast<lite3cpp::Buffer*>(&buffer()), buffer().get_obj(offset(), "KeyValPair_PI")};
            }
            return KeyValPairView{nullptr, 0};
        }
    };

    /// \brief `DataObjInfo_PI` (`dataObjInfo_t`) facade over a `lite3cpp::Buffer`.
    class DataObjInfo : public struct_buffer<DataObjInfo, ::dataObjInfo_t>
    {
    public:
        static constexpr std::string_view pi_name = "DataObjInfo_PI";

        using struct_buffer<DataObjInfo, ::dataObjInfo_t>::struct_buffer;

        [[nodiscard]] auto obj_path() const noexcept -> std::string_view
        {
            return detail::get_str_or(buffer(), offset(), "objPath");
        }
        void obj_path(std::string_view _val)
        {
            buffer().set_str(offset(), "objPath", _val);
        }

        [[nodiscard]] auto resc_name() const noexcept -> std::string_view
        {
            return detail::get_str_or(buffer(), offset(), "rescName");
        }
        void resc_name(std::string_view _val)
        {
            buffer().set_str(offset(), "rescName", _val);
        }

        [[nodiscard]] auto resc_hier() const noexcept -> std::string_view
        {
            return detail::get_str_or(buffer(), offset(), "rescHier");
        }
        void resc_hier(std::string_view _val)
        {
            buffer().set_str(offset(), "rescHier", _val);
        }

        [[nodiscard]] auto data_type() const noexcept -> std::string_view
        {
            return detail::get_str_or(buffer(), offset(), "dataType");
        }
        void data_type(std::string_view _val)
        {
            buffer().set_str(offset(), "dataType", _val);
        }

        [[nodiscard]] auto data_size() const noexcept -> rodsLong_t
        {
            return static_cast<rodsLong_t>(detail::get_i64_or(buffer(), offset(), "dataSize"));
        }
        void data_size(rodsLong_t _val)
        {
            buffer().set_i64(offset(), "dataSize", _val);
        }

        [[nodiscard]] auto chksum() const noexcept -> std::string_view
        {
            return detail::get_str_or(buffer(), offset(), "chksum");
        }
        void chksum(std::string_view _val)
        {
            buffer().set_str(offset(), "chksum", _val);
        }

        [[nodiscard]] auto version() const noexcept -> std::string_view
        {
            return detail::get_str_or(buffer(), offset(), "version");
        }
        void version(std::string_view _val)
        {
            buffer().set_str(offset(), "version", _val);
        }

        [[nodiscard]] auto file_path() const noexcept -> std::string_view
        {
            return detail::get_str_or(buffer(), offset(), "filePath");
        }
        void file_path(std::string_view _val)
        {
            buffer().set_str(offset(), "filePath", _val);
        }

        [[nodiscard]] auto data_owner_name() const noexcept -> std::string_view
        {
            return detail::get_str_or(buffer(), offset(), "dataOwnerName");
        }
        void data_owner_name(std::string_view _val)
        {
            buffer().set_str(offset(), "dataOwnerName", _val);
        }

        [[nodiscard]] auto data_owner_zone() const noexcept -> std::string_view
        {
            return detail::get_str_or(buffer(), offset(), "dataOwnerZone");
        }
        void data_owner_zone(std::string_view _val)
        {
            buffer().set_str(offset(), "dataOwnerZone", _val);
        }

        [[nodiscard]] auto repl_num() const noexcept -> int
        {
            return static_cast<int>(detail::get_i64_or(buffer(), offset(), "replNum"));
        }
        void repl_num(int _val)
        {
            buffer().set_i64(offset(), "replNum", _val);
        }

        [[nodiscard]] auto repl_status() const noexcept -> int
        {
            return static_cast<int>(detail::get_i64_or(buffer(), offset(), "replStatus"));
        }
        void repl_status(int _val)
        {
            buffer().set_i64(offset(), "replStatus", _val);
        }

        [[nodiscard]] auto status_string() const noexcept -> std::string_view
        {
            return detail::get_str_or(buffer(), offset(), "statusString");
        }
        void status_string(std::string_view _val)
        {
            buffer().set_str(offset(), "statusString", _val);
        }

        [[nodiscard]] auto data_id() const noexcept -> rodsLong_t
        {
            return static_cast<rodsLong_t>(detail::get_i64_or(buffer(), offset(), "dataId"));
        }
        void data_id(rodsLong_t _val)
        {
            buffer().set_i64(offset(), "dataId", _val);
        }

        [[nodiscard]] auto coll_id() const noexcept -> rodsLong_t
        {
            return static_cast<rodsLong_t>(detail::get_i64_or(buffer(), offset(), "collId"));
        }
        void coll_id(rodsLong_t _val)
        {
            buffer().set_i64(offset(), "collId", _val);
        }

        [[nodiscard]] auto data_mode() const noexcept -> std::string_view
        {
            return detail::get_str_or(buffer(), offset(), "dataMode");
        }
        void data_mode(std::string_view _val)
        {
            buffer().set_str(offset(), "dataMode", _val);
        }

        [[nodiscard]] auto resc_id() const noexcept -> rodsLong_t
        {
            return static_cast<rodsLong_t>(detail::get_i64_or(buffer(), offset(), "rescId"));
        }
        void resc_id(rodsLong_t _val)
        {
            buffer().set_i64(offset(), "rescId", _val);
        }

        [[nodiscard]] auto cond_input() -> KeyValPairView
        {
            const auto kvp_ofs = detail::ensure_sub_obj(buffer(), offset(), "KeyValPair_PI");
            return KeyValPairView{&buffer(), kvp_ofs};
        }

        [[nodiscard]] auto cond_input() const noexcept -> KeyValPairView
        {
            if (buffer().get_type(offset(), "KeyValPair_PI") == lite3cpp::Type::Object) {
                return KeyValPairView{
                    const_cast<lite3cpp::Buffer*>(&buffer()), buffer().get_obj(offset(), "KeyValPair_PI")};
            }
            return KeyValPairView{nullptr, 0};
        }
    };

    /// \brief Contiguous RAII replica list replacing singly-linked `::dataObjInfo_t*` lists.
    class DataObjInfoList
    {
    public:
        DataObjInfoList() = default;

        void push_back(DataObjInfo _info)
        {
            replicas_.push_back(std::move(_info));
        }

        [[nodiscard]] auto size() const noexcept -> std::size_t
        {
            return replicas_.size();
        }

        [[nodiscard]] auto empty() const noexcept -> bool
        {
            return replicas_.empty();
        }

        [[nodiscard]] auto operator[](std::size_t _idx) noexcept -> DataObjInfo&
        {
            return replicas_[_idx];
        }

        [[nodiscard]] auto operator[](std::size_t _idx) const noexcept -> const DataObjInfo&
        {
            return replicas_[_idx];
        }

        auto begin() noexcept
        {
            return replicas_.begin();
        }
        auto end() noexcept
        {
            return replicas_.end();
        }
        auto begin() const noexcept
        {
            return replicas_.begin();
        }
        auto end() const noexcept
        {
            return replicas_.end();
        }

        [[nodiscard]] static auto from_legacy_list(const ::dataObjInfo_t* _head) -> irods::expected<DataObjInfoList, int>
        {
            DataObjInfoList list;
            for (const ::dataObjInfo_t* cur = _head; cur != nullptr; cur = cur->next) {
                auto item = DataObjInfo::from_legacy(*cur);
                if (!item) {
                    return irods::unexpected{item.error()};
                }
                list.push_back(std::move(*item));
            }
            return list;
        }

        [[nodiscard]] auto to_legacy_list() const -> irods::expected<::dataObjInfo_t*, int>
        {
            ::dataObjInfo_t* head = nullptr;
            ::dataObjInfo_t* tail = nullptr;
            for (const auto& item : replicas_) {
                auto node_res = item.to_legacy();
                if (!node_res) {
                    freeAllDataObjInfo(head);
                    return irods::unexpected{node_res.error()};
                }
                (*node_res)->next = nullptr;
                if (!head) {
                    head = *node_res;
                    tail = *node_res;
                }
                else {
                    tail->next = *node_res;
                    tail = *node_res;
                }
            }
            return head;
        }

    private:
        std::vector<DataObjInfo> replicas_;
    };

    /// \brief `RodsObjStat_PI` (`rodsObjStat_t`) facade over a `lite3cpp::Buffer`.
    class RodsObjStat : public struct_buffer<RodsObjStat, ::rodsObjStat_t>
    {
    public:
        static constexpr std::string_view pi_name = "RodsObjStat_PI";

        using struct_buffer<RodsObjStat, ::rodsObjStat_t>::struct_buffer;

        [[nodiscard]] auto obj_size() const noexcept -> rodsLong_t
        {
            return static_cast<rodsLong_t>(detail::get_i64_or(buffer(), offset(), "objSize"));
        }
        void obj_size(rodsLong_t _val)
        {
            buffer().set_i64(offset(), "objSize", _val);
        }

        [[nodiscard]] auto obj_type() const noexcept -> objType_t
        {
            return static_cast<objType_t>(detail::get_i64_or(buffer(), offset(), "objType"));
        }
        void obj_type(objType_t _val)
        {
            buffer().set_i64(offset(), "objType", static_cast<std::int64_t>(_val));
        }

        [[nodiscard]] auto data_mode() const noexcept -> int
        {
            return static_cast<int>(detail::get_i64_or(buffer(), offset(), "dataMode"));
        }
        void data_mode(int _val)
        {
            buffer().set_i64(offset(), "dataMode", _val);
        }

        [[nodiscard]] auto data_id() const noexcept -> std::string_view
        {
            return detail::get_str_or(buffer(), offset(), "dataId");
        }
        void data_id(std::string_view _val)
        {
            buffer().set_str(offset(), "dataId", _val);
        }

        [[nodiscard]] auto chksum() const noexcept -> std::string_view
        {
            return detail::get_str_or(buffer(), offset(), "chksum");
        }
        void chksum(std::string_view _val)
        {
            buffer().set_str(offset(), "chksum", _val);
        }

        [[nodiscard]] auto owner_name() const noexcept -> std::string_view
        {
            return detail::get_str_or(buffer(), offset(), "ownerName");
        }
        void owner_name(std::string_view _val)
        {
            buffer().set_str(offset(), "ownerName", _val);
        }

        [[nodiscard]] auto owner_zone() const noexcept -> std::string_view
        {
            return detail::get_str_or(buffer(), offset(), "ownerZone");
        }
        void owner_zone(std::string_view _val)
        {
            buffer().set_str(offset(), "ownerZone", _val);
        }

        [[nodiscard]] auto create_time() const noexcept -> std::string_view
        {
            return detail::get_str_or(buffer(), offset(), "createTime");
        }
        void create_time(std::string_view _val)
        {
            buffer().set_str(offset(), "createTime", _val);
        }

        [[nodiscard]] auto modify_time() const noexcept -> std::string_view
        {
            return detail::get_str_or(buffer(), offset(), "modifyTime");
        }
        void modify_time(std::string_view _val)
        {
            buffer().set_str(offset(), "modifyTime", _val);
        }

        [[nodiscard]] auto has_spec_coll() const noexcept -> bool
        {
            return buffer().get_type(offset(), "SpecColl_PI") == lite3cpp::Type::Object;
        }

        [[nodiscard]] auto spec_coll() -> SpecCollView
        {
            const auto sc_ofs = detail::ensure_sub_obj(buffer(), offset(), "SpecColl_PI");
            return SpecCollView{&buffer(), sc_ofs};
        }
    };

    /// \brief `ModAVUMetadataInp_PI` (`modAVUMetadataInp_t`) facade over a `lite3cpp::Buffer`.
    class ModAVUMetadataInp : public struct_buffer<ModAVUMetadataInp, ::modAVUMetadataInp_t>
    {
    public:
        static constexpr std::string_view pi_name = "ModAVUMetadataInp_PI";

        using struct_buffer<ModAVUMetadataInp, ::modAVUMetadataInp_t>::struct_buffer;

        [[nodiscard]] auto option() const noexcept -> std::string_view
        {
            return detail::get_str_or(buffer(), offset(), "arg0");
        }
        void option(std::string_view _val)
        {
            buffer().set_str(offset(), "arg0", _val);
        }

        [[nodiscard]] auto item_type() const noexcept -> std::string_view
        {
            return detail::get_str_or(buffer(), offset(), "arg1");
        }
        void item_type(std::string_view _val)
        {
            buffer().set_str(offset(), "arg1", _val);
        }

        [[nodiscard]] auto item_name() const noexcept -> std::string_view
        {
            return detail::get_str_or(buffer(), offset(), "arg2");
        }
        void item_name(std::string_view _val)
        {
            buffer().set_str(offset(), "arg2", _val);
        }

        [[nodiscard]] auto attr_name() const noexcept -> std::string_view
        {
            return detail::get_str_or(buffer(), offset(), "arg3");
        }
        void attr_name(std::string_view _val)
        {
            buffer().set_str(offset(), "arg3", _val);
        }

        [[nodiscard]] auto attr_value() const noexcept -> std::string_view
        {
            return detail::get_str_or(buffer(), offset(), "arg4");
        }
        void attr_value(std::string_view _val)
        {
            buffer().set_str(offset(), "arg4", _val);
        }

        [[nodiscard]] auto attr_unit() const noexcept -> std::string_view
        {
            return detail::get_str_or(buffer(), offset(), "arg5");
        }
        void attr_unit(std::string_view _val)
        {
            buffer().set_str(offset(), "arg5", _val);
        }

        [[nodiscard]] auto new_attr_name() const noexcept -> std::string_view
        {
            return detail::get_str_or(buffer(), offset(), "arg6");
        }
        void new_attr_name(std::string_view _val)
        {
            buffer().set_str(offset(), "arg6", _val);
        }

        [[nodiscard]] auto new_attr_value() const noexcept -> std::string_view
        {
            return detail::get_str_or(buffer(), offset(), "arg7");
        }
        void new_attr_value(std::string_view _val)
        {
            buffer().set_str(offset(), "arg7", _val);
        }

        [[nodiscard]] auto new_attr_unit() const noexcept -> std::string_view
        {
            return detail::get_str_or(buffer(), offset(), "arg8");
        }
        void new_attr_unit(std::string_view _val)
        {
            buffer().set_str(offset(), "arg8", _val);
        }

        [[nodiscard]] auto cond_input() -> KeyValPairView
        {
            const auto kvp_ofs = detail::ensure_sub_obj(buffer(), offset(), "KeyValPair_PI");
            return KeyValPairView{&buffer(), kvp_ofs};
        }

        [[nodiscard]] auto cond_input() const noexcept -> KeyValPairView
        {
            if (buffer().get_type(offset(), "KeyValPair_PI") == lite3cpp::Type::Object) {
                return KeyValPairView{
                    const_cast<lite3cpp::Buffer*>(&buffer()), buffer().get_obj(offset(), "KeyValPair_PI")};
            }
            return KeyValPairView{nullptr, 0};
        }
    };

    /// \brief `ExecCmd_PI` (`execCmd_t`) facade over a `lite3cpp::Buffer`.
    class ExecCmdInp : public struct_buffer<ExecCmdInp, ::execCmd_t>
    {
    public:
        static constexpr std::string_view pi_name = "ExecCmd_PI";

        using struct_buffer<ExecCmdInp, ::execCmd_t>::struct_buffer;

        [[nodiscard]] auto cmd() const noexcept -> std::string_view
        {
            return detail::get_str_or(buffer(), offset(), "cmd");
        }
        void cmd(std::string_view _val)
        {
            buffer().set_str(offset(), "cmd", _val);
        }

        [[nodiscard]] auto cmd_argv() const noexcept -> std::string_view
        {
            return detail::get_str_or(buffer(), offset(), "cmdArgv");
        }
        void cmd_argv(std::string_view _val)
        {
            buffer().set_str(offset(), "cmdArgv", _val);
        }

        [[nodiscard]] auto exec_addr() const noexcept -> std::string_view
        {
            return detail::get_str_or(buffer(), offset(), "execAddr");
        }
        void exec_addr(std::string_view _val)
        {
            buffer().set_str(offset(), "execAddr", _val);
        }

        [[nodiscard]] auto hint_path() const noexcept -> std::string_view
        {
            return detail::get_str_or(buffer(), offset(), "hintPath");
        }
        void hint_path(std::string_view _val)
        {
            buffer().set_str(offset(), "hintPath", _val);
        }

        [[nodiscard]] auto add_path_to_argv() const noexcept -> int
        {
            return static_cast<int>(detail::get_i64_or(buffer(), offset(), "addPathToArgv"));
        }
        void add_path_to_argv(int _val)
        {
            buffer().set_i64(offset(), "addPathToArgv", _val);
        }

        [[nodiscard]] auto cond_input() -> KeyValPairView
        {
            const auto kvp_ofs = detail::ensure_sub_obj(buffer(), offset(), "KeyValPair_PI");
            return KeyValPairView{&buffer(), kvp_ofs};
        }

        [[nodiscard]] auto cond_input() const noexcept -> KeyValPairView
        {
            if (buffer().get_type(offset(), "KeyValPair_PI") == lite3cpp::Type::Object) {
                return KeyValPairView{
                    const_cast<lite3cpp::Buffer*>(&buffer()), buffer().get_obj(offset(), "KeyValPair_PI")};
            }
            return KeyValPairView{nullptr, 0};
        }
    };

    /// \brief `ExecCmdOut_PI` (`execCmdOut_t`) facade over a `lite3cpp::Buffer`.
    class ExecCmdOut : public struct_buffer<ExecCmdOut, ::execCmdOut_t>
    {
    public:
        static constexpr std::string_view pi_name = "ExecCmdOut_PI";

        using struct_buffer<ExecCmdOut, ::execCmdOut_t>::struct_buffer;

        [[nodiscard]] auto status() const noexcept -> int
        {
            return static_cast<int>(detail::get_i64_or(buffer(), offset(), "status"));
        }
        void status(int _val)
        {
            buffer().set_i64(offset(), "status", _val);
        }
    };

    // Domain Type Aliases (preserving readability across server, client, and plugins):
    using keyValPair_t = KeyValPair;
    using specColl_t = SpecColl;
    using dataObjInp_t = DataObjInp;
    using openedDataObjInp_t = OpenedDataObjInp;
    using dataObjCopyInp_t = DataObjCopyInp;
    using collInp_t = CollInp;
    using dataObjInfo_t = DataObjInfo;
    using rodsObjStat_t = RodsObjStat;
    using modAVUMetadataInp_t = ModAVUMetadataInp;
    using execCmd_t = ExecCmdInp;
    using execCmdOut_t = ExecCmdOut;

    // Transitional C-style helper overloads for KeyValPairView / KeyValPair:
    inline auto addKeyVal(KeyValPairView* _cond_input, const char* _kw, const char* _value) -> int
    {
        if (!_cond_input || !_kw) {
            return SYS_INTERNAL_NULL_INPUT_ERR;
        }
        _cond_input->set(_kw, _value ? _value : "");
        return 0;
    }

    inline auto addKeyVal(KeyValPairView& _cond_input, std::string_view _kw, std::string_view _value) -> int
    {
        _cond_input.set(_kw, _value);
        return 0;
    }

    inline auto addKeyVal(KeyValPair* _cond_input, const char* _kw, const char* _value) -> int
    {
        if (!_cond_input || !_kw) {
            return SYS_INTERNAL_NULL_INPUT_ERR;
        }
        _cond_input->set(_kw, _value ? _value : "");
        return 0;
    }

    inline auto addKeyVal(KeyValPair& _cond_input, std::string_view _kw, std::string_view _value) -> int
    {
        _cond_input.set(_kw, _value);
        return 0;
    }

    inline auto getValByKey(const KeyValPairView* _cond_input, const char* _kw) noexcept -> const char*
    {
        if (!_cond_input || !_kw) {
            return nullptr;
        }
        return _cond_input->c_str(_kw);
    }

    inline auto getValByKey(const KeyValPairView& _cond_input, std::string_view _kw) noexcept -> const char*
    {
        return _cond_input.c_str(_kw);
    }

    inline auto getValByKey(const KeyValPair* _cond_input, const char* _kw) noexcept -> const char*
    {
        if (!_cond_input || !_kw) {
            return nullptr;
        }
        return _cond_input->c_str(_kw);
    }

    inline auto getValByKey(const KeyValPair& _cond_input, std::string_view _kw) noexcept -> const char*
    {
        return _cond_input.c_str(_kw);
    }

    inline auto rmKeyVal(KeyValPairView* _cond_input, const char* _kw) -> int
    {
        if (!_cond_input || !_kw) {
            return SYS_INTERNAL_NULL_INPUT_ERR;
        }
        _cond_input->erase(_kw);
        return 0;
    }

    inline auto rmKeyVal(KeyValPair* _cond_input, const char* _kw) -> int
    {
        if (!_cond_input || !_kw) {
            return SYS_INTERNAL_NULL_INPUT_ERR;
        }
        _cond_input->erase(_kw);
        return 0;
    }

    inline auto clearKeyVal(KeyValPairView* _cond_input) noexcept -> int
    {
        if (_cond_input) {
            _cond_input->clear();
        }
        return 0;
    }

    inline auto clearKeyVal(KeyValPair* _cond_input) noexcept -> int
    {
        if (_cond_input) {
            _cond_input->clear();
        }
        return 0;
    }

    /// Scoped active `lite3cpp::Buffer` request/reply context for `rsApiHandler` when `LITE3_PROT` is active.
    /// Exposes the raw `LITE3_PROT` request buffer directly to `rs*` API handlers via typed
    /// `struct_buffer` facades (`DataObjInp`, `CollInp`, etc.) and allows handlers to emit a
    /// pre-built `lite3cpp::Buffer` reply directly to `sendApiReply` without legacy struct packing.
    class active_api_buffer_scope
    {
    public:
        active_api_buffer_scope(const bytesBuf_t* _in_bbuf, irodsProt_t _prot) noexcept
            : prev_{current_scope_()}
            , prot_{_prot}
        {
            if (_in_bbuf && _in_bbuf->buf && _in_bbuf->len >= static_cast<int>(lite3cpp::config::node_size) &&
                _prot == LITE3_PROT)
            {
                try {
                    req_buf_.emplace(static_cast<const std::uint8_t*>(_in_bbuf->buf),
                                     static_cast<std::size_t>(_in_bbuf->len));
                }
                catch (...) {
                }
            }
            current_scope_() = this;
        }

        ~active_api_buffer_scope() noexcept
        {
            current_scope_() = prev_;
        }

        active_api_buffer_scope(const active_api_buffer_scope&) = delete;
        auto operator=(const active_api_buffer_scope&) -> active_api_buffer_scope& = delete;
        active_api_buffer_scope(active_api_buffer_scope&&) = delete;
        auto operator=(active_api_buffer_scope&&) -> active_api_buffer_scope& = delete;

        [[nodiscard]] static auto has_active_request_buffer() noexcept -> bool
        {
            const auto* s = current_scope_();
            return s && s->req_buf_.has_value();
        }

        [[nodiscard]] static auto active_request_buffer() noexcept -> const lite3cpp::Buffer*
        {
            const auto* s = current_scope_();
            return (s && s->req_buf_.has_value()) ? &(*s->req_buf_) : nullptr;
        }

        template <typename StructBufT>
        [[nodiscard]] static auto try_get_active_request() -> std::optional<StructBufT>
        {
            if (const auto* buf = active_request_buffer()) {
                try {
                    return StructBufT{lite3cpp::Buffer{buf->data(), buf->size()}};
                }
                catch (...) {
                }
            }
            return std::nullopt;
        }

        static void set_active_reply_buffer(lite3cpp::Buffer&& _reply) noexcept
        {
            if (auto* s = current_scope_(); s && s->prot_ == LITE3_PROT) {
                s->reply_buf_ = std::move(_reply);
            }
        }

        template <typename StructBufT>
        static void set_active_reply(StructBufT&& _reply) noexcept
        {
            set_active_reply_buffer(std::move(_reply.buffer()));
        }

        [[nodiscard]] static auto has_active_reply_buffer() noexcept -> bool
        {
            const auto* s = current_scope_();
            return s && s->reply_buf_.has_value();
        }

        [[nodiscard]] static auto take_active_reply_buffer() noexcept -> std::optional<lite3cpp::Buffer>
        {
            if (auto* s = current_scope_(); s && s->reply_buf_.has_value()) {
                auto out = std::move(*s->reply_buf_);
                s->reply_buf_.reset();
                return out;
            }
            return std::nullopt;
        }

    private:
        static auto current_scope_() noexcept -> active_api_buffer_scope*&
        {
            thread_local active_api_buffer_scope* scope = nullptr;
            return scope;
        }

        active_api_buffer_scope* prev_{nullptr};
        irodsProt_t prot_{NATIVE_PROT};
        std::optional<lite3cpp::Buffer> req_buf_;
        std::optional<lite3cpp::Buffer> reply_buf_;
    };

} // namespace irods::protocol

#endif // IRODS_PROTOCOL_LITE3_STRUCTS_HPP
