#undef IRODS_FILESYSTEM_COLLECTION_ITERATOR_HPP_INCLUDE_HEADER

#if defined(IRODS_FILESYSTEM_ENABLE_SERVER_SIDE_API)
#  if !defined(IRODS_FILESYSTEM_COLLECTION_ITERATOR_HPP_FOR_SERVER)
#    define IRODS_FILESYSTEM_COLLECTION_ITERATOR_HPP_FOR_SERVER
#    define IRODS_FILESYSTEM_COLLECTION_ITERATOR_HPP_INCLUDE_HEADER
#  endif
#elif !defined(IRODS_FILESYSTEM_COLLECTION_ITERATOR_HPP_FOR_CLIENT)
#  define IRODS_FILESYSTEM_COLLECTION_ITERATOR_HPP_FOR_CLIENT
#  define IRODS_FILESYSTEM_COLLECTION_ITERATOR_HPP_INCLUDE_HEADER
#endif

#ifdef IRODS_FILESYSTEM_COLLECTION_ITERATOR_HPP_INCLUDE_HEADER

#include "irods/filesystem/config.hpp"
#include "irods/filesystem/filesystem.hpp"
#include "irods/filesystem/collection_entry.hpp"
#include "irods/filesystem/path.hpp"

#ifndef IRODS_FILESYSTEM_ENABLE_SERVER_SIDE_API
#  include "irods/miscUtil.h"
#endif // IRODS_FILESYSTEM_ENABLE_SERVER_SIDE_API

#include <iterator>
#include <memory>
#include <system_error>
#include <utility>

#ifndef IRODS_FILESYSTEM_COLLECTION_OPTIONS_DEFINED
#define IRODS_FILESYSTEM_COLLECTION_OPTIONS_DEFINED
namespace irods::experimental::filesystem
{
    enum class collection_options
    {
        none,
        skip_permission_denied
    };
} // namespace irods::experimental::filesystem
#endif // IRODS_FILESYSTEM_COLLECTION_OPTIONS_DEFINED

namespace irods::experimental::filesystem::NAMESPACE_IMPL
{
    using ::irods::experimental::filesystem::collection_options;

    class collection_iterator
    {
    public:
        // clang-format off
        using value_type        = collection_entry;
        using difference_type   = std::ptrdiff_t;
        using pointer           = const value_type*;
        using reference         = const value_type&;
        using iterator_category = std::input_iterator_tag;
        // clang-format on

        class post_increment_proxy
        {
        public:
            explicit post_increment_proxy(value_type _val) noexcept
                : val_{std::move(_val)}
            {
            }

            auto operator*() const noexcept -> const value_type& { return val_; }
            auto operator->() const noexcept -> const value_type* { return &val_; }

        private:
            value_type val_;
        };

        // Constructors and destructor

        collection_iterator() = default;

        collection_iterator(rxComm& _comm,
                            const path& _p,
                            collection_options _opts = collection_options::none);

        collection_iterator(rxComm& _comm,
                            const path& _p,
                            std::error_code& _ec);

        collection_iterator(rxComm& _comm,
                            const path& _p,
                            collection_options _opts,
                            std::error_code& _ec);

        collection_iterator(const collection_iterator& _other) = default;
        auto operator=(const collection_iterator& _other) -> collection_iterator& = default;

        collection_iterator(collection_iterator&& _other) = default;
        auto operator=(collection_iterator&& _other) -> collection_iterator& = default;

        ~collection_iterator();

        // Observers

        auto connection() const noexcept -> rxComm* { return ctx_ ? ctx_->comm : nullptr; }

        // clang-format off
        auto operator*() const noexcept -> reference { return ctx_->entry; }
        auto operator->() const noexcept -> pointer  { return &ctx_->entry; }
        // clang-format on

        // Modifiers

        auto operator++() -> collection_iterator&;
        auto increment(std::error_code& _ec) -> collection_iterator&;

        auto operator++(int) -> post_increment_proxy
        {
            post_increment_proxy proxy{**this};
            ++(*this);
            return proxy;
        }

        // Compare

        // clang-format off
        auto operator==(const collection_iterator& _rhs) const noexcept -> bool { return _rhs.ctx_ == ctx_; }
        auto operator!=(const collection_iterator& _rhs) const noexcept -> bool { return !(*this == _rhs); }
        auto operator==(std::default_sentinel_t) const noexcept -> bool { return !ctx_; }
        // clang-format on

    private:
        struct context
        {
            rxComm* comm{};
            path path{};
            collection_options opts{collection_options::none};
            bool handle_open{false};
#ifdef IRODS_FILESYSTEM_ENABLE_SERVER_SIDE_API
            int handle{-1};
#else
            collHandle_t handle{};
#endif // IRODS_FILESYSTEM_ENABLE_SERVER_SIDE_API
            value_type entry{};

            ~context() noexcept;
            auto close() noexcept -> void;
        };

        auto close() noexcept -> void;

        std::shared_ptr<context> ctx_;
    };

    // Enables support for range-based for-loops.

    inline auto begin(collection_iterator _iter) noexcept -> collection_iterator
    {
        return _iter;
    }

    inline auto end([[maybe_unused]] const collection_iterator& _iter) noexcept -> collection_iterator
    {
        return {};
    }
} // namespace irods::experimental::filesystem::NAMESPACE_IMPL

#endif // IRODS_FILESYSTEM_COLLECTION_ITERATOR_HPP_INCLUDE_HEADER
