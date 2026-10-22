#undef IRODS_FILESYSTEM_RECURSIVE_COLLECTION_ITERATOR_HPP_INCLUDE_HEADER

#if defined(IRODS_FILESYSTEM_ENABLE_SERVER_SIDE_API)
#  if !defined(IRODS_FILESYSTEM_RECURSIVE_COLLECTION_ITERATOR_HPP_FOR_SERVER)
#    define IRODS_FILESYSTEM_RECURSIVE_COLLECTION_ITERATOR_HPP_FOR_SERVER
#    define IRODS_FILESYSTEM_RECURSIVE_COLLECTION_ITERATOR_HPP_INCLUDE_HEADER
#  endif
#elif !defined(IRODS_FILESYSTEM_RECURSIVE_COLLECTION_ITERATOR_HPP_FOR_CLIENT)
#  define IRODS_FILESYSTEM_RECURSIVE_COLLECTION_ITERATOR_HPP_FOR_CLIENT
#  define IRODS_FILESYSTEM_RECURSIVE_COLLECTION_ITERATOR_HPP_INCLUDE_HEADER
#endif

#ifdef IRODS_FILESYSTEM_RECURSIVE_COLLECTION_ITERATOR_HPP_INCLUDE_HEADER

#include "irods/filesystem/config.hpp"
#include "irods/filesystem/collection_iterator.hpp"
#include "irods/filesystem/collection_entry.hpp"

#include <iterator>
#include <memory>
#include <stack>
#include <system_error>

namespace irods::filesystem::NAMESPACE_IMPL
{
    class recursive_collection_iterator
    {
    public:
        // clang-format off
        using value_type        = collection_entry;
        using difference_type   = std::ptrdiff_t;
        using pointer           = const value_type*;
        using reference         = const value_type&;
        using iterator_category = std::input_iterator_tag;
        // clang-format on

        using post_increment_proxy = collection_iterator::post_increment_proxy;

        // Constructors and destructor

        recursive_collection_iterator() = default;

        recursive_collection_iterator(rxComm& _comm,
                                      const path& _p,
                                      collection_options _opts = collection_options::none);

        recursive_collection_iterator(rxComm& _comm,
                                      const path& _p,
                                      std::error_code& _ec);

        recursive_collection_iterator(rxComm& _comm,
                                      const path& _p,
                                      collection_options _opts,
                                      std::error_code& _ec);

        recursive_collection_iterator(const recursive_collection_iterator& _other) = default;
        auto operator=(const recursive_collection_iterator& _other) -> recursive_collection_iterator& = default;

        recursive_collection_iterator(recursive_collection_iterator&& _other) = default;
        auto operator=(recursive_collection_iterator&& _other) -> recursive_collection_iterator& = default;

        ~recursive_collection_iterator() = default;

        // Observers

        auto connection() const noexcept -> rxComm*
        {
            return (ctx_ && !ctx_->stack.empty()) ? ctx_->stack.top().connection() : nullptr;
        }

        // clang-format off
        auto operator*() const noexcept -> reference { return *ctx_->stack.top(); }
        auto operator->() const noexcept -> pointer  { return &*ctx_->stack.top(); }

        auto options() const noexcept -> collection_options { return ctx_ ? ctx_->opts : collection_options::none; }
        auto depth() const noexcept -> int                  { return ctx_ ? static_cast<int>(ctx_->stack.size()) - 1 : -1; }
        auto recursion_pending() const noexcept -> bool     { return ctx_ ? ctx_->recurse : false; }
        // clang-format on

        // Modifiers

        auto operator++() -> recursive_collection_iterator&;
        auto increment(std::error_code& _ec) -> recursive_collection_iterator&;

        auto operator++(int) -> post_increment_proxy
        {
            post_increment_proxy proxy{**this};
            ++(*this);
            return proxy;
        }

        auto pop() -> void;
        auto pop(std::error_code& _ec) -> void;

        auto disable_recursion_pending() noexcept -> void
        {
            if (ctx_) {
                ctx_->recurse = false;
            }
        }

        // Compare

        // clang-format off
        auto operator==(const recursive_collection_iterator& _rhs) const noexcept -> bool { return _rhs.ctx_ == ctx_; }
        auto operator!=(const recursive_collection_iterator& _rhs) const noexcept -> bool { return !(*this == _rhs); }
        auto operator==(std::default_sentinel_t) const noexcept -> bool { return !ctx_; }
        // clang-format on

    private:
        struct context
        {
            std::stack<collection_iterator> stack;
            collection_options opts = collection_options::none;
            bool recurse = true;
        };

        std::shared_ptr<context> ctx_;
    };

    // Enables support for range-based for-loops.

    inline auto begin(recursive_collection_iterator _iter) noexcept -> recursive_collection_iterator
    {
        return _iter;
    }

    inline auto end([[maybe_unused]] const recursive_collection_iterator& _iter) noexcept
        -> recursive_collection_iterator
    {
        return {};
    }
} // namespace irods::filesystem::NAMESPACE_IMPL

#endif // IRODS_FILESYSTEM_RECURSIVE_COLLECTION_ITERATOR_HPP_INCLUDE_HEADER
