#include "irods/filesystem/recursive_collection_iterator.hpp"

#include "irods/filesystem/filesystem_error.hpp"

namespace irods::experimental::filesystem::NAMESPACE_IMPL
{
    // Constructors and destructor

    recursive_collection_iterator::recursive_collection_iterator(rxComm& _comm,
                                                                 const path& _p,
                                                                 collection_options _opts)
        : ctx_{}
    {
        std::error_code ec;
        *this = recursive_collection_iterator{_comm, _p, _opts, ec};
        if (ec) {
            throw filesystem_error{"could not open collection for reading", _p, ec};
        }
    }

    recursive_collection_iterator::recursive_collection_iterator(rxComm& _comm,
                                                                 const path& _p,
                                                                 std::error_code& _ec)
        : recursive_collection_iterator{_comm, _p, collection_options::none, _ec}
    {
    }

    recursive_collection_iterator::recursive_collection_iterator(rxComm& _comm,
                                                                 const path& _p,
                                                                 collection_options _opts,
                                                                 std::error_code& _ec)
        : ctx_{}
    {
        _ec.clear();
        if (collection_iterator iter{_comm, _p, _opts, _ec}; !_ec && iter != std::default_sentinel) {
            ctx_ = std::make_shared<context>();
            ctx_->opts = _opts;
            ctx_->stack.push(std::move(iter));
        }
    }

    // Modifiers

    auto recursive_collection_iterator::operator++() -> recursive_collection_iterator&
    {
        std::error_code ec;
        increment(ec);
        if (ec) {
            throw filesystem_error{"could not increment recursive collection iterator", ec};
        }
        return *this;
    }

    auto recursive_collection_iterator::increment(std::error_code& _ec) -> recursive_collection_iterator&
    {
        _ec.clear();

        if (!ctx_ || ctx_->stack.empty()) {
            ctx_.reset();
            return *this;
        }

        auto& iter = ctx_->stack.top();
        bool added_new_collection = false;

        if (iter->is_collection() && ctx_->recurse) {
            collection_iterator sub_iter{*iter.connection(), iter->path(), ctx_->opts, _ec};
            if (_ec) {
                ctx_.reset();
                return *this;
            }

            if (sub_iter != std::default_sentinel) {
                added_new_collection = true;

                // Move the iterator forward to avoid re-entering the same
                // collection when the stack is popped.
                iter.increment(_ec);
                if (_ec) {
                    ctx_.reset();
                    return *this;
                }

                if (iter == std::default_sentinel) {
                    ctx_->stack.pop();
                }

                ctx_->stack.push(std::move(sub_iter));
            }
        }

        // Handle leaving a collection. Increment the iterator only if a new
        // collection was NOT pushed on to the stack. Reordering the conditions
        // in this if-stmt will make this iterator impl produce incorrect results.
        if (!added_new_collection) {
            iter.increment(_ec);
            if (_ec) {
                ctx_.reset();
                return *this;
            }

            if (iter == std::default_sentinel) {
                ctx_->stack.pop();

                if (ctx_->stack.empty()) {
                    ctx_.reset();
                    return *this;
                }
            }
        }

        // Iterating forward must always reset the iterator to recurse into
        // subcollections as defined by the C++ Std.
        ctx_->recurse = true;

        return *this;
    }

    auto recursive_collection_iterator::pop() -> void
    {
        std::error_code ec;
        pop(ec);
    }

    auto recursive_collection_iterator::pop(std::error_code& _ec) -> void
    {
        _ec.clear();

        if (!ctx_ || ctx_->stack.empty()) {
            ctx_.reset();
            return;
        }

        // If there is only a single iterator on the stack, then popping it must
        // produce the end iterator.
        if (ctx_->stack.size() == 1) {
            ctx_.reset();
            return;
        }

        ctx_->stack.pop();
    }
} // namespace irods::experimental::filesystem::NAMESPACE_IMPL
