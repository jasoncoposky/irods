#include "irods/filesystem/collection_iterator.hpp"

#include "irods/filesystem/path_utilities.hpp"
#include "irods/filesystem/filesystem_error.hpp"

#ifdef IRODS_FILESYSTEM_ENABLE_SERVER_SIDE_API
#  include "irods/rsOpenCollection.hpp"
#  include "irods/rsReadCollection.hpp"
#  include "irods/rsCloseCollection.hpp"
#endif // IRODS_FILESYSTEM_ENABLE_SERVER_SIDE_API

#include "irods/collection_entry_validation.hpp"
#include "irods/irods_at_scope_exit.hpp"
#include "irods/rodsErrorTable.h"
#include "irods/system_error.hpp"

#include <cassert>
#include <charconv>
#include <chrono>
#include <cstdlib>
#include <string_view>

namespace irods::experimental::filesystem::NAMESPACE_IMPL
{
    namespace
    {
        auto parse_timestamp(const char* _str, object_time_type& _out) noexcept -> void
        {
            if (!_str) {
                _out = object_time_type{};
                return;
            }

            const std::string_view sv{_str};
            long long secs = 0;
            const auto [ptr, err] = std::from_chars(sv.data(), sv.data() + sv.size(), secs);

            if (err == std::errc{} && ptr == sv.data() + sv.size()) {
                _out = object_time_type{std::chrono::seconds{secs}};
            }
            else {
                _out = object_time_type{};
            }
        }
    } // anonymous namespace

    collection_iterator::context::~context() noexcept
    {
        close();
    }

    auto collection_iterator::context::close() noexcept -> void
    {
        if (handle_open) {
            handle_open = false;
#ifdef IRODS_FILESYSTEM_ENABLE_SERVER_SIDE_API
            rsCloseCollection(comm, &handle);
#else
            rclCloseCollection(&handle);
#endif // IRODS_FILESYSTEM_ENABLE_SERVER_SIDE_API
        }
    }

    collection_iterator::collection_iterator(rxComm& _comm,
                                             const path& _p,
                                             collection_options _opts)
        : ctx_{}
    {
        std::error_code ec;
        *this = collection_iterator{_comm, _p, _opts, ec};
        if (ec) {
            throw filesystem_error{"could not open collection for reading", _p, ec};
        }
    }

    collection_iterator::collection_iterator(rxComm& _comm,
                                             const path& _p,
                                             std::error_code& _ec)
        : collection_iterator{_comm, _p, collection_options::none, _ec}
    {
    }

    collection_iterator::collection_iterator(rxComm& _comm,
                                             const path& _p,
                                             collection_options _opts,
                                             std::error_code& _ec)
        : ctx_{}
    {
        if (!validate_path(_p, _ec)) {
            return;
        }

#ifdef IRODS_FILESYSTEM_ENABLE_SERVER_SIDE_API
        collInp_t input{};
        if (!copy_to_c_array(input.collName, _p.string_view(), _ec)) {
            return;
        }

        const auto handle = rsOpenCollection(&_comm, &input);
        if (handle < 0) {
            if (_opts == collection_options::skip_permission_denied &&
                (handle == CAT_NO_ACCESS_PERMISSION || handle == SYS_NO_API_PRIV))
            {
                _ec.clear();
                return;
            }

            _ec = make_error_code(handle);
            return;
        }
#else
        collHandle_t handle{};
        const auto no_flags = 0;

        if (const auto ec = rclOpenCollection(&_comm, const_cast<char*>(_p.c_str()), no_flags, &handle); ec < 0) {
            if (_opts == collection_options::skip_permission_denied &&
                (ec == CAT_NO_ACCESS_PERMISSION || ec == SYS_NO_API_PRIV))
            {
                _ec.clear();
                return;
            }

            _ec = make_error_code(ec);
            return;
        }
#endif // IRODS_FILESYSTEM_ENABLE_SERVER_SIDE_API

        ctx_ = std::make_shared<context>();
        ctx_->comm = &_comm;
        ctx_->path = _p;
        ctx_->opts = _opts;
        ctx_->handle = handle;
        ctx_->handle_open = true;

        // Point to the first entry.
        increment(_ec);
        if (_ec) {
            ctx_.reset();
        }
    }

    collection_iterator::~collection_iterator()
    {
        close();
    }

    auto collection_iterator::operator++() -> collection_iterator&
    {
        std::error_code ec;
        increment(ec);
        if (ec) {
            throw filesystem_error{"could not read collection entry", ec};
        }
        return *this;
    }

    auto collection_iterator::increment(std::error_code& _ec) -> collection_iterator&
    {
        _ec.clear();

        if (!ctx_) {
            return *this;
        }

        collEnt_t* e{};

#ifdef IRODS_FILESYSTEM_ENABLE_SERVER_SIDE_API
        const auto ec = rsReadCollection(ctx_->comm, &ctx_->handle, &e);
        irods::at_scope_exit free_collection_entry{[&e] {
            if (e) {
                // Do NOT free the contents of the collEnt_t. Its contents are managed
                // by the rs* collection APIs.
                std::free(e);
            }
        }};
#else
        // The rcl* collection functions manage memory for us. These functions
        // will free any heap allocated memory for us. Attempting to free any memory
        // returned from rclReadCollection will result in a crash.
        collEnt_t ce{};
        e = &ce;
        const auto ec = rclReadCollection(ctx_->comm, &ctx_->handle, &ce);
#endif // IRODS_FILESYSTEM_ENABLE_SERVER_SIDE_API

        if (ec < 0) {
            if (ec == CAT_NO_ROWS_FOUND ||
                (ctx_->opts == collection_options::skip_permission_denied &&
                 (ec == CAT_NO_ACCESS_PERMISSION || ec == SYS_NO_API_PRIV)))
            {
                close();
                ctx_.reset();
                return *this;
            }

            _ec = make_error_code(ec);
            close();
            ctx_.reset();
            return *this;
        }

        if (!e || !irods::is_valid_collection_entry(*e)) {
            _ec = make_error_code(SYS_INVALID_FILE_PATH);
            close();
            ctx_.reset();
            return *this;
        }

        auto& entry = ctx_->entry;

        entry.data_mode_ = static_cast<unsigned>(e->dataMode);
        entry.data_size_ = e->dataSize >= 0 ? static_cast<std::uintmax_t>(e->dataSize) : 0;

        if (e->dataId) {
            entry.data_id_.assign(e->dataId);
        }
        else {
            entry.data_id_.clear();
        }

        parse_timestamp(e->createTime, entry.ctime_);
        parse_timestamp(e->modifyTime, entry.mtime_);

        if (e->chksum) {
            entry.checksum_.assign(e->chksum);
        }
        else {
            entry.checksum_.clear();
        }

        if (e->ownerName) {
            entry.owner_.assign(e->ownerName);
        }
        else {
            entry.owner_.clear();
        }

        if (e->dataType) {
            entry.data_type_.assign(e->dataType);
        }
        else {
            entry.data_type_.clear();
        }

        switch (e->objType) {
            case COLL_OBJ_T:
                entry.status_.type(object_type::collection);
                entry.path_.assign(std::string_view{e->collName});
                break;

            case DATA_OBJ_T:
                entry.status_.type(object_type::data_object);
                entry.path_.assign_concat(ctx_->path.string_view(), e->dataName);
                break;

            default:
                entry.status_.type(object_type::none);
                entry.path_.clear();
                break;
        }

        return *this;
    }

    auto collection_iterator::close() noexcept -> void
    {
        if (ctx_.use_count() == 1) {
            ctx_->close();
        }
    }
} // namespace irods::experimental::filesystem::NAMESPACE_IMPL
