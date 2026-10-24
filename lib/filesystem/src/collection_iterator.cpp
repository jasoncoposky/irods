#include "irods/filesystem/collection_iterator.hpp"

#include "irods/filesystem/path_utilities.hpp"
#include "irods/filesystem/filesystem_error.hpp"

#ifdef IRODS_FILESYSTEM_ENABLE_SERVER_SIDE_API
#  include "irods/rs_genquery2.hpp"
#  include "irods/rsObjStat.hpp"
#  include "irods/rsOpenCollection.hpp"
#  include "irods/rsReadCollection.hpp"
#  include "irods/rsCloseCollection.hpp"
#else
#  include "irods/genquery2.h"
#  include "irods/objStat.h"
#endif // IRODS_FILESYSTEM_ENABLE_SERVER_SIDE_API

#include "irods/collection_entry_validation.hpp"
#include "irods/genquery2_builder.hpp"
#include "irods/irods_at_scope_exit.hpp"
#include "irods/rcMisc.h"
#include "irods/rodsErrorTable.h"
#include "irods/system_error.hpp"

#include <nlohmann/json.hpp>

#include <cassert>
#include <charconv>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <string_view>

namespace irods::filesystem::NAMESPACE_IMPL
{
    namespace
    {
        namespace gq2 = irods::experimental::genquery2;
        using gq2::builder::col;

        constexpr std::size_t gq2_page_size = 256;

        auto parse_timestamp_sv(std::string_view _sv, object_time_type& _out) noexcept -> void
        {
            if (_sv.empty()) {
                _out = object_time_type{};
                return;
            }

            long long secs = 0;
            const auto [ptr, err] = std::from_chars(_sv.data(), _sv.data() + _sv.size(), secs);

            if (err == std::errc{} && ptr == _sv.data() + _sv.size()) {
                _out = object_time_type{std::chrono::seconds{secs}};
            }
            else {
                _out = object_time_type{};
            }
        }

        auto parse_timestamp(const char* _str, object_time_type& _out) noexcept -> void
        {
            if (!_str) {
                _out = object_time_type{};
                return;
            }
            parse_timestamp_sv(std::string_view{_str}, _out);
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

        dataObjInp_t stat_inp{};
        if (!copy_to_c_array(stat_inp.objPath, _p.string_view(), _ec)) {
            return;
        }

        rodsObjStat_t* stat_out{};
        const auto stat_ec = rxObjStat(&_comm, &stat_inp, &stat_out);
        irods::at_scope_exit free_stat{[&stat_out] { freeRodsObjStat(stat_out); }};

        if (stat_ec < 0) {
            if (_opts == collection_options::skip_permission_denied &&
                (stat_ec == CAT_NO_ACCESS_PERMISSION || stat_ec == SYS_NO_API_PRIV))
            {
                _ec.clear();
                return;
            }
            _ec = make_error_code(stat_ec);
            return;
        }

        if (!stat_out || stat_out->objType != COLL_OBJ_T) {
#ifdef IRODS_FILESYSTEM_ENABLE_SERVER_SIDE_API
            _ec = make_error_code(CAT_NAME_EXISTS_AS_DATAOBJ);
#else
            _ec = make_error_code(CAT_UNKNOWN_COLLECTION);
#endif
            return;
        }

        const bool is_non_linked_spec_coll =
            (stat_out->specColl != nullptr && stat_out->specColl->collClass != LINKED_COLL);

        if (!is_non_linked_spec_coll) {
            ctx_ = std::make_shared<context>();
            ctx_->comm = &_comm;
            ctx_->path = _p;
            if (stat_out->specColl != nullptr &&
                stat_out->specColl->collClass == LINKED_COLL &&
                std::strlen(stat_out->specColl->objPath) > 0)
            {
                ctx_->target_coll = stat_out->specColl->objPath;
            }
            else {
                ctx_->target_coll = std::string{_p.string_view()};
                while (ctx_->target_coll.size() > 1 && ctx_->target_coll.back() == '/') {
                    ctx_->target_coll.pop_back();
                }
            }
            ctx_->opts = _opts;
            ctx_->use_genquery2 = true;

            increment(_ec);
            if (_ec) {
                ctx_.reset();
            }
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

        if (ctx_->use_genquery2) {
            while (ctx_->batch_index >= ctx_->batch.size()) {
                ctx_->batch.clear();
                ctx_->batch_index = 0;

                if (ctx_->stage == context::query_stage::done) {
                    ctx_.reset();
                    return *this;
                }

                const auto zh = zone_name_view(ctx_->path).value_or(std::string_view{});

                if (ctx_->stage == context::query_stage::subcollections) {
                    const auto q = gq2::builder::select({"COLL_NAME",
                                                         "COLL_OWNER_NAME",
                                                         "COLL_CREATE_TIME",
                                                         "COLL_MODIFY_TIME"})
                                       .where(col("COLL_NAME") != "/" &&
                                              col("COLL_PARENT_NAME") == ctx_->target_coll)
                                       .order_by("COLL_NAME")
                                       .offset(ctx_->row_offset)
                                       .limit(gq2_page_size)
                                       .build();

                    nlohmann::json rows;
                    const int ec = gq2::try_execute(*ctx_->comm, q, rows, zh);
                    if (ec < 0 && ec != CAT_NO_ROWS_FOUND) {
                        if (ctx_->opts == collection_options::skip_permission_denied &&
                            (ec == CAT_NO_ACCESS_PERMISSION || ec == SYS_NO_API_PRIV))
                        {
                            ctx_.reset();
                            return *this;
                        }
                        _ec = make_error_code(ec);
                        ctx_.reset();
                        return *this;
                    }

                    if (ec == CAT_NO_ROWS_FOUND || rows.empty()) {
                        ctx_->stage = context::query_stage::data_objects;
                        ctx_->row_offset = 0;
                        continue;
                    }

                    ctx_->row_offset += rows.size();
                    if (rows.size() < gq2_page_size) {
                        ctx_->stage = context::query_stage::data_objects;
                        ctx_->row_offset = 0;
                    }

                    ctx_->batch.reserve(rows.size());
                    for (const auto& row : rows) {
                        const auto& coll_name = row[0].get_ref<const std::string&>();
                        if (!irods::is_valid_collection_path(coll_name)) {
                            _ec = make_error_code(SYS_INVALID_FILE_PATH);
                            ctx_.reset();
                            return *this;
                        }

                        value_type ent{};
                        ent.status_.type(object_type::collection);
                        ent.path_.assign(std::string_view{coll_name});
                        ent.owner_.assign(row[1].get_ref<const std::string&>());
                        parse_timestamp_sv(row[2].get_ref<const std::string&>(), ent.ctime_);
                        parse_timestamp_sv(row[3].get_ref<const std::string&>(), ent.mtime_);
                        ctx_->batch.push_back(std::move(ent));
                    }
                }
                else if (ctx_->stage == context::query_stage::data_objects) {
                    const auto q = gq2::builder::select({"COLL_NAME",
                                                         "DATA_NAME",
                                                         "DATA_ID",
                                                         "DATA_MODE",
                                                         "DATA_SIZE",
                                                         "DATA_MODIFY_TIME",
                                                         "DATA_CREATE_TIME"})
                                       .where(col("COLL_NAME") == ctx_->target_coll)
                                       .order_by("DATA_NAME")
                                       .order_by("DATA_ID")
                                       .offset(ctx_->row_offset)
                                       .limit(gq2_page_size)
                                       .build();

                    nlohmann::json rows;
                    const int ec = gq2::try_execute(*ctx_->comm, q, rows, zh);
                    if (ec < 0 && ec != CAT_NO_ROWS_FOUND) {
                        if (ctx_->opts == collection_options::skip_permission_denied &&
                            (ec == CAT_NO_ACCESS_PERMISSION || ec == SYS_NO_API_PRIV))
                        {
                            ctx_.reset();
                            return *this;
                        }
                        _ec = make_error_code(ec);
                        ctx_.reset();
                        return *this;
                    }

                    if (ec == CAT_NO_ROWS_FOUND || rows.empty()) {
                        ctx_->stage = context::query_stage::done;
                        ctx_.reset();
                        return *this;
                    }

                    ctx_->row_offset += rows.size();
                    if (rows.size() < gq2_page_size) {
                        ctx_->stage = context::query_stage::done;
                    }

                    ctx_->batch.reserve(rows.size());
                    for (const auto& row : rows) {
                        const auto& coll_name = row[0].get_ref<const std::string&>();
                        const auto& data_name = row[1].get_ref<const std::string&>();
                        if (!irods::is_valid_collection_path(coll_name) ||
                            !irods::is_valid_child_name(data_name))
                        {
                            _ec = make_error_code(SYS_INVALID_FILE_PATH);
                            ctx_.reset();
                            return *this;
                        }

                        value_type ent{};
                        ent.status_.type(object_type::data_object);
                        ent.path_.assign_concat(ctx_->path.string_view(), data_name);
                        ent.data_id_.assign(row[2].get_ref<const std::string&>());

                        const auto& mode_sv = row[3].get_ref<const std::string&>();
                        int mode_val = 0;
                        std::from_chars(mode_sv.data(), mode_sv.data() + mode_sv.size(), mode_val);
                        ent.data_mode_ = static_cast<unsigned>(mode_val);

                        const auto& size_sv = row[4].get_ref<const std::string&>();
                        long long sz = 0;
                        std::from_chars(size_sv.data(), size_sv.data() + size_sv.size(), sz);
                        ent.data_size_ = (sz >= 0) ? static_cast<std::uintmax_t>(sz) : 0;

                        parse_timestamp_sv(row[5].get_ref<const std::string&>(), ent.mtime_);
                        parse_timestamp_sv(row[6].get_ref<const std::string&>(), ent.ctime_);
                        ctx_->batch.push_back(std::move(ent));
                    }
                }
            }

            ctx_->entry = std::move(ctx_->batch[ctx_->batch_index++]);
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
} // namespace irods::filesystem::NAMESPACE_IMPL
