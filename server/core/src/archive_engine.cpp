#include "irods/archive_engine.hpp"

#include "irods/irods_at_scope_exit.hpp"
#include "irods/rodsDef.h"
#include "irods/rodsErrorTable.h"
#include "irods/rodsLog.h"

#if defined(RODS_SERVER) || defined(RODS_CLERVER)
#  include "irods/fileClose.h"
#  include "irods/fileOpen.h"
#  include "irods/fileRead.h"
#  include "irods/fileWrite.h"
#  include "irods/irods_resource_backport.hpp"
#  include "irods/miscServerFunct.hpp"
#  include "irods/physPath.hpp"
#  include "irods/rsFileClose.hpp"
#  include "irods/rsFileOpen.hpp"
#  include "irods/rsFileRead.hpp"
#  include "irods/rsFileWrite.hpp"
#endif

#include <archive.h>
#include <archive_entry.h>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <fmt/format.h>
#include <string>
#include <sys/stat.h>
#include <unistd.h>

namespace irods::experimental::collections::archive_engine
{
    namespace
    {
        constexpr std::size_t archive_io_block_size = 1 * 1024 * 1024;

        struct local_file_ctx
        {
            std::string path;
            int fd = -1;
            int flags = O_RDONLY;
            void* buf = nullptr;
        };

        auto local_open_cb(struct archive* _arch, void* _data) -> int
        {
            if (!_arch || !_data) {
                return ARCHIVE_FATAL;
            }
            auto* ctx = static_cast<local_file_ctx*>(_data);
            ctx->fd = ::open(ctx->path.c_str(), ctx->flags, 0600);
            if (ctx->fd < 0) {
                archive_set_error(_arch, errno, "Failed to open '%s'", ctx->path.c_str());
                return ARCHIVE_FATAL;
            }
            return ARCHIVE_OK;
        }

        auto local_read_cb(struct archive* _arch, void* _data, const void** _block) -> la_ssize_t
        {
            if (!_arch || !_data || !_block) {
                return ARCHIVE_FATAL;
            }
            auto* ctx = static_cast<local_file_ctx*>(_data);
            if (!ctx->buf) {
                ctx->buf = std::malloc(archive_io_block_size);
                if (!ctx->buf) {
                    return ARCHIVE_FATAL;
                }
            }
            const auto n = ::read(ctx->fd, ctx->buf, archive_io_block_size);
            if (n < 0) {
                archive_set_error(_arch, errno, "Failed to read '%s'", ctx->path.c_str());
                return ARCHIVE_FATAL;
            }
            *_block = ctx->buf;
            return n;
        }

        auto local_write_cb(struct archive* _arch, void* _data, const void* _buff, std::size_t _len) -> la_ssize_t
        {
            if (!_arch || !_data || !_buff) {
                return ARCHIVE_FATAL;
            }
            auto* ctx = static_cast<local_file_ctx*>(_data);
            const auto n = ::write(ctx->fd, _buff, _len);
            if (n < 0) {
                archive_set_error(_arch, errno, "Failed to write '%s'", ctx->path.c_str());
                return ARCHIVE_FATAL;
            }
            return n;
        }

        auto local_close_cb(struct archive* _arch, void* _data) -> int
        {
            if (!_arch || !_data) {
                return ARCHIVE_FATAL;
            }
            auto* ctx = static_cast<local_file_ctx*>(_data);
            if (ctx->buf) {
                std::free(ctx->buf);
                ctx->buf = nullptr;
            }
            if (ctx->fd >= 0) {
                const int rc = ::close(ctx->fd);
                ctx->fd = -1;
                if (rc < 0) {
                    return ARCHIVE_FATAL;
                }
            }
            return ARCHIVE_OK;
        }

        auto write_regular_file_to_archive(
            const std::filesystem::path& _file_path,
            const std::filesystem::path& _cache_dir,
            struct archive* _archive) -> int
        {
            std::error_code ec;
            const auto rel_path = std::filesystem::relative(_file_path, _cache_dir, ec);
            if (ec || rel_path.empty()) {
                return SYS_STRUCT_FILE_PATH_ERR;
            }

            const int fd = ::open(_file_path.c_str(), O_RDONLY);
            if (fd < 0) {
                return UNIX_FILE_OPEN_ERR - errno;
            }
            const irods::at_scope_exit close_fd{[fd] { ::close(fd); }};

            struct stat st{};
            if (::fstat(fd, &st) < 0) {
                return UNIX_FILE_STAT_ERR - errno;
            }

            struct archive_entry* entry = archive_entry_new();
            if (!entry) {
                return SYS_MALLOC_ERR;
            }
            const irods::at_scope_exit free_entry{[entry] { archive_entry_free(entry); }};

            archive_entry_set_pathname(entry, rel_path.generic_string().c_str());
            archive_entry_set_size(entry, st.st_size);
            archive_entry_set_filetype(entry, AE_IFREG);
            archive_entry_set_perm(entry, 0600);
            archive_entry_set_mtime(entry, st.st_mtime, 0);

            if (archive_write_header(_archive, entry) != ARCHIVE_OK) {
                rodsLog(LOG_ERROR,
                        "write_regular_file_to_archive: archive_write_header failed for [%s]: %s",
                        _file_path.c_str(),
                        archive_error_string(_archive));
                return SYS_TAR_STRUCT_FILE_EXTRACT_ERR;
            }

            char buff[65536];
            ssize_t len = ::read(fd, buff, sizeof(buff));
            while (len > 0) {
                if (archive_write_data(_archive, buff, static_cast<std::size_t>(len)) < 0) {
                    return SYS_TAR_STRUCT_FILE_EXTRACT_ERR;
                }
                len = ::read(fd, buff, sizeof(buff));
            }
            if (len < 0) {
                return UNIX_FILE_READ_ERR - errno;
            }

            return 0;
        }

#if defined(RODS_SERVER) || defined(RODS_CLERVER)
        struct irods_l3_cb_ctx
        {
            RsComm* comm = nullptr;
            const specColl_t* spec_coll = nullptr;
            std::string location;
            int file_inx = -1;
            int open_flags = O_RDONLY;
            bytesBuf_t read_buf{};
        };

        auto irods_l3_open_cb(struct archive* _arch, void* _data) -> int
        {
            if (!_arch || !_data) {
                return ARCHIVE_FATAL;
            }
            auto* ctx = static_cast<irods_l3_cb_ctx*>(_data);

            fileOpenInp_t f_inp{};
            std::snprintf(f_inp.resc_name_, sizeof(f_inp.resc_name_), "%s", ctx->spec_coll->resource);
            std::snprintf(f_inp.resc_hier_, sizeof(f_inp.resc_hier_), "%s", ctx->spec_coll->rescHier);
            std::snprintf(f_inp.objPath, sizeof(f_inp.objPath), "%s", ctx->spec_coll->objPath);
            std::snprintf(f_inp.addr.hostAddr, sizeof(f_inp.addr.hostAddr), "%s", ctx->location.c_str());
            std::snprintf(f_inp.fileName, sizeof(f_inp.fileName), "%s", ctx->spec_coll->phyPath);
            f_inp.mode = getDefFileMode();
            f_inp.flags = ctx->open_flags;

            ctx->file_inx = rsFileOpen(ctx->comm, &f_inp);
            if (ctx->file_inx < 0) {
                return ARCHIVE_FATAL;
            }
            return ARCHIVE_OK;
        }

        auto irods_l3_read_cb(struct archive* _arch, void* _data, const void** _block) -> la_ssize_t
        {
            if (!_arch || !_data || !_block) {
                return ARCHIVE_FATAL;
            }
            auto* ctx = static_cast<irods_l3_cb_ctx*>(_data);

            if (ctx->read_buf.buf) {
                std::free(ctx->read_buf.buf);
                ctx->read_buf.buf = nullptr;
            }
            ctx->read_buf.buf = std::malloc(archive_io_block_size);
            ctx->read_buf.len = static_cast<int>(archive_io_block_size);

            fileReadInp_t r_inp{};
            r_inp.fileInx = ctx->file_inx;
            r_inp.len = static_cast<int>(archive_io_block_size);
            const int status = rsFileRead(ctx->comm, &r_inp, &ctx->read_buf);
            if (status < 0) {
                return -1;
            }
            *_block = ctx->read_buf.buf;
            return status;
        }

        auto irods_l3_write_cb(struct archive* _arch, void* _data, const void* _buff, std::size_t _len) -> la_ssize_t
        {
            if (!_arch || !_data || !_buff) {
                return ARCHIVE_FATAL;
            }
            auto* ctx = static_cast<irods_l3_cb_ctx*>(_data);

            fileWriteInp_t w_inp{};
            w_inp.fileInx = ctx->file_inx;
            w_inp.len = static_cast<int>(_len);

            bytesBuf_t w_buf{};
            w_buf.buf = const_cast<void*>(_buff);
            w_buf.len = static_cast<int>(_len);

            const int sz = rsFileWrite(ctx->comm, &w_inp, &w_buf);
            return sz < 0 ? -1 : sz;
        }

        auto irods_l3_close_cb(struct archive* _arch, void* _data) -> int
        {
            if (!_arch || !_data) {
                return ARCHIVE_FATAL;
            }
            auto* ctx = static_cast<irods_l3_cb_ctx*>(_data);

            if (ctx->read_buf.buf) {
                std::free(ctx->read_buf.buf);
                ctx->read_buf.buf = nullptr;
            }

            if (ctx->file_inx >= 0) {
                fileCloseInp_t c_inp{};
                c_inp.fileInx = ctx->file_inx;
                const int rc = rsFileClose(ctx->comm, &c_inp);
                ctx->file_inx = -1;
                return rc;
            }
            return ARCHIVE_OK;
        }
#endif // defined(RODS_SERVER) || defined(RODS_CLERVER)
    } // anonymous namespace

    auto directory_contains_symlinks(const std::filesystem::path& _root_dir) -> bool
    {
        namespace fs = std::filesystem;

        std::error_code ec;
        const auto root_st = fs::symlink_status(_root_dir, ec);
        if (ec) {
            return false;
        }
        if (fs::is_symlink(root_st)) {
            return true;
        }
        if (!fs::is_directory(root_st)) {
            return false;
        }

        for (fs::directory_iterator itr(_root_dir, ec), end; itr != end; itr.increment(ec)) {
            if (ec) {
                break;
            }
            const auto st = fs::symlink_status(itr->path(), ec);
            if (!ec && fs::is_symlink(st)) {
                return true;
            }
            if (!ec && fs::is_directory(st) && directory_contains_symlinks(itr->path())) {
                return true;
            }
        }

        return false;
    }

    auto collect_regular_files(
        const std::filesystem::path& _root_dir,
        std::vector<std::filesystem::path>& _out) -> int
    {
        namespace fs = std::filesystem;

        std::error_code ec;
        if (!fs::exists(_root_dir, ec) || !fs::is_directory(_root_dir, ec)) {
            return UNIX_FILE_OPENDIR_ERR - ec.value();
        }

        for (fs::directory_iterator itr(_root_dir, ec), end; itr != end; itr.increment(ec)) {
            if (ec) {
                return UNIX_FILE_READDIR_ERR - ec.value();
            }

            const auto& entry_path = itr->path();
            const auto st = fs::symlink_status(entry_path, ec);
            if (ec) {
                return UNIX_FILE_STAT_ERR - ec.value();
            }

            if (fs::is_symlink(st)) {
                return SYMLINKED_BUNFILE_NOT_ALLOWED;
            }

            if (fs::is_directory(st)) {
                if (const int rc = collect_regular_files(entry_path, _out); rc < 0) {
                    return rc;
                }
            }
            else if (fs::is_regular_file(st)) {
                _out.push_back(entry_path);
            }
        }

        return 0;
    }

    auto extract_stream_to_directory(
        const archive_read_callbacks& _cb,
        const std::filesystem::path& _cache_dir) -> int
    {
        namespace fs = std::filesystem;

        if (_cache_dir.empty()) {
            return SYS_STRUCT_FILE_DESC_ERR;
        }

        constexpr int extract_flags =
            ARCHIVE_EXTRACT_TIME | ARCHIVE_EXTRACT_SECURE_NODOTDOT | ARCHIVE_EXTRACT_SECURE_SYMLINKS;

        struct archive* arch = archive_read_new();
        if (!arch) {
            return SYS_LIBRARY_ERROR;
        }
        const irods::at_scope_exit free_arch{[arch] { archive_read_free(arch); }};

        if (archive_read_support_filter_all(arch) != ARCHIVE_OK) {
            return SYS_LIBRARY_ERROR;
        }
        if (archive_read_support_format_all(arch) != ARCHIVE_OK) {
            return SYS_LIBRARY_ERROR;
        }

        if (archive_read_open(arch, _cb.user_data, _cb.open_cb, _cb.read_cb, _cb.close_cb) != ARCHIVE_OK) {
            return SYS_TAR_STRUCT_FILE_EXTRACT_ERR;
        }

        struct archive_entry* entry = nullptr;
        while (true) {
            const int ec = archive_read_next_header(arch, &entry);
            if (ec == ARCHIVE_EOF) {
                break;
            }
            if (ec != ARCHIVE_OK && ec != ARCHIVE_WARN) {
                return SYS_TAR_STRUCT_FILE_EXTRACT_ERR;
            }

            const char* entry_pathname = archive_entry_pathname(entry);
            if (!entry_pathname || *entry_pathname == '\0') {
                return SYS_STRUCT_FILE_PATH_ERR;
            }

            if (archive_entry_filetype(entry) == AE_IFLNK) {
                return SYS_STRUCT_FILE_PATH_ERR;
            }

            if (const char* hardlink_path = archive_entry_hardlink(entry);
                hardlink_path && *hardlink_path != '\0')
            {
                return SYS_STRUCT_FILE_PATH_ERR;
            }

            const auto relative_entry_path = fs::path{entry_pathname}.lexically_normal();
            if (relative_entry_path.is_absolute() || relative_entry_path.has_root_name()) {
                return SYS_STRUCT_FILE_PATH_ERR;
            }

            if (!relative_entry_path.empty() && *relative_entry_path.begin() == "..") {
                return SYS_STRUCT_FILE_PATH_ERR;
            }

            const auto target_path = _cache_dir / relative_entry_path;
            archive_entry_set_pathname(entry, target_path.c_str());

            const int ext_rc = archive_read_extract(arch, entry, extract_flags);
            if (ext_rc != ARCHIVE_OK && ext_rc != ARCHIVE_WARN) {
                rodsLog(LOG_ERROR,
                        "extract_stream_to_directory: archive_read_extract error [%d: %s] for [%s]",
                        ext_rc,
                        archive_error_string(arch),
                        target_path.c_str());
                return SYS_TAR_STRUCT_FILE_EXTRACT_ERR;
            }
        }

        return 0;
    }

    auto bundle_directory_to_stream(
        const std::filesystem::path& _cache_dir,
        std::string_view _data_type,
        const archive_write_callbacks& _cb) -> int
    {
        namespace fs = std::filesystem;

        std::error_code ec;
        const auto full_path = fs::absolute(_cache_dir, ec);
        if (ec || !fs::exists(full_path, ec) || !fs::is_directory(full_path, ec)) {
            return UNIX_FILE_OPENDIR_ERR;
        }

        std::vector<fs::path> listing;
        if (const int rc = collect_regular_files(full_path, listing); rc < 0) {
            return rc;
        }

        struct archive* arch = archive_write_new();
        if (!arch) {
            return SYS_LIBRARY_ERROR;
        }
        const irods::at_scope_exit free_arch{[arch] { archive_write_free(arch); }};

        if (_data_type == ZIP_DT_STR) {
            if (archive_write_set_format_zip(arch) != ARCHIVE_OK) {
                return SYS_LIBRARY_ERROR;
            }
        }
        else if (_data_type == GZIP_TAR_DT_STR) {
            if (archive_write_add_filter_gzip(arch) != ARCHIVE_OK) {
                return SYS_LIBRARY_ERROR;
            }
            archive_write_set_format_pax(arch);
        }
        else if (_data_type == BZIP2_TAR_DT_STR) {
            if (archive_write_add_filter_bzip2(arch) != ARCHIVE_OK) {
                return SYS_LIBRARY_ERROR;
            }
            archive_write_set_format_pax(arch);
        }
        else {
            if (archive_write_add_filter_none(arch) != ARCHIVE_OK) {
                return SYS_LIBRARY_ERROR;
            }
            archive_write_set_format_pax(arch);
        }

        if (archive_write_open(arch, _cb.user_data, _cb.open_cb, _cb.write_cb, _cb.close_cb) < ARCHIVE_OK) {
            return SYS_TAR_STRUCT_FILE_EXTRACT_ERR;
        }

        int final_rc = 0;
        for (const auto& file_path : listing) {
            if (const int rc = write_regular_file_to_archive(file_path, full_path, arch); rc < 0 && final_rc == 0) {
                final_rc = rc;
            }
        }

        archive_write_close(arch);
        return final_rc;
    }

    auto extract_file_to_directory(
        const std::filesystem::path& _archive_file,
        const std::filesystem::path& _cache_dir) -> int
    {
        local_file_ctx ctx{};
        ctx.path = _archive_file.string();
        ctx.flags = O_RDONLY;
        const irods::at_scope_exit cleanup{[&ctx] {
            if (ctx.buf) {
                std::free(ctx.buf);
            }
            if (ctx.fd >= 0) {
                ::close(ctx.fd);
            }
        }};

        const archive_read_callbacks cb{
            .user_data = &ctx,
            .open_cb = local_open_cb,
            .read_cb = local_read_cb,
            .close_cb = local_close_cb};

        return extract_stream_to_directory(cb, _cache_dir);
    }

    auto bundle_directory_to_file(
        const std::filesystem::path& _cache_dir,
        const std::filesystem::path& _archive_file,
        std::string_view _data_type) -> int
    {
        local_file_ctx ctx{};
        ctx.path = _archive_file.string();
        ctx.flags = O_WRONLY | O_CREAT | O_TRUNC;
        const irods::at_scope_exit cleanup{[&ctx] {
            if (ctx.fd >= 0) {
                ::close(ctx.fd);
            }
        }};

        const archive_write_callbacks cb{
            .user_data = &ctx,
            .open_cb = local_open_cb,
            .write_cb = local_write_cb,
            .close_cb = local_close_cb};

        return bundle_directory_to_stream(_cache_dir, _data_type, cb);
    }

#if defined(RODS_SERVER) || defined(RODS_CLERVER)
    auto extract_to_cache_dir(RsComm& _comm, const specColl_t& _spec_coll) -> irods::error
    {
        if (std::strlen(_spec_coll.cacheDir) == 0 || std::strlen(_spec_coll.phyPath) == 0) {
            return ERROR(SYS_STRUCT_FILE_DESC_ERR, "extract_to_cache_dir - empty cacheDir or phyPath");
        }

        std::string location;
        if (const auto ret = irods::get_loc_for_hier_string(_spec_coll.rescHier, location); !ret.ok()) {
            return PASSMSG("extract_to_cache_dir - failed in get_loc_for_hier_string", ret);
        }

        irods_l3_cb_ctx ctx{};
        ctx.comm = &_comm;
        ctx.spec_coll = &_spec_coll;
        ctx.location = std::move(location);
        ctx.open_flags = O_RDONLY;
        const irods::at_scope_exit cleanup{[&ctx] {
            if (ctx.read_buf.buf) {
                std::free(ctx.read_buf.buf);
            }
        }};

        const archive_read_callbacks cb{
            .user_data = &ctx,
            .open_cb = irods_l3_open_cb,
            .read_cb = irods_l3_read_cb,
            .close_cb = irods_l3_close_cb};

        if (const int rc = extract_stream_to_directory(cb, _spec_coll.cacheDir); rc < 0) {
            return ERROR(rc, fmt::format("extract_to_cache_dir failed for [{}]", _spec_coll.phyPath));
        }

        return SUCCESS();
    }

    auto bundle_cache_dir(
        RsComm& _comm,
        const specColl_t& _spec_coll,
        std::string_view _data_type) -> irods::error
    {
        if (_spec_coll.cacheDirty <= 0 ||
            std::strlen(_spec_coll.cacheDir) == 0 ||
            std::strlen(_spec_coll.phyPath) == 0)
        {
            return ERROR(SYS_STRUCT_FILE_DESC_ERR, "bundle_cache_dir - invalid specColl state");
        }

        std::string location;
        if (const auto ret = irods::get_loc_for_hier_string(_spec_coll.rescHier, location); !ret.ok()) {
            return PASSMSG("bundle_cache_dir - failed in get_loc_for_hier_string", ret);
        }

        irods_l3_cb_ctx ctx{};
        ctx.comm = &_comm;
        ctx.spec_coll = &_spec_coll;
        ctx.location = std::move(location);
        ctx.open_flags = O_WRONLY | O_CREAT;

        const archive_write_callbacks cb{
            .user_data = &ctx,
            .open_cb = irods_l3_open_cb,
            .write_cb = irods_l3_write_cb,
            .close_cb = irods_l3_close_cb};

        if (const int rc = bundle_directory_to_stream(_spec_coll.cacheDir, _data_type, cb); rc < 0) {
            return ERROR(rc, fmt::format("bundle_cache_dir failed for [{}]", _spec_coll.phyPath));
        }

        return SUCCESS();
    }
#endif // defined(RODS_SERVER) || defined(RODS_CLERVER)

} // namespace irods::experimental::collections::archive_engine
