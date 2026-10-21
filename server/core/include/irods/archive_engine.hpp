#ifndef IRODS_ARCHIVE_ENGINE_HPP
#define IRODS_ARCHIVE_ENGINE_HPP

/// \file

#include "irods/collection_types.hpp"
#include "irods/irods_error.hpp"
#include "irods/objInfo.h"
#include "irods/rodsConnect.h"

#include <archive.h>
#include <cstddef>
#include <filesystem>
#include <string_view>
#include <sys/types.h>
#include <vector>

namespace irods::experimental::collections::archive_engine
{
    /// Callbacks for streaming archive reads via libarchive.
    struct archive_read_callbacks
    {
        void* user_data = nullptr;
        archive_open_callback* open_cb = nullptr;
        archive_read_callback* read_cb = nullptr;
        archive_close_callback* close_cb = nullptr;
    };

    /// Callbacks for streaming archive writes via libarchive.
    struct archive_write_callbacks
    {
        void* user_data = nullptr;
        archive_open_callback* open_cb = nullptr;
        archive_write_callback* write_cb = nullptr;
        archive_close_callback* close_cb = nullptr;
    };

    /// Returns true if any entry under \p _root_dir (recursively) is a symbolic link.
    auto directory_contains_symlinks(const std::filesystem::path& _root_dir) -> bool;

    /// Recursively collects all regular files under \p _root_dir into \p _out.
    /// Rejects symbolic links with \p SYMLINKED_BUNFILE_NOT_ALLOWED.
    auto collect_regular_files(
        const std::filesystem::path& _root_dir,
        std::vector<std::filesystem::path>& _out) -> int;

    /// Extracts an archive stream into \p _cache_dir while enforcing:
    /// - No empty entry paths
    /// - No symbolic links (`AE_IFLNK`) or hardlinks
    /// - No absolute paths or `..` traversal outside \p _cache_dir
    /// - Post-extraction symlink scan of \p _cache_dir
    auto extract_stream_to_directory(
        const archive_read_callbacks& _cb,
        const std::filesystem::path& _cache_dir) -> int;

    /// Bundles all regular files under \p _cache_dir into an archive stream
    /// using the format/compression specified by \p _data_type (`zip`, `gzipTar`, `bzip2Tar`, or plain pax tar).
    auto bundle_directory_to_stream(
        const std::filesystem::path& _cache_dir,
        std::string_view _data_type,
        const archive_write_callbacks& _cb) -> int;

    /// Convenience wrapper that extracts a local archive file \p _archive_file into \p _cache_dir.
    auto extract_file_to_directory(
        const std::filesystem::path& _archive_file,
        const std::filesystem::path& _cache_dir) -> int;

    /// Convenience wrapper that bundles a local directory \p _cache_dir into \p _archive_file.
    auto bundle_directory_to_file(
        const std::filesystem::path& _cache_dir,
        const std::filesystem::path& _archive_file,
        std::string_view _data_type = {}) -> int;

    /// Extracts the structured archive replica described by \p _spec_coll into `_spec_coll.cacheDir`
    /// over iRODS L3 `rsFileOpen`/`rsFileRead`/`rsFileClose` operations.
    auto extract_to_cache_dir(RsComm& _comm, const specColl_t& _spec_coll) -> irods::error;

    /// Bundles the staging directory `_spec_coll.cacheDir` into the structured archive replica
    /// `_spec_coll.phyPath` over iRODS L3 `rsFileOpen`/`rsFileWrite`/`rsFileClose` operations.
    auto bundle_cache_dir(
        RsComm& _comm,
        const specColl_t& _spec_coll,
        std::string_view _data_type) -> irods::error;

} // namespace irods::experimental::collections::archive_engine

#endif // IRODS_ARCHIVE_ENGINE_HPP
