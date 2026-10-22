#include <catch2/catch_all.hpp>

#include "irods/archive_engine.hpp"
#include "irods/collection_entry_stream.hpp"
#include "irods/collection_types.hpp"
#include "irods/rcMisc.h"
#include "irods/rodsErrorTable.h"

#include <archive.h>
#include <archive_entry.h>
#include <filesystem>
#include <fstream>
#include <variant>

namespace colls = irods::experimental::collections;
namespace fs = irods::filesystem;

TEST_CASE("collection_types parses and serializes R_COLL_MAIN special collection encodings", "[collections][types]")
{
    SECTION("Normal catalog collection")
    {
        colls::collection_info_variant info;
        REQUIRE(colls::parse_special_collection_info("/tempZone/home/rods", "", "", "", info) == 0);
        REQUIRE(std::holds_alternative<colls::catalog_collection_info>(info));
    }

    SECTION("Linked collection (linkPoint)")
    {
        colls::collection_info_variant info;
        REQUIRE(colls::parse_special_collection_info(
                    "/tempZone/home/rods/link", LINK_POINT_STR, "/tempZone/home/rods/target", "", info) == 0);
        REQUIRE(std::holds_alternative<colls::linked_collection_info>(info));
        const auto& link = std::get<colls::linked_collection_info>(info);
        CHECK(link.target_collection_path.string() == "/tempZone/home/rods/target");

        specColl_t legacy{};
        colls::populate_legacy_spec_coll("/tempZone/home/rods/link", info, legacy);
        CHECK(legacy.collClass == LINKED_COLL);
        CHECK(std::string_view{legacy.collection} == "/tempZone/home/rods/link");
        CHECK(std::string_view{legacy.phyPath} == "/tempZone/home/rods/target");

        const auto round_tripped = colls::from_legacy_spec_coll(legacy);
        REQUIRE(std::holds_alternative<colls::linked_collection_info>(round_tripped));
        CHECK(std::get<colls::linked_collection_info>(round_tripped).target_collection_path.string() ==
              "/tempZone/home/rods/target");
    }

    SECTION("Mounted collection (mountPoint)")
    {
        colls::collection_info_variant info;
        REQUIRE(colls::parse_special_collection_info(
                    "/tempZone/home/rods/mnt", MOUNT_POINT_STR, "/var/lib/irods/mnt", "ptResc;unixResc", info) == 0);
        REQUIRE(std::holds_alternative<colls::mounted_collection_info>(info));
        const auto& mnt = std::get<colls::mounted_collection_info>(info);
        CHECK(mnt.physical_directory_path == "/var/lib/irods/mnt");
        CHECK(mnt.resource_hierarchy == "ptResc;unixResc");
        CHECK(mnt.root_resource == "ptResc");

        specColl_t legacy{};
        colls::populate_legacy_spec_coll("/tempZone/home/rods/mnt", info, legacy);
        CHECK(legacy.collClass == MOUNTED_COLL);
        CHECK(std::string_view{legacy.phyPath} == "/var/lib/irods/mnt");
        CHECK(std::string_view{legacy.rescHier} == "ptResc;unixResc");
        CHECK(std::string_view{legacy.resource} == "ptResc");

        const auto round_tripped = colls::from_legacy_spec_coll(legacy);
        REQUIRE(std::holds_alternative<colls::mounted_collection_info>(round_tripped));
        CHECK(std::get<colls::mounted_collection_info>(round_tripped).physical_directory_path == "/var/lib/irods/mnt");
        CHECK(std::get<colls::mounted_collection_info>(round_tripped).resource_hierarchy == "ptResc;unixResc");
    }

    SECTION("Archive collection (tarStructFile) round-trips collInfo2 and resolveSpecCollType")
    {
        colls::collection_info_variant info;
        REQUIRE(colls::parse_special_collection_info(
                    "/tempZone/home/rods/tar_mnt",
                    TAR_STRUCT_FILE_STR,
                    "/tempZone/home/rods/archive.tar",
                    "/vault/archive.tar.cacheDir0;;;ptResc;unixResc;;;1",
                    info) == 0);
        REQUIRE(std::holds_alternative<colls::archive_collection_info>(info));
        const auto& arch = std::get<colls::archive_collection_info>(info);
        CHECK(arch.archive_logical_path.string() == "/tempZone/home/rods/archive.tar");
        CHECK(arch.cache_directory_path == "/vault/archive.tar.cacheDir0");
        CHECK(arch.resource_hierarchy == "ptResc;unixResc");
        CHECK(arch.root_resource == "ptResc");
        CHECK(arch.cache_dirty);
        CHECK(colls::format_cached_archive_info2(arch) == "/vault/archive.tar.cacheDir0;;;ptResc;unixResc;;;1");

        specColl_t legacy{};
        char info1[] = "/tempZone/home/rods/archive.tar";
        char info2[] = "/vault/archive.tar.cacheDir0;;;ptResc;unixResc;;;1";
        char coll[] = "/tempZone/home/rods/tar_mnt";
        char type[] = TAR_STRUCT_FILE_STR;
        REQUIRE(resolveSpecCollType(type, coll, info1, info2, &legacy) == 0);
        CHECK(legacy.collClass == STRUCT_FILE_COLL);
        CHECK(legacy.type == TAR_STRUCT_FILE_T);
        CHECK(std::string_view{legacy.cacheDir} == "/vault/archive.tar.cacheDir0");
        CHECK(std::string_view{legacy.rescHier} == "ptResc;unixResc");
        CHECK(std::string_view{legacy.resource} == "ptResc");
        CHECK(legacy.cacheDirty == 1);
    }

    SECTION("Dead HAAW and MSSO types are rejected")
    {
        colls::collection_info_variant info;
        CHECK(colls::parse_special_collection_info(
                  "/tempZone/home/rods/haaw", HAAW_STRUCT_FILE_STR, "/tempZone/home/rods/f", "", info) ==
              SYS_UNMATCHED_SPEC_COLL_TYPE);
        CHECK(colls::parse_special_collection_info(
                  "/tempZone/home/rods/msso", MSSO_STRUCT_FILE_STR, "/tempZone/home/rods/f", "", info) ==
              SYS_UNMATCHED_SPEC_COLL_TYPE);
    }
}

TEST_CASE("collection_types enforces physical path containment and symlink/hardlink rejection", "[collections][security]")
{
    SECTION("compose_contained_physical_path maps valid subpaths and rejects traversal")
    {
        std::filesystem::path phys;
        REQUIRE(colls::compose_contained_physical_path(
                    "/vault/cacheDir0", "/tempZone/home/rods/mnt", "/tempZone/home/rods/mnt", phys) == 0);
        CHECK(phys == "/vault/cacheDir0");

        REQUIRE(colls::compose_contained_physical_path(
                    "/vault/cacheDir0", "/tempZone/home/rods/mnt", "/tempZone/home/rods/mnt/sub/file.txt", phys) == 0);
        CHECK(phys == "/vault/cacheDir0/sub/file.txt");

        CHECK(colls::compose_contained_physical_path(
                  "/vault/cacheDir0", "/tempZone/home/rods/mnt", "/tempZone/home/rods/mnt_other/file.txt", phys) ==
              SYS_STRUCT_FILE_PATH_ERR);
        CHECK(colls::compose_contained_physical_path(
                  "/vault/cacheDir0", "/tempZone/home/rods/mnt", "/tempZone/home/rods/mnt/../../etc/passwd", phys) ==
              SYS_STRUCT_FILE_PATH_ERR);
    }

    SECTION("project_linked_logical_path maps canonical paths back to link path")
    {
        CHECK(colls::project_linked_logical_path(
                  "/tempZone/home/rods/link", "/tempZone/home/rods/target", "/tempZone/home/rods/target/sub/a") ==
              "/tempZone/home/rods/link/sub/a");
        CHECK(colls::project_linked_logical_path(
                  "/tempZone/home/rods/link", "/tempZone/home/rods/target", "/tempZone/home/rods/target") ==
              "/tempZone/home/rods/link");
    }

    SECTION("validate_cache_path_no_links detects symlinks and hardlinks")
    {
        const auto tmp_base = std::filesystem::temp_directory_path() / "irods_test_coll_types_cache";
        std::filesystem::remove_all(tmp_base);
        std::filesystem::create_directories(tmp_base / "sub");

        const auto reg_file = tmp_base / "sub" / "regular.txt";
        {
            std::ofstream out{reg_file};
            out << "hello";
        }
        CHECK(colls::validate_cache_path_no_links(tmp_base, "sub/regular.txt") == 0);

        const auto sym_file = tmp_base / "sub" / "sym.txt";
        std::filesystem::create_symlink(reg_file, sym_file);
        CHECK(colls::validate_cache_path_no_links(tmp_base, "sub/sym.txt") == SYS_STRUCT_FILE_PATH_ERR);

        const auto hard_file = tmp_base / "sub" / "hard.txt";
        std::filesystem::create_hard_link(reg_file, hard_file);
        CHECK(colls::validate_cache_path_no_links(tmp_base, "sub/hard.txt") == SYS_STRUCT_FILE_PATH_ERR);

        std::filesystem::remove_all(tmp_base);
    }
}

TEST_CASE("collection_resolution_cache and resolve_collection_path_with_lookup handle links, cycles, and invalidation",
          "[collections][resolver]")
{
    colls::collection_resolution_cache cache;

    std::unordered_map<std::string, colls::collection_info_variant> mock_catalog;
    mock_catalog["/tempZone/home/rods/link1"] = colls::linked_collection_info{
        101, fs::path{"/tempZone/home/rods/link2"}};
    mock_catalog["/tempZone/home/rods/link2"] = colls::linked_collection_info{
        102, fs::path{"/tempZone/home/rods/mnt"}};
    mock_catalog["/tempZone/home/rods/mnt"] = colls::mounted_collection_info{
        103, "/var/lib/irods/mounted_dir", "ptResc;unixResc", "ptResc"};
    mock_catalog["/tempZone/home/rods/cycleA"] = colls::linked_collection_info{
        104, fs::path{"/tempZone/home/rods/cycleB"}};
    mock_catalog["/tempZone/home/rods/cycleB"] = colls::linked_collection_info{
        105, fs::path{"/tempZone/home/rods/cycleA"}};

    int catalog_queries = 0;
    auto lookup_fn = [&](std::string_view coll_path,
                         std::string& out_governing_coll,
                         colls::collection_info_variant& out) -> int {
        ++catalog_queries;
        std::string current{coll_path};
        while (!current.empty() && current != "/") {
            if (auto it = mock_catalog.find(current); it != mock_catalog.end()) {
                out_governing_coll = current;
                out = it->second;
                return 0;
            }
            const auto slash = current.rfind('/');
            if (slash == std::string::npos || slash == 0) {
                break;
            }
            current.resize(slash);
        }
        return CAT_NO_ROWS_FOUND;
    };

    SECTION("Multi-hop linked collection into mounted collection resolves cleanly without mutating input")
    {
        const std::string input = "/tempZone/home/rods/link1/sub/file.txt";
        colls::resolved_path resolved;
        REQUIRE(colls::resolve_collection_path_with_lookup(
                    input, colls::link_resolution_mode::follow_links, cache, lookup_fn, resolved) == 0);

        CHECK(resolved.original_path.string() == "/tempZone/home/rods/link1/sub/file.txt");
        CHECK(resolved.canonical_path.string() == "/tempZone/home/rods/mnt/sub/file.txt");
        CHECK(resolved.governing_coll.string() == "/tempZone/home/rods/mnt");
        CHECK(resolved.relative_subpath.string() == "sub/file.txt");
        CHECK(resolved.was_link_translated);
        REQUIRE(std::holds_alternative<colls::mounted_collection_info>(resolved.info));
        CHECK(std::get<colls::mounted_collection_info>(resolved.info).physical_directory_path ==
              "/var/lib/irods/mounted_dir");
    }

    SECTION("do_not_follow_link stops at terminal link point itself")
    {
        colls::resolved_path resolved;
        REQUIRE(colls::resolve_collection_path_with_lookup(
                    "/tempZone/home/rods/link1",
                    colls::link_resolution_mode::do_not_follow_link,
                    cache,
                    lookup_fn,
                    resolved) == 0);

        CHECK(resolved.canonical_path.string() == "/tempZone/home/rods/link1");
        CHECK(resolved.governing_coll.string() == "/tempZone/home/rods/link1");
        CHECK(!resolved.was_link_translated);
        REQUIRE(std::holds_alternative<colls::linked_collection_info>(resolved.info));
    }

    SECTION("Link cycle returns SYS_COLL_LINK_PATH_ERR")
    {
        colls::resolved_path resolved;
        CHECK(colls::resolve_collection_path_with_lookup(
                  "/tempZone/home/rods/cycleA/data.txt",
                  colls::link_resolution_mode::follow_links,
                  cache,
                  lookup_fn,
                  resolved) == SYS_COLL_LINK_PATH_ERR);
    }

    SECTION("Cache stores positive and negative lookups and invalidates prefixes on mutation")
    {
        colls::resolved_path resolved;
        REQUIRE(colls::resolve_collection_path_with_lookup(
                    "/tempZone/home/rods/normal_coll/file.txt",
                    colls::link_resolution_mode::follow_links,
                    cache,
                    lookup_fn,
                    resolved) == 0);
        CHECK(std::holds_alternative<colls::catalog_collection_info>(resolved.info));
        const int queries_after_first = catalog_queries;

        // Second lookup on same negative path hits cache without querying catalog
        REQUIRE(colls::resolve_collection_path_with_lookup(
                    "/tempZone/home/rods/normal_coll/file.txt",
                    colls::link_resolution_mode::follow_links,
                    cache,
                    lookup_fn,
                    resolved) == 0);
        CHECK(catalog_queries == queries_after_first);

        // Now mount /tempZone/home/rods/normal_coll and invalidate cache
        mock_catalog["/tempZone/home/rods/normal_coll"] = colls::mounted_collection_info{
            200, "/var/lib/irods/new_mnt", "rescA", "rescA"};
        cache.invalidate("/tempZone/home/rods/normal_coll");

        REQUIRE(colls::resolve_collection_path_with_lookup(
                    "/tempZone/home/rods/normal_coll/file.txt",
                    colls::link_resolution_mode::follow_links,
                    cache,
                    lookup_fn,
                    resolved) == 0);
        CHECK(catalog_queries > queries_after_first);
        REQUIRE(std::holds_alternative<colls::mounted_collection_info>(resolved.info));
        CHECK(std::get<colls::mounted_collection_info>(resolved.info).physical_directory_path ==
              "/var/lib/irods/new_mnt");
    }
}

namespace
{
    auto write_Synthetic_tar_with_entry(
        const std::filesystem::path& _tar_path,
        const char* _entry_path,
        mode_t _filetype,
        const char* _link_target = nullptr) -> void
    {
        struct archive* a = archive_write_new();
        archive_write_add_filter_none(a);
        archive_write_set_format_pax_restricted(a);
        REQUIRE(archive_write_open_filename(a, _tar_path.c_str()) == ARCHIVE_OK);

        struct archive_entry* entry = archive_entry_new();
        archive_entry_set_pathname(entry, _entry_path);
        archive_entry_set_filetype(entry, _filetype);
        archive_entry_set_perm(entry, 0644);
        if (_filetype == AE_IFLNK && _link_target) {
            archive_entry_set_symlink(entry, _link_target);
            archive_entry_set_size(entry, 0);
        }
        else if (_link_target) {
            archive_entry_set_hardlink(entry, _link_target);
            archive_entry_set_size(entry, 0);
        }
        else {
            archive_entry_set_size(entry, 4);
        }

        REQUIRE(archive_write_header(a, entry) == ARCHIVE_OK);
        if (_filetype == AE_IFREG && !_link_target) {
            archive_write_data(a, "test", 4);
        }

        archive_entry_free(entry);
        archive_write_close(a);
        archive_write_free(a);
    }
} // anonymous namespace

TEST_CASE("archive_engine bundles and extracts safely", "[collections][archive_engine]")
{
    namespace fs = std::filesystem;

    const auto tmp_root = fs::temp_directory_path() / "irods_test_archive_engine";
    std::error_code ec;
    fs::remove_all(tmp_root, ec);
    fs::create_directories(tmp_root);

    SECTION("Bundle directory to tar/gzip/zip and extract round-trip")
    {
        const auto src_dir = tmp_root / "src";
        const auto dst_dir = tmp_root / "dst";
        const auto tar_file = tmp_root / "bundle.tar.gz";

        fs::create_directories(src_dir / "nested" / "deep");
        {
            std::ofstream{src_dir / "root.txt"} << "root payload";
            std::ofstream{src_dir / "nested" / "child.txt"} << "child payload";
            std::ofstream{src_dir / "nested" / "deep" / "leaf.txt"} << "leaf payload";
        }

        REQUIRE(colls::archive_engine::bundle_directory_to_file(src_dir, tar_file, GZIP_TAR_DT_STR) == 0);
        REQUIRE(fs::exists(tar_file));
        CHECK(fs::file_size(tar_file) > 0);

        fs::create_directories(dst_dir);
        REQUIRE(colls::archive_engine::extract_file_to_directory(tar_file, dst_dir) == 0);

        CHECK(fs::is_regular_file(dst_dir / "root.txt"));
        CHECK(fs::is_regular_file(dst_dir / "nested" / "child.txt"));
        CHECK(fs::is_regular_file(dst_dir / "nested" / "deep" / "leaf.txt"));

        std::string content;
        std::getline(std::ifstream{dst_dir / "nested" / "deep" / "leaf.txt"}, content);
        CHECK(content == "leaf payload");
    }

    SECTION("Rejects archive entries with .. traversal, absolute paths, symlinks, and hardlinks")
    {
        const auto dst_dir = tmp_root / "extract_sec";
        fs::create_directories(dst_dir);

        const auto dotdot_tar = tmp_root / "dotdot.tar";
        write_Synthetic_tar_with_entry(dotdot_tar, "../escape.txt", AE_IFREG);
        CHECK(colls::archive_engine::extract_file_to_directory(dotdot_tar, dst_dir) == SYS_STRUCT_FILE_PATH_ERR);

        const auto abs_tar = tmp_root / "abs.tar";
        write_Synthetic_tar_with_entry(abs_tar, "/etc/passwd", AE_IFREG);
        CHECK(colls::archive_engine::extract_file_to_directory(abs_tar, dst_dir) == SYS_STRUCT_FILE_PATH_ERR);

        const auto symlink_tar = tmp_root / "symlink.tar";
        write_Synthetic_tar_with_entry(symlink_tar, "evil_link", AE_IFLNK, "/etc/shadow");
        CHECK(colls::archive_engine::extract_file_to_directory(symlink_tar, dst_dir) == SYS_STRUCT_FILE_PATH_ERR);

        const auto hardlink_tar = tmp_root / "hardlink.tar";
        write_Synthetic_tar_with_entry(hardlink_tar, "evil_hardlink", AE_IFREG, "other_file");
        CHECK(colls::archive_engine::extract_file_to_directory(hardlink_tar, dst_dir) == SYS_STRUCT_FILE_PATH_ERR);
    }

    fs::remove_all(tmp_root, ec);
}

TEST_CASE("collection_entry projects linked paths and populates specColl query batches", "[collections][entry_stream]")
{
    colls::collection_entry file_entry{};
    file_entry.obj_type = DATA_OBJ_T;
    file_entry.coll_name = "/tempZone/home/rods/real_target/sub";
    file_entry.data_name = "report.csv";
    file_entry.data_size = 4096;
    file_entry.create_time = "01700000000";
    file_entry.modify_time = "01700000100";

    const auto projected = colls::project_entry_for_link(
        file_entry,
        "/tempZone/home/rods/link_pt",
        "/tempZone/home/rods/real_target");
    CHECK(projected.coll_name == "/tempZone/home/rods/link_pt/sub");
    CHECK(projected.data_name == "report.csv");

    genQueryOut_t* out = nullptr;
    REQUIRE(colls::init_spec_coll_query_out(&out) == 0);
    REQUIRE(out != nullptr);
    CHECK(out->attriCnt == 5);
    CHECK(out->rowCnt == 0);

    REQUIRE(colls::append_to_spec_coll_query_out(projected, *out) == 0);
    CHECK(out->rowCnt == 1);
    CHECK(std::string{out->sqlResult[0].value} == "/tempZone/home/rods/link_pt/sub");
    CHECK(std::string{out->sqlResult[1].value} == "report.csv");
    CHECK(std::string{out->sqlResult[4].value} == "4096");

    freeGenQueryOut(&out);
}



