#include <catch2/catch_all.hpp>

#include "irods/collection_types.hpp"
#include "irods/rcMisc.h"
#include "irods/rodsErrorTable.h"

#include <filesystem>
#include <fstream>
#include <variant>

namespace colls = irods::experimental::collections;
namespace fs = irods::experimental::filesystem;

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
