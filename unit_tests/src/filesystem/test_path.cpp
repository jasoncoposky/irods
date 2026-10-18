//
// The following unit tests were implemented based on code examples from
// "cppreference.com". This code is licensed under the following:
//
//   - Creative Commons Attribution-Sharealike 3.0 Unported License (CC-BY-SA)
//   - GNU Free Documentation License (GFDL)
//
// For more information about these licenses, visit:
//   
//   - https://en.cppreference.com/w/Cppreference:FAQ
//   - https://en.cppreference.com/w/Cppreference:Copyright/CC-BY-SA
//   - https://en.cppreference.com/w/Cppreference:Copyright/GDFL
//

#include <catch2/catch_all.hpp>

#include "irods/filesystem/path.hpp"
#include "irods/filesystem/path_utilities.hpp"
#include "irods/filesystem/filesystem_error.hpp"

#include <string>
#include <string_view>
#include <vector>
#include <utility>
#include <algorithm>
#include <cstring>

#ifdef __cpp_lib_filesystem
    #include <filesystem>
#endif // __cpp_lib_filesystem

namespace fs = irods::experimental::filesystem;

TEST_CASE("path construction", "[constructors]")
{
    const std::string path_string = "/a/b/c";

    SECTION("default construct")
    {
        fs::path p;
        REQUIRE(p.empty());
    }

    SECTION("construct using string literal")
    {
        REQUIRE(path_string == fs::path{path_string.c_str()});
    }

    SECTION("construct using std::string")
    {
        REQUIRE(path_string == fs::path{path_string});
    }

    SECTION("construct using standard container (std::vector)")
    {
        const std::vector<char> the_path(std::begin(path_string), std::end(path_string));
        REQUIRE(path_string == fs::path{the_path});
    }

    SECTION("construct using iterators")
    {
        REQUIRE(path_string == fs::path{std::begin(path_string), std::end(path_string)});
    }

    SECTION("copy construct")
    {
        const fs::path src = path_string;
        REQUIRE(path_string == fs::path{src});
    }

    SECTION("move construct")
    {
        fs::path src = path_string;
        REQUIRE(path_string == src);

        const auto dst = std::move(src);
        REQUIRE(src.empty());
        REQUIRE(path_string == dst);
    }
}

TEST_CASE("path assignment", "[assignment]")
{
    const std::string path_string = "/a/b/c";

    fs::path path = path_string;
    REQUIRE(path_string == path);

    SECTION("copy assign using path object")
    {
        const fs::path other = "/a/b/c/d";
        path = other;
        REQUIRE(other == path);
    }

    SECTION("move assign using path object")
    {
        const char* other_path_string = "/a/b/c/d";
        fs::path src = other_path_string;
        path = std::move(src);
        REQUIRE(src.empty());
        REQUIRE(other_path_string == path);
    }

    SECTION("copy assign using const character buffer")
    {
        fs::path path;
        path = path_string.c_str();
        REQUIRE(path_string == path);
    }

    SECTION("copy assign using character buffer")
    {
        path = const_cast<char*>(path_string.c_str());
        REQUIRE(path_string == path);
    }

    SECTION("move assign using std::string")
    {
        fs::path src = path_string;
        path = std::move(src);
        REQUIRE(src.empty());
        REQUIRE(path_string == path);
    }

    SECTION("copy assign using standard container")
    {
        path = std::vector<char>(std::begin(path_string), std::end(path_string));
        REQUIRE(path_string == path);
    }

    SECTION("assign using std::string")
    {
        path.assign(path_string);
        REQUIRE(path_string == path);
    }

    SECTION("assign using standard container")
    {
        path.assign(std::vector<char>(std::begin(path_string), std::end(path_string)));
        REQUIRE(path_string == path);
    }

    SECTION("assign using iterators")
    {
        const std::vector<char> src(std::begin(path_string), std::end(path_string));
        path.assign(std::begin(src), std::end(src));
        REQUIRE(path_string == path);
    }
}

TEST_CASE("path appends", "[append]")
{
    const std::string path_string = "b/c";
    fs::path path = "/a";

    SECTION("append using operator/=()")
    {
        path /= "b";
        path /= "c";
        REQUIRE("/a/b/c" == path);
    }

    SECTION("append to empty path using operator/=()")
    {
        path.clear();
        path /= "a";
        path /= "b";
        path /= "c";
        REQUIRE("a/b/c" == path);
    }

    SECTION("append standard container using operator/=()")
    {
        const std::vector<char> sub_path(std::begin(path_string), std::end(path_string));
        path /= sub_path;
        REQUIRE("/a/b/c" == path);
    }

    SECTION("append iterators using operator/=()")
    {
        path.append(std::begin(path_string), std::end(path_string));
        REQUIRE("/a/b/c" == path);
    }

    SECTION("append using free function operator/()")
    {
        const auto new_path = path / "b" / "c";
        REQUIRE("/a" == path);
        REQUIRE("/a/b/c" == new_path);
    }
}

TEST_CASE("path concatenation", "[concat][concatenation]")
{
    const char* target = "/a/b/c/d_x";
    fs::path path = "/a/b/c/d";

    SECTION("concat a path")
    {
        REQUIRE(target == (path += fs::path{"_x"}));
    }

    SECTION("concat a std::string")
    {
        REQUIRE(target == (path += "_x"));
    }

    SECTION("concat a std::string_view")
    {
        const std::string value = "_x";
        REQUIRE(target == (path += std::string_view{"_x"}));
    }

    SECTION("concat a const character buffer")
    {
        const char* value = "_x";
        REQUIRE(target == (path += value));
    }

    SECTION("concat a character buffer")
    {
        char value[] = "_x";
        REQUIRE(target == (path += value));
    }

    SECTION("concat a single character")
    {
        path += '_';
        path += 'x';
        REQUIRE(target == path);
    }

    SECTION("concat a standard container")
    {
        const std::vector<char> value{'_', 'x'};
        REQUIRE(target == (path += value));
    }

    SECTION("concat an iterator range")
    {
        const std::string value = "_x";
        REQUIRE(target == path.concat(std::begin(value), std::end(value)));
    }
}

TEST_CASE("path modifiers", "[modifiers]")
{
    fs::path path = "/a/b/c/d.txt";

    SECTION("clear the path")
    {
        path.clear();
        REQUIRE(path.empty());
    }

    SECTION("remove the object name")
    {
        REQUIRE("foo/" == fs::path{"foo/bar"}.remove_object_name());
        REQUIRE("foo/" == fs::path{"foo/"}.remove_object_name());
        REQUIRE("/" == fs::path{"/foo"}.remove_object_name());
        REQUIRE("/" == fs::path{"/"}.remove_object_name());

        path.remove_object_name();
        REQUIRE("/a/b/c/" == path);
    }

    SECTION("replace object name")
    {
        REQUIRE("/bar" == fs::path{"/foo"}.replace_object_name("bar"));
        REQUIRE("/bar" == fs::path{"/"}.replace_object_name("bar"));
        REQUIRE("/a/b/c/e.txt" == path.replace_object_name("e.txt"));
    }

    SECTION("replace the extension")
    {
        path.replace_extension("doc");
        REQUIRE("/a/b/c/d.doc" == path);

        path.replace_extension();
        REQUIRE("/a/b/c/d" == path);

        path.replace_extension(".TXT");
        REQUIRE("/a/b/c/d.TXT" == path);
    }

    SECTION("swap paths")
    {
        fs::path other_path = "/x/y/z";
        fs::swap(path, other_path);
        REQUIRE("/x/y/z" == path);
        REQUIRE("/a/b/c/d.txt" == other_path);
    }
}

TEST_CASE("path lexical operations", "[lexical operations]")
{
    SECTION("normal form of path")
    {
#ifdef __cpp_lib_filesystem
        const std::vector<std::string> paths{
            "/tempZone/home/rods/.",
            "/tempZone/home/rods/..",
            "/tempZone/home/rods/foo",
            "/tempZone/home/rods/foo.",
            "/tempZone/home/rods/foo^",
            "/tempZone/home/rods/foo~",
            "/tempZone/home/rods/foo/",
            "/tempZone/home/rods/foo/.",
            "/tempZone/home/rods/foo/..",
            "tempZone/home/rods/.",
            "tempZone/home/rods/..",
            "tempZone/home/rods/foo",
            "tempZone/home/rods/foo.",
            "tempZone/home/rods/foo^",
            "tempZone/home/rods/foo~",
            "tempZone/home/rods/foo/",
            "tempZone/home/rods/foo/.",
            "tempZone/home/rods/foo/..",
            "/foo",
            "/foo/",
            "foo/",
            "/",
            "/.",
            "/..",
            ".",
            "./",
            "./.",
            "././.",
            "..",
            "../",
            "../..",
            "../../..",
            "../file/../..",
            "/../file/../..",
            ""
        };

        for (auto&& p : paths) {
            DYNAMIC_SECTION("normal form of path [" << p << "]")
            {
                namespace std_fs = std::filesystem;
                REQUIRE(std_fs::path{p}.lexically_normal().string() == fs::path{p}.lexically_normal().string());
            }
        }
#else
        // This table holds the expected output of the C++17 Std. Filesystem's
        // path::lexically_normal() function. The first value represents the string
        // that lexically normal will be called on. The second value is the expected output.
        const std::vector<std::pair<std::string, std::string>> paths{
            {"/tempZone/home/rods/.", "/tempZone/home/rods/"},
            {"/tempZone/home/rods/..", "/tempZone/home/"},
            {"/tempZone/home/rods/foo", "/tempZone/home/rods/foo"},
            {"/tempZone/home/rods/foo.", "/tempZone/home/rods/foo."},
            {"/tempZone/home/rods/foo^", "/tempZone/home/rods/foo^"},
            {"/tempZone/home/rods/foo~", "/tempZone/home/rods/foo~"},
            {"/tempZone/home/rods/foo/", "/tempZone/home/rods/foo/"},
            {"/tempZone/home/rods/foo/.", "/tempZone/home/rods/foo/"},
            {"/tempZone/home/rods/foo/..", "/tempZone/home/rods/"},
            {"tempZone/home/rods/.", "tempZone/home/rods/"},
            {"tempZone/home/rods/..", "tempZone/home/"},
            {"tempZone/home/rods/foo", "tempZone/home/rods/foo"},
            {"tempZone/home/rods/foo.", "tempZone/home/rods/foo."},
            {"tempZone/home/rods/foo^", "tempZone/home/rods/foo^"},
            {"tempZone/home/rods/foo~", "tempZone/home/rods/foo~"},
            {"tempZone/home/rods/foo/", "tempZone/home/rods/foo/"},
            {"tempZone/home/rods/foo/.", "tempZone/home/rods/foo/"},
            {"tempZone/home/rods/foo/..", "tempZone/home/rods/"},
            {"/foo", "/foo"},
            {"/foo/", "/foo/"},
            {"foo/", "foo/"},
            {"/", "/"},
            {"/.", "/"},
            {"/..", "/"},
            {".", "."},
            {"./", "."},
            {"./.", "."},
            {"././.", "."},
            {"..", ".."},
            {"../", ".."},
            {"../..", "../.."},
            {"../../..", "../../.."},
            {"../file/../..", "../.."},
            {"/../file/../..", "/"},
            {"", ""}
        };

        for (auto&& p : paths) {
            DYNAMIC_SECTION("normal form of path [" << p.first << "]")
            {
                REQUIRE(p.second == fs::path{p.first}.lexically_normal());
            }
        }
#endif // __cpp_lib_filesystem

        REQUIRE("foo/" == fs::path{"foo/./bar/.."}.lexically_normal());
        REQUIRE("foo/" == fs::path{"foo/.///bar/../"}.lexically_normal());
    }

    SECTION("relative form of path")
    {
        REQUIRE("../../d" == fs::path{"/a/d"}.lexically_relative("/a/b/c"));
        REQUIRE("../b/c" == fs::path{"/a/b/c"}.lexically_relative("/a/d"));
        REQUIRE("b/c" == fs::path{"a/b/c"}.lexically_relative("a"));
        REQUIRE("../.." == fs::path{"a/b/c"}.lexically_relative("a/b/c/x/y"));
        REQUIRE("." == fs::path{"a/b/c"}.lexically_relative("a/b/c"));
        REQUIRE("../../a/b" == fs::path{"a/b"}.lexically_relative("c/d"));
    }
}

TEST_CASE("path format observers", "[format observers]")
{
    const std::string p = "/a/b";

    SECTION("get path as const character string")
    {
        REQUIRE(std::strncmp(p.c_str(), fs::path{p}.c_str(), p.size()) == 0);
    }

    SECTION("get path as std::string")
    {
        REQUIRE(p == fs::path{p}.string());
    }

    SECTION("implicit cast to std::string")
    {
        std::string s = fs::path{p};
        REQUIRE(p == s);
    }
}

TEST_CASE("path comparison", "[comparison]")
{
    REQUIRE(fs::path{"/a/b"}.compare("/a/b") == 0);
    REQUIRE(fs::path{"/a"}.compare("/a/b") < 0);
    REQUIRE(fs::path{"/a/b"}.compare("/a") > 0);
}

TEST_CASE("path decomposition", "[decomposition]")
{
    const fs::path p = "/a/b/c/d.txt";

    REQUIRE(fs::path{"/"}.parent_path() == "/");
    REQUIRE(p.root_collection() == "/");
    REQUIRE(p.relative_path() == "a/b/c/d.txt");
    REQUIRE(p.parent_path() == "/a/b/c");
    REQUIRE(p.object_name() == "d.txt");
    REQUIRE(p.stem() == "d");
    REQUIRE(p.extension() == ".txt");
}

TEST_CASE("path query", "[query]")
{
    const fs::path p = "/a/b/c/d.txt";

    REQUIRE(fs::path{}.empty());
    REQUIRE_FALSE(p.empty());

    REQUIRE_FALSE(fs::path{}.has_root_collection());
    REQUIRE(p.has_root_collection());

    REQUIRE_FALSE(fs::path{}.has_relative_path());
    REQUIRE(p.relative_path().has_relative_path());

    REQUIRE_FALSE(fs::path{}.has_parent_path());
    REQUIRE(p.has_parent_path());

    REQUIRE_FALSE(fs::path{}.has_object_name());
    REQUIRE(p.has_object_name());

    REQUIRE_FALSE(fs::path{}.has_stem());
    REQUIRE(p.has_stem());

    REQUIRE_FALSE(fs::path{}.has_extension());
    REQUIRE(p.has_extension());

    REQUIRE_FALSE(fs::path{}.is_absolute());
    REQUIRE(p.is_absolute());

    REQUIRE(fs::path{}.is_relative());
    REQUIRE_FALSE(p.is_relative());
}

TEST_CASE("path iterators", "[iterator]")
{
    const fs::path p = "/a/b/c/d";

    SECTION("iterate over path elements in forward order")
    {
        auto pb = std::begin(p);
        auto pe = std::end(p);

        REQUIRE(*pb   == "/");
        REQUIRE(*++pb == "a");
        REQUIRE(*++pb == "b");
        REQUIRE(*++pb == "c");
        REQUIRE(*++pb == "d");
        REQUIRE(++pb  == pe);
    }

    SECTION("iterate over path elements in reverse order")
    {
        auto pb = std::rbegin(p);
        auto pe = std::rend(p);

        REQUIRE(*pb   == "d");
        REQUIRE(*++pb == "c");
        REQUIRE(*++pb == "b");
        REQUIRE(*++pb == "a");
        REQUIRE(*++pb == "/");
        REQUIRE(++pb  == pe);
    }

    SECTION("STL algorithm support")
    {
        auto pb = std::begin(p);
        auto pe = std::end(p);

        std::vector<fs::path> copy;
        std::copy(pb, pe, std::back_inserter(copy));

        auto cb = std::cbegin(copy);
        auto ce = std::cend(copy);
        auto mm = std::mismatch(pb, pe, cb, ce);

        REQUIRE(std::distance(pb, pe) == std::distance(cb, ce));
        REQUIRE(mm.first == pe);
        REQUIRE(mm.second == ce);
    }
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity, readability-function-size)
TEST_CASE("path validation functions", "[validation]")
{
    // clang-format off
    CHECK_THROWS_AS([] {
        fs::throw_if_path_is_empty("");
    }(), fs::filesystem_error);
    // clang-format on

    // clang-format off
    CHECK_THROWS_AS([] {
        const std::string p(MAX_NAME_LEN, 'X');
        fs::throw_if_path_length_exceeds_limit(p);
    }(), fs::filesystem_error);
    // clang-format on

    // clang-format off
    CHECK_NOTHROW([] {
        fs::throw_if_path_is_empty("a");
    }());
    // clang-format on

    // clang-format off
    CHECK_NOTHROW([] {
        const std::string p(MAX_NAME_LEN - 1, 'X');
        fs::throw_if_path_length_exceeds_limit(p);
    }());
    // clang-format on
}

TEST_CASE("path zero-copy string_view observers and decomposition", "[zero-copy][views]")
{
    const fs::path p{"/tempZone/home/rods/archive.tar.gz"};

    // string() and native() return references to the internal buffer (zero copy)
    REQUIRE(&p.string() == &p.native());
    REQUIRE(p.string().data() == p.c_str());
    REQUIRE(p.string_view().data() == p.c_str());
    REQUIRE(p.size() == std::strlen("/tempZone/home/rods/archive.tar.gz"));

    // Construct and append from std::string_view directly
    constexpr std::string_view sv = "/tempZone/home/rods";
    fs::path p_from_sv{sv};
    p_from_sv /= std::string_view{"sub/file.txt"};
    REQUIRE(p_from_sv == "/tempZone/home/rods/sub/file.txt");

    // Zero-allocation string_view decomposition views match path decomposition
    REQUIRE(p.root_collection_view() == "/");
    REQUIRE(p.relative_path_view() == "tempZone/home/rods/archive.tar.gz");
    REQUIRE(p.parent_path_view() == "/tempZone/home/rods");
    REQUIRE(p.object_name_view() == "archive.tar.gz");
    REQUIRE(p.stem_view() == "archive.tar");
    REQUIRE(p.extension_view() == ".gz");

    // Views point directly into p.c_str() buffer (zero allocation)
    REQUIRE(p.parent_path_view().data() == p.c_str());
    REQUIRE(p.object_name_view().data() == p.c_str() + std::strlen("/tempZone/home/rods/"));

    // Edge cases for decomposition views
    REQUIRE(fs::path{"/"}.parent_path_view() == "/");
    REQUIRE(fs::path{"/"}.object_name_view().empty());
    REQUIRE(fs::path{"/tempZone/home/rods/"}.object_name_view().empty());
    REQUIRE(fs::path{"/tempZone/home/rods/"}.parent_path_view() == "/tempZone/home/rods");
    REQUIRE(fs::path{"/tempZone/home/rods/."}.stem_view().empty());
    REQUIRE(fs::path{"/tempZone/home/rods/.."}.stem_view().empty());
    REQUIRE(fs::path{"/tempZone/home/rods/."}.extension_view().empty());
    REQUIRE(fs::path{"/tempZone/home/rods/.."}.extension_view().empty());

    // Zone name view extracts zone without allocation
    const auto zv = fs::zone_name_view(p);
    REQUIRE(zv.has_value());
    REQUIRE(*zv == "tempZone");
    REQUIRE(zv->data() == p.c_str() + 1);
    REQUIRE(fs::zone_name(p) == std::optional<std::string>{"tempZone"});
    REQUIRE_FALSE(fs::zone_name_view(fs::path{"/"}).has_value());
    REQUIRE_FALSE(fs::zone_name_view(fs::path{}).has_value());

    // In-place assign_concat reuses buffer capacity
    fs::path reusable;
    reusable.assign_concat("/tempZone/home/rods", "data.bin");
    REQUIRE(reusable == "/tempZone/home/rods/data.bin");
    reusable.assign_concat("/tempZone/home/rods/", "next.bin");
    REQUIRE(reusable == "/tempZone/home/rods/next.bin");
}

TEST_CASE("non-throwing path validation and bounded C-array copy", "[validation][error_code]")
{
    std::error_code ec;

    REQUIRE_FALSE(fs::validate_path("", ec));
    REQUIRE(ec == irods::experimental::make_error_code(SYS_INVALID_INPUT_PARAM));

    const std::string too_long(MAX_NAME_LEN, 'a');
    REQUIRE_FALSE(fs::validate_path(fs::path{too_long}, ec));
    REQUIRE(ec == irods::experimental::make_error_code(USER_PATH_EXCEEDS_MAX));

    REQUIRE(fs::validate_path("/tempZone/home/rods", ec));
    REQUIRE_FALSE(ec);

    char buf[16];
    REQUIRE(fs::copy_to_c_array(buf, "/a/b/c", ec));
    REQUIRE_FALSE(ec);
    REQUIRE(std::string_view{buf} == "/a/b/c");

    REQUIRE_FALSE(fs::copy_to_c_array(buf, "0123456789abcdef", ec));
    REQUIRE(ec == irods::experimental::make_error_code(USER_PATH_EXCEEDS_MAX));
}

#include "irods/filesystem/object_status.hpp"

TEST_CASE("object_status move semantics and status_options bitmask", "[object_status][zero-copy]")
{
    std::vector<fs::entity_permission> perms{
        {"rods", "tempZone", fs::perms::own, "rodsadmin"}
    };
    const auto* original_data_ptr = perms.data();

    fs::object_status st{fs::object_type::collection, std::move(perms)};
    REQUIRE(st.permissions().data() == original_data_ptr);

    fs::object_status moved_st{std::move(st)};
    REQUIRE(moved_st.permissions().data() == original_data_ptr);
    REQUIRE(moved_st.type() == fs::object_type::collection);

    std::vector<fs::entity_permission> perms2{
        {"alice", "tempZone", fs::perms::read_object, "rodsuser"}
    };
    const auto* perms2_ptr = perms2.data();
    moved_st.permissions(std::move(perms2));
    REQUIRE(moved_st.permissions().data() == perms2_ptr);

    constexpr auto opts = fs::status_options::include_permissions | fs::status_options::include_inheritance;
    STATIC_REQUIRE(opts == fs::status_options::full);
    STATIC_REQUIRE((opts & fs::status_options::include_permissions) == fs::status_options::include_permissions);
    STATIC_REQUIRE((opts & fs::status_options::include_inheritance) == fs::status_options::include_inheritance);
    STATIC_REQUIRE((fs::status_options::basic & fs::status_options::full) == fs::status_options::basic);
}

#include "irods/filesystem/filesystem.hpp"

TEST_CASE("filesystem client non-throwing error_code overloads on invalid paths", "[filesystem][error_code]")
{
    RcComm* dummy_comm_ptr = nullptr;
    auto& dummy_comm = *dummy_comm_ptr;
    std::error_code ec;

    const fs::path empty_path{};
    const fs::path overlong_path{std::string(MAX_NAME_LEN, 'x')};

    REQUIRE(fs::client::status(dummy_comm, empty_path, fs::status_options::basic, ec).type() == fs::object_type::none);
    REQUIRE(ec == irods::experimental::make_error_code(SYS_INVALID_INPUT_PARAM));

    REQUIRE(fs::client::status(dummy_comm, overlong_path, ec).type() == fs::object_type::none);
    REQUIRE(ec == irods::experimental::make_error_code(USER_PATH_EXCEEDS_MAX));

    REQUIRE_FALSE(fs::client::exists(dummy_comm, empty_path, ec));
    REQUIRE(ec == irods::experimental::make_error_code(SYS_INVALID_INPUT_PARAM));

    REQUIRE_FALSE(fs::client::is_collection_registered(dummy_comm, empty_path, ec));
    REQUIRE(ec == irods::experimental::make_error_code(SYS_INVALID_INPUT_PARAM));

    REQUIRE_FALSE(fs::client::is_data_object_registered(dummy_comm, overlong_path, ec));
    REQUIRE(ec == irods::experimental::make_error_code(USER_PATH_EXCEEDS_MAX));

    REQUIRE(fs::client::data_object_size(dummy_comm, empty_path, ec) == static_cast<std::uintmax_t>(-1));
    REQUIRE(ec == irods::experimental::make_error_code(SYS_INVALID_INPUT_PARAM));

    REQUIRE(fs::client::remove_all(dummy_comm, empty_path, fs::remove_options::none, ec) == static_cast<std::uintmax_t>(-1));
    REQUIRE(ec == irods::experimental::make_error_code(SYS_INVALID_INPUT_PARAM));
}



