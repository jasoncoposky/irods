#include <catch2/catch_all.hpp>

#include "irods/client_connection.hpp"
#include "irods/connection_pool.hpp"
#include "irods/dstream.hpp"
#include "irods/filesystem.hpp"
#include "irods/genquery2_builder.hpp"
#include "irods/irods_at_scope_exit.hpp"
#include "irods/replica.hpp"
#include "irods/rodsClient.h"
#include "irods/rodsErrorTable.h"
#include "irods/system_error.hpp"
#include "irods/transport/default_transport.hpp"

#include <fmt/format.h>

#include <chrono>
#include <filesystem>
#include <ios>
#include <limits>
#include <string_view>

#include <unistd.h>

namespace fs = irods::experimental::filesystem;
namespace io = irods::experimental::io;

auto get_hostname() noexcept -> std::string;
auto create_resource_vault(const std::string& _vault_name) -> std::filesystem::path;

TEST_CASE("dstream", "[iostreams]")
{
    load_client_api_plugins();

    auto conn_pool = irods::make_connection_pool(2);

    rodsEnv env;
    _getRodsEnv(env);

    const auto sandbox = fs::path{env.rodsHome} / "unit_testing_sandbox";
    auto conn = conn_pool->get_connection();

    if (!fs::client::exists(conn, sandbox)) {
        REQUIRE(fs::client::create_collection(conn, sandbox));
    }

    irods::at_scope_exit remove_sandbox{[&conn, &sandbox] {
        REQUIRE(fs::client::remove_all(conn, sandbox, fs::remove_options::no_trash));
    }};

    SECTION("default constructed stream does not cause segfault on destruction")
    {
        io::dstream stream;
    }

    SECTION("supports move semantics")
    {
        const auto path = sandbox / "data_object.txt";

        // Guarantees that the stream is closed before clean up.
        {
            io::client::default_transport xport{conn};
            io::odstream stream{xport, path};
            REQUIRE(stream.is_open());

            // Move construct.
            auto other_stream = std::move(stream);
            REQUIRE_FALSE(stream.is_open());
            REQUIRE(other_stream.is_open());
            
            // Construct and then move assign the stream.
            io::odstream another_stream;
            REQUIRE_FALSE(another_stream.is_open());

            another_stream = std::move(other_stream);
            REQUIRE_FALSE(other_stream.is_open());
            REQUIRE(another_stream.is_open());
        }
    }

    SECTION("allows access to underlying stream buffer object")
    {
        io::dstream stream;
        REQUIRE_FALSE(!stream.rdbuf());
    }

    SECTION("in, out, app openmode combinations are correctly translated to POSIX open flags")
    {
        const auto path = sandbox / "data_object.txt";
        std::string_view message = "Hello, iRODS!";

        // Show that (in | out) openmode creates a new data object if
        // it does not exist. The (out) flag is implicit due to the use of odstream.
        {
            io::client::default_transport xport{conn};
            io::odstream{xport, path, std::ios_base::in | std::ios_base::app} << message;
        }

        REQUIRE(fs::client::exists(conn, path));
        REQUIRE(irods::experimental::replica::replica_size<rcComm_t>(conn, path, 0) == message.size());

        // Verify that the data object contains the expected message.
        {
            io::client::default_transport xport{conn};
            io::idstream in{xport, path};

            std::string line;
            std::getline(in, line);

            REQUIRE(message == line);
        }

        SECTION("out+in+app openmode appends to data object without truncating")
        {
            io::client::default_transport xport{conn};
            io::odstream out{xport, path, std::ios_base::out | std::ios_base::in | std::ios_base::app};
            out.close();

            REQUIRE(irods::experimental::replica::replica_size<rcComm_t>(conn, path, 0) > 0);

            out.open(xport, path, std::ios_base::in | std::ios_base::out | std::ios_base::app);
            out << message;
        }

        SECTION("in+app openmode appends to data object without truncating")
        {
            io::client::default_transport xport{conn};
            io::odstream out{xport, path, std::ios_base::out | std::ios_base::in | std::ios_base::app};
            out.close();

            REQUIRE(irods::experimental::replica::replica_size<rcComm_t>(conn, path, 0) > 0);

            out.open(xport, path, std::ios_base::in | std::ios_base::out | std::ios_base::app);
            out << message;
        }

        // The final data object should contain the message twice.
        io::client::default_transport xport{conn};
        io::idstream in{xport, path};

        std::string line;
        std::getline(in, line);

        REQUIRE(std::string{message} + message.data() == line);
    }

    SECTION("dstream supports intermediate replicas and replica tokens")
    {
        const auto path = sandbox / "data_object.txt";

        io::client::default_transport tp0{conn};
        io::odstream ds0{tp0, path};
        REQUIRE(ds0);

        auto other_conn = conn_pool->get_connection();

        SECTION("opening replica in intermediate state without replica token generates an error")
        {
            io::client::default_transport tp1{other_conn};
            io::odstream ds1{tp1, path, io::replica_number{ds0.replica_number()}};
            REQUIRE_FALSE(ds1);
        }

        SECTION("opening replica in intermediate state with replica token")
        {
            io::client::default_transport tp1{other_conn};
            io::odstream ds1{tp1, ds0.replica_token(), path, io::replica_number{ds0.replica_number()}};
            REQUIRE(ds1);
        }
    }

    SECTION("create replica using root resource")
    {
        const auto hostname = get_hostname();
        const auto vault = create_resource_vault("irods_unit_testing_vault");

        {
            const auto cmd = fmt::format("iadmin mkresc ut_ufs_resc unixfilesystem {}:{}", hostname, vault.c_str());
            REQUIRE(std::system(cmd.data()) == 0);
        }

        // A new connection pool must be used so that the resource manager within
        // the agents can see the new resources.
        auto conn_pool = irods::make_connection_pool();
        auto conn = conn_pool->get_connection();
        const auto path = sandbox / "data_object.txt";

        irods::at_scope_exit remove_resources{[&conn, &path] {
            REQUIRE(fs::client::remove(conn, path, fs::remove_options::no_trash));
            REQUIRE(std::system("iadmin rmresc ut_ufs_resc") == 0);
        }};

        io::client::native_transport tp{conn};
        io::odstream out{tp, path, io::root_resource_name{"ut_ufs_resc"}};
        REQUIRE(out);
        out << "Hello, iRODS!";
        out.close();

        REQUIRE(fs::client::is_data_object(conn, path));
    }

    SECTION("create replica using leaf resource")
    {
        const auto hostname = get_hostname();
        const auto vault = create_resource_vault("irods_unit_testing_vault_1");

        {
            // Create the resource hierarchy (ut_pt_resc;ut_ufs_resc_1)
            //                               (passthru  ;unixfilesystem)
            const auto cmd = fmt::format("iadmin mkresc ut_ufs_resc_1 unixfilesystem {}:{}", hostname, vault.c_str());
            REQUIRE(std::system("iadmin mkresc ut_pt_resc passthru") == 0);
            REQUIRE(std::system(cmd.data()) == 0);
            REQUIRE(std::system("iadmin addchildtoresc ut_pt_resc ut_ufs_resc_1") == 0);
        }

        // A new connection pool must be used so that the resource manager within
        // the agents can see the new resources.
        auto conn_pool = irods::make_connection_pool();
        auto conn = conn_pool->get_connection();
        const auto path = sandbox / "data_object.txt";

        irods::at_scope_exit remove_resources{[&conn, &path] {
            if (fs::client::exists(conn, path)) {
                REQUIRE(fs::client::remove(conn, path, fs::remove_options::no_trash));
            }

            REQUIRE(std::system("iadmin rmchildfromresc ut_pt_resc ut_ufs_resc_1") == 0);
            REQUIRE(std::system("iadmin rmresc ut_ufs_resc_1") == 0);
            REQUIRE(std::system("iadmin rmresc ut_pt_resc") == 0);
        }};

        io::client::native_transport tp{conn};
        io::odstream out{tp, path, io::leaf_resource_name{"ut_ufs_resc_1"}};
        REQUIRE(out);
        out << "Hello, iRODS!";
        out.close();

        REQUIRE(fs::client::is_data_object(conn, path));

        SECTION("create a new replica")
        {
            const auto vault = create_resource_vault("irods_unit_testing_vault_2");

            {
                const auto cmd = fmt::format("iadmin mkresc ut_ufs_resc_2 unixfilesystem {}:{}", hostname, vault.c_str());
                REQUIRE(std::system(cmd.data()) == 0);
            }

            // A new connection pool must be used so that the resource manager within
            // the agents can see the new resources.
            auto conn_pool = irods::make_connection_pool();
            auto conn = conn_pool->get_connection();
            const auto path = sandbox / "data_object.txt";

            irods::at_scope_exit remove_resources{[&conn, &path] {
                REQUIRE(fs::client::remove(conn, path, fs::remove_options::no_trash));
                REQUIRE(std::system("iadmin rmresc ut_ufs_resc_2") == 0);
            }};

            // Create the second replica.
            io::client::native_transport tp{conn};
            io::odstream out{tp, path, io::root_resource_name{"ut_ufs_resc_2"}};
            REQUIRE(out);
            out << "Hello, iRODS!";
            out.close();

            // Show that two replicas exist.
            namespace gq2 = irods::experimental::genquery2;
            const auto gql = gq2::builder::select(gq2::builder::count("DATA_ID"))
                                 .where(gq2::builder::col("COLL_NAME") == path.parent_path().c_str() &&
                                        gq2::builder::col("DATA_NAME") == path.object_name().c_str());

            for (const auto& row : gq2::execute(*static_cast<rcComm_t*>(conn), gql)) {
                REQUIRE(row[0].get_ref<const std::string&>() == "2");
            }
        }
    }

    SECTION("non-existent root resource causes open to fail")
    {
        io::client::native_transport tp{conn};
        io::odstream out{tp, sandbox / "data_object.txt", io::root_resource_name{"bad_root_resc"}};
        REQUIRE_FALSE(out);
    }

    SECTION("non-existent leaf resource causes open to fail")
    {
        io::client::native_transport tp{conn};
        io::odstream out{tp, sandbox / "data_object.txt", io::leaf_resource_name{"bad_leaf_resc"}};
        REQUIRE_FALSE(out);
    }

    SECTION("replica number cannot be used to create new replicas")
    {
        io::client::native_transport tp{conn};
        io::odstream out{tp, sandbox / "should_not_exist", io::replica_number{1}};
        REQUIRE_FALSE(out);
    }

    SECTION("read previously written bytes using the same stream")
    {
        io::client::native_transport tp{conn};
        io::dstream ds{tp, sandbox / "data_object.txt", std::ios::in | std::ios::out | std::ios::trunc};

        ds.write("abcd", 4);
        ds.seekp(-2, std::ios::cur);

        char buf[2]{};
        ds.read(buf, 2);
        REQUIRE(std::string_view(buf, 2) == "cd");
    }
}

TEST_CASE("#8459: dstream tracks last error code")
{
    load_client_api_plugins();

    rodsEnv env;
    _getRodsEnv(env);

    irods::experimental::client_connection conn;

    const auto sandbox = fs::path{env.rodsHome} / "issue_8459_sandbox";
    if (!fs::client::exists(conn, sandbox)) {
        REQUIRE(fs::client::create_collection(conn, sandbox));
    }

    const irods::at_scope_exit remove_sandbox{
        [&conn, &sandbox] { REQUIRE(fs::client::remove_all(conn, sandbox, fs::remove_options::no_trash)); }};

    io::client::native_transport tp{conn};

    SECTION("opening a nonexistent data object")
    {
        io::idstream in{tp, sandbox / "does_not_exist"};
        CHECK_FALSE(in);
        const auto errc = irods::experimental::make_error_code(in.last_error());
        CHECK(irods::experimental::get_irods_error_code(errc) == OBJ_PATH_DOES_NOT_EXIST);
    }

    SECTION("passing an invalid offset to seek")
    {
        const auto data_object = sandbox / "data_object.txt";

        io::odstream out{tp, data_object};
        CHECK(out);
        CHECK_FALSE(out.seekp(-1, std::ios::beg));
        auto errc = irods::experimental::make_error_code(out.last_error());
        CHECK(irods::experimental::get_irods_error_code(errc) == UNIX_FILE_LSEEK_ERR);
        out.close();

        io::idstream in{tp, data_object};
        CHECK(in);
        CHECK_FALSE(in.seekg(-1, std::ios::beg));
        errc = irods::experimental::make_error_code(in.last_error());
        CHECK(irods::experimental::get_irods_error_code(errc) == UNIX_FILE_LSEEK_ERR);
    }
}

TEST_CASE("#9002: dstream detects narrowing conversion errors on reads and writes")
{
    load_client_api_plugins();

    rodsEnv env;
    _getRodsEnv(env);

    irods::experimental::client_connection conn;

    const auto sandbox = fs::path{env.rodsHome} / "issue_9002_sandbox";
    if (!fs::client::exists(conn, sandbox)) {
        REQUIRE(fs::client::create_collection(conn, sandbox));
    }

    const irods::at_scope_exit remove_sandbox{
        [&conn, &sandbox] { REQUIRE(fs::client::remove_all(conn, sandbox, fs::remove_options::no_trash)); }};

    const auto data_object = sandbox / "data_object.txt";

    io::client::native_transport tp{conn};

    // Create a data object.
    {
        io::odstream{tp, data_object} << "some data";
    }

    io::dstream stream{tp, data_object};
    REQUIRE(stream);

    char buf{};

    SECTION("read")
    {
        CHECK_FALSE(stream.read(&buf, std::numeric_limits<std::streamsize>::max()));
        CHECK(stream.last_error() == NARROWING_CONVERSION_ERROR);
    }

    SECTION("write")
    {
        CHECK_FALSE(stream.write(&buf, std::numeric_limits<std::streamsize>::max()));
        CHECK(stream.last_error() == NARROWING_CONVERSION_ERROR);
    }
}

auto get_hostname() noexcept -> std::string
{
    char hostname[HOST_NAME_MAX + 1]{};
    gethostname(hostname, sizeof(hostname));
    return hostname;
}

auto create_resource_vault(const std::string& _vault_name) -> std::filesystem::path
{
    namespace fs = std::filesystem;

    const auto suffix = "_" + std::to_string(std::chrono::system_clock::now().time_since_epoch().count());
    const auto vault = fs::temp_directory_path() / (_vault_name + suffix).data();

    // Create the vault for the resource and allow the iRODS server to
    // read and write to the vault.

    if (!fs::exists(vault)) {
        REQUIRE(fs::create_directory(vault));
    }

    fs::permissions(vault, fs::perms::others_read | fs::perms::others_write, fs::perm_options::add);

    return vault;
}

namespace
{
    class memory_transport final : public io::transport<char>
    {
    public:
        std::string data;
        pos_type pos{0};
        bool open_state = false;
        io::root_resource_name root_resc{"demoResc"};
        io::leaf_resource_name leaf_resc{"demoResc"};
        io::replica_number repl_num{0};
        io::replica_token repl_tok{"tok"};
        int err = 0;

        bool open(const fs::path&, std::ios_base::openmode) override
        {
            if (open_state) {
                err = SYS_NOT_ALLOWED;
                return false;
            }
            open_state = true;
            pos = 0;
            return true;
        }

        bool open(const fs::path&, const io::replica_number&, std::ios_base::openmode) override
        {
            return open_state ? false : (open_state = true);
        }

        bool open(const fs::path&, const io::root_resource_name&, std::ios_base::openmode) override
        {
            return open_state ? false : (open_state = true);
        }

        bool open(const fs::path&, const io::leaf_resource_name&, std::ios_base::openmode) override
        {
            return open_state ? false : (open_state = true);
        }

        bool open(const io::replica_token&, const fs::path&, const io::replica_number&, std::ios_base::openmode) override
        {
            return open_state ? false : (open_state = true);
        }

        bool open(const io::replica_token&, const fs::path&, const io::leaf_resource_name&, std::ios_base::openmode) override
        {
            return open_state ? false : (open_state = true);
        }

        bool close(const io::on_close_success* = nullptr) override
        {
            if (!open_state) {
                return false;
            }
            open_state = false;
            return true;
        }

        std::streamsize receive(char_type* _buf, std::streamsize _size) override
        {
            const auto cur = static_cast<std::size_t>(pos);
            if (cur >= data.size()) {
                return 0;
            }
            const auto n = std::min<std::size_t>(static_cast<std::size_t>(_size), data.size() - cur);
            std::memcpy(_buf, data.data() + cur, n);
            pos += static_cast<off_type>(n);
            return static_cast<std::streamsize>(n);
        }

        std::streamsize send(const char_type* _buf, std::streamsize _size) override
        {
            const auto cur = static_cast<std::size_t>(pos);
            const auto n = static_cast<std::size_t>(_size);
            if (cur + n > data.size()) {
                data.resize(cur + n);
            }
            std::memcpy(data.data() + cur, _buf, n);
            pos += static_cast<off_type>(n);
            return _size;
        }

        pos_type seekpos(off_type _off, std::ios_base::seekdir _dir) override
        {
            off_type base = 0;
            if (_dir == std::ios_base::beg) {
                base = 0;
            }
            else if (_dir == std::ios_base::cur) {
                base = static_cast<off_type>(pos);
            }
            else if (_dir == std::ios_base::end) {
                base = static_cast<off_type>(data.size());
            }
            else {
                return pos_type{off_type{-1}};
            }

            if (base + _off < 0) {
                return pos_type{off_type{-1}};
            }
            pos = base + _off;
            return pos;
        }

        bool is_open() const noexcept override
        {
            return open_state;
        }

        int file_descriptor() const noexcept override
        {
            return open_state ? 3 : -1;
        }

        const io::root_resource_name& root_resource_name() const override
        {
            return root_resc;
        }

        const io::leaf_resource_name& leaf_resource_name() const override
        {
            return leaf_resc;
        }

        const io::replica_number& replica_number() const override
        {
            return repl_num;
        }

        const io::replica_token& replica_token() const override
        {
            return repl_tok;
        }

        int last_error() const noexcept override
        {
            return err;
        }
    };
} // anonymous namespace

TEST_CASE("dstream offline buffer and transport semantics", "[offline]")
{
    SECTION("default constructed dstream has null-safe accessors")
    {
        io::dstream ds;
        CHECK_FALSE(ds.is_open());
        CHECK(ds.file_descriptor() == -1);
        CHECK(ds.last_error() == 0);
    }

    SECTION("move construction preserves buffered get/put pointers without dangling")
    {
        memory_transport tp;
        tp.data = "0123456789abcdef";

        std::string tail;
        {
            io::idstream in1{tp, "/tempZone/home/rods/obj"};
            REQUIRE(in1.is_open());
            // Prime the internal Get buffer via peek()/get().
            CHECK(in1.get() == '0');
            CHECK(in1.peek() == '1');

            auto in2 = std::move(in1);
            CHECK_FALSE(in1.is_open());
            REQUIRE(in2.is_open());

            // Reading from in2 after in1's buffer was moved must read from in2's own buffer.
            std::getline(in2, tail);
        }
        CHECK(tail == "123456789abcdef");
    }

    SECTION("xsgetn does not overflow buffer when Get area has more bytes than requested")
    {
        memory_transport tp;
        tp.data = "abcdefghijklmnopqrstuvwxyz";

        io::idstream in{tp, "/tempZone/home/rods/obj"};
        REQUIRE(in.is_open());

        // Trigger underflow() so all 26 bytes are in the internal Get buffer.
        CHECK(in.peek() == 'a');

        char small_buf[5]{};
        in.read(small_buf, 5);
        CHECK(in.gcount() == 5);
        CHECK(std::string_view{small_buf, 5} == "abcde");

        // Now read 10 more bytes (from the remaining 21 buffered bytes).
        char next_buf[10]{};
        in.read(next_buf, 10);
        CHECK(in.gcount() == 10);
        CHECK(std::string_view{next_buf, 10} == "fghijklmno");
    }

    SECTION("xsgetn combines buffered bytes and transport bytes accurately")
    {
        memory_transport tp;
        tp.data.resize(5000, 'X');
        std::memcpy(tp.data.data(), "HELLO", 5);
        std::memcpy(tp.data.data() + 4096, "WORLD", 5);

        io::idstream in{tp, "/tempZone/home/rods/obj"};
        REQUIRE(in.is_open());

        // Trigger underflow() to load the first 4096 bytes into buf_.
        CHECK(in.peek() == 'H');

        std::string out(4101, '\0');
        in.read(out.data(), static_cast<std::streamsize>(out.size()));
        CHECK(in.gcount() == 4101);
        CHECK(out.substr(0, 5) == "HELLO");
        CHECK(out.substr(4096, 5) == "WORLD");
    }

    SECTION("seekg and tellg account for unconsumed read-ahead bytes in Get area")
    {
        memory_transport tp;
        tp.data = "0123456789abcdef";

        io::idstream in{tp, "/tempZone/home/rods/obj"};
        REQUIRE(in.is_open());

        // Read 4 characters via get() so underflow() buffers all 16 bytes and advances gptr() by 4.
        CHECK(in.get() == '0');
        CHECK(in.get() == '1');
        CHECK(in.get() == '2');
        CHECK(in.get() == '3');

        // Logical position must be 4 even though the transport read ahead to 16.
        CHECK(in.tellg() == std::streampos{4});

        // Relative seek back by 2 from logical pos 4 -> pos 2.
        REQUIRE(in.seekg(-2, std::ios::cur));
        CHECK(in.tellg() == std::streampos{2});
        CHECK(in.get() == '2');
    }

    SECTION("double open on already-open stream fails cleanly")
    {
        memory_transport tp;
        io::odstream out{tp, "/tempZone/home/rods/obj"};
        REQUIRE(out.is_open());

        out.open(tp, "/tempZone/home/rods/obj2");
        CHECK(out.fail());
    }
}

