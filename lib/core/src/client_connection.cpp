#include "irods/client_connection.hpp"

#include "irods/authentication_plugin_framework.hpp"
#include "irods/rcConnect.h"
#include "irods/rodsErrorTable.h"
#include "irods/irods_exception.hpp"

#include <array>
#include <cstring>

namespace irods::experimental
{
    namespace
    {
        struct stack_cstr
        {
            std::array<char, MAX_NAME_LEN> buf{};
            std::string fallback{};
            const char* ptr = nullptr;

            explicit stack_cstr(std::string_view _sv)
            {
                if (_sv.size() < buf.size()) {
                    std::memcpy(buf.data(), _sv.data(), _sv.size());
                    buf[_sv.size()] = '\0';
                    ptr = buf.data();
                }
                else {
                    fallback.assign(_sv);
                    ptr = fallback.c_str();
                }
            }
        };

        auto do_connect(std::string_view _host,
                        const int _port,
                        const fully_qualified_username& _username) -> RcComm*
        {
            const stack_cstr host{_host};
            rErrMsg_t error{};
            auto* conn = rcConnect(
                host.ptr, _port, _username.name().c_str(), _username.zone().c_str(), NO_RECONN, &error);

            if (error.status < 0) {
                // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
                THROW(error.status, error.msg);
            }

            if (!conn) {
                // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
                THROW(USER_SOCK_CONNECT_ERR, "Connect error");
            }

            return conn;
        }

        auto do_connect(std::string_view _host,
                        const int _port,
                        const fully_qualified_username& _proxy_username,
                        const fully_qualified_username& _username) -> RcComm*
        {
            const stack_cstr host{_host};
            rErrMsg_t error{};
            auto* conn = _rcConnect(host.ptr,
                                    _port,
                                    _proxy_username.name().c_str(),
                                    _proxy_username.zone().c_str(),
                                    _username.name().c_str(),
                                    _username.zone().c_str(),
                                    &error,
                                    1,
                                    NO_RECONN);

            if (error.status < 0) {
                // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
                THROW(error.status, error.msg);
            }

            if (!conn) {
                // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
                THROW(USER_SOCK_CONNECT_ERR, "Connect error");
            }

            return conn;
        }
    } // anonymous namespace

    client_connection::client_connection()
        : conn_{nullptr, rcDisconnect}
    {
        rodsEnv env{};
        _getRodsEnv(env);

        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
        conn_.reset(do_connect(env.rodsHost, env.rodsPort, {env.rodsUserName, env.rodsZone}));
        login();
    } // default constructor

    client_connection::client_connection(struct defer_authentication) // NOLINT(readability-named-parameter)
        : conn_{nullptr, rcDisconnect}
    {
        rodsEnv env{};
        _getRodsEnv(env);

        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
        conn_.reset(do_connect(env.rodsHost, env.rodsPort, {env.rodsUserName, env.rodsZone}));
    } // constructor

    client_connection::client_connection(const std::string_view _host,
                                         const int _port,
                                         const fully_qualified_username& _username)
        : conn_{nullptr, rcDisconnect}
    {
        conn_.reset(do_connect(_host, _port, _username));
        login();
    } // constructor

    client_connection::client_connection(struct defer_authentication, // NOLINT(readability-named-parameter)
                                         const std::string_view _host,
                                         const int _port,
                                         const fully_qualified_username& _username)
        : conn_{nullptr, rcDisconnect}
    {
        conn_.reset(do_connect(_host, _port, _username));
    } // constructor

    client_connection::client_connection(const std::string_view _host,
                                         const int _port,
                                         const fully_qualified_username& _proxy_username,
                                         const fully_qualified_username& _username)
        : conn_{nullptr, rcDisconnect}
    {
        conn_.reset(do_connect(_host, _port, _proxy_username, _username));
        login();
    } // constructor

    client_connection::client_connection(struct defer_authentication, // NOLINT(readability-named-parameter)
                                         const std::string_view _host,
                                         const int _port,
                                         const fully_qualified_username& _proxy_username,
                                         const fully_qualified_username& _username)
        : conn_{nullptr, rcDisconnect}
    {
        conn_.reset(do_connect(_host, _port, _proxy_username, _username));
    } // constructor

    client_connection::client_connection(RcComm& _conn)
        : conn_{&_conn, rcDisconnect}
    {
    } // constructor

    client_connection::client_connection(struct defer_connection) // NOLINT(readability-named-parameter)
        : conn_{nullptr, rcDisconnect}
    {
    } // constructor

    auto client_connection::connect() -> void
    {
        rodsEnv env{};
        _getRodsEnv(env);

        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
        conn_.reset(do_connect(env.rodsHost, env.rodsPort, {env.rodsUserName, env.rodsZone}));
        login();
    } // connect

    auto client_connection::connect(struct defer_authentication) -> void // NOLINT(readability-named-parameter)
    {
        rodsEnv env{};
        _getRodsEnv(env);

        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
        conn_.reset(do_connect(env.rodsHost, env.rodsPort, {env.rodsUserName, env.rodsZone}));
    } // connect

    auto client_connection::connect(const std::string_view _host,
                                    const int _port,
                                    const fully_qualified_username& _username) -> void
    {
        conn_.reset(do_connect(_host, _port, _username));
        login();
    } // connect

    auto client_connection::connect(struct defer_authentication, // NOLINT(readability-named-parameter)
                                    const std::string_view _host,
                                    const int _port,
                                    const fully_qualified_username& _username) -> void
    {
        conn_.reset(do_connect(_host, _port, _username));
    } // connect

    auto client_connection::connect(struct defer_authentication, // NOLINT(readability-named-parameter)
                                    const std::string_view _host,
                                    const int _port,
                                    const fully_qualified_username& _proxy_username,
                                    const fully_qualified_username& _username) -> void
    {
        conn_.reset(do_connect(_host, _port, _proxy_username, _username));
    } // connect

    auto client_connection::connect(const fully_qualified_username& _proxy_username) -> void
    {
        rodsEnv env{};
        _getRodsEnv(env);

        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
        conn_.reset(do_connect(env.rodsHost, env.rodsPort, _proxy_username, {env.rodsUserName, env.rodsZone}));
        login();
    } // connect

    // NOLINTNEXTLINE(readability-named-parameter)
    auto client_connection::connect(struct defer_authentication, const fully_qualified_username& _proxy_username)
        -> void
    {
        rodsEnv env{};
        _getRodsEnv(env);

        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
        conn_.reset(do_connect(env.rodsHost, env.rodsPort, _proxy_username, {env.rodsUserName, env.rodsZone}));
    } // connect

    auto client_connection::disconnect() noexcept -> void
    {
        conn_.reset();
    } // disconnect

    client_connection::operator bool() const noexcept
    {
        return static_cast<bool>(conn_);
    } // operator bool

    client_connection::operator RcComm&() const
    {
        if (!conn_) {
            // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
            THROW(SYS_LIBRARY_ERROR, "Invalid connection object");
        }

        return *conn_;
    } // operator RcComm&

    client_connection::operator RcComm*() const noexcept
    {
        return conn_.get();
    } // operator RcComm*

    auto client_connection::connect_and_login(const std::string& _host,
                                              const int _port,
                                              const fully_qualified_username& _username) -> void
    {
        only_connect(_host, _port, _username);
        login();
    } // connect_and_login

    auto client_connection::connect_and_login(const std::string& _host,
                                              const int _port,
                                              const fully_qualified_username& _proxy_username,
                                              const fully_qualified_username& _username) -> void
    {
        only_connect(_host, _port, _proxy_username, _username);
        login();
    } // connect_and_login

    auto client_connection::only_connect(const std::string& _host,
                                         const int _port,
                                         const fully_qualified_username& _username) -> void
    {
        conn_.reset(do_connect(_host, _port, _username));
    } // only_connect

    auto client_connection::only_connect(const std::string& _host,
                                         const int _port,
                                         const fully_qualified_username& _proxy_username,
                                         const fully_qualified_username& _username) -> void
    {
        conn_.reset(do_connect(_host, _port, _proxy_username, _username));
    } // only_connect

    auto client_connection::login() -> void
    {
        namespace ia = irods::authentication;

        if (!conn_) {
            // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
            THROW(SYS_LIBRARY_ERROR, "Invalid connection object");
        }

        RodsEnvironment env;
        _getRodsEnv(env);

        const auto ctx = nlohmann::json{{ia::scheme_name, env.rodsAuthScheme}};
        if (const auto err = ia::authenticate_client(*conn_, ctx); err < 0) {
            // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
            THROW(err, "Client login error");
        }
    } // login
} // namespace irods::experimental
