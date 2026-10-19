#ifndef IRODS_DATABASE_SESSION_HPP
#define IRODS_DATABASE_SESSION_HPP

#include <nanodbc/nanodbc.h>

#include "irods/catalog.hpp"
#include "irods/db_flavor.hpp"
#include "irods/private/nanodbc_executor.hpp"

#include <cstddef>
#include <memory>
#include <string>

namespace irods::experimental::catalog
{
    /// \brief Encapsulates a persistent per-thread database connection and statement cache.
    class database_session
    {
    public:
        explicit database_session(std::size_t _cache_capacity = 128);
        ~database_session() noexcept;

        database_session(const database_session&) = delete;
        auto operator=(const database_session&) -> database_session& = delete;

        database_session(database_session&&) noexcept;
        auto operator=(database_session&&) noexcept -> database_session&;

        /// \brief Returns the active nanodbc connection, establishing or reconnecting if needed.
        auto connection() -> nanodbc::connection&;

        /// \brief Returns the persistent nanodbc_executor with statement cache.
        auto executor() -> nanodbc_executor&;

        /// \brief Returns the database instance name ("postgres", "mysql", "oracle").
        auto db_instance_name() -> const std::string&;

        /// \brief Returns the database type integer (DB_TYPE_POSTGRES, etc.).
        auto db_type() -> int;

        /// \brief Disconnects the active connection and clears the statement cache.
        auto reset() noexcept -> void;

        /// \brief Returns true if currently connected to the database.
        [[nodiscard]] auto is_connected() const noexcept -> bool;

        /// \brief Sets the active connection and instance name (useful for testing).
        auto set_connection(nanodbc::connection _conn, std::string _instance_name) -> void;

    private:
        auto ensure_connected() -> void;

        nanodbc::connection conn_;
        nanodbc_executor executor_;
        std::string db_instance_name_;
        int db_type_ = 0;
    };

    /// \brief Returns the thread-local database session.
    auto get_database_session() -> database_session&;

    /// \brief Helper proxy returned by get_session() supporting structured bindings:
    /// auto [db_instance, db_conn, executor] = irods::experimental::catalog::get_session();
    struct session_handle
    {
        std::string_view db_instance;
        nanodbc::connection& db_conn;
        nanodbc_executor& executor;
    };

    /// \brief Helper proxy returned by get_session_connection() supporting structured bindings:
    /// auto [db_instance, db_conn] = irods::experimental::catalog::get_session_connection();
    struct connection_handle
    {
        std::string_view db_instance;
        nanodbc::connection& db_conn;
    };

    /// \brief Returns the active session handle with connection, executor, and instance name.
    inline auto get_session() -> session_handle
    {
        auto& s = get_database_session();
        return {s.db_instance_name(), s.connection(), s.executor()};
    }

    /// \brief Returns the active connection handle with connection and instance name.
    inline auto get_session_connection() -> connection_handle
    {
        auto& s = get_database_session();
        return {s.db_instance_name(), s.connection()};
    }

} // namespace irods::experimental::catalog

#endif // IRODS_DATABASE_SESSION_HPP
