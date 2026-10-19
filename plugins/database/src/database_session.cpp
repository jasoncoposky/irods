#include "irods/private/database_session.hpp"

#include "irods/catalog.hpp"
#include "irods/irods_logger.hpp"

namespace irods::experimental::catalog
{
    database_session::database_session(std::size_t _cache_capacity)
        : conn_{}
        , executor_{_cache_capacity}
        , db_instance_name_{}
        , db_type_{0}
    {
    }

    database_session::~database_session() noexcept
    {
        reset();
    }

    database_session::database_session(database_session&& _other) noexcept
        : conn_{std::move(_other.conn_)}
        , executor_{std::move(_other.executor_)}
        , db_instance_name_{std::move(_other.db_instance_name_)}
        , db_type_{_other.db_type_}
    {
        _other.db_type_ = 0;
    }

    auto database_session::operator=(database_session&& _other) noexcept -> database_session&
    {
        if (this != &_other) {
            reset();
            conn_ = std::move(_other.conn_);
            executor_ = std::move(_other.executor_);
            db_instance_name_ = std::move(_other.db_instance_name_);
            db_type_ = _other.db_type_;
            _other.db_type_ = 0;
        }
        return *this;
    }

    auto database_session::connection() -> nanodbc::connection&
    {
        ensure_connected();
        return conn_;
    }

    auto database_session::executor() -> nanodbc_executor&
    {
        return executor_;
    }

    auto database_session::db_instance_name() -> const std::string&
    {
        if (db_instance_name_.empty()) {
            ensure_connected();
        }
        return db_instance_name_;
    }

    auto database_session::db_type() -> int
    {
        if (db_type_ == 0) {
            ensure_connected();
        }
        return db_type_;
    }

    auto database_session::is_connected() const noexcept -> bool
    {
        return conn_.connected();
    }

    auto database_session::reset() noexcept -> void
    {
        try {
            executor_.cache().clear();
            if (conn_.connected()) {
                conn_.disconnect();
            }
            conn_ = nanodbc::connection{};
            db_instance_name_.clear();
            db_type_ = 0;
        }
        catch (...) {
            // Ignore any disconnect errors during reset.
        }
    }

    auto database_session::set_connection(nanodbc::connection _conn, std::string _instance_name) -> void
    {
        reset();
        conn_ = std::move(_conn);
        db_instance_name_ = std::move(_instance_name);
        db_type_ = get_db_type_from_name(db_instance_name_);
    }

    auto database_session::ensure_connected() -> void
    {
        if (!conn_.connected()) {
            reset();
            auto [name, new_conn] = irods::experimental::catalog::new_database_connection();
            conn_ = std::move(new_conn);
            db_instance_name_ = std::move(name);
            db_type_ = get_db_type_from_name(db_instance_name_);
        }
    }

    auto get_database_session() -> database_session&
    {
        thread_local database_session session;
        return session;
    }

} // namespace irods::experimental::catalog
