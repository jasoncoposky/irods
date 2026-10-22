#undef IRODS_FILESYSTEM_COLLECTION_ENTRY_HPP_INCLUDE_HEADER

#if defined(IRODS_FILESYSTEM_ENABLE_SERVER_SIDE_API)
#  if !defined(IRODS_FILESYSTEM_COLLECTION_ENTRY_HPP_FOR_SERVER)
#    define IRODS_FILESYSTEM_COLLECTION_ENTRY_HPP_FOR_SERVER
#    define IRODS_FILESYSTEM_COLLECTION_ENTRY_HPP_INCLUDE_HEADER
#  endif
#elif !defined(IRODS_FILESYSTEM_COLLECTION_ENTRY_HPP_FOR_CLIENT)
#  define IRODS_FILESYSTEM_COLLECTION_ENTRY_HPP_FOR_CLIENT
#  define IRODS_FILESYSTEM_COLLECTION_ENTRY_HPP_INCLUDE_HEADER
#endif

#ifdef IRODS_FILESYSTEM_COLLECTION_ENTRY_HPP_INCLUDE_HEADER

#include "irods/filesystem/config.hpp"
#include "irods/filesystem/filesystem.hpp"
#include "irods/filesystem/path.hpp"
#include "irods/filesystem/object_status.hpp"

#include <chrono>

#ifndef IRODS_FILESYSTEM_COLLECTION_ENTRY_CLASS_DEFINED
#define IRODS_FILESYSTEM_COLLECTION_ENTRY_CLASS_DEFINED

namespace irods::filesystem
{
    namespace client
    {
        class collection_iterator;
        class recursive_collection_iterator;
    } // namespace client

    namespace server
    {
        class collection_iterator;
        class recursive_collection_iterator;
    } // namespace server

    class collection_entry
    {
    public:
        // Observers

        // clang-format off
        // NOLINTNEXTLINE(google-explicit-constructor)
        operator const path&() const noexcept                     { return path_; }
        auto path() const noexcept -> const path&                 { return path_; }
        auto exists() const noexcept -> bool                      { return status_.type() != object_type::none && status_.type() != object_type::not_found; }
        auto is_data_object() const noexcept -> bool              { return status_.type() == object_type::data_object; }
        auto is_collection() const noexcept -> bool               { return status_.type() == object_type::collection; }
        auto is_other() const noexcept -> bool                    { return status_.type() == object_type::unknown; }
        auto creation_time() const noexcept -> object_time_type   { return ctime_; }
        auto last_write_time() const noexcept -> object_time_type { return mtime_; }
        auto status() const noexcept -> const object_status&      { return status_; }
        auto data_mode() const noexcept -> unsigned               { return data_mode_; }
        auto data_id() const noexcept -> const std::string&       { return data_id_; }
        auto checksum() const noexcept -> const std::string&      { return checksum_; }
        auto owner() const noexcept -> const std::string&         { return owner_; }
        auto data_type() const noexcept -> const std::string&     { return data_type_; }
        auto data_size() const noexcept -> std::uintmax_t         { return data_size_; }

        // Comparisons

        auto operator==(const collection_entry& _rhs) const noexcept -> bool { return path_ == _rhs.path_; }
        auto operator!=(const collection_entry& _rhs) const noexcept -> bool { return path_ != _rhs.path_; }
        auto operator< (const collection_entry& _rhs) const noexcept -> bool { return path_ <  _rhs.path_; }
        auto operator<=(const collection_entry& _rhs) const noexcept -> bool { return path_ <= _rhs.path_; }
        auto operator> (const collection_entry& _rhs) const noexcept -> bool { return path_ >  _rhs.path_; }
        auto operator>=(const collection_entry& _rhs) const noexcept -> bool { return path_ >= _rhs.path_; }
        // clang-format on

    private:
        friend class client::collection_iterator;
        friend class client::recursive_collection_iterator;
        friend class server::collection_iterator;
        friend class server::recursive_collection_iterator;

        mutable class path path_{};
        mutable object_status status_{};
        mutable unsigned data_mode_{};
        mutable std::uintmax_t data_size_{};
        mutable std::string data_id_{};
        mutable object_time_type ctime_{};
        mutable object_time_type mtime_{};
        mutable std::string checksum_{};
        mutable std::string owner_{};
        mutable std::string data_type_{};
    };
} // namespace irods::filesystem

#endif // IRODS_FILESYSTEM_COLLECTION_ENTRY_CLASS_DEFINED

namespace irods::filesystem::NAMESPACE_IMPL
{
    using collection_entry = ::irods::filesystem::collection_entry;
} // namespace irods::filesystem::NAMESPACE_IMPL

#endif // IRODS_FILESYSTEM_COLLECTION_ENTRY_HPP_INCLUDE_HEADER
