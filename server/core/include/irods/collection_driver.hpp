#ifndef IRODS_COLLECTION_DRIVER_HPP
#define IRODS_COLLECTION_DRIVER_HPP

/// \file

#include "irods/collCreate.h"
#include "irods/collection_entry_stream.hpp"
#include "irods/collection_resolver.hpp"
#include "irods/collection_types.hpp"
#include "irods/objInfo.h"
#include "irods/objStat.h"
#include "irods/rodsConnect.h"
#include "irods/rodsDef.h"

namespace irods::experimental::collections
{
    /// Abstract polymorphic interface for collection storage drivers (`Catalog`, `Mounted`, `Archive`).
    class collection_driver
    {
      public:
        virtual ~collection_driver() = default;

        virtual auto stat_path(
            RsComm& _comm,
            const resolved_path& _resolved,
            rodsObjStat_t** _out) const -> int = 0;

        virtual auto create_directory(
            RsComm& _comm,
            const resolved_path& _resolved,
            collInp_t& _inp) const -> int = 0;

        virtual auto remove_directory(
            RsComm& _comm,
            const resolved_path& _resolved,
            collInp_t& _inp) const -> int = 0;

        virtual auto sync(
            RsComm& _comm,
            const resolved_path& _resolved,
            int _opr_type) const -> int = 0;

        virtual auto open_dir(
            RsComm& _comm,
            dataObjInfo_t& _info) const -> int = 0;

        virtual auto read_dir(
            RsComm& _comm,
            const dataObjInfo_t& _info,
            int _l3_desc_inx,
            rodsDirent_t** _out) const -> int = 0;

        virtual auto close_dir(
            RsComm& _comm,
            const dataObjInfo_t& _info,
            int _l3_desc_inx) const -> int = 0;

        virtual auto stat_entry(
            RsComm& _comm,
            dataObjInfo_t& _info,
            rodsStat_t** _out) const -> int = 0;
    };

    /// Driver for standard catalog-backed collections (`NO_SPEC_COLL`).
    class CatalogCollectionDriver final : public collection_driver
    {
      public:
        auto stat_path(
            RsComm& _comm,
            const resolved_path& _resolved,
            rodsObjStat_t** _out) const -> int override;

        auto create_directory(
            RsComm& _comm,
            const resolved_path& _resolved,
            collInp_t& _inp) const -> int override;

        auto remove_directory(
            RsComm& _comm,
            const resolved_path& _resolved,
            collInp_t& _inp) const -> int override;

        auto sync(
            RsComm& _comm,
            const resolved_path& _resolved,
            int _opr_type) const -> int override;

        auto open_dir(
            RsComm& _comm,
            dataObjInfo_t& _info) const -> int override;

        auto read_dir(
            RsComm& _comm,
            const dataObjInfo_t& _info,
            int _l3_desc_inx,
            rodsDirent_t** _out) const -> int override;

        auto close_dir(
            RsComm& _comm,
            const dataObjInfo_t& _info,
            int _l3_desc_inx) const -> int override;

        auto stat_entry(
            RsComm& _comm,
            dataObjInfo_t& _info,
            rodsStat_t** _out) const -> int override;
    };

    /// Driver for mounted physical directory collections (`MOUNTED_COLL`).
    class MountedCollectionDriver : public collection_driver
    {
      public:
        auto stat_path(
            RsComm& _comm,
            const resolved_path& _resolved,
            rodsObjStat_t** _out) const -> int override;

        auto create_directory(
            RsComm& _comm,
            const resolved_path& _resolved,
            collInp_t& _inp) const -> int override;

        auto remove_directory(
            RsComm& _comm,
            const resolved_path& _resolved,
            collInp_t& _inp) const -> int override;

        auto sync(
            RsComm& _comm,
            const resolved_path& _resolved,
            int _opr_type) const -> int override;

        auto open_dir(
            RsComm& _comm,
            dataObjInfo_t& _info) const -> int override;

        auto read_dir(
            RsComm& _comm,
            const dataObjInfo_t& _info,
            int _l3_desc_inx,
            rodsDirent_t** _out) const -> int override;

        auto close_dir(
            RsComm& _comm,
            const dataObjInfo_t& _info,
            int _l3_desc_inx) const -> int override;

        auto stat_entry(
            RsComm& _comm,
            dataObjInfo_t& _info,
            rodsStat_t** _out) const -> int override;
    };

    /// Driver for mounted structured archive collections (`STRUCT_FILE_COLL` / `tar`),
    /// composing `MountedCollectionDriver` directory semantics with `tar` staging/syncing.
    class ArchiveCollectionDriver final : public collection_driver
    {
      public:
        auto stat_path(
            RsComm& _comm,
            const resolved_path& _resolved,
            rodsObjStat_t** _out) const -> int override;

        auto create_directory(
            RsComm& _comm,
            const resolved_path& _resolved,
            collInp_t& _inp) const -> int override;

        auto remove_directory(
            RsComm& _comm,
            const resolved_path& _resolved,
            collInp_t& _inp) const -> int override;

        auto sync(
            RsComm& _comm,
            const resolved_path& _resolved,
            int _opr_type) const -> int override;

        auto open_dir(
            RsComm& _comm,
            dataObjInfo_t& _info) const -> int override;

        auto read_dir(
            RsComm& _comm,
            const dataObjInfo_t& _info,
            int _l3_desc_inx,
            rodsDirent_t** _out) const -> int override;

        auto close_dir(
            RsComm& _comm,
            const dataObjInfo_t& _info,
            int _l3_desc_inx) const -> int override;

        auto stat_entry(
            RsComm& _comm,
            dataObjInfo_t& _info,
            rodsStat_t** _out) const -> int override;

      private:
        MountedCollectionDriver mounted_driver_{};
    };

    /// Returns the singleton `collection_driver` corresponding to \p _info.
    auto driver_for_collection_info(const collection_info_variant& _info) noexcept -> const collection_driver&;

    /// Returns the singleton `collection_driver` corresponding to a legacy \p _spec_coll pointer.
    auto driver_for_legacy_spec_coll(const specColl_t* _spec_coll) noexcept -> const collection_driver&;

    /// Lightweight value facade wrapping a resolved logical path and dispatching operations
    /// to its governing `collection_driver`.
    class collection_handle
    {
      public:
        explicit collection_handle(resolved_path _resolved)
            : resolved_{std::move(_resolved)}
            , driver_{&driver_for_collection_info(resolved_.info)}
        {
        }

        [[nodiscard]] auto resolved() const noexcept -> const resolved_path&
        {
            return resolved_;
        }

        [[nodiscard]] auto driver() const noexcept -> const collection_driver&
        {
            return *driver_;
        }

        [[nodiscard]] auto is_special() const noexcept -> bool
        {
            return !std::holds_alternative<catalog_collection_info>(resolved_.info);
        }

        [[nodiscard]] auto is_mounted() const noexcept -> bool
        {
            return std::holds_alternative<mounted_collection_info>(resolved_.info);
        }

        [[nodiscard]] auto is_archive() const noexcept -> bool
        {
            return std::holds_alternative<archive_collection_info>(resolved_.info);
        }

        [[nodiscard]] auto is_linked() const noexcept -> bool
        {
            return std::holds_alternative<linked_collection_info>(resolved_.info);
        }

        auto stat(RsComm& _comm, rodsObjStat_t** _out) const -> int
        {
            return driver_->stat_path(_comm, resolved_, _out);
        }

        auto create(RsComm& _comm, collInp_t& _inp) const -> int
        {
            return driver_->create_directory(_comm, resolved_, _inp);
        }

        auto remove(RsComm& _comm, collInp_t& _inp) const -> int
        {
            return driver_->remove_directory(_comm, resolved_, _inp);
        }

        auto sync(RsComm& _comm, int _opr_type) const -> int
        {
            return driver_->sync(_comm, resolved_, _opr_type);
        }

      private:
        resolved_path resolved_;
        const collection_driver* driver_;
    };

    /// Resolves \p _logical_path through any `LINKED_COLL` chain and constructs a `collection_handle`
    /// bound to the governing `CatalogCollectionDriver`, `MountedCollectionDriver`, or `ArchiveCollectionDriver`.
    auto resolve_collection_handle(
        RsComm& _comm,
        std::string_view _logical_path,
        link_resolution_mode _mode = link_resolution_mode::follow_links) -> std::pair<int, collection_handle>;

} // namespace irods::experimental::collections

#endif // IRODS_COLLECTION_DRIVER_HPP
