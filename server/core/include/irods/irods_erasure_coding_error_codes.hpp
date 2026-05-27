#ifndef IRODS_ERASURE_CODING_ERROR_CODES_HPP
#define IRODS_ERASURE_CODING_ERROR_CODES_HPP

namespace irods::erasurecoding::error_codes {

    // --- High-Performance I/O Errors ---
    constexpr int WRITE_ERROR = -10001000;
    constexpr int READ_ERROR  = -10002000;
    constexpr int LSEEK_ERROR = -10003000;

    // --- Configuration & Lifecycle Errors ---
    constexpr int INVALID_PLUGIN_CONFIG = -10004000;
    constexpr int PIPELINE_CREATE_ERROR = -10005000;
    constexpr int FILE_STATE_NOT_FOUND  = -10006000;

    // --- Control Plane (L3KVG) Errors ---
    constexpr int L3KVG_COMM_ERROR      = -10007000;
    constexpr int L3KVG_SCHEMA_MISMATCH = -10008000;

    // --- Catalog Agnostic Hooks (Core) ---
    constexpr int CATALOG_VERSION_NOT_FOUND    = -10009000;
    constexpr int CATALOG_PLUGIN_RESOLVE_ERR   = -10010000;
    constexpr int CATALOG_TYPE_IDENTIFY_ERR    = -10011000;
    constexpr int CATALOG_INIT_FAILED          = -10012000;

} // namespace irods::erasurecoding::error_codes

#endif // IRODS_ERASURE_CODING_ERROR_CODES_HPP
