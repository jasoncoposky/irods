#ifndef IRODS_DB_FLAVOR_HPP
#define IRODS_DB_FLAVOR_HPP

#include "irods/icatDefines.h"

#include <array>
#include <cctype>
#include <string>
#include <string_view>

namespace irods::experimental::catalog
{
    struct db_flavor
    {
        std::string_view technology_name;

        // Sequence SQL templates
        std::string_view next_sequence_sql;
        std::string_view current_sequence_sql;
        std::string_view next_sequence_expr;
        std::string_view current_sequence_expr;

        // SQL function and syntax fragments
        std::string_view substring_fn;
        std::string_view length_fn;
        std::string_view cast_integer;
        std::string_view cast_bigint;
        std::string_view cast_integer_type;
        std::string_view cast_decimal_or_number;
        std::string_view from_dual;
        std::string_view escape_backslash;

        // Specific flavored queries
        std::string_view remove_unused_avus_sql;
        std::string_view quota_update_per_resc;
        std::string_view quota_over_sum_cast;
        std::string_view quota_over_param_cast;
        std::string_view resc_free_space_add;
        std::string_view resc_free_space_sub;
        std::string_view del_user_pw_expired_sql;
        std::string_view sel_user_pw_valid_pam_sql;
        std::string_view del_obj_access_recursive;
        std::string_view del_coll_access_recursive;
        std::string_view ins_obj_access_recursive;
        std::string_view ins_coll_access_recursive;
        std::string_view get_hier_resc_vault_template;
        std::string_view get_repl_list_leaf_bundles_template;

        // Driver behavioral flags
        bool lowercase_column_names;
        bool explicit_begin_required;
        bool supports_catalog_properties;
        bool row_offset_requires_cursor_skip;
    };

    inline constexpr std::array<db_flavor, 3> db_flavor_table = {{
        // DB_TYPE_POSTGRES = 1
        {
            .technology_name = "postgres",
            .next_sequence_sql = "select nextval('{}')",
            .current_sequence_sql = "select currval('{}')",
            .next_sequence_expr = "nextval('{}')",
            .current_sequence_expr = "currval('{}')",
            .substring_fn = "substring",
            .length_fn = "char_length(",
            .cast_integer = "cast(? as integer)",
            .cast_bigint = "cast(? as bigint)",
            .cast_integer_type = "integer",
            .cast_decimal_or_number = " as decimal)",
            .from_dual = "",
            .escape_backslash = "",
            .remove_unused_avus_sql =
                "delete from R_META_MAIN where meta_id in (select meta_id from R_META_MAIN except select meta_id from R_OBJT_METAMAP)",
            .quota_update_per_resc =
                "update R_QUOTA_MAIN set quota_over = quota_usage - quota_limit from R_QUOTA_USAGE where R_QUOTA_MAIN.user_id = R_QUOTA_USAGE.user_id and R_QUOTA_MAIN.resc_id = R_QUOTA_USAGE.resc_id",
            .quota_over_sum_cast = "cast('0' as bigint)",
            .quota_over_param_cast = "cast(? as bigint)",
            .resc_free_space_add =
                "update R_RESC_MAIN set free_space = cast(free_space as bigint) + cast(? as bigint), free_space_ts = ?, modify_ts=?, modify_ts_millis=? where resc_id=?",
            .resc_free_space_sub =
                "update R_RESC_MAIN set free_space = cast(free_space as bigint) - cast(? as bigint), free_space_ts = ?, modify_ts=?, modify_ts_millis=? where resc_id=?",
            .del_user_pw_expired_sql =
                "delete from R_USER_PASSWORD where pass_expiry_ts not like '9999%' and cast(pass_expiry_ts as integer)>=? and cast(pass_expiry_ts as integer)<=? and (cast(pass_expiry_ts as integer) + cast(modify_ts as integer) < ?)",
            .sel_user_pw_valid_pam_sql =
                "select rcat_password, modify_ts from R_USER_PASSWORD where user_id=? and pass_expiry_ts not like '9999%' and cast(pass_expiry_ts as integer) >= ? and cast (pass_expiry_ts as integer) <= ?",
            .del_obj_access_recursive =
                "delete from R_OBJT_ACCESS where user_id=? and object_id = ANY(ARRAY(select data_id from R_DATA_MAIN where coll_id in (select coll_id from R_MOD_ACCESS_TEMP1 where coll_name = ? or coll_name like ?)))",
            .del_coll_access_recursive =
                "delete from R_OBJT_ACCESS where user_id=? and object_id = ANY(ARRAY(select coll_id from R_MOD_ACCESS_TEMP1 where coll_name = ? or coll_name like ?))",
            .ins_obj_access_recursive =
                "insert into R_OBJT_ACCESS (object_id, user_id, access_type_id, create_ts, modify_ts)  (select distinct data_id, cast(? as bigint), (select token_id from R_TOKN_MAIN where token_namespace = 'access_type' and token_name = ?), ?, ? from R_DATA_MAIN where coll_id in (select coll_id from R_MOD_ACCESS_TEMP1 where coll_name = ? or coll_name like ?))",
            .ins_coll_access_recursive =
                "insert into R_OBJT_ACCESS (object_id, user_id, access_type_id, create_ts, modify_ts)  (select distinct coll_id, cast(? as bigint), (select token_id from R_TOKN_MAIN where token_namespace = 'access_type' and token_name = ?), ?, ? from R_COLL_MAIN where coll_id in (select coll_id from R_MOD_ACCESS_TEMP1 where coll_name = ? or coll_name like ?))",
            .get_hier_resc_vault_template =
                "select distinct data_id from R_DATA_MAIN where resc_hier like '{}' or resc_hier like '{}' or resc_hier like '{}' except ( select data_id from R_DATA_MAIN where resc_hier like '{}' or resc_hier like '{}' or resc_hier like '{}' ) limit {}",
            .get_repl_list_leaf_bundles_template =
                "select distinct data_id from R_DATA_MAIN where resc_id in ({0}) and modify_ts <= '{1}' except select data_id from R_DATA_MAIN where resc_id in ({2}) limit {3}",
            .lowercase_column_names = false,
            .explicit_begin_required = true,
            .supports_catalog_properties = true,
            .row_offset_requires_cursor_skip = false
        },
        // DB_TYPE_ORACLE = 2
        {
            .technology_name = "oracle",
            .next_sequence_sql = "select {}.nextval from DUAL",
            .current_sequence_sql = "select {}.currval from DUAL",
            .next_sequence_expr = "{}.nextval",
            .current_sequence_expr = "{}.currval",
            .substring_fn = "substr",
            .length_fn = "length(",
            .cast_integer = "cast(? as integer)",
            .cast_bigint = "cast(? as integer)",
            .cast_integer_type = "integer",
            .cast_decimal_or_number = " as number)",
            .from_dual = " from DUAL",
            .escape_backslash = " ESCAPE '\\'",
            .remove_unused_avus_sql =
                "delete from R_META_MAIN where meta_id in (select meta_id from R_META_MAIN minus select meta_id from R_OBJT_METAMAP)",
            .quota_update_per_resc =
                "update R_QUOTA_MAIN set quota_over = (select distinct R_QUOTA_USAGE.quota_usage - R_QUOTA_MAIN.quota_limit from R_QUOTA_USAGE where R_QUOTA_MAIN.user_id = R_QUOTA_USAGE.user_id and R_QUOTA_MAIN.resc_id = R_QUOTA_USAGE.resc_id) where exists (select 1 from R_QUOTA_USAGE where R_QUOTA_MAIN.user_id = R_QUOTA_USAGE.user_id and R_QUOTA_MAIN.resc_id = R_QUOTA_USAGE.resc_id)",
            .quota_over_sum_cast = "cast('0' as integer)",
            .quota_over_param_cast = "cast(? as integer)",
            .resc_free_space_add =
                "update R_RESC_MAIN set free_space = cast(free_space as integer) + cast(? as integer), free_space_ts = ?, modify_ts=?, modify_ts_millis=? where resc_id=?",
            .resc_free_space_sub =
                "update R_RESC_MAIN set free_space = cast(free_space as integer) - cast(? as integer), free_space_ts = ?, modify_ts=?, modify_ts_millis=? where resc_id=?",
            .del_user_pw_expired_sql =
                "delete from R_USER_PASSWORD where pass_expiry_ts not like '9999%' and cast(pass_expiry_ts as integer)>=? and cast(pass_expiry_ts as integer)<=? and (cast(pass_expiry_ts as integer) + cast(modify_ts as integer) < ?)",
            .sel_user_pw_valid_pam_sql =
                "select rcat_password, modify_ts from R_USER_PASSWORD where user_id=? and pass_expiry_ts not like '9999%' and cast(pass_expiry_ts as integer) >= ? and cast (pass_expiry_ts as integer) <= ?",
            .del_obj_access_recursive =
                "delete from R_OBJT_ACCESS where user_id=? and object_id = ANY (select data_id from R_DATA_MAIN where coll_id in (select coll_id from R_COLL_MAIN where coll_name = ? or coll_name like ? ESCAPE '\\'))",
            .del_coll_access_recursive =
                "delete from R_OBJT_ACCESS where user_id=? and object_id = ANY (select coll_id from R_COLL_MAIN where coll_name = ? or coll_name like ? ESCAPE '\\')",
            .ins_obj_access_recursive =
                "insert into R_OBJT_ACCESS (object_id, user_id, access_type_id, create_ts, modify_ts)  (select distinct data_id, cast(? as integer), (select token_id from R_TOKN_MAIN where token_namespace = 'access_type' and token_name = ?), ?, ? from R_DATA_MAIN where coll_id in (select coll_id from R_COLL_MAIN where coll_name = ? or coll_name like ? ESCAPE '\\'))",
            .ins_coll_access_recursive =
                "insert into R_OBJT_ACCESS (object_id, user_id, access_type_id, create_ts, modify_ts)  (select distinct coll_id, cast(? as integer), (select token_id from R_TOKN_MAIN where token_namespace = 'access_type' and token_name = ?), ?, ? from R_COLL_MAIN where coll_name = ? or coll_name like ? ESCAPE '\\'))",
            .get_hier_resc_vault_template =
                "select distinct data_id from R_DATA_MAIN where ( resc_hier like '{}' or resc_hier like '{}' or resc_hier like '{}' ) and data_id not in ( select data_id from R_DATA_MAIN where resc_hier like '{}' or resc_hier like '{}' or resc_hier like '{}' ) and rownum < {}",
            .get_repl_list_leaf_bundles_template =
                "select data_id from (select distinct data_id from R_DATA_MAIN where data_id in (select data_id from R_DATA_MAIN where resc_id in ({0})) and data_id not in (select data_id from R_DATA_MAIN where resc_id in ({2})) and modify_ts <= '{1}') where rownum <= {3}",
            .lowercase_column_names = true,
            .explicit_begin_required = false,
            .supports_catalog_properties = false,
            .row_offset_requires_cursor_skip = true
        },
        // DB_TYPE_MYSQL = 3
        {
            .technology_name = "mysql",
            .next_sequence_sql = "select {}_nextval()",
            .current_sequence_sql = "select {}_currval()",
            .next_sequence_expr = "{}_nextval()",
            .current_sequence_expr = "{}_currval()",
            .substring_fn = "substring",
            .length_fn = "char_length(",
            .cast_integer = "cast(? as signed integer)",
            .cast_bigint = "?",
            .cast_integer_type = "signed integer",
            .cast_decimal_or_number = " as decimal)",
            .from_dual = " from DUAL",
            .escape_backslash = "",
            .remove_unused_avus_sql =
                "delete from R_META_MAIN where meta_id not in (select meta_id from R_OBJT_METAMAP)",
            .quota_update_per_resc =
                "update R_QUOTA_MAIN, R_QUOTA_USAGE set R_QUOTA_MAIN.quota_over = R_QUOTA_USAGE.quota_usage - R_QUOTA_MAIN.quota_limit where R_QUOTA_MAIN.user_id = R_QUOTA_USAGE.user_id and R_QUOTA_MAIN.resc_id = R_QUOTA_USAGE.resc_id",
            .quota_over_sum_cast = "'0'",
            .quota_over_param_cast = "?",
            .resc_free_space_add =
                "update R_RESC_MAIN set free_space = free_space + ?, free_space_ts = ?, modify_ts=?, modify_ts_millis=? where resc_id=?",
            .resc_free_space_sub =
                "update R_RESC_MAIN set free_space = free_space - ?, free_space_ts = ?, modify_ts=?, modify_ts_millis=? where resc_id=?",
            .del_user_pw_expired_sql =
                "delete from R_USER_PASSWORD where pass_expiry_ts not like '9999%' and cast(pass_expiry_ts as signed integer)>=? and cast(pass_expiry_ts as signed integer)<=? and (cast(pass_expiry_ts as signed integer) + cast(modify_ts as signed integer) < ?)",
            .sel_user_pw_valid_pam_sql =
                "select rcat_password, modify_ts from R_USER_PASSWORD where user_id=? and pass_expiry_ts not like '9999%' and cast(pass_expiry_ts as signed integer) >= ? and cast (pass_expiry_ts as signed integer) <= ?",
            .del_obj_access_recursive =
                "delete from R_OBJT_ACCESS where user_id=? and object_id = ANY (select data_id from R_DATA_MAIN where coll_id in (select coll_id from R_COLL_MAIN where coll_name = ? or coll_name like ?))",
            .del_coll_access_recursive =
                "delete from R_OBJT_ACCESS where user_id=? and object_id = ANY (select coll_id from R_COLL_MAIN where coll_name = ? or coll_name like ?)",
            .ins_obj_access_recursive =
                "insert into R_OBJT_ACCESS (object_id, user_id, access_type_id, create_ts, modify_ts)  (select distinct data_id, ?, (select token_id from R_TOKN_MAIN where token_namespace = 'access_type' and token_name = ?), ?, ? from R_DATA_MAIN where coll_id in (select coll_id from R_COLL_MAIN where coll_name = ? or coll_name like ?))",
            .ins_coll_access_recursive =
                "insert into R_OBJT_ACCESS (object_id, user_id, access_type_id, create_ts, modify_ts)  (select distinct coll_id, ?, (select token_id from R_TOKN_MAIN where token_namespace = 'access_type' and token_name = ?), ?, ? from R_COLL_MAIN where coll_id in (select coll_id from R_COLL_MAIN where coll_name = ? or coll_name like ?))",
            .get_hier_resc_vault_template =
                "select distinct data_id from R_DATA_MAIN where ( resc_hier like '{}' or resc_hier like '{}' or resc_hier like '{}' ) and data_id not in ( select data_id from R_DATA_MAIN where resc_hier like '{}' or resc_hier like '{}' or resc_hier like '{}' ) limit {};",
            .get_repl_list_leaf_bundles_template =
                "select distinct data_id from R_DATA_MAIN where resc_id in ({0}) and data_id not in ( select data_id from R_DATA_MAIN where resc_id in ({2}) ) and modify_ts <= '{1}' limit {3}",
            .lowercase_column_names = false,
            .explicit_begin_required = true,
            .supports_catalog_properties = false,
            .row_offset_requires_cursor_skip = false
        }
    }};

    [[nodiscard]] inline auto get_db_flavor(int _db_type) noexcept -> const db_flavor&
    {
        if (_db_type >= DB_TYPE_POSTGRES && _db_type <= DB_TYPE_MYSQL) {
            return db_flavor_table[_db_type - 1];
        }
        return db_flavor_table[0]; // fallback to Postgres
    }

    [[nodiscard]] inline auto get_db_type_from_name(std::string_view _name) noexcept -> int
    {
        std::string lower;
        lower.reserve(_name.size());
        for (const char c : _name) {
            lower.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
        }

        if (lower == "mysql" || lower.find("mysql") != std::string::npos || lower.find("mariadb") != std::string::npos) {
            return DB_TYPE_MYSQL;
        }
        if (lower == "oracle" || lower.find("oracle") != std::string::npos) {
            return DB_TYPE_ORACLE;
        }
        return DB_TYPE_POSTGRES;
    }
} // namespace irods::experimental::catalog

#endif // IRODS_DB_FLAVOR_HPP
