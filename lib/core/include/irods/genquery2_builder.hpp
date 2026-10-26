#ifndef IRODS_GENQUERY2_BUILDER_HPP
#define IRODS_GENQUERY2_BUILDER_HPP

#include "irods/genquery2_ast_types.hpp"

#include <charconv>
#include <cstdlib>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

namespace irods::experimental::genquery2
{
    namespace detail
    {
        inline auto append_escaped_literal(std::string& _out, std::string_view _val) -> void
        {
            _out.push_back('\'');
            if (_val.find('\'') == std::string_view::npos) {
                _out.append(_val);
            }
            else {
                for (const char c : _val) {
                    if (c == '\'') {
                        _out.append("''");
                    }
                    else {
                        _out.push_back(c);
                    }
                }
            }
            _out.push_back('\'');
        }

        inline auto append_column(std::string& _out, const column& _col) -> void
        {
            if (_col.type_name.empty()) {
                _out.append(_col.name);
            }
            else {
                _out.append("cast(");
                _out.append(_col.name);
                _out.append(" as ");
                _out.append(_col.type_name);
                _out.push_back(')');
            }
        }

        inline auto append_function(std::string& _out, const function& _fn) -> void
        {
            _out.append(_fn.name);
            _out.push_back('(');
            if (_fn.distinct) {
                _out.append("distinct ");
            }
            for (std::size_t i = 0; i < _fn.arguments.size(); ++i) {
                if (i > 0) {
                    _out.append(", ");
                }
                const auto& arg = _fn.arguments[i];
                if (std::holds_alternative<std::string>(arg)) {
                    append_escaped_literal(_out, std::get<std::string>(arg));
                }
                else if (std::holds_alternative<column>(arg)) {
                    append_column(_out, std::get<column>(arg));
                }
                else if (std::holds_alternative<function>(arg)) {
                    append_function(_out, std::get<function>(arg));
                }
            }
            _out.push_back(')');
        }

        inline auto append_column_or_function(std::string& _out, const std::variant<column, function>& _v) -> void
        {
            if (std::holds_alternative<column>(_v)) {
                append_column(_out, std::get<column>(_v));
            }
            else {
                append_function(_out, std::get<function>(_v));
            }
        }

        inline auto append_condition_expression(std::string& _out, const condition_expression& _expr) -> void
        {
            struct visitor
            {
                std::string& out;
                explicit visitor(std::string& _o)
                    : out{_o}
                {
                }

                void operator()(const condition_equal& _e) const
                {
                    out.append(" = ");
                    append_escaped_literal(out, _e.string_literal);
                }

                void operator()(const condition_not_equal& _e) const
                {
                    out.append(" != ");
                    append_escaped_literal(out, _e.string_literal);
                }

                void operator()(const condition_less_than& _e) const
                {
                    out.append(" < ");
                    append_escaped_literal(out, _e.string_literal);
                }

                void operator()(const condition_less_than_or_equal_to& _e) const
                {
                    out.append(" <= ");
                    append_escaped_literal(out, _e.string_literal);
                }

                void operator()(const condition_greater_than& _e) const
                {
                    out.append(" > ");
                    append_escaped_literal(out, _e.string_literal);
                }

                void operator()(const condition_greater_than_or_equal_to& _e) const
                {
                    out.append(" >= ");
                    append_escaped_literal(out, _e.string_literal);
                }

                void operator()(const condition_like& _e) const
                {
                    out.append(" like ");
                    append_escaped_literal(out, _e.string_literal);
                }

                void operator()(const condition_in& _e) const
                {
                    out.append(" in (");
                    for (std::size_t i = 0; i < _e.list_of_string_literals.size(); ++i) {
                        if (i > 0) {
                            out.append(", ");
                        }
                        append_escaped_literal(out, _e.list_of_string_literals[i]);
                    }
                    out.push_back(')');
                }

                void operator()(const condition_between& _e) const
                {
                    out.append(" between ");
                    append_escaped_literal(out, _e.low);
                    out.append(" and ");
                    append_escaped_literal(out, _e.high);
                }

                void operator()(const condition_is_null&) const
                {
                    out.append(" is null");
                }

                void operator()(const condition_is_not_null&) const
                {
                    out.append(" is not null");
                }

                void operator()(const condition_operator_not& _e) const
                {
                    out.append(" not");
                    std::visit(*this, _e.expression);
                }
            };

            std::visit(visitor{_out}, _expr);
        }

        inline auto append_conditions(std::string& _out, const conditions& _conds) -> void
        {
            struct visitor
            {
                std::string& out;
                explicit visitor(std::string& _o)
                    : out{_o}
                {
                }

                void operator()(const condition& _c) const
                {
                    append_column_or_function(out, _c.lhs);
                    append_condition_expression(out, _c.expression);
                }

                void operator()(const logical_and& _a) const
                {
                    out.append(" and ");
                    append_conditions(out, _a.condition);
                }

                void operator()(const logical_or& _o) const
                {
                    out.append(" or ");
                    append_conditions(out, _o.condition);
                }

                void operator()(const logical_not& _n) const
                {
                    out.append("not ");
                    append_conditions(out, _n.condition);
                }

                void operator()(const logical_grouping& _g) const
                {
                    out.push_back('(');
                    append_conditions(out, _g.conditions);
                    out.push_back(')');
                }
            };

            visitor v{_out};
            for (const auto& cw : _conds) {
                std::visit(v, cw);
            }
        }
    } // namespace detail

    /// Serializes a GenQuery2 \p select AST into a canonical GenQuery2 DSL query string
    /// with automatic single-quote escaping on all literal operands.
    inline auto to_query_string(const select& _sel) -> std::string
    {
        std::string out;
        out.reserve(128);
        out.append("select ");
        if (_sel.distinct) {
            out.append("distinct ");
        }

        struct proj_visitor
        {
            std::string& out;
            explicit proj_visitor(std::string& _o)
                : out{_o}
            {
            }

            void operator()(const column& _c) const
            {
                detail::append_column(out, _c);
            }

            void operator()(const function& _f) const
            {
                detail::append_function(out, _f);
            }
        };

        proj_visitor pv{out};
        for (std::size_t i = 0; i < _sel.projections.size(); ++i) {
            if (i > 0) {
                out.append(", ");
            }
            std::visit(pv, _sel.projections[i]);
        }

        if (!_sel.conditions.empty()) {
            out.append(" where ");
            detail::append_conditions(out, _sel.conditions);
        }

        if (!_sel.group_by.expressions.empty()) {
            out.append(" group by ");
            for (std::size_t i = 0; i < _sel.group_by.expressions.size(); ++i) {
                if (i > 0) {
                    out.append(", ");
                }
                const auto& expr = _sel.group_by.expressions[i];
                if (std::holds_alternative<column>(expr)) {
                    detail::append_column(out, std::get<column>(expr));
                }
                else {
                    detail::append_function(out, std::get<function>(expr));
                }
            }
        }

        if (!_sel.order_by.sort_expressions.empty()) {
            out.append(" order by ");
            for (std::size_t i = 0; i < _sel.order_by.sort_expressions.size(); ++i) {
                if (i > 0) {
                    out.append(", ");
                }
                const auto& se = _sel.order_by.sort_expressions[i];
                detail::append_column_or_function(out, se.expr);
                out.append(se.ascending_order ? " asc" : " desc");
            }
        }

        if (!_sel.range.offset.empty() && _sel.range.offset != "0") {
            out.append(" offset ");
            out.append(_sel.range.offset);
        }

        if (!_sel.range.number_of_rows.empty()) {
            out.append(" fetch first ");
            out.append(_sel.range.number_of_rows);
            out.append(" rows only");
        }

        return out;
    }

    namespace builder
    {
        class condition_builder
        {
        public:
            condition_builder() = default;

            /* implicit */ condition_builder(conditions _conds)
                : conds_{std::move(_conds)}
            {
            }

            /* implicit */ condition_builder(condition _cond)
                : conds_{condition_wrapper{std::move(_cond)}}
            {
            }

            auto to_conditions() const & -> conditions
            {
                return conds_;
            }

            auto to_conditions() && -> conditions
            {
                return std::move(conds_);
            }

            auto group() const -> condition_builder
            {
                return condition_builder{conditions{logical_grouping{conds_}}};
            }

            auto operator&&(const condition_builder& _rhs) const -> condition_builder
            {
                conditions res = conds_;
                res.push_back(logical_and{_rhs.conds_});
                return condition_builder{std::move(res)};
            }

            auto operator||(const condition_builder& _rhs) const -> condition_builder
            {
                conditions res = conds_;
                res.push_back(logical_or{_rhs.conds_});
                return condition_builder{std::move(res)};
            }

            auto operator!() const -> condition_builder
            {
                return condition_builder{conditions{logical_not{conds_}}};
            }

        private:
            conditions conds_;
        };

        class col
        {
        public:
            explicit col(std::string_view _name)
                : name_{_name}
            {
            }

            explicit col(std::string_view _name, std::string_view _type_name)
                : name_{_name}
                , type_name_{_type_name}
            {
            }

            auto operator==(std::string_view _val) const -> condition_builder
            {
                return condition{make_col(), condition_equal{std::string{_val}}};
            }

            auto operator!=(std::string_view _val) const -> condition_builder
            {
                return condition{make_col(), condition_not_equal{std::string{_val}}};
            }

            auto operator<(std::string_view _val) const -> condition_builder
            {
                return condition{make_col(), condition_less_than{std::string{_val}}};
            }

            auto operator<=(std::string_view _val) const -> condition_builder
            {
                return condition{make_col(), condition_less_than_or_equal_to{std::string{_val}}};
            }

            auto operator>(std::string_view _val) const -> condition_builder
            {
                return condition{make_col(), condition_greater_than{std::string{_val}}};
            }

            auto operator>=(std::string_view _val) const -> condition_builder
            {
                return condition{make_col(), condition_greater_than_or_equal_to{std::string{_val}}};
            }

            auto like(std::string_view _pattern) const -> condition_builder
            {
                return condition{make_col(), condition_like{std::string{_pattern}}};
            }

            auto in(std::vector<std::string> _values) const -> condition_builder
            {
                return condition{make_col(), condition_in{std::move(_values)}};
            }

            /// Builds an `IN (...)` predicate containing all ancestor collection prefixes of \p _path
            /// (including `"/"` and \p _path itself), matching legacy GenQuery1 `parent_of` semantics.
            auto parent_of(std::string_view _path) const -> condition_builder
            {
                std::vector<std::string> prefixes;
                if (_path.empty()) {
                    prefixes.emplace_back();
                    return in(std::move(prefixes));
                }

                std::string current;
                current.reserve(_path.size());

                std::size_t pos = 0;
                if (_path.front() == '/') {
                    prefixes.emplace_back("/");
                    while (pos < _path.size() && _path[pos] == '/') {
                        ++pos;
                    }
                }

                while (pos < _path.size()) {
                    const auto next_sep = _path.find('/', pos);
                    const auto segment = (next_sep == std::string_view::npos)
                        ? _path.substr(pos)
                        : _path.substr(pos, next_sep - pos);

                    if (!segment.empty()) {
                        if (_path.front() == '/' || !current.empty()) {
                            current.push_back('/');
                        }
                        current.append(segment);
                        prefixes.push_back(current);
                    }

                    if (next_sep == std::string_view::npos) {
                        break;
                    }
                    pos = next_sep + 1;
                    while (pos < _path.size() && _path[pos] == '/') {
                        ++pos;
                    }
                }

                return in(std::move(prefixes));
            }

            auto between(std::string_view _low, std::string_view _high) const -> condition_builder
            {
                return condition{make_col(), condition_between{std::string{_low}, std::string{_high}}};
            }

            auto is_null() const -> condition_builder
            {
                return condition{make_col(), condition_is_null{}};
            }

            auto is_not_null() const -> condition_builder
            {
                return condition{make_col(), condition_is_not_null{}};
            }

        private:
            auto make_col() const -> column
            {
                if (type_name_.empty()) {
                    return column{std::string{name_}};
                }
                return column{std::string{name_}, std::string{type_name_}};
            }

            std::string_view name_;
            std::string_view type_name_;
        };

        class insert_builder
        {
        public:
            explicit insert_builder(std::string_view _entity)
            {
                ins_.target_entity = _entity;
            }

            auto set(std::string_view _col_name, std::string_view _val) -> insert_builder&
            {
                ins_.assignments.emplace_back(std::string{_col_name}, std::string{_val});
                return *this;
            }

            auto build() const & -> insert
            {
                return ins_;
            }

            auto build() && -> insert
            {
                return std::move(ins_);
            }

        private:
            insert ins_;
        };

        class update_builder
        {
        public:
            explicit update_builder(std::string_view _entity)
            {
                upd_.target_entity = _entity;
            }

            auto set(std::string_view _col_name, std::string_view _val) -> update_builder&
            {
                upd_.assignments.emplace_back(std::string{_col_name}, std::string{_val});
                return *this;
            }

            auto where(condition_builder _cond) -> update_builder&
            {
                upd_.where_conditions = std::move(_cond).to_conditions();
                return *this;
            }

            auto build() const & -> update
            {
                return upd_;
            }

            auto build() && -> update
            {
                return std::move(upd_);
            }

        private:
            update upd_;
        };

        class remove_builder
        {
        public:
            explicit remove_builder(std::string_view _entity)
            {
                rem_.target_entity = _entity;
            }

            auto where(condition_builder _cond) -> remove_builder&
            {
                rem_.where_conditions = std::move(_cond).to_conditions();
                return *this;
            }

            auto build() const & -> remove
            {
                return rem_;
            }

            auto build() && -> remove
            {
                return std::move(rem_);
            }

        private:
            remove rem_;
        };

        class select_builder
        {
        public:
            select_builder() = default;

            explicit select_builder(std::vector<std::string> _cols)
            {
                sel_.projections.reserve(_cols.size());
                for (auto&& c : _cols) {
                    sel_.projections.push_back(column{std::move(c)});
                }
            }

            auto project(std::string _col) -> select_builder&
            {
                sel_.projections.push_back(column{std::move(_col)});
                return *this;
            }

            auto project(column _col) -> select_builder&
            {
                sel_.projections.push_back(std::move(_col));
                return *this;
            }

            auto project(function _func) -> select_builder&
            {
                sel_.projections.push_back(std::move(_func));
                return *this;
            }

            auto from(std::string_view _entity) -> select_builder&
            {
                sel_.from_entity = std::string{_entity};
                return *this;
            }

            auto distinct(bool _d = true) -> select_builder&
            {
                sel_.distinct = _d;
                return *this;
            }

            auto where(condition_builder _cond) -> select_builder&
            {
                sel_.conditions = std::move(_cond).to_conditions();
                return *this;
            }

            auto group_by(std::vector<std::string> _cols) -> select_builder&
            {
                for (auto&& c : _cols) {
                    sel_.group_by.expressions.push_back(column{std::move(c)});
                }
                return *this;
            }

            auto order_by(std::string_view _col, bool _asc = true) -> select_builder&
            {
                sel_.order_by.sort_expressions.push_back(sort_expression{column{std::string{_col}}, _asc});
                return *this;
            }

            auto order_by(std::initializer_list<std::string_view> _cols, bool _asc = true) -> select_builder&
            {
                for (const auto c : _cols) {
                    sel_.order_by.sort_expressions.push_back(sort_expression{column{std::string{c}}, _asc});
                }
                return *this;
            }

            auto limit(std::string _number_of_rows, std::string _offset = "0") -> select_builder&
            {
                sel_.range.number_of_rows = std::move(_number_of_rows);
                sel_.range.offset = std::move(_offset);
                return *this;
            }

            auto limit(std::size_t _number_of_rows, std::size_t _offset = 0) -> select_builder&
            {
                sel_.range.number_of_rows = std::to_string(_number_of_rows);
                sel_.range.offset = std::to_string(_offset);
                return *this;
            }

            auto offset(std::size_t _offset) -> select_builder&
            {
                sel_.range.offset = std::to_string(_offset);
                return *this;
            }

            auto build() const & -> select
            {
                return sel_;
            }

            auto build() && -> select
            {
                return std::move(sel_);
            }

            auto to_query_string() const -> std::string
            {
                return ::irods::experimental::genquery2::to_query_string(sel_);
            }

        private:
            select sel_;
        };

        inline auto count(std::string _col) -> function
        {
            return function{"count", {column{std::move(_col)}}};
        }

        inline auto count_distinct(std::string _col) -> function
        {
            return function{"count", {column{std::move(_col)}}, true};
        }

        inline auto max(std::string _col) -> function
        {
            return function{"max", {column{std::move(_col)}}};
        }

        inline auto min(std::string _col) -> function
        {
            return function{"min", {column{std::move(_col)}}};
        }

        inline auto sum(std::string _col) -> function
        {
            return function{"sum", {column{std::move(_col)}}};
        }

        inline auto insert_into(std::string_view _entity) -> insert_builder
        {
            return insert_builder{_entity};
        }

        inline auto update(std::string_view _entity) -> update_builder
        {
            return update_builder{_entity};
        }

        inline auto remove_from(std::string_view _entity) -> remove_builder
        {
            return remove_builder{_entity};
        }

        inline auto select(std::vector<std::string> _cols = {}) -> select_builder
        {
            return select_builder{std::move(_cols)};
        }

        inline auto select(function _func) -> select_builder
        {
            select_builder sb;
            sb.project(std::move(_func));
            return sb;
        }

        inline auto group(const condition_builder& _cb) -> condition_builder
        {
            return _cb.group();
        }
    } // namespace builder
} // namespace irods::experimental::genquery2

#if __has_include("irods/genquery2.h") && __has_include(<nlohmann/json.hpp>)
#  include "irods/genquery2.h"
#  include "irods/irods_at_scope_exit.hpp"
#  include "irods/irods_exception.hpp"
#  include "irods/rodsErrorTable.h"
#  if __has_include("irods/rodsGenQuery.h")
#    include "irods/rodsGenQuery.h"
#  endif
#  if __has_include("buffer.hpp")
#    include "buffer.hpp"
#  endif
#  include <algorithm>
#  include <cstring>
#  include <initializer_list>
#  include <nlohmann/json.hpp>

struct RcComm;
struct RsComm;

namespace irods::experimental::genquery2
{
#  if __has_include("irods/rodsGenQuery.h")
    /// Populates a legacy `GenQueryOut*` C structure from a GenQuery2 JSON row array and a span of
    /// `COL_*` attribute indices. Returns `CAT_NO_ROWS_FOUND` when \p _rows is empty.
    inline auto rows_to_genquery_out(const nlohmann::json& _rows,
                                     std::span<const int> _col_indices,
                                     GenQueryOut** _out,
                                     int _continue_inx = 0) -> int
    {
        if (!_out) {
            return USER__NULL_INPUT_ERR;
        }

        if (*_out) {
            for (int i = 0; i < (*_out)->attriCnt; ++i) {
                std::free((*_out)->sqlResult[i].value);
            }
            std::free(*_out);
            *_out = nullptr;
        }

        if (!_rows.is_array() || _rows.empty()) {
            return CAT_NO_ROWS_FOUND;
        }

        const auto num_cols = _col_indices.size();
        if (num_cols > MAX_SQL_ATTR) {
            return SYS_INVALID_INPUT_PARAM;
        }

        auto* res = static_cast<GenQueryOut*>(std::calloc(1, sizeof(GenQueryOut)));
        if (!res) {
            return SYS_MALLOC_ERR;
        }

        res->rowCnt = static_cast<int>(_rows.size());
        res->attriCnt = static_cast<int>(num_cols);
        res->continueInx = _continue_inx;
        res->totalRowCount = res->rowCnt;

        std::size_t col_idx = 0;
        for (const int attri_inx : _col_indices) {
            std::size_t max_len = 50;
            for (const auto& row : _rows) {
                if (col_idx < row.size() && row[col_idx].is_string()) {
                    const auto& s = row[col_idx].get_ref<const std::string&>();
                    max_len = std::max(max_len, s.size() + 1);
                }
            }

            res->sqlResult[col_idx].attriInx = attri_inx;
            res->sqlResult[col_idx].len = static_cast<int>(max_len);
            res->sqlResult[col_idx].value = static_cast<char*>(std::calloc(_rows.size(), max_len));
            if (!res->sqlResult[col_idx].value) {
                for (std::size_t j = 0; j < col_idx; ++j) {
                    std::free(res->sqlResult[j].value);
                }
                std::free(res);
                return SYS_MALLOC_ERR;
            }

            for (std::size_t r = 0; r < _rows.size(); ++r) {
                if (col_idx < _rows[r].size() && _rows[r][col_idx].is_string()) {
                    const auto& s = _rows[r][col_idx].get_ref<const std::string&>();
                    std::memcpy(&res->sqlResult[col_idx].value[r * max_len], s.data(), s.size());
                }
            }

            ++col_idx;
        }

        *_out = res;
        return 0;
    }

    inline auto rows_to_genquery_out(const nlohmann::json& _rows,
                                     std::initializer_list<int> _col_indices,
                                     GenQueryOut** _out,
                                     int _continue_inx = 0) -> int
    {
        return rows_to_genquery_out(
            _rows, std::span<const int>{_col_indices.begin(), _col_indices.size()}, _out, _continue_inx);
    }

    inline auto rows_to_genquery_out(const nlohmann::json& _rows,
                                     std::span<const int> _col_indices,
                                     GenQueryOut*& _out,
                                     int _continue_inx = 0) -> int
    {
        return rows_to_genquery_out(_rows, _col_indices, &_out, _continue_inx);
    }

    inline auto rows_to_genquery_out(const nlohmann::json& _rows,
                                     std::initializer_list<int> _col_indices,
                                     GenQueryOut*& _out,
                                     int _continue_inx = 0) -> int
    {
        return rows_to_genquery_out(
            _rows, std::span<const int>{_col_indices.begin(), _col_indices.size()}, &_out, _continue_inx);
    }

    inline auto rows_to_genquery_out(const std::string& _json_rows,
                                     std::span<const int> _col_indices,
                                     GenQueryOut** _out,
                                     int _continue_inx = 0) -> int
    {
        nlohmann::json rows;
        try {
            rows = nlohmann::json::parse(_json_rows);
        }
        catch (...) {
            return SYS_LIBRARY_ERROR;
        }
        return rows_to_genquery_out(rows, _col_indices, _out, _continue_inx);
    }

    inline auto rows_to_genquery_out(const std::string& _json_rows,
                                     std::span<const int> _col_indices,
                                     GenQueryOut*& _out,
                                     int _continue_inx = 0) -> int
    {
        return rows_to_genquery_out(_json_rows, _col_indices, &_out, _continue_inx);
    }

#  endif

#  if __has_include("buffer.hpp")
    namespace detail
    {
        inline auto resolve_rows_array_offset(const lite3cpp::Buffer& _rows_buf) noexcept -> std::optional<std::size_t>
        {
            if (_rows_buf.size() < sizeof(lite3cpp::PackedNodeLayout)) {
                return std::nullopt;
            }
            const lite3cpp::NodeView root_nv{reinterpret_cast<const lite3cpp::PackedNodeLayout*>(_rows_buf.data())};
            if (root_nv.type() == lite3cpp::Type::Array) {
                return std::size_t{0};
            }
            if (root_nv.type() == lite3cpp::Type::Object) {
                try {
                    if (_rows_buf.get_type(0, "rows") == lite3cpp::Type::Array) {
                        return _rows_buf.get_arr(0, "rows");
                    }
                }
                catch (...) {
                    return std::nullopt;
                }
            }
            return std::nullopt;
        }

        inline auto format_cell_value(const lite3cpp::Buffer& _buf,
                                      std::size_t _arr_ofs,
                                      std::uint32_t _col_idx,
                                      char (&_scratch)[64]) noexcept -> std::string_view
        {
            try {
                const auto t = _buf.arr_get_type(_arr_ofs, _col_idx);
                if (t == lite3cpp::Type::String) {
                    return _buf.arr_get_str(_arr_ofs, _col_idx);
                }
                if (t == lite3cpp::Type::Int64) {
                    const auto v = _buf.arr_get_i64(_arr_ofs, _col_idx);
                    const auto res = fmt::format_to_n(_scratch, sizeof(_scratch) - 1, "{}", v);
                    *res.out = '\0';
                    return std::string_view{_scratch, static_cast<std::size_t>(res.out - _scratch)};
                }
                if (t == lite3cpp::Type::Float64) {
                    const auto v = _buf.arr_get_f64(_arr_ofs, _col_idx);
                    const auto res = fmt::format_to_n(_scratch, sizeof(_scratch) - 1, "{}", v);
                    *res.out = '\0';
                    return std::string_view{_scratch, static_cast<std::size_t>(res.out - _scratch)};
                }
                if (t == lite3cpp::Type::Bool) {
                    return _buf.arr_get_bool(_arr_ofs, _col_idx) ? "1" : "0";
                }
            }
            catch (...) {
            }
            return {};
        }
    } // namespace detail

    /// Returns the number of rows in a `lite3cpp::Buffer` GenQuery2 result buffer
    /// (either a top-level `Array` or an `Object` with a `"rows"` `Array`).
    inline auto rows_buffer_get_row_count(const lite3cpp::Buffer& _rows_buf) noexcept -> std::size_t
    {
        const auto arr_ofs_opt = detail::resolve_rows_array_offset(_rows_buf);
        if (!arr_ofs_opt || *arr_ofs_opt + sizeof(lite3cpp::PackedNodeLayout) > _rows_buf.size()) {
            return 0;
        }
        const lite3cpp::NodeView arr_nv{
            reinterpret_cast<const lite3cpp::PackedNodeLayout*>(_rows_buf.data() + *arr_ofs_opt)};
        if (arr_nv.type() != lite3cpp::Type::Array) {
            return 0;
        }
        return arr_nv.size();
    }

    /// Zero-copy random-access view over a `lite3cpp::Buffer` GenQuery2 result row array.
    class rows_buffer_view
    {
    public:
        struct row_proj_info
        {
            std::size_t ofs = 0;
            std::uint32_t count = 0;
        };

        explicit rows_buffer_view(const lite3cpp::Buffer& _buf)
            : buf_{&_buf}
        {
            const auto rows_arr_ofs_opt = detail::resolve_rows_array_offset(_buf);
            if (!rows_arr_ofs_opt) {
                return;
            }
            const std::size_t rows_arr_ofs = *rows_arr_ofs_opt;
            const std::size_t total_rows = rows_buffer_get_row_count(_buf);
            rows_.resize(total_rows);
            for (std::size_t r = 0; r < total_rows; ++r) {
                const auto elem_type = _buf.arr_get_type(rows_arr_ofs, static_cast<std::uint32_t>(r));
                std::size_t proj_ofs = 0;
                if (elem_type == lite3cpp::Type::Object) {
                    const std::size_t row_ofs = _buf.arr_get_obj(rows_arr_ofs, static_cast<std::uint32_t>(r));
                    if (_buf.get_type(row_ofs, "proj") == lite3cpp::Type::Array) {
                        proj_ofs = _buf.get_arr(row_ofs, "proj");
                    }
                }
                else if (elem_type == lite3cpp::Type::Array) {
                    proj_ofs = _buf.arr_get_arr(rows_arr_ofs, static_cast<std::uint32_t>(r));
                }

                if (proj_ofs != 0 && proj_ofs + sizeof(lite3cpp::PackedNodeLayout) <= _buf.size()) {
                    const lite3cpp::NodeView pnv{
                        reinterpret_cast<const lite3cpp::PackedNodeLayout*>(_buf.data() + proj_ofs)};
                    rows_[r] = row_proj_info{proj_ofs, pnv.size()};
                }
            }
        }

        [[nodiscard]] auto empty() const noexcept -> bool
        {
            return rows_.empty();
        }

        [[nodiscard]] auto size() const noexcept -> std::size_t
        {
            return rows_.size();
        }

        [[nodiscard]] auto col_count(std::size_t _row) const noexcept -> std::size_t
        {
            return _row < rows_.size() ? rows_[_row].count : 0;
        }

        [[nodiscard]] auto cell(std::size_t _row, std::size_t _col, char (&_scratch)[64]) const noexcept
            -> std::string_view
        {
            if (!buf_ || _row >= rows_.size() || rows_[_row].ofs == 0 || _col >= rows_[_row].count) {
                return {};
            }
            return detail::format_cell_value(*buf_, rows_[_row].ofs, static_cast<std::uint32_t>(_col), _scratch);
        }

        [[nodiscard]] auto cell_str(std::size_t _row, std::size_t _col) const -> std::string
        {
            char scratch[64]{};
            return std::string{cell(_row, _col, scratch)};
        }

        auto copy_cell(std::size_t _row, std::size_t _col, char* _dst, std::size_t _dst_cap) const noexcept
            -> std::size_t
        {
            if (!_dst || _dst_cap == 0) {
                return 0;
            }
            char scratch[64]{};
            const auto sv = cell(_row, _col, scratch);
            const std::size_t copy_len = std::min(sv.size(), _dst_cap - 1);
            if (copy_len > 0) {
                std::memcpy(_dst, sv.data(), copy_len);
            }
            _dst[copy_len] = '\0';
            return copy_len;
        }

        template <typename IntT>
        auto cell_int(std::size_t _row, std::size_t _col, IntT& _out) const noexcept -> bool
        {
            char scratch[64]{};
            const auto sv = cell(_row, _col, scratch);
            if (sv.empty()) {
                return false;
            }
            const auto [ptr, ec] = std::from_chars(sv.data(), sv.data() + sv.size(), _out);
            return ec == std::errc{} && ptr == sv.data() + sv.size();
        }

        [[nodiscard]] auto cell_strdup(std::size_t _row, std::size_t _col) const noexcept -> char*
        {
            char scratch[64]{};
            const auto sv = cell(_row, _col, scratch);
            auto* out = static_cast<char*>(std::malloc(sv.size() + 1));
            if (!out) {
                return nullptr;
            }
            if (!sv.empty()) {
                std::memcpy(out, sv.data(), sv.size());
            }
            out[sv.size()] = '\0';
            return out;
        }

        [[nodiscard]] auto to_json() const -> nlohmann::json
        {
            auto out = nlohmann::json::array();
            char scratch[64]{};
            for (std::size_t r = 0; r < rows_.size(); ++r) {
                auto jrow = nlohmann::json::array();
                for (std::uint32_t c = 0; c < rows_[r].count; ++c) {
                    const auto sv = detail::format_cell_value(*buf_, rows_[r].ofs, c, scratch);
                    jrow.push_back(std::string{sv});
                }
                out.push_back(std::move(jrow));
            }
            return out;
        }

        [[nodiscard]] auto rows_info() const noexcept -> std::span<const row_proj_info>
        {
            return rows_;
        }

    private:
        const lite3cpp::Buffer* buf_{nullptr};
        std::vector<row_proj_info> rows_;
    };

#    if __has_include("irods/rodsGenQuery.h")
    /// Populates a legacy `GenQueryOut*` C structure directly from a `lite3cpp::Buffer`
    /// of GenQuery2 result rows (either `{"proj": ["col0", ...]}` objects or `["col0", ...]` arrays)
    /// using zero-copy `std::string_view` slices without intermediate `std::string` or JSON allocations.
    inline auto rows_buffer_to_genquery_out(const lite3cpp::Buffer& _rows_buf,
                                            std::span<const int> _col_indices,
                                            GenQueryOut** _out,
                                            int _continue_inx = 0,
                                            std::size_t _max_rows = MAX_SQL_ROWS) -> int
    {
        if (!_out) {
            return USER__NULL_INPUT_ERR;
        }

        if (*_out) {
            for (int i = 0; i < (*_out)->attriCnt; ++i) {
                std::free((*_out)->sqlResult[i].value);
            }
            std::free(*_out);
            *_out = nullptr;
        }

        const rows_buffer_view view{_rows_buf};
        if (view.empty() || _max_rows == 0) {
            return CAT_NO_ROWS_FOUND;
        }

        const std::size_t row_count =
            std::min({view.size(), _max_rows, static_cast<std::size_t>(MAX_SQL_ROWS)});
        const auto num_cols = _col_indices.size();
        if (num_cols > MAX_SQL_ATTR) {
            return SYS_INVALID_INPUT_PARAM;
        }

        const auto proj_rows = view.rows_info();

        auto* res = static_cast<GenQueryOut*>(std::calloc(1, sizeof(GenQueryOut)));
        if (!res) {
            return SYS_MALLOC_ERR;
        }

        res->rowCnt = static_cast<int>(row_count);
        res->attriCnt = static_cast<int>(num_cols);
        res->continueInx = _continue_inx;
        res->totalRowCount = res->rowCnt;

        char scratch[64]{};
        std::size_t col_idx = 0;
        for (const int attri_inx : _col_indices) {
            std::size_t max_len = 50;
            const auto col_u32 = static_cast<std::uint32_t>(col_idx);
            for (std::size_t r = 0; r < row_count; ++r) {
                if (proj_rows[r].ofs != 0 && col_u32 < proj_rows[r].count) {
                    const auto sv = detail::format_cell_value(_rows_buf, proj_rows[r].ofs, col_u32, scratch);
                    max_len = std::max(max_len, sv.size() + 1);
                }
            }

            res->sqlResult[col_idx].attriInx = attri_inx;
            res->sqlResult[col_idx].len = static_cast<int>(max_len);
            res->sqlResult[col_idx].value = static_cast<char*>(std::calloc(row_count, max_len));
            if (!res->sqlResult[col_idx].value) {
                for (std::size_t j = 0; j < col_idx; ++j) {
                    std::free(res->sqlResult[j].value);
                }
                std::free(res);
                return SYS_MALLOC_ERR;
            }

            for (std::size_t r = 0; r < row_count; ++r) {
                if (proj_rows[r].ofs != 0 && col_u32 < proj_rows[r].count) {
                    const auto sv = detail::format_cell_value(_rows_buf, proj_rows[r].ofs, col_u32, scratch);
                    if (!sv.empty()) {
                        std::memcpy(&res->sqlResult[col_idx].value[r * max_len], sv.data(), sv.size());
                    }
                }
            }

            ++col_idx;
        }

        *_out = res;
        return 0;
    }

    inline auto rows_buffer_to_genquery_out(const lite3cpp::Buffer& _rows_buf,
                                            std::span<const int> _col_indices,
                                            GenQueryOut*& _out,
                                            int _continue_inx = 0,
                                            std::size_t _max_rows = MAX_SQL_ROWS) -> int
    {
        return rows_buffer_to_genquery_out(_rows_buf, _col_indices, &_out, _continue_inx, _max_rows);
    }

    inline auto rows_buffer_to_genquery_out(const lite3cpp::Buffer& _rows_buf,
                                            std::initializer_list<int> _col_indices,
                                            GenQueryOut** _out,
                                            int _continue_inx = 0,
                                            std::size_t _max_rows = MAX_SQL_ROWS) -> int
    {
        return rows_buffer_to_genquery_out(
            _rows_buf, std::span<const int>{_col_indices.begin(), _col_indices.size()}, _out, _continue_inx, _max_rows);
    }

    inline auto rows_buffer_to_genquery_out(const lite3cpp::Buffer& _rows_buf,
                                            std::initializer_list<int> _col_indices,
                                            GenQueryOut*& _out,
                                            int _continue_inx = 0,
                                            std::size_t _max_rows = MAX_SQL_ROWS) -> int
    {
        return rows_buffer_to_genquery_out(
            _rows_buf, std::span<const int>{_col_indices.begin(), _col_indices.size()}, &_out, _continue_inx, _max_rows);
    }
#    endif
#  endif
    int rs_genquery2(RsComm* _comm,
                     const select& _select,
                     const char* _zone,
                     char** _output);

#  if __has_include("buffer.hpp")
    int rs_genquery2_buffer(RsComm* _comm,
                            const select& _select,
                            const char* _zone,
                            lite3cpp::Buffer& _output);

    /// Zero-copy server-side GenQuery2 AST execution helper populating a `lite3cpp::Buffer` directly
    /// without JSON serialization or parsing.
    inline auto try_execute_buffer(RsComm& _comm,
                                   const select& _sel,
                                   lite3cpp::Buffer& _rows_buf,
                                   std::string_view _zone_hint = {}) -> int
    {
        std::string zone_buf;
        const char* zone_ptr = nullptr;
        if (!_zone_hint.empty()) {
            zone_buf.assign(_zone_hint);
            zone_ptr = zone_buf.c_str();
        }
        return rs_genquery2_buffer(&_comm, _sel, zone_ptr, _rows_buf);
    }

    inline auto try_execute_buffer(RsComm& _comm,
                                   const builder::select_builder& _sb,
                                   lite3cpp::Buffer& _rows_buf,
                                   std::string_view _zone_hint = {}) -> int
    {
        return try_execute_buffer(_comm, _sb.build(), _rows_buf, _zone_hint);
    }

    inline auto try_execute_buffer(RsComm& _comm,
                                   const select& _sel,
                                   std::string_view _zone_hint = {}) -> std::optional<lite3cpp::Buffer>
    {
        lite3cpp::Buffer rows_buf;
        if (try_execute_buffer(_comm, _sel, rows_buf, _zone_hint) < 0) {
            return std::nullopt;
        }
        return rows_buf;
    }

    inline auto try_execute_buffer(RsComm& _comm,
                                   const builder::select_builder& _sb,
                                   std::string_view _zone_hint = {}) -> std::optional<lite3cpp::Buffer>
    {
        return try_execute_buffer(_comm, _sb.build(), _zone_hint);
    }
#  endif

    /// Non-throwing GenQuery2 AST execution helper.
    /// On the server (`RsComm`), executes the in-memory `select` AST directly via the zero-copy
    /// `lite3cpp::Buffer` pipeline (`rs_genquery2_buffer`) without string serialization, Bison/Flex
    /// parsing, or JSON text round-tripping when connected to the local catalog provider.
    /// On the client (`RcComm`), serializes the AST via `to_query_string` and invokes `rc_genquery2`.
    template <typename RxComm>
    auto try_execute(RxComm& _comm,
                     const select& _sel,
                     nlohmann::json& _rows,
                     std::string_view _zone_hint = {}) -> int
    {
        if constexpr (std::is_same_v<std::remove_cv_t<RxComm>, RsComm>) {
#  if __has_include("buffer.hpp")
            lite3cpp::Buffer rows_buf;
            const int ec = try_execute_buffer(_comm, _sel, rows_buf, _zone_hint);
            if (ec < 0) {
                return ec;
            }
            try {
                _rows = rows_buffer_view{rows_buf}.to_json();
                return 0;
            }
            catch (...) {
                return SYS_LIBRARY_ERROR;
            }
#  else
            char* output = nullptr;
            at_scope_exit free_output{[&output] { std::free(output); }};

            std::string zone_buf;
            const char* zone_ptr = nullptr;
            if (!_zone_hint.empty()) {
                zone_buf.assign(_zone_hint);
                zone_ptr = zone_buf.c_str();
            }

            const int ec = rs_genquery2(&_comm, _sel, zone_ptr, &output);
            if (ec < 0) {
                return ec;
            }
            if (!output) {
                _rows = nlohmann::json::array();
                return 0;
            }
            try {
                _rows = nlohmann::json::parse(output);
            }
            catch (...) {
                return SYS_LIBRARY_ERROR;
            }
            return _rows.is_array() ? 0 : SYS_LIBRARY_ERROR;
#  endif
        }
        else {
            char* output = nullptr;
            at_scope_exit free_output{[&output] { std::free(output); }};

            std::string zone_buf;
            const char* zone_ptr = nullptr;
            if (!_zone_hint.empty()) {
                zone_buf.assign(_zone_hint);
                zone_ptr = zone_buf.c_str();
            }

            auto query_str = to_query_string(_sel);
            Genquery2Input input{};
            input.query_string = query_str.data();
            input.zone = const_cast<char*>(zone_ptr);
            const int ec = rc_genquery2(&_comm, &input, &output);
            if (ec < 0) {
                return ec;
            }
            if (!output) {
                _rows = nlohmann::json::array();
                return 0;
            }
            try {
                _rows = nlohmann::json::parse(output);
            }
            catch (...) {
                return SYS_LIBRARY_ERROR;
            }
            return _rows.is_array() ? 0 : SYS_LIBRARY_ERROR;
        }
    }

    /// Non-throwing GenQuery2 AST execution helper returning `std::optional<nlohmann::json>`.
    template <typename RxComm>
    auto try_execute(RxComm& _comm,
                     const select& _sel,
                     std::string_view _zone_hint = {}) -> std::optional<nlohmann::json>
    {
        nlohmann::json rows;
        if (try_execute(_comm, _sel, rows, _zone_hint) < 0) {
            return std::nullopt;
        }
        return rows;
    }

    /// Throwing GenQuery2 AST execution helper.
    template <typename RxComm>
    auto execute(RxComm& _comm,
                 const select& _sel,
                 std::string_view _zone_hint = {}) -> nlohmann::json
    {
        nlohmann::json rows;
        if (const int ec = try_execute(_comm, _sel, rows, _zone_hint); ec < 0) {
            // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
            THROW(ec, "GenQuery2 AST execution failed");
        }
        return rows;
    }

    /// Convenience overload accepting a `builder::select_builder` directly.
    template <typename RxComm>
    auto try_execute(RxComm& _comm,
                     const builder::select_builder& _sb,
                     nlohmann::json& _rows,
                     std::string_view _zone_hint = {}) -> int
    {
        return try_execute(_comm, _sb.build(), _rows, _zone_hint);
    }

    /// Convenience overload accepting a `builder::select_builder` directly.
    template <typename RxComm>
    auto try_execute(RxComm& _comm,
                     const builder::select_builder& _sb,
                     std::string_view _zone_hint = {}) -> std::optional<nlohmann::json>
    {
        return try_execute(_comm, _sb.build(), _zone_hint);
    }

    /// Convenience overload accepting a `builder::select_builder` directly.
    template <typename RxComm>
    auto execute(RxComm& _comm,
                 const builder::select_builder& _sb,
                 std::string_view _zone_hint = {}) -> nlohmann::json
    {
        return execute(_comm, _sb.build(), _zone_hint);
    }
} // namespace irods::experimental::genquery2
#endif

#endif // IRODS_GENQUERY2_BUILDER_HPP
