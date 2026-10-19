#ifndef IRODS_GENQUERY2_BUILDER_HPP
#define IRODS_GENQUERY2_BUILDER_HPP

#include "irods/genquery2_ast_types.hpp"

#include <cstdlib>
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
            _out.append(_col.name);
            if (!_col.type_name.empty()) {
                _out.push_back(':');
                _out.append(_col.type_name);
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
            struct visitor : boost::static_visitor<void>
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
                    boost::apply_visitor(*this, _e.expression);
                }
            };

            boost::apply_visitor(visitor{_out}, _expr);
        }

        inline auto append_conditions(std::string& _out, const conditions& _conds) -> void
        {
            struct visitor : boost::static_visitor<void>
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
                boost::apply_visitor(v, cw);
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

        struct proj_visitor : boost::static_visitor<void>
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
            boost::apply_visitor(pv, _sel.projections[i]);
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
#  include <nlohmann/json.hpp>

struct RcComm;
struct RsComm;

namespace irods::experimental::genquery2
{
    int rs_genquery2(RsComm* _comm,
                     const select& _select,
                     const char* _zone,
                     char** _output);

    /// Non-throwing GenQuery2 AST execution helper.
    /// On the server (`RsComm`), executes the in-memory `select` AST directly without string
    /// serialization or Bison/Flex parsing when connected to the local catalog provider.
    /// On the client (`RcComm`), serializes the AST via `to_query_string` and invokes `rc_genquery2`.
    template <typename RxComm>
    auto try_execute(RxComm& _comm,
                     const select& _sel,
                     nlohmann::json& _rows,
                     std::string_view _zone_hint = {}) -> int
    {
        char* output = nullptr;
        at_scope_exit free_output{[&output] { std::free(output); }};

        std::string zone_buf;
        const char* zone_ptr = nullptr;
        if (!_zone_hint.empty()) {
            zone_buf.assign(_zone_hint);
            zone_ptr = zone_buf.c_str();
        }

        int ec = 0;
        if constexpr (std::is_same_v<std::remove_cv_t<RxComm>, RsComm>) {
            ec = rs_genquery2(&_comm, _sel, zone_ptr, &output);
        }
        else {
            auto query_str = to_query_string(_sel);
            Genquery2Input input{};
            input.query_string = query_str.data();
            input.zone = const_cast<char*>(zone_ptr);
            ec = rc_genquery2(&_comm, &input, &output);
        }

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

        if (!_rows.is_array()) {
            return SYS_LIBRARY_ERROR;
        }

        return 0;
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
} // namespace irods::experimental::genquery2
#endif

#endif // IRODS_GENQUERY2_BUILDER_HPP
