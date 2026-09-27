#ifndef IRODS_GENQUERY2_BUILDER_HPP
#define IRODS_GENQUERY2_BUILDER_HPP

#include "irods/private/genquery2_ast_types.hpp"

#include <string>
#include <string_view>
#include <vector>
#include <utility>

namespace irods::experimental::genquery2::builder
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

        auto operator==(std::string_view _val) const -> condition_builder
        {
            return condition{column{std::string{name_}}, condition_equal{std::string{_val}}};
        }

        auto operator!=(std::string_view _val) const -> condition_builder
        {
            return condition{column{std::string{name_}}, condition_not_equal{std::string{_val}}};
        }

        auto operator<(std::string_view _val) const -> condition_builder
        {
            return condition{column{std::string{name_}}, condition_less_than{std::string{_val}}};
        }

        auto operator<=(std::string_view _val) const -> condition_builder
        {
            return condition{column{std::string{name_}}, condition_less_than_or_equal_to{std::string{_val}}};
        }

        auto operator>(std::string_view _val) const -> condition_builder
        {
            return condition{column{std::string{name_}}, condition_greater_than{std::string{_val}}};
        }

        auto operator>=(std::string_view _val) const -> condition_builder
        {
            return condition{column{std::string{name_}}, condition_greater_than_or_equal_to{std::string{_val}}};
        }

        auto like(std::string_view _pattern) const -> condition_builder
        {
            return condition{column{std::string{name_}}, condition_like{std::string{_pattern}}};
        }

        auto in(std::vector<std::string> _values) const -> condition_builder
        {
            return condition{column{std::string{name_}}, condition_in{std::move(_values)}};
        }

        auto is_null() const -> condition_builder
        {
            return condition{column{std::string{name_}}, condition_is_null{}};
        }

        auto is_not_null() const -> condition_builder
        {
            return condition{column{std::string{name_}}, condition_is_not_null{}};
        }

    private:
        std::string_view name_;
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
            sel_.group_by.columns = std::move(_cols);
            return *this;
        }

        auto order_by(std::vector<std::string> _cols, bool _asc = true) -> select_builder&
        {
            for (auto&& c : _cols) {
                sel_.order_by.sort_expressions.push_back(sort_expression{column{std::move(c)}, _asc});
            }
            return *this;
        }

        auto limit(std::string _number_of_rows, std::string _offset = "0") -> select_builder&
        {
            sel_.range.number_of_rows = std::move(_number_of_rows);
            sel_.range.offset = std::move(_offset);
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

    private:
        select sel_;
    };

    inline auto count(std::string _col) -> function
    {
        return function{"count", {column{std::move(_col)}}};
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
} // namespace irods::experimental::genquery2::builder

#endif // IRODS_GENQUERY2_BUILDER_HPP
