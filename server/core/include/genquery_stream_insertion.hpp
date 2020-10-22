#ifndef GENQUERY_STREAM_INSERTION_HPP
#define GENQUERY_STREAM_INSERTION_HPP

#include "genquery_ast_types.hpp"

namespace irods::experimental::api::genquery {
    template <typename T>
    T&
    operator<<(T& os, const Column& column) {
        os << column.name;
        return os;
    }

    template <typename T>
    T&
    operator<<(T& os, const SelectFunction& select_function) {
        os << select_function.name << "("
           << select_function.column << ")";
        return os;
    }

    template <typename T>
    T&
    operator<<(T& os, const Selections& selections) {
        os << "select ";
        for (auto it = selections.cbegin(); it != selections.cend(); ++it) {
            os << *it;
            if (it+1 != selections.cend()) {
                os << ", ";
            }
        }
        return os;
    }

    template <typename T>
    T&
    operator<<(T& os, const ConditionOperator_Or& op_or) {
        return os << "(" << op_or.left << " || " << op_or.right << ")";
    }

    template <typename T>
    T&
    operator<<(T& os, const ConditionOperator_And& op_and) {
        return os << "(" << op_and.left << " && " << op_and.right << ")";
    }

    template <typename T>
    T&
    operator<<(T& os, const ConditionOperator_Not& op_not) {
        return os << "not " << op_not.expression;
    }

    template <typename T>
    T&
    operator<<(T& os, const ConditionNotEqual& not_equal) {
        return os << "!= '" << not_equal.string_literal << "'";
    }

    template <typename T>
    T&
    operator<<(T& os, const ConditionEqual& equal) {
        return os << "= '" << equal.string_literal << "'";
    }

    template <typename T>
    T&
    operator<<(T& os, const ConditionLessThan& less_than) {
        return os << "< '" << less_than.string_literal << "'";
    }

    template <typename T>
    T&
    operator<<(T& os, const ConditionLessThanOrEqualTo& less_than_or_equal_to) {
        return os << "<= '" << less_than_or_equal_to.string_literal << "'";
    }

    template <typename T>
    T&
    operator<<(T& os, const ConditionGreaterThan& greater_than) {
        return os << "> '" << greater_than.string_literal << "'";
    }

    template <typename T>
    T&
    operator<<(T& os, const ConditionGreaterThanOrEqualTo& greater_than_or_equal_to) {
        return os << ">= '" << greater_than_or_equal_to.string_literal << "'";
    }

    template <typename T>
    T&
    operator<<(T& os, const ConditionBetween& between) {
        os << "between "
           << "'" << between.low << "'"
           << " "
           << "'" << between.high << "'";
        return os;
    }

    template <typename T>
    T&
    operator<<(T& os, const ConditionIn& in) {
        os << "in (";
        for (auto it = in.list_of_string_literals.cbegin(); it != in.list_of_string_literals.cend(); ++it) {
            os << "'" << *it << "'";
            if (it+1 != in.list_of_string_literals.cend()) {
                os << ", ";
            }
        }
        os << ")";
        return os;
    }

    template <typename T>
    T&
    operator<<(T& os, const ConditionLike& like) {
        return os << "like '" << like.string_literal << "'";
    }

    template <typename T>
    T&
    operator<<(T& os, const ConditionParentOf& parent_of) {
        return os << "parent_of '" << parent_of.string_literal << "'";
    }

    template <typename T>
    T&
    operator<<(T& os, const ConditionBeginningOf& beginning_of) {
        return os << "begin_of '" << beginning_of.string_literal << "'";
    }

    template <typename T>
    T&
    operator<<(T& os, const Condition& condition) {
        return os << condition.column << " " << condition.expression;
    }

    template <typename T>
    T&
    operator<<(T& os, const Conditions& conditions) {
        for (auto it = conditions.cbegin(); it != conditions.cend(); ++it) {
            os << *it;
            if (it+1 != conditions.cend()) {
                os << " and ";
            }
        }
        return os;
    }

    template <typename T>
    T&
    operator<<(T& os, const Select& select) {
        os << select.selections;
        if (select.conditions.size() > 0) {
            os << " where ";
            os << select.conditions;
        }
        return os;
    }
}
#endif // GENQUERY_STREAM_INSERTION_HPP
