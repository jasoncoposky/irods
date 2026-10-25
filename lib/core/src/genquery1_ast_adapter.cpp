#include "irods/genquery1_ast_adapter.hpp"

#include "irods/genquery2_builder.hpp"
#include "irods/irods_exception.hpp"
#include "irods/rcMisc.h"
#include "irods/rodsErrorTable.h"
#include "irods/rodsGenQueryNames.h"

#include <fmt/format.h>

#include <algorithm>
#include <cctype>
#include <charconv>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace
{
    auto iequals(std::string_view _lhs, std::string_view _rhs) noexcept -> bool
    {
        return _lhs.size() == _rhs.size() &&
               std::equal(_lhs.begin(), _lhs.end(), _rhs.begin(), [](unsigned char a, unsigned char b) {
                   return std::tolower(a) == std::tolower(b);
               });
    }

    auto istarts_with(std::string_view _str, std::string_view _prefix) noexcept -> bool
    {
        return _str.size() >= _prefix.size() && iequals(_str.substr(0, _prefix.size()), _prefix);
    }

    auto trim_ws(std::string_view _sv) noexcept -> std::string_view
    {
        while (!_sv.empty() && (_sv.front() == ' ' || _sv.front() == '\t' || _sv.front() == '\n' || _sv.front() == '\r'))
        {
            _sv.remove_prefix(1);
        }
        while (!_sv.empty() && (_sv.back() == ' ' || _sv.back() == '\t' || _sv.back() == '\n' || _sv.back() == '\r')) {
            _sv.remove_suffix(1);
        }
        return _sv;
    }

    auto find_column_id_in_table(std::string_view _column) noexcept -> int
    {
        for (int i = 0; i < NumOfColumnNames; ++i) {
            if (const auto& entry = columnNames[i]; _column == entry.columnName) {
                return entry.columnId;
            }
        }
        return NO_COLUMN_NAME_FOUND;
    }

    enum class gq1_token_type
    {
        end_of_input,
        select_kw,
        where_kw,
        like_kw,
        in_kw,
        between_kw,
        and_kw,
        not_kw,
        begin_of_kw,
        parent_of_kw,
        equal,
        not_equal,
        less_than,
        less_than_or_equal_to,
        greater_than,
        greater_than_or_equal_to,
        comma,
        paren_open,
        paren_close,
        double_vertical_bars,
        double_ampersands,
        identifier,
        string_literal
    };

    struct gq1_token
    {
        gq1_token_type type = gq1_token_type::end_of_input;
        std::string text;
    };

    class gq1_lexer
    {
    public:
        explicit gq1_lexer(std::string_view _input) noexcept
            : input_{_input}
        {
        }

        auto next() -> std::optional<gq1_token>
        {
            while (pos_ < input_.size() &&
                   (input_[pos_] == ' ' || input_[pos_] == '\t' || input_[pos_] == '\n' || input_[pos_] == '\r'))
            {
                ++pos_;
            }

            if (pos_ >= input_.size()) {
                return gq1_token{gq1_token_type::end_of_input, {}};
            }

            const char c = input_[pos_];

            // String literal: matches lexer.l ss_string_literal state
            if (c == '\'') {
                ++pos_;
                std::string lit;
                while (pos_ < input_.size()) {
                    if (input_[pos_] == '\'') {
                        if (pos_ + 1 < input_.size() && input_[pos_ + 1] == '\'') {
                            lit.push_back('\'');
                            pos_ += 2;
                            continue;
                        }
                        ++pos_;
                        return gq1_token{gq1_token_type::string_literal, std::move(lit)};
                    }

                    if (input_[pos_] == '\\' && pos_ + 3 < input_.size() && input_[pos_ + 1] == 'x' &&
                        std::isxdigit(static_cast<unsigned char>(input_[pos_ + 2])) &&
                        std::isxdigit(static_cast<unsigned char>(input_[pos_ + 3])))
                    {
                        std::uint8_t value = 0;
                        const auto* start = input_.data() + pos_ + 2;
                        const auto err = std::from_chars(start, start + 2, value, 16);
                        if (err.ec != std::errc{}) {
                            return std::nullopt;
                        }
                        lit.push_back(static_cast<char>(value));
                        pos_ += 4;
                        continue;
                    }

                    lit.push_back(input_[pos_]);
                    ++pos_;
                }

                // Missing closing single quote
                return std::nullopt;
            }

            // Two-character operators
            if (pos_ + 1 < input_.size()) {
                const auto two = input_.substr(pos_, 2);
                if (two == "!=" || two == "<>") {
                    pos_ += 2;
                    return gq1_token{gq1_token_type::not_equal, "!="};
                }
                if (two == "<=") {
                    pos_ += 2;
                    return gq1_token{gq1_token_type::less_than_or_equal_to, "<="};
                }
                if (two == ">=") {
                    pos_ += 2;
                    return gq1_token{gq1_token_type::greater_than_or_equal_to, ">="};
                }
                if (two == "||") {
                    pos_ += 2;
                    return gq1_token{gq1_token_type::double_vertical_bars, "||"};
                }
                if (two == "&&") {
                    pos_ += 2;
                    return gq1_token{gq1_token_type::double_ampersands, "&&"};
                }
            }

            // Single-character operators and punctuation
            switch (c) {
                case '=':
                    ++pos_;
                    return gq1_token{gq1_token_type::equal, "="};
                case '<':
                    ++pos_;
                    return gq1_token{gq1_token_type::less_than, "<"};
                case '>':
                    ++pos_;
                    return gq1_token{gq1_token_type::greater_than, ">"};
                case ',':
                    ++pos_;
                    return gq1_token{gq1_token_type::comma, ","};
                case '(':
                    ++pos_;
                    return gq1_token{gq1_token_type::paren_open, "("};
                case ')':
                    ++pos_;
                    return gq1_token{gq1_token_type::paren_close, ")"};
                default:
                    break;
            }

            // Identifiers and keywords: [a-zA-Z][a-zA-Z0-9_]*
            if (std::isalpha(static_cast<unsigned char>(c))) {
                const std::size_t start = pos_;
                ++pos_;
                while (pos_ < input_.size() &&
                       (std::isalnum(static_cast<unsigned char>(input_[pos_])) || input_[pos_] == '_'))
                {
                    ++pos_;
                }

                const auto word = input_.substr(start, pos_ - start);
                if (iequals(word, "select")) {
                    return gq1_token{gq1_token_type::select_kw, std::string{word}};
                }
                if (iequals(word, "where")) {
                    return gq1_token{gq1_token_type::where_kw, std::string{word}};
                }
                if (iequals(word, "like")) {
                    return gq1_token{gq1_token_type::like_kw, std::string{word}};
                }
                if (iequals(word, "in")) {
                    return gq1_token{gq1_token_type::in_kw, std::string{word}};
                }
                if (iequals(word, "between")) {
                    return gq1_token{gq1_token_type::between_kw, std::string{word}};
                }
                if (iequals(word, "and")) {
                    return gq1_token{gq1_token_type::and_kw, std::string{word}};
                }
                if (iequals(word, "not")) {
                    return gq1_token{gq1_token_type::not_kw, std::string{word}};
                }
                if (iequals(word, "begin_of")) {
                    return gq1_token{gq1_token_type::begin_of_kw, std::string{word}};
                }
                if (iequals(word, "parent_of")) {
                    return gq1_token{gq1_token_type::parent_of_kw, std::string{word}};
                }

                return gq1_token{gq1_token_type::identifier, std::string{word}};
            }

            return std::nullopt;
        }

    private:
        std::string_view input_;
        std::size_t pos_ = 0;
    };

    auto add_column_to_projection_list(genQueryInp_t* _out,
                                       std::string_view _column,
                                       std::optional<std::string_view> _fn = std::nullopt) -> int
    {
        const auto column_id = find_column_id_in_table(_column);
        if (column_id < 0) {
            return column_id;
        }

        if (_fn) {
            int aggregate_fn = -1;

            // clang-format off
            if      (iequals(*_fn, "order"))      { aggregate_fn = ORDER_BY; }
            else if (iequals(*_fn, "order_asc"))  { aggregate_fn = ORDER_BY; }
            else if (iequals(*_fn, "order_desc")) { aggregate_fn = ORDER_BY_DESC; }
            else if (iequals(*_fn, "min"))        { aggregate_fn = SELECT_MIN; }
            else if (iequals(*_fn, "max"))        { aggregate_fn = SELECT_MAX; }
            else if (iequals(*_fn, "sum"))        { aggregate_fn = SELECT_SUM; }
            else if (iequals(*_fn, "avg"))        { aggregate_fn = SELECT_AVG; }
            else if (iequals(*_fn, "count"))      { aggregate_fn = SELECT_COUNT; }
            // clang-format on
            else {
                return INVALID_GENQUERY_AGGREGATE_FUNCTION;
            }

            addInxIval(&_out->selectInp, column_id, aggregate_fn);
            return 0;
        }

        addInxIval(&_out->selectInp, column_id, 1);
        return 0;
    }

    auto parse_condition_expression(gq1_lexer& _lex, std::string& _out_expr, gq1_token& _lookahead) -> int
    {
        const auto op_tok = _lex.next();
        if (!op_tok) {
            return INPUT_ARG_NOT_WELL_FORMED_ERR;
        }

        switch (op_tok->type) {
            case gq1_token_type::like_kw: {
                const auto lit = _lex.next();
                if (!lit || lit->type != gq1_token_type::string_literal) {
                    return INPUT_ARG_NOT_WELL_FORMED_ERR;
                }
                _out_expr = fmt::format("like '{}'", lit->text);
                break;
            }
            case gq1_token_type::not_kw: {
                const auto like_tok = _lex.next();
                if (!like_tok || like_tok->type != gq1_token_type::like_kw) {
                    return INPUT_ARG_NOT_WELL_FORMED_ERR;
                }
                const auto lit = _lex.next();
                if (!lit || lit->type != gq1_token_type::string_literal) {
                    return INPUT_ARG_NOT_WELL_FORMED_ERR;
                }
                _out_expr = fmt::format("not like '{}'", lit->text);
                break;
            }
            case gq1_token_type::between_kw: {
                const auto lit1 = _lex.next();
                if (!lit1 || lit1->type != gq1_token_type::string_literal) {
                    return INPUT_ARG_NOT_WELL_FORMED_ERR;
                }
                const auto lit2 = _lex.next();
                if (!lit2 || lit2->type != gq1_token_type::string_literal) {
                    return INPUT_ARG_NOT_WELL_FORMED_ERR;
                }
                _out_expr = fmt::format("between '{}' '{}'", lit1->text, lit2->text);
                break;
            }
            case gq1_token_type::equal:
            case gq1_token_type::not_equal:
            case gq1_token_type::less_than:
            case gq1_token_type::less_than_or_equal_to:
            case gq1_token_type::greater_than:
            case gq1_token_type::greater_than_or_equal_to: {
                const auto lit = _lex.next();
                if (!lit || lit->type != gq1_token_type::string_literal) {
                    return INPUT_ARG_NOT_WELL_FORMED_ERR;
                }
                _out_expr = fmt::format("{} '{}'", op_tok->text, lit->text);
                break;
            }
            case gq1_token_type::begin_of_kw: {
                const auto lit = _lex.next();
                if (!lit || lit->type != gq1_token_type::string_literal) {
                    return INPUT_ARG_NOT_WELL_FORMED_ERR;
                }
                _out_expr = fmt::format("begin_of '{}'", lit->text);
                break;
            }
            case gq1_token_type::parent_of_kw: {
                const auto lit = _lex.next();
                if (!lit || lit->type != gq1_token_type::string_literal) {
                    return INPUT_ARG_NOT_WELL_FORMED_ERR;
                }
                _out_expr = fmt::format("parent_of '{}'", lit->text);
                break;
            }
            case gq1_token_type::in_kw: {
                const auto first = _lex.next();
                if (!first) {
                    return INPUT_ARG_NOT_WELL_FORMED_ERR;
                }
                if (first->type == gq1_token_type::paren_open) {
                    const auto first_lit = _lex.next();
                    if (!first_lit || first_lit->type != gq1_token_type::string_literal) {
                        return INPUT_ARG_NOT_WELL_FORMED_ERR;
                    }
                    std::string list = fmt::format("'{}'", first_lit->text);
                    while (true) {
                        const auto next_tok = _lex.next();
                        if (!next_tok) {
                            return INPUT_ARG_NOT_WELL_FORMED_ERR;
                        }
                        if (next_tok->type == gq1_token_type::comma) {
                            const auto elem = _lex.next();
                            if (!elem || elem->type != gq1_token_type::string_literal) {
                                return INPUT_ARG_NOT_WELL_FORMED_ERR;
                            }
                            list += fmt::format(", '{}'", elem->text);
                        }
                        else if (next_tok->type == gq1_token_type::paren_close) {
                            break;
                        }
                        else {
                            return INPUT_ARG_NOT_WELL_FORMED_ERR;
                        }
                    }
                    _out_expr = fmt::format("in ({})", list);
                }
                else if (first->type == gq1_token_type::string_literal) {
                    std::string list = fmt::format("'{}'", first->text);
                    while (true) {
                        const auto next_tok = _lex.next();
                        if (!next_tok) {
                            return INPUT_ARG_NOT_WELL_FORMED_ERR;
                        }
                        if (next_tok->type == gq1_token_type::comma) {
                            const auto elem = _lex.next();
                            if (!elem || elem->type != gq1_token_type::string_literal) {
                                return INPUT_ARG_NOT_WELL_FORMED_ERR;
                            }
                            list += fmt::format(", '{}'", elem->text);
                        }
                        else {
                            _lookahead = *next_tok;
                            _out_expr = fmt::format("in {}", list);
                            return 0;
                        }
                    }
                }
                else {
                    return INPUT_ARG_NOT_WELL_FORMED_ERR;
                }
                break;
            }
            default:
                return INPUT_ARG_NOT_WELL_FORMED_ERR;
        }

        const auto next_la = _lex.next();
        if (!next_la) {
            return INPUT_ARG_NOT_WELL_FORMED_ERR;
        }
        _lookahead = *next_la;
        return 0;
    }

    auto extract_single_literal(std::string_view _s) -> std::string
    {
        const auto first_quote = _s.find('\'');
        const auto last_quote = _s.rfind('\'');
        if (first_quote == std::string_view::npos || last_quote == std::string_view::npos || last_quote == first_quote)
        {
            // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
            THROW(CAT_INVALID_ARGUMENT, fmt::format("missing quoted literal in GenQuery1 condition: [{}]", _s));
        }
        return std::string{_s.substr(first_quote + 1, last_quote - first_quote - 1)};
    }

    auto extract_literal_list(std::string_view _s) -> std::vector<std::string>
    {
        const auto is_closing_quote = [_s](std::size_t _idx) noexcept -> bool {
            std::size_t j = _idx + 1;
            while (j < _s.size() && (_s[j] == ' ' || _s[j] == '\t' || _s[j] == '\n' || _s[j] == '\r')) {
                ++j;
            }
            if (j >= _s.size()) {
                return true;
            }
            if (_s[j] == ',' || _s[j] == ')') {
                return true;
            }
            if (_s[j] == '\'' && j > _idx + 1) {
                return true;
            }
            return false;
        };

        std::vector<std::string> result;
        std::size_t i = 0;
        while (i < _s.size()) {
            const auto open_q = _s.find('\'', i);
            if (open_q == std::string_view::npos) {
                break;
            }
            std::string item;
            i = open_q + 1;
            bool closed = false;
            while (i < _s.size()) {
                if (_s[i] == '\'') {
                    if (i + 1 < _s.size() && _s[i + 1] == '\'' && !is_closing_quote(i + 1)) {
                        item.push_back('\'');
                        i += 2;
                        continue;
                    }
                    if (is_closing_quote(i)) {
                        ++i;
                        closed = true;
                        break;
                    }
                }
                item.push_back(_s[i]);
                ++i;
            }
            if (!closed) {
                // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
                THROW(CAT_INVALID_ARGUMENT, fmt::format("unterminated string literal in GenQuery1 condition: [{}]", _s));
            }
            result.push_back(std::move(item));
        }

        if (result.empty()) {
            // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
            THROW(CAT_INVALID_ARGUMENT, fmt::format("no string literals found in GenQuery1 condition: [{}]", _s));
        }
        return result;
    }

    struct compound_piece
    {
        bool is_or = false; // true = ||, false = && (ignored for index 0)
        std::string_view expr;
    };

    auto split_compound_condition(std::string_view _cond) -> std::vector<compound_piece>
    {
        const auto is_closing_quote = [_cond](std::size_t _idx) noexcept -> bool {
            std::size_t j = _idx + 1;
            while (j < _cond.size() &&
                   (_cond[j] == ' ' || _cond[j] == '\t' || _cond[j] == '\n' || _cond[j] == '\r' || _cond[j] == ')'))
            {
                ++j;
            }
            if (j >= _cond.size()) {
                return true;
            }
            if (j + 1 < _cond.size() && ((_cond[j] == '|' && _cond[j + 1] == '|') || (_cond[j] == '&' && _cond[j + 1] == '&'))) {
                return true;
            }
            return false;
        };

        std::vector<compound_piece> pieces;
        bool in_quote = false;
        std::size_t piece_start = 0;
        bool next_is_or = false;

        for (std::size_t i = 0; i < _cond.size(); ++i) {
            if (_cond[i] == '\'') {
                if (!in_quote) {
                    in_quote = true;
                }
                else if (i + 1 < _cond.size() && _cond[i + 1] == '\'' && !is_closing_quote(i + 1)) {
                    ++i;
                }
                else if (is_closing_quote(i)) {
                    in_quote = false;
                }
                continue;
            }
            if (!in_quote && i + 1 < _cond.size()) {
                if (_cond[i] == '|' && _cond[i + 1] == '|') {
                    pieces.push_back(compound_piece{next_is_or, trim_ws(_cond.substr(piece_start, i - piece_start))});
                    next_is_or = true;
                    piece_start = i + 2;
                    ++i;
                }
                else if (_cond[i] == '&' && _cond[i + 1] == '&') {
                    pieces.push_back(compound_piece{next_is_or, trim_ws(_cond.substr(piece_start, i - piece_start))});
                    next_is_or = false;
                    piece_start = i + 2;
                    ++i;
                }
            }
        }

        pieces.push_back(compound_piece{next_is_or, trim_ws(_cond.substr(piece_start))});
        return pieces;
    }

    auto consume_keyword(std::string_view& _s, std::string_view _kw) noexcept -> bool
    {
        const auto trimmed = trim_ws(_s);
        if (!istarts_with(trimmed, _kw)) {
            return false;
        }
        const auto rest = trimmed.substr(_kw.size());
        if (!rest.empty()) {
            const char next_c = rest.front();
            if (std::isalnum(static_cast<unsigned char>(next_c)) || next_c == '_') {
                return false;
            }
        }
        _s = trim_ws(rest);
        return true;
    }

    auto parse_single_condition_expr(std::string_view _col_name, std::string_view _expr, bool& _has_numeric_cast)
        -> irods::experimental::genquery2::condition_expression
    {
        namespace gq2 = irods::experimental::genquery2;

        auto s = trim_ws(_expr);
        if (s.size() >= 2 && (s[0] == 'n' || s[0] == 'N') && (s[1] == '<' || s[1] == '>' || s[1] == '=')) {
            _has_numeric_cast = true;
            s = trim_ws(s.substr(1));
        }

        if (consume_keyword(s, "is")) {
            if (consume_keyword(s, "not")) {
                if (consume_keyword(s, "null") && s.empty()) {
                    return gq2::condition_is_not_null{};
                }
            }
            else if (consume_keyword(s, "null") && s.empty()) {
                return gq2::condition_is_null{};
            }
            // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
            THROW(CAT_INVALID_ARGUMENT, fmt::format("invalid IS condition in GenQuery1: [{}]", _expr));
        }

        if (consume_keyword(s, "not")) {
            if (consume_keyword(s, "like")) {
                return gq2::condition_operator_not{gq2::condition_like{extract_single_literal(s)}};
            }
            if (consume_keyword(s, "between")) {
                auto vals = extract_literal_list(s);
                if (vals.size() != 2) {
                    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
                    THROW(CAT_INVALID_ARGUMENT, fmt::format("invalid NOT BETWEEN condition in GenQuery1: [{}]", _expr));
                }
                return gq2::condition_operator_not{gq2::condition_between{std::move(vals[0]), std::move(vals[1])}};
            }
            if (consume_keyword(s, "in")) {
                return gq2::condition_operator_not{gq2::condition_in{extract_literal_list(s)}};
            }
            // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
            THROW(CAT_INVALID_ARGUMENT, fmt::format("invalid NOT condition in GenQuery1: [{}]", _expr));
        }

        if (consume_keyword(s, "like")) {
            return gq2::condition_like{extract_single_literal(s)};
        }

        if (consume_keyword(s, "between")) {
            auto vals = extract_literal_list(s);
            if (vals.size() != 2) {
                // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
                THROW(CAT_INVALID_ARGUMENT, fmt::format("invalid BETWEEN condition in GenQuery1: [{}]", _expr));
            }
            return gq2::condition_between{std::move(vals[0]), std::move(vals[1])};
        }

        if (consume_keyword(s, "in")) {
            return gq2::condition_in{extract_literal_list(s)};
        }

        if (consume_keyword(s, "parent_of") || consume_keyword(s, "begin_of")) {
            const auto path = extract_single_literal(s);
            auto conds = gq2::builder::col{_col_name}.parent_of(path).to_conditions();
            return std::move(std::get<gq2::condition>(conds.front()).expression);
        }

        if (s.starts_with("!=") || s.starts_with("<>")) {
            return gq2::condition_not_equal{extract_single_literal(s.substr(2))};
        }
        if (s.starts_with("<=")) {
            return gq2::condition_less_than_or_equal_to{extract_single_literal(s.substr(2))};
        }
        if (s.starts_with(">=")) {
            return gq2::condition_greater_than_or_equal_to{extract_single_literal(s.substr(2))};
        }
        if (s.starts_with('=')) {
            return gq2::condition_equal{extract_single_literal(s.substr(1))};
        }
        if (s.starts_with('<')) {
            return gq2::condition_less_than{extract_single_literal(s.substr(1))};
        }
        if (s.starts_with('>')) {
            return gq2::condition_greater_than{extract_single_literal(s.substr(1))};
        }

        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
        THROW(CAT_INVALID_ARGUMENT, fmt::format("unrecognized GenQuery1 condition: [{}]", _expr));
    }
} // anonymous namespace

namespace irods::experimental::genquery2
{
    auto genquery1_column_id_to_name(int _col_id) -> std::string_view
    {
        for (int i = 0; i < NumOfColumnNames; ++i) {
            if (const auto& entry = columnNames[i]; entry.columnId == _col_id) {
                return entry.columnName;
            }
        }

        if (_col_id == 408) {
            return "DATA_RESC_GROUP_NAME";
        }
        if (_col_id == COL_USER_AUTH_ID) {
            return "USER_AUTH_ID";
        }
        if (_col_id == COL_USER_DN_INVALID) {
            return "USER_DN_INVALID";
        }

        return {};
    }

    auto genquery1_column_name_to_id(std::string_view _name) -> int
    {
        if (const auto id = find_column_id_in_table(_name); id >= 0) {
            return id;
        }

        if (_name == "USER_AUTH_ID") {
            return COL_USER_AUTH_ID;
        }
        if (_name == "USER_DN_INVALID") {
            return COL_USER_DN_INVALID;
        }

        return NO_COLUMN_NAME_FOUND;
    }

    auto genquery1_to_genquery2_ast(const genQueryInp_t& _inp) -> select
    {
        select sel;
        sel.distinct = (_inp.options & NO_DISTINCT) == 0;

        const bool upper_case_where = (_inp.options & UPPER_CASE_WHERE) != 0;

        bool has_aggregates = false;
        std::vector<column> non_aggregated_columns;
        non_aggregated_columns.reserve(static_cast<std::size_t>( std::max(0, _inp.selectInp.len)));

        for (int i = 0; i < _inp.selectInp.len; ++i) {
            // NOLINTBEGIN(cppcoreguidelines-pro-bounds-pointer-arithmetic)
            const int col_id = _inp.selectInp.inx[i];
            const int val = _inp.selectInp.value[i];
            // NOLINTEND(cppcoreguidelines-pro-bounds-pointer-arithmetic)

            const auto col_name = genquery1_column_id_to_name(col_id);
            if (col_name.empty()) {
                // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
                THROW(CAT_INVALID_ARGUMENT, fmt::format("unknown GenQuery1 column ID: {}", col_id));
            }

            const int select_opt = val & 0xf;
            std::string_view agg_fn;
            switch (select_opt) {
                case SELECT_MIN:
                    agg_fn = "min";
                    break;
                case SELECT_MAX:
                    agg_fn = "max";
                    break;
                case SELECT_SUM:
                    agg_fn = "sum";
                    break;
                case SELECT_AVG:
                    agg_fn = "avg";
                    break;
                case SELECT_COUNT:
                    agg_fn = "count";
                    break;
                default:
                    break;
            }

            if (!agg_fn.empty()) {
                has_aggregates = true;
                sel.projections.emplace_back(
                    function{std::string{agg_fn}, {column{std::string{col_name}}}});
            }
            else {
                column col{std::string{col_name}};
                non_aggregated_columns.push_back(col);
                sel.projections.emplace_back(std::move(col));
            }

            if ((val & ORDER_BY) != 0 || (val & ORDER_BY_DESC) != 0) {
                const bool asc = (val & ORDER_BY_DESC) == 0;
                sel.order_by.sort_expressions.push_back(
                    sort_expression{column{std::string{col_name}}, asc});
            }
        }

        if (has_aggregates) {
            for (auto& col : non_aggregated_columns) {
                sel.group_by.expressions.emplace_back(std::move(col));
            }
        }

        // Default ascending sort on non-aggregated COLL_NAME, DATA_NAME, DATA_REPL_NUM
        for (const int default_sort_col_id : {COL_COLL_NAME, COL_DATA_NAME, COL_DATA_REPL_NUM}) {
            for (int i = 0; i < _inp.selectInp.len; ++i) {
                // NOLINTBEGIN(cppcoreguidelines-pro-bounds-pointer-arithmetic)
                const int col_id = _inp.selectInp.inx[i];
                const int val = _inp.selectInp.value[i];
                // NOLINTEND(cppcoreguidelines-pro-bounds-pointer-arithmetic)

                if (col_id != default_sort_col_id) {
                    continue;
                }

                const int select_opt = val & 0xf;
                const bool is_aggregated = (select_opt == SELECT_MIN || select_opt == SELECT_MAX ||
                                            select_opt == SELECT_SUM || select_opt == SELECT_AVG ||
                                            select_opt == SELECT_COUNT);
                const bool already_ordered = (val & (ORDER_BY | ORDER_BY_DESC)) != 0;
                if (!is_aggregated && !already_ordered) {
                    const auto col_name = genquery1_column_id_to_name(col_id);
                    sel.order_by.sort_expressions.push_back(
                        sort_expression{column{std::string{col_name}}, true});
                    break;
                }
            }
        }

        // Translate WHERE conditions
        for (int i = 0; i < _inp.sqlCondInp.len; ++i) {
            // NOLINTBEGIN(cppcoreguidelines-pro-bounds-pointer-arithmetic)
            const int col_id = _inp.sqlCondInp.inx[i];
            const char* raw_cond_ptr = _inp.sqlCondInp.value[i];
            // NOLINTEND(cppcoreguidelines-pro-bounds-pointer-arithmetic)

            const auto col_name = genquery1_column_id_to_name(col_id);
            if (col_name.empty()) {
                // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
                THROW(CAT_INVALID_ARGUMENT, fmt::format("unknown GenQuery1 column ID in WHERE: {}", col_id));
            }

            if (!raw_cond_ptr) {
                // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
                THROW(CAT_INVALID_ARGUMENT, "null condition string in GenQuery1 sqlCondInp");
            }

            auto cond_sv = trim_ws(raw_cond_ptr);
            bool has_numeric_cast = false;
            if (cond_sv.size() >= 2 && (cond_sv[0] == 'n' || cond_sv[0] == 'N') &&
                (cond_sv[1] == '<' || cond_sv[1] == '>' || cond_sv[1] == '='))
            {
                has_numeric_cast = true;
                cond_sv = trim_ws(cond_sv.substr(1));
            }

            const auto pieces = split_compound_condition(cond_sv);

            auto make_lhs = [&](bool _numeric_cast) -> std::variant<column, function> {
                column col{std::string{col_name}};
                if (_numeric_cast) {
                    col.type_name = "decimal";
                    return col;
                }
                if (upper_case_where) {
                    return function{"upper", {std::move(col)}};
                }
                return col;
            };

            condition_wrapper entry_wrapper;
            if (pieces.size() == 1) {
                bool piece_cast = has_numeric_cast;
                auto expr = parse_single_condition_expr(col_name, pieces[0].expr, piece_cast);
                auto lhs = make_lhs(piece_cast);
                if (std::holds_alternative<column>(lhs)) {
                    entry_wrapper = condition{std::move(std::get<column>(lhs)), std::move(expr)};
                }
                else {
                    entry_wrapper = condition{std::move(std::get<function>(lhs)), std::move(expr)};
                }
            }
            else {
                conditions group_conds;
                for (std::size_t p_idx = 0; p_idx < pieces.size(); ++p_idx) {
                    bool piece_cast = has_numeric_cast;
                    auto expr = parse_single_condition_expr(col_name, pieces[p_idx].expr, piece_cast);
                    auto lhs = make_lhs(piece_cast);
                    condition single_cond = std::holds_alternative<column>(lhs)
                        ? condition{std::move(std::get<column>(lhs)), std::move(expr)}
                        : condition{std::move(std::get<function>(lhs)), std::move(expr)};

                    if (p_idx == 0) {
                        group_conds.emplace_back(std::move(single_cond));
                    }
                    else if (pieces[p_idx].is_or) {
                        group_conds.emplace_back(logical_or{conditions{std::move(single_cond)}});
                    }
                    else {
                        group_conds.emplace_back(logical_and{conditions{std::move(single_cond)}});
                    }
                }
                entry_wrapper = logical_grouping{std::move(group_conds)};
            }

            if (sel.conditions.empty()) {
                sel.conditions.emplace_back(std::move(entry_wrapper));
            }
            else {
                sel.conditions.emplace_back(logical_and{conditions{std::move(entry_wrapper)}});
            }
        }

        if (_inp.rowOffset > 0) {
            sel.range.offset = std::to_string(_inp.rowOffset);
        }
        if (_inp.maxRows > 0) {
            sel.range.number_of_rows = std::to_string(_inp.maxRows);
        }

        return sel;
    }
} // namespace irods::experimental::genquery2

extern "C" int parse_genquery1_string(const char* _s, genQueryInp_t* _out)
{
    if (!_s || !_out) {
        return SYS_INVALID_INPUT_PARAM;
    }

    try {
        gq1_lexer lex{_s};

        const auto select_tok = lex.next();
        if (!select_tok || select_tok->type != gq1_token_type::select_kw) {
            return INPUT_ARG_NOT_WELL_FORMED_ERR;
        }

        gq1_token lookahead;

        // Parse projection_list: projection (',' projection)*
        while (true) {
            const auto id1 = lex.next();
            if (!id1 || id1->type != gq1_token_type::identifier) {
                return INPUT_ARG_NOT_WELL_FORMED_ERR;
            }

            const auto next_tok = lex.next();
            if (!next_tok) {
                return INPUT_ARG_NOT_WELL_FORMED_ERR;
            }

            if (next_tok->type == gq1_token_type::paren_open) {
                const auto col_tok = lex.next();
                if (!col_tok || col_tok->type != gq1_token_type::identifier) {
                    return INPUT_ARG_NOT_WELL_FORMED_ERR;
                }

                const auto close_tok = lex.next();
                if (!close_tok || close_tok->type != gq1_token_type::paren_close) {
                    return INPUT_ARG_NOT_WELL_FORMED_ERR;
                }

                if (const int ec = add_column_to_projection_list(_out, col_tok->text, id1->text); ec < 0) {
                    return ec;
                }

                const auto after_proj = lex.next();
                if (!after_proj) {
                    return INPUT_ARG_NOT_WELL_FORMED_ERR;
                }
                lookahead = *after_proj;
            }
            else {
                lookahead = *next_tok;
                if (lookahead.type != gq1_token_type::comma && lookahead.type != gq1_token_type::where_kw &&
                    lookahead.type != gq1_token_type::end_of_input)
                {
                    return INPUT_ARG_NOT_WELL_FORMED_ERR;
                }

                if (const int ec = add_column_to_projection_list(_out, id1->text); ec < 0) {
                    return ec;
                }
            }

            if (lookahead.type == gq1_token_type::comma) {
                continue;
            }
            break;
        }

        if (lookahead.type == gq1_token_type::end_of_input) {
            return 0;
        }

        if (lookahead.type != gq1_token_type::where_kw) {
            return INPUT_ARG_NOT_WELL_FORMED_ERR;
        }

        // Parse condition_list: condition (AND condition)*
        while (true) {
            const auto col_tok = lex.next();
            if (!col_tok || col_tok->type != gq1_token_type::identifier) {
                return INPUT_ARG_NOT_WELL_FORMED_ERR;
            }

            std::string expr_list;
            if (const int ec = parse_condition_expression(lex, expr_list, lookahead); ec < 0) {
                return ec;
            }

            while (lookahead.type == gq1_token_type::double_vertical_bars ||
                   lookahead.type == gq1_token_type::double_ampersands)
            {
                const std::string op = lookahead.text;
                std::string rhs_expr;
                if (const int ec = parse_condition_expression(lex, rhs_expr, lookahead); ec < 0) {
                    return ec;
                }
                expr_list = fmt::format("{} {} {}", expr_list, op, rhs_expr);
            }

            if (lookahead.type != gq1_token_type::and_kw && lookahead.type != gq1_token_type::end_of_input) {
                return INPUT_ARG_NOT_WELL_FORMED_ERR;
            }

            const auto col_id = find_column_id_in_table(col_tok->text);
            if (col_id < 0) {
                return col_id;
            }
            addInxVal(&_out->sqlCondInp, col_id, expr_list.c_str());

            if (lookahead.type == gq1_token_type::and_kw) {
                continue;
            }
            break;
        }

        return 0;
    }
    catch (const irods::exception& e) {
        return static_cast<int>(e.code());
    }
    catch (const std::exception&) {
        return SYS_LIBRARY_ERROR;
    }
}
