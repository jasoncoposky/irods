#include "irods/filesystem/path.hpp"

#include "irods/filesystem/path_utilities.hpp"

#include <algorithm>
#include <filesystem>
#include <functional>
#include <iomanip>
#include <iostream>
#include <string>
#include <string_view>

namespace irods::filesystem
{
    namespace
    {
        auto join(path::iterator _first, path::iterator _last) -> path
        {
            path p;

            for (; _first != _last; ++_first) {
                p /= *_first;
            }

            return p;
        }

        // Zero-allocation forward component scanner matching path::iterator semantics.
        struct component_scanner
        {
            std::string_view full;
            std::size_t pos{0};
            std::string_view current{};
            bool at_end{false};

            explicit constexpr component_scanner(std::string_view _s) noexcept
                : full{_s}
            {
                if (full.empty()) {
                    at_end = true;
                    return;
                }

                if (is_separator(full.front())) {
                    current = full.substr(0, 1);
                }
                else {
                    const auto end = full.find_first_of(path::preferred_separator);
                    current = (end != std::string_view::npos) ? full.substr(0, end) : full;
                }
            }

            constexpr void advance() noexcept
            {
                if (at_end) {
                    return;
                }

                if (full.size() - 1 == pos && current.empty()) {
                    ++pos;
                    at_end = true;
                    return;
                }

                pos += current.size();

                if (full.size() == pos) {
                    current = {};
                    at_end = true;
                    return;
                }

                while (pos < full.size() && is_separator(full[pos])) {
                    ++pos;
                }

                if (full.size() == pos && is_separator(full.back())) {
                    current = {};
                    pos = full.size() - 1;
                }
                else if (const auto end = full.find_first_of(path::preferred_separator, pos);
                         end != std::string_view::npos)
                {
                    current = full.substr(pos, end - pos);
                }
                else {
                    current = full.substr(pos, full.size() - pos);
                }
            }
        };
    } // anonymous namespace

    //
    // Path
    //

    auto path::operator/=(const path& _p) -> path&
    {
        return append_view(_p.string_view());
    }

    auto path::append_view(std::string_view _p) -> path&
    {
        if (_p.empty()) {
            if (!value_.empty() && preferred_separator != value_.back()) {
                value_ += preferred_separator;
            }
        }
        else {
            if (is_separator(_p.front())) {
                value_.assign(_p);
                return *this;
            }

            append_separator_if_needed(_p);
            value_.append(_p);
        }

        return *this;
    }

    auto path::assign_concat(std::string_view _parent, std::string_view _child) -> path&
    {
        if (!_child.empty() && is_separator(_child.front())) {
            value_.assign(_child);
            return *this;
        }

        const bool need_sep = !_parent.empty() && !is_separator(_parent.back()) && !_child.empty();
        value_.clear();
        value_.reserve(_parent.size() + (need_sep ? 1 : 0) + _child.size());
        value_.append(_parent);
        if (need_sep) {
            value_.push_back(preferred_separator);
        }
        value_.append(_child);
        return *this;
    }

    auto path::remove_object_name() -> path&
    {
        if (has_object_name()) {
            if (const auto pos = value_.find_last_of(preferred_separator); string_type::npos != pos) {
                value_.erase(pos + 1);
            }
        }

        return *this;
    }

    auto path::replace_object_name(const path& _replacement) -> path&
    {
        remove_object_name();
        return *this /= _replacement;
    }

    auto path::replace_extension(const path& _replacement) -> path&
    {
        if (has_extension()) {
            if (const auto pos = value_.find_last_of(dot); string_type::npos != pos) {
                value_.erase(pos);
            }
        }

        if (const char dot = '.'; !_replacement.empty() && dot != _replacement.value_[0]) {
            value_ += dot;
        }

        return *this += _replacement;
    }

    auto path::lexically_normal() const -> path
    {
        return path{std::filesystem::path{value_}.lexically_normal().native()};
    }

    auto path::lexically_relative(const path& _base) const -> path
    {
        auto base_b = _base.begin();
        auto base_e = _base.end();
        auto mm = std::mismatch(begin(), end(), base_b, base_e);

        if (mm.first == end() && mm.second == base_e) {
            return {dot};
        }

        path p;

        for (; mm.second != base_e; ++mm.second) {
            p /= dot_dot;
        }

        for (; mm.first != end(); ++mm.first) {
            p /= *mm.first;
        }

        return p;
    }

    auto path::lexically_proximate(const path& _base) const -> path
    {
        auto p = lexically_relative(_base);
        return !p.empty() ? p : *this;
    }

    auto path::compare(std::string_view _p) const noexcept -> int
    {
        component_scanner s1{value_};
        component_scanner s2{_p};

        for (; !s1.at_end && !s2.at_end; s1.advance(), s2.advance()) {
            if (const int cmp = s1.current.compare(s2.current); cmp != 0) {
                return cmp < 0 ? -1 : 1;
            }
        }

        if (s1.at_end) {
            return s2.at_end ? 0 : -1;
        }

        return 1;
    }

    auto path::compare(const path& _p) const noexcept -> int
    {
        return compare(_p.string_view());
    }

    auto path::root_collection_view() const noexcept -> std::string_view
    {
        return is_absolute() ? std::string_view{value_}.substr(0, 1) : std::string_view{};
    }

    auto path::relative_path_view() const noexcept -> std::string_view
    {
        if (value_.empty()) {
            return {};
        }

        if (is_absolute()) {
            const auto pos = value_.find_first_not_of(preferred_separator);
            return (pos != string_type::npos) ? std::string_view{value_}.substr(pos) : std::string_view{};
        }

        return std::string_view{value_};
    }

    auto path::parent_path_view() const noexcept -> std::string_view
    {
        if (!has_relative_path()) {
            return is_absolute() ? std::string_view{value_}.substr(0, 1) : std::string_view{value_};
        }

        const auto last_sep = value_.find_last_of(preferred_separator);
        if (last_sep == string_type::npos) {
            return {};
        }

        auto end_pos = last_sep;
        while (end_pos > 0 && is_separator(value_[end_pos - 1])) {
            --end_pos;
        }

        if (end_pos == 0) {
            return std::string_view{value_}.substr(0, 1);
        }

        return std::string_view{value_}.substr(0, end_pos);
    }

    auto path::object_name_view() const noexcept -> std::string_view
    {
        if (!has_relative_path() || is_separator(value_.back())) {
            return {};
        }

        const auto last_sep = value_.find_last_of(preferred_separator);
        if (last_sep == string_type::npos) {
            return std::string_view{value_};
        }

        return std::string_view{value_}.substr(last_sep + 1);
    }

    auto path::stem_view() const noexcept -> std::string_view
    {
        const auto n = object_name_view();
        if (n.empty() || n == dot || n == dot_dot) {
            return {};
        }

        if (const auto pos = n.find_last_of(dot); std::string_view::npos != pos) {
            return n.substr(0, pos);
        }

        return {};
    }

    auto path::extension_view() const noexcept -> std::string_view
    {
        const auto n = object_name_view();
        if (n.empty() || n == dot || n == dot_dot) {
            return {};
        }

        if (const auto pos = n.find_last_of(dot); std::string_view::npos != pos) {
            return n.substr(pos);
        }

        return {};
    }

    auto path::root_collection() const -> path
    {
        return path{root_collection_view()};
    }

    auto path::relative_path() const -> path
    {
        const auto rv = relative_path_view();
        if (rv.find("//") != std::string_view::npos) {
            return join(++begin(), end());
        }
        return path{rv};
    }

    auto path::parent_path() const -> path
    {
        if (!has_relative_path()) {
            return *this;
        }
        const auto pv = parent_path_view();
        if (pv.find("//") != std::string_view::npos) {
            return join(begin(), --end());
        }
        return path{pv};
    }

    auto path::object_name() const -> path
    {
        return path{object_name_view()};
    }

    auto path::stem() const -> path
    {
        return path{stem_view()};
    }

    auto path::extension() const -> path
    {
        return path{extension_view()};
    }

    auto path::begin() const -> iterator
    {
        return iterator{*this};
    }

    auto path::end() const -> iterator
    {
        iterator it;
        it.path_ptr_ = this;
        it.pos_ = value_.size();
        return it;
    }

    auto path::rbegin() const -> reverse_iterator
    {
        return reverse_iterator{end()};
    }

    auto path::rend() const -> reverse_iterator
    {
        return reverse_iterator{begin()};
    }

    auto path::append_separator_if_needed(std::string_view _p) -> void
    {
        if (value_.empty() ||
            preferred_separator == value_.back() ||
            preferred_separator == _p.front())
        {
            return;
        }

        value_ += preferred_separator;
    }

    //
    // Iterator
    //
    
    path::iterator::iterator(const path& _p)
        : path_ptr_{&_p}
        , element_{}
        , pos_{}
    {
        if (path_ptr_->empty())
        {
            return;
        }

        // Does the path contain a leading forward slash "/"?
        if (path_ptr_->is_absolute())
        {
            element_.value_ = path::preferred_separator;
        }
        else
        {
            const auto& full_path = path_ptr_->value_;
            const auto end = full_path.find_first_of(path::preferred_separator);

            element_.value_ = (path::string_type::npos != end)
                ? full_path.substr(0, end)
                : full_path;
        }
    }

    auto path::iterator::operator++() -> iterator&
    {
        const auto& fp = path_ptr_->value_; // Full path
        auto& e = element_.value_;          // Path element

        // If we're just before the "end" iterator and the current value of
        // the element is empty, then we know the path ended with a separator and
        // therefore, the iterator should now be set to the "end" iterator.
        if (fp.size() - 1 == pos_ && e.empty()) {
            ++pos_;
            return *this;
        }

        // Skip the element currently pointed to.
        pos_ += e.size();

        // If we're at the end of the path, then we're done.
        if (fp.size() == pos_)
        {
            e.clear();
            return *this;
        }

        // If we're not at the end of the path, then we're most likely at a separator.
        // Skip consecutive separators.
        while (is_separator(fp[pos_])) {
            ++pos_;
        }

        if (fp.size() == pos_ && is_separator(fp[fp.size() - 1])) {
            // Found a trailing separator.
            e.clear();
            pos_ = fp.size() - 1;
        }
        // Found a character that is not a separator.
        else if (const auto end = fp.find_first_of(preferred_separator, pos_);
                 path::string_type::npos != end)
        {
            // Found a separator.
            e = fp.substr(pos_, end - pos_);
        }
        else
        {
            // No trailing separator found.
            e = fp.substr(pos_, fp.size() - pos_);
        }

        return *this;
    }

    auto path::iterator::operator--() -> iterator&
    {
        const auto& fp = path_ptr_->value_; // Full path
        auto& e = element_.value_;          // Path element

        // Handle trailing separator at the end of the path.
        // If the iterator represents the end iterator and the character preceding it
        // is a separator, then set the current element to the dot path.
        if (fp.size() == pos_ && fp.size() > 1 && is_separator(fp[pos_ - 1])) {
            --pos_;
            e.clear();
            return *this;
        }

        if (pos_ > 0 && is_separator(fp[pos_ - 1])) {
            // We've reached the root separator of the path.
            if (0 == pos_ - 1)
            {
                --pos_;
                e = preferred_separator;
                return *this;
            }

            // Point at the separator just after the preceding element.
            // Separators represent the end of a path element.
            while (pos_ > 0 && is_separator(fp[pos_ - 1])) {
                --pos_;
            }
        }

        const auto end = pos_;

        // Find the start of the path element.
        while (pos_ > 0 && !is_separator(fp[pos_ - 1])) {
            --pos_;
        }

        e = fp.substr(pos_, end - pos_);

        return *this;
    }

    auto lexicographical_compare(path::iterator _first1, path::iterator _last1,
                                 path::iterator _first2, path::iterator _last2) -> bool
    {
        for (; (_first1 != _last1) && (_first2 != _last2); ++_first1, ++_first2) {
            if (_first1->string() < _first2->string()) {
                return true;
            }

            if (_first2->string() < _first1->string()) {
                return false;
            }
        }

        return (_first1 == _last1) && (_first2 != _last2);
    }

    auto operator<<(std::ostream& _os, const path& _p) -> std::ostream&
    {
        return _os << std::quoted(_p.string());
    }

    auto operator>>(std::istream& _is, path& _p) -> std::istream&
    {
        path::string_type t;
        _is >> std::quoted(t);
        _p = std::move(t);
        return _is;
    }

    auto hash_value(const path& _p) noexcept -> std::size_t
    {
        return std::hash<path::string_type>{}(_p.native());
    }

    auto zone_name_view(const path& _p) noexcept -> std::optional<std::string_view>
    {
        component_scanner scanner{_p.string_view()};
        if (scanner.at_end) {
            return std::nullopt;
        }
        scanner.advance();
        if (scanner.at_end) {
            return std::nullopt;
        }
        return scanner.current;
    }

    auto zone_name(const path& _p) -> std::optional<std::string>
    {
        if (const auto zv = zone_name_view(_p); zv.has_value()) {
            return std::string{*zv};
        }

        return std::nullopt;
    }
} // namespace irods::filesystem
