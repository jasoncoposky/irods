#ifndef IRODS_CONFIGURATION_PARSER_HPP
#define IRODS_CONFIGURATION_PARSER_HPP

/// \file

#include "irods/irods_exception.hpp"
#include "irods/irods_error.hpp"
#include "irods/rodsErrorTable.h"

#include <any>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <fmt/format.h>
#include <nlohmann/json.hpp>

namespace irods
{
    class configuration_parser
    {
    public:
        using key_path_t = std::vector<std::string>;

        configuration_parser();

        ~configuration_parser();

        configuration_parser(const configuration_parser& _other);

        configuration_parser(const std::string&);

        configuration_parser& operator=(const configuration_parser&);

        void clear();

        error load(const std::string&);

        error write(const std::string&);

        error write();

        bool has_entry(const std::string_view _key) const;

        template <typename T>
        T& set(const std::string& _key, const T& _val)
        {
            root_[_key] = std::any(_val);
            return std::any_cast<T&>(root_[_key]);
        } // set

        template <typename T>
        T& set(const key_path_t& _keys, const T& _val)
        {
            if (_keys.empty()) {
                THROW(SYS_INVALID_INPUT_PARAM, "\"set\" requires at least one key");
            }

            std::any* cur_val = nullptr;
            for (const auto& key : _keys) {
                if (!cur_val) {
                    cur_val = &root_[key];
                    continue;
                }

                if (!cur_val->has_value()) {
                    *cur_val = std::unordered_map<std::string, std::any>();
                }

                try {
                    cur_val = &(std::any_cast<std::unordered_map<std::string, std::any>&>(*cur_val)[key]);
                }
                catch (const std::bad_any_cast&) {
                    THROW(INVALID_ANY_CAST, "value was not a map");
                }
            }

            *cur_val = _val;

            return std::any_cast<T&>(*cur_val);
        } // set with path

        template <typename T>
        T& get(const std::string& _key)
        {
            try {
                return std::any_cast<T&>(root_.at(_key));
            }
            catch (const std::bad_any_cast&) {
                THROW(INVALID_ANY_CAST, fmt::format("value at {} was incorrect type", _key));
            }
            catch (const std::out_of_range&) {
                THROW(KEY_NOT_FOUND, fmt::format("key \"{}\" not found in map.", _key));
            }
        } // get

        template <typename T>
        T& get(const key_path_t& _keys)
        {
            if (_keys.empty()) {
                THROW(SYS_INVALID_INPUT_PARAM, "\"get\" requires at least one key");
            }

            std::any* cur_val = nullptr;
            for (const auto& key : _keys) {
                try {
                    if (!cur_val) {
                        cur_val = &root_.at(key);
                        continue;
                    }

                    try {
                        cur_val = &(std::any_cast<std::unordered_map<std::string, std::any>&>(*cur_val).at(key));
                    }
                    catch (const std::bad_any_cast&) {
                        THROW(INVALID_ANY_CAST, "value was not a map");
                    }
                }
                catch (const std::out_of_range&) {
                    THROW(KEY_NOT_FOUND, fmt::format("key \"{}\" not found in map.", key));
                }
            }

            try {
                return std::any_cast<T&>(*cur_val);
            }
            catch (const std::bad_any_cast&) {
                THROW(INVALID_ANY_CAST, "value was incorrect type");
            }
        } // get with path

        template <typename T>
        T remove(const std::string& _key)
        {
            auto find_it = root_.find(_key);
            if (find_it == root_.end()) {
                THROW(KEY_NOT_FOUND, fmt::format("key \"{}\" not found in map.", _key));
            }
            T val = find_it->second;
            root_.erase(find_it);
            return val;
        }

        void remove(const std::string& _key);

        std::unordered_map<std::string, std::any>& map()
        {
            return root_;
        }

    private:
        error load_json_object(const std::string& _filename);
        error load_json_object(const nlohmann::json& _json);

        std::any convert_json(const nlohmann::json& _json);

        error copy_and_swap(const std::unordered_map<std::string, std::any>& _object_to_swap_in);

        std::string file_name_;                             // full path to file
        std::unordered_map<std::string, std::any> root_;    // root config object
    }; // class configuration_parser

    std::string to_env( const std::string& );
} // namespace irods

#endif // IRODS_CONFIGURATION_PARSER_HPP
