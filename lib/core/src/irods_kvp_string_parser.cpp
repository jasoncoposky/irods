#include "irods/irods_log.hpp"

// =-=-=-=-=-=-=-
#include "irods/irods_kvp_string_parser.hpp"
#include "irods/irods_stacktrace.hpp"

// =-=-=-=-=-=-=-
// rods includes
#include "irods/rodsErrorTable.h"
#include <sstream>
#include <set>
#include <string_view>
#include <vector>

namespace irods {
    namespace {
        auto split_string(std::string_view _str, std::string_view _delims, bool _compress = false) -> std::vector<std::string>
        {
            std::vector<std::string> out;
            std::size_t pos = 0;
            while (true) {
                const auto delim_pos = _delims.empty() ? std::string_view::npos : _str.find_first_of(_delims, pos);
                if (delim_pos == std::string_view::npos) {
                    out.emplace_back(_str.substr(pos));
                    break;
                }
                out.emplace_back(_str.substr(pos, delim_pos - pos));
                if (_compress) {
                    const auto next_pos = _str.find_first_not_of(_delims, delim_pos);
                    pos = (next_pos == std::string_view::npos) ? _str.size() : next_pos;
                }
                else {
                    pos = delim_pos + 1;
                }
            }
            return out;
        }
    } // anonymous namespace
/// =-=-=-=-=-=-=-
/// @brief function to return defined delimiter
    std::string kvp_delimiter() {
        return KVP_DEF_DELIMITER;
    }

/// =-=-=-=-=-=-=-
/// @brief function to return defined delimiter
    std::string kvp_association() {
        return KVP_DEF_ASSOCIATION;
    }

/// =-=-=-=-=-=-=-
/// @brief function to return defined delimiter
    std::string kvp_escape() {
        return KVP_DEF_ESCAPE;
    }

/// =-=-=-=-=-=-=-
/// @brief given a string, break the string along the kvp association and then
///        place the pair into the map
    static error parse_token_into_kvp(
        const std::string& _token,
        kvp_map_t&         _kvp,
        const std::string& _assoc ) {
        // =-=-=-=-=-=-=-
        // split along the associative delimiter and place into the map
        const std::vector< std::string > token_vec = split_string( _token, _assoc, true );
        if ( token_vec.size() != 2 ) {
            std::stringstream msg;
            msg << "token vector size != 2 during parsing of ["
                << _token
                << "]";
            return ERROR(
                       SYS_INVALID_INPUT_PARAM,
                       msg.str() );
        }

        _kvp[ token_vec[0] ] = token_vec[1];

        return SUCCESS();

    } // parse_token_into_kvp

/// =-=-=-=-=-=-=-
/// @brief given a string, break the string along the delimiter and then
///        break the tokens along the assignment for key-value pairs
    error parse_kvp_string(
        const std::string& _string,
        kvp_map_t&         _kvp,
        const std::string& _assoc,
        const std::string& _delim ) {

        // =-=-=-=-=-=-=-
        // parse the string into tokens split by the delimiter
        const std::vector< std::string > tokens = split_string( _string, _delim, false );
        for( const auto& token : tokens ) {
            if( token.empty() ) {
                    continue;
            }

            // =-=-=-=-=-=-=-
            // now that the string is broken into tokens we need to
            // extract the key and value to put them into the map
            error ret = parse_token_into_kvp( token, _kvp, _assoc );
            if( !ret.ok() ) {
                return PASS( ret );
            }
        }

        return SUCCESS();

    } // parse_kvp_string

    /// =-=-=-=-=-=-=-
    /// @brief given a string, break the string along the delimiter and then
    ///        break the tokens along the assignment for key-value pairs
    error parse_escaped_kvp_string(
        const std::string& _string,
        kvp_map_t&         _kvp,
        const std::string& _assoc,
        const std::string& _delim,
        const std::string& _escape ) {
        if ( _delim.empty() || _assoc.empty() || _escape.empty() ) {
            return ERROR( SYS_BAD_INPUT, "_delim, _assoc, and _escape may not be empty" );
        }
        if ( _delim == _escape || _delim == _assoc || _assoc == _escape ||
                _assoc.starts_with( _delim ) || _delim.starts_with( _assoc ) ) {
            return ERROR( SYS_BAD_INPUT, "_delim, _assoc, and _escape must all be distinct in calls to parse_kvp_string" );
        }
        if ( _assoc.find( _delim ) != std::string::npos || _assoc.find( _escape ) != std::string::npos ||
                _delim.find( _assoc ) != std::string::npos || _delim.find( _escape ) != std::string::npos ||
                _escape.find( _assoc ) != std::string::npos || _escape.find( _delim ) != std::string::npos ) {
            return ERROR( SYS_BAD_INPUT, "_delim, _assoc, and _escape may not contain each other in calls to parse_kvp_string" );
        }
        std::stringstream key;
        std::stringstream value;
        bool assoc_encountered = false;
        const std::string_view str_sv{_string};
        for ( size_t i = 0; i < _string.size(); ) {
            if ( str_sv.substr( i ).starts_with( _escape ) ) {
                i += _escape.size();
                if ( i >= _string.size() ) {
                    return ERROR( SYS_BAD_INPUT, "kvp ended in an unescaped _escape token" );
                }
            }
            else if ( str_sv.substr( i ).starts_with( _assoc ) ) {
                if ( assoc_encountered ) {
                    return ERROR( SYS_BAD_INPUT, "unescaped _assoc token encountered in key or value" );
                }
                assoc_encountered = true;
                i += _assoc.size();
                continue;
            }
            else if ( str_sv.substr( i ).starts_with( _delim ) ) {
                if ( !assoc_encountered ) {
                    return ERROR( SYS_BAD_INPUT,
                                  "no unescaped _assoc token encountered before unescaped delimiter when parsing kvp" );
                }
                _kvp[ key.str() ] = value.str();
                assoc_encountered = false;
                key.str( "" );
                value.str( "" );
                i += _delim.size();
                continue;
            }

            ( assoc_encountered ? value : key ) << _string[i];
            ++i;
        }

        if ( !key.str().empty() && !assoc_encountered ) {
            return ERROR( SYS_BAD_INPUT,
                          "no unescaped _assoc token encountered before end of string when parsing kvp" );
        }
        else if ( assoc_encountered ) {
            _kvp[ key.str() ] = value.str();
        }
        return SUCCESS();
    }

    std::string kvp_string(
        const kvp_map_t& _kvp ) {
        std::stringstream str;
        bool first = true;
        for ( kvp_map_t::const_iterator it = _kvp.begin(); it != _kvp.end(); ++it ) {
            if ( first ) {
                first = false;
            }
            else {
                str << kvp_delimiter();
            }
            str << it->first << kvp_association() << it->second;
        }
        return str.str();
    }

    std::string escape_string( const std::string& _string,
                               const std::string& _escape_token,
                               const std::set<std::string>& _special_tokens ) {
        std::stringstream escaped_str;
        const std::string_view str_sv{_string};
        for ( size_t i = 0; i < _string.size(); ) {
            std::set<std::string>::const_iterator tok;
            for ( tok = _special_tokens.begin(); tok != _special_tokens.end(); ++tok ) {
                if ( str_sv.substr( i ).starts_with( *tok ) ) {
                    escaped_str << _escape_token << *tok;
                    i += tok->size();
                    break;
                }
            }
            if ( tok != _special_tokens.end() ) {
                continue;
            }
            else if ( _special_tokens.count( _escape_token ) == 0 &&
                      str_sv.substr( i ).starts_with( _escape_token ) ) {
                escaped_str << _escape_token << _escape_token;
                i += _escape_token.size();
                continue;
            }
            escaped_str << _string[i];
            ++i;
        }
        return escaped_str.str();
    }


    std::string escaped_kvp_string(
        const kvp_map_t& _kvp ) {
        std::stringstream str;
        std::set<std::string> special_tokens;
        special_tokens.insert( kvp_delimiter() );
        special_tokens.insert( kvp_association() );
        bool first = true;
        for ( kvp_map_t::const_iterator it = _kvp.begin(); it != _kvp.end(); ++it ) {
            if ( first ) {
                first = false;
            }
            else {
                str << kvp_delimiter();
            }
            str << escape_string( it->first, kvp_escape(), special_tokens );
            str << kvp_association();
            str << escape_string( it->second, kvp_escape(), special_tokens );
        }
        return str.str();
    }
} // namespace irods
