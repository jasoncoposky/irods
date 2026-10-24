#include "irods/escape_utilities.hpp"

#include "irods/irods_exception.hpp"
#include "irods/rodsErrorTable.h"

#include <fmt/format.h>

#include <exception>

namespace irods
{
    auto single_quotes_to_hex(const std::string& _s) -> std::string
    {
        try {
            std::string out;
            out.reserve(_s.size());
            std::size_t prev = 0;
            for (std::size_t pos = _s.find('\''); pos != std::string::npos; pos = _s.find('\'', prev)) {
                out.append(_s, prev, pos - prev);
                out.append("\\x27");
                prev = pos + 1;
            }
            out.append(_s, prev, std::string::npos);
            return out;
        }
        catch (const std::exception& e) {
            THROW(SYS_LIBRARY_ERROR, fmt::format("{}: {}", __func__, e.what()));
        }
    } // single_quotes_to_hex
} // namespace irods
