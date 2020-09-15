#include <sstream>
#include <utility>

#include "genquery_ast_types.hpp"
#include "genquery_wrapper.hpp"

namespace irods::experimental::api::genquery {
    Wrapper::Wrapper(std::istream* istream)
        : _scanner(*this)
        , _parser(_scanner, *this)
        , _select{}
        , _location(0) {
        _scanner.switch_streams(istream, nullptr);
        _parser.parse(); // TODO: handle error here
    }

    Select
    Wrapper::parse(std::istream& istream) {
        Wrapper wrapper(&istream);
        return std::move(wrapper._select);
    }

    Select
    Wrapper::parse(const char* s) {
        std::istringstream iss(s);
        return parse(iss);
    }

    Select
    Wrapper::parse(const std::string& s) {
        std::istringstream iss(s);
        return parse(iss);
    }

    void
    Wrapper::increaseLocation(uint64_t location) {
        _location += location;
    }

    uint64_t
    Wrapper::location() const {
        return _location;
    }
} // namespace irods::experimental::api::genquery

