#include <sstream>
#include <utility>

#include "genquery_ast_types.hpp"
#include "genquery_wrapper.hpp"

namespace irods::experimental::api::genquery {
    wrapper::wrapper(std::istream* istream)
        : _scanner(*this)
        , _parser(_scanner, *this)
        , _select{}
        , _location(0) {
        _scanner.switch_streams(istream, nullptr);
        _parser.parse(); // TODO: handle error here
    }

    Select
    wrapper::parse(std::istream& istream) {
        wrapper wrapper(&istream);
        return std::move(wrapper._select);
    }

    Select
    wrapper::parse(const char* s) {
        std::istringstream iss(s);
        return parse(iss);
    }

    Select
    wrapper::parse(const std::string& s) {
        std::istringstream iss(s);
        return parse(iss);
    }

    void
    wrapper::increaseLocation(uint64_t location) {
        _location += location;
    }

    uint64_t
    wrapper::location() const {
        return _location;
    }
} // namespace irods::experimental::api::genquery

