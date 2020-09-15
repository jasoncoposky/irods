#ifndef GENQUERY_WRAPPER_HPP
#define GENQUERY_WRAPPER_HPP

#include <cstdint>
#include <memory>
#include <string>

#include "genquery_ast_types.hpp"
#include "genquery_parser_bison_generated.hpp"
#include "genquery_scanner.hpp"

namespace irods::experimental::api::genquery {
    class Wrapper {
    public:
        explicit Wrapper(std::istream*);

        static Select parse(std::istream&);
        static Select parse(const char*);
        static Select parse(const std::string&);

        friend class Parser;
        friend class Scanner;
    private:
        void increaseLocation(uint64_t);
        uint64_t location() const;

        Scanner _scanner;
        Parser _parser;
        Select _select;
        uint64_t _location;
    };
}

#endif // GENQUERY_WRAPPER_HPP
