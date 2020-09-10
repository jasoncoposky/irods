%{
        #include <iostream>
        #include "genquery_scanner.hpp"
        #include "genquery_wrapper.hpp"
        #include "genquery_parser_bison_generated.hpp"
        #include "location.hh"

        #define yyterminate() Genquery::Parser::make_END_OF_INPUT(Genquery::location());

        #define YY_USER_ACTION _wrapper.increaseLocation(yyleng);
%}

%option c++
%option nodefault
%option noyywrap
%option yyclass="Scanner"
%option prefix="Genquery_"

%%

[ \t\n]                ;
'(''|[^'])*'           { yytext[yyleng-1] = '\0'; ++yytext; return Genquery::Parser::make_STRING_LITERAL(yytext, Genquery::location()); }
select                 return Genquery::Parser::make_SELECT(Genquery::location());
where                  return Genquery::Parser::make_WHERE(Genquery::location());
like                   return Genquery::Parser::make_LIKE(Genquery::location());
in                     return Genquery::Parser::make_IN(Genquery::location());
between                return Genquery::Parser::make_BETWEEN(Genquery::location());
"="                    return Genquery::Parser::make_EQUAL(Genquery::location());
"!="                   return Genquery::Parser::make_NOT_EQUAL(Genquery::location());
"<>"                   return Genquery::Parser::make_NOT_EQUAL(Genquery::location());
"<"                    return Genquery::Parser::make_LESS_THAN(Genquery::location());
"<="                   return Genquery::Parser::make_LESS_THAN_OR_EQUAL_TO(Genquery::location());
">"                    return Genquery::Parser::make_GREATER_THAN(Genquery::location());
">="                   return Genquery::Parser::make_GREATER_THAN_OR_EQUAL_TO(Genquery::location());
begin_of               return Genquery::Parser::make_BEGINNING_OF(Genquery::location());
parent_of              return Genquery::Parser::make_PARENT_OF(Genquery::location());
"||"                   return Genquery::Parser::make_CONDITION_OR(Genquery::location());
"&&"                   return Genquery::Parser::make_CONDITION_AND(Genquery::location());
not                    return Genquery::Parser::make_CONDITION_NOT(Genquery::location());
and                    return Genquery::Parser::make_AND(Genquery::location());
,                      return Genquery::Parser::make_COMMA(Genquery::location());
"("                    return Genquery::Parser::make_OPEN_PAREN(Genquery::location());
")"                    return Genquery::Parser::make_CLOSE_PAREN(Genquery::location());
[a-zA-Z][a-zA-Z0-9_]*  return Genquery::Parser::make_IDENTIFIER(yytext, Genquery::location());
.                      std::cerr << "Scanner: unknown character [" << yytext << "]" << std::endl; // TODO: improve error handling
<<EOF>>                return yyterminate();

%%
