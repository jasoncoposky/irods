%{
        #include <iostream>
        #include "genquery_scanner.hpp"
        #include "genquery_wrapper.hpp"
        #include "genquery_parser_bison_generated.hpp"
        #include "location.hh"

        #define yyterminate() gq::Parser::make_END_OF_INPUT(gq::location());

        #define YY_USER_ACTION _wrapper.increaseLocation(yyleng);
%}

%option c++
%option nodefault
%option noyywrap
%option yyclass="scanner"
%option prefix="Genquery_"

%%

[ \t\n]                ;
'(''|[^'])*'           { yytext[yyleng-1] = '\0'; ++yytext; return gq::Parser::make_STRING_LITERAL(yytext, gq::location()); }
select                 return gq::Parser::make_SELECT(gq::location());
where                  return gq::Parser::make_WHERE(gq::location());
like                   return gq::Parser::make_LIKE(gq::location());
in                     return gq::Parser::make_IN(gq::location());
between                return gq::Parser::make_BETWEEN(gq::location());
SELECT                 return gq::Parser::make_SELECT(gq::location());
WHERE                  return gq::Parser::make_WHERE(gq::location());
LIKE                   return gq::Parser::make_LIKE(gq::location());
IN                     return gq::Parser::make_IN(gq::location());
BETWEEN                return gq::Parser::make_BETWEEN(gq::location());
"="                    return gq::Parser::make_EQUAL(gq::location());
"!="                   return gq::Parser::make_NOT_EQUAL(gq::location());
"<>"                   return gq::Parser::make_NOT_EQUAL(gq::location());
"<"                    return gq::Parser::make_LESS_THAN(gq::location());
"<="                   return gq::Parser::make_LESS_THAN_OR_EQUAL_TO(gq::location());
">"                    return gq::Parser::make_GREATER_THAN(gq::location());
">="                   return gq::Parser::make_GREATER_THAN_OR_EQUAL_TO(gq::location());
begin_of               return gq::Parser::make_BEGINNING_OF(gq::location());
parent_of              return gq::Parser::make_PARENT_OF(gq::location());
BEGIN_OF               return gq::Parser::make_BEGINNING_OF(gq::location());
PARENT_OF              return gq::Parser::make_PARENT_OF(gq::location());
"||"                   return gq::Parser::make_CONDITION_OR(gq::location());
"||="                  return gq::Parser::make_CONDITION_OR_EQUAL(gq::location());
"&&"                   return gq::Parser::make_CONDITION_AND(gq::location());
not                    return gq::Parser::make_CONDITION_NOT(gq::location());
and                    return gq::Parser::make_AND(gq::location());
or                     return gq::Parser::make_CONDITION_OR(gq::location());
NOT                    return gq::Parser::make_CONDITION_NOT(gq::location());
AND                    return gq::Parser::make_AND(gq::location());
OR                     return gq::Parser::make_CONDITION_OR(gq::location());
,                      return gq::Parser::make_COMMA(gq::location());
"("                    return gq::Parser::make_OPEN_PAREN(gq::location());
")"                    return gq::Parser::make_CLOSE_PAREN(gq::location());
[a-zA-Z][a-zA-Z0-9_]*  return gq::Parser::make_IDENTIFIER(yytext, gq::location());
.                      std::cerr << "scanner: unknown character [" << yytext << "]" << std::endl; // TODO: improve error handling
<<EOF>>                return yyterminate();

%%
