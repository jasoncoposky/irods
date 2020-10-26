%skeleton "lalr1.cc" /* -*- C++ -*- */
%require "3.0"
%defines
%define parser_class_name { Parser }
%define api.token.constructor
%define api.value.type variant
%define parse.assert
%define api.namespace { irods::experimental::api::genquery }
%code requires
{
    #include <iostream>
    #include <string>
    #include <vector>
    #include "genquery_ast_types.hpp"

    namespace irods::experimental::api::genquery {
        class scanner;
        class wrapper;
    }

    namespace gq = irods::experimental::api::genquery;
}

%code top
{
    #include "genquery_scanner.hpp"
    #include "genquery_parser_bison_generated.hpp"
    #include "genquery_wrapper.hpp"
    #include "location.hh"

    static gq::Parser::symbol_type yylex(gq::scanner& scanner, gq::wrapper& wrapper) {
        return scanner.get_next_token();
    }
}

%lex-param { gq::scanner& scanner } { gq::wrapper& wrapper }
%parse-param { gq::scanner& scanner } { gq::wrapper& wrapper }
%locations
%define parse.trace
%define parse.error verbose

%define api.token.prefix {GENQUERY_TOKEN_}

%token <std::string> IDENTIFIER STRING_LITERAL
%token SELECT NO_DISTINCT WHERE AND COMMA OPEN_PAREN CLOSE_PAREN
%token BETWEEN EQUAL NOT_EQUAL BEGINNING_OF LIKE IN PARENT_OF
%token LESS_THAN GREATER_THAN LESS_THAN_OR_EQUAL_TO GREATER_THAN_OR_EQUAL_TO
%token CONDITION_OR CONDITION_AND CONDITION_NOT CONDITION_OR_EQUAL
%token END_OF_INPUT 0

%left CONDITION_OR
%left CONDITION_AND
%precedence CONDITION_NOT

%type<gq::Selections> selections;
%type<gq::Conditions> conditions;
%type<gq::Selection> selection;
%type<gq::Column> column;
%type<gq::SelectFunction> select_function;
%type<gq::Condition> condition;
%type<gq::ConditionExpression> condition_expression;
%type<std::vector<std::string>> list_of_string_literals;

%start select

%%

select:
    SELECT NO_DISTINCT selections  { wrapper._select.no_distinct = true; std::swap(wrapper._select.selections, $3); }
  | SELECT NO_DISTINCT selections WHERE conditions  { wrapper._select.no_distinct = true; std::swap(wrapper._select.selections, $3); std::swap(wrapper._select.conditions, $5); }
  | SELECT selections  { std::swap(wrapper._select.selections, $2); }
  | SELECT selections WHERE conditions  { std::swap(wrapper._select.selections, $2); std::swap(wrapper._select.conditions, $4); }

selections:
    selection  { $$ = gq::Selections{std::move($1)}; }
  | selections COMMA selection  { $1.push_back(std::move($3)); std::swap($$, $1); }

selection:
    column  { $$ = std::move($1); }
  | select_function  { $$ = std::move($1); }

column:
    IDENTIFIER  { $$ = gq::Column{std::move($1)}; }

select_function:
    IDENTIFIER OPEN_PAREN IDENTIFIER CLOSE_PAREN  { $$ = gq::SelectFunction{std::move($1), gq::Column{std::move($3)}}; }

conditions:
    condition  { $$ = gq::Conditions{std::move($1)}; }
  | conditions AND condition  { $1.push_back(std::move($3)); std::swap($$, $1); }

condition:
    column condition_expression  { $$ = gq::Condition(std::move($1), std::move($2)); }

condition_expression:
    LIKE STRING_LITERAL  { $$ = gq::ConditionLike(std::move($2)); }
  | IN OPEN_PAREN list_of_string_literals CLOSE_PAREN  { $$ = gq::ConditionIn(std::move($3)); }
  | BETWEEN STRING_LITERAL STRING_LITERAL { $$ = gq::ConditionBetween(std::move($2), std::move($3)); }
  | EQUAL STRING_LITERAL  { $$ = gq::ConditionEqual(std::move($2)); }
  | NOT_EQUAL STRING_LITERAL  { $$ = gq::ConditionNotEqual(std::move($2)); }
  | LESS_THAN STRING_LITERAL  { $$ = gq::ConditionLessThan(std::move($2)); }
  | LESS_THAN_OR_EQUAL_TO STRING_LITERAL  { $$ = gq::ConditionLessThanOrEqualTo(std::move($2)); }
  | GREATER_THAN STRING_LITERAL  { $$ = gq::ConditionGreaterThan(std::move($2)); }
  | GREATER_THAN_OR_EQUAL_TO STRING_LITERAL  { $$ = gq::ConditionGreaterThanOrEqualTo(std::move($2)); }
  | PARENT_OF STRING_LITERAL  { $$ = gq::ConditionParentOf(std::move($2)); }
  | BEGINNING_OF STRING_LITERAL  { $$ = gq::ConditionBeginningOf(std::move($2)); }
  | condition_expression CONDITION_AND condition_expression  { $$ = gq::ConditionOperator_And(std::move($1), std::move($3)); }
  | condition_expression CONDITION_OR  condition_expression  { $$ = gq::ConditionOperator_Or (std::move($1), std::move($3)); }
  | CONDITION_NOT condition_expression  { $$ = gq::ConditionOperator_Not(std::move($2)); }

list_of_string_literals:
    STRING_LITERAL  { $$ = std::vector<std::string>{std::move($1)}; }
  | list_of_string_literals COMMA STRING_LITERAL  { $1.push_back(std::move($3)); std::swap($$, $1); }

%%

void gq::Parser::error(const location& location, const std::string& message) {
    // TODO: improve error handling
    std::cerr << "Error: " << message << std::endl << "Error location: " << wrapper.location() << std::endl;
}
