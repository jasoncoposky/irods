%skeleton "lalr1.cc" /* -*- C++ -*- */
%require "3.0"
%defines
%define parser_class_name { Parser }
%define api.token.constructor
%define api.value.type variant
%define parse.assert
%define api.namespace { Genquery }
%code requires
{
    #include <iostream>
    #include <string>
    #include <vector>
    #include "genquery_ast_types.hpp"

    namespace Genquery {
        class Scanner;
        class Wrapper;
    }
}

%code top
{
    #include "genquery_scanner.hpp"
    #include "genquery_parser_bison_generated.hpp"
    #include "genquery_wrapper.hpp"
    #include "location.hh"

    static Genquery::Parser::symbol_type yylex(Genquery::Scanner& scanner, Genquery::Wrapper& wrapper) {
        return scanner.get_next_token();
    }
}

%lex-param { Genquery::Scanner& scanner } { Genquery::Wrapper& wrapper }
%parse-param { Genquery::Scanner& scanner } { Genquery::Wrapper& wrapper }
%locations
%define parse.trace
%define parse.error verbose

%define api.token.prefix {GENQUERY_TOKEN_}

%token <std::string> IDENTIFIER STRING_LITERAL
%token SELECT WHERE AND COMMA OPEN_PAREN CLOSE_PAREN
%token BETWEEN EQUAL NOT_EQUAL BEGINNING_OF LIKE IN PARENT_OF
%token LESS_THAN GREATER_THAN LESS_THAN_OR_EQUAL_TO GREATER_THAN_OR_EQUAL_TO
%token CONDITION_OR CONDITION_AND CONDITION_NOT CONDITION_OR_EQUAL
%token END_OF_INPUT 0

%left CONDITION_OR
%left CONDITION_AND
%precedence CONDITION_NOT

%type<Genquery::Selections> selections;
%type<Genquery::Conditions> conditions;
%type<Genquery::Selection> selection;
%type<Genquery::Column> column;
%type<Genquery::SelectFunction> select_function;
%type<Genquery::Condition> condition;
%type<Genquery::ConditionExpression> condition_expression;
%type<std::vector<std::string>> list_of_string_literals;

%start select

%%

select:
    SELECT selections  { std::swap(wrapper._select.selections, $2); }
  | SELECT selections WHERE conditions  { std::swap(wrapper._select.selections, $2); std::swap(wrapper._select.conditions, $4); }

selections:
    selection  { $$ = Genquery::Selections{std::move($1)}; }
  | selections COMMA selection  { $1.push_back(std::move($3)); std::swap($$, $1); }

selection:
    column  { $$ = std::move($1); }
  | select_function  { $$ = std::move($1); }

column:
    IDENTIFIER  { $$ = Genquery::Column{std::move($1)}; }

select_function:
    IDENTIFIER OPEN_PAREN IDENTIFIER CLOSE_PAREN  { $$ = Genquery::SelectFunction{std::move($1), Genquery::Column{std::move($3)}}; }

conditions:
    condition  { $$ = Genquery::Conditions{std::move($1)}; }
  | conditions AND condition  { $1.push_back(std::move($3)); std::swap($$, $1); }

condition:
    column condition_expression  { $$ = Genquery::Condition(std::move($1), std::move($2)); }

condition_expression:
    LIKE STRING_LITERAL  { $$ = Genquery::ConditionLike(std::move($2)); }
  | IN OPEN_PAREN list_of_string_literals CLOSE_PAREN  { $$ = Genquery::ConditionIn(std::move($3)); }
  | BETWEEN STRING_LITERAL STRING_LITERAL { $$ = Genquery::ConditionBetween(std::move($2), std::move($3)); }
  | EQUAL STRING_LITERAL  { $$ = Genquery::ConditionEqual(std::move($2)); }
  | NOT_EQUAL STRING_LITERAL  { $$ = Genquery::ConditionNotEqual(std::move($2)); }
  | LESS_THAN STRING_LITERAL  { $$ = Genquery::ConditionLessThan(std::move($2)); }
  | LESS_THAN_OR_EQUAL_TO STRING_LITERAL  { $$ = Genquery::ConditionLessThanOrEqualTo(std::move($2)); }
  | GREATER_THAN STRING_LITERAL  { $$ = Genquery::ConditionGreaterThan(std::move($2)); }
  | GREATER_THAN_OR_EQUAL_TO STRING_LITERAL  { $$ = Genquery::ConditionGreaterThanOrEqualTo(std::move($2)); }
  | PARENT_OF STRING_LITERAL  { $$ = Genquery::ConditionParentOf(std::move($2)); }
  | BEGINNING_OF STRING_LITERAL  { $$ = Genquery::ConditionBeginningOf(std::move($2)); }
  | condition_expression CONDITION_AND condition_expression  { $$ = Genquery::ConditionOperator_And(std::move($1), std::move($3)); }
  | condition_expression CONDITION_OR  condition_expression  { $$ = Genquery::ConditionOperator_Or (std::move($1), std::move($3)); }
  | CONDITION_NOT condition_expression  { $$ = Genquery::ConditionOperator_Not(std::move($2)); }

list_of_string_literals:
    STRING_LITERAL  { $$ = std::vector<std::string>{std::move($1)}; }
  | list_of_string_literals COMMA STRING_LITERAL  { $1.push_back(std::move($3)); std::swap($$, $1); }

%%

void Genquery::Parser::error(const location& location, const std::string& message) {
    // TODO: improve error handling
    std::cerr << "Error: " << message << std::endl << "Error location: " << wrapper.location() << std::endl;
}
