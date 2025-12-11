lexer grammar SQLSplitLexer;

@header {
    package io.kestra.sql.grammar;
}

// Whitespace
WS                  : [ \t\r\n]+ ;

// Comments
LINE_COMMENT        : '--' ~[\r\n]* ;
BLOCK_COMMENT       : '/*' .*? '*/' ;

// Strings
SINGLE_QUOTE_STRING : '\'' ( '\'\'' | ~'\'' )* '\'' ;
DOUBLE_QUOTE_STRING : '"'  ( '""'   | ~'"'   )* '"' ;

// PostgreSQL dollar-quoted strings: $$…$$ or $tag$ … $tag$
DOLLAR_QUOTE
    : '$' TAG? '$' .*? '$' TAG? '$'
    ;
fragment TAG : [A-Za-z_][A-Za-z_0-9]* ;

// Procedural block keywords
BEGIN_KW            : [Bb][Ee][Gg][Ii][Nn] ;
END_KW              : [Ee][Nn][Dd] ;

// Delimiters
SEMICOLON           : ';' ;
GO_DELIMITER        : [Gg][Oo] ;
SLASH_DELIMITER     : '/' ;

// Keywords (fallback)
KEYWORD             : [A-Za-z_][A-Za-z_0-9]* ;

// Fallback token
OTHER               : . ;