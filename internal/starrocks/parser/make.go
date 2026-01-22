package parser

import "github.com/antlr4-go/antlr/v4"

// MakeStarRocksLexer produces a new lexer instance for the optional input antlr.CharStream.
func MakeStarRocksLexer(input antlr.CharStream) *StarRocksLexer {
	lex := NewStarRocksLexer(input)
	lex.sqlMode = 32
	return lex
}
