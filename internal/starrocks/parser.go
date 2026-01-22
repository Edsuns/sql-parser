package starrocks

import (
	"github.com/Edsuns/sql-parser/analyzer"
	"github.com/Edsuns/sql-parser/internal/starrocks/parser"
	"github.com/antlr4-go/antlr/v4"
)

func makeLexer(sql string) *parser.StarRocksLexer {
	// 创建字符流
	input := analyzer.NewCaseInsensitiveInputStream(sql)

	// 创建词法分析器
	lexer := parser.MakeStarRocksLexer(input)
	lexer.RemoveErrorListeners()
	return lexer
}

func makeParser(lexer antlr.Lexer) *parser.StarRocksParser {
	// 创建词法符号流
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)

	// 创建语法分析器
	p := parser.NewStarRocksParser(stream)
	p.RemoveErrorListeners()
	return p
}
