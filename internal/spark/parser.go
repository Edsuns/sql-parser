package spark

import (
	"github.com/Edsuns/sql-parser/analyzer"
	"github.com/Edsuns/sql-parser/internal/spark/parser"
	"github.com/antlr4-go/antlr/v4"
)

func makeLexer(sql string) *parser.SqlBaseLexer {
	// 创建字符流
	input := analyzer.NewCaseInsensitiveInputStream(sql)

	// 创建词法分析器
	lexer := parser.NewSqlBaseLexer(input)
	lexer.RemoveErrorListeners()
	return lexer
}

func makeParser(lexer antlr.Lexer) *parser.SqlBaseParser {
	// 创建词法符号流
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)

	// 创建语法分析器
	p := parser.NewSqlBaseParser(stream)
	p.RemoveErrorListeners()
	return p

}
