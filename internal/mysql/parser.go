package mysql

import (
	"sql-parser/internal/mysql/parser"

	"github.com/antlr4-go/antlr/v4"
)

func makeLexer(sql string) *parser.MySQLLexer {
	// 创建字符流
	input := antlr.NewInputStream(sql)

	// 创建词法分析器
	lexer := parser.NewMySQLLexer(input)
	lexer.RemoveErrorListeners()
	return lexer
}

func makeParser(lexer antlr.Lexer) *parser.MySQLParser {
	// 创建词法符号流
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)

	// 创建语法分析器
	p := parser.NewMySQLParser(stream)
	p.RemoveErrorListeners()
	return p
}
