package hive

import (
	"github.com/Edsuns/sql-parser/internal/hive/parser"
	"github.com/antlr4-go/antlr/v4"
)

func makeLexer(sql string) *parser.HiveLexer {
	// 创建字符流
	input := antlr.NewInputStream(sql)

	// 创建词法分析器
	lexer := parser.NewHiveLexer(input)
	lexer.RemoveErrorListeners()
	return lexer
}

func makeParser(lexer antlr.Lexer) *parser.HiveParser {
	// 创建词法符号流
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)

	// 创建语法分析器
	p := parser.NewHiveParser(stream)
	p.RemoveErrorListeners()
	return p
}
