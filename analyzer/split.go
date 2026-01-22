package analyzer

import (
	"strings"

	"github.com/antlr4-go/antlr/v4"
)

// SplitSQL 使用parser包拆分SQL语句，保留原始缩进和换行
func SplitSQL[T antlr.Lexer](lexer T) []string {
	var result []string

	// 逐个读取token，直到遇到EOF
	var s strings.Builder
	for {
		token := lexer.NextToken()
		if token.GetTokenType() == antlr.TokenEOF {
			break
		}
		s.WriteString(token.GetText())
		// 分号则是语句结束
		if token.GetText() == ";" {
			result = append(result, strings.TrimSpace(s.String()))
			s.Reset()
		}
	}
	// 处理最后一条可能没有分号结束的语句
	if s.Len() > 0 {
		result = append(result, strings.TrimSpace(s.String()))
	}

	return result
}
