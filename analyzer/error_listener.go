package analyzer

import (
	"github.com/antlr4-go/antlr/v4"
	"strconv"
)

// SyntaxErrorListener 错误监听器
type SyntaxErrorListener struct {
	*antlr.DefaultErrorListener
	isOnlyComment *bool
	Errors        []string
}

// NewSyntaxErrorListener 创建错误监听器
func NewSyntaxErrorListener(isOnlyComment *bool) *SyntaxErrorListener {
	return &SyntaxErrorListener{
		isOnlyComment: isOnlyComment,
	}
}

// SyntaxError 处理语法错误
func (l *SyntaxErrorListener) SyntaxError(recognizer antlr.Recognizer, offendingSymbol any, line, column int, msg string, e antlr.RecognitionException) {
	if token, ok := offendingSymbol.(antlr.Token); ok && token.GetTokenType() == antlr.TokenEOF && *l.isOnlyComment {
		return
	}
	l.Errors = append(l.Errors, "line "+strconv.Itoa(line)+":"+strconv.Itoa(column)+" "+msg)
}
