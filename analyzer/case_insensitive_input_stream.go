package analyzer

import (
	"github.com/antlr4-go/antlr/v4"
	"unicode"
)

type caseInsensitiveInputStream struct {
	antlr.CharStream
}

func NewCaseInsensitiveInputStream(input string) antlr.CharStream {
	return &caseInsensitiveInputStream{
		CharStream: antlr.NewInputStream(input),
	}
}

func (is *caseInsensitiveInputStream) LA(offset int) int {
	data := is.CharStream.LA(offset)
	return int(unicode.ToUpper(rune(data)))
}
