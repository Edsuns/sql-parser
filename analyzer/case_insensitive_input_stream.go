package analyzer

import (
	"github.com/antlr4-go/antlr/v4"
	"unicode"
)

// caseInsensitiveInputStream adaptation of: https://github.com/StarRocks/starrocks/blob/3.5.11/fe/fe-core/src/main/java/com/starrocks/sql/parser/CaseInsensitiveStream.java
type caseInsensitiveInputStream struct {
	antlr.CharStream
}

func NewCaseInsensitiveInputStream(input string) antlr.CharStream {
	return &caseInsensitiveInputStream{
		CharStream: antlr.NewInputStream(input),
	}
}

func (is *caseInsensitiveInputStream) LA(offset int) int {
	result := is.CharStream.LA(offset)
	switch result {
	case 0, antlr.TokenEOF:
		return result
	default:
		return int(unicode.ToUpper(rune(result)))
	}
}
