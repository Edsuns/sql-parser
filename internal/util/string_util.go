package util

// TrimQuotes 去除字符串首尾的单引号或双引号，保留内部的引号
func TrimQuotes(s string) string {
	if len(s) >= 2 {
		// 检查是否以单引号开头和结尾
		if s[0] == '\'' && s[len(s)-1] == '\'' {
			return s[1 : len(s)-1]
		}
		// 检查是否以双引号开头和结尾
		if s[0] == '"' && s[len(s)-1] == '"' {
			return s[1 : len(s)-1]
		}
		// 检查是否以反引号开头和结尾
		if s[0] == '`' && s[len(s)-1] == '`' {
			return s[1 : len(s)-1]
		}
	}
	return s
}
