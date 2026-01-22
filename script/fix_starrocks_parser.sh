#!/bin/sh

# 显示帮助信息
show_help() {
    echo "用法: $0 <parser_file>"
    echo "  <parser_file>  - starrocks_parser.go文件的路径"
    echo ""
    echo "示例: $0 ../internal/starrocks/parser/starrocks_parser.go"
}

# 检查是否提供了参数
if [ $# -eq 0 ]; then
    show_help
    exit 0
fi

# 获取parser文件路径
parser_file="$1"

# 检查文件是否存在
if [ ! -f "$parser_file" ]; then
    echo "错误: 文件不存在: $parser_file"
    exit 1
fi

echo "处理 $parser_file 文件..."

# 删除多余的 NewEmptyStatementContext 函数
sed -i '' '/func NewEmptyStatementContext() \*StatementContext {/,/^}$/d' "$parser_file"

# 删除该函数后面可能存在的空行
sed -i '' -e '/^$/N' -e '/^\n$/D' "$parser_file"

# 将 ParserRuleContext 替换为 antlr.ParserRuleContext，但只替换没有 antlr. 前缀的
sed -i '' 's/\([^a-zA-Z.]\)ParserRuleContext/\1antlr.ParserRuleContext/g' "$parser_file"
sed -i '' 's/^ParserRuleContext/antlr.ParserRuleContext/g' "$parser_file"

echo "$parser_file 文件处理完成！"