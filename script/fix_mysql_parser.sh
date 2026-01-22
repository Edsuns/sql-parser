#!/bin/sh

# 修复 MySQL 解析器代码，参考 transformGrammar.py

fix_lexer() {
    local file_path=$1
    echo "Altering $file_path"
    
    # 备份文件
    cp "$file_path" "$file_path.bak"
    
    # 修复词法分析器：将 'this.' 替换为 'p.'（如果行中包含 '}?' 字符串），否则替换为 'l.'
    awk '{
        if (index($0, "this.") > 0 && index($0, "}?") > 0) {
            gsub(/this\./, "p.")
        } else if (index($0, "this.") > 0) {
            gsub(/this\./, "l.")
        }
        print
    }' "$file_path.bak" > "$file_path"
    
    echo "Writing ..."
}

fix_parser() {
    local file_path=$1
    echo "Altering $file_path"
    
    # 备份文件
    cp "$file_path" "$file_path.bak"
    
    # 修复语法分析器：将所有 'this.' 替换为 'p.'
    sed 's/this\./p./g' "$file_path.bak" > "$file_path"
    
    echo "Writing ..."
}

main() {
    local parser_dir=$1
    
    # 修复语法分析器文件
    for file in "$parser_dir"/*Parser.g4; do
        fix_parser "$file"
    done
    
    # 修复词法分析器文件
    for file in "$parser_dir"/*Lexer.g4; do
        fix_lexer "$file"
    done
}

# 检查参数
if [ $# -eq 0 ]; then
    echo "用法: $0 <parser_dir>"
    echo "示例: $0 ../internal/mysql/parser"
    exit 1
fi

main "$1"
