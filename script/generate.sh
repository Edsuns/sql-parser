#!/bin/sh

# 显示帮助信息
show_help() {
    echo "用法: $0 <parser_dir> [--fix-starrocks-parser]"
    echo "  <parser_dir>  - parser目录的路径，包含.g4文件"
    echo "  --fix-starrocks-parser  - 可选参数，修复生成的starrocks_parser.go文件"
    echo ""
    echo "示例: $0 ../internal/spark/parser"
    echo "示例: $0 ../internal/starrocks/parser StarRocks.g4 --fix-starrocks-parser"
}

# 检查是否提供了第一个参数
if [ $# -eq 0 ]; then
    show_help
    exit 0
fi

# 解析参数
fix_starrocks=false
fix_mysql=false
parser_dir=""
g4_files=""

while [ $# -gt 0 ]; do
    case $1 in
        --fix-starrocks-parser)
            fix_starrocks=true
            shift
            ;;
        --fix-mysql-parser)
            fix_mysql=true
            shift
            ;;
        *)
            # 第一个非选项参数是parser_dir
            if [ -z "$parser_dir" ]; then
                parser_dir="$1"
            else
                # 后续的非选项参数是g4文件
                g4_files="$g4_files $1"
            fi
            shift
            ;;
    esac
done

# 检查是否提供了parser_dir
if [ -z "$parser_dir" ]; then
    show_help
    exit 1
fi

# 定义lib目录路径（相对于项目根目录）
lib_dir="./lib"

# 创建lib目录（如果不存在）
mkdir -p "$lib_dir"

# 获取lib目录的绝对路径
lib_dir_abs="$(readlink -f "$lib_dir")"

# 定义antlr版本和下载URL
antlr_version="4.13.2"
antlr_jar="antlr-$antlr_version-complete.jar"
antlr_url="https://www.antlr.org/download/$antlr_jar"

# 完整的antlr jar路径（使用绝对路径）
antlr_path="$lib_dir_abs/$antlr_jar"

# 检查是否需要下载antlr jar文件
if [ ! -f "$antlr_path" ]; then
    echo "正在下载 $antlr_jar 到 $lib_dir_abs..."
    if command -v wget >/dev/null 2>&1; then
        wget -O "$antlr_path" "$antlr_url"
    elif command -v curl >/dev/null 2>&1; then
        curl -o "$antlr_path" "$antlr_url"
    else
        echo "Error: 需要wget或curl来下载antlr jar文件"
        exit 1
    fi
    
    if [ $? -ne 0 ]; then
        echo "Error: 下载antlr jar文件失败"
        exit 1
    fi
    
    echo "下载完成: $antlr_path"
fi

# 检查是否找到antlr jar文件
if [ ! -f "$antlr_path" ]; then
    echo "Error: 未找到antlr*.jar文件"
    exit 1
fi

# 保存当前脚本所在目录的绝对路径
SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"

# 保存生成的目录的绝对路径
GENERATED_DIR="$(readlink -f "$parser_dir")"

cd "$parser_dir"

# 获取g4文件列表
if [ -n "$g4_files" ]; then
    # 使用解析出的g4_files参数
    echo "使用指定的g4文件: $g4_files"
else
    # 否则查找所有.g4文件
    g4_files=$(ls *.g4 2>/dev/null)
    
    # 检查是否找到g4文件
    if [ -z "$g4_files" ]; then
        echo "Error: 未找到g4文件"
        exit 1
    fi
fi

# 如果指定了--fix-mysql-parser参数，先运行修复脚本
if [ "$fix_mysql" = true ]; then
    echo "检测到--fix-mysql-parser参数，运行修复脚本..."
    # 运行修复脚本，生成临时.g4文件
    "$SCRIPT_DIR/fix_mysql_parser.sh" "$GENERATED_DIR"
    
    # 记录原始.g4文件，用于后续清理
    original_g4_files="$g4_files"
    
    # 更新g4_files为修复后的文件
    g4_files=$(ls *.g4 2>/dev/null)
    echo "使用修复后的g4文件: $g4_files"
fi

# 直接使用java命令执行antlr工具，传入所有g4文件（使用相对路径）
java -Xmx500M -cp "$antlr_path:$CLASSPATH" org.antlr.v4.Tool -Dlanguage=Go -no-visitor -package parser $g4_files

echo "Generate $parser_dir success!"

# 如果指定了--fix-mysql-parser参数，清理临时生成的.g4文件
if [ "$fix_mysql" = true ]; then
    echo "清理MySQL临时.g4文件..."
    # 删除修复脚本生成的.bak文件和可能的临时.g4文件
    # 首先恢复原始.g4文件（如果被覆盖）
    for g4_file in $original_g4_files; do
        if [ -f "$g4_file.bak" ]; then
            mv "$g4_file.bak" "$g4_file"
            echo "恢复原始文件: $g4_file"
        fi
    done
fi

# 如果指定了--fix-starrocks-parser参数，调用专门的修复脚本处理生成的文件
if [ "$fix_starrocks" = true ]; then
    # 修复脚本需要绝对路径
    starrocks_parser="$GENERATED_DIR/starrocks_parser.go"
    if [ -f "starrocks_parser.go" ]; then
        echo "调用 starrocks_parser.go 修复脚本..."
        "$SCRIPT_DIR/fix_starrocks_parser.sh" "$GENERATED_DIR/starrocks_parser.go"
    else
        echo "警告: 当前目录下未找到starrocks_parser.go文件，路径为: $(pwd)"
    fi
fi
