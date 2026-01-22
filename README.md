# SQL Parser for Go

一个Go语言SQL解析器，支持Hive、Spark、StarRocks、TiDB和MySQL SQL，可以提取SQL语句的类型、数据库名和表名。

## 项目结构

```
sql-parser/
├── analyzer/                     # SQL依赖分析器
│   ├── dependency_analyzer.go    # 依赖分析器核心逻辑
│   ├── engine_type.go            # 数据库引擎类型定义
│   ├── split.go                  # SQL语句拆分逻辑
│   └── stmt_type.go              # SQL语句类型定义
├── internal/                     # 具体数据库实现
│   ├── hive/                     # Hive SQL实现
│   │   ├── dependency_analyzer.go      # Hive依赖分析器
│   │   ├── dependency_analyzer_test.go # Hive依赖分析器测试
│   │   ├── listener.go                 # Hive SQL监听器
│   │   ├── parser.go                   # Hive SQL解析器入口
│   │   └── parser/                     # ANTLR生成的解析器
│   ├── mysql/                    # MySQL SQL实现
│   │   ├── dependency_analyzer.go      # MySQL依赖分析器
│   │   ├── dependency_analyzer_test.go # MySQL依赖分析器测试
│   │   ├── listener.go                 # MySQL SQL监听器
│   │   ├── parser.go                   # MySQL SQL解析器入口
│   │   └── parser/                     # ANTLR生成的解析器
│   ├── spark/                    # Spark SQL实现
│   │   ├── dependency_analyzer.go      # Spark依赖分析器
│   │   ├── dependency_analyzer_test.go # Spark依赖分析器测试
│   │   ├── listener.go                 # Spark SQL监听器
│   │   ├── parser.go                   # Spark SQL解析器入口
│   │   ├── split_test.go               # SQL拆分测试
│   │   └── parser/                     # ANTLR生成的解析器
│   ├── starrocks/                # StarRocks SQL实现
│   │   ├── dependency_analyzer.go      # StarRocks依赖分析器
│   │   ├── dependency_analyzer_test.go # StarRocks依赖分析器测试
│   │   ├── listener.go                 # StarRocks SQL监听器
│   │   ├── parser.go                   # StarRocks SQL解析器入口
│   │   └── parser/                     # ANTLR生成的解析器
│   └── tidb/                     # TiDB SQL实现
│       ├── dependency_analyzer.go      # TiDB依赖分析器
│       ├── dependency_analyzer_test.go # TiDB依赖分析器测试
│       ├── listener.go                 # TiDB SQL监听器
│       ├── parser.go                   # TiDB SQL解析器入口
│       └── parser/                     # ANTLR生成的解析器
├── script/                       # 脚本工具
│   ├── generate.go               # 生成解析器的Go脚本
│   ├── generate.sh               # 生成解析器的Shell脚本
│   └── lib/                      # 脚本依赖库（不要提交到Git！）
├── analyzer.go                   # 暴露出来的分析器创建方法
├── go.mod                        # Go模块依赖文件
└── go.sum                        # Go模块依赖校验文件
```

## 功能特性

- 支持多种SQL语句类型的解析
- 自动提取数据库名和表名
- 支持SQL语句依赖分析
- 支持SQL语句拆分
- 简洁易用的API

## 使用方法

### 1. 安装依赖

最新版本号请看[Releases](https://github.com/Edsuns/sql-parser/releases)

```bash
go get github.com/Edsuns/sql-parser@0.1
```

```bash
go mod tidy
```

### 2. 生成解析器

生成代码使用到[ANTLR](https://www.antlr.org)，确保已经安装Java环境

```bash
go generate ./...
```

### 3. 示例代码

```go
package main

import (
	"fmt"

	parser "github.com/Edsuns/sql-parser"
	"github.com/Edsuns/sql-parser/analyzer"
)

func main() {
	// 创建Spark SQL依赖分析器
	a := parser.NewSparkDependencyAnalyzer()

	// 准备SQL语句
	sql := `SELECT * FROM my_db.my_table WHERE id > 100;
	        INSERT INTO another_db.another_table VALUES (1, 'test');`

	// 分析SQL依赖
	req := &analyzer.DependencyAnalyzeReq{
		DefaultCluster:  "cluster1",
		DefaultDatabase: "db1",
		Type:            analyzer.EngineSpark,
		SQL:             sql,
	}

	results, err := a.Analyze(req)
	if err != nil {
		fmt.Printf("分析失败: %v\n", err)
		return
	}

	// 输出分析结果
	for _, result := range results {
		fmt.Printf("语句: %s\n", result.Stmt)
		fmt.Printf("类型: %s\n", result.StmtType)
		fmt.Printf("读表: %v\n", result.Read)
		fmt.Printf("写表: %v\n", result.Write)
		fmt.Println()
	}
}
```

## 技术栈

- Go 1.24.10
- ANTLR 4.13.2
