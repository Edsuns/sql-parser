# SQL Parser for Go

一个Go语言SQL解析器，支持Hive、Spark、StarRocks、TiDB和MySQL SQL，提供血缘分析、DDL分析、注释修改和SQL拆分功能。

## 项目结构

```
sql-parser/
├── analyzer/                                # SQL分析器公共定义
│   ├── analyzer.go                          # 核心接口与数据结构定义
│   ├── enums.go                             # 引擎类型、语句类型、操作类型枚举
│   ├── split.go                             # SQL语句拆分工具函数
│   ├── error_listener.go                    # 语法错误监听器
│   └── case_insensitive_input_stream.go     # 大小写不敏感输入流
├── internal/                                # 具体数据库实现
│   ├── hive/                                # Hive SQL实现
│   │   ├── analyzer.go                      # Hive SQL分析器
│   │   ├── analyzer_test.go                 # Hive分析器测试
│   │   ├── listener.go                      # Hive SQL监听器
│   │   ├── parser.go                        # Hive SQL解析器入口
│   │   └── parser/                          # ANTLR生成的解析器
│   ├── mysql/                               # MySQL SQL实现
│   │   ├── analyzer.go                      # MySQL SQL分析器
│   │   ├── analyzer_test.go                 # MySQL分析器测试
│   │   ├── listener.go                      # MySQL SQL监听器
│   │   ├── parser.go                        # MySQL SQL解析器入口
│   │   └── parser/                          # ANTLR生成的解析器
│   ├── spark/                               # Spark SQL实现
│   │   ├── analyzer.go                      # Spark SQL分析器
│   │   ├── analyzer_test.go                 # Spark分析器测试
│   │   ├── listener.go                      # Spark SQL监听器
│   │   ├── parser.go                        # Spark SQL解析器入口
│   │   ├── split_test.go                    # SQL拆分测试
│   │   └── parser/                          # ANTLR生成的解析器
│   ├── starrocks/                           # StarRocks SQL实现
│   │   ├── analyzer.go                      # StarRocks SQL分析器
│   │   ├── analyzer_test.go                 # StarRocks分析器测试
│   │   ├── listener.go                      # StarRocks SQL监听器
│   │   ├── parser.go                        # StarRocks SQL解析器入口
│   │   └── parser/                          # ANTLR生成的解析器
│   ├── tidb/                                # TiDB SQL实现
│   │   ├── analyzer.go                      # TiDB SQL分析器
│   │   ├── analyzer_test.go                 # TiDB分析器测试
│   │   ├── visitor.go                       # TiDB SQL访问器
│   │   ├── split_test.go                    # SQL拆分测试
│   │   └── README.md                        # TiDB实现说明
│   └── util/                                # 内部工具函数
│       └── string_util.go                   # 字符串工具
├── script/                                  # 脚本工具
│   ├── generate.go                          # 生成解析器的Go脚本
│   ├── generate.sh                          # 生成解析器的Shell脚本
│   ├── fix_mysql_parser.sh                  # 修复MySQL解析器脚本
│   ├── fix_starrocks_parser.sh              # 修复StarRocks解析器脚本
│   └── lib/                                 # 脚本依赖库（不要提交到Git！）
├── analyzer.go                              # 暴露的分析器入口
├── go.mod                                   # Go模块依赖文件
└── go.sum                                   # Go模块依赖校验文件
```

## 功能特性

- **血缘分析**：分析SQL语句的读表和写表依赖关系
- **DDL分析**：解析DDL语句，提取表结构、字段信息和操作类型
- **注释修改**：生成修改字段注释的SQL语句
- **SQL拆分**：将多句SQL拆分为独立的单条语句
- 支持多种SQL语句类型的解析
- 自动提取信息并结构化返回

## 支持的引擎

| 引擎 | 类型标识 | 说明 |
|------|---------|------|
| MySQL | `mysql` | MySQL SQL |
| TiDB | `tidb` | TiDB SQL（基于TiDB Parser） |
| Spark | `spark` | Spark SQL |
| Hive | `hive` | Hive SQL |
| StarRocks | `starrocks` | StarRocks SQL |

## 使用方法

### 1. 安装依赖

最新版本号请看[Releases](https://github.com/Edsuns/sql-parser/releases)

```bash
go get github.com/Edsuns/sql-parser@0.2
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

#### 血缘分析

```go
package main

import (
	"testing"

	sqlparser "github.com/Edsuns/sql-parser"
	"github.com/Edsuns/sql-parser/analyzer"
)

func TestAnalyzeLineage(t *testing.T) {
	a := sqlparser.NewAnalyzer()

	results, err := a.AnalyzeLineage(&analyzer.AnalyzeReq{
		DefaultCluster:  "cluster1",
		DefaultDatabase: "db1",
		Type:            analyzer.EngineSpark,
		SQL:             "SELECT * FROM my_db.my_table WHERE id > 100; INSERT INTO another_db.another_table VALUES (1, 'test');",
	})

	expected := []*analyzer.LineageResult{
		{
			StmtType: analyzer.StmtTypeSelect,
			Stmt: "SELECT * FROM my_db.my_table WHERE id > 100;",
			Reads: []*analyzer.Dependency{
				{Cluster: "cluster1", Database: "my_db", Table: "my_table"},
			},
		},
		{
			StmtType: analyzer.StmtTypeInsert,
			Stmt: "INSERT INTO another_db.another_table VALUES (1, 'test');",
			Writes: []*analyzer.Dependency{
				{Cluster: "cluster1", Database: "another_db", Table: "another_table"},
			},
		},
	}

	assertEquals(t, expected, results)
}
```

#### DDL分析

```go
package main

import (
	"testing"

	sqlparser "github.com/Edsuns/sql-parser"
	"github.com/Edsuns/sql-parser/analyzer"
)

func TestAnalyzeDDL(t *testing.T) {
	a := sqlparser.NewAnalyzer()

	result, err := a.AnalyzeDDL(&analyzer.AnalyzeReq{
		DefaultCluster:  "cluster1",
		DefaultDatabase: "db1",
		Type:            analyzer.EngineSpark,
		SQL:             "CREATE TABLE my_table ( id INT COMMENT '主键', name STRING COMMENT '名称' );",
	})

	expected := []*analyzer.DDLResult{
		{
			StmtType: analyzer.StmtTypeCreateTable,
			Stmt: "CREATE TABLE my_table ( id INT COMMENT '主键', name STRING COMMENT '名称' );",
			Action: &analyzer.ActionInfo{
				IsSpecifiedCluster:  false,
				ClusterName:         "cluster1",
				IsSpecifiedDatabase: false,
				DatabaseName:        "db1",
				TableName:           "my_table",
				ActionType:          analyzer.ActionTypeCreate,
				Columns: []*analyzer.ActionColumn{
					{Name: "id", Type: "INT", Comment: "主键", Action: analyzer.ActionTypeCreate},
					{Name: "name", Type: "STRING", Comment: "名称", Action: analyzer.ActionTypeCreate},
				},
			},
		},
	}

	assertEquals(t, expected, result)
}
```

#### SQL拆分

```go
package main

import (
	"testing"

	sqlparser "github.com/Edsuns/sql-parser"
	"github.com/Edsuns/sql-parser/analyzer"
)

func TestSplit(t *testing.T) {
	a := sqlparser.NewAnalyzer()

	result, err := a.Split(&analyzer.SplitReq{
		Type: analyzer.EngineSpark,
		SQL:  "SELECT * FROM t1; INSERT INTO t2 VALUES (1); DELETE FROM t3;",
	})

	expected := []string{
		"SELECT * FROM t1;",
		"INSERT INTO t2 VALUES (1);",
		"DELETE FROM t3;",
	}

	assertEquals(t, expected, result)
}
```

#### 注释修改

```go
package main

import (
	"strings"
	"testing"

	sqlparser "github.com/Edsuns/sql-parser"
	"github.com/Edsuns/sql-parser/analyzer"
)

func TestMakeCommentModification(t *testing.T) {
	a := sqlparser.NewAnalyzer()

	result, err := a.MakeCommentModification(&analyzer.MakeCommentModificationReq{
		Type:       analyzer.EngineSpark,
		DDL:        "CREATE TABLE db1.t1 (id INT COMMENT 'old comment')",
		ColumnName: "id",
		Comment:    "new comment",
	})

	expected := "ALTER TABLE `db1`.`t1` ALTER COLUMN `id` COMMENT 'new comment';"

	assertEquals(t, expected, result)
}
```

## API 接口

```go
type SQLAnalyzer interface {
    // Split 分割多句SQL为多个单句SQL
    Split(req *SplitReq) ([]string, error)
    // AnalyzeLineage 分析SQL血缘
    AnalyzeLineage(req *AnalyzeReq) ([]*LineageResult, error)
    // AnalyzeDDL 分析DDL信息
    AnalyzeDDL(req *AnalyzeReq) ([]*DDLResult, error)
    // MakeCommentModification 生成注释修改语句
    MakeCommentModification(req *MakeCommentModificationReq) (string, error)
}
```

## 技术栈

- Go 1.24.10
- ANTLR 4.13.2
