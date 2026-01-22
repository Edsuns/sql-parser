package spark

import (
	"errors"
	"sql-parser/analyzer"
	"strings"

	"github.com/antlr4-go/antlr/v4"
)

// dependencyAnalyzer 实现了 DependencyAnalyzer 接口
type dependencyAnalyzer struct {
}

// NewDependencyAnalyzer 创建一个新的 DependencyAnalyzer 实例
func NewDependencyAnalyzer() analyzer.DependencyAnalyzer {
	return &dependencyAnalyzer{}
}

func (a *dependencyAnalyzer) Analyze(req *analyzer.DependencyAnalyzeReq) ([]*analyzer.DependencyResult, error) {
	// 使用SplitSQL函数拆分SQL语句
	statements := analyzer.SplitSQL(makeLexer(req.SQL))
	var result []*analyzer.DependencyResult
	for _, stmt := range statements {
		ddl, err := a.ParseOne(stmt, req.DefaultCluster, req.DefaultDatabase)
		if err != nil {
			return nil, err
		}
		if ddl != nil {
			result = append(result, ddl)
		}
	}
	return result, nil
}

// ParseOne 解析SQL语句并返回Dependencies列表
func (a *dependencyAnalyzer) ParseOne(sql, defaultCluster, defaultDatabase string) (*analyzer.DependencyResult, error) {
	// 创建语法分析器
	p := makeParser(makeLexer(sql))

	// 创建自定义监听器
	listener := newDependencyListener(defaultCluster, defaultDatabase)

	// 创建自定义错误监听器
	errListener := newSyntaxErrorListener(listener)
	p.AddErrorListener(errListener)

	// 解析并遍历语法树
	antlr.ParseTreeWalkerDefault.Walk(listener, p.CompoundOrSingleStatement())

	// 检查是否有语法错误
	if len(errListener.errors) > 0 {
		return nil, errors.New(strings.Join(errListener.errors, "; "))
	}

	// 过滤掉只有注释的语句
	if listener.isOnlyComment {
		return nil, nil
	}

	// 设置语句和操作类型
	listener.dependencies.Stmt = sql
	listener.dependencies.StmtType = listener.firstOpType

	return listener.dependencies, nil
}
