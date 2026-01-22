package tidb

import (
	"strings"

	"github.com/Edsuns/sql-parser/analyzer"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
)

// dependencyAnalyzer 实现了 DependencyAnalyzer 接口
type dependencyAnalyzer struct{}

// NewDependencyAnalyzer 创建一个新的 DependencyAnalyzer 实例
func NewDependencyAnalyzer() analyzer.DependencyAnalyzer {
	return &dependencyAnalyzer{}
}

func (a *dependencyAnalyzer) Analyze(req *analyzer.DependencyAnalyzeReq) ([]*analyzer.DependencyResult, error) {
	// 创建TiDB解析器
	p := parser.New()

	// 解析SQL语句
	stmts, _, err := p.Parse(req.SQL, "", "")
	if err != nil {
		return nil, err
	}
	var result []*analyzer.DependencyResult
	for _, stmt := range stmts {
		deps := a.parseOneStmt(stmt, req.DefaultCluster, req.DefaultDatabase)
		if deps != nil {
			result = append(result, deps)
		}
	}
	return result, nil
}

func (a *dependencyAnalyzer) ParseOne(sql, defaultCluster, defaultDatabase string) (*analyzer.DependencyResult, error) {
	// 创建TiDB解析器
	p := parser.New()

	// 解析SQL语句
	stmt, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		return nil, err
	}

	return a.parseOneStmt(stmt, defaultCluster, defaultDatabase), nil
}

func (a *dependencyAnalyzer) parseOneStmt(stmt ast.StmtNode, defaultCluster, defaultDatabase string) *analyzer.DependencyResult {
	// 创建依赖结果
	deps := &analyzer.DependencyResult{
		Stmt:     strings.TrimSpace(stmt.OriginalText()),
		Read:     make([]*analyzer.DependencyTable, 0),
		Write:    make([]*analyzer.DependencyTable, 0),
		StmtType: "",
	}
	visitor := &dependencyVisitor{
		deps:            deps,
		defaultCluster:  defaultCluster,
		defaultDatabase: defaultDatabase,
		readTables:      make(map[string]bool),
		writeTables:     make(map[string]bool),
		isCreateView:    false,
		createViewName:  "",
	}
	stmt.Accept(visitor)
	return deps
}
