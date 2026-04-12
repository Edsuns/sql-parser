package tidb

import (
	"fmt"
	"strings"

	"github.com/Edsuns/sql-parser/analyzer"
	"github.com/pingcap/tidb/pkg/parser"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
)

// sqlAnalyzer 实现了 SQLAnalyzer 接口
type sqlAnalyzer struct{}

// NewSQLAnalyzer 创建一个新的 SQLAnalyzer 实例
func NewSQLAnalyzer() analyzer.SQLAnalyzer {
	return &sqlAnalyzer{}
}

// AnalyzeLineage 分析SQL血缘关系（Stmt,StmtType,Reads,Writes）
func (a *sqlAnalyzer) AnalyzeLineage(req *analyzer.AnalyzeReq) ([]*analyzer.LineageResult, error) {
	// 创建TiDB解析器
	p := parser.New()

	// 解析SQL语句
	stmts, _, err := p.Parse(req.SQL, "", "")
	if err != nil {
		return nil, err
	}
	var result []*analyzer.LineageResult
	currentDatabase := req.DefaultDatabase
	// 共享临时表状态
	tempTables := make(map[string]bool)
	for _, stmt := range stmts {
		sql := stmt.OriginalText()
		lineageResult, err := a.parseOneForLineage(sql, req.DefaultCluster, currentDatabase)
		if err != nil {
			continue
		}
		if lineageResult != nil {
			// 检查是否是CREATE TEMPORARY TABLE语句
			if lineageResult.StmtType == analyzer.StmtTypeCreateTemporaryTable && len(lineageResult.Writes) > 0 {
				// 记录临时表
				for _, write := range lineageResult.Writes {
					tempTables[strings.ToLower(write.Table)] = true
				}
			}
			// 检查读取的表是否是临时表
			for _, read := range lineageResult.Reads {
				if tempTables[strings.ToLower(read.Table)] {
					read.IsTemp = true
				}
			}
			// 检查写入的表是否是临时表（可能是CREATE TEMPORARY TABLE ... AS SELECT）
			for _, write := range lineageResult.Writes {
				if tempTables[strings.ToLower(write.Table)] {
					write.IsTemp = true
				}
			}
			result = append(result, lineageResult)
			// 如果是USE语句，更新当前数据库
			if lineageResult.StmtType == analyzer.StmtTypeUseDatabase && len(lineageResult.Reads) > 0 && lineageResult.Reads[0].Database != "" {
				currentDatabase = lineageResult.Reads[0].Database
			}
		}
	}
	return result, nil
}

// AnalyzeDDL 分析SQL DDL信息（Stmt,StmtType,TableInfo,Actions）
func (a *sqlAnalyzer) AnalyzeDDL(req *analyzer.AnalyzeReq) ([]*analyzer.DDLResult, error) {
	// 创建TiDB解析器
	p := parser.New()

	// 解析SQL语句
	stmts, _, err := p.Parse(req.SQL, "", "")
	if err != nil {
		return nil, err
	}
	var result []*analyzer.DDLResult
	for _, stmt := range stmts {
		sql := stmt.OriginalText()
		ddlResult, err := a.parseOneForDDL(sql, req.DefaultCluster, req.DefaultDatabase)
		if err != nil {
			continue
		}
		if ddlResult != nil {
			result = append(result, ddlResult)
		}
	}
	return result, nil
}

// parseOneForLineage 解析SQL语句并返回血缘分析结果
func (a *sqlAnalyzer) parseOneForLineage(sql, defaultCluster, defaultDatabase string) (*analyzer.LineageResult, error) {
	// 创建TiDB解析器
	p := parser.New()

	// 解析SQL语句
	stmt, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		return nil, err
	}

	// 创建血缘分析结果
	result := &analyzer.LineageResult{
		Stmt:     strings.TrimSpace(stmt.OriginalText()),
		Reads:    make([]*analyzer.Dependency, 0),
		Writes:   make([]*analyzer.Dependency, 0),
		StmtType: "",
	}

	// 创建血缘分析访问器
	visitor := &lineageVisitor{
		result:          result,
		defaultCluster:  defaultCluster,
		defaultDatabase: defaultDatabase,
		reads:           make(map[string]bool),
		writes:          make(map[string]bool),
		isCreateView:    false,
		createViewName:  "",
	}

	// 遍历语法树
	stmt.Accept(visitor)

	// 如果没有设置StmtType，设置为Unknown
	if result.StmtType == "" {
		result.StmtType = analyzer.StmtTypeUnknown
	}

	return result, nil
}

// parseOneForDDL 解析SQL语句并返回DDL分析结果
func (a *sqlAnalyzer) parseOneForDDL(sql, defaultCluster, defaultDatabase string) (*analyzer.DDLResult, error) {
	// 创建TiDB解析器
	p := parser.New()

	// 解析SQL语句
	stmt, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		return nil, err
	}

	// 创建DDL分析结果
	result := &analyzer.DDLResult{
		Stmt:     strings.TrimSpace(stmt.OriginalText()),
		StmtType: "",
	}

	// 创建DDL分析访问器
	visitor := &ddlVisitor{
		result:          result,
		defaultCluster:  defaultCluster,
		defaultDatabase: defaultDatabase,
		isCreateView:    false,
		createViewName:  "",
	}

	// 遍历语法树
	stmt.Accept(visitor)

	// 如果没有设置StmtType，设置为Unknown
	if result.StmtType == "" {
		result.StmtType = analyzer.StmtTypeUnknown
	}

	return result, nil
}

// MakeCommentModification 生成注释修改语句
// 参考：https://docs.pingcap.com/zh/tidb/stable/sql-statement-modify-column/
func (a *sqlAnalyzer) MakeCommentModification(req *analyzer.MakeCommentModificationReq) (string, error) {
	// 创建TiDB解析器
	p := parser.New()

	// 解析SQL语句
	stmt, err := p.ParseOneStmt(req.DDL, "", "")
	if err != nil {
		return "", err
	}

	// 创建DDL分析结果
	result := &analyzer.DDLResult{
		Stmt:     strings.TrimSpace(stmt.OriginalText()),
		StmtType: "",
	}

	// 创建DDL分析访问器
	visitor := &ddlVisitor{
		result:          result,
		defaultCluster:  "",
		defaultDatabase: "",
		isCreateView:    false,
		createViewName:  "",
	}

	// 遍历语法树
	stmt.Accept(visitor)

	// 检查是否有表信息
	if result.Action == nil {
		return "", fmt.Errorf("no table information found in DDL")
	}

	// 查找指定字段
	var tableName, databaseName string
	tableName = result.Action.TableName
	databaseName = result.Action.DatabaseName

	// 查找指定字段的详细信息
	var targetColumn *analyzer.ActionColumn
	for _, col := range result.Action.Columns {
		if col.Name == req.ColumnName {
			targetColumn = col
			break
		}
	}

	if targetColumn == nil {
		return "", fmt.Errorf("column %s not found in DDL", req.ColumnName)
	}

	// 生成修改注释的语句，包含完整的字段定义
	var commentStmt string
	if databaseName != "" {
		commentStmt = fmt.Sprintf("ALTER TABLE `%s`.`%s` MODIFY COLUMN `%s` %s", databaseName, tableName, req.ColumnName, targetColumn.Type)
	} else {
		commentStmt = fmt.Sprintf("ALTER TABLE `%s` MODIFY COLUMN `%s` %s", tableName, req.ColumnName, targetColumn.Type)
	}

	// 添加NOT NULL约束
	if targetColumn.IsNotNull {
		commentStmt += " NOT NULL"
	}

	// 添加默认值
	if targetColumn.DefaultValue != "" {
		commentStmt += " DEFAULT " + targetColumn.DefaultValue
	}

	// 添加新注释
	commentStmt += fmt.Sprintf(" COMMENT '%s';", req.Comment)

	return commentStmt, nil
}

// Split 分割多句SQL为多个单句SQL
func (a *sqlAnalyzer) Split(req *analyzer.SplitReq) ([]string, error) {
	// 创建TiDB解析器
	p := parser.New()

	// 解析SQL语句
	stmts, _, err := p.Parse(req.SQL, "", "")
	if err != nil {
		return nil, err
	}

	// 提取每个语句的原始文本
	var result []string
	for _, stmt := range stmts {
		result = append(result, strings.TrimSpace(stmt.OriginalText()))
	}

	return result, nil
}
