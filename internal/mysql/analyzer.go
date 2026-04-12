package mysql

import (
	"errors"
	"fmt"
	"strings"

	"github.com/Edsuns/sql-parser/analyzer"
	"github.com/antlr4-go/antlr/v4"
)

// sqlAnalyzer 实现了 SQLAnalyzer 接口
type sqlAnalyzer struct {
}

// NewSQLAnalyzer 创建一个新的 SQLAnalyzer 实例
func NewSQLAnalyzer() analyzer.SQLAnalyzer {
	return &sqlAnalyzer{}
}

// AnalyzeLineage 分析SQL血缘关系（Stmt,StmtType,Reads,Writes）
func (a *sqlAnalyzer) AnalyzeLineage(req *analyzer.AnalyzeReq) ([]*analyzer.LineageResult, error) {
	// 使用SplitSQL函数拆分SQL语句
	statements := analyzer.SplitSQL(makeLexer(req.SQL))
	var result []*analyzer.LineageResult
	currentDatabase := req.DefaultDatabase
	// 共享临时表状态
	tempTables := make(map[string]bool)
	for _, stmt := range statements {
		// 解析语句
		lineageResult, err := a.parseOneForLineage(stmt, req.DefaultCluster, currentDatabase)
		if err != nil {
			return nil, err
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
	// 使用SplitSQL函数拆分SQL语句
	statements := analyzer.SplitSQL(makeLexer(req.SQL))
	var result []*analyzer.DDLResult
	for _, stmt := range statements {
		ddlResult, err := a.parseOneForDDL(stmt, req.DefaultCluster, req.DefaultDatabase)
		if err != nil {
			return nil, err
		}
		if ddlResult != nil {
			result = append(result, ddlResult)
		}
	}
	return result, nil
}

// parseOneForLineage 解析SQL语句并返回血缘分析结果
func (a *sqlAnalyzer) parseOneForLineage(sql, defaultCluster, defaultDatabase string) (*analyzer.LineageResult, error) {
	// 创建语法分析器
	p := makeParser(makeLexer(sql))

	// 创建自定义监听器
	listener := newLineageListener(defaultCluster, defaultDatabase)

	// 创建自定义错误监听器
	errListener := analyzer.NewSyntaxErrorListener(&listener.isOnlyComment)
	p.AddErrorListener(errListener)

	// 解析并遍历语法树
	antlr.ParseTreeWalkerDefault.Walk(listener, p.Queries())

	// 检查是否有语法错误
	if len(errListener.Errors) > 0 {
		return nil, errors.New(strings.Join(errListener.Errors, "; "))
	}

	// 过滤掉只有注释的语句
	if listener.isOnlyComment {
		return nil, nil
	}

	// 设置语句和操作类型
	listener.result.Stmt = sql
	if listener.firstOpType == "" {
		listener.result.StmtType = analyzer.StmtTypeUnknown
	} else {
		listener.result.StmtType = listener.firstOpType
	}

	return listener.result, nil
}

// parseOneForDDL 解析SQL语句并返回DDL分析结果
func (a *sqlAnalyzer) parseOneForDDL(sql, defaultCluster, defaultDatabase string) (*analyzer.DDLResult, error) {
	// 创建语法分析器
	p := makeParser(makeLexer(sql))

	// 创建自定义监听器
	listener := newDDLListener(defaultCluster, defaultDatabase)

	// 创建自定义错误监听器
	errListener := analyzer.NewSyntaxErrorListener(&listener.isOnlyComment)
	p.AddErrorListener(errListener)

	// 解析并遍历语法树
	antlr.ParseTreeWalkerDefault.Walk(listener, p.Queries())

	// 检查是否有语法错误
	if len(errListener.Errors) > 0 {
		return nil, errors.New(strings.Join(errListener.Errors, "; "))
	}

	// 过滤掉只有注释的语句
	if listener.isOnlyComment {
		return nil, nil
	}

	// 设置语句和操作类型
	listener.result.Stmt = sql
	if listener.firstOpType == "" {
		listener.result.StmtType = analyzer.StmtTypeUnknown
	} else {
		listener.result.StmtType = listener.firstOpType
	}

	return listener.result, nil
}

// MakeCommentModification 生成注释修改语句
func (a *sqlAnalyzer) MakeCommentModification(req *analyzer.MakeCommentModificationReq) (string, error) {
	// 创建语法分析器
	p := makeParser(makeLexer(req.DDL))

	// 创建自定义监听器
	listener := newDDLListener("", "")

	// 创建自定义错误监听器
	errListener := analyzer.NewSyntaxErrorListener(&listener.isOnlyComment)
	p.AddErrorListener(errListener)

	// 解析并遍历语法树
	antlr.ParseTreeWalkerDefault.Walk(listener, p.Queries())

	// 检查是否有语法错误
	if len(errListener.Errors) > 0 {
		return "", errors.New(strings.Join(errListener.Errors, "; "))
	}

	// 过滤掉只有注释的语句
	if listener.isOnlyComment {
		return "", nil
	}

	// 检查是否有表信息
	if listener.result.Action == nil {
		return "", errors.New("no table information found in DDL")
	}

	// 查找指定字段
	var tableName, databaseName string
	tableName = listener.result.Action.TableName
	databaseName = listener.result.Action.DatabaseName

	// 查找指定字段的详细信息
	var targetColumn *analyzer.ActionColumn
	for _, col := range listener.result.Action.Columns {
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
	return analyzer.SplitSQL(makeLexer(req.SQL)), nil
}
