package starrocks

import (
	"fmt"
	"strings"

	"github.com/Edsuns/sql-parser/analyzer"
	"github.com/Edsuns/sql-parser/internal/starrocks/parser"
	"github.com/antlr4-go/antlr/v4"
)

// dependencyListener 自定义监听器，用于提取SQL语句中的读写表信息和注释
type dependencyListener struct {
	*parser.BaseStarRocksListener

	dependencies    *analyzer.DependencyResult
	defaultCluster  string
	defaultDatabase string
	curOpType       analyzer.StmtType
	firstOpType     analyzer.StmtType
	comments        []string
	isOnlyComment   bool
	isWriteOp       bool
	cteNames        map[string]bool
}

// newDependencyListener 创建新的监听器实例
func newDependencyListener(defaultCluster, defaultDatabase string) *dependencyListener {
	return &dependencyListener{
		dependencies: &analyzer.DependencyResult{
			Read:  []*analyzer.DependencyTable{},
			Write: []*analyzer.DependencyTable{},
		},
		defaultCluster:  defaultCluster,
		defaultDatabase: defaultDatabase,
		curOpType:       "",
		comments:        []string{},
		isOnlyComment:   true,
		isWriteOp:       false,
		cteNames:        make(map[string]bool),
	}
}

// EnterSingleStatement 进入单条语句时调用
func (l *dependencyListener) EnterSingleStatement(ctx *parser.SingleStatementContext) {
	l.isOnlyComment = false
}

// EnterCreateTableStatement 进入创建表语句时调用
func (l *dependencyListener) EnterCreateTableStatement(ctx *parser.CreateTableStatementContext) {
	l.curOpType = analyzer.StmtTypeCreateTable
	l.onWriteStmt()
}

// EnterCreateViewStatement 进入创建视图语句时调用
func (l *dependencyListener) EnterCreateViewStatement(ctx *parser.CreateViewStatementContext) {
	l.curOpType = analyzer.StmtTypeCreateView
	l.onWriteStmt()
}

// EnterAlterTableStatement 进入修改表语句时调用
func (l *dependencyListener) EnterAlterTableStatement(ctx *parser.AlterTableStatementContext) {
	l.curOpType = analyzer.StmtTypeAlterTable
	l.onWriteStmt()
}

// EnterDropTableStatement 进入删除表语句时调用
func (l *dependencyListener) EnterDropTableStatement(ctx *parser.DropTableStatementContext) {
	l.curOpType = analyzer.StmtTypeDropTable
	l.onWriteStmt()
}

// EnterInsertStatement 进入插入语句时调用
func (l *dependencyListener) EnterInsertStatement(ctx *parser.InsertStatementContext) {
	l.curOpType = analyzer.StmtTypeInsert
	l.onWriteStmt()
}

// EnterUpdateStatement 进入更新语句时调用
func (l *dependencyListener) EnterUpdateStatement(ctx *parser.UpdateStatementContext) {
	l.curOpType = analyzer.StmtTypeUpdate
	l.onWriteStmt()
}

// EnterDeleteStatement 进入删除语句时调用
func (l *dependencyListener) EnterDeleteStatement(ctx *parser.DeleteStatementContext) {
	l.curOpType = analyzer.StmtTypeDelete
	l.onWriteStmt()
}

// EnterQueryStatement 进入查询语句时调用
func (l *dependencyListener) EnterQueryStatement(ctx *parser.QueryStatementContext) {
	l.curOpType = analyzer.StmtTypeSelect
	l.isOnlyComment = false
	if !l.isWriteOp && l.firstOpType == "" {
		l.firstOpType = analyzer.StmtTypeSelect
	}
}

// EnterQualifiedName 进入表名节点时调用
func (l *dependencyListener) EnterQualifiedName(ctx *parser.QualifiedNameContext) {
	if ctx != nil {
		// 直接获取表名文本
		tableName := ctx.GetText()
		// 检查是否是CTE名称
		if _, isCTE := l.cteNames[tableName]; isCTE {
			return
		}

		// 解析表名，支持 cluster.db.table 格式
		cluster, database, table := l.parseTableName(tableName)

		// 根据当前操作类型决定是读表还是写表
		if l.isWriteOperation() {
			l.addWriteTable(cluster, database, table)
		} else {
			l.addReadTable(cluster, database, table)
		}
	}
}

// EnterTableName 进入表名节点时调用
func (l *dependencyListener) EnterTableName(ctx *parser.TableNameContext) {
	if ctx != nil {
		// 直接获取表名文本
		tableName := ctx.GetText()
		// 检查是否是CTE名称
		if _, isCTE := l.cteNames[tableName]; isCTE {
			return
		}

		// 解析表名，支持 cluster.db.table 格式
		cluster, database, table := l.parseTableName(tableName)

		// 根据当前操作类型决定是读表还是写表
		if l.isWriteOperation() {
			l.addWriteTable(cluster, database, table)
		} else {
			l.addReadTable(cluster, database, table)
		}
	}
}

// isWriteOperation 检查当前操作是否是写操作
func (l *dependencyListener) isWriteOperation() bool {
	return l.curOpType == analyzer.StmtTypeCreateTable ||
		l.curOpType == analyzer.StmtTypeCreateView ||
		l.curOpType == analyzer.StmtTypeAlterTable ||
		l.curOpType == analyzer.StmtTypeDropTable ||
		l.curOpType == analyzer.StmtTypeInsert ||
		l.curOpType == analyzer.StmtTypeUpdate ||
		l.curOpType == analyzer.StmtTypeDelete
}

// parseTableName 解析表名，支持 cluster.db.table 格式
func (l *dependencyListener) parseTableName(tableName string) (cluster, database, table string) {
	parts := strings.Split(tableName, ".")

	switch len(parts) {
	case 1:
		// 只有表名
		table = parts[0]
	case 2:
		// 数据库名和表名
		database = parts[0]
		table = parts[1]
	case 3:
		// 集群名、数据库名和表名
		cluster = parts[0]
		database = parts[1]
		table = parts[2]
	}

	// 使用默认值
	if cluster == "" {
		cluster = l.defaultCluster
	}
	if database == "" {
		database = l.defaultDatabase
	}

	return
}

// addReadTable 添加读表信息
func (l *dependencyListener) addReadTable(cluster, database, table string) {
	l.dependencies.Read = append(l.dependencies.Read, &analyzer.DependencyTable{
		Cluster:  cluster,
		Database: database,
		Table:    table,
	})
}

// addWriteTable 添加写表信息
func (l *dependencyListener) addWriteTable(cluster, database, table string) {
	l.dependencies.Write = append(l.dependencies.Write, &analyzer.DependencyTable{
		Cluster:  cluster,
		Database: database,
		Table:    table,
	})
}

// onWriteStmt 处理写操作语句
func (l *dependencyListener) onWriteStmt() {
	l.isWriteOp = true
	if l.firstOpType == "" {
		l.firstOpType = l.curOpType
	}
}

// syntaxErrorListener 自定义错误监听器
type syntaxErrorListener struct {
	*antlr.DefaultErrorListener
	parent *dependencyListener
	errors []string
}

// newSyntaxErrorListener 创建新的错误监听器
func newSyntaxErrorListener(parent *dependencyListener) *syntaxErrorListener {
	return &syntaxErrorListener{
		parent: parent,
	}
}

// SyntaxError 处理语法错误
func (l *syntaxErrorListener) SyntaxError(recognizer antlr.Recognizer, offendingSymbol any, line, column int, msg string, e antlr.RecognitionException) {
	if token, ok := offendingSymbol.(antlr.Token); ok && token.GetTokenType() == antlr.TokenEOF && l.parent.isOnlyComment {
		return
	}
	l.errors = append(l.errors, fmt.Sprintf("line:%d column:%d %s", line, column, msg))
}
