package mysql

import (
	"fmt"
	"strings"

	"github.com/Edsuns/sql-parser/analyzer"
	"github.com/Edsuns/sql-parser/internal/mysql/parser"
	"github.com/antlr4-go/antlr/v4"
)

// dependencyListener 自定义监听器，用于提取SQL语句中的读写表信息和注释
type dependencyListener struct {
	*parser.BaseMySQLParserListener

	dependencies    *analyzer.DependencyResult
	defaultCluster  string
	defaultDatabase string
	curOpType       analyzer.StmtType // 当前操作类型
	firstOpType     analyzer.StmtType // 第一个操作类型
	comments        []string          // 存储解析到的注释
	isOnlyComment   bool              // 标记当前SQL是否只包含注释
	isWriteOp       bool              // 是否已遇到写入操作
	cteNames        map[string]bool   // 存储CTE名称，避免将CTE作为表依赖
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
		isOnlyComment:   true, // 默认认为是只有注释，遇到非注释内容时设置为false
		isWriteOp:       false,
		cteNames:        make(map[string]bool),
	}
}

// EnterQuerySpecification 进入查询规范时调用，这是SELECT语句的主要部分
func (l *dependencyListener) EnterQuerySpecification(ctx *parser.QuerySpecificationContext) {
	l.curOpType = analyzer.StmtTypeSelect
	l.isOnlyComment = false
	// 如果还没有遇到写入操作，设置第一个操作类型
	if !l.isWriteOp && l.firstOpType == "" {
		l.firstOpType = analyzer.StmtTypeSelect
	}
}

// EnterInsertStatement 进入INSERT语句时调用
func (l *dependencyListener) EnterInsertStatement(ctx *parser.InsertStatementContext) {
	l.curOpType = analyzer.StmtTypeInsert
	l.onWriteStmt()
}

// EnterUpdateStatement 进入UPDATE语句时调用
func (l *dependencyListener) EnterUpdateStatement(ctx *parser.UpdateStatementContext) {
	l.curOpType = analyzer.StmtTypeUpdate
	l.onWriteStmt()
}

// EnterDeleteStatement 进入DELETE语句时调用
func (l *dependencyListener) EnterDeleteStatement(ctx *parser.DeleteStatementContext) {
	l.curOpType = analyzer.StmtTypeDelete
	l.onWriteStmt()
}

// EnterTruncateTableStatement 进入TRUNCATE TABLE语句时调用
func (l *dependencyListener) EnterTruncateTableStatement(ctx *parser.TruncateTableStatementContext) {
	l.curOpType = analyzer.StmtTypeTruncate
	l.onWriteStmt()
}

// EnterCreateTable 进入CREATE TABLE语句时调用
func (l *dependencyListener) EnterCreateTable(ctx *parser.CreateTableContext) {
	l.curOpType = analyzer.StmtTypeCreateTable
	l.onWriteStmt()
}

// EnterCreateView 进入CREATE VIEW语句时调用
func (l *dependencyListener) EnterCreateView(ctx *parser.CreateViewContext) {
	l.curOpType = analyzer.StmtTypeCreateView
	l.onWriteStmt()
}

// EnterDropTable 进入DROP TABLE语句时调用
func (l *dependencyListener) EnterDropTable(ctx *parser.DropTableContext) {
	l.curOpType = analyzer.StmtTypeDropTable
	l.onWriteStmt()
}

// EnterAlterTable 进入ALTER TABLE语句时调用
func (l *dependencyListener) EnterAlterTable(ctx *parser.AlterTableContext) {
	l.curOpType = analyzer.StmtTypeAlterTable
	l.onWriteStmt()
}

// EnterReplaceStatement 进入REPLACE语句时调用
func (l *dependencyListener) EnterReplaceStatement(ctx *parser.ReplaceStatementContext) {
	l.curOpType = analyzer.StmtTypeReplaceTable
	l.onWriteStmt()
}

// EnterUseCommand 进入USE语句时调用
func (l *dependencyListener) EnterUseCommand(ctx *parser.UseCommandContext) {
	l.curOpType = analyzer.StmtTypeUseDatabase
	l.isOnlyComment = false
	if l.firstOpType == "" {
		l.firstOpType = analyzer.StmtTypeUseDatabase
	}
}

// EnterTableRef 进入表引用时调用，用于提取数据库名和表名
func (l *dependencyListener) EnterTableRef(ctx *parser.TableRefContext) {
	// 简单处理，直接获取文本并分割
	name := ctx.GetText()
	parts := strings.Split(name, ".")
	cluster, database, table := "", "", ""

	if len(parts) == 1 {
		table = parts[0]
	} else if len(parts) == 2 {
		database = parts[0]
		table = parts[1]
	} else if len(parts) > 2 {
		cluster = parts[0]
		database = parts[1]
		table = parts[2]
	}

	// 检查是否是CTE名称，如果是则跳过，CTE不是实际的表依赖
	if _, isCTE := l.cteNames[table]; isCTE {
		return
	}

	// 对于CTE中的表，总是作为读表处理，除非明确是写操作
	if l.curOpType == "" || l.curOpType == analyzer.StmtTypeSelect {
		l.addReadTable(cluster, database, table)
	} else {
		// 这些操作中的标识符引用通常是写表
		l.addWriteTable(cluster, database, table)
	}
}

// EnterTableName 进入表名时调用，用于提取数据库名和表名
func (l *dependencyListener) EnterTableName(ctx *parser.TableNameContext) {
	// 简单处理，直接获取文本并分割
	name := ctx.GetText()
	parts := strings.Split(name, ".")
	cluster, database, table := "", "", ""

	if len(parts) == 1 {
		table = parts[0]
	} else if len(parts) == 2 {
		database = parts[0]
		table = parts[1]
	} else if len(parts) > 2 {
		cluster = parts[0]
		database = parts[1]
		table = parts[2]
	}

	// 检查是否是CTE名称，如果是则跳过，CTE不是实际的表依赖
	if _, isCTE := l.cteNames[table]; isCTE {
		return
	}

	// 对于CTE中的表，总是作为读表处理，除非明确是写操作
	if l.curOpType == "" || l.curOpType == analyzer.StmtTypeSelect {
		l.addReadTable(cluster, database, table)
	} else {
		// 这些操作中的标识符引用通常是写表
		l.addWriteTable(cluster, database, table)
	}
}

// addReadTable 添加读表信息
func (l *dependencyListener) addReadTable(cluster, database, table string) {
	if cluster == "" {
		cluster = l.defaultCluster
	}
	if database == "" {
		database = l.defaultDatabase
	}
	l.dependencies.Read = append(l.dependencies.Read, &analyzer.DependencyTable{
		Cluster:  cluster,
		Database: database,
		Table:    table,
	})
}

// addWriteTable 添加写表信息
func (l *dependencyListener) addWriteTable(cluster, database, table string) {
	if cluster == "" {
		cluster = l.defaultCluster
	}
	if database == "" {
		database = l.defaultDatabase
	}
	l.dependencies.Write = append(l.dependencies.Write, &analyzer.DependencyTable{
		Cluster:  cluster,
		Database: database,
		Table:    table,
	})
}

func (l *dependencyListener) onWriteStmt() {
	// 标记为写入操作，并设置第一个操作类型
	l.isWriteOp = true
	l.isOnlyComment = false
	if l.firstOpType == "" {
		l.firstOpType = l.curOpType
	}
}

// 自定义错误监听器，用于捕获语法错误
type syntaxErrorListener struct {
	*antlr.DefaultErrorListener
	parent *dependencyListener
	errors []string
}

func newSyntaxErrorListener(parent *dependencyListener) *syntaxErrorListener {
	return &syntaxErrorListener{
		parent: parent,
	}
}

func (l *syntaxErrorListener) SyntaxError(recognizer antlr.Recognizer, offendingSymbol any, line, column int, msg string, e antlr.RecognitionException) {
	if token, ok := offendingSymbol.(antlr.Token); ok && token.GetTokenType() == antlr.TokenEOF && l.parent.isOnlyComment {
		// 忽略纯注释语句的报错
		return
	}
	l.errors = append(l.errors, fmt.Sprintf("line:%d column:%d %s", line, column, msg))
}
