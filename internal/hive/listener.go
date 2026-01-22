package hive

import (
	"fmt"
	"sql-parser/analyzer"
	"sql-parser/internal/hive/parser"
	"strings"

	"github.com/antlr4-go/antlr/v4"
)

// dependencyListener 用于提取SQL中的依赖关系
type dependencyListener struct {
	*parser.BaseHiveParserListener

	firstOpType     analyzer.StmtType
	defaultCluster  string
	defaultDatabase string

	isOnlyComment bool

	// 保存解析结果
	dependencies *analyzer.DependencyResult

	// 当前正在处理的表名
	currentTable   string
	currentDb      string
	currentCluster string

	// 记录操作类型
	readTables  []*analyzer.DependencyTable
	writeTables []*analyzer.DependencyTable

	// 记录CTE表名，用于排除CTE表
	cteTables map[string]bool

	// 语句类型映射
	stmtTypeMap map[int]analyzer.StmtType

	// 标志：是否正在处理SELECT语句的FROM子句（源表）
	isProcessingSourceTable bool
}

// newDependencyListener 创建一个新的DependencyListener实例
func newDependencyListener(defaultCluster, defaultDatabase string) *dependencyListener {
	return &dependencyListener{
		defaultCluster:  defaultCluster,
		defaultDatabase: defaultDatabase,
		isOnlyComment:   true,
		dependencies: &analyzer.DependencyResult{
			Read:  []*analyzer.DependencyTable{},
			Write: []*analyzer.DependencyTable{},
		},
		readTables:              []*analyzer.DependencyTable{},
		writeTables:             []*analyzer.DependencyTable{},
		cteTables:               make(map[string]bool),
		isProcessingSourceTable: false,
		stmtTypeMap: map[int]analyzer.StmtType{
			parser.HiveParserRULE_selectStatement:        analyzer.StmtTypeSelect,
			parser.HiveParserRULE_insertClause:           analyzer.StmtTypeInsert,
			parser.HiveParserRULE_updateStatement:        analyzer.StmtTypeUpdate,
			parser.HiveParserRULE_deleteStatement:        analyzer.StmtTypeDelete,
			parser.HiveParserRULE_createTableStatement:   analyzer.StmtTypeCreateTable,
			parser.HiveParserRULE_alterStatement:         analyzer.StmtTypeAlterTable,
			parser.HiveParserRULE_dropTableStatement:     analyzer.StmtTypeDropTable,
			parser.HiveParserRULE_createViewStatement:    analyzer.StmtTypeCreateView,
			parser.HiveParserRULE_dropViewStatement:      analyzer.StmtTypeDropTable,
			parser.HiveParserRULE_truncateTableStatement: analyzer.StmtTypeTruncate,
		},
	}
}

// 辅助函数：提取表信息
func (l *dependencyListener) extractTable(ctx *parser.TableNameContext) {
	if ctx == nil {
		return
	}

	// 解析数据库和表名
	db, table := "", ""
	if ctx.GetText() != "" {
		parts := strings.Split(ctx.GetText(), ".")
		if len(parts) == 1 {
			table = parts[0]
			db = l.defaultDatabase
		} else if len(parts) == 2 {
			db = parts[0]
			table = parts[1]
		}
	}

	// 检查该表是否为CTE表，如果是则跳过
	if l.cteTables[table] {
		return
	}

	// 创建表依赖
	tableDep := &analyzer.DependencyTable{
		Cluster:  l.defaultCluster,
		Database: db,
		Table:    table,
	}

	// 根据当前上下文判断是读表还是写表
	if l.isProcessingSourceTable {
		// 正在处理FROM子句，是源表，添加到readTables
		l.readTables = append(l.readTables, tableDep)
	} else {
		// 不是FROM子句，是目标表，根据语句类型添加
		switch l.firstOpType {
		case analyzer.StmtTypeSelect:
			// SELECT语句，所有表都是读表
			l.readTables = append(l.readTables, tableDep)
		case analyzer.StmtTypeInsert, analyzer.StmtTypeUpdate, analyzer.StmtTypeDelete,
			analyzer.StmtTypeCreateTable, analyzer.StmtTypeAlterTable,
			analyzer.StmtTypeDropTable, analyzer.StmtTypeTruncate,
			analyzer.StmtTypeCreateView:
			// 这些语句中的表都是目标表，添加到写表
			l.writeTables = append(l.writeTables, tableDep)
		}
	}
}

// 监听进入FROM子句
func (l *dependencyListener) EnterFromClause(ctx *parser.FromClauseContext) {
	l.isOnlyComment = false
	// 设置标志，表示正在处理源表
	l.isProcessingSourceTable = true
}

// 监听离开FROM子句
func (l *dependencyListener) ExitFromClause(ctx *parser.FromClauseContext) {
	// 清除标志，表示已经处理完源表
	l.isProcessingSourceTable = false
}

// 监听进入查询语句
func (l *dependencyListener) EnterSelectStatement(ctx *parser.SelectStatementContext) {
	l.isOnlyComment = false
	// 只有当firstOpType尚未设置时，才设置为SELECT
	if l.firstOpType == "" {
		l.firstOpType = analyzer.StmtTypeSelect
	}
}

// 监听进入插入语句
func (l *dependencyListener) EnterInsertClause(ctx *parser.InsertClauseContext) {
	l.isOnlyComment = false
	l.firstOpType = analyzer.StmtTypeInsert

	// 处理插入的目标表
	if ctx.GetText() != "" {
		// 简单提取目标表名
		parts := strings.Split(ctx.GetText(), " ")
		for i, part := range parts {
			if part == "INTO" || part == "OVERWRITE" {
				if i+1 < len(parts) {
					tableName := parts[i+1]
					// 解析数据库和表名
					db, table := "", ""
					if strings.Contains(tableName, ".") {
						tableParts := strings.Split(tableName, ".")
						db = tableParts[0]
						table = tableParts[1]
					} else {
						table = tableName
						db = l.defaultDatabase
					}
					// 创建写表依赖
					tableDep := &analyzer.DependencyTable{
						Cluster:  l.defaultCluster,
						Database: db,
						Table:    table,
					}
					l.writeTables = append(l.writeTables, tableDep)
					break
				}
			}
		}
	}
}

// 监听进入更新语句
func (l *dependencyListener) EnterUpdateStatement(ctx *parser.UpdateStatementContext) {
	l.isOnlyComment = false
	l.firstOpType = analyzer.StmtTypeUpdate
}

// 监听进入删除语句
func (l *dependencyListener) EnterDeleteStatement(ctx *parser.DeleteStatementContext) {
	l.isOnlyComment = false
	l.firstOpType = analyzer.StmtTypeDelete
}

// 监听进入创建表语句
func (l *dependencyListener) EnterCreateTableStatement(ctx *parser.CreateTableStatementContext) {
	l.isOnlyComment = false
	l.firstOpType = analyzer.StmtTypeCreateTable
}

// 监听进入修改表语句
func (l *dependencyListener) EnterAlterStatement(ctx *parser.AlterStatementContext) {
	l.isOnlyComment = false
	l.firstOpType = analyzer.StmtTypeAlterTable
}

// 监听进入删除表语句
func (l *dependencyListener) EnterDropTableStatement(ctx *parser.DropTableStatementContext) {
	l.isOnlyComment = false
	l.firstOpType = analyzer.StmtTypeDropTable
}

// 监听进入创建视图语句
func (l *dependencyListener) EnterCreateViewStatement(ctx *parser.CreateViewStatementContext) {
	l.isOnlyComment = false
	l.firstOpType = analyzer.StmtTypeCreateView

	// 处理创建的视图名
	if ctx.GetText() != "" {
		// 简单提取视图名
		parts := strings.Split(ctx.GetText(), " ")
		for i, part := range parts {
			if part == "VIEW" {
				if i+1 < len(parts) {
					viewName := parts[i+1]
					// 解析数据库和视图名
					db, view := "", ""
					if strings.Contains(viewName, ".") {
						viewParts := strings.Split(viewName, ".")
						db = viewParts[0]
						view = viewParts[1]
					} else {
						view = viewName
						db = l.defaultDatabase
					}
					// 创建写表依赖
					tableDep := &analyzer.DependencyTable{
						Cluster:  l.defaultCluster,
						Database: db,
						Table:    view,
					}
					l.writeTables = append(l.writeTables, tableDep)
					break
				}
			}
		}
	}
}

// 监听进入删除视图语句
func (l *dependencyListener) EnterDropViewStatement(ctx *parser.DropViewStatementContext) {
	l.isOnlyComment = false
	l.firstOpType = analyzer.StmtTypeDropTable
}

// 监听进入截断表语句
func (l *dependencyListener) EnterTruncateTableStatement(ctx *parser.TruncateTableStatementContext) {
	l.isOnlyComment = false
	l.firstOpType = analyzer.StmtTypeTruncate
}

// 监听进入表名
func (l *dependencyListener) EnterTableName(ctx *parser.TableNameContext) {
	l.isOnlyComment = false
	l.extractTable(ctx)
}

// 监听进入表源
func (l *dependencyListener) EnterTableSource(ctx *parser.TableSourceContext) {
	l.isOnlyComment = false
	// 表源可能包含表名
}

// 监听进入视图名
func (l *dependencyListener) EnterViewName(ctx *parser.ViewNameContext) {
	l.isOnlyComment = false
	// 视图名也作为表依赖处理
	if ctx.GetText() != "" {
		parts := strings.Split(ctx.GetText(), ".")
		db, view := "", ""
		if len(parts) == 1 {
			view = parts[0]
			db = l.defaultDatabase
		} else if len(parts) == 2 {
			db = parts[0]
			view = parts[1]
		}
		tableDep := &analyzer.DependencyTable{
			Cluster:  l.defaultCluster,
			Database: db,
			Table:    view,
		}
		if l.firstOpType == analyzer.StmtTypeSelect {
			l.readTables = append(l.readTables, tableDep)
		} else {
			l.writeTables = append(l.writeTables, tableDep)
		}
	}
}

// 监听进入CTE语句
func (l *dependencyListener) EnterCteStatement(ctx *parser.CteStatementContext) {
	l.isOnlyComment = false
	// 提取CTE表名
	if ctx.GetText() != "" {
		// 简单处理：查找AS关键字前的表名
		text := ctx.GetText()
		asIndex := strings.Index(text, "AS")
		if asIndex > 0 {
			// 提取CTE表名
			cteName := strings.TrimSpace(text[:asIndex])
			// 移除可能的前缀，如WITH或逗号
			cteName = strings.TrimPrefix(cteName, "WITH")
			cteName = strings.TrimSpace(cteName)
			// 处理多个CTE的情况
			if strings.Contains(cteName, ",") {
				cteNames := strings.Split(cteName, ",")
				for _, name := range cteNames {
					name = strings.TrimSpace(name)
					if name != "" {
						l.cteTables[name] = true
					}
				}
			} else {
				l.cteTables[cteName] = true
			}
		}
	}
}

// 监听进入WithClause
func (l *dependencyListener) EnterWithClause(ctx *parser.WithClauseContext) {
	l.isOnlyComment = false
	// 提取所有CTE表名
	if ctx.GetText() != "" {
		text := ctx.GetText()
		// 移除WITH关键字
		text = strings.TrimPrefix(text, "WITH")
		text = strings.TrimSpace(text)
		// 分割CTE定义
		cteDefs := strings.Split(text, ",")
		for _, def := range cteDefs {
			def = strings.TrimSpace(def)
			if def != "" {
				// 提取CTE表名
				asIndex := strings.Index(def, "AS")
				if asIndex > 0 {
					cteName := strings.TrimSpace(def[:asIndex])
					l.cteTables[cteName] = true
				}
			}
		}
	}
}

// syntaxErrorListener 用于捕获语法错误
type syntaxErrorListener struct {
	*antlr.DefaultErrorListener
	listener *dependencyListener
	errors   []string
}

// newSyntaxErrorListener 创建一个新的SyntaxErrorListener实例
func newSyntaxErrorListener(listener *dependencyListener) *syntaxErrorListener {
	return &syntaxErrorListener{
		listener: listener,
		errors:   []string{},
	}
}

// SyntaxError 处理语法错误
func (l *syntaxErrorListener) SyntaxError(recognizer antlr.Recognizer, offendingSymbol interface{}, line, column int, msg string, e antlr.RecognitionException) {
	l.errors = append(l.errors, fmt.Sprintf("line %d:%d %s", line, column, msg))
}
