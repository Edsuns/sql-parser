package hive

import (
	"strings"

	"github.com/Edsuns/sql-parser/analyzer"
	"github.com/Edsuns/sql-parser/internal/hive/parser"
	"github.com/Edsuns/sql-parser/internal/util"
)

// lineageListener 用于提取SQL中的血缘关系信息
type lineageListener struct {
	*parser.BaseHiveParserListener

	firstOpType     analyzer.StmtType
	defaultCluster  string
	defaultDatabase string

	isOnlyComment bool

	// 保存解析结果
	result *analyzer.LineageResult

	// 当前正在处理的表名
	currentTable   string
	currentDb      string
	currentCluster string

	// 记录操作类型
	reads  []*analyzer.Dependency
	writes []*analyzer.Dependency

	// 记录CTE表名，用于排除CTE表
	cteTables map[string]bool

	// 语句类型映射
	stmtTypeMap map[int]analyzer.StmtType

	// 标志：是否正在处理SELECT语句的FROM子句（源表）
	isProcessingSourceTable bool

	// 存储解析到的注释
	comments []string
}

// newLineageListener 创建一个新的LineageListener实例
func newLineageListener(defaultCluster, defaultDatabase string) *lineageListener {
	return &lineageListener{
		defaultCluster:  defaultCluster,
		defaultDatabase: defaultDatabase,
		isOnlyComment:   true,
		result: &analyzer.LineageResult{
			Reads:  []*analyzer.Dependency{},
			Writes: []*analyzer.Dependency{},
		},
		reads:                   []*analyzer.Dependency{},
		writes:                  []*analyzer.Dependency{},
		cteTables:               make(map[string]bool),
		isProcessingSourceTable: false,
		comments:                []string{},
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
			parser.HiveParserRULE_truncateTableStatement: analyzer.StmtTypeTruncateTable,
		},
	}
}

// ddlListener 用于提取SQL中的DDL信息
type ddlListener struct {
	*parser.BaseHiveParserListener

	firstOpType     analyzer.StmtType
	defaultCluster  string
	defaultDatabase string

	isOnlyComment bool

	// 保存解析结果
	result *analyzer.DDLResult

	// 记录当前正在处理的表，用于action解析
	currentTableName string

	// 语句类型映射
	stmtTypeMap map[int]analyzer.StmtType

	// 存储解析到的注释
	comments []string
}

// newDDLListener 创建一个新的DDLListener实例
func newDDLListener(defaultCluster, defaultDatabase string) *ddlListener {
	return &ddlListener{
		defaultCluster:  defaultCluster,
		defaultDatabase: defaultDatabase,
		isOnlyComment:   true,
		result:          &analyzer.DDLResult{},
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
			parser.HiveParserRULE_truncateTableStatement: analyzer.StmtTypeTruncateTable,
		},
		comments: []string{},
	}
}

// 辅助函数：提取表信息
func (l *lineageListener) extractTable(ctx *parser.TableNameContext) {
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

	// 对于ALTER TABLE语句，跳过处理，因为EnterAlterStatement函数已经处理了
	if l.firstOpType == analyzer.StmtTypeAlterTable {
		return
	}

	// 创建表依赖
	tableDep := &analyzer.Dependency{
		Cluster:  l.defaultCluster,
		Database: db,
		Table:    table,
	}

	// 根据当前上下文判断是读表还是写表
	if l.isProcessingSourceTable {
		// 正在处理FROM子句，是源表，添加到reads
		l.reads = append(l.reads, tableDep)
	} else {
		// 不是FROM子句，是目标表，根据语句类型添加
		switch l.firstOpType {
		case analyzer.StmtTypeSelect:
			// SELECT语句，所有表都是读表
			l.reads = append(l.reads, tableDep)
		case analyzer.StmtTypeCreateTemporaryTable:
			// CREATE TEMPORARY TABLE语句的目标表应该被添加为写表
			// （已经在EnterCreateTableStatement中处理）
		case analyzer.StmtTypeInsert, analyzer.StmtTypeUpdate, analyzer.StmtTypeDelete,
			analyzer.StmtTypeCreateTable, analyzer.StmtTypeDropTable,
			analyzer.StmtTypeTruncateTable, analyzer.StmtTypeCreateView:
			// 这些语句中的表都是目标表，添加到写表
			l.writes = append(l.writes, tableDep)
		}
	}
}

// EnterFromClause 监听进入FROM子句
func (l *lineageListener) EnterFromClause(ctx *parser.FromClauseContext) {
	l.isOnlyComment = false
	// 设置标志，表示正在处理源表
	l.isProcessingSourceTable = true
}

// ExitFromClause 监听离开FROM子句
func (l *lineageListener) ExitFromClause(ctx *parser.FromClauseContext) {
	// 清除标志，表示已经处理完源表
	l.isProcessingSourceTable = false
}

// EnterSelectStatement 监听进入查询语句
func (l *lineageListener) EnterSelectStatement(ctx *parser.SelectStatementContext) {
	l.isOnlyComment = false
	// 只有当firstOpType尚未设置时，才设置为SELECT
	// 或者当firstOpType是CREATE TEMPORARY TABLE时，不覆盖，保持为CREATE TEMPORARY TABLE
	if l.firstOpType == "" {
		l.firstOpType = analyzer.StmtTypeSelect
	}
}

// EnterSelectStatement 监听进入查询语句
func (l *ddlListener) EnterSelectStatement(ctx *parser.SelectStatementContext) {
	l.isOnlyComment = false
	// 只有当firstOpType尚未设置时，才设置为SELECT
	if l.firstOpType == "" {
		l.firstOpType = analyzer.StmtTypeSelect
	}
}

// EnterInsertClause 监听进入插入语句
func (l *lineageListener) EnterInsertClause(ctx *parser.InsertClauseContext) {
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
					tableDep := &analyzer.Dependency{
						Cluster:  l.defaultCluster,
						Database: db,
						Table:    table,
					}
					l.writes = append(l.writes, tableDep)
					break
				}
			}
		}
	}
}

// EnterInsertClause 监听进入插入语句
func (l *ddlListener) EnterInsertClause(ctx *parser.InsertClauseContext) {
	l.isOnlyComment = false
	l.firstOpType = analyzer.StmtTypeInsert
}

// EnterUpdateStatement 监听进入更新语句
func (l *lineageListener) EnterUpdateStatement(ctx *parser.UpdateStatementContext) {
	l.isOnlyComment = false
	l.firstOpType = analyzer.StmtTypeUpdate
}

// EnterUpdateStatement 监听进入更新语句
func (l *ddlListener) EnterUpdateStatement(ctx *parser.UpdateStatementContext) {
	l.isOnlyComment = false
	l.firstOpType = analyzer.StmtTypeUpdate
}

// EnterDeleteStatement 监听进入删除语句
func (l *lineageListener) EnterDeleteStatement(ctx *parser.DeleteStatementContext) {
	l.isOnlyComment = false
	l.firstOpType = analyzer.StmtTypeDelete
}

// EnterDeleteStatement 监听进入删除语句
func (l *ddlListener) EnterDeleteStatement(ctx *parser.DeleteStatementContext) {
	l.isOnlyComment = false
	l.firstOpType = analyzer.StmtTypeDelete
}

// EnterCreateTableStatement 监听进入创建表语句
func (l *ddlListener) EnterCreateTableStatement(ctx *parser.CreateTableStatementContext) {
	l.isOnlyComment = false
	if ctx.KW_TEMPORARY() != nil {
		l.firstOpType = analyzer.StmtTypeCreateTemporaryTable
	} else {
		l.firstOpType = analyzer.StmtTypeCreateTable
	}

	// 提取表名
	if ctx.TableName() != nil {
		tableName := ctx.TableName().GetText()
		l.currentTableName = tableName

		// 解析数据库和表名
		db, table := "", ""
		if strings.Contains(tableName, ".") {
			parts := strings.Split(tableName, ".")
			db = parts[0]
			table = parts[1]
		} else {
			table = tableName
			db = l.defaultDatabase
		}

		// 提取列信息
		columns := []*analyzer.ActionColumn{}
		if ctx.ColumnNameTypeOrConstraintList() != nil {
			for _, colOrConstraint := range ctx.ColumnNameTypeOrConstraintList().AllColumnNameTypeOrConstraint() {
				if columnDef := colOrConstraint.ColumnNameTypeConstraint(); columnDef != nil {
					columnName := ""
					columnType := ""
					comment := ""

					// 提取列名
					if columnDef.Id_() != nil {
						columnName = columnDef.Id_().GetText()
					}

					// 提取列类型
					if columnDef.ColType() != nil {
						columnType = columnDef.ColType().GetText()
					}

					// 提取列注释
					if columnDef.GetComment() != nil {
						commentText := columnDef.GetComment().GetText()
						comment = util.TrimQuotes(commentText)
					}

					if columnName != "" {
						columns = append(columns, &analyzer.ActionColumn{
							Name:    columnName,
							Type:    columnType,
							Comment: comment,
							Action:  analyzer.ActionTypeCreate,
						})
					}
				}
			}
		}

		// 添加CREATE TABLE action
		action := &analyzer.ActionInfo{
			ClusterName:  l.defaultCluster,
			DatabaseName: db,
			TableName:    table,
			Columns:      columns,
			ActionType:   analyzer.ActionTypeCreate,
		}
		l.result.Action = action
	}
}

// EnterCreateTableStatement 监听进入创建表语句
func (l *lineageListener) EnterCreateTableStatement(ctx *parser.CreateTableStatementContext) {
	l.isOnlyComment = false
	if ctx.KW_TEMPORARY() != nil {
		l.firstOpType = analyzer.StmtTypeCreateTemporaryTable
		// 将目标表添加到写表
		if ctx.TableName() != nil {
			tableName := ctx.TableName().GetText()
			// 解析数据库和表名
			db, table := "", ""
			if strings.Contains(tableName, ".") {
				parts := strings.Split(tableName, ".")
				db = parts[0]
				table = parts[1]
			} else {
				table = tableName
				db = l.defaultDatabase
			}
			// 创建写表依赖
			tableDep := &analyzer.Dependency{
				Cluster:  l.defaultCluster,
				Database: db,
				Table:    table,
			}
			l.writes = append(l.writes, tableDep)
		}
	} else {
		l.firstOpType = analyzer.StmtTypeCreateTable
	}
}

// EnterAlterStatement 监听进入修改表语句
func (l *ddlListener) EnterAlterStatement(ctx *parser.AlterStatementContext) {
	l.isOnlyComment = false
	// 只有当firstOpType还没有设置时，才处理ALTER TABLE语句
	if l.firstOpType != analyzer.StmtTypeAlterTable {
		l.firstOpType = analyzer.StmtTypeAlterTable

		// 提取表名
		if ctx.TableName() != nil {
			tableName := ctx.TableName().GetText()
			l.currentTableName = tableName

			// 解析数据库和表名
			db, table := "", ""
			if strings.Contains(tableName, ".") {
				parts := strings.Split(tableName, ".")
				db = parts[0]
				table = parts[1]
			} else {
				table = tableName
				db = l.defaultDatabase
			}

			// 检查是否是表重命名操作
			isRenameTable := false
			if ctx.AlterTableStatementSuffix() != nil {
				alterTableSuffix := ctx.AlterTableStatementSuffix()
				// 直接检查是否是表重命名操作
				if alterTableSuffix.AlterStatementSuffixRename() != nil {
					isRenameTable = true
				}
			}

			// 处理其他ALTER操作
			columns := []*analyzer.ActionColumn{}
			if !isRenameTable && ctx.AlterTableStatementSuffix() != nil {
				alterTableSuffix := ctx.AlterTableStatementSuffix()
				if alterTblPartitionSuffix := alterTableSuffix.AlterTblPartitionStatementSuffix(); alterTblPartitionSuffix != nil {
					// 处理添加列
					if addColCtx := alterTblPartitionSuffix.AlterStatementSuffixAddCol(); addColCtx != nil {
						if addColCtx.ColumnNameTypeList() != nil {
							for _, col := range addColCtx.ColumnNameTypeList().AllColumnNameType() {
								columnName := ""
								columnType := ""
								comment := ""

								// 提取列名
								if col.Id_() != nil {
									columnName = col.Id_().GetText()
								}

								// 提取列类型
								if col.ColType() != nil {
									columnType = col.ColType().GetText()
								}

								// 提取列注释
								if col.GetComment() != nil {
									commentText := col.GetComment().GetText()
									comment = util.TrimQuotes(commentText)
								}

								if columnName != "" {
									columns = append(columns, &analyzer.ActionColumn{
										Name:    columnName,
										Type:    columnType,
										Comment: comment,
										Action:  analyzer.ActionTypeCreate,
									})
								}
							}
						}
					}
					// 处理修改列
					if alterTblPartitionSuffix.AlterStatementSuffixUpdateColumns() != nil {
						// 处理更新列的情况
						columns = append(columns, &analyzer.ActionColumn{
							Action: analyzer.ActionTypeAlter,
						})
					}
					// 处理重命名列
					if renameColCtx := alterTblPartitionSuffix.AlterStatementSuffixRenameCol(); renameColCtx != nil {
						if renameColCtx.GetOldName() != nil {
							columnName := renameColCtx.GetOldName().GetText()
							columnType := ""
							comment := ""
							if renameColCtx.ColType() != nil {
								columnType = renameColCtx.ColType().GetText()
							}
							// 提取列注释
							if renameColCtx.GetComment() != nil {
								commentText := renameColCtx.GetComment().GetText()
								comment = util.TrimQuotes(commentText)
							}
							columns = append(columns, &analyzer.ActionColumn{
								Name:    columnName,
								Type:    columnType,
								Comment: comment,
								Action:  analyzer.ActionTypeAlter,
							})
						}
					}
					// 处理删除列
					// if dropColCtx := alterTblPartitionSuffix.GetAlterStatementSuffixDropCol(); dropColCtx != nil {
					// 	if dropColCtx.GetName() != nil {
					// 		columnName := dropColCtx.GetName().GetText()
					// 		columns = append(columns, &analyzer.ActionColumn{
					// 			Name:   columnName,
					// 			ActionType: analyzer.ActionTypeDrop,
					// 		})
					// 	}
					// }
				}
			}

			// 清空Actions，确保只添加一个ActionTable
			l.result.Action = nil

			// 添加ActionTable
			action := &analyzer.ActionInfo{
				ClusterName:  l.defaultCluster,
				DatabaseName: db,
				TableName:    table,
				Columns:      columns,
				ActionType:   analyzer.ActionTypeAlter,
			}
			l.result.Action = action
		}
	}
}

// EnterAlterStatement 监听进入修改表语句
func (l *lineageListener) EnterAlterStatement(ctx *parser.AlterStatementContext) {
	l.isOnlyComment = false
	// 只有当firstOpType还没有设置时，才处理ALTER TABLE语句
	if l.firstOpType != analyzer.StmtTypeAlterTable {
		l.firstOpType = analyzer.StmtTypeAlterTable

		// 提取表名
		if ctx.TableName() != nil {
			tableName := ctx.TableName().GetText()

			// 解析数据库和表名
			db, table := "", ""
			if strings.Contains(tableName, ".") {
				parts := strings.Split(tableName, ".")
				db = parts[0]
				table = parts[1]
			} else {
				table = tableName
				db = l.defaultDatabase
			}

			// 添加写表
			tableDep := &analyzer.Dependency{
				Cluster:  l.defaultCluster,
				Database: db,
				Table:    table,
			}
			l.writes = []*analyzer.Dependency{tableDep}
		}
	}
}

// EnterDropTableStatement 监听进入删除表语句
func (l *ddlListener) EnterDropTableStatement(ctx *parser.DropTableStatementContext) {
	l.isOnlyComment = false
	l.firstOpType = analyzer.StmtTypeDropTable

	// 提取表名
	if ctx.TableName() != nil {
		tableName := ctx.TableName().GetText()

		// 解析数据库和表名
		db, table := "", ""
		if strings.Contains(tableName, ".") {
			parts := strings.Split(tableName, ".")
			db = parts[0]
			table = parts[1]
		} else {
			table = tableName
			db = l.defaultDatabase
		}

		// 添加DROP TABLE action
		action := &analyzer.ActionInfo{
			ClusterName:  l.defaultCluster,
			DatabaseName: db,
			TableName:    table,
			ActionType:   analyzer.ActionTypeDrop,
		}
		l.result.Action = action
	}
}

// EnterDropTableStatement 监听进入删除表语句
func (l *lineageListener) EnterDropTableStatement(ctx *parser.DropTableStatementContext) {
	l.isOnlyComment = false
	l.firstOpType = analyzer.StmtTypeDropTable
}

// EnterCreateViewStatement 监听进入创建视图语句
func (l *lineageListener) EnterCreateViewStatement(ctx *parser.CreateViewStatementContext) {
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
					tableDep := &analyzer.Dependency{
						Cluster:  l.defaultCluster,
						Database: db,
						Table:    view,
					}
					l.writes = append(l.writes, tableDep)
					break
				}
			}
		}
	}
}

// EnterCreateViewStatement 监听进入创建视图语句
func (l *ddlListener) EnterCreateViewStatement(ctx *parser.CreateViewStatementContext) {
	l.isOnlyComment = false
	l.firstOpType = analyzer.StmtTypeCreateView

	// 提取视图名
	if ctx.TableName() != nil {
		viewName := ctx.TableName().GetText()

		// 解析数据库和视图名
		db, view := "", ""
		if strings.Contains(viewName, ".") {
			parts := strings.Split(viewName, ".")
			db = parts[0]
			view = parts[1]
		} else {
			view = viewName
			db = l.defaultDatabase
		}

		// 添加CREATE VIEW action
		action := &analyzer.ActionInfo{
			ClusterName:  l.defaultCluster,
			DatabaseName: db,
			TableName:    view,
			ActionType:   analyzer.ActionTypeCreate,
		}
		l.result.Action = action
	}
}

// EnterDropViewStatement 监听进入删除视图语句
func (l *lineageListener) EnterDropViewStatement(ctx *parser.DropViewStatementContext) {
	l.isOnlyComment = false
	l.firstOpType = analyzer.StmtTypeDropTable
}

// EnterDropViewStatement 监听进入删除视图语句
func (l *ddlListener) EnterDropViewStatement(ctx *parser.DropViewStatementContext) {
	l.isOnlyComment = false
	l.firstOpType = analyzer.StmtTypeDropTable
}

// EnterTruncateTableStatement 监听进入截断表语句
func (l *lineageListener) EnterTruncateTableStatement(ctx *parser.TruncateTableStatementContext) {
	l.isOnlyComment = false
	l.firstOpType = analyzer.StmtTypeTruncateTable
}

// EnterTruncateTableStatement 监听进入截断表语句
func (l *ddlListener) EnterTruncateTableStatement(ctx *parser.TruncateTableStatementContext) {
	l.isOnlyComment = false
	l.firstOpType = analyzer.StmtTypeTruncateTable
}

// EnterSwitchDatabaseStatement 监听进入USE语句
func (l *lineageListener) EnterSwitchDatabaseStatement(ctx *parser.SwitchDatabaseStatementContext) {
	l.isOnlyComment = false
	l.firstOpType = analyzer.StmtTypeUseDatabase

	// 提取数据库名并添加到Reads
	if ctx.Id_() != nil {
		database := ctx.Id_().GetText()
		tableDep := &analyzer.Dependency{
			Cluster:  l.defaultCluster,
			Database: database,
			Table:    "",
		}
		l.reads = append(l.reads, tableDep)
	}
}

// EnterSwitchDatabaseStatement 监听进入USE语句
func (l *ddlListener) EnterSwitchDatabaseStatement(ctx *parser.SwitchDatabaseStatementContext) {
	l.isOnlyComment = false
	l.firstOpType = analyzer.StmtTypeUseDatabase
}

// EnterTableName 监听进入表名
func (l *lineageListener) EnterTableName(ctx *parser.TableNameContext) {
	l.isOnlyComment = false
	l.extractTable(ctx)
}

// ExitStatement 监听离开语句，将临时存储的表信息复制到result中
func (l *lineageListener) ExitStatement(ctx *parser.StatementContext) {
	// 将临时存储的表信息复制到result中
	l.result.Reads = l.reads
	l.result.Writes = l.writes
}

// EnterTableSource 监听进入表源
func (l *lineageListener) EnterTableSource(ctx *parser.TableSourceContext) {
	l.isOnlyComment = false
	// 表源可能包含表名
}

// EnterViewName 监听进入视图名
func (l *lineageListener) EnterViewName(ctx *parser.ViewNameContext) {
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
		tableDep := &analyzer.Dependency{
			Cluster:  l.defaultCluster,
			Database: db,
			Table:    view,
		}
		if l.firstOpType == analyzer.StmtTypeSelect {
			l.reads = append(l.reads, tableDep)
		} else {
			l.writes = append(l.writes, tableDep)
		}
	}
}

// EnterCteStatement 监听进入CTE语句
func (l *lineageListener) EnterCteStatement(ctx *parser.CteStatementContext) {
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

// EnterWithClause 监听进入WithClause
func (l *lineageListener) EnterWithClause(ctx *parser.WithClauseContext) {
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
