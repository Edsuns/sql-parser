package mysql

import (
	"strings"

	"github.com/Edsuns/sql-parser/analyzer"
	"github.com/Edsuns/sql-parser/internal/mysql/parser"
	"github.com/Edsuns/sql-parser/internal/util"
)

// lineageListener 自定义监听器，用于提取SQL语句中的血缘关系信息
type lineageListener struct {
	*parser.BaseMySQLParserListener

	result          *analyzer.LineageResult
	defaultCluster  string
	defaultDatabase string
	curOpType       analyzer.StmtType // 当前操作类型
	firstOpType     analyzer.StmtType // 第一个操作类型
	comments        []string          // 存储解析到的注释
	isOnlyComment   bool              // 标记当前SQL是否只包含注释
	isWriteOp       bool              // 是否已遇到写入操作
	cteNames        map[string]bool   // 存储CTE名称，避免将CTE作为表依赖
}

// onWriteStmt 标记为写入操作，并设置第一个操作类型
func (l *lineageListener) onWriteStmt() {
	l.isWriteOp = true
	l.isOnlyComment = false
	if l.firstOpType == "" {
		l.firstOpType = l.curOpType
	}
}

// newLineageListener 创建新的血缘分析监听器实例
func newLineageListener(defaultCluster, defaultDatabase string) *lineageListener {
	return &lineageListener{
		result: &analyzer.LineageResult{
			Reads:  []*analyzer.Dependency{},
			Writes: []*analyzer.Dependency{},
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

// ddlListener 自定义监听器，用于提取SQL语句中的DDL信息
type ddlListener struct {
	*parser.BaseMySQLParserListener

	result          *analyzer.DDLResult
	defaultCluster  string
	defaultDatabase string
	curOpType       analyzer.StmtType // 当前操作类型
	firstOpType     analyzer.StmtType // 第一个操作类型
	comments        []string          // 存储解析到的注释
	isOnlyComment   bool              // 标记当前SQL是否只包含注释
	isWriteOp       bool              // 是否已遇到写入操作
}

// onWriteStmt 标记为写入操作，并设置第一个操作类型
func (l *ddlListener) onWriteStmt() {
	l.isWriteOp = true
	l.isOnlyComment = false
	if l.firstOpType == "" {
		l.firstOpType = l.curOpType
	}
}

// newDDLListener 创建新的DDL分析监听器实例
func newDDLListener(defaultCluster, defaultDatabase string) *ddlListener {
	return &ddlListener{
		result:          &analyzer.DDLResult{},
		defaultCluster:  defaultCluster,
		defaultDatabase: defaultDatabase,
		curOpType:       "",
		comments:        []string{},
		isOnlyComment:   true, // 默认认为是只有注释，遇到非注释内容时设置为false
		isWriteOp:       false,
	}
}

// EnterQueryExpression 进入查询表达式时调用，用于处理SELECT部分
func (l *lineageListener) EnterQueryExpression(ctx *parser.QueryExpressionContext) {
	// 设置为SELECT操作类型，确保表引用被正确识别为读表
	l.curOpType = analyzer.StmtTypeSelect
	l.isOnlyComment = false
	// 如果还没有遇到写入操作，设置第一个操作类型
	if !l.isWriteOp && l.firstOpType == "" {
		l.firstOpType = analyzer.StmtTypeSelect
	}
}

// EnterQuerySpecification 进入查询规范时调用，这是SELECT语句的主要部分
func (l *lineageListener) EnterQuerySpecification(ctx *parser.QuerySpecificationContext) {
	// 设置为SELECT操作类型，确保表引用被正确识别为读表
	l.curOpType = analyzer.StmtTypeSelect
	l.isOnlyComment = false
	// 如果还没有遇到写入操作，设置第一个操作类型
	if !l.isWriteOp && l.firstOpType == "" {
		l.firstOpType = analyzer.StmtTypeSelect
	}
}

// ExitQueryExpression 退出查询表达式时调用
func (l *lineageListener) ExitQueryExpression(ctx *parser.QueryExpressionContext) {
	// 不需要恢复原始操作类型，因为INSERT语句的写表已经在EnterInsertStatement中处理
}

// ExitQuerySpecification 退出查询规范时调用
func (l *lineageListener) ExitQuerySpecification(ctx *parser.QuerySpecificationContext) {
	// 不需要恢复原始操作类型，因为INSERT语句的写表已经在EnterInsertStatement中处理
}

// EnterInsertStatement 进入INSERT语句时调用
func (l *lineageListener) EnterInsertStatement(ctx *parser.InsertStatementContext) {
	// 设置为INSERT操作类型
	l.curOpType = analyzer.StmtTypeInsert
	l.onWriteStmt()
}

// EnterInsertQueryExpression 进入INSERT语句的查询表达式时调用，用于处理SELECT部分
func (l *lineageListener) EnterInsertQueryExpression(ctx *parser.InsertQueryExpressionContext) {
	// 设置为SELECT操作类型，确保表引用被正确识别为读表
	l.curOpType = analyzer.StmtTypeSelect
	l.isOnlyComment = false
}

// EnterInsertFromConstructor 进入INSERT语句的FROM子句时调用，用于处理SELECT部分
func (l *lineageListener) EnterInsertFromConstructor(ctx *parser.InsertFromConstructorContext) {
	// 设置为SELECT操作类型，确保表引用被正确识别为读表
	l.curOpType = analyzer.StmtTypeSelect
	l.isOnlyComment = false
}

// EnterFromClause 进入FROM子句时调用
func (l *lineageListener) EnterFromClause(ctx *parser.FromClauseContext) {
	// 确保当前操作类型为SELECT，以便表引用被正确识别为读表
	l.curOpType = analyzer.StmtTypeSelect
	l.isOnlyComment = false
}

// EnterUpdateStatement 进入UPDATE语句时调用
func (l *lineageListener) EnterUpdateStatement(ctx *parser.UpdateStatementContext) {
	l.curOpType = analyzer.StmtTypeUpdate
	l.onWriteStmt()
}

// EnterDeleteStatement 进入DELETE语句时调用
func (l *lineageListener) EnterDeleteStatement(ctx *parser.DeleteStatementContext) {
	l.curOpType = analyzer.StmtTypeDelete
	l.onWriteStmt()
}

// EnterTruncateTableStatement 进入TRUNCATE TABLE语句时调用
func (l *lineageListener) EnterTruncateTableStatement(ctx *parser.TruncateTableStatementContext) {
	l.curOpType = analyzer.StmtTypeTruncateTable
	l.onWriteStmt()
}

// EnterInsertStatement 进入INSERT语句时调用
func (l *ddlListener) EnterInsertStatement(ctx *parser.InsertStatementContext) {
	l.curOpType = analyzer.StmtTypeInsert
	l.onWriteStmt()
}

// EnterUpdateStatement 进入UPDATE语句时调用
func (l *ddlListener) EnterUpdateStatement(ctx *parser.UpdateStatementContext) {
	l.curOpType = analyzer.StmtTypeUpdate
	l.onWriteStmt()
}

// EnterDeleteStatement 进入DELETE语句时调用
func (l *ddlListener) EnterDeleteStatement(ctx *parser.DeleteStatementContext) {
	l.curOpType = analyzer.StmtTypeDelete
	l.onWriteStmt()
}

// EnterTruncateTableStatement 进入TRUNCATE TABLE语句时调用
func (l *ddlListener) EnterTruncateTableStatement(ctx *parser.TruncateTableStatementContext) {
	l.curOpType = analyzer.StmtTypeTruncateTable
	l.onWriteStmt()
}

// EnterCreateTable 进入CREATE TABLE语句时调用
func (l *ddlListener) EnterCreateTable(ctx *parser.CreateTableContext) {
	// 检查是否是临时表
	if ctx.TEMPORARY_SYMBOL() != nil {
		l.curOpType = analyzer.StmtTypeCreateTemporaryTable
	} else {
		l.curOpType = analyzer.StmtTypeCreateTable
	}
	l.onWriteStmt()

	// 提取表名
	if ctx.TableName() != nil {
		tableName := ctx.TableName().GetText()

		// 解析数据库和表名
		cluster, database, table := "", "", ""
		isSpecifiedCluster := false
		isSpecifiedDatabase := false
		parts := strings.Split(tableName, ".")
		if len(parts) == 1 {
			table = parts[0]
			database = l.defaultDatabase
		} else if len(parts) == 2 {
			database = parts[0]
			table = parts[1]
			isSpecifiedDatabase = true
		} else if len(parts) > 2 {
			cluster = parts[0]
			database = parts[1]
			table = parts[2]
			isSpecifiedCluster = true
			isSpecifiedDatabase = true
		}

		// 提取列信息
		columns := []*analyzer.ActionColumn{}
		if ctx.TableElementList() != nil {
			for _, tableElement := range ctx.TableElementList().AllTableElement() {
				if columnDef := tableElement.ColumnDefinition(); columnDef != nil {
					columnName := ""
					columnType := ""
					comment := ""
					isNotNull := false
					isPrimary := false
					defaultValue := ""

					// 提取列名
					if columnDef.ColumnName() != nil && columnDef.ColumnName().Identifier() != nil {
						columnName = columnDef.ColumnName().Identifier().GetText()
					}

					// 提取列类型
					if columnDef.FieldDefinition() != nil && columnDef.FieldDefinition().DataType() != nil {
						columnType = columnDef.FieldDefinition().DataType().GetText()
					}

					// 提取列属性
					if columnDef.FieldDefinition() != nil {
						for _, attr := range columnDef.FieldDefinition().AllColumnAttribute() {
							// 提取注释
							if attr.COMMENT_SYMBOL() != nil && attr.TextLiteral() != nil {
								commentText := attr.TextLiteral().GetText()
								comment = util.TrimQuotes(commentText)
							}
							// 提取NOT NULL约束
							if attr.NOT_SYMBOL() != nil {
								isNotNull = true
							}
							// 提取默认值
							if attr.DEFAULT_SYMBOL() != nil {
								if attr.NowOrSignedLiteral() != nil {
									defaultValue = attr.NowOrSignedLiteral().GetText()
								}
							}
							// 提取PRIMARY KEY约束
							if attr.PRIMARY_SYMBOL() != nil {
								isPrimary = true
							}
						}
					}

					// 检查是否是表级PRIMARY KEY约束
					if tableElement.TableConstraintDef() != nil {
						constraintDef := tableElement.TableConstraintDef()
						if constraintDef.PRIMARY_SYMBOL() != nil {
							isPrimary = true
						}
					}

					if columnName != "" {
						columns = append(columns, &analyzer.ActionColumn{
							Name:         columnName,
							Type:         columnType,
							Comment:      comment,
							Action:       analyzer.ActionTypeCreate,
							IsNotNull:    isNotNull,
							IsPrimary:    isPrimary,
							DefaultValue: defaultValue,
						})
					}
				}
			}
		}

		// 设置CREATE TABLE action
		clusterName := l.defaultCluster
		if cluster != "" {
			clusterName = cluster
		}
		l.result.Action = &analyzer.ActionInfo{
			ClusterName:         clusterName,
			IsSpecifiedCluster:  isSpecifiedCluster,
			DatabaseName:        database,
			IsSpecifiedDatabase: isSpecifiedDatabase,
			TableName:           table,
			Columns:             columns,
			ActionType:          analyzer.ActionTypeCreate,
		}
	}
}

// EnterCreateView 进入CREATE VIEW语句时调用
func (l *lineageListener) EnterCreateView(ctx *parser.CreateViewContext) {
	l.curOpType = analyzer.StmtTypeCreateView
	l.onWriteStmt()
}

// EnterCreateView 进入CREATE VIEW语句时调用
func (l *ddlListener) EnterCreateView(ctx *parser.CreateViewContext) {
	l.curOpType = analyzer.StmtTypeCreateView
	l.onWriteStmt()
}

// EnterDropTable 进入DROP TABLE语句时调用
func (l *ddlListener) EnterDropTable(ctx *parser.DropTableContext) {
	l.curOpType = analyzer.StmtTypeDropTable
	l.onWriteStmt()

	// 提取表名
	if ctx.TableRefList() != nil {
		for _, tableRefCtx := range ctx.TableRefList().AllTableRef() {
			tableName := tableRefCtx.GetText()

			// 解析数据库和表名
			cluster, database, table := "", "", ""
			isSpecifiedCluster := false
			isSpecifiedDatabase := false
			parts := strings.Split(tableName, ".")
			if len(parts) == 1 {
				table = parts[0]
				database = l.defaultDatabase
			} else if len(parts) == 2 {
				database = parts[0]
				table = parts[1]
				isSpecifiedDatabase = true
			} else if len(parts) > 2 {
				cluster = parts[0]
				database = parts[1]
				table = parts[2]
				isSpecifiedCluster = true
				isSpecifiedDatabase = true
			}

			// 设置DROP TABLE action
			clusterName := l.defaultCluster
			if cluster != "" {
				clusterName = cluster
			}
			l.result.Action = &analyzer.ActionInfo{
				ClusterName:         clusterName,
				IsSpecifiedCluster:  isSpecifiedCluster,
				DatabaseName:        database,
				IsSpecifiedDatabase: isSpecifiedDatabase,
				TableName:           table,
				ActionType:          analyzer.ActionTypeDrop,
			}
		}
	}
}

// EnterAlterTable 进入ALTER TABLE语句时调用
func (l *ddlListener) EnterAlterTable(ctx *parser.AlterTableContext) {
	l.curOpType = analyzer.StmtTypeAlterTable
	l.onWriteStmt()

	// 提取表名
	if ctx.TableRef() != nil {
		tableName := ctx.TableRef().GetText()

		// 解析数据库和表名
		cluster, database, table := "", "", ""
		isSpecifiedCluster := false
		isSpecifiedDatabase := false
		parts := strings.Split(tableName, ".")
		if len(parts) == 1 {
			table = parts[0]
			database = l.defaultDatabase
		} else if len(parts) == 2 {
			database = parts[0]
			table = parts[1]
			isSpecifiedDatabase = true
		} else if len(parts) > 2 {
			cluster = parts[0]
			database = parts[1]
			table = parts[2]
			isSpecifiedCluster = true
			isSpecifiedDatabase = true
		}

		// 提取列信息
		columns := []*analyzer.ActionColumn{}

		// 处理ALTER TABLE actions
		if ctx.AlterTableActions() != nil {
			alterActions := ctx.AlterTableActions()
			if alterActions.AlterCommandList() != nil {
				alterCommandList := alterActions.AlterCommandList()
				if alterCommandList.AlterList() != nil {
					alterList := alterCommandList.AlterList()
					for _, alterItem := range alterList.AllAlterListItem() {
						// 检查是否是添加列的操作
						if alterItem.ADD_SYMBOL() != nil {
							// 处理单个列添加
							if alterItem.FieldDefinition() != nil {
								fieldDef := alterItem.FieldDefinition()
								columnName := ""
								columnType := ""
								comment := ""

								// 提取列名
								if alterItem.Identifier() != nil {
									columnName = alterItem.Identifier().GetText()
								}

								// 提取列类型
								if fieldDef.DataType() != nil {
									columnType = fieldDef.DataType().GetText()
								}

								// 提取列注释
								for _, attr := range fieldDef.AllColumnAttribute() {
									if attr.COMMENT_SYMBOL() != nil && attr.TextLiteral() != nil {
										commentText := attr.TextLiteral().GetText()
										comment = util.TrimQuotes(commentText)
										break
									}
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

							// 处理多个列添加
							if alterItem.OPEN_PAR_SYMBOL() != nil && alterItem.TableElementList() != nil {
								tableElementList := alterItem.TableElementList()
								for _, tableElement := range tableElementList.AllTableElement() {
									if tableElement.ColumnDefinition() != nil {
										columnDef := tableElement.ColumnDefinition()
										columnName := ""
										columnType := ""
										comment := ""

										// 提取列名
										if columnDef.ColumnName() != nil && columnDef.ColumnName().Identifier() != nil {
											columnName = columnDef.ColumnName().Identifier().GetText()
										}

										// 提取列类型
										if columnDef.FieldDefinition() != nil && columnDef.FieldDefinition().DataType() != nil {
											columnType = columnDef.FieldDefinition().DataType().GetText()
										}

										// 提取列注释
										if columnDef.FieldDefinition() != nil {
											for _, attr := range columnDef.FieldDefinition().AllColumnAttribute() {
												if attr.COMMENT_SYMBOL() != nil && attr.TextLiteral() != nil {
													commentText := attr.TextLiteral().GetText()
													comment = util.TrimQuotes(commentText)
													break
												}
											}
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
						}
						// 检查是否是删除列的操作
						if alterItem.DROP_SYMBOL() != nil {
							if alterItem.ColumnInternalRef() != nil {
								columnName := alterItem.ColumnInternalRef().GetText()
								if columnName != "" {
									columns = append(columns, &analyzer.ActionColumn{
										Name:   columnName,
										Action: analyzer.ActionTypeDrop,
									})
								}
							}
						}
						// 检查是否是修改列的操作
						if alterItem.MODIFY_SYMBOL() != nil || alterItem.CHANGE_SYMBOL() != nil {
							if alterItem.FieldDefinition() != nil {
								fieldDef := alterItem.FieldDefinition()
								columnName := ""
								columnType := ""
								comment := ""

								// 提取列名
								if alterItem.CHANGE_SYMBOL() != nil {
									// 对于CHANGE COLUMN，ColumnInternalRef是旧列名
									if alterItem.ColumnInternalRef() != nil {
										columnName = alterItem.ColumnInternalRef().GetText()
									}
								} else {
									// 对于MODIFY COLUMN，ColumnInternalRef是列名
									if alterItem.ColumnInternalRef() != nil {
										columnName = alterItem.ColumnInternalRef().GetText()
									}
								}

								// 提取列类型
								if fieldDef.DataType() != nil {
									columnType = fieldDef.DataType().GetText()
								}

								// 提取列注释
								for _, attr := range fieldDef.AllColumnAttribute() {
									if attr.COMMENT_SYMBOL() != nil && attr.TextLiteral() != nil {
										commentText := attr.TextLiteral().GetText()
										comment = util.TrimQuotes(commentText)
										break
									}
								}

								if columnName != "" {
									columns = append(columns, &analyzer.ActionColumn{
										Name:    columnName,
										Type:    columnType,
										Comment: comment,
										Action:  analyzer.ActionTypeAlter,
									})
								}
							}
						}
					}
				}
			}
		}

		// 设置ALTER TABLE action
		clusterName := l.defaultCluster
		if cluster != "" {
			clusterName = cluster
		}
		l.result.Action = &analyzer.ActionInfo{
			ClusterName:         clusterName,
			IsSpecifiedCluster:  isSpecifiedCluster,
			DatabaseName:        database,
			IsSpecifiedDatabase: isSpecifiedDatabase,
			TableName:           table,
			Columns:             columns,
			ActionType:          analyzer.ActionTypeAlter,
		}
	}
}

// EnterReplaceStatement 进入REPLACE语句时调用
func (l *lineageListener) EnterReplaceStatement(ctx *parser.ReplaceStatementContext) {
	l.curOpType = analyzer.StmtTypeReplaceTable
	l.onWriteStmt()
}

// EnterReplaceStatement 进入REPLACE语句时调用
func (l *ddlListener) EnterReplaceStatement(ctx *parser.ReplaceStatementContext) {
	l.curOpType = analyzer.StmtTypeReplaceTable
	l.onWriteStmt()
}

// EnterCreateTable 进入CREATE TABLE语句时调用
func (l *lineageListener) EnterCreateTable(ctx *parser.CreateTableContext) {
	// 检查是否是临时表
	if ctx.TEMPORARY_SYMBOL() != nil {
		l.curOpType = analyzer.StmtTypeCreateTemporaryTable
	} else {
		l.curOpType = analyzer.StmtTypeCreateTable
	}
	l.onWriteStmt()

	// 提取表名
	if ctx.TableName() != nil {
		tableName := ctx.TableName().GetText()

		// 解析数据库和表名
		cluster, database, table := "", "", ""
		parts := strings.Split(tableName, ".")
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

		// 添加写表
		l.addWriteTable(cluster, database, table)
	}
}

// EnterUseCommand 进入USE语句时调用
func (l *lineageListener) EnterUseCommand(ctx *parser.UseCommandContext) {
	l.curOpType = analyzer.StmtTypeUseDatabase
	l.isOnlyComment = false
	if l.firstOpType == "" {
		l.firstOpType = analyzer.StmtTypeUseDatabase
	}

	// 提取数据库名并添加到Reads
	if ctx.SchemaRef() != nil {
		identifier := ctx.SchemaRef().Identifier()
		if identifier != nil {
			pureIdentifier := identifier.PureIdentifier()
			if pureIdentifier != nil {
				if ident := pureIdentifier.IDENTIFIER(); ident != nil {
					database := ident.GetText()
					l.addReadTable("", database, "")
				} else if quotedIdent := pureIdentifier.BACK_TICK_QUOTED_ID(); quotedIdent != nil {
					database := quotedIdent.GetText()
					l.addReadTable("", database, "")
				} else if doubleQuotedIdent := pureIdentifier.DOUBLE_QUOTED_TEXT(); doubleQuotedIdent != nil {
					database := doubleQuotedIdent.GetText()
					l.addReadTable("", database, "")
				}
			}
		}
	}
}

// EnterUseCommand 进入USE语句时调用
func (l *ddlListener) EnterUseCommand(ctx *parser.UseCommandContext) {
	l.curOpType = analyzer.StmtTypeUseDatabase
	l.isOnlyComment = false
	if l.firstOpType == "" {
		l.firstOpType = analyzer.StmtTypeUseDatabase
	}
}

// EnterWithClause 进入WITH子句时调用，处理CTE
func (l *lineageListener) EnterWithClause(ctx *parser.WithClauseContext) {
	l.isOnlyComment = false
}

// EnterCommonTableExpression 进入公共表表达式时调用，处理CTE
func (l *lineageListener) EnterCommonTableExpression(ctx *parser.CommonTableExpressionContext) {
	l.isOnlyComment = false
	// 记录CTE名称
	if ctx.Identifier() != nil {
		l.cteNames[ctx.Identifier().GetText()] = true
	}
}

// EnterTableRef 进入表引用时调用，用于提取数据库名和表名
func (l *lineageListener) EnterTableRef(ctx *parser.TableRefContext) {
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

	// 检查是否是CREATE TEMPORARY TABLE语句中的表名，如果是则跳过，因为已经在EnterCreateTable中处理过了
	if l.curOpType == analyzer.StmtTypeCreateTemporaryTable {
		return
	}

	// 检查父节点链，看看是否在FROM子句中
	isInFromClause := false
	parent := ctx.GetParent()
	for parent != nil {
		if _, ok := parent.(*parser.FromClauseContext); ok {
			isInFromClause = true
			break
		}
		parent = parent.GetParent()
	}

	// 如果在FROM子句中，添加为读表
	if isInFromClause {
		l.addReadTable(cluster, database, table)
		return
	}

	// 其他情况，根据当前操作类型判断
	if l.curOpType == analyzer.StmtTypeSelect {
		l.addReadTable(cluster, database, table)
	} else {
		l.addWriteTable(cluster, database, table)
	}
}

// EnterTableName 进入表名时调用，用于提取数据库名和表名
func (l *lineageListener) EnterTableName(ctx *parser.TableNameContext) {
	// 检查父节点是否是TableRefContext，如果是，由EnterTableRef处理
	parent := ctx.GetParent()
	if _, ok := parent.(*parser.TableRefContext); ok {
		return
	}

	// 检查是否是CREATE TEMPORARY TABLE语句中的表名，如果是则跳过，因为已经在EnterCreateTable中处理过了
	if l.curOpType == analyzer.StmtTypeCreateTemporaryTable {
		return
	}

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

	// 检查是否是INSERT语句的目标表
	// 检查父节点链，看看是否在INSERT语句中
	isInsertTarget := false
	parent = ctx.GetParent()
	for parent != nil {
		if _, ok := parent.(*parser.InsertStatementContext); ok {
			isInsertTarget = true
			break
		}
		parent = parent.GetParent()
	}

	// 如果是INSERT语句的目标表，添加为写表
	if isInsertTarget {
		l.addWriteTable(cluster, database, table)
		return
	}

	// 其他情况都添加为读表
	l.addReadTable(cluster, database, table)
}

// addReadTable 添加读表信息
func (l *lineageListener) addReadTable(cluster, database, table string) {
	if cluster == "" {
		cluster = l.defaultCluster
	}
	if database == "" {
		database = l.defaultDatabase
	}
	l.result.Reads = append(l.result.Reads, &analyzer.Dependency{
		Cluster:  cluster,
		Database: database,
		Table:    table,
	})
}

// addWriteTable 添加写表信息
func (l *lineageListener) addWriteTable(cluster, database, table string) {
	if cluster == "" {
		cluster = l.defaultCluster
	}
	if database == "" {
		database = l.defaultDatabase
	}
	l.result.Writes = append(l.result.Writes, &analyzer.Dependency{
		Cluster:  cluster,
		Database: database,
		Table:    table,
	})
}
