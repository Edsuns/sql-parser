package starrocks

import (
	"strings"

	"github.com/Edsuns/sql-parser/analyzer"
	"github.com/Edsuns/sql-parser/internal/starrocks/parser"
	"github.com/Edsuns/sql-parser/internal/util"
)

// lineageListener 自定义监听器，用于提取SQL语句中的血缘关系信息
type lineageListener struct {
	*parser.BaseStarRocksListener

	result          *analyzer.LineageResult
	defaultCluster  string
	defaultDatabase string
	curOpType       analyzer.StmtType
	firstOpType     analyzer.StmtType
	comments        []string
	isOnlyComment   bool
	isWriteOp       bool
	cteNames        map[string]bool
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
		isOnlyComment:   true,
		isWriteOp:       false,
		cteNames:        make(map[string]bool),
	}
}

// ddlListener 自定义监听器，用于提取SQL语句中的DDL信息
type ddlListener struct {
	*parser.BaseStarRocksListener

	result          *analyzer.DDLResult
	defaultCluster  string
	defaultDatabase string
	curOpType       analyzer.StmtType
	firstOpType     analyzer.StmtType
	comments        []string
	isOnlyComment   bool
	isWriteOp       bool
	techInfo        *analyzer.TableInfo
}

// newDDLListener 创建新的DDL分析监听器实例
func newDDLListener(defaultCluster, defaultDatabase string) *ddlListener {
	return &ddlListener{
		result:          &analyzer.DDLResult{},
		defaultCluster:  defaultCluster,
		defaultDatabase: defaultDatabase,
		curOpType:       "",
		comments:        []string{},
		isOnlyComment:   true,
		isWriteOp:       false,
		techInfo:        &analyzer.TableInfo{},
	}
}

// EnterSingleStatement 进入单条语句时调用
func (l *lineageListener) EnterSingleStatement(ctx *parser.SingleStatementContext) {
	l.isOnlyComment = false
}

// EnterSingleStatement 进入单条语句时调用
func (l *ddlListener) EnterSingleStatement(ctx *parser.SingleStatementContext) {
	l.isOnlyComment = false
}

// EnterCreateTableStatement 进入创建表语句时调用
func (l *ddlListener) EnterCreateTableStatement(ctx *parser.CreateTableStatementContext) {
	l.curOpType = analyzer.StmtTypeCreateTable
	l.onWriteStmt()

	// 提取表名
	if ctx.QualifiedName() != nil {
		tableName := ctx.QualifiedName().GetText()

		// 解析数据库和表名
		cluster, database, table := l.parseTableName(tableName)

		// 提取列信息
		var columns []*analyzer.ActionColumn
		if columnDescs := ctx.AllColumnDesc(); columnDescs != nil {
			for _, columnDesc := range columnDescs {
				columnName := ""
				columnType := ""
				comment := ""
				isNotNull := false
				isPrimary := false

				// 提取列名
				if id := columnDesc.Identifier(); id != nil {
					columnName = util.TrimQuotes(id.GetText())
				}

				// 提取列类型
				if t := columnDesc.Type_(); t != nil {
					columnType = t.GetText()
				}

				// 提取列注释
				if commentCtx := columnDesc.Comment(); commentCtx != nil {
					commentText := commentCtx.String_().GetText()
					comment = util.TrimQuotes(commentText)
				}

				// 提取NOT NULL约束
				if columnDesc.ColumnNullable() != nil && columnDesc.ColumnNullable().NOT() != nil {
					isNotNull = true
				}

				if columnName != "" {
					columns = append(columns, &analyzer.ActionColumn{
						Name:      columnName,
						Type:      columnType,
						Comment:   comment,
						IsNotNull: isNotNull,
						IsPrimary: isPrimary,
						Action:    analyzer.ActionTypeCreate,
					})
				}
			}
		}

		// 处理主键信息
		if keyDesc := ctx.KeyDesc(); keyDesc != nil {
			if keyDesc.PRIMARY() != nil {
				// 查找PRIMARY KEY子句中的列名
				if primaryKeyColumns := keyDesc.IdentifierList(); primaryKeyColumns != nil {
					for _, id := range primaryKeyColumns.AllIdentifier() {
						primaryKeyName := id.GetText()
						// 更新对应列的IsPrimary属性
						for _, col := range columns {
							if col.Name == primaryKeyName {
								col.IsPrimary = true
								break
							}
						}
					}
				}
			}
		}

		// 添加CREATE TABLE action
		action := &analyzer.ActionInfo{
			ClusterName:  cluster,
			DatabaseName: database,
			TableName:    table,
			Columns:      columns,
			ActionType:   analyzer.ActionTypeCreate,
		}
		l.result.Action = action
	}

	// 提取 TableInfo
	l.extractTechInfo(ctx)
}

// extractTechInfo 从 CREATE TABLE 语句中提取 TableInfo 信息
func (l *ddlListener) extractTechInfo(ctx *parser.CreateTableStatementContext) {
	// 初始化 TableInfo
	l.techInfo = &analyzer.TableInfo{
		DistributedColumnNames: []string{},
		PartitionColumnNames:   []string{},
	}

	// 提取分区列
	if partitionDesc := ctx.PartitionDesc(); partitionDesc != nil {
		l.extractPartitionColumns(partitionDesc)
	}

	// 提取分桶列
	if distributionDesc := ctx.DistributionDesc(); distributionDesc != nil {
		l.extractDistributionColumns(distributionDesc)
	}

	// 提取key类型，确定数据模型
	if keyDesc := ctx.KeyDesc(); keyDesc != nil {
		l.extractDataModel(keyDesc)
	}

	// 提取属性信息（压缩方式等）
	if properties := ctx.Properties(); properties != nil {
		l.extractProperties(properties)
	}

	// 将 TableInfo 设置到 ActionInfo 中
	if l.result.Action != nil {
		l.result.Action.TableInfo = l.techInfo
	}
}

// extractDataModel 从 KeyDesc 中提取数据模型信息
func (l *ddlListener) extractDataModel(ctx parser.IKeyDescContext) {
	if ctx.PRIMARY() != nil {
		l.techInfo.DataModel = "PRIMARY"
	} else if ctx.DUPLICATE() != nil {
		l.techInfo.DataModel = "DUPLICATE"
	} else if ctx.UNIQUE() != nil {
		l.techInfo.DataModel = "UNIQUE"
	} else if ctx.AGGREGATE() != nil {
		l.techInfo.DataModel = "AGGREGATE"
	}
}

// extractPartitionColumns 提取分区列信息
func (l *ddlListener) extractPartitionColumns(ctx parser.IPartitionDescContext) {
	// 处理 PARTITION BY RANGE 或 LIST
	if ctx.IdentifierList() != nil {
		for _, id := range ctx.IdentifierList().AllIdentifier() {
			columnName := util.TrimQuotes(id.GetText())
			l.techInfo.PartitionColumnNames = append(l.techInfo.PartitionColumnNames, columnName)
		}
	}
	// 处理多个分区表达式
	for _, expr := range ctx.AllPartitionExpr() {
		columnName := util.TrimQuotes(expr.GetText())
		l.techInfo.PartitionColumnNames = append(l.techInfo.PartitionColumnNames, columnName)
	}
}

// extractDistributionColumns 提取分桶列信息
func (l *ddlListener) extractDistributionColumns(ctx parser.IDistributionDescContext) {
	if ctx.IdentifierList() != nil {
		for _, id := range ctx.IdentifierList().AllIdentifier() {
			columnName := util.TrimQuotes(id.GetText())
			l.techInfo.DistributedColumnNames = append(l.techInfo.DistributedColumnNames, columnName)
		}
	}
}

// extractProperties 提取属性信息
func (l *ddlListener) extractProperties(ctx parser.IPropertiesContext) {
	for _, prop := range ctx.AllProperty() {
		key := prop.GetKey().GetText()
		value := prop.GetValue().GetText()
		// 移除引号
		key = util.TrimQuotes(key)
		value = util.TrimQuotes(value)

		switch strings.ToLower(key) {
		case "compression":
			l.techInfo.Compression = value
		}
	}
}

// EnterCreateTableStatement 进入创建表语句时调用
func (l *lineageListener) EnterCreateTableStatement(ctx *parser.CreateTableStatementContext) {
	// 检查是否是临时表
	if ctx.TEMPORARY() != nil {
		l.curOpType = analyzer.StmtTypeCreateTemporaryTable
	} else {
		l.curOpType = analyzer.StmtTypeCreateTable
	}
	l.onWriteStmt()

	// 提取表名
	if ctx.QualifiedName() != nil {
		tableName := ctx.QualifiedName().GetText()

		// 解析数据库和表名
		cluster, database, table := l.parseTableName(tableName)

		// 添加写表信息
		l.addWriteTable(cluster, database, table)
	}
}

// EnterCreateTableAsSelectStatement 进入创建表AS SELECT语句时调用
func (l *lineageListener) EnterCreateTableAsSelectStatement(ctx *parser.CreateTableAsSelectStatementContext) {
	// 检查是否是临时表
	if ctx.TEMPORARY() != nil {
		l.curOpType = analyzer.StmtTypeCreateTemporaryTable
	} else {
		l.curOpType = analyzer.StmtTypeCreateTable
	}
	l.onWriteStmt()

	// 提取表名
	if ctx.QualifiedName() != nil {
		tableName := ctx.QualifiedName().GetText()

		// 解析数据库和表名
		cluster, database, table := l.parseTableName(tableName)

		// 添加写表信息
		l.addWriteTable(cluster, database, table)
	}
}

// EnterCreateViewStatement 进入创建视图语句时调用
func (l *lineageListener) EnterCreateViewStatement(ctx *parser.CreateViewStatementContext) {
	l.curOpType = analyzer.StmtTypeCreateView
	l.onWriteStmt()
}

// EnterCreateViewStatement 进入创建视图语句时调用
func (l *ddlListener) EnterCreateViewStatement(ctx *parser.CreateViewStatementContext) {
	l.curOpType = analyzer.StmtTypeCreateView
	l.onWriteStmt()

	// 提取视图名
	if ctx.QualifiedName() != nil {
		viewName := ctx.QualifiedName().GetText()

		// 解析数据库和视图名
		cluster, database, view := l.parseTableName(viewName)

		// 添加CREATE VIEW action
		action := &analyzer.ActionInfo{
			ClusterName:  cluster,
			DatabaseName: database,
			TableName:    view,
			ActionType:   analyzer.ActionTypeCreate,
		}
		l.result.Action = action
	}
}

// EnterAlterTableStatement 进入修改表语句时调用
func (l *ddlListener) EnterAlterTableStatement(ctx *parser.AlterTableStatementContext) {
	// 检查是否是SWAP WITH操作
	isSwapWith := false
	// 检查是否是RENAME操作
	isRename := false
	var newTableName string

	for _, alterClause := range ctx.AllAlterClause() {
		if alterClause.SwapTableClause() != nil {
			isSwapWith = true
			break
		} else if alterClause.TableRenameClause() != nil {
			isRename = true
			if alterClause.TableRenameClause().Identifier() != nil {
				newTableName = alterClause.TableRenameClause().Identifier().GetText()
			}
			break
		}
	}

	// 设置语句类型
	if isSwapWith {
		l.curOpType = analyzer.StmtTypeSwapTable
	} else if isRename {
		l.curOpType = analyzer.StmtTypeRenameTable
	} else {
		l.curOpType = analyzer.StmtTypeAlterTable
	}

	l.onWriteStmt()

	// 提取表名
	if ctx.QualifiedName() != nil {
		tableName := ctx.QualifiedName().GetText()

		// 解析数据库和表名
		cluster, database, table := l.parseTableName(tableName)
		parts := strings.Split(tableName, ".")
		isSpecifiedCluster := len(parts) >= 3
		isSpecifiedDatabase := len(parts) >= 2

		// 处理RENAME操作
		if isRename && newTableName != "" {
			// 解析新表名
			newCluster, newDatabase, newTable := l.parseTableName(newTableName)
			newParts := strings.Split(newTableName, ".")
			newIsSpecifiedCluster := len(newParts) >= 3
			newIsSpecifiedDatabase := len(newParts) >= 2

			// 为旧表创建DROP action
			action := &analyzer.ActionInfo{
				ClusterName:         cluster,
				IsSpecifiedCluster:  isSpecifiedCluster,
				DatabaseName:        database,
				IsSpecifiedDatabase: isSpecifiedDatabase,
				TableName:           table,
				ActionType:          analyzer.ActionTypeDrop,
			}
			l.result.Action = action

			// 为新表创建CREATE action
			anotherAction := &analyzer.ActionInfo{
				ClusterName:         newCluster,
				IsSpecifiedCluster:  newIsSpecifiedCluster,
				DatabaseName:        newDatabase,
				IsSpecifiedDatabase: newIsSpecifiedDatabase,
				TableName:           newTable,
				ActionType:          analyzer.ActionTypeCreate,
			}
			l.result.AnotherAction = anotherAction
		} else if isSwapWith {
			// 处理SWAP WITH操作
			// 提取第二个表名
			var secondTableName string
			for _, alterClause := range ctx.AllAlterClause() {
				if swapTableClause := alterClause.SwapTableClause(); swapTableClause != nil {
					if swapTableClause.Identifier() != nil {
						secondTableName = swapTableClause.Identifier().GetText()
					}
					break
				}
			}

			// 为第一个表创建Action
			action := &analyzer.ActionInfo{
				ClusterName:         cluster,
				IsSpecifiedCluster:  isSpecifiedCluster,
				DatabaseName:        database,
				IsSpecifiedDatabase: isSpecifiedDatabase,
				TableName:           table,
				ActionType:          analyzer.ActionTypeAlter,
			}
			l.result.Action = action

			// 为第二个表创建AnotherAction
			if secondTableName != "" {
				secondCluster, secondDatabase, secondTable := l.parseTableName(secondTableName)
				secondParts := strings.Split(secondTableName, ".")
				secondIsSpecifiedCluster := len(secondParts) >= 3
				secondIsSpecifiedDatabase := len(secondParts) >= 2

				anotherAction := &analyzer.ActionInfo{
					ClusterName:         secondCluster,
					IsSpecifiedCluster:  secondIsSpecifiedCluster,
					DatabaseName:        secondDatabase,
					IsSpecifiedDatabase: secondIsSpecifiedDatabase,
					TableName:           secondTable,
					ActionType:          analyzer.ActionTypeAlter,
				}
				l.result.AnotherAction = anotherAction
			}
		} else {
			// 提取列信息
			columns := []*analyzer.ActionColumn{}

			// 遍历所有alterClause
			for _, alterClause := range ctx.AllAlterClause() {
				// 处理ADD COLUMN语句
				if addColumnClause := alterClause.AddColumnClause(); addColumnClause != nil {
					if columnDesc := addColumnClause.ColumnDesc(); columnDesc != nil {
						columnName := ""
						columnType := ""
						comment := ""

						// 提取列名
						if id := columnDesc.Identifier(); id != nil {
							columnName = util.TrimQuotes(id.GetText())
						}

						// 提取列类型
						if t := columnDesc.Type_(); t != nil {
							columnType = t.GetText()
						}

						// 提取列注释
						if commentCtx := columnDesc.Comment(); commentCtx != nil {
							commentText := commentCtx.String_().GetText()
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

				// 处理ADD COLUMNS语句（多个列）
				if addColumnsClause := alterClause.AddColumnsClause(); addColumnsClause != nil {
					for _, columnDesc := range addColumnsClause.AllColumnDesc() {
						columnName := ""
						columnType := ""
						comment := ""

						// 提取列名
						if id := columnDesc.Identifier(); id != nil {
							columnName = util.TrimQuotes(id.GetText())
						}

						// 提取列类型
						if t := columnDesc.Type_(); t != nil {
							columnType = t.GetText()
						}

						// 提取列注释
						if commentCtx := columnDesc.Comment(); commentCtx != nil {
							commentText := commentCtx.String_().GetText()
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

				// 处理DROP COLUMN语句
				if dropColumnClause := alterClause.DropColumnClause(); dropColumnClause != nil {
					if id := dropColumnClause.Identifier(0); id != nil {
						columnName := util.TrimQuotes(id.GetText())
						columns = append(columns, &analyzer.ActionColumn{
							Name:   columnName,
							Action: analyzer.ActionTypeDrop,
						})
					}
				}

				// 处理MODIFY COLUMN语句
				if modifyColumnClause := alterClause.ModifyColumnClause(); modifyColumnClause != nil {
					if columnDesc := modifyColumnClause.ColumnDesc(); columnDesc != nil {
						columnName := ""
						columnType := ""

						// 提取列名
						if id := columnDesc.Identifier(); id != nil {
							columnName = util.TrimQuotes(id.GetText())
						}

						// 提取列类型
						if t := columnDesc.Type_(); t != nil {
							columnType = t.GetText()
						}

						if columnName != "" {
							columns = append(columns, &analyzer.ActionColumn{
								Name:   columnName,
								Type:   columnType,
								Action: analyzer.ActionTypeAlter,
							})
						}
					}
				}

				// 处理RENAME COLUMN语句
				if columnRenameClause := alterClause.ColumnRenameClause(); columnRenameClause != nil {
					if oldId := columnRenameClause.Identifier(0); oldId != nil {
						oldColumnName := util.TrimQuotes(oldId.GetText())
						columns = append(columns, &analyzer.ActionColumn{
							Name:   oldColumnName,
							Action: analyzer.ActionTypeDrop,
						})
					}
					if newId := columnRenameClause.Identifier(1); newId != nil {
						newColumnName := util.TrimQuotes(newId.GetText())
						columns = append(columns, &analyzer.ActionColumn{
							Name:   newColumnName,
							Action: analyzer.ActionTypeCreate,
						})
					}
				}
			}

			// 添加action
			action := &analyzer.ActionInfo{
				ClusterName:         cluster,
				IsSpecifiedCluster:  isSpecifiedCluster,
				DatabaseName:        database,
				IsSpecifiedDatabase: isSpecifiedDatabase,
				TableName:           table,
				Columns:             columns,
				ActionType:          analyzer.ActionTypeAlter,
			}
			l.result.Action = action
		}
	}
}

// EnterAlterTableStatement 进入修改表语句时调用
func (l *lineageListener) EnterAlterTableStatement(ctx *parser.AlterTableStatementContext) {
	// 检查是否是SWAP WITH操作
	isSwapWith := false
	for _, alterClause := range ctx.AllAlterClause() {
		if alterClause.SwapTableClause() != nil {
			isSwapWith = true
			break
		}
	}

	// 设置语句类型
	if isSwapWith {
		l.curOpType = analyzer.StmtTypeSwapTable
	} else {
		l.curOpType = analyzer.StmtTypeAlterTable
	}

	l.onWriteStmt()

	// 提取表名
	if ctx.QualifiedName() != nil {
		tableName := ctx.QualifiedName().GetText()

		// 解析数据库和表名
		cluster, database, table := l.parseTableName(tableName)

		// 添加写表信息
		l.addWriteTable(cluster, database, table)
	}
}

// EnterDropTableStatement 进入删除表语句时调用
func (l *ddlListener) EnterDropTableStatement(ctx *parser.DropTableStatementContext) {
	l.curOpType = analyzer.StmtTypeDropTable
	l.onWriteStmt()

	// 提取表名
	if ctx.QualifiedName() != nil {
		tableName := ctx.QualifiedName().GetText()

		// 解析数据库和表名
		cluster, database, table := l.parseTableName(tableName)
		parts := strings.Split(tableName, ".")
		isSpecifiedCluster := len(parts) >= 3
		isSpecifiedDatabase := len(parts) >= 2

		// 添加DROP TABLE action
		action := &analyzer.ActionInfo{
			ClusterName:         cluster,
			IsSpecifiedCluster:  isSpecifiedCluster,
			DatabaseName:        database,
			IsSpecifiedDatabase: isSpecifiedDatabase,
			TableName:           table,
			ActionType:          analyzer.ActionTypeDrop,
		}
		l.result.Action = action
	}
}

// EnterDropTableStatement 进入删除表语句时调用
func (l *lineageListener) EnterDropTableStatement(ctx *parser.DropTableStatementContext) {
	l.curOpType = analyzer.StmtTypeDropTable
	l.onWriteStmt()

	// 提取表名
	if ctx.QualifiedName() != nil {
		tableName := ctx.QualifiedName().GetText()

		// 解析数据库和表名
		cluster, database, table := l.parseTableName(tableName)

		// 添加写表信息
		l.addWriteTable(cluster, database, table)
	}
}

// EnterDropViewStatement 进入删除视图语句时调用
func (l *ddlListener) EnterDropViewStatement(ctx *parser.DropViewStatementContext) {
	l.curOpType = analyzer.StmtTypeDropView
	l.onWriteStmt()

	// 提取视图名
	if ctx.QualifiedName() != nil {
		viewName := ctx.QualifiedName().GetText()

		// 解析数据库和视图名
		cluster, database, view := l.parseTableName(viewName)

		// 添加DROP VIEW action
		action := &analyzer.ActionInfo{
			ClusterName:  cluster,
			DatabaseName: database,
			TableName:    view,
			ActionType:   analyzer.ActionTypeDrop,
		}
		l.result.Action = action
	}
}

// EnterDropViewStatement 进入删除视图语句时调用
func (l *lineageListener) EnterDropViewStatement(ctx *parser.DropViewStatementContext) {
	l.curOpType = analyzer.StmtTypeDropView
	l.onWriteStmt()

	// 提取视图名
	if ctx.QualifiedName() != nil {
		viewName := ctx.QualifiedName().GetText()

		// 解析数据库和视图名
		cluster, database, view := l.parseTableName(viewName)

		// 添加写表信息
		l.addWriteTable(cluster, database, view)
	}
}

// EnterDropMaterializedViewStatement 进入删除物化视图语句时调用
func (l *ddlListener) EnterDropMaterializedViewStatement(ctx *parser.DropMaterializedViewStatementContext) {
	l.curOpType = analyzer.StmtTypeDropMaterializedView
	l.onWriteStmt()

	// 提取物化视图名
	if ctx.QualifiedName() != nil {
		mvName := ctx.QualifiedName().GetText()

		// 解析数据库和物化视图名
		cluster, database, mv := l.parseTableName(mvName)

		// 添加DROP MATERIALIZED VIEW action
		action := &analyzer.ActionInfo{
			ClusterName:  cluster,
			DatabaseName: database,
			TableName:    mv,
			ActionType:   analyzer.ActionTypeDrop,
		}
		l.result.Action = action
	}
}

// EnterDropMaterializedViewStatement 进入删除物化视图语句时调用
func (l *lineageListener) EnterDropMaterializedViewStatement(ctx *parser.DropMaterializedViewStatementContext) {
	l.curOpType = analyzer.StmtTypeDropMaterializedView
	l.onWriteStmt()

	// 提取物化视图名
	if ctx.QualifiedName() != nil {
		mvName := ctx.QualifiedName().GetText()

		// 解析数据库和物化视图名
		cluster, database, mv := l.parseTableName(mvName)

		// 添加写表信息
		l.addWriteTable(cluster, database, mv)
	}
}

// EnterAlterViewStatement 进入修改视图语句时调用
func (l *ddlListener) EnterAlterViewStatement(ctx *parser.AlterViewStatementContext) {
	l.curOpType = analyzer.StmtTypeAlterView
	l.onWriteStmt()

	// 提取视图名
	if ctx.QualifiedName() != nil {
		viewName := ctx.QualifiedName().GetText()

		// 解析数据库和视图名
		cluster, database, view := l.parseTableName(viewName)

		// 添加ALTER VIEW action
		action := &analyzer.ActionInfo{
			ClusterName:  cluster,
			DatabaseName: database,
			TableName:    view,
			ActionType:   analyzer.ActionTypeAlter,
		}
		l.result.Action = action
	}
}

// EnterAlterViewStatement 进入修改视图语句时调用
func (l *lineageListener) EnterAlterViewStatement(ctx *parser.AlterViewStatementContext) {
	l.curOpType = analyzer.StmtTypeAlterView
	l.onWriteStmt()

	// 提取视图名
	if ctx.QualifiedName() != nil {
		viewName := ctx.QualifiedName().GetText()

		// 解析数据库和视图名
		cluster, database, view := l.parseTableName(viewName)

		// 添加写表信息
		l.addWriteTable(cluster, database, view)
	}
}

// EnterCreateDbStatement 进入创建数据库语句时调用
func (l *ddlListener) EnterCreateDbStatement(ctx *parser.CreateDbStatementContext) {
	l.curOpType = analyzer.StmtTypeCreateDatabase
	l.onWriteStmt()

	// 提取数据库名
	if ctx.QualifiedName() != nil {
		dbName := ctx.QualifiedName().GetText()

		// 解析数据库名，支持 cluster.db 格式
		parts := strings.Split(dbName, ".")
		var cluster, database string
		var isSpecifiedCluster bool

		switch len(parts) {
		case 1:
			// 只有数据库名
			database = util.TrimQuotes(parts[0])
			isSpecifiedCluster = false
		case 2:
			// 集群名和数据库名
			cluster = util.TrimQuotes(parts[0])
			database = util.TrimQuotes(parts[1])
			isSpecifiedCluster = true
		}

		// 使用默认值
		if cluster == "" {
			cluster = l.defaultCluster
		}

		// 添加CREATE DATABASE action
		action := &analyzer.ActionInfo{
			ClusterName:         cluster,
			IsSpecifiedCluster:  isSpecifiedCluster,
			DatabaseName:        database,
			IsSpecifiedDatabase: true,
			TableName:           "",
			ActionType:          analyzer.ActionTypeCreate,
		}
		l.result.Action = action
	}
}

// EnterCreateDbStatement 进入创建数据库语句时调用
func (l *lineageListener) EnterCreateDbStatement(ctx *parser.CreateDbStatementContext) {
	l.curOpType = analyzer.StmtTypeCreateDatabase
	l.onWriteStmt()
}

// EnterDropDbStatement 进入删除数据库语句时调用
func (l *ddlListener) EnterDropDbStatement(ctx *parser.DropDbStatementContext) {
	l.curOpType = analyzer.StmtTypeDropDatabase
	l.onWriteStmt()

	// 提取数据库名
	if ctx.QualifiedName() != nil {
		dbName := util.TrimQuotes(ctx.QualifiedName().GetText())

		// 添加DROP DATABASE action
		action := &analyzer.ActionInfo{
			ClusterName:  l.defaultCluster,
			DatabaseName: dbName,
			TableName:    "",
			ActionType:   analyzer.ActionTypeDrop,
		}
		l.result.Action = action
	}
}

// EnterDropDbStatement 进入删除数据库语句时调用
func (l *lineageListener) EnterDropDbStatement(ctx *parser.DropDbStatementContext) {
	l.curOpType = analyzer.StmtTypeDropDatabase
	l.onWriteStmt()
}

// EnterAlterDatabaseRenameStatement 进入修改数据库语句时调用
func (l *ddlListener) EnterAlterDatabaseRenameStatement(ctx *parser.AlterDatabaseRenameStatementContext) {
	l.curOpType = analyzer.StmtTypeAlterDatabase
	l.onWriteStmt()

	// 提取数据库名
	if ctx.Identifier(0) != nil {
		dbName := util.TrimQuotes(ctx.Identifier(0).GetText())

		// 添加ALTER DATABASE action
		action := &analyzer.ActionInfo{
			ClusterName:  l.defaultCluster,
			DatabaseName: dbName,
			TableName:    "",
			ActionType:   analyzer.ActionTypeAlter,
		}
		l.result.Action = action
	}
}

// EnterAlterDatabaseRenameStatement 进入修改数据库语句时调用
func (l *lineageListener) EnterAlterDatabaseRenameStatement(ctx *parser.AlterDatabaseRenameStatementContext) {
	l.curOpType = analyzer.StmtTypeAlterDatabase
	l.onWriteStmt()
}

// EnterInsertStatement 进入插入语句时调用
func (l *lineageListener) EnterInsertStatement(ctx *parser.InsertStatementContext) {
	l.curOpType = analyzer.StmtTypeInsert
	l.onWriteStmt()
}

// EnterInsertStatement 进入插入语句时调用
func (l *ddlListener) EnterInsertStatement(ctx *parser.InsertStatementContext) {
	l.curOpType = analyzer.StmtTypeInsert
	l.onWriteStmt()
}

// EnterUpdateStatement 进入更新语句时调用
func (l *lineageListener) EnterUpdateStatement(ctx *parser.UpdateStatementContext) {
	l.curOpType = analyzer.StmtTypeUpdate
	l.onWriteStmt()
}

// EnterUpdateStatement 进入更新语句时调用
func (l *ddlListener) EnterUpdateStatement(ctx *parser.UpdateStatementContext) {
	l.curOpType = analyzer.StmtTypeUpdate
	l.onWriteStmt()
}

// EnterDeleteStatement 进入删除语句时调用
func (l *lineageListener) EnterDeleteStatement(ctx *parser.DeleteStatementContext) {
	l.curOpType = analyzer.StmtTypeDelete
	l.onWriteStmt()
}

// EnterDeleteStatement 进入删除语句时调用
func (l *ddlListener) EnterDeleteStatement(ctx *parser.DeleteStatementContext) {
	l.curOpType = analyzer.StmtTypeDelete
	l.onWriteStmt()
}

// EnterQueryStatement 进入查询语句时调用
func (l *lineageListener) EnterQueryStatement(ctx *parser.QueryStatementContext) {
	l.curOpType = analyzer.StmtTypeSelect
	l.isOnlyComment = false
	if !l.isWriteOp && l.firstOpType == "" {
		l.firstOpType = analyzer.StmtTypeSelect
	}
}

// EnterQueryStatement 进入查询语句时调用
func (l *ddlListener) EnterQueryStatement(ctx *parser.QueryStatementContext) {
	l.curOpType = analyzer.StmtTypeSelect
	l.isOnlyComment = false
	if !l.isWriteOp && l.firstOpType == "" {
		l.firstOpType = analyzer.StmtTypeSelect
	}
}

// EnterUseDatabaseStatement 进入USE DATABASE语句时调用
func (l *lineageListener) EnterUseDatabaseStatement(ctx *parser.UseDatabaseStatementContext) {
	l.curOpType = analyzer.StmtTypeUseDatabase
	l.isOnlyComment = false
	// 设置为firstOpType
	l.firstOpType = analyzer.StmtTypeUseDatabase
	// 提取数据库名并添加到Reads
	if ctx.QualifiedName() != nil {
		dbName := ctx.QualifiedName().GetText()
		// 解析数据库名，支持 cluster.db 格式
		parts := strings.Split(dbName, ".")
		var cluster, database string

		switch len(parts) {
		case 1:
			// 只有数据库名
			database = util.TrimQuotes(parts[0])
		case 2:
			// 集群名和数据库名
			cluster = util.TrimQuotes(parts[0])
			database = util.TrimQuotes(parts[1])
		}

		// 使用默认值
		if cluster == "" {
			cluster = l.defaultCluster
		}

		// 添加到Reads
		l.addReadTable(cluster, database, "")
	}
}

// EnterUseDatabaseStatement 进入USE DATABASE语句时调用
func (l *ddlListener) EnterUseDatabaseStatement(ctx *parser.UseDatabaseStatementContext) {
	l.curOpType = analyzer.StmtTypeUseDatabase
	l.isOnlyComment = false
	// 设置为firstOpType，但不进行读写表操作
	l.firstOpType = analyzer.StmtTypeUseDatabase
}

// EnterUseCatalogStatement 进入USE CATALOG语句时调用
func (l *lineageListener) EnterUseCatalogStatement(ctx *parser.UseCatalogStatementContext) {
	l.curOpType = analyzer.StmtTypeUseCatalog
	l.isOnlyComment = false
	// 设置为firstOpType，但不进行读写表操作
	l.firstOpType = analyzer.StmtTypeUseCatalog
}

// EnterUseCatalogStatement 进入USE CATALOG语句时调用
func (l *ddlListener) EnterUseCatalogStatement(ctx *parser.UseCatalogStatementContext) {
	l.curOpType = analyzer.StmtTypeUseCatalog
	l.isOnlyComment = false
	// 设置为firstOpType，但不进行读写表操作
	l.firstOpType = analyzer.StmtTypeUseCatalog
}

// EnterAlterMaterializedViewStatement 进入修改物化视图语句时调用
func (l *ddlListener) EnterAlterMaterializedViewStatement(ctx *parser.AlterMaterializedViewStatementContext) {
	l.curOpType = analyzer.StmtTypeAlterMaterializedView
	l.onWriteStmt()

	// 提取表名
	if ctx.QualifiedName() != nil {
		tableName := ctx.QualifiedName().GetText()

		// 解析数据库和表名
		cluster, database, table := l.parseTableName(tableName)

		// 添加ALTER MATERIALIZED VIEW action
		action := &analyzer.ActionInfo{
			ClusterName:  cluster,
			DatabaseName: database,
			TableName:    table,
			ActionType:   analyzer.ActionTypeAlter,
		}
		l.result.Action = action
	}
}

// EnterCreateMaterializedViewStatement 进入创建物化视图语句时调用
func (l *ddlListener) EnterCreateMaterializedViewStatement(ctx *parser.CreateMaterializedViewStatementContext) {
	l.curOpType = analyzer.StmtTypeCreateMaterializedView
	l.onWriteStmt()

	// 提取表名
	if ctx.QualifiedName() != nil {
		tableName := ctx.QualifiedName().GetText()

		// 解析数据库和表名
		cluster, database, table := l.parseTableName(tableName)

		// 添加CREATE MATERIALIZED VIEW action
		action := &analyzer.ActionInfo{
			ClusterName:  cluster,
			DatabaseName: database,
			TableName:    table,
			ActionType:   analyzer.ActionTypeCreate,
		}
		l.result.Action = action
	}
}

// EnterAlterMaterializedViewStatement 进入修改物化视图语句时调用
func (l *lineageListener) EnterAlterMaterializedViewStatement(ctx *parser.AlterMaterializedViewStatementContext) {
	l.curOpType = analyzer.StmtTypeAlterMaterializedView
	l.onWriteStmt()

	// 提取表名
	if ctx.QualifiedName() != nil {
		tableName := ctx.QualifiedName().GetText()

		// 解析数据库和表名
		cluster, database, table := l.parseTableName(tableName)

		// 添加写表信息
		l.addWriteTable(cluster, database, table)
	}
}

// EnterCreateMaterializedViewStatement 进入创建物化视图语句时调用
func (l *lineageListener) EnterCreateMaterializedViewStatement(ctx *parser.CreateMaterializedViewStatementContext) {
	l.curOpType = analyzer.StmtTypeCreateMaterializedView
	l.onWriteStmt()

	// 提取表名
	if ctx.QualifiedName() != nil {
		tableName := ctx.QualifiedName().GetText()

		// 解析数据库和表名
		cluster, database, table := l.parseTableName(tableName)

		// 添加写表信息
		l.addWriteTable(cluster, database, table)
	}
}

// EnterQualifiedName 进入表名节点时调用
func (l *lineageListener) EnterQualifiedName(ctx *parser.QualifiedNameContext) {
	if ctx != nil {
		// USE语句不进行读写表操作
		if l.curOpType == analyzer.StmtTypeUseDatabase || l.curOpType == analyzer.StmtTypeUseCatalog {
			return
		}
		// CREATE TABLE、CREATE VIEW等语句已经在各自的Enter方法中处理了表名，这里跳过
		if l.curOpType == analyzer.StmtTypeCreateTable || l.curOpType == analyzer.StmtTypeCreateTemporaryTable || l.curOpType == analyzer.StmtTypeCreateView || l.curOpType == analyzer.StmtTypeCreateMaterializedView {
			return
		}
		// 直接获取表名文本
		tableName := ctx.GetText()
		// 检查是否是CTE名称
		if _, isCTE := l.cteNames[strings.ReplaceAll(tableName, "`", "")]; isCTE {
			return
		}

		// 解析表名，支持 cluster.db.table 格式
		cluster, database, table := l.parseTableName(tableName)

		// 跳过函数名：检查当前qualifiedName是否是函数调用的一部分
		// 如果父节点是函数调用，那么当前qualifiedName是函数名，不是表名
		parent := ctx.GetParent()
		if parent != nil {
			// 检查父节点是否是函数调用
			if _, ok := parent.(*parser.FunctionCallContext); ok {
				return
			}
			// 检查父节点是否是简单函数调用
			if _, ok := parent.(*parser.SimpleFunctionCallContext); ok {
				return
			}
			// 检查父节点是否是聚合函数调用
			if _, ok := parent.(*parser.AggregationFunctionCallContext); ok {
				return
			}
			// 检查父节点是否是窗口函数调用
			if _, ok := parent.(*parser.WindowFunctionCallContext); ok {
				return
			}
			// 检查父节点是否是翻译函数调用
			if _, ok := parent.(*parser.TranslateFunctionCallContext); ok {
				return
			}
		}

		// 根据当前操作类型决定是读表还是写表
		if l.isWriteOperation() {
			l.addWriteTable(cluster, database, table)
		} else {
			l.addReadTable(cluster, database, table)
		}
	}
}

// EnterTableName 进入表名节点时调用
func (l *lineageListener) EnterTableName(ctx *parser.TableNameContext) {
	if ctx != nil {
		// USE语句不进行读写表操作
		if l.curOpType == analyzer.StmtTypeUseDatabase || l.curOpType == analyzer.StmtTypeUseCatalog {
			return
		}
		// CREATE TABLE、CREATE VIEW等语句已经在各自的Enter方法中处理了表名，这里跳过
		if l.curOpType == analyzer.StmtTypeCreateTable || l.curOpType == analyzer.StmtTypeCreateTemporaryTable || l.curOpType == analyzer.StmtTypeCreateView || l.curOpType == analyzer.StmtTypeCreateMaterializedView {
			return
		}
		// 直接获取表名文本
		tableName := ctx.GetText()
		// 检查是否是CTE名称
		if _, isCTE := l.cteNames[strings.ReplaceAll(tableName, "`", "")]; isCTE {
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

// EnterWithClause 进入WITH子句时调用，处理CTE
func (l *lineageListener) EnterWithClause(ctx *parser.WithClauseContext) {
	if ctx != nil {
		for _, cte := range ctx.AllCommonTableExpression() {
			if name := cte.GetName(); name != nil {
				cteName := name.GetText()
				l.cteNames[strings.ReplaceAll(cteName, "`", "")] = true
			}
		}
	}
}

// isWriteOperation 检查当前操作是否是写操作
func (l *lineageListener) isWriteOperation() bool {
	// USE语句不进行读写表操作
	if l.curOpType == analyzer.StmtTypeUseDatabase || l.curOpType == analyzer.StmtTypeUseCatalog {
		return false
	}
	return l.curOpType == analyzer.StmtTypeCreateTable ||
		l.curOpType == analyzer.StmtTypeCreateTemporaryTable ||
		l.curOpType == analyzer.StmtTypeCreateView ||
		l.curOpType == analyzer.StmtTypeAlterTable ||
		l.curOpType == analyzer.StmtTypeDropTable ||
		l.curOpType == analyzer.StmtTypeDropView ||
		l.curOpType == analyzer.StmtTypeInsert ||
		l.curOpType == analyzer.StmtTypeUpdate ||
		l.curOpType == analyzer.StmtTypeDelete ||
		l.curOpType == analyzer.StmtTypeCreateMaterializedView ||
		l.curOpType == analyzer.StmtTypeAlterMaterializedView ||
		l.curOpType == analyzer.StmtTypeDropMaterializedView ||
		l.curOpType == analyzer.StmtTypeAlterView ||
		l.curOpType == analyzer.StmtTypeCreateDatabase ||
		l.curOpType == analyzer.StmtTypeDropDatabase ||
		l.curOpType == analyzer.StmtTypeAlterDatabase
}

// parseTableName 解析表名，支持 cluster.db.table 格式
func (l *lineageListener) parseTableName(tableName string) (cluster, database, table string) {
	parts := strings.Split(tableName, ".")

	switch len(parts) {
	case 1:
		// 只有表名
		table = util.TrimQuotes(parts[0])
	case 2:
		// 数据库名和表名
		database = util.TrimQuotes(parts[0])
		table = util.TrimQuotes(parts[1])
	case 3:
		// 集群名、数据库名和表名
		cluster = util.TrimQuotes(parts[0])
		database = util.TrimQuotes(parts[1])
		table = util.TrimQuotes(parts[2])
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

// parseTableName 解析表名，支持 cluster.db.table 格式
func (l *ddlListener) parseTableName(tableName string) (cluster, database, table string) {
	parts := strings.Split(tableName, ".")

	switch len(parts) {
	case 1:
		// 只有表名
		table = util.TrimQuotes(parts[0])
	case 2:
		// 数据库名和表名
		database = util.TrimQuotes(parts[0])
		table = util.TrimQuotes(parts[1])
	case 3:
		// 集群名、数据库名和表名
		cluster = util.TrimQuotes(parts[0])
		database = util.TrimQuotes(parts[1])
		table = util.TrimQuotes(parts[2])
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
func (l *lineageListener) addReadTable(cluster, database, table string) {
	l.result.Reads = append(l.result.Reads, &analyzer.Dependency{
		Cluster:  cluster,
		Database: database,
		Table:    table,
	})
}

// addWriteTable 添加写表信息
func (l *lineageListener) addWriteTable(cluster, database, table string) {
	l.result.Writes = append(l.result.Writes, &analyzer.Dependency{
		Cluster:  cluster,
		Database: database,
		Table:    table,
	})
}

// onWriteStmt 处理写操作语句
func (l *lineageListener) onWriteStmt() {
	l.isWriteOp = true
	if l.firstOpType == "" {
		l.firstOpType = l.curOpType
	}
}

// onWriteStmt 处理写操作语句
func (l *ddlListener) onWriteStmt() {
	l.isWriteOp = true
	if l.firstOpType == "" {
		l.firstOpType = l.curOpType
	}
}

// EnterComment 进入注释语句时调用
func (l *lineageListener) EnterComment(ctx *parser.CommentContext) {
	// 记录注释内容
	commentText := util.TrimQuotes(ctx.GetText())
	l.comments = append(l.comments, commentText)
}

// EnterComment 进入注释语句时调用
func (l *ddlListener) EnterComment(ctx *parser.CommentContext) {
	// 记录注释内容
	commentText := util.TrimQuotes(ctx.GetText())
	l.comments = append(l.comments, commentText)
}
