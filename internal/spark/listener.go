package spark

import (
	"strings"

	"github.com/Edsuns/sql-parser/analyzer"
	"github.com/Edsuns/sql-parser/internal/spark/parser"
	"github.com/Edsuns/sql-parser/internal/util"
)

// lineageListener 自定义监听器，用于提取SQL语句中的血缘关系信息
type lineageListener struct {
	*parser.BaseSqlBaseParserListener

	result          *analyzer.LineageResult
	defaultCluster  string
	defaultDatabase string
	curOpType       analyzer.StmtType // 当前操作类型：SELECT, INSERT, UPDATE, DELETE, MERGE, CREATE_TABLE, CREATE_VIEW, ALTER_TABLE, REPLACE_TABLE, DROP_TABLE, CREATE_LIKE
	firstOpType     analyzer.StmtType // 第一个操作类型，根据规则：第一个写入表的OpType，若没有写入则取第一个读取表的OpType
	comments        []string          // 存储解析到的注释
	isOnlyComment   bool              // 标记当前SQL是否只包含注释
	isWriteOp       bool              // 是否已遇到写入操作
	cteNames        map[string]bool   // 存储CTE名称，避免将CTE作为表依赖
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
	*parser.BaseSqlBaseParserListener

	result          *analyzer.DDLResult
	defaultCluster  string
	defaultDatabase string
	curOpType       analyzer.StmtType    // 当前操作类型：SELECT, INSERT, UPDATE, DELETE, MERGE, CREATE_TABLE, CREATE_VIEW, ALTER_TABLE, REPLACE_TABLE, DROP_TABLE, CREATE_LIKE
	firstOpType     analyzer.StmtType    // 第一个操作类型，根据规则：第一个写入表的OpType，若没有写入则取第一个读取表的OpType
	comments        []string             // 存储解析到的注释
	isOnlyComment   bool                 // 标记当前SQL是否只包含注释
	isWriteOp       bool                 // 是否已遇到写入操作
	currentTable    *analyzer.ActionInfo // 当前正在处理的表
	techInfo        *analyzer.TableInfo  // 技术信息
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
		techInfo:        nil,
	}
}

// EnterCtes 进入CTE列表时调用
func (l *lineageListener) EnterCtes(ctx *parser.CtesContext) {
	l.onReadStmt()
}

// EnterNamedQuery 进入单个CTE查询时调用
func (l *lineageListener) EnterNamedQuery(ctx *parser.NamedQueryContext) {
	l.onReadStmt()
	// 记录CTE名称，避免将其作为表依赖
	if ctx.GetName() != nil {
		cteName := ctx.GetName().GetText()
		l.cteNames[strings.ReplaceAll(cteName, "`", "")] = true
	}
}

// EnterRegularQuerySpecification 进入常规查询规范时调用，这是SELECT语句的主要部分
func (l *lineageListener) EnterRegularQuerySpecification(ctx *parser.RegularQuerySpecificationContext) {
	l.curOpType = analyzer.StmtTypeSelect
	l.isOnlyComment = false
	// 如果还没有遇到写入操作，设置第一个操作类型
	if !l.isWriteOp && l.firstOpType == "" {
		l.firstOpType = analyzer.StmtTypeSelect
	}
}

// EnterSubquery 进入子查询时调用
func (l *lineageListener) EnterSubquery(ctx *parser.SubqueryContext) {
	l.onReadStmt()
}

// EnterAliasedRelation 进入别名关系表达式时调用，这是表引用的常见上下文
func (l *lineageListener) EnterAliasedRelation(ctx *parser.AliasedRelationContext) {
	l.onReadStmt()
}

// EnterQuery 进入查询语句时调用（SELECT），通常是读操作
func (l *lineageListener) EnterQuery(ctx *parser.QueryContext) {
	l.onReadStmt()
}

// EnterStatement 进入语句时调用
func (l *lineageListener) EnterStatement(ctx *parser.SingleStatementContext) {
	l.isOnlyComment = false
}

// EnterSingleStatement 进入单条语句时调用
func (l *lineageListener) EnterSingleStatement(ctx *parser.SingleStatementContext) {
	l.isOnlyComment = false
}

// EnterCompoundStatement 进入复合语句时调用
func (l *lineageListener) EnterCompoundStatement(ctx *parser.CompoundStatementContext) {
	l.isOnlyComment = false
}

// EnterStatement 进入语句时调用
func (l *ddlListener) EnterStatement(ctx *parser.SingleStatementContext) {
	l.isOnlyComment = false
}

// EnterSingleStatement 进入单条语句时调用
func (l *ddlListener) EnterSingleStatement(ctx *parser.SingleStatementContext) {
	l.isOnlyComment = false
}

// EnterCompoundStatement 进入复合语句时调用
func (l *ddlListener) EnterCompoundStatement(ctx *parser.CompoundStatementContext) {
	l.isOnlyComment = false
}

// EnterSingleInsertQuery 进入单条插入语句时调用
func (l *lineageListener) EnterSingleInsertQuery(ctx *parser.SingleInsertQueryContext) {
	l.curOpType = analyzer.StmtTypeInsert
	l.onWriteStmt()
}

// EnterSingleInsertQuery 进入单条插入语句时调用
func (l *ddlListener) EnterSingleInsertQuery(ctx *parser.SingleInsertQueryContext) {
	l.curOpType = analyzer.StmtTypeInsert
	l.onWriteStmt()
}

// EnterMultiInsertQuery 进入多条插入语句时调用
func (l *lineageListener) EnterMultiInsertQuery(ctx *parser.MultiInsertQueryContext) {
	l.curOpType = analyzer.StmtTypeInsert
	l.onWriteStmt()
}

// EnterMultiInsertQuery 进入多条插入语句时调用
func (l *ddlListener) EnterMultiInsertQuery(ctx *parser.MultiInsertQueryContext) {
	l.curOpType = analyzer.StmtTypeInsert
	l.onWriteStmt()
}

// EnterInsertInto 进入INSERT INTO语句时调用
func (l *lineageListener) EnterInsertInto(ctx *parser.InsertIntoContext) {
	l.isOnlyComment = false
	l.curOpType = analyzer.StmtTypeInsert
	l.onWriteStmt()
}

// EnterInsertOverwriteTable 进入INSERT OVERWRITE语句时调用
func (l *lineageListener) EnterInsertOverwriteTable(ctx *parser.InsertOverwriteTableContext) {
	l.isOnlyComment = false
	l.curOpType = analyzer.StmtTypeInsert
	l.onWriteStmt()
}

// EnterInsertInto 进入INSERT INTO语句时调用
func (l *ddlListener) EnterInsertInto(ctx *parser.InsertIntoContext) {
	l.curOpType = analyzer.StmtTypeInsert
	l.onWriteStmt()
}

// EnterInsertOverwriteTable 进入INSERT OVERWRITE语句时调用
func (l *ddlListener) EnterInsertOverwriteTable(ctx *parser.InsertOverwriteTableContext) {
	l.isOnlyComment = false
	l.curOpType = analyzer.StmtTypeInsert
	l.onWriteStmt()
}

// EnterDeleteFromTable 进入删除语句时调用
func (l *lineageListener) EnterDeleteFromTable(ctx *parser.DeleteFromTableContext) {
	l.curOpType = analyzer.StmtTypeDelete
	l.onWriteStmt()
}

// EnterTruncateTable 进入清空表语句时调用
func (l *lineageListener) EnterTruncateTable(ctx *parser.TruncateTableContext) {
	l.curOpType = analyzer.StmtTypeTruncateTable
	l.onWriteStmt()
}

// EnterUpdateTable 进入更新语句时调用
func (l *lineageListener) EnterUpdateTable(ctx *parser.UpdateTableContext) {
	l.curOpType = analyzer.StmtTypeUpdate
	l.onWriteStmt()
}

// EnterMergeIntoTable 进入合并语句时调用
func (l *lineageListener) EnterMergeIntoTable(ctx *parser.MergeIntoTableContext) {
	l.curOpType = analyzer.StmtTypeMerge
	l.onWriteStmt()
}

// EnterCreateView 进入创建视图语句时调用
func (l *lineageListener) EnterCreateView(ctx *parser.CreateViewContext) {
	// 检查是否是临时视图
	if ctx.TEMPORARY() != nil {
		l.curOpType = analyzer.StmtTypeCreateTemporaryView
	} else {
		l.curOpType = analyzer.StmtTypeCreateView
	}
	l.isOnlyComment = false
	l.onWriteStmt()

	// 提取视图名信息
	if ctx.IdentifierReference() != nil {
		if ctx.IdentifierReference().MultipartIdentifier() != nil {
			parts := ctx.IdentifierReference().MultipartIdentifier().AllErrorCapturingIdentifier()
			cluster, database, viewName := l.extractTableInfo(parts)
			l.addWriteTable(cluster, database, viewName)
		}
	}
}

// EnterCreateView 进入创建视图语句时调用
func (l *ddlListener) EnterCreateView(ctx *parser.CreateViewContext) {
	// 检查是否是临时视图
	if ctx.TEMPORARY() != nil {
		l.curOpType = analyzer.StmtTypeCreateTemporaryView
	} else {
		l.curOpType = analyzer.StmtTypeCreateView
	}
	l.onWriteStmt()

	// 初始化TechInfo
	if l.techInfo == nil {
		l.techInfo = &analyzer.TableInfo{}
	}

	// 提取视图名信息
	if ctx.IdentifierReference() != nil {
		if ctx.IdentifierReference().MultipartIdentifier() != nil {
			parts := ctx.IdentifierReference().MultipartIdentifier().AllErrorCapturingIdentifier()
			cluster, database, viewName := l.extractTableInfo(parts)
			isSpecifiedCluster := cluster != ""
			isSpecifiedDatabase := database != ""

			// 设置默认值
			if cluster == "" {
				cluster = l.defaultCluster
			}
			if database == "" {
				database = l.defaultDatabase
			}

			// 创建ActionTable
			actionTable := &analyzer.ActionInfo{
				ClusterName:         cluster,
				IsSpecifiedCluster:  isSpecifiedCluster,
				DatabaseName:        database,
				IsSpecifiedDatabase: isSpecifiedDatabase,
				TableName:           viewName,
				Columns:             []*analyzer.ActionColumn{},
				ActionType:          analyzer.ActionTypeCreate,
			}
			l.result.Action = actionTable
		}
	}
}

// EnterAlterTableAlterColumn 进入修改表列语句时调用
func (l *ddlListener) EnterAlterTableAlterColumn(ctx *parser.AlterTableAlterColumnContext) {
	l.curOpType = analyzer.StmtTypeAlterTable
	l.onWriteStmt()

	// 提取表名信息
	if ctx.IdentifierReference() != nil {
		if ctx.IdentifierReference().MultipartIdentifier() != nil {
			parts := ctx.IdentifierReference().MultipartIdentifier().AllErrorCapturingIdentifier()
			cluster, database, tableName := l.extractTableInfo(parts)

			// 创建或获取ActionTable
			l.ensureActionTable(cluster, database, tableName, analyzer.ActionTypeAlter)
		}
	}
}

// EnterAddTableColumns 进入添加表列语句时调用
func (l *ddlListener) EnterAddTableColumns(ctx *parser.AddTableColumnsContext) {
	l.curOpType = analyzer.StmtTypeAlterTable
	l.onWriteStmt()

	// 提取表名信息
	if ctx.IdentifierReference() != nil {
		if ctx.IdentifierReference().MultipartIdentifier() != nil {
			parts := ctx.IdentifierReference().MultipartIdentifier().AllErrorCapturingIdentifier()
			cluster, database, tableName := l.extractTableInfo(parts)

			// 创建或获取ActionTable
			l.ensureActionTable(cluster, database, tableName, analyzer.ActionTypeAlter)

			// 提取列信息
			if l.currentTable != nil && ctx.QualifiedColTypeWithPositionList() != nil {
				for _, col := range ctx.QualifiedColTypeWithPositionList().AllQualifiedColTypeWithPosition() {
					// 提取列名
					columnName := ""
					if col.MultipartIdentifier() != nil {
						parts := col.MultipartIdentifier().AllErrorCapturingIdentifier()
						if len(parts) > 0 {
							columnName = util.TrimQuotes(parts[len(parts)-1].GetText())
						}
					}

					// 提取列类型
					columnType := ""
					if col.DataType() != nil {
						columnType = col.DataType().GetText()
					}

					// 提取列选项
					isNotNull := false
					isPrimary := false
					defaultValue := ""
					comment := ""

					// 处理列定义描述符
					for _, desc := range col.AllColDefinitionDescriptorWithPosition() {
						if desc.CommentSpec() != nil {
							if stringLit := desc.CommentSpec().StringLit(); stringLit != nil {
								comment = util.TrimQuotes(stringLit.GetText())
							}
						} else if desc.ErrorCapturingNot() != nil && desc.NULL() != nil {
							isNotNull = true
						} else if desc.DefaultExpression() != nil {
							defaultValue = desc.DefaultExpression().GetText()
						}
					}

					// 创建ActionColumn并添加到当前表
					actionColumn := &analyzer.ActionColumn{
						Name:         columnName,
						Type:         columnType,
						IsNotNull:    isNotNull,
						IsPrimary:    isPrimary,
						DefaultValue: defaultValue,
						Comment:      comment,
						Action:       analyzer.ActionTypeAlter,
					}

					l.currentTable.Columns = append(l.currentTable.Columns, actionColumn)
				}
			}
		}
	}
}

// ensureActionTable 确保ActionTable存在，如果不存在则创建
func (l *ddlListener) ensureActionTable(cluster, database, tableName string, action analyzer.ActionType) {
	// 保存原始值以判断是否指定了集群和数据库
	isSpecifiedCluster := cluster != ""
	isSpecifiedDatabase := database != ""

	// 设置默认值
	if cluster == "" {
		cluster = l.defaultCluster
	}
	if database == "" {
		database = l.defaultDatabase
	}

	// 检查是否已存在相同的ActionTable
	if l.result.Action != nil {
		if l.result.Action.ClusterName == cluster && l.result.Action.DatabaseName == database && l.result.Action.TableName == tableName {
			l.currentTable = l.result.Action
			return
		}
	}

	// 创建新的ActionTable
	newTable := &analyzer.ActionInfo{
		ClusterName:         cluster,
		IsSpecifiedCluster:  isSpecifiedCluster,
		DatabaseName:        database,
		IsSpecifiedDatabase: isSpecifiedDatabase,
		TableName:           tableName,
		Columns:             []*analyzer.ActionColumn{},
		ActionType:          action,
		TableInfo:           l.techInfo,
	}
	l.result.Action = newTable
	l.currentTable = newTable
}

// EnterRenameTableColumn 进入重命名表列语句时调用
func (l *ddlListener) EnterRenameTableColumn(ctx *parser.RenameTableColumnContext) {
	l.curOpType = analyzer.StmtTypeAlterTable
	l.onWriteStmt()

	// 提取表名信息
	if ctx.GetTable() != nil {
		if ctx.GetTable().MultipartIdentifier() != nil {
			parts := ctx.GetTable().MultipartIdentifier().AllErrorCapturingIdentifier()
			cluster, database, tableName := l.extractTableInfo(parts)

			// 创建或获取ActionTable
			l.ensureActionTable(cluster, database, tableName, analyzer.ActionTypeAlter)
		}
	}

	// 提取列名信息
	if l.currentTable != nil {
		// 提取旧列名
		if ctx.GetFrom() != nil {
			oldColumnName := util.TrimQuotes(ctx.GetFrom().GetText())

			actionColumn := &analyzer.ActionColumn{
				Name:         oldColumnName,
				Type:         "", // 重命名操作不需要类型
				IsNotNull:    false,
				IsPrimary:    false,
				DefaultValue: "",
				Comment:      "",
				Action:       analyzer.ActionTypeDrop,
			}

			l.currentTable.Columns = append(l.currentTable.Columns, actionColumn)
		}
		// 提取新列名
		if ctx.GetTo() != nil {
			newColumnName := util.TrimQuotes(ctx.GetTo().GetText())

			actionColumn := &analyzer.ActionColumn{
				Name:         newColumnName,
				Type:         "", // 重命名操作不需要类型
				IsNotNull:    false,
				IsPrimary:    false,
				DefaultValue: "",
				Comment:      "",
				Action:       analyzer.ActionTypeCreate,
			}

			l.currentTable.Columns = append(l.currentTable.Columns, actionColumn)
		}
	}
}

// EnterDropTableColumns 进入删除表列语句时调用
func (l *ddlListener) EnterDropTableColumns(ctx *parser.DropTableColumnsContext) {
	l.curOpType = analyzer.StmtTypeAlterTable
	l.onWriteStmt()

	// 提取表名信息
	if ctx.IdentifierReference() != nil {
		if ctx.IdentifierReference().MultipartIdentifier() != nil {
			parts := ctx.IdentifierReference().MultipartIdentifier().AllErrorCapturingIdentifier()
			cluster, database, tableName := l.extractTableInfo(parts)

			// 创建或获取ActionTable
			l.ensureActionTable(cluster, database, tableName, analyzer.ActionTypeAlter)
		}
	}

	// 提取列名信息
	if l.currentTable != nil {
		if ctx.GetColumns() != nil {
			for _, id := range ctx.GetColumns().AllMultipartIdentifier() {
				if len(id.AllErrorCapturingIdentifier()) > 0 {
					columnName := id.AllErrorCapturingIdentifier()[0].GetText()

					actionColumn := &analyzer.ActionColumn{
						Name:         columnName,
						Type:         "", // 删除操作不需要类型
						IsNotNull:    false,
						IsPrimary:    false,
						DefaultValue: "",
						Comment:      "",
						Action:       analyzer.ActionTypeDrop,
					}

					l.currentTable.Columns = append(l.currentTable.Columns, actionColumn)
				}
			}
		}
	}
}

// EnterRenameTable 进入重命名表语句时调用
func (l *ddlListener) EnterRenameTable(ctx *parser.RenameTableContext) {
	l.curOpType = analyzer.StmtTypeRenameTable
	l.onWriteStmt()

	// 提取表名信息
	if ctx.GetFrom() != nil {
		if ctx.GetFrom().MultipartIdentifier() != nil {
			parts := ctx.GetFrom().MultipartIdentifier().AllErrorCapturingIdentifier()
			cluster, database, tableName := l.extractTableInfo(parts)

			// 创建或获取ActionTable
			l.ensureActionTable(cluster, database, tableName, analyzer.ActionTypeDrop)
		}
	}

	// 提取新表名信息
	if ctx.GetTo() != nil {
		parts := ctx.GetTo().AllErrorCapturingIdentifier()
		cluster, database, tableName := l.extractTableInfo(parts)

		// 创建AnotherAction
		isSpecifiedCluster := cluster != ""
		isSpecifiedDatabase := database != ""

		// 设置默认值
		if cluster == "" {
			cluster = l.defaultCluster
		}
		if database == "" {
			database = l.defaultDatabase
		}

		// 创建AnotherAction
		anotherAction := &analyzer.ActionInfo{
			ClusterName:         cluster,
			IsSpecifiedCluster:  isSpecifiedCluster,
			DatabaseName:        database,
			IsSpecifiedDatabase: isSpecifiedDatabase,
			TableName:           tableName,
			Columns:             []*analyzer.ActionColumn{},
			ActionType:          analyzer.ActionTypeCreate,
		}
		l.result.AnotherAction = anotherAction
	}
}

// EnterSetTableProperties 进入设置表属性语句时调用
func (l *ddlListener) EnterSetTableProperties(ctx *parser.SetTablePropertiesContext) {
	l.curOpType = analyzer.StmtTypeAlterTable
	l.onWriteStmt()

	// 提取表名信息
	if ctx.IdentifierReference() != nil {
		if ctx.IdentifierReference().MultipartIdentifier() != nil {
			parts := ctx.IdentifierReference().MultipartIdentifier().AllErrorCapturingIdentifier()
			cluster, database, tableName := l.extractTableInfo(parts)

			// 创建或获取ActionTable
			l.ensureActionTable(cluster, database, tableName, analyzer.ActionTypeAlter)

			// 提取表属性信息
			if l.techInfo == nil {
				l.techInfo = &analyzer.TableInfo{}
			}

			// 提取表属性
			if propertyList := ctx.PropertyList(); propertyList != nil {
				l.extractTableProperties(propertyList)
			}

			// 调用exitDdl来设置TableInfo
			l.exitDdl()
		}
	}
}

// EnterUnsetTableProperties 进入取消设置表属性语句时调用
func (l *ddlListener) EnterUnsetTableProperties(ctx *parser.UnsetTablePropertiesContext) {
	l.curOpType = analyzer.StmtTypeAlterTable
	l.onWriteStmt()
}

// EnterHiveChangeColumn 进入Hive修改列语句时调用
func (l *ddlListener) EnterHiveChangeColumn(ctx *parser.HiveChangeColumnContext) {
	l.curOpType = analyzer.StmtTypeAlterTable
	l.onWriteStmt()

	// 提取表名信息
	if ctx.GetTable() != nil {
		if ctx.GetTable().MultipartIdentifier() != nil {
			parts := ctx.GetTable().MultipartIdentifier().AllErrorCapturingIdentifier()
			cluster, database, tableName := l.extractTableInfo(parts)

			// 创建或获取ActionTable
			l.ensureActionTable(cluster, database, tableName, analyzer.ActionTypeAlter)

			// 提取列信息
			if l.currentTable != nil {
				// 提取列名
				columnName := ""
				if ctx.GetColName() != nil {
					parts := ctx.GetColName().AllErrorCapturingIdentifier()
					if len(parts) > 0 {
						columnName = strings.Trim(parts[len(parts)-1].GetText(), "`")
					}
				}

				// 提取列类型
				columnType := ""
				if ctx.ColType() != nil {
					if ctx.ColType().DataType() != nil {
						columnType = ctx.ColType().DataType().GetText()
					}
				}

				// 提取列注释
				comment := ""
				if ctx.ColType() != nil {
					if ctx.ColType().CommentSpec() != nil {
						if stringLit := ctx.ColType().CommentSpec().StringLit(); stringLit != nil {
							comment = strings.Trim(stringLit.GetText(), "'")
						}
					}
				}

				// 创建ActionColumn并添加到当前表
				actionColumn := &analyzer.ActionColumn{
					Name:         columnName,
					Type:         columnType,
					IsNotNull:    false,
					IsPrimary:    false,
					DefaultValue: "",
					Comment:      comment,
					Action:       analyzer.ActionTypeAlter,
				}

				l.currentTable.Columns = append(l.currentTable.Columns, actionColumn)
			}
		}
	}
}

// EnterHiveReplaceColumns 进入Hive替换列语句时调用
func (l *ddlListener) EnterHiveReplaceColumns(ctx *parser.HiveReplaceColumnsContext) {
	l.curOpType = analyzer.StmtTypeAlterTable
	l.onWriteStmt()
}

// EnterSetTableSerDe 进入设置表SerDe语句时调用
func (l *ddlListener) EnterSetTableSerDe(ctx *parser.SetTableSerDeContext) {
	l.curOpType = analyzer.StmtTypeAlterTable
	l.onWriteStmt()
}

// EnterAddTablePartition 进入添加表分区语句时调用
func (l *ddlListener) EnterAddTablePartition(ctx *parser.AddTablePartitionContext) {
	l.curOpType = analyzer.StmtTypeAlterTable
	l.onWriteStmt()
}

// EnterRenameTablePartition 进入重命名表分区语句时调用
func (l *ddlListener) EnterRenameTablePartition(ctx *parser.RenameTablePartitionContext) {
	l.curOpType = analyzer.StmtTypeAlterTable
	l.onWriteStmt()
}

// EnterDropTablePartitions 进入删除表分区语句时调用
func (l *ddlListener) EnterDropTablePartitions(ctx *parser.DropTablePartitionsContext) {
	l.curOpType = analyzer.StmtTypeAlterTable
	l.onWriteStmt()
}

// EnterSetTableLocation 进入设置表位置语句时调用
func (l *ddlListener) EnterSetTableLocation(ctx *parser.SetTableLocationContext) {
	l.curOpType = analyzer.StmtTypeAlterTable
	l.onWriteStmt()
}

// EnterRecoverPartitions 进入恢复分区语句时调用
func (l *ddlListener) EnterRecoverPartitions(ctx *parser.RecoverPartitionsContext) {
	l.curOpType = analyzer.StmtTypeAlterTable
	l.onWriteStmt()
}

// EnterAlterClusterBy 进入修改聚类语句时调用
func (l *ddlListener) EnterAlterClusterBy(ctx *parser.AlterClusterByContext) {
	l.curOpType = analyzer.StmtTypeAlterTable
	l.onWriteStmt()
}

// EnterAlterTableCollation 进入修改表排序规则语句时调用
func (l *ddlListener) EnterAlterTableCollation(ctx *parser.AlterTableCollationContext) {
	l.curOpType = analyzer.StmtTypeAlterTable
	l.onWriteStmt()
}

// EnterAddTableConstraint 进入添加表约束语句时调用
func (l *ddlListener) EnterAddTableConstraint(ctx *parser.AddTableConstraintContext) {
	l.curOpType = analyzer.StmtTypeAlterTable
	l.onWriteStmt()
}

// EnterDropTableConstraint 进入删除表约束语句时调用
func (l *ddlListener) EnterDropTableConstraint(ctx *parser.DropTableConstraintContext) {
	l.curOpType = analyzer.StmtTypeAlterTable
	l.onWriteStmt()
}

// EnterDropTable 进入删除表语句时调用
func (l *ddlListener) EnterDropTable(ctx *parser.DropTableContext) {
	l.curOpType = analyzer.StmtTypeDropTable
	l.onWriteStmt()

	// 提取表名信息
	if ctx.IdentifierReference() != nil {
		if ctx.IdentifierReference().MultipartIdentifier() != nil {
			parts := ctx.IdentifierReference().MultipartIdentifier().AllErrorCapturingIdentifier()
			cluster, database, tableName := l.extractTableInfo(parts)
			isSpecifiedCluster := len(parts) >= 3
			isSpecifiedDatabase := len(parts) >= 2

			// 设置默认值
			if cluster == "" {
				cluster = l.defaultCluster
			}
			if database == "" {
				database = l.defaultDatabase
			}

			// 创建ActionTable
			actionTable := &analyzer.ActionInfo{
				ClusterName:         cluster,
				IsSpecifiedCluster:  isSpecifiedCluster,
				DatabaseName:        database,
				IsSpecifiedDatabase: isSpecifiedDatabase,
				TableName:           tableName,
				Columns:             []*analyzer.ActionColumn{},
				ActionType:          analyzer.ActionTypeDrop,
			}
			l.result.Action = actionTable
		}
	}
}

// EnterCreateTable 进入创建表语句时调用
func (l *lineageListener) EnterCreateTable(ctx *parser.CreateTableContext) {
	// 检查是否是临时表
	if ctx.CreateTableHeader() != nil && ctx.CreateTableHeader().TEMPORARY() != nil {
		l.curOpType = analyzer.StmtTypeCreateTemporaryTable
	} else {
		l.curOpType = analyzer.StmtTypeCreateTable
	}
	l.onWriteStmt()

	// 提取表名信息
	if ctx.CreateTableHeader() != nil {
		if ctx.CreateTableHeader().IdentifierReference() != nil {
			if ctx.CreateTableHeader().IdentifierReference().MultipartIdentifier() != nil {
				parts := ctx.CreateTableHeader().IdentifierReference().MultipartIdentifier().AllErrorCapturingIdentifier()
				cluster, database, tableName := l.extractTableInfo(parts)
				l.addWriteTable(cluster, database, tableName)
			}
		}
	}
}

// EnterCreateTable 进入创建表语句时调用
func (l *ddlListener) EnterCreateTable(ctx *parser.CreateTableContext) {
	l.curOpType = analyzer.StmtTypeCreateTable
	l.onWriteStmt()

	// 提取表名信息
	if ctx.CreateTableHeader() != nil {
		if ctx.CreateTableHeader().IdentifierReference() != nil {
			if ctx.CreateTableHeader().IdentifierReference().MultipartIdentifier() != nil {
				parts := ctx.CreateTableHeader().IdentifierReference().MultipartIdentifier().AllErrorCapturingIdentifier()
				cluster, database, tableName := l.extractTableInfo(parts)
				isSpecifiedCluster := cluster != ""
				isSpecifiedDatabase := database != ""

				// 设置默认值
				if cluster == "" {
					cluster = l.defaultCluster
				}
				if database == "" {
					database = l.defaultDatabase
				}

				// 创建ActionTable
				l.currentTable = &analyzer.ActionInfo{
					ClusterName:         cluster,
					IsSpecifiedCluster:  isSpecifiedCluster,
					DatabaseName:        database,
					IsSpecifiedDatabase: isSpecifiedDatabase,
					TableName:           tableName,
					Columns:             []*analyzer.ActionColumn{},
					ActionType:          analyzer.ActionTypeCreate,
				}
				l.result.Action = l.currentTable
			}
		}
	}

	// 提取技术信息
	l.extractTechInfo(ctx)

	// 提取表格式信息（从USING子句）
	if tableProvider := ctx.TableProvider(); tableProvider != nil {
		if multipartIdentifier := tableProvider.MultipartIdentifier(); multipartIdentifier != nil {
			if len(multipartIdentifier.AllErrorCapturingIdentifier()) > 0 {
				l.techInfo.LakehouseTableFormat = multipartIdentifier.AllErrorCapturingIdentifier()[0].GetText()
			}
		}
	}

	// 提取存储路径信息（从TBLPROPERTIES或LOCATION子句）
	if createTableClauses := ctx.CreateTableClauses(); createTableClauses != nil {
		// 提取LOCATION子句
		for _, locationSpec := range createTableClauses.AllLocationSpec() {
			if stringLit := locationSpec.StringLit(); stringLit != nil {
				l.techInfo.Locations = append(l.techInfo.Locations, stringLit.GetText())
			}
		}

		// 提取TBLPROPERTIES中的path属性
		if propertyList := createTableClauses.GetTableProps(); propertyList != nil {
			for _, property := range propertyList.AllProperty() {
				if propertyWithKeyAndEquals, ok := property.(*parser.PropertyWithKeyAndEqualsContext); ok {
					if key := propertyWithKeyAndEquals.GetKey(); key != nil {
						if keyText := key.GetText(); strings.Trim(keyText, "'\"") == "path" {
							if value := propertyWithKeyAndEquals.GetValue(); value != nil {
								l.techInfo.Locations = append(l.techInfo.Locations, value.GetText())
							}
						}
					}
				}
			}
		}
	}
}

// ExitCreateTable 退出创建表语句时调用
func (l *ddlListener) ExitCreateTable(ctx *parser.CreateTableContext) {
	l.exitDdl()
}

// ExitCreateView 退出创建表语句时调用
func (l *ddlListener) ExitCreateView(ctx *parser.CreateViewContext) {
	l.exitDdl()
}

// ExitReplaceTable 退出创建表语句时调用
func (l *ddlListener) ExitReplaceTable(ctx *parser.ReplaceTableContext) {
	l.exitDdl()
}

// exitDdl 退出时调用
func (l *ddlListener) exitDdl() {
	// 总是设置TechInfo，即使没有实际信息
	if l.currentTable != nil {
		l.currentTable.TableInfo = l.techInfo
	}

	// 设置主键列的IsPrimary字段
	if l.currentTable != nil && l.techInfo != nil && len(l.techInfo.PrimaryKeyColumnNames) > 0 {
		// 创建主键列名的映射，方便查找
		primaryKeyMap := make(map[string]bool)
		for _, pkColumn := range l.techInfo.PrimaryKeyColumnNames {
			primaryKeyMap[pkColumn] = true
		}

		// 遍历所有列，标记主键列
		for _, column := range l.currentTable.Columns {
			if primaryKeyMap[column.Name] {
				column.IsPrimary = true
			}
		}
	}
}

// EnterCreateTableLike 进入创建表（LIKE）语句时调用
func (l *lineageListener) EnterCreateTableLike(ctx *parser.CreateTableLikeContext) {
	l.curOpType = analyzer.StmtTypeCreateTableLike
	l.onWriteStmt()
}

// EnterCreateTableLike 进入创建表（LIKE）语句时调用
func (l *ddlListener) EnterCreateTableLike(ctx *parser.CreateTableLikeContext) {
	l.curOpType = analyzer.StmtTypeCreateTable
	l.onWriteStmt()

	// 提取表名信息
	if ctx.GetTarget() != nil {
		parts := ctx.GetTarget().AllErrorCapturingIdentifier()
		cluster, database, tableName := l.extractTableInfo(parts)
		isSpecifiedCluster := cluster != ""
		isSpecifiedDatabase := database != ""

		// 设置默认值
		if cluster == "" {
			cluster = l.defaultCluster
		}
		if database == "" {
			database = l.defaultDatabase
		}

		// 创建ActionTable
		actionTable := &analyzer.ActionInfo{
			ClusterName:         cluster,
			DatabaseName:        database,
			IsSpecifiedCluster:  isSpecifiedCluster,
			IsSpecifiedDatabase: isSpecifiedDatabase,
			TableName:           tableName,
			Columns:             []*analyzer.ActionColumn{},
			ActionType:          analyzer.ActionTypeCreate,
		}
		l.result.Action = actionTable
	}

	// 初始化TechInfo
	l.techInfo = &analyzer.TableInfo{
		PartitionColumnNames:  nil,
		PrimaryKeyColumnNames: nil,
		Locations:             nil,
	}
}

// ExitCreateTableLike 退出创建表（LIKE）语句时调用
func (l *ddlListener) ExitCreateTableLike(ctx *parser.CreateTableLikeContext) {
	l.exitDdl()
}

// EnterReplaceTable 进入替换表语句时调用
func (l *lineageListener) EnterReplaceTable(ctx *parser.ReplaceTableContext) {
	l.curOpType = analyzer.StmtTypeReplaceTable
	l.onWriteStmt()
}

// EnterReplaceTable 进入替换表语句时调用
func (l *ddlListener) EnterReplaceTable(ctx *parser.ReplaceTableContext) {
	l.curOpType = analyzer.StmtTypeReplaceTable
	l.onWriteStmt()

	// 提取表名信息
	if ctx.ReplaceTableHeader() != nil {
		if ctx.ReplaceTableHeader().IdentifierReference() != nil {
			if ctx.ReplaceTableHeader().IdentifierReference().MultipartIdentifier() != nil {
				parts := ctx.ReplaceTableHeader().IdentifierReference().MultipartIdentifier().AllErrorCapturingIdentifier()
				cluster, database, tableName := l.extractTableInfo(parts)
				isSpecifiedCluster := cluster != ""
				isSpecifiedDatabase := database != ""

				// 设置默认值
				if cluster == "" {
					cluster = l.defaultCluster
				}
				if database == "" {
					database = l.defaultDatabase
				}

				// 创建ActionTable
				actionTable := &analyzer.ActionInfo{
					ClusterName:         cluster,
					IsSpecifiedCluster:  isSpecifiedCluster,
					DatabaseName:        database,
					IsSpecifiedDatabase: isSpecifiedDatabase,
					TableName:           tableName,
					Columns:             []*analyzer.ActionColumn{},
					ActionType:          analyzer.ActionTypeCreate,
					TableInfo:           &analyzer.TableInfo{},
				}
				l.result.Action = actionTable
			}
		}
	}

	// 初始化TechInfo
	if l.techInfo == nil {
		l.techInfo = &analyzer.TableInfo{}
	}
}

// EnterAlterTableAlterColumn 进入修改表列语句时调用
func (l *lineageListener) EnterAlterTableAlterColumn(ctx *parser.AlterTableAlterColumnContext) {
	l.curOpType = analyzer.StmtTypeAlterTable
	l.onWriteStmt()
}

// EnterAddTableColumns 进入添加表列语句时调用
func (l *lineageListener) EnterAddTableColumns(ctx *parser.AddTableColumnsContext) {
	l.curOpType = analyzer.StmtTypeAlterTable
	l.onWriteStmt()
}

// EnterRenameTableColumn 进入重命名表列语句时调用
func (l *lineageListener) EnterRenameTableColumn(ctx *parser.RenameTableColumnContext) {
	l.curOpType = analyzer.StmtTypeAlterTable
	l.onWriteStmt()
}

// EnterDropTableColumns 进入删除表列语句时调用
func (l *lineageListener) EnterDropTableColumns(ctx *parser.DropTableColumnsContext) {
	l.curOpType = analyzer.StmtTypeAlterTable
	l.onWriteStmt()
}

// EnterRenameTable 进入重命名表语句时调用
func (l *lineageListener) EnterRenameTable(ctx *parser.RenameTableContext) {
	l.curOpType = analyzer.StmtTypeAlterTable
	l.onWriteStmt()
}

// EnterSetTableProperties 进入设置表属性语句时调用
func (l *lineageListener) EnterSetTableProperties(ctx *parser.SetTablePropertiesContext) {
	l.curOpType = analyzer.StmtTypeAlterTable
	l.onWriteStmt()
}

// EnterUnsetTableProperties 进入取消设置表属性语句时调用
func (l *lineageListener) EnterUnsetTableProperties(ctx *parser.UnsetTablePropertiesContext) {
	l.curOpType = analyzer.StmtTypeAlterTable
	l.onWriteStmt()
}

// EnterHiveChangeColumn 进入Hive修改列语句时调用
func (l *lineageListener) EnterHiveChangeColumn(ctx *parser.HiveChangeColumnContext) {
	l.curOpType = analyzer.StmtTypeAlterTable
	l.onWriteStmt()
}

// EnterHiveReplaceColumns 进入Hive替换列语句时调用
func (l *lineageListener) EnterHiveReplaceColumns(ctx *parser.HiveReplaceColumnsContext) {
	l.curOpType = analyzer.StmtTypeAlterTable
	l.onWriteStmt()
}

// EnterSetTableSerDe 进入设置表SerDe语句时调用
func (l *lineageListener) EnterSetTableSerDe(ctx *parser.SetTableSerDeContext) {
	l.curOpType = analyzer.StmtTypeAlterTable
	l.onWriteStmt()
}

// EnterAddTablePartition 进入添加表分区语句时调用
func (l *lineageListener) EnterAddTablePartition(ctx *parser.AddTablePartitionContext) {
	l.curOpType = analyzer.StmtTypeAlterTable
	l.onWriteStmt()
}

// EnterRenameTablePartition 进入重命名表分区语句时调用
func (l *lineageListener) EnterRenameTablePartition(ctx *parser.RenameTablePartitionContext) {
	l.curOpType = analyzer.StmtTypeAlterTable
	l.onWriteStmt()
}

// EnterDropTablePartitions 进入删除表分区语句时调用
func (l *lineageListener) EnterDropTablePartitions(ctx *parser.DropTablePartitionsContext) {
	l.curOpType = analyzer.StmtTypeAlterTable
	l.onWriteStmt()
}

// EnterSetTableLocation 进入设置表位置语句时调用
func (l *lineageListener) EnterSetTableLocation(ctx *parser.SetTableLocationContext) {
	l.curOpType = analyzer.StmtTypeAlterTable
	l.onWriteStmt()
}

// EnterRecoverPartitions 进入恢复分区语句时调用
func (l *lineageListener) EnterRecoverPartitions(ctx *parser.RecoverPartitionsContext) {
	l.curOpType = analyzer.StmtTypeAlterTable
	l.onWriteStmt()
}

// EnterAlterClusterBy 进入修改聚类语句时调用
func (l *lineageListener) EnterAlterClusterBy(ctx *parser.AlterClusterByContext) {
	l.curOpType = analyzer.StmtTypeAlterTable
	l.onWriteStmt()
}

// EnterAlterTableCollation 进入修改表排序规则语句时调用
func (l *lineageListener) EnterAlterTableCollation(ctx *parser.AlterTableCollationContext) {
	l.curOpType = analyzer.StmtTypeAlterTable
	l.onWriteStmt()
}

// EnterAddTableConstraint 进入添加表约束语句时调用
func (l *lineageListener) EnterAddTableConstraint(ctx *parser.AddTableConstraintContext) {
	l.curOpType = analyzer.StmtTypeAlterTable
	l.onWriteStmt()
}

// EnterDropTableConstraint 进入删除表约束语句时调用
func (l *lineageListener) EnterDropTableConstraint(ctx *parser.DropTableConstraintContext) {
	l.curOpType = analyzer.StmtTypeAlterTable
	l.onWriteStmt()
}

// EnterDropTable 进入删除表语句时调用
func (l *lineageListener) EnterDropTable(ctx *parser.DropTableContext) {
	l.curOpType = analyzer.StmtTypeDropTable
	l.onWriteStmt()
}

// EnterComment 进入注释语句时调用
func (l *lineageListener) EnterComment(ctx *parser.CommentContext) {
	// 记录注释内容
	commentText := ctx.GetText()
	l.comments = append(l.comments, commentText)
}

// EnterComment 进入注释语句时调用
func (l *ddlListener) EnterComment(ctx *parser.CommentContext) {
	// 记录注释内容
	commentText := ctx.GetText()
	l.comments = append(l.comments, commentText)
}

// EnterUse 进入USE语句时调用，处理USE database形式
func (l *lineageListener) EnterUse(ctx *parser.UseContext) {
	l.curOpType = analyzer.StmtTypeUseDatabase
	l.isOnlyComment = false
	if l.firstOpType == "" {
		l.firstOpType = analyzer.StmtTypeUseDatabase
	}

	// 提取数据库名并添加到Reads
	if ctx.IdentifierReference() != nil {
		if ctx.IdentifierReference().MultipartIdentifier() != nil {
			parts := ctx.IdentifierReference().MultipartIdentifier().AllErrorCapturingIdentifier()
			if len(parts) > 0 {
				database := util.TrimQuotes(parts[0].GetText())
				l.addReadTable("", database, "")
			}
		}
	}
}

// EnterUse 进入USE语句时调用，处理USE database形式
func (l *ddlListener) EnterUse(ctx *parser.UseContext) {
	l.curOpType = analyzer.StmtTypeUseDatabase
	l.isOnlyComment = false
	if l.firstOpType == "" {
		l.firstOpType = analyzer.StmtTypeUseDatabase
	}
}

// EnterUseNamespace 进入USE语句时调用，处理USE namespace database形式（如USE CATALOG db）
func (l *lineageListener) EnterUseNamespace(ctx *parser.UseNamespaceContext) {
	l.curOpType = analyzer.StmtTypeUseCatalog
	l.isOnlyComment = false
	if l.firstOpType == "" {
		l.firstOpType = analyzer.StmtTypeUseCatalog
	}
}

// EnterUseNamespace 进入USE语句时调用，处理USE namespace database形式（如USE CATALOG db）
func (l *ddlListener) EnterUseNamespace(ctx *parser.UseNamespaceContext) {
	l.curOpType = analyzer.StmtTypeUseCatalog
	l.isOnlyComment = false
	if l.firstOpType == "" {
		l.firstOpType = analyzer.StmtTypeUseCatalog
	}
}

// EnterSetVariable 进入SET变量语句时调用
func (l *lineageListener) EnterSetVariable(ctx *parser.SetVariableContext) {
	l.curOpType = analyzer.StmtTypeSetVar
	l.isOnlyComment = false
	if l.firstOpType == "" {
		l.firstOpType = analyzer.StmtTypeSetVar
	}
}

// EnterSetVariable 进入SET变量语句时调用
func (l *ddlListener) EnterSetVariable(ctx *parser.SetVariableContext) {
	l.curOpType = analyzer.StmtTypeSetVar
	l.isOnlyComment = false
	if l.firstOpType == "" {
		l.firstOpType = analyzer.StmtTypeSetVar
	}
}

// EnterSetConfiguration 进入SET配置语句时调用
func (l *lineageListener) EnterSetConfiguration(ctx *parser.SetConfigurationContext) {
	l.curOpType = analyzer.StmtTypeSetVar
	l.isOnlyComment = false
	if l.firstOpType == "" {
		l.firstOpType = analyzer.StmtTypeSetVar
	}
}

// EnterSetConfiguration 进入SET配置语句时调用
func (l *ddlListener) EnterSetConfiguration(ctx *parser.SetConfigurationContext) {
	l.curOpType = analyzer.StmtTypeSetVar
	l.isOnlyComment = false
	if l.firstOpType == "" {
		l.firstOpType = analyzer.StmtTypeSetVar
	}
}

// EnterSetVariableInsideSqlScript 进入SQL脚本中的SET变量语句时调用
func (l *lineageListener) EnterSetVariableInsideSqlScript(ctx *parser.SetVariableInsideSqlScriptContext) {
	l.curOpType = analyzer.StmtTypeSetVar
	l.isOnlyComment = false
	if l.firstOpType == "" {
		l.firstOpType = analyzer.StmtTypeSetVar
	}
}

// EnterSetVariableInsideSqlScript 进入SQL脚本中的SET变量语句时调用
func (l *ddlListener) EnterSetVariableInsideSqlScript(ctx *parser.SetVariableInsideSqlScriptContext) {
	l.curOpType = analyzer.StmtTypeSetVar
	l.isOnlyComment = false
	if l.firstOpType == "" {
		l.firstOpType = analyzer.StmtTypeSetVar
	}
}

// EnterCreateNamespace 进入创建数据库/命名空间语句时调用
func (l *ddlListener) EnterCreateNamespace(ctx *parser.CreateNamespaceContext) {
	l.curOpType = analyzer.StmtTypeCreateDatabase
	l.onWriteStmt()

	// 提取数据库名信息
	if ctx.IdentifierReference() != nil {
		if ctx.IdentifierReference().MultipartIdentifier() != nil {
			parts := ctx.IdentifierReference().MultipartIdentifier().AllErrorCapturingIdentifier()
			var cluster, database string
			var isSpecifiedCluster bool

			// 根据parts长度确定层次：cluster.db 或 db
			switch len(parts) {
			case 1:
				// 只有数据库名
				database = util.TrimQuotes(parts[0].GetText())
				isSpecifiedCluster = false
			default:
				// 集群名和数据库名
				cluster = util.TrimQuotes(parts[0].GetText())
				database = util.TrimQuotes(parts[1].GetText())
				isSpecifiedCluster = true
			}

			// 设置默认值
			if cluster == "" {
				cluster = l.defaultCluster
			}

			// 创建ActionTable
			actionTable := &analyzer.ActionInfo{
				ClusterName:         cluster,
				IsSpecifiedCluster:  isSpecifiedCluster,
				DatabaseName:        database,
				IsSpecifiedDatabase: true,
				TableName:           "",
				Columns:             []*analyzer.ActionColumn{},
				ActionType:          analyzer.ActionTypeCreate,
			}
			l.result.Action = actionTable
		}
	}
}

// EnterIdentifierReference 进入标识符引用时调用，用于提取数据库名和表名
func (l *lineageListener) EnterIdentifierReference(ctx *parser.IdentifierReferenceContext) {
	// USE语句不应该添加表依赖
	if l.curOpType == analyzer.StmtTypeUseDatabase || l.curOpType == analyzer.StmtTypeUseCatalog {
		return
	}

	// DDL语句已经在各自的Enter方法中处理了表名，不需要在这里重复处理
	// 但CREATE_VIEW和CREATE_TEMPORARY_VIEW需要保留，因为它们包含SELECT语句需要解析
	if l.curOpType == analyzer.StmtTypeCreateTable || l.curOpType == analyzer.StmtTypeCreateTemporaryTable || l.curOpType == analyzer.StmtTypeAlterTable ||
		l.curOpType == analyzer.StmtTypeDropTable || l.curOpType == analyzer.StmtTypeTruncateTable ||
		l.curOpType == analyzer.StmtTypeCreateTableLike || l.curOpType == analyzer.StmtTypeTempFunc || l.curOpType == analyzer.StmtTypeCall {
		return
	}

	// 对于CREATE_VIEW和CREATE_TEMPORARY_VIEW语句，只处理SELECT部分的表引用，不处理视图名
	if l.curOpType == analyzer.StmtTypeCreateView || l.curOpType == analyzer.StmtTypeCreateTemporaryView {
		// 检查是否是视图名（父节点是CreateViewContext）
		if _, ok := ctx.GetParent().(*parser.CreateViewContext); ok {
			// 是视图名，跳过
			return
		}
	}

	if ctx.MultipartIdentifier() != nil {
		parts := ctx.MultipartIdentifier().AllErrorCapturingIdentifier()
		if len(parts) > 0 {
			cluster, database, tableName := l.extractTableInfo(parts)
			// 检查是否是CTE名称，如果是则跳过，CTE不是实际的表依赖
			// 只有当没有指定数据库名时，才检查表名是否是CTE名称
			if database == "" {
				if _, isCTE := l.cteNames[strings.ReplaceAll(tableName, "`", "")]; isCTE {
					return
				}
			}
			// 对于CTE中的表，总是作为读表处理，除非明确是写操作
			if l.curOpType == "" || l.curOpType == analyzer.StmtTypeSelect {
				l.addReadTable(cluster, database, tableName)
			} else {
				// 这些操作中的标识符引用通常是写表
				l.addWriteTable(cluster, database, tableName)
			}
		}
	}
}

// extractTableInfo 从MultipartIdentifier中提取表信息
func (l *lineageListener) extractTableInfo(parts []parser.IErrorCapturingIdentifierContext) (cluster, database, table string) {
	if len(parts) == 0 {
		return
	}

	// 根据parts长度确定层次：cluster.db.table 或 db.table 或 table
	switch len(parts) {
	case 1:
		// 只有表名
		table = util.TrimQuotes(parts[0].GetText())
	case 2:
		// 数据库名和表名
		database = util.TrimQuotes(parts[0].GetText())
		table = util.TrimQuotes(parts[1].GetText())
	default:
		// 集群名、数据库名和表名
		cluster = util.TrimQuotes(parts[0].GetText())
		database = util.TrimQuotes(parts[1].GetText())
		table = util.TrimQuotes(parts[2].GetText())
	}

	return
}

// extractTableInfo 从MultipartIdentifier中提取表信息
func (l *ddlListener) extractTableInfo(parts []parser.IErrorCapturingIdentifierContext) (cluster, database, table string) {
	if len(parts) == 0 {
		return
	}

	// 根据parts长度确定层次：cluster.db.table 或 db.table 或 table
	switch len(parts) {
	case 1:
		// 只有表名
		table = util.TrimQuotes(parts[0].GetText())
	case 2:
		// 数据库名和表名
		database = util.TrimQuotes(parts[0].GetText())
		table = util.TrimQuotes(parts[1].GetText())
	default:
		// 集群名、数据库名和表名
		cluster = util.TrimQuotes(parts[0].GetText())
		database = util.TrimQuotes(parts[1].GetText())
		table = util.TrimQuotes(parts[2].GetText())
	}

	return
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

func (l *lineageListener) onWriteStmt() {
	// 标记为写入操作，并设置第一个操作类型
	l.isWriteOp = true
	// 总是使用写入操作作为firstOpType，因为写入操作是主要操作
	l.firstOpType = l.curOpType
}

func (l *lineageListener) onReadStmt() {
	l.curOpType = analyzer.StmtTypeSelect
	// 如果还没有遇到写入操作，设置第一个操作类型
	if !l.isWriteOp && l.firstOpType == "" {
		l.firstOpType = analyzer.StmtTypeSelect
	}
}

func (l *ddlListener) onWriteStmt() {
	// 标记为写入操作，并设置第一个操作类型
	l.isWriteOp = true
	// 总是使用写入操作作为firstOpType，因为写入操作是主要操作
	l.firstOpType = l.curOpType
}

// EnterColDefinition 进入列定义时调用
func (l *ddlListener) EnterColDefinition(ctx *parser.ColDefinitionContext) {
	if l.currentTable == nil {
		return
	}

	// 提取列名
	columnName := ""
	if ctx.GetColName() != nil {
		columnName = util.TrimQuotes(ctx.GetColName().GetText())
	}

	// 提取列类型
	columnType := ""
	if ctx.DataType() != nil {
		columnType = ctx.DataType().GetText()
	}

	// 提取列选项
	isNotNull := false
	isPrimary := false
	defaultValue := ""
	comment := ""

	for _, option := range ctx.AllColDefinitionOption() {
		// 检查NOT NULL约束
		if option.ErrorCapturingNot() != nil && option.NULL() != nil {
			isNotNull = true
		} else if option.DefaultExpression() != nil {
			// 检查DEFAULT值
			defaultValue = option.DefaultExpression().GetText()
		} else if option.CommentSpec() != nil {
			// 检查注释
			if stringLit := option.CommentSpec().StringLit(); stringLit != nil {
				comment = util.TrimQuotes(stringLit.GetText())
			}
		} else if option.ColumnConstraintDefinition() != nil {
			// 检查PRIMARY KEY约束
			// 简化处理：如果有ColumnConstraintDefinition，假设是PRIMARY KEY
			// 实际情况可能更复杂，需要检查具体约束类型
			isPrimary = true
			// 将主键列添加到PrimaryKeyColumnNames
			if columnName != "" {
				// 确保techInfo已初始化
				if l.techInfo == nil {
					l.techInfo = &analyzer.TableInfo{
						PartitionColumnNames:  nil,
						PrimaryKeyColumnNames: nil,
						Locations:             nil,
					}
				}
				if l.techInfo.PrimaryKeyColumnNames == nil {
					l.techInfo.PrimaryKeyColumnNames = []string{}
				}
				l.techInfo.PrimaryKeyColumnNames = append(l.techInfo.PrimaryKeyColumnNames, columnName)
			}
		}
	}

	// 创建ActionColumn并添加到当前表
	actionColumn := &analyzer.ActionColumn{
		Name:         columnName,
		Type:         columnType,
		IsNotNull:    isNotNull,
		IsPrimary:    isPrimary,
		DefaultValue: defaultValue,
		Comment:      comment,
		Action:       analyzer.ActionTypeCreate,
	}

	l.currentTable.Columns = append(l.currentTable.Columns, actionColumn)
}

// EnterTableConstraintDefinition 进入表约束定义时调用
func (l *ddlListener) EnterTableConstraintDefinition(ctx *parser.TableConstraintDefinitionContext) {
	if l.currentTable == nil {
		return
	}

	// 提取表约束
	if tableConstraint := ctx.TableConstraint(); tableConstraint != nil {
		// 检查是否是唯一约束（包括PRIMARY KEY）
		if uniqueConstraint := tableConstraint.UniqueConstraint(); uniqueConstraint != nil {
			// 检查是否是PRIMARY KEY
			if uniqueSpec := uniqueConstraint.UniqueSpec(); uniqueSpec != nil {
				if uniqueSpec.PRIMARY() != nil {
					// 提取主键列名
					if identifierList := uniqueConstraint.IdentifierList(); identifierList != nil {
						// 确保techInfo已初始化
						if l.techInfo == nil {
							l.techInfo = &analyzer.TableInfo{
								PartitionColumnNames:  nil,
								PrimaryKeyColumnNames: nil,
								Locations:             nil,
							}
						}
						if l.techInfo.PrimaryKeyColumnNames == nil {
							l.techInfo.PrimaryKeyColumnNames = []string{}
						}
						// 提取所有主键列
						if identifierSeq := identifierList.IdentifierSeq(); identifierSeq != nil {
							for _, id := range identifierSeq.AllErrorCapturingIdentifier() {
								columnName := id.GetText()
								// 移除反引号
								columnName = util.TrimQuotes(columnName)
								l.techInfo.PrimaryKeyColumnNames = append(l.techInfo.PrimaryKeyColumnNames, columnName)
							}
						}
					}
				}
			}
		}
	}
}

// extractTechInfo 从 CREATE TABLE 语句中提取 TableInfo 信息
func (l *ddlListener) extractTechInfo(ctx *parser.CreateTableContext) {
	// 初始化TechInfo
	if l.techInfo == nil {
		l.techInfo = &analyzer.TableInfo{}
	}

	// 提取分区列和表属性
	if createTableClauses := ctx.CreateTableClauses(); createTableClauses != nil {
		// 提取分区列
		if partitionFieldList := createTableClauses.GetPartitioning(); partitionFieldList != nil {
			l.extractPartitionColumns(partitionFieldList)
		}

		// 提取表属性
		if propertyList := createTableClauses.GetTableProps(); propertyList != nil {
			l.extractTableProperties(propertyList)
		}

		// 提取存储位置
		for _, locationSpec := range createTableClauses.AllLocationSpec() {
			if locationSpec.StringLit() != nil {
				l.techInfo.Locations = append(l.techInfo.Locations, locationSpec.StringLit().GetText())
			}
		}
	}
}

// extractPartitionColumns 提取分区列信息
func (l *ddlListener) extractPartitionColumns(ctx parser.IPartitionFieldListContext) {
	// 使用AllPartitionField()方法获取所有分区字段
	for _, partitionField := range ctx.AllPartitionField() {
		// 检查是否是分区列
		if partitionColumn, ok := partitionField.(*parser.PartitionColumnContext); ok {
			// 获取ColTypeContext
			colType := partitionColumn.ColType()
			// 获取列名
			if colName := colType.GetColName(); colName != nil {
				if l.techInfo.PartitionColumnNames == nil {
					l.techInfo.PartitionColumnNames = []string{}
				}
				l.techInfo.PartitionColumnNames = append(l.techInfo.PartitionColumnNames, colName.GetText())
			}
		} else if partitionTransform, ok := partitionField.(*parser.PartitionTransformContext); ok {
			// 处理分区转换
			transform := partitionTransform.Transform()
			// 处理身份转换 (如 col)
			if identityTransform, ok := transform.(*parser.IdentityTransformContext); ok {
				if qualifiedName := identityTransform.QualifiedName(); qualifiedName != nil {
					if l.techInfo.PartitionColumnNames == nil {
						l.techInfo.PartitionColumnNames = []string{}
					}
					l.techInfo.PartitionColumnNames = append(l.techInfo.PartitionColumnNames, qualifiedName.GetText())
				}
			} else if applyTransform, ok := transform.(*parser.ApplyTransformContext); ok {
				// 处理应用转换 (如 year(col))
				for _, arg := range applyTransform.AllTransformArgument() {
					if transformArg, ok := arg.(*parser.TransformArgumentContext); ok {
						if qualifiedName := transformArg.QualifiedName(); qualifiedName != nil {
							if l.techInfo.PartitionColumnNames == nil {
								l.techInfo.PartitionColumnNames = []string{}
							}
							l.techInfo.PartitionColumnNames = append(l.techInfo.PartitionColumnNames, qualifiedName.GetText())
						}
					}
				}
			}
		}
	}
}

// EnterCall 进入CALL语句时调用
func (l *lineageListener) EnterCall(ctx *parser.CallContext) {
	l.isOnlyComment = false
	l.curOpType = analyzer.StmtTypeCall

	// 提取函数名称
	var funcName string
	if ctx.IdentifierReference() != nil {
		if ctx.IdentifierReference().MultipartIdentifier() != nil {
			parts := ctx.IdentifierReference().MultipartIdentifier().AllErrorCapturingIdentifier()
			if len(parts) > 0 {
				funcName = parts[len(parts)-1].GetText()
			}
		}
	}

	// 提取函数参数中的表名信息
	for _, arg := range ctx.AllFunctionArgument() {
		// 检查参数是否是命名参数，并且名称是 "table"
		if namedArg := arg.NamedArgumentExpression(); namedArg != nil {
			if namedArg.Identifier() != nil {
				argName := namedArg.Identifier().GetText()
				if strings.ToLower(argName) == "table" {
					// 提取表名值
					if namedArg.Expression() != nil {
						// 直接获取表达式的文本，然后解析出表名
						exprText := namedArg.Expression().GetText()
						// 移除引号
						exprText = util.TrimQuotes(exprText)
						// 解析表名
						parts := strings.Split(exprText, ".")
						var cluster, database, tableName string
						if len(parts) == 3 {
							cluster = parts[0]
							database = parts[1]
							tableName = parts[2]
						} else if len(parts) == 2 {
							database = parts[0]
							tableName = parts[1]
						} else if len(parts) == 1 {
							tableName = parts[0]
							database = l.defaultDatabase
						}
						if tableName != "" {
							// 根据函数名称决定是添加读表还是写表
							if strings.ToLower(funcName) == "delete_tag" || strings.ToLower(funcName) == "set_tag" ||
								strings.ToLower(funcName) == "add_data_quality_rule" || strings.ToLower(funcName) == "remove_data_quality_rule" {
								// 这些函数是写操作
								l.addWriteTable(cluster, database, tableName)
							} else if strings.ToLower(funcName) == "table_lineage" || strings.ToLower(funcName) == "column_lineage" ||
								strings.ToLower(funcName) == "access" || strings.ToLower(funcName) == "catalog_changes" {
								// 这些函数是读操作
								l.addReadTable(cluster, database, tableName)
							}
						}
					}
				}
			}
		}
	}

	// 设置 firstOpType
	if l.firstOpType == "" {
		l.firstOpType = l.curOpType
	}
}

// EnterCreateFunction 进入创建函数语句时调用
func (l *lineageListener) EnterCreateFunction(ctx *parser.CreateFunctionContext) {
	l.isOnlyComment = false
	l.curOpType = analyzer.StmtTypeTempFunc

	// 设置 firstOpType
	if l.firstOpType == "" {
		l.firstOpType = l.curOpType
	}
}

// extractTableProperties 提取表属性信息
func (l *ddlListener) extractTableProperties(ctx parser.IPropertyListContext) {
	if ctx != nil {
		// 解析表属性
		for _, property := range ctx.AllProperty() {
			if propertyWithKeyAndEquals, ok := property.(*parser.PropertyWithKeyAndEqualsContext); ok {
				if key := propertyWithKeyAndEquals.GetKey(); key != nil {
					keyText := util.TrimQuotes(key.GetText())
					// 解析主键信息
					if keyText == "primary-key" || keyText == "primaryKey" || keyText == "hoodie.datasource.write.recordkey.field" {
						if value := propertyWithKeyAndEquals.GetValue(); value != nil {
							primaryKeyText := util.TrimQuotes(value.GetText())
							// 处理逗号分隔的多个主键
							primaryKeyColumns := strings.Split(primaryKeyText, ",")
							for _, col := range primaryKeyColumns {
								col = strings.TrimSpace(col)
								if col != "" {
									if l.techInfo.PrimaryKeyColumnNames == nil {
										l.techInfo.PrimaryKeyColumnNames = []string{}
									}
									l.techInfo.PrimaryKeyColumnNames = append(l.techInfo.PrimaryKeyColumnNames, col)
								}
							}
						}
					}
				}
			}
		}
	}
}
