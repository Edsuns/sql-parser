package spark

import (
	"fmt"

	"github.com/Edsuns/sql-parser/analyzer"
	"github.com/Edsuns/sql-parser/internal/spark/parser"
	"github.com/antlr4-go/antlr/v4"
)

// dependencyListener 自定义监听器，用于提取SQL语句中的读写表信息和注释
type dependencyListener struct {
	*parser.BaseSqlBaseParserListener

	dependencies    *analyzer.DependencyResult
	defaultCluster  string
	defaultDatabase string
	curOpType       analyzer.StmtType     // 当前操作类型：SELECT, INSERT, UPDATE, DELETE, MERGE, CREATE_TABLE, CREATE_VIEW, ALTER_TABLE, REPLACE_TABLE, DROP_TABLE, CREATE_LIKE
	firstOpType     analyzer.StmtType     // 第一个操作类型，根据规则：第一个写入表的OpType，若没有写入则取第一个读取表的OpType
	comments        []string              // 存储解析到的注释
	isOnlyComment   bool                  // 标记当前SQL是否只包含注释
	isWriteOp       bool                  // 是否已遇到写入操作
	cteNames        map[string]bool       // 存储CTE名称，避免将CTE作为表依赖
	currentTable    *analyzer.ActionTable // 当前正在处理的表
}

// newDependencyListener 创建新的监听器实例
func newDependencyListener(defaultCluster, defaultDatabase string) *dependencyListener {
	return &dependencyListener{
		dependencies: &analyzer.DependencyResult{
			Read:    []*analyzer.DependencyTable{},
			Write:   []*analyzer.DependencyTable{},
			Actions: []*analyzer.ActionTable{},
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

// EnterCtes 进入CTE列表时调用
func (l *dependencyListener) EnterCtes(ctx *parser.CtesContext) {
	l.onReadStmt()
}

// EnterNamedQuery 进入单个CTE查询时调用
func (l *dependencyListener) EnterNamedQuery(ctx *parser.NamedQueryContext) {
	l.onReadStmt()
	// 记录CTE名称，避免将其作为表依赖
	if ctx.GetName() != nil {
		cteName := ctx.GetName().GetText()
		l.cteNames[cteName] = true
	}
}

// EnterRegularQuerySpecification 进入常规查询规范时调用，这是SELECT语句的主要部分
func (l *dependencyListener) EnterRegularQuerySpecification(ctx *parser.RegularQuerySpecificationContext) {
	l.curOpType = analyzer.StmtTypeSelect
	l.isOnlyComment = false
	// 如果还没有遇到写入操作，设置第一个操作类型
	if !l.isWriteOp && l.firstOpType == "" {
		l.firstOpType = analyzer.StmtTypeSelect
	}
}

// EnterSubquery 进入子查询时调用
func (l *dependencyListener) EnterSubquery(ctx *parser.SubqueryContext) {
	l.onReadStmt()
}

// EnterAliasedRelation 进入别名关系表达式时调用，这是表引用的常见上下文
func (l *dependencyListener) EnterAliasedRelation(ctx *parser.AliasedRelationContext) {
	l.onReadStmt()
}

// EnterQuery 进入查询语句时调用（SELECT），通常是读操作
func (l *dependencyListener) EnterQuery(ctx *parser.QueryContext) {
	l.onReadStmt()
}

// EnterStatement 进入语句时调用
func (l *dependencyListener) EnterStatement(ctx *parser.SingleStatementContext) {
	l.isOnlyComment = false
}

// EnterSingleStatement 进入单条语句时调用
func (l *dependencyListener) EnterSingleStatement(ctx *parser.SingleStatementContext) {
	l.isOnlyComment = false
}

// EnterCompoundStatement 进入复合语句时调用
func (l *dependencyListener) EnterCompoundStatement(ctx *parser.CompoundStatementContext) {
	l.isOnlyComment = false
}

// EnterSingleInsertQuery 进入单条插入语句时调用
func (l *dependencyListener) EnterSingleInsertQuery(ctx *parser.SingleInsertQueryContext) {
	l.curOpType = analyzer.StmtTypeInsert
	l.onWriteStmt()
}

// EnterMultiInsertQuery 进入多条插入语句时调用
func (l *dependencyListener) EnterMultiInsertQuery(ctx *parser.MultiInsertQueryContext) {
	l.curOpType = analyzer.StmtTypeInsert
	l.onWriteStmt()
}

// EnterInsertInto 进入INSERT INTO语句时调用
func (l *dependencyListener) EnterInsertInto(ctx *parser.InsertIntoContext) {
	l.curOpType = analyzer.StmtTypeInsert
	l.onWriteStmt()
}

// EnterDeleteFromTable 进入删除语句时调用
func (l *dependencyListener) EnterDeleteFromTable(ctx *parser.DeleteFromTableContext) {
	l.curOpType = analyzer.StmtTypeDelete
	l.onWriteStmt()
}

// EnterTruncateTable 进入清空表语句时调用
func (l *dependencyListener) EnterTruncateTable(c *parser.TruncateTableContext) {
	l.curOpType = analyzer.StmtTypeTruncate
	l.onWriteStmt()
}

// EnterUpdateTable 进入更新语句时调用
func (l *dependencyListener) EnterUpdateTable(ctx *parser.UpdateTableContext) {
	l.curOpType = analyzer.StmtTypeUpdate
	l.onWriteStmt()
}

// EnterMergeIntoTable 进入合并语句时调用
func (l *dependencyListener) EnterMergeIntoTable(ctx *parser.MergeIntoTableContext) {
	l.curOpType = analyzer.StmtTypeMerge
	l.onWriteStmt()
}

// EnterCreateView 进入创建视图语句时调用
func (l *dependencyListener) EnterCreateView(ctx *parser.CreateViewContext) {
	l.curOpType = analyzer.StmtTypeCreateView
	l.onWriteStmt()
}

// EnterAlterTableAlterColumn 进入修改表列语句时调用
func (l *dependencyListener) EnterAlterTableAlterColumn(ctx *parser.AlterTableAlterColumnContext) {
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
func (l *dependencyListener) EnterAddTableColumns(ctx *parser.AddTableColumnsContext) {
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
			if l.currentTable != nil && ctx.GetColumns() != nil {
				for _, colType := range ctx.GetColumns().AllQualifiedColTypeWithPosition() {
					// 提取列名
					columnName := ""
					if colType.GetName() != nil {
						if len(colType.GetName().AllErrorCapturingIdentifier()) > 0 {
							columnName = colType.GetName().AllErrorCapturingIdentifier()[0].GetText()
						}
					}

					// 提取列类型
					columnType := ""
					if colType.DataType() != nil {
						columnType = colType.DataType().GetText()
					}

					// 创建ActionColumn并添加到当前表
					actionColumn := &analyzer.ActionColumn{
						Name:         columnName,
						Type:         columnType,
						IsNotNull:    false,
						IsPrimary:    false,
						DefaultValue: "",
						Comment:      "",
						Action:       analyzer.ActionTypeAlter,
					}

					l.currentTable.Columns = append(l.currentTable.Columns, actionColumn)
				}
			}
		}
	}
}

// ensureActionTable 确保ActionTable存在，如果不存在则创建
func (l *dependencyListener) ensureActionTable(cluster, database, tableName string, action analyzer.ActionType) {
	// 设置默认值
	if cluster == "" {
		cluster = l.defaultCluster
	}
	if database == "" {
		database = l.defaultDatabase
	}

	// 检查是否已存在相同的ActionTable
	for _, existingTable := range l.dependencies.Actions {
		if existingTable.Cluster == cluster && existingTable.Database == database && existingTable.Table == tableName {
			l.currentTable = existingTable
			return
		}
	}

	// 创建新的ActionTable
	newTable := &analyzer.ActionTable{
		Cluster:  cluster,
		Database: database,
		Table:    tableName,
		Columns:  []*analyzer.ActionColumn{},
		Action:   action,
	}
	l.dependencies.Actions = append(l.dependencies.Actions, newTable)
	l.currentTable = newTable
}

// EnterRenameTableColumn 进入重命名表列语句时调用
func (l *dependencyListener) EnterRenameTableColumn(ctx *parser.RenameTableColumnContext) {
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
		// 提取新列名作为操作的列名
		if ctx.GetTo() != nil {
			columnName := ctx.GetTo().GetText()

			actionColumn := &analyzer.ActionColumn{
				Name:         columnName,
				Type:         "", // 重命名操作不需要类型
				IsNotNull:    false,
				IsPrimary:    false,
				DefaultValue: "",
				Comment:      "",
				Action:       analyzer.ActionTypeAlter,
			}

			l.currentTable.Columns = append(l.currentTable.Columns, actionColumn)
		}
	}
}

// EnterDropTableColumns 进入删除表列语句时调用
func (l *dependencyListener) EnterDropTableColumns(ctx *parser.DropTableColumnsContext) {
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
func (l *dependencyListener) EnterRenameTable(ctx *parser.RenameTableContext) {
	l.curOpType = analyzer.StmtTypeAlterTable
	l.onWriteStmt()

	// 提取表名信息
	if ctx.GetFrom() != nil {
		if ctx.GetFrom().MultipartIdentifier() != nil {
			parts := ctx.GetFrom().MultipartIdentifier().AllErrorCapturingIdentifier()
			cluster, database, tableName := l.extractTableInfo(parts)

			// 创建或获取ActionTable
			l.ensureActionTable(cluster, database, tableName, analyzer.ActionTypeAlter)
		}
	}
}

// EnterSetTableProperties 进入设置表属性语句时调用
func (l *dependencyListener) EnterSetTableProperties(ctx *parser.SetTablePropertiesContext) {
	l.curOpType = analyzer.StmtTypeAlterTable
	l.onWriteStmt()
}

// EnterUnsetTableProperties 进入取消设置表属性语句时调用
func (l *dependencyListener) EnterUnsetTableProperties(ctx *parser.UnsetTablePropertiesContext) {
	l.curOpType = analyzer.StmtTypeAlterTable
	l.onWriteStmt()
}

// EnterHiveChangeColumn 进入Hive修改列语句时调用
func (l *dependencyListener) EnterHiveChangeColumn(ctx *parser.HiveChangeColumnContext) {
	l.curOpType = analyzer.StmtTypeAlterTable
	l.onWriteStmt()
}

// EnterHiveReplaceColumns 进入Hive替换列语句时调用
func (l *dependencyListener) EnterHiveReplaceColumns(ctx *parser.HiveReplaceColumnsContext) {
	l.curOpType = analyzer.StmtTypeAlterTable
	l.onWriteStmt()
}

// EnterSetTableSerDe 进入设置表SerDe语句时调用
func (l *dependencyListener) EnterSetTableSerDe(ctx *parser.SetTableSerDeContext) {
	l.curOpType = analyzer.StmtTypeAlterTable
	l.onWriteStmt()
}

// EnterAddTablePartition 进入添加表分区语句时调用
func (l *dependencyListener) EnterAddTablePartition(ctx *parser.AddTablePartitionContext) {
	l.curOpType = analyzer.StmtTypeAlterTable
	l.onWriteStmt()
}

// EnterRenameTablePartition 进入重命名表分区语句时调用
func (l *dependencyListener) EnterRenameTablePartition(ctx *parser.RenameTablePartitionContext) {
	l.curOpType = analyzer.StmtTypeAlterTable
	l.onWriteStmt()
}

// EnterDropTablePartitions 进入删除表分区语句时调用
func (l *dependencyListener) EnterDropTablePartitions(ctx *parser.DropTablePartitionsContext) {
	l.curOpType = analyzer.StmtTypeAlterTable
	l.onWriteStmt()
}

// EnterSetTableLocation 进入设置表位置语句时调用
func (l *dependencyListener) EnterSetTableLocation(ctx *parser.SetTableLocationContext) {
	l.curOpType = analyzer.StmtTypeAlterTable
	l.onWriteStmt()
}

// EnterRecoverPartitions 进入恢复分区语句时调用
func (l *dependencyListener) EnterRecoverPartitions(ctx *parser.RecoverPartitionsContext) {
	l.curOpType = analyzer.StmtTypeAlterTable
	l.onWriteStmt()
}

// EnterAlterClusterBy 进入修改聚类语句时调用
func (l *dependencyListener) EnterAlterClusterBy(ctx *parser.AlterClusterByContext) {
	l.curOpType = analyzer.StmtTypeAlterTable
	l.onWriteStmt()
}

// EnterAlterTableCollation 进入修改表排序规则语句时调用
func (l *dependencyListener) EnterAlterTableCollation(ctx *parser.AlterTableCollationContext) {
	l.curOpType = analyzer.StmtTypeAlterTable
	l.onWriteStmt()
}

// EnterAddTableConstraint 进入添加表约束语句时调用
func (l *dependencyListener) EnterAddTableConstraint(ctx *parser.AddTableConstraintContext) {
	l.curOpType = analyzer.StmtTypeAlterTable
	l.onWriteStmt()
}

// EnterDropTableConstraint 进入删除表约束语句时调用
func (l *dependencyListener) EnterDropTableConstraint(ctx *parser.DropTableConstraintContext) {
	l.curOpType = analyzer.StmtTypeAlterTable
	l.onWriteStmt()
}

// EnterDropTable 进入删除表语句时调用
func (l *dependencyListener) EnterDropTable(ctx *parser.DropTableContext) {
	l.curOpType = analyzer.StmtTypeDropTable
	l.onWriteStmt()

	// 提取表名信息
	if ctx.IdentifierReference() != nil {
		if ctx.IdentifierReference().MultipartIdentifier() != nil {
			parts := ctx.IdentifierReference().MultipartIdentifier().AllErrorCapturingIdentifier()
			cluster, database, tableName := l.extractTableInfo(parts)

			// 设置默认值
			if cluster == "" {
				cluster = l.defaultCluster
			}
			if database == "" {
				database = l.defaultDatabase
			}

			// 创建ActionTable
			actionTable := &analyzer.ActionTable{
				Cluster:  cluster,
				Database: database,
				Table:    tableName,
				Columns:  []*analyzer.ActionColumn{},
				Action:   analyzer.ActionTypeDrop,
			}
			l.dependencies.Actions = append(l.dependencies.Actions, actionTable)
		}
	}
}

// EnterCreateTable 进入创建表语句时调用
func (l *dependencyListener) EnterCreateTable(ctx *parser.CreateTableContext) {
	l.curOpType = analyzer.StmtTypeCreateTable
	l.onWriteStmt()

	// 提取表名信息
	if ctx.CreateTableHeader() != nil {
		if ctx.CreateTableHeader().IdentifierReference() != nil {
			if ctx.CreateTableHeader().IdentifierReference().MultipartIdentifier() != nil {
				parts := ctx.CreateTableHeader().IdentifierReference().MultipartIdentifier().AllErrorCapturingIdentifier()
				cluster, database, tableName := l.extractTableInfo(parts)

				// 设置默认值
				if cluster == "" {
					cluster = l.defaultCluster
				}
				if database == "" {
					database = l.defaultDatabase
				}

				// 创建ActionTable
				l.currentTable = &analyzer.ActionTable{
					Cluster:  cluster,
					Database: database,
					Table:    tableName,
					Columns:  []*analyzer.ActionColumn{},
					Action:   analyzer.ActionTypeCreate,
				}
				l.dependencies.Actions = append(l.dependencies.Actions, l.currentTable)
			}
		}
	}
}

// EnterCreateTableLike 进入创建表（LIKE）语句时调用
func (l *dependencyListener) EnterCreateTableLike(ctx *parser.CreateTableLikeContext) {
	l.curOpType = analyzer.StmtTypeCreateLike
	l.onWriteStmt()
}

// EnterReplaceTable 进入替换表语句时调用
func (l *dependencyListener) EnterReplaceTable(ctx *parser.ReplaceTableContext) {
	l.curOpType = analyzer.StmtTypeReplaceTable
	l.onWriteStmt()
}

// EnterComment 进入注释语句时调用
func (l *dependencyListener) EnterComment(ctx *parser.CommentContext) {
	// 记录注释内容
	commentText := ctx.GetText()
	l.comments = append(l.comments, commentText)
}

// EnterUse 进入USE语句时调用，处理USE database形式
func (l *dependencyListener) EnterUse(ctx *parser.UseContext) {
	l.curOpType = analyzer.StmtTypeUseDatabase
	l.isOnlyComment = false
	if l.firstOpType == "" {
		l.firstOpType = analyzer.StmtTypeUseDatabase
	}
}

// EnterUseNamespace 进入USE语句时调用，处理USE namespace database形式（如USE CATALOG db）
func (l *dependencyListener) EnterUseNamespace(ctx *parser.UseNamespaceContext) {
	l.curOpType = analyzer.StmtTypeUseCatalog
	l.isOnlyComment = false
	if l.firstOpType == "" {
		l.firstOpType = analyzer.StmtTypeUseCatalog
	}
}

// EnterIdentifierReference 进入标识符引用时调用，用于提取数据库名和表名
func (l *dependencyListener) EnterIdentifierReference(ctx *parser.IdentifierReferenceContext) {
	// USE语句不应该添加表依赖
	if l.curOpType == analyzer.StmtTypeUseDatabase || l.curOpType == analyzer.StmtTypeUseCatalog {
		return
	}

	if ctx.MultipartIdentifier() != nil {
		parts := ctx.MultipartIdentifier().AllErrorCapturingIdentifier()
		if len(parts) > 0 {
			cluster, database, tableName := l.extractTableInfo(parts)
			// 检查是否是CTE名称，如果是则跳过，CTE不是实际的表依赖
			if _, isCTE := l.cteNames[tableName]; isCTE {
				return
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
func (l *dependencyListener) extractTableInfo(parts []parser.IErrorCapturingIdentifierContext) (cluster, database, table string) {
	if len(parts) == 0 {
		return
	}

	// 根据parts长度确定层次：cluster.db.table 或 db.table 或 table
	switch len(parts) {
	case 1:
		// 只有表名
		table = parts[0].GetText()
	case 2:
		// 数据库名和表名
		database = parts[0].GetText()
		table = parts[1].GetText()
	default:
		// 集群名、数据库名和表名
		cluster = parts[0].GetText()
		database = parts[1].GetText()
		table = parts[2].GetText()
	}

	return
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
	if l.firstOpType == "" {
		l.firstOpType = l.curOpType
	}
}

func (l *dependencyListener) onReadStmt() {
	l.curOpType = analyzer.StmtTypeSelect
	// 如果还没有遇到写入操作，设置第一个操作类型
	if !l.isWriteOp && l.firstOpType == "" {
		l.firstOpType = analyzer.StmtTypeSelect
	}
}

// EnterColDefinition 进入列定义时调用
func (l *dependencyListener) EnterColDefinition(ctx *parser.ColDefinitionContext) {
	if l.currentTable == nil {
		return
	}

	// 提取列名
	columnName := ""
	if ctx.GetColName() != nil {
		columnName = ctx.GetColName().GetText()
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
			comment = option.CommentSpec().GetText()
		} else if option.ColumnConstraintDefinition() != nil {
			// 检查PRIMARY KEY约束
			// 简化处理：如果有ColumnConstraintDefinition，假设是PRIMARY KEY
			// 实际情况可能更复杂，需要检查具体约束类型
			isPrimary = true
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
