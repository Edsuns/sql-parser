package tidb

import (
	"github.com/Edsuns/sql-parser/analyzer"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

// lineageVisitor 用于遍历TiDB语法树，分析血缘关系
type lineageVisitor struct {
	result          *analyzer.LineageResult
	defaultCluster  string
	defaultDatabase string
	reads           map[string]bool
	writes          map[string]bool
	isCreateView    bool   // 标记是否在CREATE VIEW语句中
	createViewName  string // CREATE VIEW的视图名
}

// ddlVisitor 用于遍历TiDB语法树，分析DDL信息
type ddlVisitor struct {
	result          *analyzer.DDLResult
	defaultCluster  string
	defaultDatabase string
	isCreateView    bool   // 标记是否在CREATE VIEW语句中
	createViewName  string // CREATE VIEW的视图名
}

// Enter 进入节点时调用
func (v *lineageVisitor) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	switch n := in.(type) {
	// SELECT语句
	case *ast.SelectStmt:
		// 只有当StmtType还未设置时，才设置为SELECT
		// 这样CREATE VIEW中的SELECT不会覆盖语句类型
		if v.result.StmtType == "" {
			v.result.StmtType = analyzer.StmtTypeSelect
		}
		// 确保当前语句类型为SELECT，以便正确识别读表
		currentStmtType := v.result.StmtType
		v.result.StmtType = analyzer.StmtTypeSelect
		// 显式处理SELECT语句的FROM子句
		if n.From != nil {
			// 处理FROM子句
			n.From.Accept(v)
		}
		// 处理WHERE子句，可能包含子查询
		if n.Where != nil {
			n.Where.Accept(v)
		}
		// 处理SELECT子句中的表达式，可能包含子查询
		if n.Fields != nil {
			n.Fields.Accept(v)
		}
		// 处理GROUP BY子句，可能包含子查询
		if n.GroupBy != nil {
			n.GroupBy.Accept(v)
		}
		// 处理HAVING子句，可能包含子查询
		if n.Having != nil {
			n.Having.Accept(v)
		}
		// 处理ORDER BY子句，可能包含子查询
		if n.OrderBy != nil {
			n.OrderBy.Accept(v)
		}
		// 恢复原始语句类型，但如果原始类型为空，保持为SELECT
		if currentStmtType != "" {
			v.result.StmtType = currentStmtType
		} else {
			v.result.StmtType = analyzer.StmtTypeSelect
		}

	// 子查询
	case *ast.SubqueryExpr:
		// 处理子查询中的表
		if n.Query != nil {
			// 临时保存当前语句类型和 isCreateView 标志
			oldStmtType := v.result.StmtType
			oldIsCreateView := v.isCreateView
			// 确保子查询中的表被识别为读表
			v.result.StmtType = analyzer.StmtTypeSelect
			v.isCreateView = false // 子查询中不应该是 CREATE VIEW 语句
			// 处理子查询
			n.Query.Accept(v)
			// 恢复语句类型和 isCreateView 标志
			v.result.StmtType = oldStmtType
			v.isCreateView = oldIsCreateView
		}

	// 模式匹配表达式（如 IN 子查询）
	case *ast.PatternInExpr:
		// 处理子查询
		if n.Sel != nil {
			n.Sel.Accept(v)
		}

	// 表引用子句 - 处理FROM子句
	case *ast.TableRefsClause:
		// 处理表引用子句中的表
		if n.TableRefs != nil {
			n.TableRefs.Accept(v)
		}

	// 二元操作表达式 - 处理WHERE子句中的IN操作等
	case *ast.BinaryOperationExpr:
		// 处理左右表达式，可能包含子查询
		if n.L != nil {
			n.L.Accept(v)
		}
		if n.R != nil {
			n.R.Accept(v)
		}

	// 字段列表 - 处理SELECT子句中的字段
	case *ast.FieldList:
		// 处理字段列表中的每个字段
		for _, field := range n.Fields {
			if field.Expr != nil {
				field.Expr.Accept(v)
			}
		}

	// INSERT语句
	case *ast.InsertStmt:
		v.result.StmtType = analyzer.StmtTypeInsert
		// INSERT语句的目标表
		if n.Table != nil {
			// 插入操作，添加写表
			if tableName := v.extractTableName(n.Table); tableName != nil {
				v.addWriteTableByName(tableName.Name, tableName.Schema)
			}
		}
		// 处理INSERT ... SELECT中的SELECT部分
		if n.Select != nil {
			// 临时将语句类型设置为SELECT，确保SELECT部分的表被识别为读表
			oldStmtType := v.result.StmtType
			v.result.StmtType = analyzer.StmtTypeSelect
			n.Select.Accept(v)
			// 恢复语句类型
			v.result.StmtType = oldStmtType
		}

	// UPDATE语句
	case *ast.UpdateStmt:
		v.result.StmtType = analyzer.StmtTypeUpdate
		// 处理UPDATE的表
		if n.TableRefs != nil {
			// 显式提取表名
			if tableName := v.extractTableName(n.TableRefs); tableName != nil {
				v.addWriteTableByName(tableName.Name, tableName.Schema)
			}
		}

	// DELETE语句
	case *ast.DeleteStmt:
		v.result.StmtType = analyzer.StmtTypeDelete
		// 处理DELETE的表
		if n.TableRefs != nil {
			// 显式提取表名
			if tableName := v.extractTableName(n.TableRefs); tableName != nil {
				v.addWriteTableByName(tableName.Name, tableName.Schema)
			}
		}

	// CREATE TABLE语句
	case *ast.CreateTableStmt:
		// 检查是否是临时表
		if n.TemporaryKeyword == ast.TemporaryGlobal || n.TemporaryKeyword == ast.TemporaryLocal {
			v.result.StmtType = analyzer.StmtTypeCreateTemporaryTable
		} else {
			v.result.StmtType = analyzer.StmtTypeCreateTable
		}
		// 添加创建的表到写表
		tableName := n.Table.Name.O
		schema := n.Table.Schema.O
		if schema == "" {
			schema = v.defaultDatabase
		}
		v.addWriteTableByName(tableName, schema)
		// 处理CREATE TABLE ... AS SELECT中的SELECT部分
		if n.Select != nil {
			// 临时将语句类型设置为SELECT，确保SELECT部分的表被识别为读表
			oldStmtType := v.result.StmtType
			v.result.StmtType = analyzer.StmtTypeSelect
			n.Select.Accept(v)
			// 恢复语句类型
			v.result.StmtType = oldStmtType
		}

	// ALTER TABLE语句
	case *ast.AlterTableStmt:
		v.result.StmtType = analyzer.StmtTypeAlterTable
		// 添加修改的表到写表
		tableName := n.Table.Name.O
		schema := n.Table.Schema.O
		if schema == "" {
			schema = v.defaultDatabase
		}
		v.addWriteTableByName(tableName, schema)

	// TRUNCATE TABLE语句
	case *ast.TruncateTableStmt:
		v.result.StmtType = analyzer.StmtTypeDropTable
		// 添加修改的表到写表
		v.addWriteTableByName(n.Table.Name.O, n.Table.Schema.O)

	// DROP TABLE语句
	case *ast.DropTableStmt:
		v.result.StmtType = analyzer.StmtTypeDropTable
		// 添加删除的表到写表
		for _, table := range n.Tables {
			tableName := table.Name.O
			schema := table.Schema.O
			if schema == "" {
				schema = v.defaultDatabase
			}
			v.addWriteTableByName(tableName, schema)
		}

	// CREATE VIEW语句
	case *ast.CreateViewStmt:
		v.result.StmtType = analyzer.StmtTypeCreateView
		v.isCreateView = true
		v.createViewName = n.ViewName.Name.O
		// 添加创建的视图到写表
		v.addWriteTableByName(n.ViewName.Name.O, n.ViewName.Schema.O)
		// 处理CREATE VIEW的SELECT部分
		if n.Select != nil {
			// 临时保存当前语句类型
			oldStmtType := v.result.StmtType
			// 确保SELECT部分的表被识别为读表
			v.result.StmtType = analyzer.StmtTypeSelect
			n.Select.Accept(v)
			// 恢复语句类型
			v.result.StmtType = oldStmtType
		}
		v.isCreateView = false // 处理完SELECT部分后重置标志

	// USE语句
	case *ast.UseStmt:
		v.result.StmtType = analyzer.StmtTypeUseDatabase
		// 提取数据库名并添加到Reads
		dbName := n.DBName
		v.result.Reads = append(v.result.Reads, &analyzer.Dependency{
			Cluster:  v.defaultCluster,
			Database: dbName,
			Table:    "",
		})

	// 表名 - 直接处理表名节点
	case *ast.TableName:
		// 根据语句类型判断是读表还是写表
		isRead := false

		// SELECT和CREATE VIEW ... AS SELECT会产生读表
		if v.result.StmtType == analyzer.StmtTypeSelect || v.result.StmtType == analyzer.StmtTypeCreateView {
			isRead = true
		}

		schema := n.Schema.O
		tableName := n.Name.O

		// 对于CREATE VIEW语句，视图名本身不应该是读表
		if isRead && !(v.isCreateView && tableName == v.createViewName) {
			v.addReadTableByName(tableName, schema)
		}

	// 表源 - 处理FROM子句中的表源
	case *ast.TableSource:
		// 递归处理表源的Source
		if n.Source != nil {
			n.Source.Accept(v)
		}

	// JOIN节点
	case *ast.Join:
		// 处理JOIN的左右节点
		if n.Left != nil {
			n.Left.Accept(v)
		}
		if n.Right != nil {
			n.Right.Accept(v)
		}
	}

	return in, false
}

// Enter 进入节点时调用
func (v *ddlVisitor) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	switch n := in.(type) {
	// SELECT语句
	case *ast.SelectStmt:
		// 只有当StmtType还未设置时，才设置为SELECT
		// 这样CREATE VIEW中的SELECT不会覆盖语句类型
		if v.result.StmtType == "" {
			v.result.StmtType = analyzer.StmtTypeSelect
		}

	// INSERT语句
	case *ast.InsertStmt:
		v.result.StmtType = analyzer.StmtTypeInsert

	// UPDATE语句
	case *ast.UpdateStmt:
		v.result.StmtType = analyzer.StmtTypeUpdate

	// DELETE语句
	case *ast.DeleteStmt:
		v.result.StmtType = analyzer.StmtTypeDelete

	// CREATE TABLE语句
	case *ast.CreateTableStmt:
		v.result.StmtType = analyzer.StmtTypeCreateTable
		// 提取表名
		tableName := n.Table.Name.O
		schema := n.Table.Schema.O
		if schema == "" {
			schema = v.defaultDatabase
		}

		// 提取列信息
		columns := []*analyzer.ActionColumn{}
		for _, col := range n.Cols {
			isNotNull := false
			for _, opt := range col.Options {
				if opt.Tp == ast.ColumnOptionNotNull {
					isNotNull = true
					break
				}
			}
			columns = append(columns, &analyzer.ActionColumn{
				Name:      col.Name.Name.O,
				Type:      col.Tp.String(),
				Action:    analyzer.ActionTypeCreate,
				IsNotNull: isNotNull,
			})
		}

		// 添加CREATE TABLE action
		action := &analyzer.ActionInfo{
			ClusterName:  v.defaultCluster,
			DatabaseName: schema,
			TableName:    tableName,
			Columns:      columns,
			ActionType:   analyzer.ActionTypeCreate,
		}
		v.result.Action = action

	// ALTER TABLE语句
	case *ast.AlterTableStmt:
		v.result.StmtType = analyzer.StmtTypeAlterTable
		// 提取表名
		tableName := n.Table.Name.O
		schema := n.Table.Schema.O
		isSpecifiedDatabase := schema != ""
		if schema == "" {
			schema = v.defaultDatabase
		}

		// 提取列信息
		columns := []*analyzer.ActionColumn{}

		// 处理ALTER TABLE操作
		for _, spec := range n.Specs {
			// 检查是否是RENAME TO操作
			if spec.NewTable != nil {
				v.result.StmtType = analyzer.StmtTypeRenameTable
				// 提取新表名
				newTableName := spec.NewTable.Name.O
				newSchema := spec.NewTable.Schema.O
				if newSchema == "" {
					newSchema = schema
				}

				// 为旧表创建DROP action
				action := &analyzer.ActionInfo{
					ClusterName:         v.defaultCluster,
					IsSpecifiedCluster:  false, // TiDB 不支持集群名
					DatabaseName:        schema,
					IsSpecifiedDatabase: isSpecifiedDatabase,
					TableName:           tableName,
					ActionType:          analyzer.ActionTypeDrop,
				}
				v.result.Action = action

				// 为新表创建CREATE action
				anotherAction := &analyzer.ActionInfo{
					ClusterName:         v.defaultCluster,
					IsSpecifiedCluster:  false, // TiDB 不支持集群名
					DatabaseName:        newSchema,
					IsSpecifiedDatabase: spec.NewTable.Schema.O != "",
					TableName:           newTableName,
					ActionType:          analyzer.ActionTypeCreate,
				}
				v.result.AnotherAction = anotherAction
			} else if spec.Tp == 9 { // CHANGE COLUMN
				// CHANGE COLUMN - 只添加一个列，标记为Alter
				if len(spec.NewColumns) > 0 {
					columns = append(columns, &analyzer.ActionColumn{
						Name:   spec.NewColumns[0].Name.Name.O,
						Type:   spec.NewColumns[0].Tp.String(),
						Action: analyzer.ActionTypeAlter,
					})
				}
			} else if spec.Tp == 8 { // MODIFY COLUMN
				// 处理MODIFY COLUMN操作
				if len(spec.NewColumns) > 0 {
					columns = append(columns, &analyzer.ActionColumn{
						Name:   spec.NewColumns[0].Name.Name.O,
						Type:   spec.NewColumns[0].Tp.String(),
						Action: analyzer.ActionTypeAlter,
					})
				}
			} else if spec.Tp == 2 { // ADD COLUMN
				// 处理ADD COLUMN操作
				for _, col := range spec.NewColumns {
					columns = append(columns, &analyzer.ActionColumn{
						Name:   col.Name.Name.O,
						Type:   col.Tp.String(),
						Action: analyzer.ActionTypeCreate,
					})
				}
			} else if spec.Tp == 10 { // RENAME COLUMN
				// 处理RENAME COLUMN操作
				// 对于RENAME COLUMN，我们需要添加两个列
				// 添加旧列，标记为删除
				columns = append(columns, &analyzer.ActionColumn{
					Name:   spec.OldColumnName.Name.O,
					Action: analyzer.ActionTypeDrop,
				})
				// 添加新列，标记为创建
				if len(spec.NewColumns) > 0 {
					columns = append(columns, &analyzer.ActionColumn{
						Name:   spec.NewColumns[0].Name.Name.O,
						Type:   spec.NewColumns[0].Tp.String(),
						Action: analyzer.ActionTypeCreate,
					})
				} else if spec.NewColumnName != nil {
					// 处理没有 NewColumns 的情况，使用 NewColumnName
					columns = append(columns, &analyzer.ActionColumn{
						Name:   spec.NewColumnName.Name.O,
						Action: analyzer.ActionTypeCreate,
					})
				}
			} else if spec.OldColumnName != nil && len(spec.NewColumns) > 0 {
				// 处理RENAME COLUMN操作（兼容旧版本）
				// 对于RENAME COLUMN，我们需要添加两个列
				// 添加旧列，标记为删除
				columns = append(columns, &analyzer.ActionColumn{
					Name:   spec.OldColumnName.Name.O,
					Action: analyzer.ActionTypeDrop,
				})
				// 添加新列，标记为创建
				columns = append(columns, &analyzer.ActionColumn{
					Name:   spec.NewColumns[0].Name.Name.O,
					Type:   spec.NewColumns[0].Tp.String(),
					Action: analyzer.ActionTypeCreate,
				})
			} else if spec.OldColumnName != nil {
				// 处理DELETE COLUMN操作
				columns = append(columns, &analyzer.ActionColumn{
					Name:   spec.OldColumnName.Name.O,
					Action: analyzer.ActionTypeDrop,
				})
			}
		}

		// 如果不是RENAME TABLE操作，添加ALTER TABLE action
		if v.result.Action == nil {
			action := &analyzer.ActionInfo{
				ClusterName:         v.defaultCluster,
				IsSpecifiedCluster:  false, // TiDB 不支持集群名
				DatabaseName:        schema,
				IsSpecifiedDatabase: isSpecifiedDatabase,
				TableName:           tableName,
				Columns:             columns,
				ActionType:          analyzer.ActionTypeAlter,
			}
			v.result.Action = action
		}

	// TRUNCATE TABLE语句
	case *ast.TruncateTableStmt:
		v.result.StmtType = analyzer.StmtTypeDropTable

	// DROP TABLE语句
	case *ast.DropTableStmt:
		v.result.StmtType = analyzer.StmtTypeDropTable
		// 添加删除的表到action
		for _, table := range n.Tables {
			tableName := table.Name.O
			schema := table.Schema.O
			isSpecifiedDatabase := schema != ""
			if schema == "" {
				schema = v.defaultDatabase
			}

			// 添加DROP TABLE action
			action := &analyzer.ActionInfo{
				ClusterName:         v.defaultCluster,
				IsSpecifiedCluster:  false, // TiDB 不支持集群名
				DatabaseName:        schema,
				IsSpecifiedDatabase: isSpecifiedDatabase,
				TableName:           tableName,
				ActionType:          analyzer.ActionTypeDrop,
			}
			v.result.Action = action
		}

	// CREATE VIEW语句
	case *ast.CreateViewStmt:
		v.result.StmtType = analyzer.StmtTypeCreateView
		v.isCreateView = true
		v.createViewName = n.ViewName.Name.O
		// 提取视图名
		viewName := n.ViewName.Name.O
		schema := n.ViewName.Schema.O
		if schema == "" {
			schema = v.defaultDatabase
		}

		// 添加CREATE VIEW action
		action := &analyzer.ActionInfo{
			ClusterName:  v.defaultCluster,
			DatabaseName: schema,
			TableName:    viewName,
			ActionType:   analyzer.ActionTypeCreate,
		}
		v.result.Action = action

	// USE语句
	case *ast.UseStmt:
		v.result.StmtType = analyzer.StmtTypeUseDatabase

	// CREATE DATABASE语句
	case *ast.CreateDatabaseStmt:
		v.result.StmtType = analyzer.StmtTypeCreateDatabase
		// 提取数据库名
		dbName := n.Name.O

		// 添加CREATE DATABASE action
		action := &analyzer.ActionInfo{
			ClusterName:         v.defaultCluster,
			IsSpecifiedCluster:  false, // TiDB 不支持集群名
			DatabaseName:        dbName,
			IsSpecifiedDatabase: true,
			TableName:           "",
			ActionType:          analyzer.ActionTypeCreate,
		}
		v.result.Action = action
	}

	return in, false
}

// Leave 离开节点时调用
func (v *lineageVisitor) Leave(in ast.Node) (out ast.Node, skipChildren bool) {
	return in, false
}

// Leave 离开节点时调用
func (v *ddlVisitor) Leave(in ast.Node) (out ast.Node, skipChildren bool) {
	return in, false
}

// extractTableName 从节点中提取表名
func (v *lineageVisitor) extractTableName(node ast.Node) *tableInfo {
	// 创建一个临时访问器来查找TableName节点
	finder := &tableNameFinder{}
	node.Accept(finder)

	if finder.tableName != nil {
		return &tableInfo{
			Name:   finder.tableName.Name.O,
			Schema: finder.tableName.Schema.O,
		}
	}

	return nil
}

// tableNameFinder 用于查找TableName节点
type tableNameFinder struct {
	tableName *ast.TableName
}

// tableInfo 表信息
type tableInfo struct {
	Name   string
	Schema string
}

// Enter 进入节点时调用
func (f *tableNameFinder) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	if tn, ok := in.(*ast.TableName); ok {
		f.tableName = tn
		return in, true // 找到后跳过子节点
	}
	return in, false
}

// Leave 离开节点时调用
func (f *tableNameFinder) Leave(in ast.Node) (out ast.Node, skipChildren bool) {
	return in, false
}

// addReadTableByName 添加读表（通过名称）
func (v *lineageVisitor) addReadTableByName(tableName, schema string) {
	cluster := v.defaultCluster
	db := v.defaultDatabase
	if schema != "" {
		db = schema
	}
	key := cluster + "." + db + "." + tableName
	if !v.reads[key] {
		v.reads[key] = true
		v.result.Reads = append(v.result.Reads, &analyzer.Dependency{
			Cluster:  cluster,
			Database: db,
			Table:    tableName,
		})
	}
}

// addWriteTableByName 添加写表（通过名称）
func (v *lineageVisitor) addWriteTableByName(tableName, schema string) {
	cluster := v.defaultCluster
	db := v.defaultDatabase
	if schema != "" {
		db = schema
	}
	key := cluster + "." + db + "." + tableName
	if !v.writes[key] {
		v.writes[key] = true
		v.result.Writes = append(v.result.Writes, &analyzer.Dependency{
			Cluster:  cluster,
			Database: db,
			Table:    tableName,
		})
	}
}
