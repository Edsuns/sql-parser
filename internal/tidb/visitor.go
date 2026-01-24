package tidb

import (
	"github.com/Edsuns/sql-parser/analyzer"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

// dependencyVisitor 用于遍历TiDB语法树，分析依赖
type dependencyVisitor struct {
	deps            *analyzer.DependencyResult
	defaultCluster  string
	defaultDatabase string
	readTables      map[string]bool
	writeTables     map[string]bool
	isCreateView    bool   // 标记是否在CREATE VIEW语句中
	createViewName  string // CREATE VIEW的视图名
}

// Enter 进入节点时调用
func (v *dependencyVisitor) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	switch n := in.(type) {
	// SELECT语句
	case *ast.SelectStmt:
		// 只有当StmtType还未设置时，才设置为SELECT
		// 这样CREATE VIEW中的SELECT不会覆盖语句类型
		if v.deps.StmtType == "" {
			v.deps.StmtType = analyzer.StmtTypeSelect
		}
		// 显式处理SELECT语句的FROM子句
		if n.From != nil {
			// 处理FROM子句
			n.From.Accept(v)
		}

	// INSERT语句
	case *ast.InsertStmt:
		v.deps.StmtType = analyzer.StmtTypeInsert
		// INSERT语句的目标表
		if n.Table != nil {
			// 插入操作，添加写表
			if tableName := v.extractTableName(n.Table); tableName != nil {
				v.addWriteTableByName(tableName.Name, tableName.Schema)
			}
		}
		// 处理INSERT ... SELECT中的SELECT部分
		if n.Select != nil {
			n.Select.Accept(v)
		}

	// UPDATE语句
	case *ast.UpdateStmt:
		v.deps.StmtType = analyzer.StmtTypeUpdate
		// 处理UPDATE的表
		if n.TableRefs != nil {
			// 显式提取表名
			if tableName := v.extractTableName(n.TableRefs); tableName != nil {
				v.addWriteTableByName(tableName.Name, tableName.Schema)
			}
		}

	// DELETE语句
	case *ast.DeleteStmt:
		v.deps.StmtType = analyzer.StmtTypeDelete
		// 处理DELETE的表
		if n.TableRefs != nil {
			// 显式提取表名
			if tableName := v.extractTableName(n.TableRefs); tableName != nil {
				v.addWriteTableByName(tableName.Name, tableName.Schema)
			}
		}

	// CREATE TABLE语句
	case *ast.CreateTableStmt:
		v.deps.StmtType = analyzer.StmtTypeCreateTable
		// 添加创建的表到写表
		v.addWriteTableByName(n.Table.Name.O, n.Table.Schema.O)

	// ALTER TABLE语句
	case *ast.AlterTableStmt:
		v.deps.StmtType = analyzer.StmtTypeAlterTable
		// 添加修改的表到写表
		v.addWriteTableByName(n.Table.Name.O, n.Table.Schema.O)

	// TRUNCATE TABLE语句
	case *ast.TruncateTableStmt:
		v.deps.StmtType = analyzer.StmtTypeDropTable
		// 添加修改的表到写表
		v.addWriteTableByName(n.Table.Name.O, n.Table.Schema.O)

	// DROP TABLE语句
	case *ast.DropTableStmt:
		v.deps.StmtType = analyzer.StmtTypeDropTable
		// 添加删除的表到写表
		for _, table := range n.Tables {
			v.addWriteTableByName(table.Name.O, table.Schema.O)
		}

	// CREATE VIEW语句
	case *ast.CreateViewStmt:
		v.deps.StmtType = analyzer.StmtTypeCreateView
		v.isCreateView = true
		v.createViewName = n.ViewName.Name.O
		// 添加创建的视图到写表
		v.addWriteTableByName(n.ViewName.Name.O, n.ViewName.Schema.O)
		// 处理CREATE VIEW的SELECT部分
		if n.Select != nil {
			n.Select.Accept(v)
		}

	// USE语句
	case *ast.UseStmt:
		v.deps.StmtType = analyzer.StmtTypeUseDatabase

	// 表名 - 直接处理表名节点
	case *ast.TableName:
		// 根据语句类型判断是读表还是写表
		// 只有SELECT和CREATE VIEW ... AS SELECT会产生读表
		isRead := v.deps.StmtType == analyzer.StmtTypeSelect || v.deps.StmtType == analyzer.StmtTypeCreateView

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

// Leave 离开节点时调用
func (v *dependencyVisitor) Leave(in ast.Node) (out ast.Node, skipChildren bool) {
	return in, false
}

// extractTableName 从节点中提取表名
func (v *dependencyVisitor) extractTableName(node ast.Node) *tableInfo {
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
func (v *dependencyVisitor) addReadTableByName(tableName, schema string) {
	cluster := v.defaultCluster
	db := v.defaultDatabase
	if schema != "" {
		db = schema
	}
	key := cluster + "." + db + "." + tableName
	if !v.readTables[key] {
		v.readTables[key] = true
		v.deps.Read = append(v.deps.Read, &analyzer.DependencyTable{
			Cluster:  cluster,
			Database: db,
			Table:    tableName,
		})
	}
}

// addWriteTableByName 添加写表（通过名称）
func (v *dependencyVisitor) addWriteTableByName(tableName, schema string) {
	cluster := v.defaultCluster
	db := v.defaultDatabase
	if schema != "" {
		db = schema
	}
	key := cluster + "." + db + "." + tableName
	if !v.writeTables[key] {
		v.writeTables[key] = true
		v.deps.Write = append(v.deps.Write, &analyzer.DependencyTable{
			Cluster:  cluster,
			Database: db,
			Table:    tableName,
		})
	}
}
