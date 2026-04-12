package hive

import (
	"testing"

	"github.com/Edsuns/sql-parser/analyzer"
	"github.com/stretchr/testify/assert"
)

func TestHiveLineageAnalyzer(t *testing.T) {
	// Hive官方文档SQL示例测试用例
	tests := []struct {
		name     string
		sql      string
		expected []*analyzer.LineageResult
	}{
		{
			name: "USE statement",
			sql:  "USE db1",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "USE db1",
					StmtType: analyzer.StmtTypeUseDatabase,
					Reads: []*analyzer.Dependency{
						{
							Cluster:  "default_cluster",
							Database: "db1",
						},
					},
					Writes: []*analyzer.Dependency{},
				},
			},
		},
		// 基本查询语句
		{
			name: "SELECT statement",
			sql:  "SELECT * FROM table1 WHERE id = 1",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "SELECT * FROM table1 WHERE id = 1",
					StmtType: analyzer.StmtTypeSelect,
					Reads: []*analyzer.Dependency{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "table1",
						},
					},
					Writes: []*analyzer.Dependency{},
				},
			},
		},
		{
			name: "SELECT statement lower case",
			sql:  "SELECT * from table1 WHERE id = 1",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "SELECT * from table1 WHERE id = 1",
					StmtType: analyzer.StmtTypeSelect,
					Reads: []*analyzer.Dependency{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "table1",
						},
					},
					Writes: []*analyzer.Dependency{},
				},
			},
		},
		{
			name: "SELECT with JOIN",
			sql:  "SELECT t1.id, t2.col1 FROM table1 t1 JOIN tb2 t2 ON t1.id = t2.col12",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "SELECT t1.id, t2.col1 FROM table1 t1 JOIN tb2 t2 ON t1.id = t2.col12",
					StmtType: analyzer.StmtTypeSelect,
					Reads: []*analyzer.Dependency{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "table1",
						},
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "tb2",
						},
					},
					Writes: []*analyzer.Dependency{},
				},
			},
		},
		{
			name: "SELECT with database specified",
			sql:  "SELECT * FROM db1.table1 WHERE id = 1",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "SELECT * FROM db1.table1 WHERE id = 1",
					StmtType: analyzer.StmtTypeSelect,
					Reads: []*analyzer.Dependency{
						{
							Cluster:  "default_cluster",
							Database: "db1",
							Table:    "table1",
						},
					},
					Writes: []*analyzer.Dependency{},
				},
			},
		},
		{
			name: "SELECT statement with function",
			sql:  "SELECT x2.col7, MAX(x2.col8) col8, CONCAT('v3', x2.col9) col11 FROM table1 x2",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "SELECT x2.col7, MAX(x2.col8) col8, CONCAT('v3', x2.col9) col11 FROM table1 x2",
					StmtType: analyzer.StmtTypeSelect,
					Reads: []*analyzer.Dependency{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "table1",
						},
					},
					Writes: []*analyzer.Dependency{},
				},
			},
		},
		// 数据修改语句
		{
			name: "INSERT statement with VALUES",
			sql:  "INSERT INTO table1 (id, col1) VALUES (1, 'v3'), (2, 'v4')",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "INSERT INTO table1 (id, col1) VALUES (1, 'v3'), (2, 'v4')",
					StmtType: analyzer.StmtTypeInsert,
					Reads:    []*analyzer.Dependency{},
					Writes: []*analyzer.Dependency{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "table1",
						},
					},
				},
			},
		},
		{
			name: "INSERT SELECT statement",
			sql:  "INSERT INTO tb2 SELECT id, col1 FROM table1 WHERE col6 = 'v1'",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "INSERT INTO tb2 SELECT id, col1 FROM table1 WHERE col6 = 'v1'",
					StmtType: analyzer.StmtTypeInsert,
					Reads: []*analyzer.Dependency{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "table1",
						},
					},
					Writes: []*analyzer.Dependency{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "tb2",
						},
					},
				},
			},
		},
		{
			name: "UPDATE statement",
			sql:  "UPDATE table1 SET col1 = 'v2' WHERE id = 1",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "UPDATE table1 SET col1 = 'v2' WHERE id = 1",
					StmtType: analyzer.StmtTypeUpdate,
					Reads:    []*analyzer.Dependency{},
					Writes: []*analyzer.Dependency{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "table1",
						},
					},
				},
			},
		},
		{
			name: "DELETE statement",
			sql:  "DELETE FROM table1 WHERE id = 1",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "DELETE FROM table1 WHERE id = 1",
					StmtType: analyzer.StmtTypeDelete,
					Reads:    []*analyzer.Dependency{},
					Writes: []*analyzer.Dependency{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "table1",
						},
					},
				},
			},
		},
		{
			name: "TRUNCATE TABLE statement",
			sql:  "TRUNCATE TABLE table1",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "TRUNCATE TABLE table1",
					StmtType: analyzer.StmtTypeTruncateTable,
					Reads:    []*analyzer.Dependency{},
					Writes: []*analyzer.Dependency{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "table1",
						},
					},
				},
			},
		},
		{
			name: "CREATE VIEW statement",
			sql:  "CREATE VIEW view1 AS SELECT id, col1 FROM table1 WHERE col6 = 'v1'",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "CREATE VIEW view1 AS SELECT id, col1 FROM table1 WHERE col6 = 'v1'",
					StmtType: analyzer.StmtTypeCreateView,
					Reads: []*analyzer.Dependency{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "table1",
						},
					},
					Writes: []*analyzer.Dependency{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "view1",
						},
					},
				},
			},
		},
		{
			name: "CREATE TEMPORARY TABLE statement",
			sql:  "CREATE TEMPORARY TABLE tb1 (id INT, col1 STRING)",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "CREATE TEMPORARY TABLE tb1 (id INT, col1 STRING)",
					StmtType: analyzer.StmtTypeCreateTemporaryTable,
					Reads:    []*analyzer.Dependency{},
					Writes: []*analyzer.Dependency{
						{
							IsTemp:   true,
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "tb1",
						},
					},
				},
			},
		},
		{
			name: "CREATE TEMPORARY TABLE AS SELECT statement",
			sql:  "CREATE TEMPORARY TABLE tb1 AS SELECT id, col1 FROM table1",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "CREATE TEMPORARY TABLE tb1 AS SELECT id, col1 FROM table1",
					StmtType: analyzer.StmtTypeCreateTemporaryTable,
					Reads: []*analyzer.Dependency{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "table1",
						},
					},
					Writes: []*analyzer.Dependency{
						{
							IsTemp:   true,
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "tb1",
						},
					},
				},
			},
		},
		// {
		// 	name: "read temporary table",
		// 	sql:  "CREATE TEMPORARY TABLE tb1 AS SELECT id, col1 FROM table1; SELECT * from tb1;",
		// 	expected: []*analyzer.LineageResult{
		// 		{
		// 			Stmt:     "CREATE TEMPORARY TABLE tb1 AS SELECT id, col1 FROM table1;",
		// 			StmtType: analyzer.StmtTypeCreateTemporaryTable,
		// 			Reads: []*analyzer.Dependency{
		// 				{
		// 					Cluster:  "default_cluster",
		// 					Database: "default_db",
		// 					Table:    "table1",
		// 				},
		// 			},
		// 			Writes: []*analyzer.Dependency{
		// 				{
		// 					IsTemp:   true,
		// 					Cluster:  "default_cluster",
		// 					Database: "default_db",
		// 					Table:    "tb1",
		// 				},
		// 			},
		// 		},
		// 		{
		// 			Stmt:     "SELECT * from tb1;",
		// 			StmtType: analyzer.StmtTypeSelect,
		// 			Reads: []*analyzer.Dependency{
		// 				{
		// 					IsTemp:   true,
		// 					Cluster:  "default_cluster",
		// 					Database: "default_db",
		// 					Table:    "tb1",
		// 				},
		// 			},
		// 			Writes: []*analyzer.Dependency{},
		// 		},
		// 	},
		// },
		{
			name: "DROP VIEW statement",
			sql:  "DROP VIEW IF EXISTS view1",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "DROP VIEW IF EXISTS view1",
					StmtType: analyzer.StmtTypeDropTable,
					Reads:    []*analyzer.Dependency{},
					Writes: []*analyzer.Dependency{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "view1",
						},
					},
				},
			},
		},
		{
			name: "ALTER TABLE rename table",
			sql:  "ALTER TABLE table1 RENAME TO tb2",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "ALTER TABLE table1 RENAME TO tb2",
					StmtType: analyzer.StmtTypeAlterTable,
					Reads:    []*analyzer.Dependency{},
					Writes: []*analyzer.Dependency{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "table1",
						},
					},
				},
			},
		},
		// 复杂查询
		{
			name: "SELECT with subquery",
			sql:  "SELECT * FROM (SELECT id, col1 FROM table1) x1 WHERE x1.id > 10",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "SELECT * FROM (SELECT id, col1 FROM table1) x1 WHERE x1.id > 10",
					StmtType: analyzer.StmtTypeSelect,
					Reads: []*analyzer.Dependency{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "table1",
						},
					},
					Writes: []*analyzer.Dependency{},
				},
			},
		},
		{
			name: "SELECT with CTE",
			sql:  "WITH cte1 AS (SELECT id, col1 FROM table1) SELECT * FROM cte1 WHERE id > 10",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "WITH cte1 AS (SELECT id, col1 FROM table1) SELECT * FROM cte1 WHERE id > 10",
					StmtType: analyzer.StmtTypeSelect,
					Reads: []*analyzer.Dependency{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "table1",
						},
					},
					Writes: []*analyzer.Dependency{},
				},
			},
		},
		{
			name: "USE statement#01",
			sql:  "USE db1",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "USE db1",
					StmtType: analyzer.StmtTypeUseDatabase,
					Reads: []*analyzer.Dependency{
						{
							Cluster:  "default_cluster",
							Database: "db1",
						},
					},
					Writes: []*analyzer.Dependency{},
				},
			},
		},
		// {
		// 	name: "USE database with INSERT SELECT and SELECT",
		// 	sql:  "USE report; INSERT INTO target_table SELECT * FROM ods.source_table; SELECT * FROM target_table;",
		// 	expected: []*analyzer.LineageResult{
		// 		{
		// 			Stmt:     "USE report",
		// 			StmtType: analyzer.StmtTypeUseDatabase,
		// 			Reads: []*analyzer.Dependency{
		// 				{
		// 					Cluster:  "default_cluster",
		// 					Database: "report",
		// 				},
		// 			},
		// 			Writes: []*analyzer.Dependency{},
		// 		},
		// 		{
		// 			Stmt:     "INSERT INTO target_table SELECT * FROM ods.source_table;",
		// 			StmtType: analyzer.StmtTypeInsert,
		// 			Reads: []*analyzer.Dependency{
		// 				{
		// 					Cluster:  "default_cluster",
		// 					Database: "ods",
		// 					Table:    "source_table",
		// 				},
		// 			},
		// 			Writes: []*analyzer.Dependency{
		// 				{
		// 					Cluster:  "default_cluster",
		// 					Database: "report",
		// 					Table:    "target_table",
		// 				},
		// 			},
		// 		},
		// 		{
		// 			Stmt:     "SELECT * FROM target_table;",
		// 			StmtType: analyzer.StmtTypeSelect,
		// 			Reads: []*analyzer.Dependency{
		// 				{
		// 					Cluster:  "default_cluster",
		// 					Database: "report",
		// 					Table:    "target_table",
		// 				},
		// 			},
		// 			Writes: []*analyzer.Dependency{},
		// 		},
		// 	},
		// },
	}

	hiveAnalyzer := NewSQLAnalyzer()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := hiveAnalyzer.AnalyzeLineage(&analyzer.AnalyzeReq{
				DefaultCluster:  "default_cluster",
				DefaultDatabase: "default_db",
				Type:            analyzer.EngineHive,
				SQL:             tt.sql,
			})
			assert.NoError(t, err)
			if assert.Equal(t, len(tt.expected), len(result)) {
				for i, r := range result {
					expected := tt.expected[i]
					assert.Equal(t, expected.Stmt, r.Stmt)
					assert.Equal(t, expected.StmtType, r.StmtType)
					assert.Equal(t, len(expected.Reads), len(r.Reads))
					assert.Equal(t, len(expected.Writes), len(r.Writes))

					// 验证读表
					for j, readTable := range r.Reads {
						expectedRead := expected.Reads[j]
						assert.Equal(t, expectedRead.Cluster, readTable.Cluster)
						assert.Equal(t, expectedRead.Database, readTable.Database)
						assert.Equal(t, expectedRead.Table, readTable.Table)
					}

					// 验证写表
					for j, writeTable := range r.Writes {
						expectedWrite := expected.Writes[j]
						assert.Equal(t, expectedWrite.Cluster, writeTable.Cluster)
						assert.Equal(t, expectedWrite.Database, writeTable.Database)
						assert.Equal(t, expectedWrite.Table, writeTable.Table)
					}
				}
			}
		})
	}
}

func TestHiveDDLAnalyzer(t *testing.T) {
	// Hive官方文档SQL示例测试用例
	tests := []struct {
		name     string
		sql      string
		expected []*analyzer.DDLResult
	}{
		// DDL语句
		{
			name: "CREATE TABLE statement",
			sql:  "CREATE TABLE table4 (id INT, col1 STRING) STORED AS PARQUET",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "CREATE TABLE table4 (id INT, col1 STRING) STORED AS PARQUET",
					StmtType: analyzer.StmtTypeCreateTable,
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						IsSpecifiedCluster:  false,
						DatabaseName:        "default_db",
						IsSpecifiedDatabase: false,
						TableName:           "table4",
						Columns: []*analyzer.ActionColumn{
							{Name: "id", Type: "INT", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
							{Name: "col1", Type: "STRING", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
						},
						ActionType: analyzer.ActionTypeCreate,
					},
				},
			},
		},
		{
			name: "CREATE TABLE with external location",
			sql:  "CREATE EXTERNAL TABLE table7 (id INT, col1 STRING) LOCATION '/path1/path2/table7'",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "CREATE EXTERNAL TABLE table7 (id INT, col1 STRING) LOCATION '/path1/path2/table7'",
					StmtType: analyzer.StmtTypeCreateTable,
					Action: &analyzer.ActionInfo{
						ClusterName:  "default_cluster",
						DatabaseName: "default_db",
						TableName:    "table7",
						Columns: []*analyzer.ActionColumn{
							{Name: "id", Type: "INT", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
							{Name: "col1", Type: "STRING", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
						},
						ActionType: analyzer.ActionTypeCreate,
					},
				},
			},
		},
		{
			name: "CREATE VIEW AS SELECT statement",
			sql:  "CREATE VIEW view1 AS SELECT id, col1 FROM table1 WHERE col6 = 'v1'",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "CREATE VIEW view1 AS SELECT id, col1 FROM table1 WHERE col6 = 'v1'",
					StmtType: analyzer.StmtTypeCreateView,
					Action: &analyzer.ActionInfo{
						ClusterName:  "default_cluster",
						DatabaseName: "default_db",
						TableName:    "view1",
						ActionType:   analyzer.ActionTypeCreate,
					},
				},
			},
		},
		{
			name: "ALTER TABLE add column",
			sql:  "ALTER TABLE table1 ADD COLUMNS (col2 STRING)",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "ALTER TABLE table1 ADD COLUMNS (col2 STRING)",
					StmtType: analyzer.StmtTypeAlterTable,
					Action: &analyzer.ActionInfo{
						ClusterName:  "default_cluster",
						DatabaseName: "default_db",
						TableName:    "table1",
						Columns: []*analyzer.ActionColumn{
							{
								Name:   "col2",
								Type:   "STRING",
								Action: analyzer.ActionTypeCreate,
							},
						},
						ActionType: analyzer.ActionTypeAlter,
					},
				},
			},
		},
		{
			name: "ALTER TABLE add multiple columns",
			sql:  "ALTER TABLE table1 ADD COLUMNS (col2 STRING, col3 INT)",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "ALTER TABLE table1 ADD COLUMNS (col2 STRING, col3 INT)",
					StmtType: analyzer.StmtTypeAlterTable,
					Action: &analyzer.ActionInfo{
						ClusterName:  "default_cluster",
						DatabaseName: "default_db",
						TableName:    "table1",
						Columns: []*analyzer.ActionColumn{
							{
								Name:   "col2",
								Type:   "STRING",
								Action: analyzer.ActionTypeCreate,
							},
							{
								Name:   "col3",
								Type:   "INT",
								Action: analyzer.ActionTypeCreate,
							},
						},
						ActionType: analyzer.ActionTypeAlter,
					},
				},
			},
		},
		{
			name: "DROP TABLE statement",
			sql:  "DROP TABLE IF EXISTS table5",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "DROP TABLE IF EXISTS table5",
					StmtType: analyzer.StmtTypeDropTable,
					Action: &analyzer.ActionInfo{
						ClusterName:  "default_cluster",
						DatabaseName: "default_db",
						TableName:    "table5",
						ActionType:   analyzer.ActionTypeDrop,
					},
				},
			},
		},
		{
			name: "ALTER TABLE rename column",
			sql:  "ALTER TABLE table1 CHANGE COLUMN col4 col5 STRING",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "ALTER TABLE table1 CHANGE COLUMN col4 col5 STRING",
					StmtType: analyzer.StmtTypeAlterTable,
					Action: &analyzer.ActionInfo{
						ClusterName:  "default_cluster",
						DatabaseName: "default_db",
						TableName:    "table1",
						Columns: []*analyzer.ActionColumn{
							{
								Name:   "col4",
								Type:   "STRING",
								Action: analyzer.ActionTypeAlter,
							},
						},
						ActionType: analyzer.ActionTypeAlter,
					},
				},
			},
		},
		{
			name: "CREATE TABLE with single quotes in comments",
			sql:  "CREATE TABLE table6 (id INT COMMENT '测试4''s 标识1''', col1 STRING)",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "CREATE TABLE table6 (id INT COMMENT '测试4''s 标识1''', col1 STRING)",
					StmtType: analyzer.StmtTypeCreateTable,
					Action: &analyzer.ActionInfo{
						ClusterName:  "default_cluster",
						DatabaseName: "default_db",
						TableName:    "table6",
						Columns: []*analyzer.ActionColumn{
							{Name: "id", Type: "INT", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "测试4''s 标识1''", Action: analyzer.ActionTypeCreate},
							{Name: "col1", Type: "STRING", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
						},
						ActionType: analyzer.ActionTypeCreate,
					},
				},
			},
		},
	}

	hiveAnalyzer := NewSQLAnalyzer()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results, err := hiveAnalyzer.AnalyzeDDL(&analyzer.AnalyzeReq{
				DefaultCluster:  "default_cluster",
				DefaultDatabase: "default_db",
				Type:            analyzer.EngineHive,
				SQL:             tt.sql,
			})
			assert.NoError(t, err)
			if assert.Equal(t, len(tt.expected), len(results)) {
				for i, result := range results {
					expected := tt.expected[i]
					assert.Equal(t, expected.Stmt, result.Stmt)
					assert.Equal(t, expected.StmtType, result.StmtType)

					// 验证Action
					if expected.Action != nil {
						assert.NotNil(t, result.Action)
						expectedAction := expected.Action
						assert.Equal(t, expectedAction.ClusterName, result.Action.ClusterName)
						assert.Equal(t, expectedAction.IsSpecifiedCluster, result.Action.IsSpecifiedCluster)
						assert.Equal(t, expectedAction.DatabaseName, result.Action.DatabaseName)
						assert.Equal(t, expectedAction.IsSpecifiedDatabase, result.Action.IsSpecifiedDatabase)
						assert.Equal(t, expectedAction.TableName, result.Action.TableName)
						assert.Equal(t, expectedAction.ActionType, result.Action.ActionType)

						// 验证Columns
						if assert.Equal(t, len(expectedAction.Columns), len(result.Action.Columns)) {
							for k, column := range result.Action.Columns {
								expectedColumn := expectedAction.Columns[k]
								assert.Equal(t, expectedColumn.Name, column.Name)
								assert.Equal(t, expectedColumn.Type, column.Type)
								assert.Equal(t, expectedColumn.IsNotNull, column.IsNotNull)
								assert.Equal(t, expectedColumn.IsPrimary, column.IsPrimary)
								assert.Equal(t, expectedColumn.DefaultValue, column.DefaultValue)
								assert.Equal(t, expectedColumn.Comment, column.Comment)
								assert.Equal(t, expectedColumn.Action, column.Action)
							}
						}

						// 验证Action
						if expected.Action.TableInfo != nil {
							assert.NotNil(t, result.Action.TableInfo)
							assert.Equal(t, expected.Action.TableInfo.PartitionColumnNames, result.Action.TableInfo.PartitionColumnNames)
							assert.Equal(t, expected.Action.TableInfo.PrimaryKeyColumnNames, result.Action.TableInfo.PrimaryKeyColumnNames)
							assert.Equal(t, expected.Action.TableInfo.Locations, result.Action.TableInfo.Locations)
							assert.Equal(t, expected.Action.TableInfo.LakehouseTableFormat, result.Action.TableInfo.LakehouseTableFormat)
						} else {
							assert.Nil(t, result.Action.TableInfo)
						}
					} else {
						assert.Nil(t, result.Action)
					}

					// 验证AnotherAction
					if expected.AnotherAction != nil {
						assert.NotNil(t, result.AnotherAction)
						if expected.AnotherAction.TableInfo != nil {
							assert.NotNil(t, result.AnotherAction.TableInfo)
							assert.Equal(t, expected.AnotherAction.TableInfo.PartitionColumnNames, result.AnotherAction.TableInfo.PartitionColumnNames)
							assert.Equal(t, expected.AnotherAction.TableInfo.PrimaryKeyColumnNames, result.AnotherAction.TableInfo.PrimaryKeyColumnNames)
							assert.Equal(t, expected.AnotherAction.TableInfo.Locations, result.AnotherAction.TableInfo.Locations)
							assert.Equal(t, expected.AnotherAction.TableInfo.LakehouseTableFormat, result.AnotherAction.TableInfo.LakehouseTableFormat)
						} else {
							assert.Nil(t, result.AnotherAction.TableInfo)
						}
					} else {
						assert.Nil(t, result.AnotherAction)
					}
				}
			}
		})
	}
}

// TestHiveDependencyAnalyzerSyntaxError 测试 Hive 解析器的语法错误情况
func TestHiveDependencyAnalyzerSyntaxError(t *testing.T) {
	// 语法错误测试用例
	tests := []struct {
		name string
		sql  string
	}{}

	hiveAnalyzer := NewSQLAnalyzer()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := hiveAnalyzer.AnalyzeLineage(&analyzer.AnalyzeReq{
				DefaultCluster:  "default_cluster",
				DefaultDatabase: "default_db",
				Type:            analyzer.EngineHive,
				SQL:             tt.sql,
			})
			// 验证解析器返回错误
			assert.Error(t, err)
			// 验证没有返回结果
			assert.Nil(t, result)
		})
	}
}

func TestHiveMakeCommentModification(t *testing.T) {
	tests := []struct {
		name        string
		ddl         string
		columnName  string
		comment     string
		expected    string
		expectedErr bool
	}{
		{
			name:        "Modify column comment",
			ddl:         "CREATE TABLE table8 (id INT PRIMARY KEY, col1 STRING NOT NULL, col3 INT)",
			columnName:  "col1",
			comment:     "测试1",
			expected:    "ALTER TABLE table8 CHANGE COLUMN col1 col1 STRING COMMENT '测试1';",
			expectedErr: false,
		},
		{
			name:        "Modify column comment with database",
			ddl:         "CREATE TABLE db1.table8 (id INT PRIMARY KEY, col1 STRING)",
			columnName:  "col1",
			comment:     "测试1",
			expected:    "ALTER TABLE db1.table8 CHANGE COLUMN col1 col1 STRING COMMENT '测试1';",
			expectedErr: false,
		},
		{
			name:        "Column not found",
			ddl:         "CREATE TABLE table8 (id INT PRIMARY KEY, col1 STRING)",
			columnName:  "col13",
			comment:     "测试2",
			expected:    "",
			expectedErr: true,
		},
	}

	hiveAnalyzer := NewSQLAnalyzer()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &analyzer.MakeCommentModificationReq{
				Type:       analyzer.EngineHive,
				DDL:        tt.ddl,
				ColumnName: tt.columnName,
				Comment:    tt.comment,
			}

			result, err := hiveAnalyzer.MakeCommentModification(req)
			if tt.expectedErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestHiveSplit(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected []string
	}{
		{
			name:     "Single statement",
			sql:      "SELECT * FROM table1",
			expected: []string{"SELECT * FROM table1"},
		},
		{
			name:     "Multiple statements",
			sql:      "SELECT * FROM table1; SELECT * FROM table2",
			expected: []string{"SELECT * FROM table1;", "SELECT * FROM table2"},
		},
		{
			name:     "Multiple statements with semicolons",
			sql:      "SELECT * FROM table1; SELECT * FROM table2;",
			expected: []string{"SELECT * FROM table1;", "SELECT * FROM table2;"},
		},
		{
			name:     "Statements with comments",
			sql:      "-- This is a comment\nSELECT * FROM table1; -- Another comment\nSELECT * FROM table2",
			expected: []string{"-- This is a comment\nSELECT * FROM table1;", "-- Another comment\nSELECT * FROM table2"},
		},
		{
			name:     "Statements with semicolons in strings",
			sql:      "INSERT INTO table1 (name) VALUES ('test;test'); SELECT * FROM table2 WHERE name = 'a;b;c';",
			expected: []string{"INSERT INTO table1 (name) VALUES ('test;test');", "SELECT * FROM table2 WHERE name = 'a;b;c';"},
		},
	}

	hiveAnalyzer := NewSQLAnalyzer()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &analyzer.SplitReq{
				Type: analyzer.EngineHive,
				SQL:  tt.sql,
			}

			result, err := hiveAnalyzer.Split(req)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}
