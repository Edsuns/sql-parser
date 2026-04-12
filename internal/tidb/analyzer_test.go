package tidb

import (
	"testing"

	"github.com/Edsuns/sql-parser/analyzer"
	"github.com/stretchr/testify/assert"
)

func TestTiDBLineageAnalyzer(t *testing.T) {
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
			sql:  "SELECT * FROM table8 WHERE id = 1",
			expected: []*analyzer.LineageResult{{
				Stmt:     "SELECT * FROM table8 WHERE id = 1",
				StmtType: analyzer.StmtTypeSelect,
				Reads: []*analyzer.Dependency{
					{Cluster: "default_cluster", Database: "default_db", Table: "table8"},
				},
				Writes: []*analyzer.Dependency{},
			}},
		},
		{
			name: "SELECT with JOIN",
			sql:  "SELECT t1.id, t2.col1 FROM table1 t1 JOIN tb2 t2 ON t1.id = t2.col14",
			expected: []*analyzer.LineageResult{{
				Stmt:     "SELECT t1.id, t2.col1 FROM table1 t1 JOIN tb2 t2 ON t1.id = t2.col14",
				StmtType: analyzer.StmtTypeSelect,
				Reads: []*analyzer.Dependency{
					{Cluster: "default_cluster", Database: "default_db", Table: "table1"},
					{Cluster: "default_cluster", Database: "default_db", Table: "tb2"},
				},
				Writes: []*analyzer.Dependency{},
			}},
		},
		{
			name: "SELECT with subquery",
			sql:  "SELECT * FROM table1 WHERE id IN (SELECT col14 FROM tb2 WHERE col8 = 'v1')",
			expected: []*analyzer.LineageResult{{
				Stmt:     "SELECT * FROM table1 WHERE id IN (SELECT col14 FROM tb2 WHERE col8 = 'v1')",
				StmtType: analyzer.StmtTypeSelect,
				Reads: []*analyzer.Dependency{
					{Cluster: "default_cluster", Database: "default_db", Table: "table1"},
					{Cluster: "default_cluster", Database: "default_db", Table: "tb2"},
				},
				Writes: []*analyzer.Dependency{},
			}},
		},
		{
			name: "SELECT statement with function",
			sql:  "SELECT\n/*+ SET_VAR (group_concat_max_len = 1048576) */\nx1.col9, MAX(x1.col10) col10\n, CONCAT('{\"r\":[\"', GROUP_CONCAT(x1.col11 ORDER BY x1.col12 SEPARATOR '\"],\"w\":[\"'), '\"]}') col13\nFROM table1 x1",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "SELECT\n/*+ SET_VAR (group_concat_max_len = 1048576) */\nx1.col9, MAX(x1.col10) col10\n, CONCAT('{\"r\":[\"', GROUP_CONCAT(x1.col11 ORDER BY x1.col12 SEPARATOR '\"],\"w\":[\"'), '\"]}') col13\nFROM table1 x1",
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
			name: "INSERT statement",
			sql:  "INSERT INTO table1 (id, col1) VALUES (1, 'v3')",
			expected: []*analyzer.LineageResult{{
				Stmt:     "INSERT INTO table1 (id, col1) VALUES (1, 'v3')",
				StmtType: analyzer.StmtTypeInsert,
				Reads:    []*analyzer.Dependency{},
				Writes: []*analyzer.Dependency{
					{Cluster: "default_cluster", Database: "default_db", Table: "table1"},
				},
			}},
		},
		{
			name: "INSERT SELECT statement",
			sql:  "INSERT INTO table1 (id, col1) SELECT id, col1 FROM tb2 WHERE col8 = 'v1'",
			expected: []*analyzer.LineageResult{{
				Stmt:     "INSERT INTO table1 (id, col1) SELECT id, col1 FROM tb2 WHERE col8 = 'v1'",
				StmtType: analyzer.StmtTypeInsert,
				Reads: []*analyzer.Dependency{
					{Cluster: "default_cluster", Database: "default_db", Table: "tb2"},
				},
				Writes: []*analyzer.Dependency{
					{Cluster: "default_cluster", Database: "default_db", Table: "table1"},
				},
			}},
		},
		{
			name: "UPDATE statement",
			sql:  "UPDATE table1 SET col1 = 'v2' WHERE id = 1",
			expected: []*analyzer.LineageResult{{
				Stmt:     "UPDATE table1 SET col1 = 'v2' WHERE id = 1",
				StmtType: analyzer.StmtTypeUpdate,
				Reads:    []*analyzer.Dependency{},
				Writes: []*analyzer.Dependency{
					{Cluster: "default_cluster", Database: "default_db", Table: "table1"},
				},
			}},
		},
		{
			name: "DELETE statement",
			sql:  "DELETE FROM table1 WHERE id = 1",
			expected: []*analyzer.LineageResult{{
				Stmt:     "DELETE FROM table1 WHERE id = 1",
				StmtType: analyzer.StmtTypeDelete,
				Writes: []*analyzer.Dependency{
					{Cluster: "default_cluster", Database: "default_db", Table: "table1"},
				},
			}},
		},
		{
			name: "TRUNCATE TABLE statement",
			sql:  "TRUNCATE TABLE table1",
			expected: []*analyzer.LineageResult{{
				Stmt:     "TRUNCATE TABLE table1",
				StmtType: analyzer.StmtTypeDropTable, // TiDB将TRUNCATE映射为DROP_TABLE
				Writes: []*analyzer.Dependency{
					{Cluster: "default_cluster", Database: "default_db", Table: "table1"},
				},
			}},
		},
		{
			name: "REPLACE statement",
			sql:  "REPLACE INTO table1 (id, col1) VALUES (1, 'v4')",
			expected: []*analyzer.LineageResult{{
				Stmt:     "REPLACE INTO table1 (id, col1) VALUES (1, 'v4')",
				StmtType: analyzer.StmtTypeInsert, // TiDB将REPLACE映射为INSERT
				Writes: []*analyzer.Dependency{
					{Cluster: "default_cluster", Database: "default_db", Table: "table1"},
				},
			}},
		},
		{
			name: "CREATE TEMPORARY TABLE statement",
			sql:  "CREATE TEMPORARY TABLE tb1 (id INT, col1 VARCHAR(50))",
			expected: []*analyzer.LineageResult{{
				Stmt:     "CREATE TEMPORARY TABLE tb1 (id INT, col1 VARCHAR(50))",
				StmtType: analyzer.StmtTypeCreateTemporaryTable,
				Reads:    []*analyzer.Dependency{},
				Writes: []*analyzer.Dependency{
					{IsTemp: true, Cluster: "default_cluster", Database: "default_db", Table: "tb1"},
				},
			}},
		},
		{
			name: "CREATE TEMPORARY TABLE AS SELECT statement",
			sql:  "CREATE TEMPORARY TABLE tb1 AS SELECT id, col1 FROM table1",
			expected: []*analyzer.LineageResult{{
				Stmt:     "CREATE TEMPORARY TABLE tb1 AS SELECT id, col1 FROM table1",
				StmtType: analyzer.StmtTypeCreateTemporaryTable,
				Reads: []*analyzer.Dependency{
					{Cluster: "default_cluster", Database: "default_db", Table: "table1"},
				},
				Writes: []*analyzer.Dependency{
					{IsTemp: true, Cluster: "default_cluster", Database: "default_db", Table: "tb1"},
				},
			}},
		},
		{
			name: "read temporary table",
			sql:  "CREATE TEMPORARY TABLE tb1 AS SELECT id, col1 FROM table1; SELECT * from tb1;",
			expected: []*analyzer.LineageResult{{
				Stmt:     "CREATE TEMPORARY TABLE tb1 AS SELECT id, col1 FROM table1",
				StmtType: analyzer.StmtTypeCreateTemporaryTable,
				Reads: []*analyzer.Dependency{
					{Cluster: "default_cluster", Database: "default_db", Table: "table1"},
				},
				Writes: []*analyzer.Dependency{
					{IsTemp: true, Cluster: "default_cluster", Database: "default_db", Table: "tb1"},
				},
			}, {
				Stmt:     "SELECT * from tb1;",
				StmtType: analyzer.StmtTypeSelect,
				Reads: []*analyzer.Dependency{
					{IsTemp: true, Cluster: "default_cluster", Database: "default_db", Table: "tb1"},
				},
				Writes: []*analyzer.Dependency{},
			}},
		},
		{
			name: "Single statement with semicolon in string",
			sql:  "INSERT INTO t1 VALUES (1, 'contains ; semicolon')",
			expected: []*analyzer.LineageResult{{
				Stmt:     "INSERT INTO t1 VALUES (1, 'contains ; semicolon')",
				StmtType: analyzer.StmtTypeInsert,
				Reads:    []*analyzer.Dependency{},
				Writes: []*analyzer.Dependency{
					{Cluster: "default_cluster", Database: "default_db", Table: "t1"},
				},
			}},
		},
		{
			name: "Multiple statements with semicolons in strings",
			sql:  "INSERT INTO t1 VALUES (1, 'contains ; semicolon'); UPDATE t2 SET col='another ; semicolon' WHERE id=2",
			expected: []*analyzer.LineageResult{{
				Stmt:     "INSERT INTO t1 VALUES (1, 'contains ; semicolon')",
				StmtType: analyzer.StmtTypeInsert,
				Reads:    []*analyzer.Dependency{},
				Writes: []*analyzer.Dependency{
					{Cluster: "default_cluster", Database: "default_db", Table: "t1"},
				},
			}, {
				Stmt:     "UPDATE t2 SET col='another ; semicolon' WHERE id=2",
				StmtType: analyzer.StmtTypeUpdate,
				Reads:    []*analyzer.Dependency{},
				Writes: []*analyzer.Dependency{
					{Cluster: "default_cluster", Database: "default_db", Table: "t2"},
				},
			}},
		},
		{
			name: "Multiple statements with comments",
			sql:  "SELECT * FROM t1; -- 注释1\nINSERT INTO t2 VALUES (1, 'v3')",
			expected: []*analyzer.LineageResult{{
				Stmt:     "SELECT * FROM t1",
				StmtType: analyzer.StmtTypeSelect,
				Reads: []*analyzer.Dependency{
					{Cluster: "default_cluster", Database: "default_db", Table: "t1"},
				},
				Writes: []*analyzer.Dependency{},
			}, {
				Stmt:     "INSERT INTO t2 VALUES (1, 'v3')",
				StmtType: analyzer.StmtTypeInsert,
				Reads:    []*analyzer.Dependency{},
				Writes: []*analyzer.Dependency{
					{Cluster: "default_cluster", Database: "default_db", Table: "t2"},
				},
			}},
		},
		{
			name: "Multiple statements with comments at the end",
			sql:  "SELECT * FROM t1; -- 注释1\n INSERT INTO t2 VALUES (1, 'v3')\n  -- 注释3",
			expected: []*analyzer.LineageResult{{
				Stmt:     "SELECT * FROM t1",
				StmtType: analyzer.StmtTypeSelect,
				Reads: []*analyzer.Dependency{
					{Cluster: "default_cluster", Database: "default_db", Table: "t1"},
				},
				Writes: []*analyzer.Dependency{},
			}, {
				Stmt:     "INSERT INTO t2 VALUES (1, 'v3')",
				StmtType: analyzer.StmtTypeInsert,
				Reads:    []*analyzer.Dependency{},
				Writes: []*analyzer.Dependency{
					{Cluster: "default_cluster", Database: "default_db", Table: "t2"},
				},
			}},
		},
		{
			name: "Statement with block comment containing semicolon",
			sql:  "SELECT * FROM t1 /* ; comment with semicolon */ WHERE id=1",
			expected: []*analyzer.LineageResult{{
				Stmt:     "SELECT * FROM t1 /* ; comment with semicolon */ WHERE id=1",
				StmtType: analyzer.StmtTypeSelect,
				Reads: []*analyzer.Dependency{
					{Cluster: "default_cluster", Database: "default_db", Table: "t1"},
				},
				Writes: []*analyzer.Dependency{},
			}},
		},
		// USE语句测试
		{
			name: "USE database statement",
			sql:  "USE db3",
			expected: []*analyzer.LineageResult{{
				Stmt:     "USE db3",
				StmtType: analyzer.StmtTypeUseDatabase,
				Reads: []*analyzer.Dependency{
					{
						Cluster:  "default_cluster",
						Database: "db3",
					},
				},
				Writes: []*analyzer.Dependency{},
			}},
		},
		{
			name: "USE database with INSERT SELECT and SELECT",
			sql:  "USE db1; INSERT INTO table3 SELECT * FROM db2.tb2; SELECT * FROM table3;",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "USE db1;",
					StmtType: analyzer.StmtTypeUseDatabase,
					Reads: []*analyzer.Dependency{
						{
							Cluster:  "default_cluster",
							Database: "db1",
						},
					},
					Writes: []*analyzer.Dependency{},
				},
				{
					Stmt:     "INSERT INTO table3 SELECT * FROM db2.tb2;",
					StmtType: analyzer.StmtTypeInsert,
					Reads: []*analyzer.Dependency{
						{
							Cluster:  "default_cluster",
							Database: "db2",
							Table:    "tb2",
						},
					},
					Writes: []*analyzer.Dependency{
						{
							Cluster:  "default_cluster",
							Database: "db1",
							Table:    "table3",
						},
					},
				},
				{
					Stmt:     "SELECT * FROM table3;",
					StmtType: analyzer.StmtTypeSelect,
					Reads: []*analyzer.Dependency{
						{
							Cluster:  "default_cluster",
							Database: "db1",
							Table:    "table3",
						},
					},
					Writes: []*analyzer.Dependency{},
				},
			},
		},
	}

	tidbAnalyzer := NewSQLAnalyzer()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &analyzer.AnalyzeReq{
				DefaultCluster:  "default_cluster",
				DefaultDatabase: "default_db",
				Type:            analyzer.EngineTiDB,
				SQL:             tt.sql,
			}

			results, err := tidbAnalyzer.AnalyzeLineage(req)
			assert.NoError(t, err)
			if assert.Equal(t, len(tt.expected), len(results)) {
				for i, result := range results {
					expected := tt.expected[i]
					assert.Equal(t, expected.StmtType, result.StmtType)
					assert.Equal(t, len(expected.Reads), len(result.Reads))
					assert.Equal(t, len(expected.Writes), len(result.Writes))

					// 验证读表
					for j, readTable := range result.Reads {
						expectedRead := expected.Reads[j]
						assert.Equal(t, expectedRead.Cluster, readTable.Cluster)
						assert.Equal(t, expectedRead.Database, readTable.Database)
						assert.Equal(t, expectedRead.Table, readTable.Table)
					}

					// 验证写表
					for j, writeTable := range result.Writes {
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

func TestTiDBDDLAnalyzer(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected []*analyzer.DDLResult
	}{
		// DDL语句
		// CREATE DATABASE
		{
			name: "create database",
			sql:  "CREATE DATABASE db1;",
			expected: []*analyzer.DDLResult{
				{
					Stmt: "CREATE DATABASE db1;",
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						IsSpecifiedCluster:  false,
						DatabaseName:        "db1",
						IsSpecifiedDatabase: true,
						ActionType:          analyzer.ActionTypeCreate,
					},
					StmtType: analyzer.StmtTypeCreateDatabase,
				},
			},
		},
		{
			name: "create database with quotes",
			sql:  "CREATE DATABASE `db1`;",
			expected: []*analyzer.DDLResult{
				{
					Stmt: "CREATE DATABASE `db1`;",
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						IsSpecifiedCluster:  false,
						DatabaseName:        "db1",
						IsSpecifiedDatabase: true,
						ActionType:          analyzer.ActionTypeCreate,
					},
					StmtType: analyzer.StmtTypeCreateDatabase,
				},
			},
		},
		{
			name: "CREATE TABLE statement",
			sql:  "CREATE TABLE table4 (id INT PRIMARY KEY, col1 VARCHAR(50))",
			expected: []*analyzer.DDLResult{{
				Stmt:     "CREATE TABLE table4 (id INT PRIMARY KEY, col1 VARCHAR(50))",
				StmtType: analyzer.StmtTypeCreateTable,
				Action: &analyzer.ActionInfo{
					ClusterName: "default_cluster", DatabaseName: "default_db", TableName: "table4", ActionType: analyzer.ActionTypeCreate,
					Columns: []*analyzer.ActionColumn{
						{Name: "id", Type: "int(11)", Action: analyzer.ActionTypeCreate},
						{Name: "col1", Type: "varchar(50)", Action: analyzer.ActionTypeCreate},
					},
				},
			}},
		},
		{
			name: "CREATE VIEW",
			sql:  "CREATE VIEW view1 AS SELECT id, col1 FROM table8",
			expected: []*analyzer.DDLResult{{
				Stmt:     "CREATE VIEW view1 AS SELECT id, col1 FROM table8",
				StmtType: analyzer.StmtTypeCreateView,
				Action: &analyzer.ActionInfo{
					ClusterName:  "default_cluster",
					DatabaseName: "default_db",
					TableName:    "view1",
					ActionType:   analyzer.ActionTypeCreate,
					Columns:      []*analyzer.ActionColumn{},
				},
			}},
		},
		{
			name: "ALTER TABLE add column",
			sql:  "ALTER TABLE table1 ADD COLUMN col4 VARCHAR(100)",
			expected: []*analyzer.DDLResult{{
				Stmt:     "ALTER TABLE table1 ADD COLUMN col4 VARCHAR(100)",
				StmtType: analyzer.StmtTypeAlterTable,
				Action: &analyzer.ActionInfo{
					ClusterName:  "default_cluster",
					DatabaseName: "default_db",
					TableName:    "table1",
					ActionType:   analyzer.ActionTypeAlter,
					Columns: []*analyzer.ActionColumn{
						{Name: "col4", Type: "varchar(100)", Action: analyzer.ActionTypeCreate},
					},
				},
			}},
		},
		{
			name: "ALTER TABLE drop column",
			sql:  "ALTER TABLE table1 DROP COLUMN col4",
			expected: []*analyzer.DDLResult{{
				Stmt:     "ALTER TABLE table1 DROP COLUMN col4",
				StmtType: analyzer.StmtTypeAlterTable,
				Action: &analyzer.ActionInfo{
					ClusterName:  "default_cluster",
					DatabaseName: "default_db",
					TableName:    "table1",
					ActionType:   analyzer.ActionTypeAlter,
					Columns: []*analyzer.ActionColumn{
						{Name: "col4", Action: analyzer.ActionTypeDrop},
					},
				},
			}},
		},
		{
			name: "ALTER TABLE modify column",
			sql:  "ALTER TABLE db1.table1 MODIFY COLUMN col4 VARCHAR(200)",
			expected: []*analyzer.DDLResult{{
				Stmt:     "ALTER TABLE db1.table1 MODIFY COLUMN col4 VARCHAR(200)",
				StmtType: analyzer.StmtTypeAlterTable,
				Action: &analyzer.ActionInfo{
					ClusterName:         "default_cluster",
					DatabaseName:        "db1",
					IsSpecifiedDatabase: true,
					TableName:           "table1",
					ActionType:          analyzer.ActionTypeAlter,
					Columns: []*analyzer.ActionColumn{
						{Name: "col4", Type: "varchar(200)", Action: analyzer.ActionTypeAlter},
					},
				},
			}},
		},
		{
			name: "ALTER TABLE change column",
			sql:  "ALTER TABLE table1 CHANGE COLUMN col4 col2 VARCHAR(200)",
			expected: []*analyzer.DDLResult{{
				Stmt:     "ALTER TABLE table1 CHANGE COLUMN col4 col2 VARCHAR(200)",
				StmtType: analyzer.StmtTypeAlterTable,
				Action: &analyzer.ActionInfo{
					ClusterName:  "default_cluster",
					DatabaseName: "default_db",
					TableName:    "table1",
					ActionType:   analyzer.ActionTypeAlter,
					Columns: []*analyzer.ActionColumn{
						{Name: "col2", Type: "varchar(200)", Action: analyzer.ActionTypeAlter},
					},
				},
			}},
		},
		{
			name: "ALTER TABLE RENAME TO",
			sql:  "ALTER TABLE db1.`tb1` RENAME TO db1.`tb2`",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "ALTER TABLE db1.`tb1` RENAME TO db1.`tb2`",
					StmtType: analyzer.StmtTypeRenameTable,
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						DatabaseName:        "db1",
						IsSpecifiedDatabase: true,
						TableName:           "tb1",
						ActionType:          analyzer.ActionTypeDrop,
					},
					AnotherAction: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						DatabaseName:        "db1",
						IsSpecifiedDatabase: true,
						TableName:           "tb2",
						ActionType:          analyzer.ActionTypeCreate,
					},
				},
			},
		},
		{
			name: "ALTER TABLE RENAME COLUMN",
			sql:  "ALTER TABLE db1.`tb1` RENAME COLUMN `col6` TO `col7`",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "ALTER TABLE db1.`tb1` RENAME COLUMN `col6` TO `col7`",
					StmtType: analyzer.StmtTypeAlterTable,
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						DatabaseName:        "db1",
						IsSpecifiedDatabase: true,
						TableName:           "tb1",
						ActionType:          analyzer.ActionTypeAlter,
						Columns: []*analyzer.ActionColumn{
							{
								Name:   "col6",
								Action: analyzer.ActionTypeDrop,
							},
							{
								Name:   "col7",
								Action: analyzer.ActionTypeCreate,
							},
						},
					},
				},
			},
		},
		{
			name: "DROP TABLE if exists",
			sql:  "DROP TABLE IF EXISTS db1.table5",
			expected: []*analyzer.DDLResult{{
				Stmt:     "DROP TABLE IF EXISTS db1.table5",
				StmtType: analyzer.StmtTypeDropTable,
				Action: &analyzer.ActionInfo{
					ClusterName:         "default_cluster",
					DatabaseName:        "db1",
					IsSpecifiedDatabase: true,
					TableName:           "table5",
					ActionType:          analyzer.ActionTypeDrop,
				},
			}},
		},
		{
			name: "DROP TABLE",
			sql:  "DROP TABLE table8",
			expected: []*analyzer.DDLResult{{
				Stmt:     "DROP TABLE table8",
				StmtType: analyzer.StmtTypeDropTable,
				Action: &analyzer.ActionInfo{
					ClusterName: "default_cluster", DatabaseName: "default_db", TableName: "table8", ActionType: analyzer.ActionTypeDrop},
			}},
		},
		// USE语句测试
		{
			name: "USE database statement",
			sql:  "USE db3",
			expected: []*analyzer.DDLResult{{
				Stmt:     "USE db3",
				StmtType: analyzer.StmtTypeUseDatabase,
			}},
		},
	}

	tidbAnalyzer := NewSQLAnalyzer()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &analyzer.AnalyzeReq{
				DefaultCluster:  "default_cluster",
				DefaultDatabase: "default_db",
				Type:            analyzer.EngineTiDB,
				SQL:             tt.sql,
			}

			results, err := tidbAnalyzer.AnalyzeDDL(req)
			assert.NoError(t, err)
			if assert.Equal(t, len(tt.expected), len(results)) {
				for i, result := range results {
					expected := tt.expected[i]
					assert.Equal(t, expected.Stmt, result.Stmt)
					assert.Equal(t, expected.StmtType, result.StmtType)

					// 验证Action
					if expected.Action != nil {
						assert.NotNil(t, result.Action)
						assert.Equal(t, expected.Action.ClusterName, result.Action.ClusterName)
						assert.Equal(t, expected.Action.DatabaseName, result.Action.DatabaseName)
						assert.Equal(t, expected.Action.TableName, result.Action.TableName)
						assert.Equal(t, expected.Action.ActionType, result.Action.ActionType)
						assert.Equal(t, expected.Action.IsSpecifiedCluster, result.Action.IsSpecifiedCluster)
						assert.Equal(t, expected.Action.IsSpecifiedDatabase, result.Action.IsSpecifiedDatabase)

						// 验证Columns
						if assert.Equal(t, len(expected.Action.Columns), len(result.Action.Columns)) {
							for k, column := range result.Action.Columns {
								expectedColumn := expected.Action.Columns[k]
								assert.Equal(t, expectedColumn.Name, column.Name)
								assert.Equal(t, expectedColumn.Type, column.Type)
								assert.Equal(t, expectedColumn.IsNotNull, column.IsNotNull)
								assert.Equal(t, expectedColumn.IsPrimary, column.IsPrimary)
								assert.Equal(t, expectedColumn.DefaultValue, column.DefaultValue)
								assert.Equal(t, expectedColumn.Comment, column.Comment)
								assert.Equal(t, expectedColumn.Action, column.Action)
							}
						}

						// 验证TableInfo
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

func TestTiDBMakeCommentModification(t *testing.T) {
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
			ddl:         "CREATE TABLE table8 (id INT PRIMARY KEY, col1 VARCHAR(50) NOT NULL, col3 INT)",
			columnName:  "col1",
			comment:     "测试1",
			expected:    "ALTER TABLE `table8` MODIFY COLUMN `col1` varchar(50) NOT NULL COMMENT '测试1';",
			expectedErr: false,
		},
		{
			name:        "Modify column comment with database",
			ddl:         "CREATE TABLE db1.table8 (id INT PRIMARY KEY, col1 VARCHAR(50))",
			columnName:  "col1",
			comment:     "测试1",
			expected:    "ALTER TABLE `db1`.`table8` MODIFY COLUMN `col1` varchar(50) COMMENT '测试1';",
			expectedErr: false,
		},
		{
			name:        "Column not found",
			ddl:         "CREATE TABLE table8 (id INT PRIMARY KEY, col1 VARCHAR(50))",
			columnName:  "col15",
			comment:     "测试2",
			expected:    "",
			expectedErr: true,
		},
	}

	tidbAnalyzer := NewSQLAnalyzer()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &analyzer.MakeCommentModificationReq{
				Type:       analyzer.EngineTiDB,
				DDL:        tt.ddl,
				ColumnName: tt.columnName,
				Comment:    tt.comment,
			}

			result, err := tidbAnalyzer.MakeCommentModification(req)
			if tt.expectedErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

// 测试 Split 方法
func TestTiDBSplit(t *testing.T) {
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

	tidbAnalyzer := NewSQLAnalyzer()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &analyzer.SplitReq{
				Type: analyzer.EngineTiDB,
				SQL:  tt.sql,
			}

			result, err := tidbAnalyzer.Split(req)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}
