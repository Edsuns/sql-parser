package mysql

import (
	"testing"

	"github.com/Edsuns/sql-parser/analyzer"
	"github.com/stretchr/testify/assert"
)

func TestMySQLLineageAnalyzer(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected []*analyzer.LineageResult
	}{
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
			name: "SELECT with multiple tables and JOIN",
			sql:  "SELECT t1.id, t2.col1 FROM table1 t1 JOIN tb2 t2 ON t1.id = t2.col13 WHERE t1.col7 = 'v1'",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "SELECT t1.id, t2.col1 FROM table1 t1 JOIN tb2 t2 ON t1.id = t2.col13 WHERE t1.col7 = 'v1'",
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
			name: "SELECT with subquery",
			sql:  "SELECT * FROM table1 WHERE id IN (SELECT col13 FROM tb2 WHERE col7 = 'v1')",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "SELECT * FROM table1 WHERE id IN (SELECT col13 FROM tb2 WHERE col7 = 'v1')",
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
			name: "SELECT statement with function",
			sql:  "SELECT\n/*+ SET_VAR (group_concat_max_len = 1048576) */\nx1.col8, MAX(x1.col9) col9\n, CONCAT('{\"r\":[\"', GROUP_CONCAT(x1.col10 ORDER BY x1.col11 SEPARATOR '\"],\"w\":[\"'), '\"]}') col12\nFROM table1 x1",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "SELECT\n/*+ SET_VAR (group_concat_max_len = 1048576) */\nx1.col8, MAX(x1.col9) col9\n, CONCAT('{\"r\":[\"', GROUP_CONCAT(x1.col10 ORDER BY x1.col11 SEPARATOR '\"],\"w\":[\"'), '\"]}') col12\nFROM table1 x1",
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
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "INSERT INTO table1 (id, col1) VALUES (1, 'v3')",
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
			sql:  "INSERT INTO table1 (id, col1) SELECT id, col1 FROM tb2 WHERE col7 = 'v1'",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "INSERT INTO table1 (id, col1) SELECT id, col1 FROM tb2 WHERE col7 = 'v1'",
					StmtType: analyzer.StmtTypeInsert,
					Reads: []*analyzer.Dependency{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "tb2",
						},
					},
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
			name: "REPLACE statement",
			sql:  "REPLACE INTO table1 (id, col1) VALUES (1, 'v4')",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "REPLACE INTO table1 (id, col1) VALUES (1, 'v4')",
					StmtType: analyzer.StmtTypeReplaceTable,
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
			name: "CREATE TEMPORARY TABLE statement",
			sql:  "CREATE TEMPORARY TABLE tb1 (id INT, col1 VARCHAR(50))",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "CREATE TEMPORARY TABLE tb1 (id INT, col1 VARCHAR(50))",
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
		{
			name: "read temporary table",
			sql:  "CREATE TEMPORARY TABLE tb1 AS SELECT id, col1 FROM table1; SELECT * from tb1;",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "CREATE TEMPORARY TABLE tb1 AS SELECT id, col1 FROM table1;",
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
				{
					Stmt:     "SELECT * from tb1;",
					StmtType: analyzer.StmtTypeSelect,
					Reads: []*analyzer.Dependency{
						{
							IsTemp:   true,
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "tb1",
						},
					},
					Writes: []*analyzer.Dependency{},
				},
			},
		},
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

	mysqlAnalyzer := NewSQLAnalyzer()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := mysqlAnalyzer.AnalyzeLineage(&analyzer.AnalyzeReq{
				DefaultCluster:  "default_cluster",
				DefaultDatabase: "default_db",
				Type:            analyzer.EngineMySQL,
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

func TestMySQLDDLAnalyzer(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected []*analyzer.DDLResult
	}{
		// DDL语句
		{
			name: "CREATE TABLE statement",
			sql:  "CREATE TABLE table4 (id INT PRIMARY KEY, col1 VARCHAR(50) NOT NULL, col4 TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "CREATE TABLE table4 (id INT PRIMARY KEY, col1 VARCHAR(50) NOT NULL, col4 TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
					StmtType: analyzer.StmtTypeCreateTable,
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						IsSpecifiedCluster:  false,
						DatabaseName:        "default_db",
						IsSpecifiedDatabase: false,
						TableName:           "table4",
						ActionType:          analyzer.ActionTypeCreate,
						Columns: []*analyzer.ActionColumn{
							{Name: "id", Type: "INT", IsNotNull: false, IsPrimary: true, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
							{Name: "col1", Type: "VARCHAR(50)", IsNotNull: true, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
							{Name: "col4", Type: "TIMESTAMP", IsNotNull: false, IsPrimary: false, DefaultValue: "CURRENT_TIMESTAMP", Comment: "", Action: analyzer.ActionTypeCreate},
						},
					},
				},
			},
		},
		{
			name: "ALTER TABLE add column",
			sql:  "ALTER TABLE table1 ADD COLUMN col6 VARCHAR(100)",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "ALTER TABLE table1 ADD COLUMN col6 VARCHAR(100)",
					StmtType: analyzer.StmtTypeAlterTable,
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						IsSpecifiedCluster:  false,
						DatabaseName:        "default_db",
						IsSpecifiedDatabase: false,
						TableName:           "table1",
						Columns: []*analyzer.ActionColumn{
							{
								Name:   "col6",
								Type:   "VARCHAR(100)",
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
			sql:  "ALTER TABLE table1 ADD COLUMN (col2 VARCHAR(100), col3 INT)",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "ALTER TABLE table1 ADD COLUMN (col2 VARCHAR(100), col3 INT)",
					StmtType: analyzer.StmtTypeAlterTable,
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						IsSpecifiedCluster:  false,
						DatabaseName:        "default_db",
						IsSpecifiedDatabase: false,
						TableName:           "table1",
						Columns: []*analyzer.ActionColumn{
							{
								Name:   "col2",
								Type:   "VARCHAR(100)",
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
			name: "ALTER TABLE drop column",
			sql:  "ALTER TABLE table1 DROP COLUMN col2",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "ALTER TABLE table1 DROP COLUMN col2",
					StmtType: analyzer.StmtTypeAlterTable,
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						IsSpecifiedCluster:  false,
						DatabaseName:        "default_db",
						IsSpecifiedDatabase: false,
						TableName:           "table1",
						Columns: []*analyzer.ActionColumn{
							{
								Name:   "col2",
								Action: analyzer.ActionTypeDrop,
							},
						},
						ActionType: analyzer.ActionTypeAlter,
					},
				},
			},
		},
		{
			name: "ALTER TABLE modify column",
			sql:  "ALTER TABLE table1 MODIFY COLUMN col2 VARCHAR(200)",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "ALTER TABLE table1 MODIFY COLUMN col2 VARCHAR(200)",
					StmtType: analyzer.StmtTypeAlterTable,
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						IsSpecifiedCluster:  false,
						DatabaseName:        "default_db",
						IsSpecifiedDatabase: false,
						TableName:           "table1",
						Columns: []*analyzer.ActionColumn{
							{
								Name:   "col2",
								Type:   "VARCHAR(200)",
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
			sql:  "CREATE TABLE table6 (id INT NOT NULL COMMENT '测试4''s 标识1''' PRIMARY KEY, col1 VARCHAR(50) NOT NULL) COMMENT '这是测试5，存放测试6''s 测试7';",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "CREATE TABLE table6 (id INT NOT NULL COMMENT '测试4''s 标识1''' PRIMARY KEY, col1 VARCHAR(50) NOT NULL) COMMENT '这是测试5，存放测试6''s 测试7';",
					StmtType: analyzer.StmtTypeCreateTable,
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						IsSpecifiedCluster:  false,
						DatabaseName:        "default_db",
						IsSpecifiedDatabase: false,
						TableName:           "table6",
						ActionType:          analyzer.ActionTypeCreate,
						Columns: []*analyzer.ActionColumn{
							{Name: "id", Type: "INT", IsNotNull: true, IsPrimary: true, DefaultValue: "", Comment: "测试4''s 标识1''", Action: analyzer.ActionTypeCreate},
							{Name: "col1", Type: "VARCHAR(50)", IsNotNull: true, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
						},
					},
				},
			},
		},
		{
			name: "ALTER TABLE change column",
			sql:  "ALTER TABLE table1 CHANGE COLUMN col2 col5 VARCHAR(200)",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "ALTER TABLE table1 CHANGE COLUMN col2 col5 VARCHAR(200)",
					StmtType: analyzer.StmtTypeAlterTable,
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						IsSpecifiedCluster:  false,
						DatabaseName:        "default_db",
						IsSpecifiedDatabase: false,
						TableName:           "table1",
						Columns: []*analyzer.ActionColumn{
							{
								Name:   "col2",
								Type:   "VARCHAR(200)",
								Action: analyzer.ActionTypeAlter,
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
						ClusterName:         "default_cluster",
						IsSpecifiedCluster:  false,
						DatabaseName:        "default_db",
						IsSpecifiedDatabase: false,
						TableName:           "table5",
						ActionType:          analyzer.ActionTypeDrop,
					},
				},
			},
		},
	}

	mysqlAnalyzer := NewSQLAnalyzer()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &analyzer.AnalyzeReq{
				DefaultCluster:  "default_cluster",
				DefaultDatabase: "default_db",
				Type:            analyzer.EngineMySQL,
				SQL:             tt.sql,
			}

			results, err := mysqlAnalyzer.AnalyzeDDL(req)
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

func TestMySQLMakeCommentModification(t *testing.T) {
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
			expected:    "ALTER TABLE `table8` MODIFY COLUMN `col1` VARCHAR(50) NOT NULL COMMENT '测试1';",
			expectedErr: false,
		},
		{
			name:        "Modify column comment with default value",
			ddl:         "CREATE TABLE table8 (id INT PRIMARY KEY, col1 VARCHAR(50) NOT NULL, col4 TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
			columnName:  "col4",
			comment:     "测试3",
			expected:    "ALTER TABLE `table8` MODIFY COLUMN `col4` TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '测试3';",
			expectedErr: false,
		},
		{
			name:        "Modify column comment with database",
			ddl:         "CREATE TABLE db1.table8 (id INT PRIMARY KEY, col1 VARCHAR(50))",
			columnName:  "col1",
			comment:     "测试1",
			expected:    "ALTER TABLE `db1`.`table8` MODIFY COLUMN `col1` VARCHAR(50) COMMENT '测试1';",
			expectedErr: false,
		},
		{
			name:        "Column not found",
			ddl:         "CREATE TABLE table8 (id INT PRIMARY KEY, col1 VARCHAR(50))",
			columnName:  "col14",
			comment:     "测试2",
			expected:    "",
			expectedErr: true,
		},
	}

	mysqlAnalyzer := NewSQLAnalyzer()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &analyzer.MakeCommentModificationReq{
				Type:       analyzer.EngineMySQL,
				DDL:        tt.ddl,
				ColumnName: tt.columnName,
				Comment:    tt.comment,
			}

			result, err := mysqlAnalyzer.MakeCommentModification(req)
			if tt.expectedErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestMySQLSplit(t *testing.T) {
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

	mysqlAnalyzer := NewSQLAnalyzer()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &analyzer.SplitReq{
				Type: analyzer.EngineMySQL,
				SQL:  tt.sql,
			}

			result, err := mysqlAnalyzer.Split(req)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}
