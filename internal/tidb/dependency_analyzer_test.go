package tidb

import (
	"testing"

	"github.com/Edsuns/sql-parser/analyzer"
	"github.com/stretchr/testify/assert"
)

func TestTiDBDependencyAnalyzer(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected []*analyzer.DependencyResult
	}{
		// 基本查询语句
		{
			name: "SELECT statement",
			sql:  "SELECT * FROM test_table WHERE id = 1",
			expected: []*analyzer.DependencyResult{{
				StmtType: analyzer.StmtTypeSelect,
				Read: []*analyzer.DependencyTable{
					{Cluster: "default_cluster", Database: "default_db", Table: "test_table"},
				},
				Write: []*analyzer.DependencyTable{},
			}},
		},
		{
			name: "SELECT with JOIN",
			sql:  "SELECT t1.id, t2.name FROM table1 t1 JOIN table2 t2 ON t1.id = t2.table1_id",
			expected: []*analyzer.DependencyResult{{
				StmtType: analyzer.StmtTypeSelect,
				Read: []*analyzer.DependencyTable{
					{Cluster: "default_cluster", Database: "default_db", Table: "table1"},
					{Cluster: "default_cluster", Database: "default_db", Table: "table2"},
				},
				Write: []*analyzer.DependencyTable{},
			}},
		},
		{
			name: "SELECT with subquery",
			sql:  "SELECT * FROM table1 WHERE id IN (SELECT table1_id FROM table2 WHERE status = 'active')",
			expected: []*analyzer.DependencyResult{{
				StmtType: analyzer.StmtTypeSelect,
				Read: []*analyzer.DependencyTable{
					{Cluster: "default_cluster", Database: "default_db", Table: "table1"},
				},
				Write: []*analyzer.DependencyTable{},
			}},
		},
		// 数据修改语句
		{
			name: "INSERT statement",
			sql:  "INSERT INTO table1 (id, name) VALUES (1, 'test')",
			expected: []*analyzer.DependencyResult{{
				StmtType: analyzer.StmtTypeInsert,
				Read:     []*analyzer.DependencyTable{},
				Write: []*analyzer.DependencyTable{
					{Cluster: "default_cluster", Database: "default_db", Table: "table1"},
				},
			}},
		},
		{
			name: "INSERT SELECT statement",
			sql:  "INSERT INTO table1 (id, name) SELECT id, name FROM table2 WHERE status = 'active'",
			expected: []*analyzer.DependencyResult{{
				StmtType: analyzer.StmtTypeInsert,
				Read:     []*analyzer.DependencyTable{},
				Write: []*analyzer.DependencyTable{
					{Cluster: "default_cluster", Database: "default_db", Table: "table1"},
				},
			}},
		},
		{
			name: "UPDATE statement",
			sql:  "UPDATE table1 SET name = 'new' WHERE id = 1",
			expected: []*analyzer.DependencyResult{{
				StmtType: analyzer.StmtTypeUpdate,
				Read:     []*analyzer.DependencyTable{},
				Write: []*analyzer.DependencyTable{
					{Cluster: "default_cluster", Database: "default_db", Table: "table1"},
				},
			}},
		},
		{
			name: "DELETE statement",
			sql:  "DELETE FROM table1 WHERE id = 1",
			expected: []*analyzer.DependencyResult{{
				StmtType: analyzer.StmtTypeDelete,
				Read:     []*analyzer.DependencyTable{},
				Write: []*analyzer.DependencyTable{
					{Cluster: "default_cluster", Database: "default_db", Table: "table1"},
				},
			}},
		},
		// DDL语句
		{
			name: "CREATE TABLE statement",
			sql:  "CREATE TABLE new_table (id INT PRIMARY KEY, name VARCHAR(50))",
			expected: []*analyzer.DependencyResult{{
				StmtType: analyzer.StmtTypeCreateTable,
				Read:     []*analyzer.DependencyTable{},
				Write: []*analyzer.DependencyTable{
					{Cluster: "default_cluster", Database: "default_db", Table: "new_table"},
				},
				Actions: []*analyzer.ActionTable{
					{Cluster: "default_cluster", Database: "default_db", Table: "new_table", Action: analyzer.ActionTypeCreate},
				},
			}},
		},
		{
			name: "ALTER TABLE add column",
			sql:  "ALTER TABLE table1 ADD COLUMN new_column VARCHAR(100)",
			expected: []*analyzer.DependencyResult{{
				StmtType: analyzer.StmtTypeAlterTable,
				Read:     []*analyzer.DependencyTable{},
				Write: []*analyzer.DependencyTable{
					{Cluster: "default_cluster", Database: "default_db", Table: "table1"},
				},
				Actions: []*analyzer.ActionTable{
					{
						Cluster:  "default_cluster",
						Database: "default_db",
						Table:    "table1",
						Action:   analyzer.ActionTypeAlter,
						Columns: []*analyzer.ActionColumn{
							{Name: "new_column", Type: "VARCHAR(100)", Action: analyzer.ActionTypeCreate},
						},
					},
				},
			}},
		},
		{
			name: "ALTER TABLE drop column",
			sql:  "ALTER TABLE table1 DROP COLUMN new_column",
			expected: []*analyzer.DependencyResult{{
				StmtType: analyzer.StmtTypeAlterTable,
				Read:     []*analyzer.DependencyTable{},
				Write: []*analyzer.DependencyTable{
					{Cluster: "default_cluster", Database: "default_db", Table: "table1"},
				},
				Actions: []*analyzer.ActionTable{
					{
						Cluster:  "default_cluster",
						Database: "default_db",
						Table:    "table1",
						Action:   analyzer.ActionTypeAlter,
						Columns: []*analyzer.ActionColumn{
							{Name: "new_column", Action: analyzer.ActionTypeDrop},
						},
					},
				},
			}},
		},
		{
			name: "ALTER TABLE modify column",
			sql:  "ALTER TABLE table1 MODIFY COLUMN new_column VARCHAR(200)",
			expected: []*analyzer.DependencyResult{{
				StmtType: analyzer.StmtTypeAlterTable,
				Read:     []*analyzer.DependencyTable{},
				Write: []*analyzer.DependencyTable{
					{Cluster: "default_cluster", Database: "default_db", Table: "table1"},
				},
				Actions: []*analyzer.ActionTable{
					{
						Cluster:  "default_cluster",
						Database: "default_db",
						Table:    "table1",
						Action:   analyzer.ActionTypeAlter,
						Columns: []*analyzer.ActionColumn{
							{Name: "new_column", Type: "VARCHAR(200)", Action: analyzer.ActionTypeAlter},
						},
					},
				},
			}},
		},
		{
			name: "ALTER TABLE change column",
			sql:  "ALTER TABLE table1 CHANGE COLUMN new_column email VARCHAR(200)",
			expected: []*analyzer.DependencyResult{{
				StmtType: analyzer.StmtTypeAlterTable,
				Read:     []*analyzer.DependencyTable{},
				Write: []*analyzer.DependencyTable{
					{Cluster: "default_cluster", Database: "default_db", Table: "table1"},
				},
				Actions: []*analyzer.ActionTable{
					{
						Cluster:  "default_cluster",
						Database: "default_db",
						Table:    "table1",
						Action:   analyzer.ActionTypeAlter,
						Columns: []*analyzer.ActionColumn{
							{Name: "email", Type: "VARCHAR(200)", Action: analyzer.ActionTypeAlter},
						},
					},
				},
			}},
		},
		{
			name: "DROP TABLE statement",
			sql:  "DROP TABLE IF EXISTS old_table",
			expected: []*analyzer.DependencyResult{{
				StmtType: analyzer.StmtTypeDropTable,
				Read:     []*analyzer.DependencyTable{},
				Write: []*analyzer.DependencyTable{
					{Cluster: "default_cluster", Database: "default_db", Table: "old_table"},
				},
				Actions: []*analyzer.ActionTable{
					{Cluster: "default_cluster", Database: "default_db", Table: "old_table", Action: analyzer.ActionTypeDrop},
				},
			}},
		},
		{
			name: "DROP TABLE",
			sql:  "DROP TABLE test_table",
			expected: []*analyzer.DependencyResult{{
				StmtType: analyzer.StmtTypeDropTable,
				Read:     []*analyzer.DependencyTable{},
				Write: []*analyzer.DependencyTable{
					{Cluster: "default_cluster", Database: "default_db", Table: "test_table"},
				},
				Actions: []*analyzer.ActionTable{
					{Cluster: "default_cluster", Database: "default_db", Table: "test_table", Action: analyzer.ActionTypeDrop},
				},
			}},
		},
		// 其他常用语句
		{
			name: "TRUNCATE TABLE statement",
			sql:  "TRUNCATE TABLE table1",
			expected: []*analyzer.DependencyResult{{
				StmtType: analyzer.StmtTypeDropTable, // TiDB将TRUNCATE映射为DROP_TABLE
				Read:     []*analyzer.DependencyTable{},
				Write: []*analyzer.DependencyTable{
					{Cluster: "default_cluster", Database: "default_db", Table: "table1"},
				},
			}},
		},
		{
			name: "REPLACE statement",
			sql:  "REPLACE INTO table1 (id, name) VALUES (1, 'replaced')",
			expected: []*analyzer.DependencyResult{{
				StmtType: analyzer.StmtTypeInsert, // TiDB将REPLACE映射为INSERT
				Read:     []*analyzer.DependencyTable{},
				Write: []*analyzer.DependencyTable{
					{Cluster: "default_cluster", Database: "default_db", Table: "table1"},
				},
			}},
		},
		{
			name: "CREATE VIEW",
			sql:  "CREATE VIEW test_view AS SELECT id, name FROM test_table",
			expected: []*analyzer.DependencyResult{{
				StmtType: analyzer.StmtTypeCreateView,
				Read: []*analyzer.DependencyTable{
					{Cluster: "default_cluster", Database: "default_db", Table: "test_table"},
				},
				Write: []*analyzer.DependencyTable{
					{Cluster: "default_cluster", Database: "default_db", Table: "test_view"},
				},
			}},
		},
		{
			name: "Single statement with semicolon in string",
			sql:  "INSERT INTO t1 VALUES (1, 'contains ; semicolon')",
			expected: []*analyzer.DependencyResult{{
				StmtType: analyzer.StmtTypeInsert,
				Read:     []*analyzer.DependencyTable{},
				Write: []*analyzer.DependencyTable{
					{Cluster: "default_cluster", Database: "default_db", Table: "t1"},
				},
			}},
		},
		{
			name: "Multiple statements with semicolons in strings",
			sql:  "INSERT INTO t1 VALUES (1, 'contains ; semicolon'); UPDATE t2 SET col='another ; semicolon' WHERE id=2",
			expected: []*analyzer.DependencyResult{{
				StmtType: analyzer.StmtTypeInsert,
				Read:     []*analyzer.DependencyTable{},
				Write: []*analyzer.DependencyTable{
					{Cluster: "default_cluster", Database: "default_db", Table: "t1"},
				},
			}, {
				StmtType: analyzer.StmtTypeUpdate,
				Read:     []*analyzer.DependencyTable{},
				Write: []*analyzer.DependencyTable{
					{Cluster: "default_cluster", Database: "default_db", Table: "t2"},
				},
			}},
		},
		{
			name: "Multiple statements with comments",
			sql:  "SELECT * FROM t1; -- This is a comment\nINSERT INTO t2 VALUES (1, 'test')",
			expected: []*analyzer.DependencyResult{{
				StmtType: analyzer.StmtTypeSelect,
				Read: []*analyzer.DependencyTable{
					{Cluster: "default_cluster", Database: "default_db", Table: "t1"},
				},
				Write: []*analyzer.DependencyTable{},
			}, {
				StmtType: analyzer.StmtTypeInsert,
				Read:     []*analyzer.DependencyTable{},
				Write: []*analyzer.DependencyTable{
					{Cluster: "default_cluster", Database: "default_db", Table: "t2"},
				},
			}},
		},
		{
			name: "Multiple statements with comments at the end",
			sql:  "SELECT * FROM t1; -- This is a comment\n INSERT INTO t2 VALUES (1, 'test')\n  -- This is another comment",
			expected: []*analyzer.DependencyResult{{
				StmtType: analyzer.StmtTypeSelect,
				Read: []*analyzer.DependencyTable{
					{Cluster: "default_cluster", Database: "default_db", Table: "t1"},
				},
				Write: []*analyzer.DependencyTable{},
			}, {
				StmtType: analyzer.StmtTypeInsert,
				Read:     []*analyzer.DependencyTable{},
				Write: []*analyzer.DependencyTable{
					{Cluster: "default_cluster", Database: "default_db", Table: "t2"},
				},
			}},
		},
		{
			name: "Statement with block comment containing semicolon",
			sql:  "SELECT * FROM t1 /* ; comment with semicolon */ WHERE id=1",
			expected: []*analyzer.DependencyResult{{
				StmtType: analyzer.StmtTypeSelect,
				Read: []*analyzer.DependencyTable{
					{Cluster: "default_cluster", Database: "default_db", Table: "t1"},
				},
				Write: []*analyzer.DependencyTable{},
			}},
		},
		// USE语句测试
		{
			name: "USE database statement",
			sql:  "USE test_db",
			expected: []*analyzer.DependencyResult{{
				StmtType: analyzer.StmtTypeUseDatabase,
				Read:     []*analyzer.DependencyTable{},
				Write:    []*analyzer.DependencyTable{},
			}},
		},
	}

	tidbAnalyzer := NewDependencyAnalyzer()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &analyzer.DependencyAnalyzeReq{
				DefaultCluster:  "default_cluster",
				DefaultDatabase: "default_db",
				Type:            analyzer.EngineTiDB,
				SQL:             tt.sql,
			}

			results, err := tidbAnalyzer.Analyze(req)
			assert.NoError(t, err)
			if assert.Equal(t, len(tt.expected), len(results)) {
				for i, result := range results {
					expected := tt.expected[i]
					assert.Equal(t, expected.StmtType, result.StmtType)
					assert.Equal(t, len(expected.Read), len(result.Read))
					assert.Equal(t, len(expected.Write), len(result.Write))

					// 验证读表
					for j, readTable := range result.Read {
						expectedRead := expected.Read[j]
						assert.Equal(t, expectedRead.Cluster, readTable.Cluster)
						assert.Equal(t, expectedRead.Database, readTable.Database)
						assert.Equal(t, expectedRead.Table, readTable.Table)
					}

					// 验证写表
					for j, writeTable := range result.Write {
						expectedWrite := expected.Write[j]
						assert.Equal(t, expectedWrite.Cluster, writeTable.Cluster)
						assert.Equal(t, expectedWrite.Database, writeTable.Database)
						assert.Equal(t, expectedWrite.Table, writeTable.Table)
					}

					// 验证Actions
					assert.Equal(t, len(expected.Actions), len(result.Actions))
					for j, action := range result.Actions {
						expectedAction := expected.Actions[j]
						assert.Equal(t, expectedAction.Cluster, action.Cluster)
						assert.Equal(t, expectedAction.Database, action.Database)
						assert.Equal(t, expectedAction.Table, action.Table)
						assert.Equal(t, expectedAction.Action, action.Action)
					}
				}
			}
		})
	}
}
