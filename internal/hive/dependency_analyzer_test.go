package hive

import (
	"testing"

	"github.com/Edsuns/sql-parser/analyzer"
	"github.com/stretchr/testify/assert"
)

func TestHiveDependencyAnalyzer(t *testing.T) {
	// Hive官方文档SQL示例测试用例
	tests := []struct {
		name     string
		sql      string
		expected []*analyzer.DependencyResult
	}{
		// 基本查询语句
		{
			name: "SELECT statement",
			sql:  "SELECT * FROM table1 WHERE id = 1",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "SELECT * FROM table1 WHERE id = 1",
					StmtType: analyzer.StmtTypeSelect,
					Read: []*analyzer.DependencyTable{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "table1",
						},
					},
					Write: []*analyzer.DependencyTable{},
				},
			},
		},
		{
			name: "SELECT statement lower case",
			sql:  "SELECT * from table1 WHERE id = 1",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "SELECT * from table1 WHERE id = 1",
					StmtType: analyzer.StmtTypeSelect,
					Read: []*analyzer.DependencyTable{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "table1",
						},
					},
					Write: []*analyzer.DependencyTable{},
				},
			},
		},
		{
			name: "SELECT with JOIN",
			sql:  "SELECT t1.id, t2.name FROM table1 t1 JOIN table2 t2 ON t1.id = t2.table1_id",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "SELECT t1.id, t2.name FROM table1 t1 JOIN table2 t2 ON t1.id = t2.table1_id",
					StmtType: analyzer.StmtTypeSelect,
					Read: []*analyzer.DependencyTable{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "table1",
						},
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "table2",
						},
					},
					Write: []*analyzer.DependencyTable{},
				},
			},
		},
		{
			name: "SELECT with database specified",
			sql:  "SELECT * FROM db1.table1 WHERE id = 1",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "SELECT * FROM db1.table1 WHERE id = 1",
					StmtType: analyzer.StmtTypeSelect,
					Read: []*analyzer.DependencyTable{
						{
							Cluster:  "default_cluster",
							Database: "db1",
							Table:    "table1",
						},
					},
					Write: []*analyzer.DependencyTable{},
				},
			},
		},
		// 数据修改语句
		{
			name: "INSERT statement with VALUES",
			sql:  "INSERT INTO table1 (id, name) VALUES (1, 'test'), (2, 'test2')",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "INSERT INTO table1 (id, name) VALUES (1, 'test'), (2, 'test2')",
					StmtType: analyzer.StmtTypeInsert,
					Read:     []*analyzer.DependencyTable{},
					Write: []*analyzer.DependencyTable{
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
			sql:  "INSERT INTO table2 SELECT id, name FROM table1 WHERE status = 'active'",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "INSERT INTO table2 SELECT id, name FROM table1 WHERE status = 'active'",
					StmtType: analyzer.StmtTypeInsert,
					Read: []*analyzer.DependencyTable{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "table1",
						},
					},
					Write: []*analyzer.DependencyTable{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "table2",
						},
					},
				},
			},
		},
		{
			name: "UPDATE statement",
			sql:  "UPDATE table1 SET name = 'new' WHERE id = 1",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "UPDATE table1 SET name = 'new' WHERE id = 1",
					StmtType: analyzer.StmtTypeUpdate,
					Read:     []*analyzer.DependencyTable{},
					Write: []*analyzer.DependencyTable{
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
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "DELETE FROM table1 WHERE id = 1",
					StmtType: analyzer.StmtTypeDelete,
					Read:     []*analyzer.DependencyTable{},
					Write: []*analyzer.DependencyTable{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "table1",
						},
					},
				},
			},
		},
		// DDL语句
		{
			name: "CREATE TABLE statement",
			sql:  "CREATE TABLE new_table (id INT, name STRING) STORED AS PARQUET",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "CREATE TABLE new_table (id INT, name STRING) STORED AS PARQUET",
					StmtType: analyzer.StmtTypeCreateTable,
					Read:     []*analyzer.DependencyTable{},
					Write: []*analyzer.DependencyTable{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "new_table",
						},
					},
				},
			},
		},
		{
			name: "CREATE TABLE with external location",
			sql:  "CREATE EXTERNAL TABLE ext_table (id INT, name STRING) LOCATION '/user/hive/warehouse/ext_table'",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "CREATE EXTERNAL TABLE ext_table (id INT, name STRING) LOCATION '/user/hive/warehouse/ext_table'",
					StmtType: analyzer.StmtTypeCreateTable,
					Read:     []*analyzer.DependencyTable{},
					Write: []*analyzer.DependencyTable{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "ext_table",
						},
					},
				},
			},
		},
		{
			name: "ALTER TABLE add column",
			sql:  "ALTER TABLE table1 ADD COLUMNS (email STRING)",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "ALTER TABLE table1 ADD COLUMNS (email STRING)",
					StmtType: analyzer.StmtTypeAlterTable,
					Read:     []*analyzer.DependencyTable{},
					Write: []*analyzer.DependencyTable{
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
			name: "DROP TABLE statement",
			sql:  "DROP TABLE IF EXISTS old_table",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "DROP TABLE IF EXISTS old_table",
					StmtType: analyzer.StmtTypeDropTable,
					Read:     []*analyzer.DependencyTable{},
					Write: []*analyzer.DependencyTable{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "old_table",
						},
					},
				},
			},
		},
		{
			name: "TRUNCATE TABLE statement",
			sql:  "TRUNCATE TABLE table1",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "TRUNCATE TABLE table1",
					StmtType: analyzer.StmtTypeTruncate,
					Read:     []*analyzer.DependencyTable{},
					Write: []*analyzer.DependencyTable{
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
			sql:  "CREATE VIEW view1 AS SELECT id, name FROM table1 WHERE status = 'active'",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "CREATE VIEW view1 AS SELECT id, name FROM table1 WHERE status = 'active'",
					StmtType: analyzer.StmtTypeCreateView,
					Read: []*analyzer.DependencyTable{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "table1",
						},
					},
					Write: []*analyzer.DependencyTable{
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
			name: "DROP VIEW statement",
			sql:  "DROP VIEW IF EXISTS view1",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "DROP VIEW IF EXISTS view1",
					StmtType: analyzer.StmtTypeDropTable,
					Read:     []*analyzer.DependencyTable{},
					Write: []*analyzer.DependencyTable{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "view1",
						},
					},
				},
			},
		},
		// 复杂查询
		{
			name: "SELECT with subquery",
			sql:  "SELECT * FROM (SELECT id, name FROM table1) t WHERE t.id > 10",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "SELECT * FROM (SELECT id, name FROM table1) t WHERE t.id > 10",
					StmtType: analyzer.StmtTypeSelect,
					Read: []*analyzer.DependencyTable{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "table1",
						},
					},
					Write: []*analyzer.DependencyTable{},
				},
			},
		},
		{
			name: "SELECT with CTE",
			sql:  "WITH cte AS (SELECT id, name FROM table1) SELECT * FROM cte WHERE id > 10",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "WITH cte AS (SELECT id, name FROM table1) SELECT * FROM cte WHERE id > 10",
					StmtType: analyzer.StmtTypeSelect,
					Read: []*analyzer.DependencyTable{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "table1",
						},
					},
					Write: []*analyzer.DependencyTable{},
				},
			},
		},
	}

	hiveAnalyzer := NewDependencyAnalyzer()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := hiveAnalyzer.Analyze(&analyzer.DependencyAnalyzeReq{
				DefaultCluster:  "default_cluster",
				DefaultDatabase: "default_db",
				Type:            analyzer.EngineHive,
				SQL:             tt.sql,
			})
			assert.NoError(t, err)
			// 打印调试信息
			t.Logf("SQL: %s", tt.sql)
			t.Logf("Expected result count: %d, Actual: %d", len(tt.expected), len(result))
			for i, r := range result {
				t.Logf("Result %d: Stmt=%s, StmtType=%s, Read=%d tables, Write=%d tables",
					i, r.Stmt, r.StmtType, len(r.Read), len(r.Write))
				for j, tbl := range r.Read {
					t.Logf("  Read table %d: %s.%s.%s", j, tbl.Cluster, tbl.Database, tbl.Table)
				}
				for j, tbl := range r.Write {
					t.Logf("  Write table %d: %s.%s.%s", j, tbl.Cluster, tbl.Database, tbl.Table)
				}
			}
			assert.Equal(t, len(tt.expected), len(result))
			if len(result) > 0 {
				assert.Equal(t, tt.expected[0].Stmt, result[0].Stmt)
				assert.Equal(t, tt.expected[0].StmtType, result[0].StmtType)
				assert.Equal(t, len(tt.expected[0].Read), len(result[0].Read))
				assert.Equal(t, len(tt.expected[0].Write), len(result[0].Write))
			}
		})
	}
}
