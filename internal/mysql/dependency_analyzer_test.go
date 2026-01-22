package mysql

import (
	"sql-parser/analyzer"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMySQLDependencyAnalyzer(t *testing.T) {
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
							Cluster:  "cl",
							Database: "db",
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
							Cluster:  "cl",
							Database: "db",
							Table:    "table1",
						},
					},
					Write: []*analyzer.DependencyTable{},
				},
			},
		},
		{
			name: "SELECT with multiple tables and JOIN",
			sql:  "SELECT t1.id, t2.name FROM table1 t1 JOIN table2 t2 ON t1.id = t2.table1_id WHERE t1.status = 'active'",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "SELECT t1.id, t2.name FROM table1 t1 JOIN table2 t2 ON t1.id = t2.table1_id WHERE t1.status = 'active'",
					StmtType: analyzer.StmtTypeSelect,
					Read: []*analyzer.DependencyTable{
						{
							Cluster:  "cl",
							Database: "db",
							Table:    "table1",
						},
						{
							Cluster:  "cl",
							Database: "db",
							Table:    "table2",
						},
					},
					Write: []*analyzer.DependencyTable{},
				},
			},
		},
		{
			name: "SELECT with subquery",
			sql:  "SELECT * FROM table1 WHERE id IN (SELECT table1_id FROM table2 WHERE status = 'active')",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "SELECT * FROM table1 WHERE id IN (SELECT table1_id FROM table2 WHERE status = 'active')",
					StmtType: analyzer.StmtTypeSelect,
					Read: []*analyzer.DependencyTable{
						{
							Cluster:  "cl",
							Database: "db",
							Table:    "table1",
						},
						{
							Cluster:  "cl",
							Database: "db",
							Table:    "table2",
						},
					},
					Write: []*analyzer.DependencyTable{},
				},
			},
		},
		// 数据修改语句
		{
			name: "INSERT statement",
			sql:  "INSERT INTO table1 (id, name) VALUES (1, 'test')",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "INSERT INTO table1 (id, name) VALUES (1, 'test')",
					StmtType: analyzer.StmtTypeInsert,
					Read:     []*analyzer.DependencyTable{},
					Write: []*analyzer.DependencyTable{
						{
							Cluster:  "cl",
							Database: "db",
							Table:    "table1",
						},
					},
				},
			},
		},
		{
			name: "INSERT SELECT statement",
			sql:  "INSERT INTO table1 (id, name) SELECT id, name FROM table2 WHERE status = 'active'",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "INSERT INTO table1 (id, name) SELECT id, name FROM table2 WHERE status = 'active'",
					StmtType: analyzer.StmtTypeInsert,
					Read: []*analyzer.DependencyTable{
						{
							Cluster:  "cl",
							Database: "db",
							Table:    "table2",
						},
					},
					Write: []*analyzer.DependencyTable{
						{
							Cluster:  "cl",
							Database: "db",
							Table:    "table1",
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
							Cluster:  "cl",
							Database: "db",
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
							Cluster:  "cl",
							Database: "db",
							Table:    "table1",
						},
					},
				},
			},
		},
		// DDL语句
		{
			name: "CREATE TABLE statement",
			sql:  "CREATE TABLE new_table (id INT PRIMARY KEY, name VARCHAR(50) NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "CREATE TABLE new_table (id INT PRIMARY KEY, name VARCHAR(50) NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
					StmtType: analyzer.StmtTypeCreateTable,
					Read:     []*analyzer.DependencyTable{},
					Write: []*analyzer.DependencyTable{
						{
							Cluster:  "cl",
							Database: "db",
							Table:    "new_table",
						},
					},
				},
			},
		},
		{
			name: "ALTER TABLE add column",
			sql:  "ALTER TABLE table1 ADD COLUMN new_column VARCHAR(100)",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "ALTER TABLE table1 ADD COLUMN new_column VARCHAR(100)",
					StmtType: analyzer.StmtTypeAlterTable,
					Read:     []*analyzer.DependencyTable{},
					Write: []*analyzer.DependencyTable{
						{
							Cluster:  "cl",
							Database: "db",
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
							Cluster:  "cl",
							Database: "db",
							Table:    "old_table",
						},
					},
				},
			},
		},
		// 其他常用语句
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
							Cluster:  "cl",
							Database: "db",
							Table:    "table1",
						},
					},
				},
			},
		},
		{
			name: "REPLACE statement",
			sql:  "REPLACE INTO table1 (id, name) VALUES (1, 'replaced')",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "REPLACE INTO table1 (id, name) VALUES (1, 'replaced')",
					StmtType: analyzer.StmtTypeReplaceTable,
					Read:     []*analyzer.DependencyTable{},
					Write: []*analyzer.DependencyTable{
						{
							Cluster:  "cl",
							Database: "db",
							Table:    "table1",
						},
					},
				},
			},
		},
	}

	mysqlAnalyzer := NewDependencyAnalyzer()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := mysqlAnalyzer.Analyze(&analyzer.DependencyAnalyzeReq{
				DefaultCluster:  "cl",
				DefaultDatabase: "db",
				Type:            analyzer.SQLTypeMySQL,
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
