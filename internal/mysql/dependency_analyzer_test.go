package mysql

import (
	"testing"

	"github.com/Edsuns/sql-parser/analyzer"
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
		{
			name: "USE statement",
			sql:  "USE db1",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "USE db1",
					StmtType: analyzer.StmtTypeUseDatabase,
					Read:     []*analyzer.DependencyTable{},
					Write:    []*analyzer.DependencyTable{},
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
				Type:            analyzer.EngineMySQL,
				SQL:             tt.sql,
			})
			assert.NoError(t, err)
			if assert.Equal(t, len(tt.expected), len(result)) {
				for i, r := range result {
					expected := tt.expected[i]
					assert.Equal(t, expected.Stmt, r.Stmt)
					assert.Equal(t, expected.StmtType, r.StmtType)
					assert.Equal(t, len(expected.Read), len(r.Read))
					assert.Equal(t, len(expected.Write), len(r.Write))

					// 验证读表
					for j, readTable := range r.Read {
						expectedRead := expected.Read[j]
						assert.Equal(t, expectedRead.Cluster, readTable.Cluster)
						assert.Equal(t, expectedRead.Database, readTable.Database)
						assert.Equal(t, expectedRead.Table, readTable.Table)
					}

					// 验证写表
					for j, writeTable := range r.Write {
						expectedWrite := expected.Write[j]
						assert.Equal(t, expectedWrite.Cluster, writeTable.Cluster)
						assert.Equal(t, expectedWrite.Database, writeTable.Database)
						assert.Equal(t, expectedWrite.Table, writeTable.Table)
					}
				}
			}
		})
	}
}
