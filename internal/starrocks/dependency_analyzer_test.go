package starrocks

import (
	"sql-parser/analyzer"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStarRocksDependencyAnalyzer(t *testing.T) {
	// StarRocks 3.5.11 常用SQL示例
	tests := []struct {
		name     string
		sql      string
		expected []*analyzer.DependencyResult
	}{
		{
			name: "SELECT statement with table",
			sql:  "SELECT id, name FROM user_table WHERE age > 18",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "SELECT id, name FROM user_table WHERE age > 18",
					StmtType: analyzer.StmtTypeSelect,
					Read: []*analyzer.DependencyTable{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "user_table",
						},
					},
					Write: []*analyzer.DependencyTable{},
				},
			},
		},
		{
			name: "SELECT statement with table and lower case",
			sql:  "SELECT id, name from user_table WHERE age > 18",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "SELECT id, name from user_table WHERE age > 18",
					StmtType: analyzer.StmtTypeSelect,
					Read: []*analyzer.DependencyTable{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "user_table",
						},
					},
					Write: []*analyzer.DependencyTable{},
				},
			},
		},
		{
			name: "SELECT statement with database and table",
			sql:  "SELECT * FROM db1.orders WHERE order_date >= '2023-01-01'",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "SELECT * FROM db1.orders WHERE order_date >= '2023-01-01'",
					StmtType: analyzer.StmtTypeSelect,
					Read: []*analyzer.DependencyTable{
						{
							Cluster:  "default_cluster",
							Database: "db1",
							Table:    "orders",
						},
					},
					Write: []*analyzer.DependencyTable{},
				},
			},
		},
		{
			name: "INSERT statement with VALUES",
			sql:  "INSERT INTO user_table (id, name, age) VALUES (1, 'John', 25), (2, 'Jane', 30)",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "INSERT INTO user_table (id, name, age) VALUES (1, 'John', 25), (2, 'Jane', 30)",
					StmtType: analyzer.StmtTypeInsert,
					Read:     []*analyzer.DependencyTable{},
					Write: []*analyzer.DependencyTable{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "user_table",
						},
					},
				},
			},
		},
		{
			name: "INSERT SELECT statement",
			sql:  "INSERT INTO target_table SELECT id, name FROM source_table WHERE age > 20",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "INSERT INTO target_table SELECT id, name FROM source_table WHERE age > 20",
					StmtType: analyzer.StmtTypeInsert,
					Read: []*analyzer.DependencyTable{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "source_table",
						},
					},
					Write: []*analyzer.DependencyTable{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "target_table",
						},
					},
				},
			},
		},
		{
			name: "UPDATE statement",
			sql:  "UPDATE user_table SET age = age + 1 WHERE id = 1",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "UPDATE user_table SET age = age + 1 WHERE id = 1",
					StmtType: analyzer.StmtTypeUpdate,
					Read:     []*analyzer.DependencyTable{},
					Write: []*analyzer.DependencyTable{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "user_table",
						},
					},
				},
			},
		},
		{
			name: "DELETE statement",
			sql:  "DELETE FROM user_table WHERE age < 18",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "DELETE FROM user_table WHERE age < 18",
					StmtType: analyzer.StmtTypeDelete,
					Read:     []*analyzer.DependencyTable{},
					Write: []*analyzer.DependencyTable{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "user_table",
						},
					},
				},
			},
		},
		{
			name: "CREATE TABLE statement",
			sql:  "CREATE TABLE test_table (id INT, name VARCHAR(50)) ENGINE=OLAP DISTRIBUTED BY HASH(id) BUCKETS 10",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "CREATE TABLE test_table (id INT, name VARCHAR(50)) ENGINE=OLAP DISTRIBUTED BY HASH(id) BUCKETS 10",
					StmtType: analyzer.StmtTypeCreateTable,
					Read:     []*analyzer.DependencyTable{},
					Write: []*analyzer.DependencyTable{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "test_table",
						},
					},
				},
			},
		},
		{
			name: "ALTER TABLE statement",
			sql:  "ALTER TABLE user_table ADD COLUMN email VARCHAR(100)",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "ALTER TABLE user_table ADD COLUMN email VARCHAR(100)",
					StmtType: analyzer.StmtTypeAlterTable,
					Read:     []*analyzer.DependencyTable{},
					Write: []*analyzer.DependencyTable{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "user_table",
						},
					},
				},
			},
		},
		{
			name: "DROP TABLE statement",
			sql:  "DROP TABLE IF EXISTS test_table",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "DROP TABLE IF EXISTS test_table",
					StmtType: analyzer.StmtTypeDropTable,
					Read:     []*analyzer.DependencyTable{},
					Write: []*analyzer.DependencyTable{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "test_table",
						},
					},
				},
			},
		},
		// {
		// 	name: "USE statement",
		// 	sql:  "USE db1",
		// 	expected: []*analyzer.DependencyResult{
		// 		{
		// 			Stmt:     "USE db1",
		// 			StmtType: "USE",
		// 			Read:     []*analyzer.DependencyTable{},
		// 			Write:    []*analyzer.DependencyTable{},
		// 		},
		// 	},
		// },
		{
			name: "MULTI TABLE SELECT statement",
			sql:  "SELECT u.id, o.order_id FROM user_table u JOIN orders o ON u.id = o.user_id",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "SELECT u.id, o.order_id FROM user_table u JOIN orders o ON u.id = o.user_id",
					StmtType: analyzer.StmtTypeSelect,
					Read: []*analyzer.DependencyTable{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "user_table",
						},
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "orders",
						},
					},
					Write: []*analyzer.DependencyTable{},
				},
			},
		},
	}

	starRocksAnalyzer := NewDependencyAnalyzer()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := starRocksAnalyzer.Analyze(&analyzer.DependencyAnalyzeReq{
				DefaultCluster:  "default_cluster",
				DefaultDatabase: "default_db",
				Type:            analyzer.SQLTypeStarRocks,
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
