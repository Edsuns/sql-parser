package starrocks

import (
	"testing"

	"github.com/Edsuns/sql-parser/analyzer"
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
					Actions: []*analyzer.ActionTable{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "test_table",
							Action:   analyzer.ActionTypeCreate,
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
					Actions: []*analyzer.ActionTable{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "user_table",
							Columns: []*analyzer.ActionColumn{
								{
									Name:   "email",
									Type:   "VARCHAR(100)",
									Action: analyzer.ActionTypeCreate,
								},
							},
							Action: analyzer.ActionTypeAlter,
						},
					},
				},
			},
		},
		{
			name: "ALTER TABLE statement with multiple columns",
			sql:  "ALTER TABLE user_table ADD COLUMN (email VARCHAR(100), age INT)",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "ALTER TABLE user_table ADD COLUMN (email VARCHAR(100), age INT)",
					StmtType: analyzer.StmtTypeAlterTable,
					Read:     []*analyzer.DependencyTable{},
					Write: []*analyzer.DependencyTable{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "user_table",
						},
					},
					Actions: []*analyzer.ActionTable{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "user_table",
							Columns: []*analyzer.ActionColumn{
								{
									Name:   "email",
									Type:   "VARCHAR(100)",
									Action: analyzer.ActionTypeCreate,
								},
								{
									Name:   "age",
									Type:   "INT",
									Action: analyzer.ActionTypeCreate,
								},
							},
							Action: analyzer.ActionTypeAlter,
						},
					},
				},
			},
		},
		{
			name: "ALTER TABLE drop column",
			sql:  "ALTER TABLE user_table DROP COLUMN email",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "ALTER TABLE user_table DROP COLUMN email",
					StmtType: analyzer.StmtTypeAlterTable,
					Read:     []*analyzer.DependencyTable{},
					Write: []*analyzer.DependencyTable{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "user_table",
						},
					},
					Actions: []*analyzer.ActionTable{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "user_table",
							Columns: []*analyzer.ActionColumn{
								{
									Name:   "email",
									Action: analyzer.ActionTypeDrop,
								},
							},
							Action: analyzer.ActionTypeAlter,
						},
					},
				},
			},
		},
		{
			name: "ALTER TABLE modify column",
			sql:  "ALTER TABLE user_table MODIFY COLUMN age BIGINT",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "ALTER TABLE user_table MODIFY COLUMN age BIGINT",
					StmtType: analyzer.StmtTypeAlterTable,
					Read:     []*analyzer.DependencyTable{},
					Write: []*analyzer.DependencyTable{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "user_table",
						},
					},
					Actions: []*analyzer.ActionTable{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "user_table",
							Columns: []*analyzer.ActionColumn{
								{
									Name:   "age",
									Type:   "BIGINT",
									Action: analyzer.ActionTypeAlter,
								},
							},
							Action: analyzer.ActionTypeAlter,
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
					Actions: []*analyzer.ActionTable{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "test_table",
							Action:   analyzer.ActionTypeDrop,
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
				},
			},
		},
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
				Type:            analyzer.EngineStarRocks,
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

					// 验证Actions
					assert.Equal(t, len(expected.Actions), len(r.Actions))
					for j, action := range r.Actions {
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
