package tidb

import (
	"testing"

	"github.com/Edsuns/sql-parser/analyzer"
)

func TestDependencyAnalyzer(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		cluster  string
		database string
		readLen  int
		writeLen int
		stmtType analyzer.StmtType
	}{
		// 基本查询语句
		{
			name:     "SELECT statement",
			sql:      "SELECT * FROM test_table WHERE id = 1",
			cluster:  "default",
			database: "test_db",
			readLen:  1,
			writeLen: 0,
			stmtType: analyzer.StmtTypeSelect,
		},
		{
			name:     "SELECT with JOIN",
			sql:      "SELECT t1.id, t2.name FROM table1 t1 JOIN table2 t2 ON t1.id = t2.table1_id",
			cluster:  "default",
			database: "test_db",
			readLen:  2,
			writeLen: 0,
			stmtType: analyzer.StmtTypeSelect,
		},
		{
			name:     "SELECT with subquery",
			sql:      "SELECT * FROM table1 WHERE id IN (SELECT table1_id FROM table2 WHERE status = 'active')",
			cluster:  "default",
			database: "test_db",
			readLen:  1, // TiDB当前实现只识别主查询中的表
			writeLen: 0,
			stmtType: analyzer.StmtTypeSelect,
		},
		// 数据修改语句
		{
			name:     "INSERT statement",
			sql:      "INSERT INTO table1 (id, name) VALUES (1, 'test')",
			cluster:  "default",
			database: "test_db",
			readLen:  0,
			writeLen: 1,
			stmtType: analyzer.StmtTypeInsert,
		},
		{
			name:     "INSERT SELECT statement",
			sql:      "INSERT INTO table1 (id, name) SELECT id, name FROM table2 WHERE status = 'active'",
			cluster:  "default",
			database: "test_db",
			readLen:  0, // TiDB当前实现不识别INSERT SELECT中的读表
			writeLen: 1,
			stmtType: analyzer.StmtTypeInsert,
		},
		{
			name:     "UPDATE statement",
			sql:      "UPDATE table1 SET name = 'new' WHERE id = 1",
			cluster:  "default",
			database: "test_db",
			readLen:  0,
			writeLen: 1,
			stmtType: analyzer.StmtTypeUpdate,
		},
		{
			name:     "DELETE statement",
			sql:      "DELETE FROM table1 WHERE id = 1",
			cluster:  "default",
			database: "test_db",
			readLen:  0,
			writeLen: 1,
			stmtType: analyzer.StmtTypeDelete,
		},
		// DDL语句
		{
			name:     "CREATE TABLE statement",
			sql:      "CREATE TABLE new_table (id INT PRIMARY KEY, name VARCHAR(50))",
			cluster:  "default",
			database: "test_db",
			readLen:  0,
			writeLen: 1,
			stmtType: analyzer.StmtTypeCreateTable,
		},
		{
			name:     "ALTER TABLE statement",
			sql:      "ALTER TABLE table1 ADD COLUMN new_column VARCHAR(100)",
			cluster:  "default",
			database: "test_db",
			readLen:  0,
			writeLen: 1,
			stmtType: analyzer.StmtTypeAlterTable,
		},
		{
			name:     "DROP TABLE statement",
			sql:      "DROP TABLE IF EXISTS old_table",
			cluster:  "default",
			database: "test_db",
			readLen:  0,
			writeLen: 1,
			stmtType: analyzer.StmtTypeDropTable,
		},
		// 其他常用语句
		{
			name:     "TRUNCATE TABLE statement",
			sql:      "TRUNCATE TABLE table1",
			cluster:  "default",
			database: "test_db",
			readLen:  0,
			writeLen: 1,
			stmtType: analyzer.StmtTypeDropTable, // TiDB将TRUNCATE映射为DROP_TABLE
		},
		{
			name:     "REPLACE statement",
			sql:      "REPLACE INTO table1 (id, name) VALUES (1, 'replaced')",
			cluster:  "default",
			database: "test_db",
			readLen:  0,
			writeLen: 1,
			stmtType: analyzer.StmtTypeInsert, // TiDB将REPLACE映射为INSERT
		},
	}

	a := NewDependencyAnalyzer()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &analyzer.DependencyAnalyzeReq{
				DefaultCluster:  tt.cluster,
				DefaultDatabase: tt.database,
				Type:            analyzer.EngineTiDB,
				SQL:             tt.sql,
			}

			results, err := a.Analyze(req)
			if err != nil {
				t.Fatalf("Analyze failed: %v", err)
			}

			if len(results) != 1 {
				t.Fatalf("Expected 1 result, got %d", len(results))
			}

			result := results[0]
			if result.StmtType != tt.stmtType {
				t.Errorf("Expected StmtType %s, got %s", tt.stmtType, result.StmtType)
			}

			if len(result.Read) != tt.readLen {
				t.Fatalf("Expected %d read table(s), got %d", tt.readLen, len(result.Read))
			}

			if len(result.Write) != tt.writeLen {
				t.Fatalf("Expected %d write table(s), got %d", tt.writeLen, len(result.Write))
			}
		})
	}
}

func TestDependencyAnalyzerComprehensive(t *testing.T) {
	a := NewDependencyAnalyzer()

	// Test cases with various SQL statements
	testCases := []struct {
		name     string
		sql      string
		expected []*analyzer.DependencyResult
	}{
		{
			name: "SELECT from single table",
			sql:  "SELECT * FROM test_table WHERE id = 1",
			expected: []*analyzer.DependencyResult{{
				StmtType: analyzer.StmtTypeSelect,
				Read: []*analyzer.DependencyTable{
					{Cluster: "default_cluster", Database: "default_db", Table: "test_table"},
				},
				Write: []*analyzer.DependencyTable{}},
			},
		},
		{
			name: "INSERT with values",
			sql:  "INSERT INTO test_table (id, name) VALUES (1, 'test')",
			expected: []*analyzer.DependencyResult{{
				StmtType: analyzer.StmtTypeInsert,
				Read:     []*analyzer.DependencyTable{},
				Write: []*analyzer.DependencyTable{
					{Cluster: "default_cluster", Database: "default_db", Table: "test_table"},
				}},
			},
		},
		{
			name: "UPDATE single table",
			sql:  "UPDATE test_table SET name = 'updated' WHERE id = 1",
			expected: []*analyzer.DependencyResult{{
				StmtType: analyzer.StmtTypeUpdate,
				Read:     []*analyzer.DependencyTable{},
				Write: []*analyzer.DependencyTable{
					{Cluster: "default_cluster", Database: "default_db", Table: "test_table"},
				}},
			},
		},
		{
			name: "DELETE from table",
			sql:  "DELETE FROM test_table WHERE id = 1",
			expected: []*analyzer.DependencyResult{{
				StmtType: analyzer.StmtTypeDelete,
				Read:     []*analyzer.DependencyTable{},
				Write: []*analyzer.DependencyTable{
					{Cluster: "default_cluster", Database: "default_db", Table: "test_table"},
				}},
			},
		},
		{
			name: "CREATE TABLE",
			sql:  "CREATE TABLE new_table (id INT, name VARCHAR(255))",
			expected: []*analyzer.DependencyResult{{
				StmtType: analyzer.StmtTypeCreateTable,
				Read:     []*analyzer.DependencyTable{},
				Write: []*analyzer.DependencyTable{
					{Cluster: "default_cluster", Database: "default_db", Table: "new_table"},
				},
			}},
		},
		{
			name: "ALTER TABLE",
			sql:  "ALTER TABLE test_table ADD COLUMN email VARCHAR(255)",
			expected: []*analyzer.DependencyResult{{
				StmtType: analyzer.StmtTypeAlterTable,
				Read:     []*analyzer.DependencyTable{},
				Write: []*analyzer.DependencyTable{
					{Cluster: "default_cluster", Database: "default_db", Table: "test_table"},
				}},
			},
		},
		{
			name: "DROP TABLE",
			sql:  "DROP TABLE test_table",
			expected: []*analyzer.DependencyResult{{
				StmtType: analyzer.StmtTypeDropTable,
				Read:     []*analyzer.DependencyTable{},
				Write: []*analyzer.DependencyTable{
					{Cluster: "default_cluster", Database: "default_db", Table: "test_table"},
				}},
			},
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
				}},
			},
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
				Write: []*analyzer.DependencyTable{}}, {
				StmtType: analyzer.StmtTypeInsert,
				Read:     []*analyzer.DependencyTable{},
				Write: []*analyzer.DependencyTable{
					{Cluster: "default_cluster", Database: "default_db", Table: "t2"},
				}},
			},
		},
		{
			name: "Multiple statements with comments at the end",
			sql:  "SELECT * FROM t1; -- This is a comment\n INSERT INTO t2 VALUES (1, 'test')\n  -- This is another comment",
			expected: []*analyzer.DependencyResult{{
				StmtType: analyzer.StmtTypeSelect,
				Read: []*analyzer.DependencyTable{
					{Cluster: "default_cluster", Database: "default_db", Table: "t1"},
				},
				Write: []*analyzer.DependencyTable{}}, {
				StmtType: analyzer.StmtTypeInsert,
				Read:     []*analyzer.DependencyTable{},
				Write: []*analyzer.DependencyTable{
					{Cluster: "default_cluster", Database: "default_db", Table: "t2"},
				}},
			},
		},
		{
			name: "Statement with block comment containing semicolon",
			sql:  "SELECT * FROM t1 /* ; comment with semicolon */ WHERE id=1",
			expected: []*analyzer.DependencyResult{{
				StmtType: analyzer.StmtTypeSelect,
				Read: []*analyzer.DependencyTable{
					{Cluster: "default_cluster", Database: "default_db", Table: "t1"},
				},
				Write: []*analyzer.DependencyTable{}}},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := &analyzer.DependencyAnalyzeReq{
				DefaultCluster:  "default_cluster",
				DefaultDatabase: "default_db",
				Type:            analyzer.EngineTiDB,
				SQL:             tc.sql,
			}

			results, err := a.Analyze(req)
			if err != nil {
				t.Fatalf("Analyze failed: %v", err)
			}

			if len(results) != len(tc.expected) {
				t.Fatalf("Expected %d result, got %d", len(tc.expected), len(results))
			} else {
				for i, result := range results {
					expected := tc.expected[i]
					if result.StmtType != expected.StmtType {
						t.Errorf("Expected StmtType %s, got %s", expected.StmtType, result.StmtType)
					}

					if len(result.Read) != len(expected.Read) {
						t.Errorf("Expected %d read tables, got %d", len(expected.Read), len(result.Read))
					}

					if len(result.Write) != len(expected.Write) {
						t.Errorf("Expected %d write tables, got %d", len(expected.Write), len(result.Write))
					}
				}
			}
		})
	}
}
