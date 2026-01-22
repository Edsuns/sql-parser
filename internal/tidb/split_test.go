package tidb

import (
	"github.com/pingcap/tidb/pkg/parser"
	"sql-parser/analyzer"
	"strings"
	"testing"
)

func TestSplit(t *testing.T) {
	// Test cases with various SQL statements
	testCases := []struct {
		name     string
		sql      string
		expected []string
	}{
		{
			name:     "Single statement with semicolon in string",
			sql:      "INSERT INTO t1 VALUES (1, 'contains ; semicolon')",
			expected: []string{"INSERT INTO t1 VALUES (1, 'contains ; semicolon')"},
		},
		{
			name: "Multiple statements with semicolons in strings",
			sql:  "INSERT INTO t1 VALUES (1, 'contains ; semicolon'); UPDATE t2 SET col='another ; semicolon' WHERE id=2",
			expected: []string{
				"INSERT INTO t1 VALUES (1, 'contains ; semicolon');", "UPDATE t2 SET col='another ; semicolon' WHERE id=2",
			},
		},
		{
			name:     "Statement with block comment containing semicolon",
			sql:      "SELECT * FROM t1 /* ; comment with semicolon */ WHERE id=1",
			expected: []string{"SELECT * FROM t1 /* ; comment with semicolon */ WHERE id=1"},
		},
		{
			name: "Multiple statements with comments",
			sql:  "SELECT * FROM t1; -- This is a comment\nINSERT INTO t2 VALUES (1, 'test')",
			expected: []string{
				"SELECT * FROM t1;", "-- This is a comment\nINSERT INTO t2 VALUES (1, 'test')",
			},
		},
		{
			name: "Multiple statements with comments at the end",
			sql:  "SELECT * FROM t1; -- This is a comment\n INSERT INTO t2 VALUES (1, 'test')\n  -- This is another comment",
			expected: []string{
				"SELECT * FROM t1;", "-- This is a comment\n INSERT INTO t2 VALUES (1, 'test')\n  -- This is another comment",
			},
		},
		{
			name: "Multiple statements with comments at the end after semicolon",
			sql:  "SELECT * FROM t1; -- This is a comment\n INSERT INTO t2 VALUES (1, 'test');\n  -- This is another comment after semicolon",
			expected: []string{
				"SELECT * FROM t1;", "-- This is a comment\n INSERT INTO t2 VALUES (1, 'test');",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := &analyzer.DependencyAnalyzeReq{
				DefaultCluster:  "default_cluster",
				DefaultDatabase: "default_db",
				SQL:             tc.sql,
			}

			// 创建TiDB解析器
			p := parser.New()
			// 解析SQL语句
			stmts, _, err := p.Parse(req.SQL, "", "")
			if err != nil {
				t.Fatalf("Analyze failed: %v", err)
			}

			if len(stmts) != len(tc.expected) {
				t.Fatalf("Expected %d result, got %d", len(tc.expected), len(stmts))
			} else {
				for i, stmt := range stmts {
					expected := tc.expected[i]
					actual := strings.TrimSpace(stmt.OriginalText())
					if actual != expected {
						t.Errorf("Expected StmtType %s, got %s", expected, actual)
					}
				}
			}
		})
	}
}
