package spark

import (
	"testing"

	"github.com/Edsuns/sql-parser/analyzer"
)

func TestSplitSQL(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected []string
	}{
		{
			name:     "single statement without semicolon",
			sql:      "SELECT * FROM table",
			expected: []string{"SELECT * FROM table"},
		},
		{
			name:     "single statement with semicolon",
			sql:      "SELECT * FROM table;",
			expected: []string{"SELECT * FROM table;"},
		},
		{
			name:     "multiple statements with semicolons",
			sql:      "SELECT * FROM table1;  SELECT * FROM table2;",
			expected: []string{"SELECT * FROM table1;", "SELECT * FROM table2;"},
		},
		{
			name:     "statement with comments",
			sql:      "-- This is a comment\nSELECT * FROM table1; -- Another comment",
			expected: []string{"-- This is a comment\nSELECT * FROM table1;", "-- Another comment"},
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
				"SELECT * FROM t1;", "-- This is a comment\n INSERT INTO t2 VALUES (1, 'test');", "-- This is another comment after semicolon",
			},
		},
		{
			name: "statements with indentation and newlines",
			sql: `SELECT
    col20,
    col21
FROM
    table1
WHERE
    col20 > 10;

SELECT
    col3
FROM
    table2;`,
			expected: []string{
				`SELECT
    col20,
    col21
FROM
    table1
WHERE
    col20 > 10;`,
				`SELECT
    col3
FROM
    table2;`,
			},
		},
		{
			name: "statement with multiple CTEs",
			sql: `WITH 
  -- 标识1：描述1 
  cte2 AS ( 
    SELECT 
      col10, 
      col11, 
      col12, 
      col13 
    FROM table17 
    WHERE YEAR(col12) = 2023 
  ), 

  -- 标识2：描述2 
  cte3 AS ( 
    SELECT 
      col11, 
      SUM(col13) AS col14, 
      COUNT(col10) AS col15 
    FROM cte2 
    GROUP BY col11 
  ), 

  -- 标识3：描述3 
  cte4 AS ( 
    SELECT 
      col11, 
      col14, 
      col15, 
      CASE 
        WHEN col14 > 10000 THEN '名称1' 
        WHEN col14 > 5000 THEN '名称2' 
        ELSE '名称3' 
      END AS col16 
    FROM cte3 
  ) 

-- 描述4 
SELECT 
  t1.col16, 
  COUNT(DISTINCT t1.col11) AS col17, 
  AVG(t1.col14) AS col18, 
  SUM(t1.col14) AS col19 
FROM cte4 t1 
WHERE t1.col15 >= 2 
GROUP BY t1.col16 
ORDER BY col19 DESC;`,
			expected: []string{`WITH 
  -- 标识1：描述1 
  cte2 AS ( 
    SELECT 
      col10, 
      col11, 
      col12, 
      col13 
    FROM table17 
    WHERE YEAR(col12) = 2023 
  ), 

  -- 标识2：描述2 
  cte3 AS ( 
    SELECT 
      col11, 
      SUM(col13) AS col14, 
      COUNT(col10) AS col15 
    FROM cte2 
    GROUP BY col11 
  ), 

  -- 标识3：描述3 
  cte4 AS ( 
    SELECT 
      col11, 
      col14, 
      col15, 
      CASE 
        WHEN col14 > 10000 THEN '名称1' 
        WHEN col14 > 5000 THEN '名称2' 
        ELSE '名称3' 
      END AS col16 
    FROM cte3 
  ) 

-- 描述4 
SELECT 
  t1.col16, 
  COUNT(DISTINCT t1.col11) AS col17, 
  AVG(t1.col14) AS col18, 
  SUM(t1.col14) AS col19 
FROM cte4 t1 
WHERE t1.col15 >= 2 
GROUP BY t1.col16 
ORDER BY col19 DESC;`},
		},
		{
			name:     "statement with semicolon in string",
			sql:      "INSERT INTO table VALUES ('a;b', 'c'); SELECT * FROM table;",
			expected: []string{"INSERT INTO table VALUES ('a;b', 'c');", "SELECT * FROM table;"},
		},
		{
			name:     "statement with comments",
			sql:      "-- This is a comment\nSELECT * FROM table; -- Another comment",
			expected: []string{"-- This is a comment\nSELECT * FROM table;", "-- Another comment"},
		}, {
			name: "CTE statement",
			sql:  `WITH orders_2023 AS (SELECT order_id, customer_id, order_date, total_amount FROM orders WHERE YEAR(order_date) = 2023) SELECT * FROM orders_2023;`,
			expected: []string{
				`WITH orders_2023 AS (SELECT order_id, customer_id, order_date, total_amount FROM orders WHERE YEAR(order_date) = 2023) SELECT * FROM orders_2023;`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := analyzer.SplitSQL(makeLexer(tt.sql))
			if len(result) != len(tt.expected) {
				t.Fatalf("Expected %d statements, got %d\nExpected: %v\nGot: %v", len(tt.expected), len(result), tt.expected, result)
			}
			for i := range result {
				if result[i] != tt.expected[i] {
					t.Errorf("Statement %d mismatch\nExpected:\n%s\nGot:\n%s", i+1, tt.expected[i], result[i])
				}
			}
		})
	}
}
