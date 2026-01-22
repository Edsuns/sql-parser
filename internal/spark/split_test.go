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
    col1,
    col2
FROM
    table1
WHERE
    col1 > 10;

SELECT
    col3
FROM
    table2;`,
			expected: []string{
				`SELECT
    col1,
    col2
FROM
    table1
WHERE
    col1 > 10;`,
				`SELECT
    col3
FROM
    table2;`,
			},
		},
		{
			name: "statement with multiple CTEs",
			sql: `WITH 
   -- 第一个CTE：过滤出2023年的订单 
   orders_2023 AS ( 
     SELECT 
       order_id, 
       customer_id, 
       order_date, 
       total_amount 
     FROM orders 
     WHERE YEAR(order_date) = 2023 
   ), 
 
   -- 第二个CTE：计算每个客户的总消费金额 
   customer_spending AS ( 
     SELECT 
       customer_id, 
       SUM(total_amount) AS total_spent, 
       COUNT(order_id) AS order_count 
     FROM orders_2023 
     GROUP BY customer_id 
   ), 
 
   -- 第三个CTE：标记高价值客户 
   high_value_customers AS ( 
     SELECT 
       customer_id, 
       total_spent, 
       order_count, 
       CASE 
         WHEN total_spent > 10000 THEN '钻石客户' 
         WHEN total_spent > 5000 THEN '黄金客户' 
         ELSE '普通客户' 
       END AS customer_segment 
     FROM customer_spending 
   ) 
 
 -- 主查询：最终输出 
 SELECT 
   hvc.customer_segment, 
   COUNT(DISTINCT hvc.customer_id) AS customer_count, 
   AVG(hvc.total_spent) AS avg_spent, 
   SUM(hvc.total_spent) AS segment_total 
 FROM high_value_customers hvc 
 WHERE hvc.order_count >= 2 
 GROUP BY hvc.customer_segment 
 ORDER BY segment_total DESC;`,
			expected: []string{`WITH 
   -- 第一个CTE：过滤出2023年的订单 
   orders_2023 AS ( 
     SELECT 
       order_id, 
       customer_id, 
       order_date, 
       total_amount 
     FROM orders 
     WHERE YEAR(order_date) = 2023 
   ), 
 
   -- 第二个CTE：计算每个客户的总消费金额 
   customer_spending AS ( 
     SELECT 
       customer_id, 
       SUM(total_amount) AS total_spent, 
       COUNT(order_id) AS order_count 
     FROM orders_2023 
     GROUP BY customer_id 
   ), 
 
   -- 第三个CTE：标记高价值客户 
   high_value_customers AS ( 
     SELECT 
       customer_id, 
       total_spent, 
       order_count, 
       CASE 
         WHEN total_spent > 10000 THEN '钻石客户' 
         WHEN total_spent > 5000 THEN '黄金客户' 
         ELSE '普通客户' 
       END AS customer_segment 
     FROM customer_spending 
   ) 
 
 -- 主查询：最终输出 
 SELECT 
   hvc.customer_segment, 
   COUNT(DISTINCT hvc.customer_id) AS customer_count, 
   AVG(hvc.total_spent) AS avg_spent, 
   SUM(hvc.total_spent) AS segment_total 
 FROM high_value_customers hvc 
 WHERE hvc.order_count >= 2 
 GROUP BY hvc.customer_segment 
 ORDER BY segment_total DESC;`},
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
