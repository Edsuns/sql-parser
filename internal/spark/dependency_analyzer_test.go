package spark

import (
	"strings"
	"testing"

	"github.com/Edsuns/sql-parser/analyzer"
	"github.com/stretchr/testify/assert"
)

func TestSparkDependencyAnalyzer(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected []*analyzer.DependencyResult
	}{
		// 基本查询语句
		{
			name: "single select statement lower case",
			sql:  "SELECT * from table1;",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "SELECT * from table1;",
					StmtType: analyzer.StmtTypeSelect,
					Read: []*analyzer.DependencyTable{
						{Cluster: "default_cluster", Database: "default_db", Table: "table1"},
					},
					Write: []*analyzer.DependencyTable{},
				},
			},
		},
		{
			name: "single select statement",
			sql:  "SELECT * FROM table1;",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "SELECT * FROM table1;",
					StmtType: analyzer.StmtTypeSelect,
					Read: []*analyzer.DependencyTable{
						{Cluster: "default_cluster", Database: "default_db", Table: "table1"},
					},
					Write: []*analyzer.DependencyTable{},
				},
			},
		},
		{
			name: "complex select with join",
			sql:  "SELECT t1.col1, t2.col2 FROM table1 t1 JOIN table2 t2 ON t1.id = t2.id;",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "SELECT t1.col1, t2.col2 FROM table1 t1 JOIN table2 t2 ON t1.id = t2.id;",
					StmtType: analyzer.StmtTypeSelect,
					Read: []*analyzer.DependencyTable{
						{Cluster: "default_cluster", Database: "default_db", Table: "table1"},
						{Cluster: "default_cluster", Database: "default_db", Table: "table2"},
					},
					Write: []*analyzer.DependencyTable{},
				},
			},
		},
		{
			name: "multiple read tables",
			sql:  "SELECT * FROM table1, table2, table3;",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "SELECT * FROM table1, table2, table3;",
					StmtType: analyzer.StmtTypeSelect,
					Read: []*analyzer.DependencyTable{
						{Cluster: "default_cluster", Database: "default_db", Table: "table1"},
						{Cluster: "default_cluster", Database: "default_db", Table: "table2"},
						{Cluster: "default_cluster", Database: "default_db", Table: "table3"},
					},
					Write: []*analyzer.DependencyTable{},
				},
			},
		},
		{
			name: "select with subquery",
			sql:  "SELECT * FROM (SELECT * FROM table1 WHERE id > 10) t;",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "SELECT * FROM (SELECT * FROM table1 WHERE id > 10) t;",
					StmtType: analyzer.StmtTypeSelect,
					Read: []*analyzer.DependencyTable{
						{Cluster: "default_cluster", Database: "default_db", Table: "table1"},
					},
					Write: []*analyzer.DependencyTable{},
				},
			},
		},
		{
			name: "join with different databases",
			sql:  "SELECT * FROM db1.table1 t1 JOIN db2.table2 t2 ON t1.id = t2.id;",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "SELECT * FROM db1.table1 t1 JOIN db2.table2 t2 ON t1.id = t2.id;",
					StmtType: analyzer.StmtTypeSelect,
					Read: []*analyzer.DependencyTable{
						{Cluster: "default_cluster", Database: "db1", Table: "table1"},
						{Cluster: "default_cluster", Database: "db2", Table: "table2"},
					},
					Write: []*analyzer.DependencyTable{},
				},
			},
		},
		// 数据修改语句
		{
			name: "insert statement",
			sql:  "INSERT INTO table2 VALUES (1, 'a');",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "INSERT INTO table2 VALUES (1, 'a');",
					StmtType: analyzer.StmtTypeInsert,
					Read:     []*analyzer.DependencyTable{},
					Write: []*analyzer.DependencyTable{
						{Cluster: "default_cluster", Database: "default_db", Table: "table2"},
					},
				},
			},
		},
		{
			name: "read and write in same statement",
			sql:  "INSERT INTO table2 SELECT * FROM table1;",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "INSERT INTO table2 SELECT * FROM table1;",
					StmtType: analyzer.StmtTypeInsert,
					Read: []*analyzer.DependencyTable{
						{Cluster: "default_cluster", Database: "default_db", Table: "table1"},
					},
					Write: []*analyzer.DependencyTable{
						{Cluster: "default_cluster", Database: "default_db", Table: "table2"},
					},
				},
			},
		},
		{
			name: "insert with select from multiple tables",
			sql:  "INSERT INTO table3 SELECT t1.id, t2.name FROM table1 t1 JOIN table2 t2 ON t1.id = t2.id;",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "INSERT INTO table3 SELECT t1.id, t2.name FROM table1 t1 JOIN table2 t2 ON t1.id = t2.id;",
					StmtType: analyzer.StmtTypeInsert,
					Read: []*analyzer.DependencyTable{
						{Cluster: "default_cluster", Database: "default_db", Table: "table1"},
						{Cluster: "default_cluster", Database: "default_db", Table: "table2"},
					},
					Write: []*analyzer.DependencyTable{
						{Cluster: "default_cluster", Database: "default_db", Table: "table3"},
					},
				},
			},
		},
		{
			name: "update statement",
			sql:  "UPDATE table1 SET col1 = 'new_value' WHERE id = 1;",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "UPDATE table1 SET col1 = 'new_value' WHERE id = 1;",
					StmtType: analyzer.StmtTypeUpdate,
					Read:     []*analyzer.DependencyTable{},
					Write: []*analyzer.DependencyTable{
						{Cluster: "default_cluster", Database: "default_db", Table: "table1"},
					},
				},
			},
		},
		{
			name: "delete statement",
			sql:  "DELETE FROM table1 WHERE id = 1;",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "DELETE FROM table1 WHERE id = 1;",
					StmtType: analyzer.StmtTypeDelete,
					Read:     []*analyzer.DependencyTable{},
					Write: []*analyzer.DependencyTable{
						{Cluster: "default_cluster", Database: "default_db", Table: "table1"},
					},
				},
			},
		},
		// 多语句测试
		{
			name: "multiple statements",
			sql:  "SELECT * FROM table1; INSERT INTO table2 VALUES (1, 'a');",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "SELECT * FROM table1;",
					StmtType: analyzer.StmtTypeSelect,
					Read: []*analyzer.DependencyTable{
						{Cluster: "default_cluster", Database: "default_db", Table: "table1"},
					},
					Write: []*analyzer.DependencyTable{},
				},
				{
					Stmt:     "INSERT INTO table2 VALUES (1, 'a');",
					StmtType: analyzer.StmtTypeInsert,
					Read:     []*analyzer.DependencyTable{},
					Write: []*analyzer.DependencyTable{
						{Cluster: "default_cluster", Database: "default_db", Table: "table2"},
					},
				},
			},
		},
		// 注释测试
		{
			name: "statement with comments",
			sql:  "-- This is a comment\nSELECT * FROM table1; -- Another comment",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "-- This is a comment\nSELECT * FROM table1;",
					StmtType: analyzer.StmtTypeSelect,
					Read: []*analyzer.DependencyTable{
						{Cluster: "default_cluster", Database: "default_db", Table: "table1"},
					},
					Write: []*analyzer.DependencyTable{},
				},
			},
		},
		{
			name:     "only comments",
			sql:      "-- This is a comment\n/* This is another comment */",
			expected: []*analyzer.DependencyResult{},
		},
		// 指定数据库和集群
		{
			name: "select with specified database",
			sql:  "SELECT * FROM db1.table1;",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "SELECT * FROM db1.table1;",
					StmtType: analyzer.StmtTypeSelect,
					Read: []*analyzer.DependencyTable{
						{Cluster: "default_cluster", Database: "db1", Table: "table1"},
					},
					Write: []*analyzer.DependencyTable{},
				},
			},
		},
		{
			name: "select with specified cluster and database",
			sql:  "SELECT * FROM cluster1.db1.table1;",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "SELECT * FROM cluster1.db1.table1;",
					StmtType: analyzer.StmtTypeSelect,
					Read: []*analyzer.DependencyTable{
						{Cluster: "cluster1", Database: "db1", Table: "table1"},
					},
					Write: []*analyzer.DependencyTable{},
				},
			},
		},
		{
			name: "insert with specified database",
			sql:  "INSERT INTO db2.table2 VALUES (1, 'a');",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "INSERT INTO db2.table2 VALUES (1, 'a');",
					StmtType: analyzer.StmtTypeInsert,
					Read:     []*analyzer.DependencyTable{},
					Write: []*analyzer.DependencyTable{
						{Cluster: "default_cluster", Database: "db2", Table: "table2"},
					},
				},
			},
		},
		// DDL语句测试
		{
			name: "create table statement",
			sql:  "CREATE TABLE table3 (id INT, name STRING);",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "CREATE TABLE table3 (id INT, name STRING);",
					StmtType: analyzer.StmtTypeCreateTable,
					Read:     []*analyzer.DependencyTable{},
					Write: []*analyzer.DependencyTable{
						{Cluster: "default_cluster", Database: "default_db", Table: "table3"},
					},
					Actions: []*analyzer.ActionTable{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "table3",
							Action:   analyzer.ActionTypeCreate,
							Columns: []*analyzer.ActionColumn{
								{Name: "id", Type: "INT", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
								{Name: "name", Type: "STRING", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
							},
						},
					},
				},
			},
		},
		{
			name: "create table statement 2",
			sql:  "CREATE TABLE table4 (id INT, name STRING);",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "CREATE TABLE table4 (id INT, name STRING);",
					StmtType: analyzer.StmtTypeCreateTable,
					Read:     []*analyzer.DependencyTable{},
					Write: []*analyzer.DependencyTable{
						{Cluster: "default_cluster", Database: "default_db", Table: "table4"},
					},
					Actions: []*analyzer.ActionTable{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "table4",
							Action:   analyzer.ActionTypeCreate,
							Columns: []*analyzer.ActionColumn{
								{Name: "id", Type: "INT", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
								{Name: "name", Type: "STRING", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
							},
						},
					},
				},
			},
		},
		{
			name: "create table as select from multiple tables",
			sql:  "CREATE TABLE table5 AS SELECT t1.id, t2.name FROM table1 t1 JOIN table2 t2 ON t1.id = t2.id;",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "CREATE TABLE table5 AS SELECT t1.id, t2.name FROM table1 t1 JOIN table2 t2 ON t1.id = t2.id;",
					StmtType: analyzer.StmtTypeCreateTable,
					Read: []*analyzer.DependencyTable{
						{Cluster: "default_cluster", Database: "default_db", Table: "table1"},
						{Cluster: "default_cluster", Database: "default_db", Table: "table2"},
					},
					Write: []*analyzer.DependencyTable{
						{Cluster: "default_cluster", Database: "default_db", Table: "table5"},
					},
					Actions: []*analyzer.ActionTable{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "table5",
							Action:   analyzer.ActionTypeCreate,
							Columns:  []*analyzer.ActionColumn{},
						},
					},
				},
			},
		},
		{
			name: "create view statement",
			sql:  "CREATE VIEW view1 AS SELECT * FROM table1;",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "CREATE VIEW view1 AS SELECT * FROM table1;",
					StmtType: analyzer.StmtTypeCreateView,
					Read: []*analyzer.DependencyTable{
						{Cluster: "default_cluster", Database: "default_db", Table: "table1"},
					},
					Write: []*analyzer.DependencyTable{
						{Cluster: "default_cluster", Database: "default_db", Table: "view1"},
					},
				},
			},
		},
		{
			name: "create view from multiple tables",
			sql:  "CREATE VIEW view2 AS SELECT t1.id, t2.name FROM table1 t1 JOIN table2 t2 ON t1.id = t2.id;",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "CREATE VIEW view2 AS SELECT t1.id, t2.name FROM table1 t1 JOIN table2 t2 ON t1.id = t2.id;",
					StmtType: analyzer.StmtTypeCreateView,
					Read: []*analyzer.DependencyTable{
						{Cluster: "default_cluster", Database: "default_db", Table: "table1"},
						{Cluster: "default_cluster", Database: "default_db", Table: "table2"},
					},
					Write: []*analyzer.DependencyTable{
						{Cluster: "default_cluster", Database: "default_db", Table: "view2"},
					},
				},
			},
		},
		{
			name: "alter table statement",
			sql:  "ALTER TABLE table1 ADD COLUMN age INT;",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "ALTER TABLE table1 ADD COLUMN age INT;",
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
								{Name: "age", Type: "INT", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeAlter},
							},
						},
					},
				},
			},
		},
		{
			name: "replace table statement",
			sql:  "REPLACE TABLE table1 (id INT, name STRING, age INT);",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "REPLACE TABLE table1 (id INT, name STRING, age INT);",
					StmtType: analyzer.StmtTypeReplaceTable,
					Read:     []*analyzer.DependencyTable{},
					Write: []*analyzer.DependencyTable{
						{Cluster: "default_cluster", Database: "default_db", Table: "table1"},
					},
				},
			},
		},
		{
			name: "replace table as select from multiple tables",
			sql:  "REPLACE TABLE table6 AS SELECT t1.id, t2.name FROM table1 t1 JOIN table2 t2 ON t1.id = t2.id;",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "REPLACE TABLE table6 AS SELECT t1.id, t2.name FROM table1 t1 JOIN table2 t2 ON t1.id = t2.id;",
					StmtType: analyzer.StmtTypeReplaceTable,
					Read: []*analyzer.DependencyTable{
						{Cluster: "default_cluster", Database: "default_db", Table: "table1"},
						{Cluster: "default_cluster", Database: "default_db", Table: "table2"},
					},
					Write: []*analyzer.DependencyTable{
						{Cluster: "default_cluster", Database: "default_db", Table: "table6"},
					},
				},
			},
		},
		{
			name: "drop table statement",
			sql:  "DROP TABLE table1;",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "DROP TABLE table1;",
					StmtType: analyzer.StmtTypeDropTable,
					Read:     []*analyzer.DependencyTable{},
					Write: []*analyzer.DependencyTable{
						{Cluster: "default_cluster", Database: "default_db", Table: "table1"},
					},
					Actions: []*analyzer.ActionTable{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "table1",
							Action:   analyzer.ActionTypeDrop,
							Columns:  []*analyzer.ActionColumn{},
						},
					},
				},
			},
		},
		{
			name: "multiple write tables (multi insert)",
			sql:  "FROM table1 INSERT INTO table2 SELECT * INSERT INTO table3 SELECT *;",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "FROM table1 INSERT INTO table2 SELECT * INSERT INTO table3 SELECT *;",
					StmtType: analyzer.StmtTypeInsert,
					Read:     []*analyzer.DependencyTable{},
					Write: []*analyzer.DependencyTable{
						{Cluster: "default_cluster", Database: "default_db", Table: "table1"},
						{Cluster: "default_cluster", Database: "default_db", Table: "table2"},
						{Cluster: "default_cluster", Database: "default_db", Table: "table3"},
					},
				},
			},
		},
		// CTE语句测试
		{
			name: "CTE statement with multiple CTEs",
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
			expected: []*analyzer.DependencyResult{
				{
					Stmt: `WITH 
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
					StmtType: analyzer.StmtTypeSelect,
					Read: []*analyzer.DependencyTable{
						{Cluster: "default_cluster", Database: "default_db", Table: "orders"},
					},
					Write: []*analyzer.DependencyTable{},
				},
			},
		},
		// 分号和注释在字符串中的测试
		{
			name: "SELECT with semicolon in string",
			sql:  "SELECT * FROM table1 WHERE name = 'test;string' AND comment = 'line1 -- comment in string';",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "SELECT * FROM table1 WHERE name = 'test;string' AND comment = 'line1 -- comment in string';",
					StmtType: analyzer.StmtTypeSelect,
					Read: []*analyzer.DependencyTable{
						{Cluster: "default_cluster", Database: "default_db", Table: "table1"},
					},
					Write: []*analyzer.DependencyTable{},
				},
			},
		},
		{
			name: "INSERT with semicolons in values",
			sql:  "INSERT INTO table2 VALUES (1, 'value;with;semicolons', 'comment--with-dashes');",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "INSERT INTO table2 VALUES (1, 'value;with;semicolons', 'comment--with-dashes');",
					StmtType: analyzer.StmtTypeInsert,
					Read:     []*analyzer.DependencyTable{},
					Write: []*analyzer.DependencyTable{
						{Cluster: "default_cluster", Database: "default_db", Table: "table2"},
					},
				},
			},
		},
		{
			name: "CREATE TABLE with comment containing --",
			sql:  "CREATE TABLE table3 (id INT, content STRING COMMENT 'table with -- comment in comment');",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "CREATE TABLE table3 (id INT, content STRING COMMENT 'table with -- comment in comment');",
					StmtType: analyzer.StmtTypeCreateTable,
					Read:     []*analyzer.DependencyTable{},
					Write: []*analyzer.DependencyTable{
						{Cluster: "default_cluster", Database: "default_db", Table: "table3"},
					},
					Actions: []*analyzer.ActionTable{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "table3",
							Action:   analyzer.ActionTypeCreate,
							Columns: []*analyzer.ActionColumn{
								{Name: "id", Type: "INT", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
								{Name: "content", Type: "STRING", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "COMMENT'table with -- comment in comment'", Action: analyzer.ActionTypeCreate},
							},
						},
					},
				},
			},
		},
		{
			name: "CREATE VIEW with -- in string",
			sql:  "CREATE VIEW view3 AS SELECT * FROM table1 WHERE description = '-- this is not a comment';",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "CREATE VIEW view3 AS SELECT * FROM table1 WHERE description = '-- this is not a comment';",
					StmtType: analyzer.StmtTypeCreateView,
					Read: []*analyzer.DependencyTable{
						{Cluster: "default_cluster", Database: "default_db", Table: "table1"},
					},
					Write: []*analyzer.DependencyTable{
						{Cluster: "default_cluster", Database: "default_db", Table: "view3"},
					},
				},
			},
		},
		// USE语句测试
		{
			name: "USE database statement",
			sql:  "USE db1;",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "USE db1;",
					StmtType: analyzer.StmtTypeUseDatabase,
					Read:     []*analyzer.DependencyTable{},
					Write:    []*analyzer.DependencyTable{},
				},
			},
		},
		// Action parsing tests - CREATE TABLE with various column types and constraints
		{
			name: "create table with complex columns and constraints",
			sql:  "CREATE TABLE users (id INT PRIMARY KEY, name STRING NOT NULL, age INT DEFAULT 18, email STRING COMMENT 'User email address');",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "CREATE TABLE users (id INT PRIMARY KEY, name STRING NOT NULL, age INT DEFAULT 18, email STRING COMMENT 'User email address');",
					StmtType: analyzer.StmtTypeCreateTable,
					Read:     []*analyzer.DependencyTable{},
					Write: []*analyzer.DependencyTable{
						{Cluster: "default_cluster", Database: "default_db", Table: "users"},
					},
					Actions: []*analyzer.ActionTable{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "users",
							Action:   analyzer.ActionTypeCreate,
							Columns: []*analyzer.ActionColumn{
								{Name: "id", Type: "INT", IsNotNull: false, IsPrimary: true, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
								{Name: "name", Type: "STRING", IsNotNull: true, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
								{Name: "age", Type: "INT", IsNotNull: false, IsPrimary: false, DefaultValue: "DEFAULT18", Comment: "", Action: analyzer.ActionTypeCreate},
								{Name: "email", Type: "STRING", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "COMMENT'User email address'", Action: analyzer.ActionTypeCreate},
							},
						},
					},
				},
			},
		},
		// Action parsing tests - ALTER TABLE operations
		{
			name: "alter table add column",
			sql:  "ALTER TABLE users ADD COLUMN phone STRING;",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "ALTER TABLE users ADD COLUMN phone STRING;",
					StmtType: analyzer.StmtTypeAlterTable,
					Read:     []*analyzer.DependencyTable{},
					Write: []*analyzer.DependencyTable{
						{Cluster: "default_cluster", Database: "default_db", Table: "users"},
					},
					Actions: []*analyzer.ActionTable{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "users",
							Action:   analyzer.ActionTypeAlter,
							Columns: []*analyzer.ActionColumn{
								{Name: "phone", Type: "STRING", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeAlter},
							},
						},
					},
				},
			},
		},
		{
			name: "alter table rename column",
			sql:  "ALTER TABLE users RENAME COLUMN phone TO mobile;",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "ALTER TABLE users RENAME COLUMN phone TO mobile;",
					StmtType: analyzer.StmtTypeAlterTable,
					Read:     []*analyzer.DependencyTable{},
					Write: []*analyzer.DependencyTable{
						{Cluster: "default_cluster", Database: "default_db", Table: "users"},
					},
					Actions: []*analyzer.ActionTable{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "users",
							Action:   analyzer.ActionTypeAlter,
							Columns: []*analyzer.ActionColumn{
								{Name: "mobile", Type: "", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeAlter},
							},
						},
					},
				},
			},
		},
		{
			name: "alter table rename table",
			sql:  "ALTER TABLE users RENAME TO new_users;",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "ALTER TABLE users RENAME TO new_users;",
					StmtType: analyzer.StmtTypeAlterTable,
					Read:     []*analyzer.DependencyTable{},
					Write: []*analyzer.DependencyTable{
						{Cluster: "default_cluster", Database: "default_db", Table: "users"},
					},
					Actions: []*analyzer.ActionTable{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "users",
							Action:   analyzer.ActionTypeAlter,
						},
					},
				},
			},
		},
		{
			name: "alter table drop column",
			sql:  "ALTER TABLE users DROP COLUMN mobile;",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "ALTER TABLE users DROP COLUMN mobile;",
					StmtType: analyzer.StmtTypeAlterTable,
					Read:     []*analyzer.DependencyTable{},
					Write: []*analyzer.DependencyTable{
						{Cluster: "default_cluster", Database: "default_db", Table: "users"},
					},
					Actions: []*analyzer.ActionTable{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "users",
							Action:   analyzer.ActionTypeAlter,
							Columns: []*analyzer.ActionColumn{
								{Name: "mobile", Type: "", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeDrop},
							},
						},
					},
				},
			},
		},
		// Action parsing tests - Complex CREATE TABLE from Spark docs
		{
			name: "create table with partitions and options",
			sql:  "CREATE TABLE sales (id INT, product STRING, amount DOUBLE) PARTITIONED BY (sale_date DATE) STORED AS PARQUET;",
			expected: []*analyzer.DependencyResult{
				{
					Stmt:     "CREATE TABLE sales (id INT, product STRING, amount DOUBLE) PARTITIONED BY (sale_date DATE) STORED AS PARQUET;",
					StmtType: analyzer.StmtTypeCreateTable,
					Read:     []*analyzer.DependencyTable{},
					Write: []*analyzer.DependencyTable{
						{Cluster: "default_cluster", Database: "default_db", Table: "sales"},
					},
					Actions: []*analyzer.ActionTable{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "sales",
							Action:   analyzer.ActionTypeCreate,
							Columns: []*analyzer.ActionColumn{
								{Name: "id", Type: "INT", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
								{Name: "product", Type: "STRING", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
								{Name: "amount", Type: "DOUBLE", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
							},
						},
					},
				},
			},
		},
	}

	sparkAnalyzer := NewDependencyAnalyzer()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &analyzer.DependencyAnalyzeReq{
				DefaultCluster:  "default_cluster",
				DefaultDatabase: "default_db",
				Type:            analyzer.EngineSpark,
				SQL:             tt.sql,
			}

			results, err := sparkAnalyzer.Analyze(req)
			assert.NoError(t, err)
			if assert.Equal(t, len(tt.expected), len(results)) {
				for i, result := range results {
					expected := tt.expected[i]
					assert.Equal(t, expected.Stmt, result.Stmt)
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
					if expected.Actions == nil {
						assert.Empty(t, result.Actions)
					} else {
						assert.Equal(t, len(expected.Actions), len(result.Actions))
						for j, actionTable := range result.Actions {
							expectedActionTable := expected.Actions[j]
							assert.Equal(t, expectedActionTable.Cluster, actionTable.Cluster)
							assert.Equal(t, expectedActionTable.Database, actionTable.Database)
							assert.Equal(t, expectedActionTable.Table, actionTable.Table)
							assert.Equal(t, expectedActionTable.Action, actionTable.Action)
							assert.Equal(t, len(expectedActionTable.Columns), len(actionTable.Columns))
							for k, actionColumn := range actionTable.Columns {
								expectedActionColumn := expectedActionTable.Columns[k]
								assert.Equal(t, expectedActionColumn.Name, actionColumn.Name)
								assert.Equal(t, expectedActionColumn.Type, actionColumn.Type)
								assert.Equal(t, expectedActionColumn.IsNotNull, actionColumn.IsNotNull)
								assert.Equal(t, expectedActionColumn.IsPrimary, actionColumn.IsPrimary)
								assert.Equal(t, expectedActionColumn.DefaultValue, actionColumn.DefaultValue)
								assert.Equal(t, expectedActionColumn.Comment, actionColumn.Comment)
								assert.Equal(t, expectedActionColumn.Action, actionColumn.Action)
							}
						}
					}
				}
			}
		})
	}
}

func TestSparkDependencyAnalyzer_SyntaxError(t *testing.T) {
	tests := []struct {
		name          string
		sql           string
		expectedError string
	}{
		{
			name:          "MERGE INTO syntax error",
			sql:           "MERGE INTO table1 t1 USING table2 t2 ON t1.id = t2.id WHEN MATCHED THEN UPDATE SET t1.name = t2.name WHEN NOT MATCHED THEN INSERT VALUES (t2.id, t2.name);",
			expectedError: "at input 'INSERT VALUES'",
		},
		{
			name:          "CREATE TABLE with comment in partition error",
			sql:           "CREATE TABLE IF NOT EXISTS sl_dw_sp_preview.adv_dwv_sp_flow_delivery_detail_flow_v2_fht (platform STRING COMMENT '@pk 平台标识，枚举：ec1(2)、ec2(4)、shopify(1)、others(8)', bulk_id STRING COMMENT '@pk bulkId') COMMENT 'sp送达事件终态表' PARTITIONED BY (index_dt comment '分区日期，YYYY-MM-DD')",
			expectedError: "extraneous input ''分区日期，YYYY-MM-DD'' expecting {')', ','}",
		},
		{
			name:          "CREATE TABLE with correct comment in partition",
			sql:           "CREATE TABLE IF NOT EXISTS sl_dw_sp_preview.adv_dwv_sp_flow_delivery_detail_flow_v2_fht (platform STRING COMMENT '@pk 平台标识，枚举：ec1(2)、ec2(4)、shopify(1)、others(8)', bulk_id STRING COMMENT '@pk bulkId') COMMENT 'sp送达事件终态表' PARTITIONED BY (index_dt) comment '分区日期，YYYY-MM-DD'",
			expectedError: "",
		},
	}

	sparkAnalyzer := NewDependencyAnalyzer()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &analyzer.DependencyAnalyzeReq{
				DefaultCluster:  "default_cluster",
				DefaultDatabase: "default_db",
				Type:            analyzer.EngineSpark,
				SQL:             tt.sql,
			}

			_, err := sparkAnalyzer.Analyze(req)
			if tt.expectedError == "" {
				if err != nil {
					t.Errorf("Expected no syntax error, actual: %s", err)
				}
				return
			}

			if err == nil || !strings.Contains(err.Error(), tt.expectedError) {
				t.Errorf("Expected syntax error containing '%s', actual: %s", tt.expectedError, err)
			}
		})
	}
}
