package spark

import (
	"fmt"
	"sql-parser/analyzer"
	"strings"
	"testing"
)

func TestAnalyze(t *testing.T) {
	tests := []struct {
		name     string
		req      *analyzer.DependencyAnalyzeReq
		expected int // 期望返回的Dependencies数量
	}{{
		name: "single select statement lower case",
		req: &analyzer.DependencyAnalyzeReq{
			DefaultCluster:  "default_cluster",
			DefaultDatabase: "default_db",
			Type:            analyzer.SQLTypeSpark,
			SQL:             "SELECT * from table1;",
		},
		expected: 1,
	}, {
		name: "single select statement",
		req: &analyzer.DependencyAnalyzeReq{
			DefaultCluster:  "default_cluster",
			DefaultDatabase: "default_db",
			Type:            analyzer.SQLTypeSpark,
			SQL:             "SELECT * FROM table1;",
		},
		expected: 1,
	}, {
		name: "insert statement",
		req: &analyzer.DependencyAnalyzeReq{
			DefaultCluster:  "default_cluster",
			DefaultDatabase: "default_db",
			Type:            analyzer.SQLTypeSpark,
			SQL:             "INSERT INTO table2 VALUES (1, 'a');",
		},
		expected: 1,
	}, {
		name: "multiple statements",
		req: &analyzer.DependencyAnalyzeReq{
			DefaultCluster:  "default_cluster",
			DefaultDatabase: "default_db",
			Type:            analyzer.SQLTypeSpark,
			SQL:             "SELECT * FROM table1; INSERT INTO table2 VALUES (1, 'a');",
		},
		expected: 2,
	}, {
		name: "statement with comments",
		req: &analyzer.DependencyAnalyzeReq{
			DefaultCluster:  "default_cluster",
			DefaultDatabase: "default_db",
			Type:            analyzer.SQLTypeSpark,
			SQL:             "-- This is a comment\nSELECT * FROM table1; -- Another comment",
		},
		expected: 1,
	}, {
		name: "only comments",
		req: &analyzer.DependencyAnalyzeReq{
			DefaultCluster:  "default_cluster",
			DefaultDatabase: "default_db",
			Type:            analyzer.SQLTypeSpark,
			SQL:             "-- This is a comment\n/* This is another comment */",
		},
		expected: 0,
	}, {
		name: "complex select with join",
		req: &analyzer.DependencyAnalyzeReq{
			DefaultCluster:  "default_cluster",
			DefaultDatabase: "default_db",
			Type:            analyzer.SQLTypeSpark,
			SQL:             "SELECT t1.col1, t2.col2 FROM table1 t1 JOIN table2 t2 ON t1.id = t2.id;",
		},
		expected: 1,
	}, {
		name: "update statement",
		req: &analyzer.DependencyAnalyzeReq{
			DefaultCluster:  "default_cluster",
			DefaultDatabase: "default_db",
			Type:            analyzer.SQLTypeSpark,
			SQL:             "UPDATE table1 SET col1 = 'new_value' WHERE id = 1;",
		},
		expected: 1,
	}, {
		name: "delete statement",
		req: &analyzer.DependencyAnalyzeReq{
			DefaultCluster:  "default_cluster",
			DefaultDatabase: "default_db",
			Type:            analyzer.SQLTypeSpark,
			SQL:             "DELETE FROM table1 WHERE id = 1;",
		},
		expected: 1,
	}, {
		name: "create table statement",
		req: &analyzer.DependencyAnalyzeReq{
			DefaultCluster:  "default_cluster",
			DefaultDatabase: "default_db",
			Type:            analyzer.SQLTypeSpark,
			SQL:             "CREATE TABLE table3 (id INT, name STRING);",
		},
		expected: 1,
	}, {
		name: "select with specified database",
		req: &analyzer.DependencyAnalyzeReq{
			DefaultCluster:  "default_cluster",
			DefaultDatabase: "default_db",
			Type:            analyzer.SQLTypeSpark,
			SQL:             "SELECT * FROM db1.table1;",
		},
		expected: 1,
	}, {
		name: "select with specified cluster and database",
		req: &analyzer.DependencyAnalyzeReq{
			DefaultCluster:  "default_cluster",
			DefaultDatabase: "default_db",
			Type:            analyzer.SQLTypeSpark,
			SQL:             "SELECT * FROM cluster1.db1.table1;",
		},
		expected: 1,
	}, {
		name: "insert with specified database",
		req: &analyzer.DependencyAnalyzeReq{
			DefaultCluster:  "default_cluster",
			DefaultDatabase: "default_db",
			Type:            analyzer.SQLTypeSpark,
			SQL:             "INSERT INTO db2.table2 VALUES (1, 'a');",
		},
		expected: 1,
	}, {
		name: "multiple read tables",
		req: &analyzer.DependencyAnalyzeReq{
			DefaultCluster:  "default_cluster",
			DefaultDatabase: "default_db",
			Type:            analyzer.SQLTypeSpark,
			SQL:             "SELECT * FROM table1, table2, table3;",
		},
		expected: 1,
	}, {
		name: "multiple write tables (multi insert)",
		req: &analyzer.DependencyAnalyzeReq{
			DefaultCluster:  "default_cluster",
			DefaultDatabase: "default_db",
			Type:            analyzer.SQLTypeSpark,
			SQL:             "FROM table1 INSERT INTO table2 SELECT * INSERT INTO table3 SELECT *;",
		},
		expected: 1,
	}, {
		name: "read and write in same statement",
		req: &analyzer.DependencyAnalyzeReq{
			DefaultCluster:  "default_cluster",
			DefaultDatabase: "default_db",
			Type:            analyzer.SQLTypeSpark,
			SQL:             "INSERT INTO table2 SELECT * FROM table1;",
		},
		expected: 1,
	}, {
		name: "select with subquery",
		req: &analyzer.DependencyAnalyzeReq{
			DefaultCluster:  "default_cluster",
			DefaultDatabase: "default_db",
			Type:            analyzer.SQLTypeSpark,
			SQL:             "SELECT * FROM (SELECT * FROM table1 WHERE id > 10) t;",
		},
		expected: 1,
	}, {
		name: "join with different databases",
		req: &analyzer.DependencyAnalyzeReq{
			DefaultCluster:  "default_cluster",
			DefaultDatabase: "default_db",
			Type:            analyzer.SQLTypeSpark,
			SQL:             "SELECT * FROM db1.table1 t1 JOIN db2.table2 t2 ON t1.id = t2.id;",
		},
		expected: 1,
	}, {
		name: "insert with select from multiple tables",
		req: &analyzer.DependencyAnalyzeReq{
			DefaultCluster:  "default_cluster",
			DefaultDatabase: "default_db",
			Type:            analyzer.SQLTypeSpark,
			SQL:             "INSERT INTO table3 SELECT t1.id, t2.name FROM table1 t1 JOIN table2 t2 ON t1.id = t2.id;",
		},
		expected: 1,
	}, {
		name: "create table statement",
		req: &analyzer.DependencyAnalyzeReq{
			DefaultCluster:  "default_cluster",
			DefaultDatabase: "default_db",
			Type:            analyzer.SQLTypeSpark,
			SQL:             "CREATE TABLE table4 (id INT, name STRING);",
		},
		expected: 1,
	}, {
		name: "create table as select from multiple tables",
		req: &analyzer.DependencyAnalyzeReq{
			DefaultCluster:  "default_cluster",
			DefaultDatabase: "default_db",
			Type:            analyzer.SQLTypeSpark,
			SQL:             "CREATE TABLE table5 AS SELECT t1.id, t2.name FROM table1 t1 JOIN table2 t2 ON t1.id = t2.id;",
		},
		expected: 1,
	}, {
		name: "create view statement",
		req: &analyzer.DependencyAnalyzeReq{
			DefaultCluster:  "default_cluster",
			DefaultDatabase: "default_db",
			Type:            analyzer.SQLTypeSpark,
			SQL:             "CREATE VIEW view1 AS SELECT * FROM table1;",
		},
		expected: 1,
	}, {
		name: "create view from multiple tables",
		req: &analyzer.DependencyAnalyzeReq{
			DefaultCluster:  "default_cluster",
			DefaultDatabase: "default_db",
			Type:            analyzer.SQLTypeSpark,
			SQL:             "CREATE VIEW view2 AS SELECT t1.id, t2.name FROM table1 t1 JOIN table2 t2 ON t1.id = t2.id;",
		},
		expected: 1,
	}, {
		name: "alter table statement",
		req: &analyzer.DependencyAnalyzeReq{
			DefaultCluster:  "default_cluster",
			DefaultDatabase: "default_db",
			Type:            analyzer.SQLTypeSpark,
			SQL:             "ALTER TABLE table1 ADD COLUMN age INT;",
		},
		expected: 1,
	}, {
		name: "replace table statement",
		req: &analyzer.DependencyAnalyzeReq{
			DefaultCluster:  "default_cluster",
			DefaultDatabase: "default_db",
			Type:            analyzer.SQLTypeSpark,
			SQL:             "REPLACE TABLE table1 (id INT, name STRING, age INT);",
		},
		expected: 1,
	}, {
		name: "replace table as select from multiple tables",
		req: &analyzer.DependencyAnalyzeReq{
			DefaultCluster:  "default_cluster",
			DefaultDatabase: "default_db",
			Type:            analyzer.SQLTypeSpark,
			SQL:             "REPLACE TABLE table6 AS SELECT t1.id, t2.name FROM table1 t1 JOIN table2 t2 ON t1.id = t2.id;",
		},
		expected: 1,
	}, {
		name: "drop table statement",
		req: &analyzer.DependencyAnalyzeReq{
			DefaultCluster:  "default_cluster",
			DefaultDatabase: "default_db",
			Type:            analyzer.SQLTypeSpark,
			SQL:             "DROP TABLE table1;",
		},
		expected: 1,
	}, {
		name: "CTE statement with multiple CTEs",
		req: &analyzer.DependencyAnalyzeReq{
			DefaultCluster:  "default_cluster",
			DefaultDatabase: "default_db",
			Type:            analyzer.SQLTypeSpark,
			SQL: `WITH 
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
		},
		expected: 1,
	},
	}

	analyzer := NewDependencyAnalyzer()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := analyzer.Analyze(tt.req)
			if err != nil {
				t.Fatalf("Analyze failed: %v", err)
			}
			if len(result) != tt.expected {
				t.Errorf("Expected %d dependencies, got %d", tt.expected, len(result))
			}
		})
	}
}

func TestSyntaxErrorListener_SyntaxError(t *testing.T) {
	req := &analyzer.DependencyAnalyzeReq{
		DefaultCluster:  "default_cluster",
		DefaultDatabase: "default_db",
		Type:            analyzer.SQLTypeSpark,
		SQL:             "MERGE INTO table1 t1 USING table2 t2 ON t1.id = t2.id WHEN MATCHED THEN UPDATE SET t1.name = t2.name WHEN NOT MATCHED THEN INSERT VALUES (t2.id, t2.name);",
	}
	a := NewDependencyAnalyzer()
	_, err := a.Analyze(req)
	if err == nil || !strings.Contains(err.Error(), "at input 'INSERT VALUES'") {
		t.Errorf("Expected syntax error, actual: %s", err)
	}

	req = &analyzer.DependencyAnalyzeReq{
		DefaultCluster:  "default_cluster",
		DefaultDatabase: "default_db",
		Type:            analyzer.SQLTypeSpark,
		SQL:             "CREATE TABLE IF NOT EXISTS sl_dw_sp_preview.adv_dwv_sp_flow_delivery_detail_flow_v2_fht (\n  platform STRING COMMENT '@pk 平台标识，枚举：ec1(2)、ec2(4)、shopify(1)、others(8)',\n  --task_type STRING COMMENT '任务类型，枚举：flow 自动化 / market 营销活动',\n  bulk_id STRING COMMENT '@pk bulkId',\n  user_id STRING COMMENT '@pk 用户id',\n  store_id STRING COMMENT '店铺ID',\n  task_id STRING COMMENT 'flowId/活动id',\n  object_type STRING COMMENT '对象类型，枚举：sms 短信 /email 邮件',\n  event_time BIGINT COMMENT '事件时间戳(毫秒)',\n  delivery_status STRING COMMENT '送达状态',\n  is_onboarding tinyint COMMENT '是否是onboarding数据,false不是/true是',\n  is_offline tinyint COMMENT '是否是离线,false不是/true是',\n  update_time STRING COMMENT '更新时间',\n  send_time BIGINT COMMENT '活动开始时间',\n  node_id STRING COMMENT '节点id',\n  recipient_domain string comment '收信域名',\n  sender_domain string comment '发信域名',\n  email_provider string comment '邮件服务商',\n  id string comment 'mongo对应的业务主键id',\n  recipient string comment '收信人邮箱/手机号',\n  error_name string comment '错误名称',\n  dt string COMMENT '发送时间对应的分区，YYYY-MM-DD'\n) COMMENT 'sp送达事件终态表' PARTITIONED BY (index_dt comment '分区日期，YYYY-MM-DD')",
	}
	_, err = a.Analyze(req)
	if err == nil || !strings.Contains(err.Error(), "extraneous input ''分区日期，YYYY-MM-DD'' expecting {')', ','}") {
		t.Errorf("Expected syntax error, actual: %s", err)
	}

	req = &analyzer.DependencyAnalyzeReq{
		DefaultCluster:  "default_cluster",
		DefaultDatabase: "default_db",
		Type:            analyzer.SQLTypeSpark,
		SQL:             "CREATE TABLE IF NOT EXISTS sl_dw_sp_preview.adv_dwv_sp_flow_delivery_detail_flow_v2_fht (\n  platform STRING COMMENT '@pk 平台标识，枚举：ec1(2)、ec2(4)、shopify(1)、others(8)',\n  --task_type STRING COMMENT '任务类型，枚举：flow 自动化 / market 营销活动',\n  bulk_id STRING COMMENT '@pk bulkId',\n  user_id STRING COMMENT '@pk 用户id',\n  store_id STRING COMMENT '店铺ID',\n  task_id STRING COMMENT 'flowId/活动id',\n  object_type STRING COMMENT '对象类型，枚举：sms 短信 /email 邮件',\n  event_time BIGINT COMMENT '事件时间戳(毫秒)',\n  delivery_status STRING COMMENT '送达状态',\n  is_onboarding tinyint COMMENT '是否是onboarding数据,false不是/true是',\n  is_offline tinyint COMMENT '是否是离线,false不是/true是',\n  update_time STRING COMMENT '更新时间',\n  send_time BIGINT COMMENT '活动开始时间',\n  node_id STRING COMMENT '节点id',\n  recipient_domain string comment '收信域名',\n  sender_domain string comment '发信域名',\n  email_provider string comment '邮件服务商',\n  id string comment 'mongo对应的业务主键id',\n  recipient string comment '收信人邮箱/手机号',\n  error_name string comment '错误名称',\n  dt string COMMENT '发送时间对应的分区，YYYY-MM-DD'\n) COMMENT 'sp送达事件终态表' PARTITIONED BY (index_dt) comment '分区日期，YYYY-MM-DD'",
	}
	_, err = a.Analyze(req)
	if err != nil {
		t.Errorf("Expected no syntax error, actual: %s", err)
	}
}

// TestAnalyzeWithBasicTableExtraction 测试基本的表提取功能
func TestAnalyzeWithBasicTableExtraction(t *testing.T) {
	req := &analyzer.DependencyAnalyzeReq{
		DefaultCluster:  "test_cluster",
		DefaultDatabase: "test_db",
		Type:            analyzer.SQLTypeSpark,
		SQL:             "SELECT * FROM table1; INSERT INTO table2 SELECT * FROM table3;",
	}

	analyzer := NewDependencyAnalyzer()
	result, err := analyzer.Analyze(req)
	if err != nil {
		t.Fatalf("Analyze failed: %v", err)
	}

	// 检查结果数量
	if len(result) != 2 {
		t.Errorf("Expected 2 dependencies, got %d", len(result))
		return
	}

	// 检查第一个语句（SELECT * FROM table1）
	if result[0].Stmt != "SELECT * FROM table1;" {
		t.Errorf("Expected first statement to be 'SELECT * FROM table1;', got '%s'", result[0].Stmt)
	}
	if len(result[0].Read) != 1 {
		t.Errorf("Expected 1 read table for first statement, got %d", len(result[0].Read))
	} else {
		readTable := result[0].Read[0]
		if readTable.Cluster != "test_cluster" || readTable.Database != "test_db" || readTable.Table != "table1" {
			t.Errorf("Expected read table to be (test_cluster, test_db, table1), got (%s, %s, %s)",
				readTable.Cluster, readTable.Database, readTable.Table)
		}
	}
	if len(result[0].Write) != 0 {
		t.Errorf("Expected 0 write tables for first statement, got %d", len(result[0].Write))
	}

	// 检查第二个语句（INSERT INTO table2 SELECT * FROM table3）
	if result[1].Stmt != "INSERT INTO table2 SELECT * FROM table3;" {
		t.Errorf("Expected second statement to be 'INSERT INTO table2 SELECT * FROM table3;', got '%s'", result[1].Stmt)
	}
	// 检查操作类型
	if result[1].StmtType != "INSERT" {
		t.Errorf("Expected OpType 'INSERT' for second statement, got '%s'", result[1].StmtType)
	}
	if len(result[1].Read) != 1 {
		t.Errorf("Expected 1 read table for second statement, got %d", len(result[1].Read))
	} else {
		readTable := result[1].Read[0]
		if readTable.Cluster != "test_cluster" || readTable.Database != "test_db" || readTable.Table != "table3" {
			t.Errorf("Expected read table to be (test_cluster, test_db, table3), got (%s, %s, %s)",
				readTable.Cluster, readTable.Database, readTable.Table)
		}
	}
	if len(result[1].Write) != 1 {
		t.Errorf("Expected 1 write table for second statement, got %d", len(result[1].Write))
	} else {
		writeTable := result[1].Write[0]
		if writeTable.Cluster != "test_cluster" || writeTable.Database != "test_db" || writeTable.Table != "table2" {
			t.Errorf("Expected write table to be (test_cluster, test_db, table2), got (%s, %s, %s)",
				writeTable.Cluster, writeTable.Database, writeTable.Table)
		}
	}
}

// TestAnalyzeWithSpecifiedDatabase 测试指定数据库的表提取
func TestAnalyzeWithSpecifiedDatabase(t *testing.T) {
	req := &analyzer.DependencyAnalyzeReq{
		DefaultCluster:  "test_cluster",
		DefaultDatabase: "test_db",
		Type:            analyzer.SQLTypeSpark,
		SQL:             "SELECT * FROM db1.table1; INSERT INTO db2.table2 VALUES (1, 'a');",
	}

	analyzer := NewDependencyAnalyzer()
	result, err := analyzer.Analyze(req)
	if err != nil {
		t.Fatalf("Analyze failed: %v", err)
	}

	// 检查结果数量
	if len(result) != 2 {
		t.Errorf("Expected 2 dependencies, got %d", len(result))
		return
	}

	// 检查第一个语句（SELECT * FROM db1.table1）
	// 检查操作类型
	if result[0].StmtType != "SELECT" {
		t.Errorf("Expected OpType 'SELECT' for first statement, got '%s'", result[0].StmtType)
	}
	if len(result[0].Read) != 1 {
		t.Errorf("Expected 1 read table for first statement, got %d", len(result[0].Read))
	} else {
		readTable := result[0].Read[0]
		if readTable.Cluster != "test_cluster" || readTable.Database != "db1" || readTable.Table != "table1" {
			t.Errorf("Expected read table to be (test_cluster, db1, table1), got (%s, %s, %s)",
				readTable.Cluster, readTable.Database, readTable.Table)
		}
	}

	// 检查第二个语句（INSERT INTO db2.table2 VALUES (1, 'a')）
	// 检查操作类型
	if result[1].StmtType != "INSERT" {
		t.Errorf("Expected OpType 'INSERT' for second statement, got '%s'", result[1].StmtType)
	}
	if len(result[1].Write) != 1 {
		t.Errorf("Expected 1 write table for second statement, got %d", len(result[1].Write))
	} else {
		writeTable := result[1].Write[0]
		if writeTable.Cluster != "test_cluster" || writeTable.Database != "db2" || writeTable.Table != "table2" {
			t.Errorf("Expected write table to be (test_cluster, db2, table2), got (%s, %s, %s)",
				writeTable.Cluster, writeTable.Database, writeTable.Table)
		}
	}
}

// TestAnalyzeWithMultipleReadTables 测试多个读表的提取
func TestAnalyzeWithMultipleReadTables(t *testing.T) {
	req := &analyzer.DependencyAnalyzeReq{
		DefaultCluster:  "test_cluster",
		DefaultDatabase: "test_db",
		Type:            analyzer.SQLTypeSpark,
		SQL:             "SELECT * FROM table1, table2, table3;",
	}

	analyzer := NewDependencyAnalyzer()
	result, err := analyzer.Analyze(req)
	if err != nil {
		t.Fatalf("Analyze failed: %v", err)
	}

	// 检查结果数量
	if len(result) != 1 {
		t.Errorf("Expected 1 dependency, got %d", len(result))
		return
	}

	// 检查操作类型
	if result[0].StmtType != "SELECT" {
		t.Errorf("Expected OpType 'SELECT' for statement, got '%s'", result[0].StmtType)
	}

	// 检查读表数量
	if len(result[0].Read) != 3 {
		t.Errorf("Expected 3 read tables, got %d", len(result[0].Read))
		return
	}

	// 检查写表数量
	if len(result[0].Write) != 0 {
		t.Errorf("Expected 0 write tables, got %d", len(result[0].Write))
	}
}

// TestAnalyzeWithDdlOperations 测试DDL操作的表提取，包括CREATE_TABLE、CREATE_VIEW、ALTER_TABLE、REPLACE_TABLE、DROP_TABLE
func TestAnalyzeWithDdlOperations(t *testing.T) {
	req := &analyzer.DependencyAnalyzeReq{
		DefaultCluster:  "test_cluster",
		DefaultDatabase: "test_db",
		Type:            analyzer.SQLTypeSpark,
		SQL: "CREATE TABLE table1 (id INT, name STRING); " +
			"CREATE VIEW view1 AS SELECT * FROM table1; " +
			"ALTER TABLE table1 ADD COLUMN age INT; " +
			"REPLACE TABLE table2 AS SELECT t1.id, t2.name FROM table1 t1 JOIN table3 t2 ON t1.id = t2.id; " +
			"DROP TABLE table4;",
	}

	analyzer := NewDependencyAnalyzer()
	result, err := analyzer.Analyze(req)
	if err != nil {
		t.Fatalf("Analyze failed: %v", err)
	}

	// 检查结果数量
	if len(result) != 5 {
		t.Errorf("Expected 5 dependencies, got %d", len(result))
		return
	}

	// 检查第一个语句（CREATE TABLE table1）
	if result[0].Stmt != "CREATE TABLE table1 (id INT, name STRING);" {
		t.Errorf("Expected first statement to be 'CREATE TABLE table1 (id INT, name STRING);', got '%s'", result[0].Stmt)
	}
	if len(result[0].Write) != 1 {
		t.Errorf("Expected 1 write table for CREATE TABLE, got %d", len(result[0].Write))
	} else {
		writeTable := result[0].Write[0]
		if writeTable.Cluster != "test_cluster" || writeTable.Database != "test_db" || writeTable.Table != "table1" {
			t.Errorf("Expected write table to be (test_cluster, test_db, table1), got (%s, %s, %s)",
				writeTable.Cluster, writeTable.Database, writeTable.Table)
		}
	}

	// 检查第二个语句（CREATE VIEW view1）
	if result[1].Stmt != "CREATE VIEW view1 AS SELECT * FROM table1;" {
		t.Errorf("Expected second statement to be 'CREATE VIEW view1 AS SELECT * FROM table1;', got '%s'", result[1].Stmt)
	}
	if len(result[1].Read) != 1 {
		t.Errorf("Expected 1 read table for CREATE VIEW, got %d", len(result[1].Read))
	} else {
		readTable := result[1].Read[0]
		if readTable.Cluster != "test_cluster" || readTable.Database != "test_db" || readTable.Table != "table1" {
			t.Errorf("Expected read table to be (test_cluster, test_db, table1), got (%s, %s, %s)",
				readTable.Cluster, readTable.Database, readTable.Table)
		}
	}
	if len(result[1].Write) != 1 {
		t.Errorf("Expected 1 write table for CREATE VIEW, got %d", len(result[1].Write))
	} else {
		writeTable := result[1].Write[0]
		if writeTable.Cluster != "test_cluster" || writeTable.Database != "test_db" || writeTable.Table != "view1" {
			t.Errorf("Expected write table to be (test_cluster, test_db, view1), got (%s, %s, %s)",
				writeTable.Cluster, writeTable.Database, writeTable.Table)
		}
	}

	// 检查第三个语句（ALTER TABLE table1）
	if result[2].Stmt != "ALTER TABLE table1 ADD COLUMN age INT;" {
		t.Errorf("Expected third statement to be 'ALTER TABLE table1 ADD COLUMN age INT;', got '%s'", result[2].Stmt)
	}
	if len(result[2].Write) != 1 {
		t.Errorf("Expected 1 write table for ALTER TABLE, got %d", len(result[2].Write))
	} else {
		writeTable := result[2].Write[0]
		if writeTable.Cluster != "test_cluster" || writeTable.Database != "test_db" || writeTable.Table != "table1" {
			t.Errorf("Expected write table to be (test_cluster, test_db, table1), got (%s, %s, %s)",
				writeTable.Cluster, writeTable.Database, writeTable.Table)
		}
	}

	// 检查第四个语句（REPLACE TABLE table2）
	if result[3].Stmt != "REPLACE TABLE table2 AS SELECT t1.id, t2.name FROM table1 t1 JOIN table3 t2 ON t1.id = t2.id;" {
		t.Errorf("Expected fourth statement to be 'REPLACE TABLE table2 AS SELECT t1.id, t2.name FROM table1 t1 JOIN table3 t2 ON t1.id = t2.id;', got '%s'", result[3].Stmt)
	}
	if len(result[3].Read) != 2 {
		t.Errorf("Expected 2 read tables for REPLACE TABLE, got %d", len(result[3].Read))
	} else {
		// 检查读表1（table1）
		if result[3].Read[0].Cluster != "test_cluster" || result[3].Read[0].Database != "test_db" || result[3].Read[0].Table != "table1" {
			t.Errorf("Expected first read table to be (test_cluster, test_db, table1), got (%s, %s, %s)",
				result[3].Read[0].Cluster, result[3].Read[0].Database, result[3].Read[0].Table)
		}
		// 检查读表2（table3）
		if result[3].Read[1].Cluster != "test_cluster" || result[3].Read[1].Database != "test_db" || result[3].Read[1].Table != "table3" {
			t.Errorf("Expected second read table to be (test_cluster, test_db, table3), got (%s, %s, %s)",
				result[3].Read[1].Cluster, result[3].Read[1].Database, result[3].Read[1].Table)
		}
	}
	if len(result[3].Write) != 1 {
		t.Errorf("Expected 1 write table for REPLACE TABLE, got %d", len(result[3].Write))
	} else {
		writeTable := result[3].Write[0]
		if writeTable.Cluster != "test_cluster" || writeTable.Database != "test_db" || writeTable.Table != "table2" {
			t.Errorf("Expected write table to be (test_cluster, test_db, table2), got (%s, %s, %s)",
				writeTable.Cluster, writeTable.Database, writeTable.Table)
		}
	}

	// 检查第五个语句（DROP TABLE table4）
	if result[4].Stmt != "DROP TABLE table4;" {
		t.Errorf("Expected fifth statement to be 'DROP TABLE table4;', got '%s'", result[4].Stmt)
	}
	if len(result[4].Write) != 1 {
		t.Errorf("Expected 1 write table for DROP TABLE, got %d", len(result[4].Write))
	} else {
		writeTable := result[4].Write[0]
		if writeTable.Cluster != "test_cluster" || writeTable.Database != "test_db" || writeTable.Table != "table4" {
			t.Errorf("Expected write table to be (test_cluster, test_db, table4), got (%s, %s, %s)",
				writeTable.Cluster, writeTable.Database, writeTable.Table)
		}
	}
}

// TestAnalyzeWithJoin 测试JOIN语句的表提取
func TestAnalyzeWithJoin(t *testing.T) {
	req := &analyzer.DependencyAnalyzeReq{
		DefaultCluster:  "test_cluster",
		DefaultDatabase: "test_db",
		Type:            analyzer.SQLTypeSpark,
		SQL:             "SELECT * FROM db1.table1 t1 JOIN db2.table2 t2 ON t1.id = t2.id;",
	}

	analyzer := NewDependencyAnalyzer()
	result, err := analyzer.Analyze(req)
	if err != nil {
		t.Fatalf("Analyze failed: %v", err)
	}

	// 检查结果数量
	if len(result) != 1 {
		t.Errorf("Expected 1 dependency, got %d", len(result))
		return
	}

	// 检查操作类型
	if result[0].StmtType != "SELECT" {
		t.Errorf("Expected OpType 'SELECT' for statement, got '%s'", result[0].StmtType)
	}

	// 检查读表数量
	if len(result[0].Read) != 2 {
		t.Errorf("Expected 2 read tables, got %d", len(result[0].Read))
		return
	}
	if result[0].Read[0].Database != "db1" || result[0].Read[0].Table != "table1" {
		t.Errorf("Expected read table, got %d", len(result[0].Read))
		return
	}
	if result[0].Read[1].Database != "db2" || result[0].Read[1].Table != "table2" {
		t.Errorf("Expected read table, got %s.%s", result[0].Read[1].Database, result[0].Read[1].Table)
		return
	}
}

// TestAnalyzeWithPartitionedTables 测试有分区的建表SQL
func TestAnalyzeWithPartitionedTables(t *testing.T) {
	req := &analyzer.DependencyAnalyzeReq{
		DefaultCluster:  "test_cluster",
		DefaultDatabase: "test_db",
		Type:            analyzer.SQLTypeSpark,
		SQL: "CREATE TABLE hive_partitioned (id INT, data MAP<INT, ARRAY<INT>>) PARTITIONED BY (dt STRING, country STRING) STORED AS PARQUET; " +
			"CREATE TABLE spark_partitioned (id INT, name STRING) USING parquet PARTITIONED BY (dt, country);",
	}

	analyzer := NewDependencyAnalyzer()
	result, err := analyzer.Analyze(req)
	if err != nil {
		t.Fatalf("Analyze failed: %v", err)
	}

	// 检查结果数量
	if len(result) != 2 {
		t.Errorf("Expected 2 dependencies, got %d", len(result))
		return
	}

	// 检查第一个语句（Hive风格分区表）
	if result[0].Stmt != "CREATE TABLE hive_partitioned (id INT, data MAP<INT, ARRAY<INT>>) PARTITIONED BY (dt STRING, country STRING) STORED AS PARQUET;" {
		t.Errorf("Expected first statement to be 'CREATE TABLE hive_partitioned (id INT, data MAP<INT, ARRAY<INT>>) PARTITIONED BY (dt STRING, country STRING) STORED AS PARQUET;', got '%s'", result[0].Stmt)
	}
	// 检查操作类型
	if result[0].StmtType != "CREATE_TABLE" {
		t.Errorf("Expected OpType 'CREATE_TABLE' for Hive partitioned table statement, got '%s'", result[0].StmtType)
	}
	if len(result[0].Write) != 1 {
		t.Errorf("Expected 1 write table for Hive partitioned table, got %d", len(result[0].Write))
	} else {
		writeTable := result[0].Write[0]
		if writeTable.Cluster != "test_cluster" || writeTable.Database != "test_db" || writeTable.Table != "hive_partitioned" {
			t.Errorf("Expected write table to be (test_cluster, test_db, hive_partitioned), got (%s, %s, %s)",
				writeTable.Cluster, writeTable.Database, writeTable.Table)
		}
	}

	// 检查第二个语句（Spark风格分区表）
	if result[1].Stmt != "CREATE TABLE spark_partitioned (id INT, name STRING) USING parquet PARTITIONED BY (dt, country);" {
		t.Errorf("Expected second statement to be 'CREATE TABLE spark_partitioned (id INT, name STRING) USING parquet PARTITIONED BY (dt, country);', got '%s'", result[1].Stmt)
	}
	// 检查操作类型
	if result[1].StmtType != "CREATE_TABLE" {
		t.Errorf("Expected OpType 'CREATE_TABLE' for Spark partitioned table statement, got '%s'", result[1].StmtType)
	}
	if len(result[1].Write) != 1 {
		t.Errorf("Expected 1 write table for Spark partitioned table, got %d", len(result[1].Write))
	} else {
		writeTable := result[1].Write[0]
		if writeTable.Cluster != "test_cluster" || writeTable.Database != "test_db" || writeTable.Table != "spark_partitioned" {
			t.Errorf("Expected write table to be (test_cluster, test_db, spark_partitioned), got (%s, %s, %s)",
				writeTable.Cluster, writeTable.Database, writeTable.Table)
		}
	}
}

// TestAnalyzeWithSemicolonAndCommentInString 测试SQL中包含分号和--注释符号的字符串
func TestAnalyzeWithSemicolonAndCommentInString(t *testing.T) {
	req := &analyzer.DependencyAnalyzeReq{
		DefaultCluster:  "test_cluster",
		DefaultDatabase: "test_db",
		Type:            analyzer.SQLTypeSpark,
		SQL: "SELECT * FROM table1 WHERE name = 'test;string' AND comment = 'line1 -- comment in string'; " +
			"INSERT INTO table2 VALUES (1, 'value;with;semicolons', 'comment--with-dashes'); " +
			"CREATE TABLE table3 (id INT, content STRING COMMENT 'table with -- comment in comment'); " +
			"CREATE VIEW view3 AS SELECT * FROM table1 WHERE description = '-- this is not a comment';",
	}

	analyzer := NewDependencyAnalyzer()
	result, err := analyzer.Analyze(req)
	if err != nil {
		t.Fatalf("Analyze failed: %v", err)
	}

	// 检查结果数量
	if len(result) != 4 {
		t.Errorf("Expected 4 dependencies, got %d", len(result))
		return
	}

	// 检查第一个语句（SELECT with semicolon in string）
	if result[0].Stmt != "SELECT * FROM table1 WHERE name = 'test;string' AND comment = 'line1 -- comment in string';" {
		t.Errorf("Expected first statement to be 'SELECT * FROM table1 WHERE name = 'test;string' AND comment = 'line1 -- comment in string';', got '%s'", result[0].Stmt)
	}
	if len(result[0].Read) != 1 {
		t.Errorf("Expected 1 read table for SELECT, got %d", len(result[0].Read))
	} else {
		readTable := result[0].Read[0]
		if readTable.Cluster != "test_cluster" || readTable.Database != "test_db" || readTable.Table != "table1" {
			t.Errorf("Expected read table to be (test_cluster, test_db, table1), got (%s, %s, %s)",
				readTable.Cluster, readTable.Database, readTable.Table)
		}
	}

	// 检查第二个语句（INSERT with semicolons in values）
	if result[1].Stmt != "INSERT INTO table2 VALUES (1, 'value;with;semicolons', 'comment--with-dashes');" {
		t.Errorf("Expected second statement to be 'INSERT INTO table2 VALUES (1, 'value;with;semicolons', 'comment--with-dashes');', got '%s'", result[1].Stmt)
	}
	if len(result[1].Write) != 1 {
		t.Errorf("Expected 1 write table for INSERT, got %d", len(result[1].Write))
	} else {
		writeTable := result[1].Write[0]
		if writeTable.Cluster != "test_cluster" || writeTable.Database != "test_db" || writeTable.Table != "table2" {
			t.Errorf("Expected write table to be (test_cluster, test_db, table2), got (%s, %s, %s)",
				writeTable.Cluster, writeTable.Database, writeTable.Table)
		}
	}

	// 检查第三个语句（CREATE TABLE with comment containing --）
	if result[2].Stmt != "CREATE TABLE table3 (id INT, content STRING COMMENT 'table with -- comment in comment');" {
		t.Errorf("Expected third statement to be 'CREATE TABLE table3 (id INT, content STRING COMMENT 'table with -- comment in comment');', got '%s'", result[2].Stmt)
	}
	if len(result[2].Write) != 1 {
		t.Errorf("Expected 1 write table for CREATE TABLE, got %d", len(result[2].Write))
	} else {
		writeTable := result[2].Write[0]
		if writeTable.Cluster != "test_cluster" || writeTable.Database != "test_db" || writeTable.Table != "table3" {
			t.Errorf("Expected write table to be (test_cluster, test_db, table3), got (%s, %s, %s)",
				writeTable.Cluster, writeTable.Database, writeTable.Table)
		}
	}

	// 检查第四个语句（CREATE VIEW with -- in string）
	if result[3].Stmt != "CREATE VIEW view3 AS SELECT * FROM table1 WHERE description = '-- this is not a comment';" {
		t.Errorf("Expected fourth statement to be 'CREATE VIEW view3 AS SELECT * FROM table1 WHERE description = '-- this is not a comment';', got '%s'", result[3].Stmt)
	}
	if len(result[3].Read) != 1 {
		t.Errorf("Expected 1 read table for CREATE VIEW, got %d", len(result[3].Read))
	} else {
		readTable := result[3].Read[0]
		if readTable.Cluster != "test_cluster" || readTable.Database != "test_db" || readTable.Table != "table1" {
			t.Errorf("Expected read table to be (test_cluster, test_db, table1), got (%s, %s, %s)",
				readTable.Cluster, readTable.Database, readTable.Table)
		}
	}
	if len(result[3].Write) != 1 {
		t.Errorf("Expected 1 write table for CREATE VIEW, got %d", len(result[3].Write))
	} else {
		writeTable := result[3].Write[0]
		if writeTable.Cluster != "test_cluster" || writeTable.Database != "test_db" || writeTable.Table != "view3" {
			t.Errorf("Expected write table to be (test_cluster, test_db, view3), got (%s, %s, %s)",
				writeTable.Cluster, writeTable.Database, writeTable.Table)
		}
	}
}

// TestCTEWithMultipleTables tests CTE statements with multiple tables
func TestCTEWithMultipleTables(t *testing.T) {
	sql := "WITH books_authored_by_rm AS (\n    SELECT *\n    FROM books b\n    LEFT JOIN book_authors ba ON b.id = ba.book_id\n    WHERE author_id = 2299112019\n), books_with_average_ratings AS (\n    SELECT\n        b.id AS book_id,\n        AVG(r.score) AS average_rating\n    FROM books_authored_by_rm b\n    LEFT JOIN ratings r ON b.id = r.book_id\n    GROUP BY b.id\n), books_with_orders AS (\n    SELECT\n        b.id AS book_id,\n        COUNT(*) AS orders\n    FROM books_authored_by_rm b\n    LEFT JOIN orders o ON b.id = o.book_id\n    GROUP BY b.id\n)\nSELECT\n    b.id AS `book_id`,\n    b.title AS `book_title`,\n    br.average_rating AS `average_rating`,\n    bo.orders AS `orders`\nFROM\n    books_authored_by_rm b\n    LEFT JOIN books_with_average_ratings br ON b.id = br.book_id\n    LEFT JOIN books_with_orders bo ON b.id = bo.book_id\n;"
	req := &analyzer.DependencyAnalyzeReq{
		DefaultCluster:  "test_cluster",
		DefaultDatabase: "test_db",
		Type:            analyzer.SQLTypeSpark,
		SQL:             sql,
	}

	analyzer := NewDependencyAnalyzer()
	result, err := analyzer.Analyze(req)
	if err != nil {
		t.Fatalf("Analyze failed: %v", err)
	}

	// 检查结果数量
	if len(result) != 1 {
		t.Errorf("Expected 1 dependency, got %d", len(result))
		return
	}

	// 检查操作类型
	if result[0].StmtType != "SELECT" {
		t.Errorf("Expected OpType 'SELECT' for CTE statement, got '%s'", result[0].StmtType)
	}

	// 检查读表数量
	if len(result[0].Read) != 4 {
		t.Errorf("Expected 4 read tables, got %d", len(result[0].Read))
		// 打印实际结果
		t.Logf("Actual read tables:")
		for _, table := range result[0].Read {
			t.Logf("  - %s.%s.%s", table.Cluster, table.Database, table.Table)
		}
		return
	} else {
		expected := []string{
			"test_cluster.test_db.books",
			"test_cluster.test_db.book_authors",
			"test_cluster.test_db.ratings",
			"test_cluster.test_db.orders",
		}
		for i, table := range result[0].Read {
			actual := fmt.Sprintf("%s.%s.%s", table.Cluster, table.Database, table.Table)
			if actual != expected[i] {
				t.Errorf("Expected read table %s, got %s", expected[i], actual)
			}
		}
	}

	// 检查写表数量
	if len(result[0].Write) != 0 {
		t.Errorf("Expected 0 write tables, got %d", len(result[0].Write))
	}
}
