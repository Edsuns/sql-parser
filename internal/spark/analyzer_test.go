package spark

import (
	"strings"
	"testing"

	"github.com/Edsuns/sql-parser/analyzer"
	"github.com/stretchr/testify/assert"
)

func TestSparkDependencyAnalyzer_SyntaxError(t *testing.T) {
	tests := []struct {
		name          string
		sql           string
		expectedError string
	}{
		{
			name:          "MERGE INTO syntax error",
			sql:           "MERGE INTO table1 t1 USING table2 t2 ON t1.id = t2.id WHEN MATCHED THEN UPDATE SET t1.col1 = t2.col1 WHEN NOT MATCHED THEN INSERT VALUES (t2.id, t2.col1);",
			expectedError: "at input 'INSERT VALUES'",
		},
		{
			name:          "CREATE TABLE with comment in partition error",
			sql:           "CREATE TABLE IF NOT EXISTS db1.table1 (col24 STRING COMMENT '@pk 标识1，枚举：val1(2)、val2(4)、val3(1)、val4(8)', col22 STRING COMMENT '@pk param1') COMMENT '测试1' PARTITIONED BY (col26 comment '测试2，YYYY-MM-DD')",
			expectedError: "extraneous input ''测试2，YYYY-MM-DD'' expecting {')', ','}",
		},
		{
			name:          "CREATE TABLE with correct comment in partition",
			sql:           "CREATE TABLE IF NOT EXISTS db1.table1 (col24 STRING COMMENT '@pk 标识1，枚举：val1(2)、val2(4)、val3(1)、val4(8)', col22 STRING COMMENT '@pk param1') COMMENT '测试1' PARTITIONED BY (col26) comment '测试2，YYYY-MM-DD'",
			expectedError: "",
		},
		{
			name:          "set properties",
			sql:           "alter table db2.table2 set\n  TBLPROPERTIES ('write-only' = 'true');",
			expectedError: "",
		},
		// Spark SQL 不支持 NULL / NOT NULL 约束
		{
			name:          "add bigint column with null",
			sql:           "alter table temp4 add column id bigint null",
			expectedError: "no viable alternative at input 'bigint'",
		},
	}

	sparkAnalyzer := NewSQLAnalyzer()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &analyzer.AnalyzeReq{
				DefaultCluster:  "default_cluster",
				DefaultDatabase: "default_db",
				Type:            analyzer.EngineSpark,
				SQL:             tt.sql,
			}

			_, err := sparkAnalyzer.AnalyzeLineage(req)
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

func TestSparkDependencyAnalyzer_AnalyzeLineage(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected []*analyzer.LineageResult
	}{
		{
			name: "single select statement (lower case)",
			sql:  "SELECT * FROM table1;",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "SELECT * FROM table1;",
					StmtType: analyzer.StmtTypeSelect,
					Reads: []*analyzer.Dependency{
						{Cluster: "default_cluster", Database: "default_db", Table: "table1"},
					},
					Writes: []*analyzer.Dependency{},
				},
			},
		},
		{
			name: "VERSION AS OF",
			sql: `select 
        s1
        ,s2
        ,col3
        ,s3
        ,s4 as s5
        ,s6
              ,case when length(decrypt(s3,'v11'))>20 then null else ipParser(decrypt(s3,'v11'))['country_name'] end as s7
     ,case when length(decrypt(s3, 'v11'))>20 then null else ipParser(decrypt(s3, 'v11'))['city_name'] end as s8
   from db3.table3
   VERSION AS OF'2026-04-01'
    where s9='v9'`,
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "select \n        s1\n        ,s2\n        ,col3\n        ,s3\n        ,s4 as s5\n        ,s6\n              ,case when length(decrypt(s3,'v11'))>20 then null else ipParser(decrypt(s3,'v11'))['country_name'] end as s7\n     ,case when length(decrypt(s3, 'v11'))>20 then null else ipParser(decrypt(s3, 'v11'))['city_name'] end as s8\n   from db3.table3\n   VERSION AS OF'2026-04-01'\n    where s9='v9'",
					StmtType: analyzer.StmtTypeSelect,
					Reads: []*analyzer.Dependency{
						{Cluster: "default_cluster", Database: "db3", Table: "table3"},
					},
					Writes: []*analyzer.Dependency{},
				},
			},
		},
		{
			name: "insert with select",
			sql:  "INSERT INTO table2 SELECT * FROM table1;",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "INSERT INTO table2 SELECT * FROM table1;",
					StmtType: analyzer.StmtTypeInsert,
					Reads: []*analyzer.Dependency{
						{Cluster: "default_cluster", Database: "default_db", Table: "table1"},
					},
					Writes: []*analyzer.Dependency{
						{Cluster: "default_cluster", Database: "default_db", Table: "table2"},
					},
				},
			},
		},
		{
			name: "insert with select from multiple tables",
			sql:  "INSERT INTO table3 SELECT t1.id, t2.col1 FROM table1 t1 JOIN table2 t2 ON t1.id = t2.id;",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "INSERT INTO table3 SELECT t1.id, t2.col1 FROM table1 t1 JOIN table2 t2 ON t1.id = t2.id;",
					StmtType: analyzer.StmtTypeInsert,
					Reads: []*analyzer.Dependency{
						{Cluster: "default_cluster", Database: "default_db", Table: "table1"},
						{Cluster: "default_cluster", Database: "default_db", Table: "table2"},
					},
					Writes: []*analyzer.Dependency{
						{Cluster: "default_cluster", Database: "default_db", Table: "table3"},
					},
				},
			},
		},
		{
			name: "multiple statements",
			sql:  "SELECT * FROM table1; INSERT INTO table2 VALUES (1, 'v1');",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "SELECT * FROM table1;",
					StmtType: analyzer.StmtTypeSelect,
					Reads: []*analyzer.Dependency{
						{Cluster: "default_cluster", Database: "default_db", Table: "table1"},
					},
					Writes: []*analyzer.Dependency{},
				},
				{
					Stmt:     "INSERT INTO table2 VALUES (1, 'v1');",
					StmtType: analyzer.StmtTypeInsert,
					Reads:    []*analyzer.Dependency{},
					Writes: []*analyzer.Dependency{
						{Cluster: "default_cluster", Database: "default_db", Table: "table2"},
					},
				},
			},
		},
		{
			name: "complex select with join",
			sql:  "SELECT t1.col20, t2.col21 FROM table1 t1 JOIN table2 t2 ON t1.id = t2.id;",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "SELECT t1.col20, t2.col21 FROM table1 t1 JOIN table2 t2 ON t1.id = t2.id;",
					StmtType: analyzer.StmtTypeSelect,
					Reads: []*analyzer.Dependency{
						{Cluster: "default_cluster", Database: "default_db", Table: "table1"},
						{Cluster: "default_cluster", Database: "default_db", Table: "table2"},
					},
					Writes: []*analyzer.Dependency{},
				},
			},
		},
		{
			name: "multiple read tables",
			sql:  "SELECT * FROM table1, table2, table3;",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "SELECT * FROM table1, table2, table3;",
					StmtType: analyzer.StmtTypeSelect,
					Reads: []*analyzer.Dependency{
						{Cluster: "default_cluster", Database: "default_db", Table: "table1"},
						{Cluster: "default_cluster", Database: "default_db", Table: "table2"},
						{Cluster: "default_cluster", Database: "default_db", Table: "table3"},
					},
					Writes: []*analyzer.Dependency{},
				},
			},
		},
		{
			name: "select with subquery",
			sql:  "SELECT * FROM (SELECT * FROM table1 WHERE id > 10) t;",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "SELECT * FROM (SELECT * FROM table1 WHERE id > 10) t;",
					StmtType: analyzer.StmtTypeSelect,
					Reads: []*analyzer.Dependency{
						{Cluster: "default_cluster", Database: "default_db", Table: "table1"},
					},
					Writes: []*analyzer.Dependency{},
				},
			},
		},
		{
			name: "join with different databases",
			sql:  "SELECT * FROM db1.table1 t1 JOIN db2.table2 t2 ON t1.id = t2.id;",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "SELECT * FROM db1.table1 t1 JOIN db2.table2 t2 ON t1.id = t2.id;",
					StmtType: analyzer.StmtTypeSelect,
					Reads: []*analyzer.Dependency{
						{Cluster: "default_cluster", Database: "db1", Table: "table1"},
						{Cluster: "default_cluster", Database: "db2", Table: "table2"},
					},
					Writes: []*analyzer.Dependency{},
				},
			},
		},
		{
			name: "SELECT statement with function",
			sql:  "SELECT j.job_id, MAX(j.job_type) job_type, CONCAT('test', j.objs) reads_writes FROM tb1 j",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "SELECT j.job_id, MAX(j.job_type) job_type, CONCAT('test', j.objs) reads_writes FROM tb1 j",
					StmtType: analyzer.StmtTypeSelect,
					Reads: []*analyzer.Dependency{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "tb1",
						},
					},
					Writes: []*analyzer.Dependency{},
				},
			},
		},
		{
			name: "create temporary table",
			sql:  "CREATE TEMPORARY TABLE temp2 AS select s10,s11,s9 from db4.table4  where length(s11)>0 and s11!=s9;",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "CREATE TEMPORARY TABLE temp2 AS select s10,s11,s9 from db4.table4  where length(s11)>0 and s11!=s9;",
					StmtType: analyzer.StmtTypeCreateTemporaryTable,
					Reads: []*analyzer.Dependency{
						{IsTemp: false, Cluster: "default_cluster", Database: "db4", Table: "table4"},
					},
					Writes: []*analyzer.Dependency{
						{IsTemp: true, Cluster: "default_cluster", Database: "default_db", Table: "temp2"},
					},
				},
			},
		},
		{
			name: "read temporary table",
			sql:  "CREATE TEMPORARY TABLE temp2 AS select s10,s11,s9 from db4.table4  where length(s11)>0 and s11!=s9; SELECT * from temp2;",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "CREATE TEMPORARY TABLE temp2 AS select s10,s11,s9 from db4.table4  where length(s11)>0 and s11!=s9;",
					StmtType: analyzer.StmtTypeCreateTemporaryTable,
					Reads: []*analyzer.Dependency{
						{IsTemp: false, Cluster: "default_cluster", Database: "db4", Table: "table4"},
					},
					Writes: []*analyzer.Dependency{
						{IsTemp: true, Cluster: "default_cluster", Database: "default_db", Table: "temp2"},
					},
				},
				{
					Stmt:     "SELECT * from temp2;",
					StmtType: analyzer.StmtTypeSelect,
					Reads: []*analyzer.Dependency{
						{IsTemp: true, Cluster: "default_cluster", Database: "default_db", Table: "temp2"},
					},
					Writes: []*analyzer.Dependency{},
				},
			},
		},
		{
			name: "read temporary view",
			sql:  "CREATE TEMPORARY VIEW temp3 AS select s10,s11,s9 from db4.table4  where length(s11)>0 and s11!=s9; SELECT * from temp3;",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "CREATE TEMPORARY VIEW temp3 AS select s10,s11,s9 from db4.table4  where length(s11)>0 and s11!=s9;",
					StmtType: analyzer.StmtTypeCreateTemporaryView,
					Reads: []*analyzer.Dependency{
						{IsTemp: false, Cluster: "default_cluster", Database: "db4", Table: "table4"},
					},
					Writes: []*analyzer.Dependency{
						{IsTemp: true, Cluster: "default_cluster", Database: "default_db", Table: "temp3"},
					},
				},
				{
					Stmt:     "SELECT * from temp3;",
					StmtType: analyzer.StmtTypeSelect,
					Reads: []*analyzer.Dependency{
						{IsTemp: true, Cluster: "default_cluster", Database: "default_db", Table: "temp3"},
					},
					Writes: []*analyzer.Dependency{},
				},
			},
		},
		// 数据修改语句
		{
			name: "insert statement",
			sql:  "INSERT INTO table2 VALUES (1, 'v1');",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "INSERT INTO table2 VALUES (1, 'v1');",
					StmtType: analyzer.StmtTypeInsert,
					Reads:    []*analyzer.Dependency{},
					Writes: []*analyzer.Dependency{
						{Cluster: "default_cluster", Database: "default_db", Table: "table2"},
					},
				},
			},
		},
		{
			name: "update statement",
			sql:  "UPDATE table1 SET col20 = 'v2' WHERE id = 1;",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "UPDATE table1 SET col20 = 'v2' WHERE id = 1;",
					StmtType: analyzer.StmtTypeUpdate,
					Reads:    []*analyzer.Dependency{},
					Writes: []*analyzer.Dependency{
						{Cluster: "default_cluster", Database: "default_db", Table: "table1"},
					},
				},
			},
		},
		{
			name: "call statement",
			sql:  "CALL sys.delete_tag(table => 'db8.table12', tag => '${date:y-m-d}');",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "CALL sys.delete_tag(table => 'db8.table12', tag => '${date:y-m-d}');",
					StmtType: analyzer.StmtTypeCall,
					Reads:    []*analyzer.Dependency{},
					Writes: []*analyzer.Dependency{
						{Cluster: "default_cluster", Database: "db8", Table: "table12"},
					},
				},
			},
		},
		{
			name: "call sys.set_tag",
			sql:  "CALL sys.set_tag(table => 'db8.table12', tag => '${date:y-m-d}');",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "CALL sys.set_tag(table => 'db8.table12', tag => '${date:y-m-d}');",
					StmtType: analyzer.StmtTypeCall,
					Reads:    []*analyzer.Dependency{},
					Writes: []*analyzer.Dependency{
						{Cluster: "default_cluster", Database: "db8", Table: "table12"},
					},
				},
			},
		},
		{
			name: "call sys.table_lineage",
			sql:  "CALL sys.table_lineage(table => 'db8.table12');",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "CALL sys.table_lineage(table => 'db8.table12');",
					StmtType: analyzer.StmtTypeCall,
					Reads: []*analyzer.Dependency{
						{Cluster: "default_cluster", Database: "db8", Table: "table12"},
					},
					Writes: []*analyzer.Dependency{},
				},
			},
		},
		{
			name: "CREATE TEMPORARY FUNCTION",
			sql:  "CREATE TEMPORARY FUNCTION func1 AS 'com.test1.udf.Func1';",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "CREATE TEMPORARY FUNCTION func1 AS 'com.test1.udf.Func1';",
					StmtType: analyzer.StmtTypeTempFunc,
					Reads:    []*analyzer.Dependency{},
					Writes:   []*analyzer.Dependency{},
				},
			},
		},
		{
			name: "create temporary table with complex select",
			sql: `create temporary table temp1 as 
select a.s9 
  ,a.s12 
  ,a.s13 
  ,sum(if(a.s14=0,a.s15,0)) s16 
  ,sum(if(a.s14=0,a.s17,0)) s18 
  ,max(if(a.s14=0 and a.s19=1,1,0)) s19 
  ,sum(if(a.s14=1,a.s15,0)) s20 
  ,sum(if(a.s14=1,a.s17,0)) s21 
from cluster1.db3.table5 a 
left semi join db3.table5 b on b.col28='${date:y-m-d}' and coalesce(b.s22,'')<>'v10' and a.s12=b.s12 and a.s13=b.s13;`,
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "create temporary table temp1 as \nselect a.s9 \n  ,a.s12 \n  ,a.s13 \n  ,sum(if(a.s14=0,a.s15,0)) s16 \n  ,sum(if(a.s14=0,a.s17,0)) s18 \n  ,max(if(a.s14=0 and a.s19=1,1,0)) s19 \n  ,sum(if(a.s14=1,a.s15,0)) s20 \n  ,sum(if(a.s14=1,a.s17,0)) s21 \nfrom cluster1.db3.table5 a \nleft semi join db3.table5 b on b.col28='${date:y-m-d}' and coalesce(b.s22,'')<>'v10' and a.s12=b.s12 and a.s13=b.s13;",
					StmtType: analyzer.StmtTypeCreateTemporaryTable,
					Reads: []*analyzer.Dependency{
						{IsTemp: false, Cluster: "cluster1", Database: "db3", Table: "table5"},
						{IsTemp: false, Cluster: "default_cluster", Database: "db3", Table: "table5"},
					},
					Writes: []*analyzer.Dependency{
						{IsTemp: true, Cluster: "default_cluster", Database: "default_db", Table: "temp1"},
					},
				},
			},
		},
		{
			name: "insert overwrite with CTE",
			sql: `with cte1 as ( 
  select s23, 
  CASE 
    WHEN s23 LIKE 'v12%' 
     AND size(split(s23, '_')) > 3 
     AND split(s23, '_')[size(split(s23, '_')) - 1] RLIKE '^[0-9]+$' 
    THEN concat_ws('_', slice(split(s23, '_'), 1, size(split(s23, '_')) - 1)) 
    ELSE s23 
  END AS s24, 
  s25,s26 from db5.table6 where col28='${date:y-m-d}' 
) 
insert overwrite table db6.table7 partition(col28='${date:y-m-d}') 
SELECT s23 
FROM cte1;`,
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "with cte1 as ( \n  select s23, \n  CASE \n    WHEN s23 LIKE 'v12%' \n     AND size(split(s23, '_')) > 3 \n     AND split(s23, '_')[size(split(s23, '_')) - 1] RLIKE '^[0-9]+$' \n    THEN concat_ws('_', slice(split(s23, '_'), 1, size(split(s23, '_')) - 1)) \n    ELSE s23 \n  END AS s24, \n  s25,s26 from db5.table6 where col28='${date:y-m-d}' \n) \ninsert overwrite table db6.table7 partition(col28='${date:y-m-d}') \nSELECT s23 \nFROM cte1;",
					StmtType: analyzer.StmtTypeInsert,
					Reads: []*analyzer.Dependency{
						{IsTemp: false, Cluster: "default_cluster", Database: "db5", Table: "table6"},
					},
					Writes: []*analyzer.Dependency{
						{IsTemp: false, Cluster: "default_cluster", Database: "db6", Table: "table7"},
					},
				},
			},
		},
		{
			name: "delete statement",
			sql:  "DELETE FROM table1 WHERE id = 1;",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "DELETE FROM table1 WHERE id = 1;",
					StmtType: analyzer.StmtTypeDelete,
					Reads:    []*analyzer.Dependency{},
					Writes: []*analyzer.Dependency{
						{Cluster: "default_cluster", Database: "default_db", Table: "table1"},
					},
				},
			},
		},
		// 注释测试
		{
			name: "statement with comments",
			sql:  "-- This is a comment\nSELECT * FROM table1; -- Another comment",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "-- This is a comment\nSELECT * FROM table1;",
					StmtType: analyzer.StmtTypeSelect,
					Reads: []*analyzer.Dependency{
						{Cluster: "default_cluster", Database: "default_db", Table: "table1"},
					},
					Writes: []*analyzer.Dependency{},
				},
			},
		},
		{
			name:     "only comments",
			sql:      "-- This is a comment\n/* This is another comment */",
			expected: []*analyzer.LineageResult{},
		},
		// 指定数据库和集群
		{
			name: "select with specified database",
			sql:  "SELECT * FROM db1.table1;",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "SELECT * FROM db1.table1;",
					StmtType: analyzer.StmtTypeSelect,
					Reads: []*analyzer.Dependency{
						{Cluster: "default_cluster", Database: "db1", Table: "table1"},
					},
					Writes: []*analyzer.Dependency{},
				},
			},
		},
		{
			name: "select with specified cluster and database",
			sql:  "SELECT * FROM cluster1.db1.table1;",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "SELECT * FROM cluster1.db1.table1;",
					StmtType: analyzer.StmtTypeSelect,
					Reads: []*analyzer.Dependency{
						{Cluster: "cluster1", Database: "db1", Table: "table1"},
					},
					Writes: []*analyzer.Dependency{},
				},
			},
		},
		{
			name: "insert with specified database",
			sql:  "INSERT INTO db2.table2 VALUES (1, 'v1');",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "INSERT INTO db2.table2 VALUES (1, 'v1');",
					StmtType: analyzer.StmtTypeInsert,
					Reads:    []*analyzer.Dependency{},
					Writes: []*analyzer.Dependency{
						{Cluster: "default_cluster", Database: "db2", Table: "table2"},
					},
				},
			},
		},
		// 分号和注释在字符串中的测试
		{
			name: "SELECT with semicolon in string",
			sql:  "SELECT * FROM table1 WHERE name = 'test;string' AND comment = 'line1 -- comment in string';",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "SELECT * FROM table1 WHERE name = 'test;string' AND comment = 'line1 -- comment in string';",
					StmtType: analyzer.StmtTypeSelect,
					Reads: []*analyzer.Dependency{
						{Cluster: "default_cluster", Database: "default_db", Table: "table1"},
					},
					Writes: []*analyzer.Dependency{},
				},
			},
		},
		{
			name: "INSERT with semicolons in values",
			sql:  "INSERT INTO table2 VALUES (1, 'value;with;semicolons', 'comment--with-dashes');",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "INSERT INTO table2 VALUES (1, 'value;with;semicolons', 'comment--with-dashes');",
					StmtType: analyzer.StmtTypeInsert,
					Reads:    []*analyzer.Dependency{},
					Writes: []*analyzer.Dependency{
						{Cluster: "default_cluster", Database: "default_db", Table: "table2"},
					},
				},
			},
		},
		// CTE语句测试
		{
			name: "CTE statement with multiple CTEs",
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
			expected: []*analyzer.LineageResult{
				{
					Stmt: `WITH 
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
					StmtType: analyzer.StmtTypeSelect,
					Reads: []*analyzer.Dependency{
						{Cluster: "default_cluster", Database: "default_db", Table: "table17"},
					},
					Writes: []*analyzer.Dependency{},
				},
			},
		},
		{
			name: "cte & tagged query",
			sql:  "set spark.syntax.extension=true;\nset spark.custom.spark354.enable=true;\nset spark.custom.blaze.failover.enable=false;\nset spark.sql.adaptive.shuffle.targetPostShuffleInputSize=64m ;\nset spark.driver.memoryOverhead=2g;\nset spark.executor.memory=3g;\nset spark.sql.autoBroadcastJoinThreshold = 300m;\nset spark.sql.adaptive.enabled=false;\nset spark.sql.shuffle.partitions = 100;\n\nwith cte5 as\n(\n  select * from paimon_incremental_query('db3.table9','TAG${date-1}','TAG${date}') \n),\n\ncte6 as\n(\nselect a.* \nfrom \n  cte5 a\njoin \n(select * from db7.table8 where col28='${date}') b\non a.s27 = b.s27\n)\n\n\ninsert into db3.table10\nselect \n  a.s9,\n  a.s27 as s27,\n  a.s28 as s28,\n  a.s29 as s29,\n  a.s30  as s30,\n  a.s31  as `s31` ,\n  b.s32 as `s32`,\n  '${date:y-m-d}' as `col28` \nfrom cte6 a\njoin \n  db3.table11 b \n on a.s9=b.id and b.col28='${date:y-m-d}'",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "set spark.syntax.extension=true;",
					StmtType: analyzer.StmtTypeSetVar,
				},
				{
					Stmt:     "set spark.custom.spark354.enable=true;",
					StmtType: analyzer.StmtTypeSetVar,
				},
				{
					Stmt:     "set spark.custom.blaze.failover.enable=false;",
					StmtType: analyzer.StmtTypeSetVar,
				},
				{
					Stmt:     "set spark.sql.adaptive.shuffle.targetPostShuffleInputSize=64m ;",
					StmtType: analyzer.StmtTypeSetVar,
				},
				{
					Stmt:     "set spark.driver.memoryOverhead=2g;",
					StmtType: analyzer.StmtTypeSetVar,
				},
				{
					Stmt:     "set spark.executor.memory=3g;",
					StmtType: analyzer.StmtTypeSetVar,
				},
				{
					Stmt:     "set spark.sql.autoBroadcastJoinThreshold = 300m;",
					StmtType: analyzer.StmtTypeSetVar,
				},
				{
					Stmt:     "set spark.sql.adaptive.enabled=false;",
					StmtType: analyzer.StmtTypeSetVar,
				},
				{
					Stmt:     "set spark.sql.shuffle.partitions = 100;",
					StmtType: analyzer.StmtTypeSetVar,
				},
				{
					Stmt:     "with cte5 as\n(\n  select * from paimon_incremental_query('db3.table9','TAG${date-1}','TAG${date}') \n),\n\ncte6 as\n(\nselect a.* \nfrom \n  cte5 a\njoin \n(select * from db7.table8 where col28='${date}') b\non a.s27 = b.s27\n)\n\n\ninsert into db3.table10\nselect \n  a.s9,\n  a.s27 as s27,\n  a.s28 as s28,\n  a.s29 as s29,\n  a.s30  as s30,\n  a.s31  as `s31` ,\n  b.s32 as `s32`,\n  '${date:y-m-d}' as `col28` \nfrom cte6 a\njoin \n  db3.table11 b \n on a.s9=b.id and b.col28='${date:y-m-d}'",
					StmtType: analyzer.StmtTypeInsert,
					Reads: []*analyzer.Dependency{
						{Cluster: "default_cluster", Database: "db7", Table: "table8"},
						{Cluster: "default_cluster", Database: "db3", Table: "table11"},
					},
					Writes: []*analyzer.Dependency{
						{Cluster: "default_cluster", Database: "db3", Table: "table10"},
					},
				},
			},
		},
		// USE语句测试
		{
			name: "USE database statement",
			sql:  "USE db1;",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "USE db1;",
					StmtType: analyzer.StmtTypeUseDatabase,
					Reads: []*analyzer.Dependency{
						{
							Cluster:  "default_cluster",
							Database: "db1",
						},
					},
					Writes: []*analyzer.Dependency{},
				},
			},
		},
		{
			name: "USE database with INSERT SELECT and SELECT",
			sql:  "USE db16; INSERT INTO tb5 SELECT * FROM db7.tb4; SELECT * FROM tb5;",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "USE db16;",
					StmtType: analyzer.StmtTypeUseDatabase,
					Reads: []*analyzer.Dependency{
						{
							Cluster:  "default_cluster",
							Database: "db16",
						},
					},
					Writes: []*analyzer.Dependency{},
				},
				{
					Stmt:     "INSERT INTO tb5 SELECT * FROM db7.tb4;",
					StmtType: analyzer.StmtTypeInsert,
					Reads: []*analyzer.Dependency{
						{
							Cluster:  "default_cluster",
							Database: "db7",
							Table:    "tb4",
						},
					},
					Writes: []*analyzer.Dependency{
						{
							Cluster:  "default_cluster",
							Database: "db16",
							Table:    "tb5",
						},
					},
				},
				{
					Stmt:     "SELECT * FROM tb5;",
					StmtType: analyzer.StmtTypeSelect,
					Reads: []*analyzer.Dependency{
						{
							Cluster:  "default_cluster",
							Database: "db16",
							Table:    "tb5",
						},
					},
					Writes: []*analyzer.Dependency{},
				},
			},
		},
	}

	sparkAnalyzer := NewSQLAnalyzer()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &analyzer.AnalyzeReq{
				DefaultCluster:  "default_cluster",
				DefaultDatabase: "default_db",
				Type:            analyzer.EngineSpark,
				SQL:             tt.sql,
			}

			results, err := sparkAnalyzer.AnalyzeLineage(req)
			assert.NoError(t, err)
			if assert.Equal(t, len(tt.expected), len(results)) {
				for i, result := range results {
					expected := tt.expected[i]
					assert.Equal(t, expected.Stmt, result.Stmt)
					assert.Equal(t, expected.StmtType, result.StmtType)
					assert.Equal(t, len(expected.Reads), len(result.Reads))
					assert.Equal(t, len(expected.Writes), len(result.Writes))

					// 验证读表
					for j, readTable := range result.Reads {
						expectedRead := expected.Reads[j]
						assert.Equal(t, expectedRead.Cluster, readTable.Cluster)
						assert.Equal(t, expectedRead.Database, readTable.Database)
						assert.Equal(t, expectedRead.Table, readTable.Table)
					}

					// 验证写表
					for j, writeTable := range result.Writes {
						expectedWrite := expected.Writes[j]
						assert.Equal(t, expectedWrite.Cluster, writeTable.Cluster)
						assert.Equal(t, expectedWrite.Database, writeTable.Database)
						assert.Equal(t, expectedWrite.Table, writeTable.Table)
					}
				}
			}
		})
	}
}

func TestSparkDependencyAnalyzer_AnalyzeDDL(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected []*analyzer.DDLResult
	}{
		{
			name: "create table statement",
			sql:  "CREATE TABLE table3 (id INT, col1 STRING);",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "CREATE TABLE table3 (id INT, col1 STRING);",
					StmtType: analyzer.StmtTypeCreateTable,
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						IsSpecifiedCluster:  false,
						DatabaseName:        "default_db",
						IsSpecifiedDatabase: false,
						TableName:           "table3",
						ActionType:          analyzer.ActionTypeCreate,
						Columns: []*analyzer.ActionColumn{
							{Name: "id", Type: "INT", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
							{Name: "col1", Type: "STRING", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
						},
						TableInfo: &analyzer.TableInfo{},
					},
				},
			},
		},
		{
			name: "create table statement 2",
			sql:  "CREATE TABLE table4 (id INT, col1 STRING);",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "CREATE TABLE table4 (id INT, col1 STRING);",
					StmtType: analyzer.StmtTypeCreateTable,
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						IsSpecifiedCluster:  false,
						DatabaseName:        "default_db",
						IsSpecifiedDatabase: false,
						TableName:           "table4",
						ActionType:          analyzer.ActionTypeCreate,
						Columns: []*analyzer.ActionColumn{
							{Name: "id", Type: "INT", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
							{Name: "col1", Type: "STRING", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
						},
						TableInfo: &analyzer.TableInfo{},
					},
				},
			},
		},
		{
			name: "create table as select from multiple tables",
			sql:  "CREATE TABLE table5 AS SELECT t1.id, t2.col1 FROM table1 t1 JOIN table2 t2 ON t1.id = t2.id;",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "CREATE TABLE table5 AS SELECT t1.id, t2.col1 FROM table1 t1 JOIN table2 t2 ON t1.id = t2.id;",
					StmtType: analyzer.StmtTypeCreateTable,
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						IsSpecifiedCluster:  false,
						DatabaseName:        "default_db",
						IsSpecifiedDatabase: false,
						TableName:           "table5",
						ActionType:          analyzer.ActionTypeCreate,
						Columns:             []*analyzer.ActionColumn{},
						TableInfo:           &analyzer.TableInfo{},
					},
				},
			},
		},
		{
			name: "create view statement",
			sql:  "CREATE VIEW view1 AS SELECT * FROM table1;",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "CREATE VIEW view1 AS SELECT * FROM table1;",
					StmtType: analyzer.StmtTypeCreateView,
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						IsSpecifiedCluster:  false,
						DatabaseName:        "default_db",
						IsSpecifiedDatabase: false,
						TableName:           "view1",
						ActionType:          analyzer.ActionTypeCreate,
						Columns:             []*analyzer.ActionColumn{},
					},
				},
			},
		},
		{
			name: "create view from multiple tables",
			sql:  "CREATE VIEW view2 AS SELECT t1.id, t2.col1 FROM table1 t1 JOIN table2 t2 ON t1.id = t2.id;",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "CREATE VIEW view2 AS SELECT t1.id, t2.col1 FROM table1 t1 JOIN table2 t2 ON t1.id = t2.id;",
					StmtType: analyzer.StmtTypeCreateView,
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						IsSpecifiedCluster:  false,
						DatabaseName:        "default_db",
						IsSpecifiedDatabase: false,
						TableName:           "view2",
						ActionType:          analyzer.ActionTypeCreate,
						Columns:             []*analyzer.ActionColumn{},
					},
				},
			},
		},
		{
			name: "alter table statement",
			sql:  "ALTER TABLE table1 ADD COLUMN col2 INT;",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "ALTER TABLE table1 ADD COLUMN col2 INT;",
					StmtType: analyzer.StmtTypeAlterTable,
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						IsSpecifiedCluster:  false,
						DatabaseName:        "default_db",
						IsSpecifiedDatabase: false,
						TableName:           "table1",
						ActionType:          analyzer.ActionTypeAlter,
						Columns: []*analyzer.ActionColumn{
							{Name: "col2", Type: "INT", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeAlter},
						},
					},
				},
			},
		},
		{
			name: "ALTER TABLE RENAME TO",
			sql:  "ALTER TABLE db1.`tb1` RENAME TO db1.`tb2`",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "ALTER TABLE db1.`tb1` RENAME TO db1.`tb2`",
					StmtType: analyzer.StmtTypeRenameTable,
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						IsSpecifiedCluster:  false,
						DatabaseName:        "db1",
						IsSpecifiedDatabase: true,
						TableName:           "tb1",
						ActionType:          analyzer.ActionTypeDrop,
					},
					AnotherAction: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						IsSpecifiedCluster:  false,
						DatabaseName:        "db1",
						IsSpecifiedDatabase: true,
						TableName:           "tb2",
						ActionType:          analyzer.ActionTypeCreate,
					},
				},
			},
		},
		{
			name: "ALTER TABLE RENAME COLUMN",
			sql:  "ALTER TABLE db1.`tb1` RENAME COLUMN `col8` TO `desc`",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "ALTER TABLE db1.`tb1` RENAME COLUMN `col8` TO `desc`",
					StmtType: analyzer.StmtTypeAlterTable,
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						DatabaseName:        "db1",
						IsSpecifiedDatabase: true,
						TableName:           "tb1",
						ActionType:          analyzer.ActionTypeAlter,
						Columns: []*analyzer.ActionColumn{
							{
								Name:   "col8",
								Action: analyzer.ActionTypeDrop,
							},
							{
								Name:   "desc",
								Action: analyzer.ActionTypeCreate,
							},
						},
					},
				},
			},
		},
		{
			name: "create table with tech info",
			sql:  "CREATE TABLE tb3 (id INT, col6 STRING, amount DOUBLE) PARTITIONED BY (col7 DATE) STORED AS PARQUET;",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "CREATE TABLE tb3 (id INT, col6 STRING, amount DOUBLE) PARTITIONED BY (col7 DATE) STORED AS PARQUET;",
					StmtType: analyzer.StmtTypeCreateTable,
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						IsSpecifiedCluster:  false,
						DatabaseName:        "default_db",
						IsSpecifiedDatabase: false,
						TableName:           "tb3",
						ActionType:          analyzer.ActionTypeCreate,
						Columns: []*analyzer.ActionColumn{
							{Name: "id", Type: "INT", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
							{Name: "col6", Type: "STRING", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
							{Name: "amount", Type: "DOUBLE", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
						},
						TableInfo: &analyzer.TableInfo{
							PartitionColumnNames: []string{"col7"},
							Locations:            nil,
							LakehouseTableFormat: "",
						},
					},
				},
			},
		},
		{
			name: "CREATE TABLE with comment containing --",
			sql:  "CREATE TABLE table3 (id INT, col9 STRING COMMENT '描述3');",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "CREATE TABLE table3 (id INT, col9 STRING COMMENT '描述3');",
					StmtType: analyzer.StmtTypeCreateTable,
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						IsSpecifiedCluster:  false,
						DatabaseName:        "default_db",
						IsSpecifiedDatabase: false,
						TableName:           "table3",
						ActionType:          analyzer.ActionTypeCreate,
						Columns: []*analyzer.ActionColumn{
							{Name: "id", Type: "INT", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
							{Name: "col9", Type: "STRING", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "描述3", Action: analyzer.ActionTypeCreate},
						},
						TableInfo: &analyzer.TableInfo{},
					},
				},
			},
		},
		{
			name: "replace table statement",
			sql:  "REPLACE TABLE table1 (id INT, col1 STRING, col2 INT);",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "REPLACE TABLE table1 (id INT, col1 STRING, col2 INT);",
					StmtType: analyzer.StmtTypeReplaceTable,
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						IsSpecifiedCluster:  false,
						DatabaseName:        "default_db",
						IsSpecifiedDatabase: false,
						TableName:           "table1",
						ActionType:          analyzer.ActionTypeCreate,
						Columns:             []*analyzer.ActionColumn{},
						TableInfo:           &analyzer.TableInfo{},
					},
				},
			},
		},
		{
			name: "replace table as select from multiple tables",
			sql:  "REPLACE TABLE table6 AS SELECT t1.id, t2.col1 FROM table1 t1 JOIN table2 t2 ON t1.id = t2.id;",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "REPLACE TABLE table6 AS SELECT t1.id, t2.col1 FROM table1 t1 JOIN table2 t2 ON t1.id = t2.id;",
					StmtType: analyzer.StmtTypeReplaceTable,
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						IsSpecifiedCluster:  false,
						DatabaseName:        "default_db",
						IsSpecifiedDatabase: false,
						TableName:           "table6",
						ActionType:          analyzer.ActionTypeCreate,
						Columns:             []*analyzer.ActionColumn{},
						TableInfo:           &analyzer.TableInfo{},
					},
				},
			},
		},
		{
			name: "drop table statement",
			sql:  "DROP TABLE db1.table1;",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "DROP TABLE db1.table1;",
					StmtType: analyzer.StmtTypeDropTable,
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						IsSpecifiedCluster:  false,
						DatabaseName:        "db1",
						IsSpecifiedDatabase: true,
						TableName:           "table1",
						ActionType:          analyzer.ActionTypeDrop,
						Columns:             []*analyzer.ActionColumn{},
					},
				},
			},
		},
		{
			name: "CREATE VIEW with -- in string",
			sql:  "CREATE VIEW view3 AS SELECT * FROM table1 WHERE col8 = '描述4';",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "CREATE VIEW view3 AS SELECT * FROM table1 WHERE col8 = '描述4';",
					StmtType: analyzer.StmtTypeCreateView,
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						IsSpecifiedCluster:  false,
						DatabaseName:        "default_db",
						IsSpecifiedDatabase: false,
						TableName:           "view3",
						ActionType:          analyzer.ActionTypeCreate,
						Columns:             []*analyzer.ActionColumn{},
					},
				},
			},
		},
		// ActionType parsing tests - CREATE TABLE with various column types and constraints
		{
			name: "create table with complex columns and constraints",
			sql:  "CREATE TABLE tb2 (id INT PRIMARY KEY, col1 STRING NOT NULL, col2 INT DEFAULT 18, col3 STRING COMMENT '描述1');",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "CREATE TABLE tb2 (id INT PRIMARY KEY, col1 STRING NOT NULL, col2 INT DEFAULT 18, col3 STRING COMMENT '描述1');",
					StmtType: analyzer.StmtTypeCreateTable,
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						IsSpecifiedCluster:  false,
						DatabaseName:        "default_db",
						IsSpecifiedDatabase: false,
						TableName:           "tb2",
						ActionType:          analyzer.ActionTypeCreate,
						TableInfo: &analyzer.TableInfo{
							PrimaryKeyColumnNames: []string{"id"},
						},
						Columns: []*analyzer.ActionColumn{
							{Name: "id", Type: "INT", IsNotNull: false, IsPrimary: true, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
							{Name: "col1", Type: "STRING", IsNotNull: true, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
							{Name: "col2", Type: "INT", IsNotNull: false, IsPrimary: false, DefaultValue: "DEFAULT18", Comment: "", Action: analyzer.ActionTypeCreate},
							{Name: "col3", Type: "STRING", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "描述1", Action: analyzer.ActionTypeCreate},
						},
					},
				},
			},
		},
		// ActionType parsing tests - ALTER TABLE operations
		{
			name: "alter table add column",
			sql:  "ALTER TABLE tb2 ADD COLUMN col4 STRING;",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "ALTER TABLE tb2 ADD COLUMN col4 STRING;",
					StmtType: analyzer.StmtTypeAlterTable,
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						IsSpecifiedCluster:  false,
						DatabaseName:        "default_db",
						IsSpecifiedDatabase: false,
						TableName:           "tb2",
						ActionType:          analyzer.ActionTypeAlter,
						Columns: []*analyzer.ActionColumn{
							{Name: "col4", Type: "STRING", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeAlter},
						},
					},
				},
			},
		},
		{
			name: "alter table rename column",
			sql:  "ALTER TABLE tb2 RENAME COLUMN col4 TO col5;",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "ALTER TABLE tb2 RENAME COLUMN col4 TO col5;",
					StmtType: analyzer.StmtTypeAlterTable,
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						IsSpecifiedCluster:  false,
						DatabaseName:        "default_db",
						IsSpecifiedDatabase: false,
						TableName:           "tb2",
						ActionType:          analyzer.ActionTypeAlter,
						Columns: []*analyzer.ActionColumn{
							{Name: "col4", Type: "", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeDrop},
							{Name: "col5", Type: "", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
						},
					},
				},
			},
		},
		{
			name: "alter table drop column",
			sql:  "ALTER TABLE tb2 DROP COLUMN col5;",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "ALTER TABLE tb2 DROP COLUMN col5;",
					StmtType: analyzer.StmtTypeAlterTable,
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						IsSpecifiedCluster:  false,
						DatabaseName:        "default_db",
						IsSpecifiedDatabase: false,
						TableName:           "tb2",
						ActionType:          analyzer.ActionTypeAlter,
						Columns: []*analyzer.ActionColumn{
							{Name: "col5", Type: "", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeDrop},
						},
					},
				},
			},
		},
		// Test for DDL TableName Info parsing
		{
			name: "create table with ddl table info",
			sql:  "CREATE TABLE db9.table13 ( `col22` STRING NOT NULL COMMENT '测试3', `col23` STRING NOT NULL COMMENT '测试4', `col24` STRING NOT NULL COMMENT '标识1：val1,val2,val3,val4', `col28` STRING NOT NULL COMMENT '测试5' ) USING paimon PARTITIONED BY (col28) COMMENT '测试6' TBLPROPERTIES( 'bucket' = '400', 'path' = 'hdfs://cluster1/warehouse/db9.db/table13', 'primary-key' = 'col28,col22,col23,col24', 'snapshot.num-retained.max' = '1', 'snapshot.num-retained.min' = '1', 'write-only' = 'true' )",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "CREATE TABLE db9.table13 ( `col22` STRING NOT NULL COMMENT '测试3', `col23` STRING NOT NULL COMMENT '测试4', `col24` STRING NOT NULL COMMENT '标识1：val1,val2,val3,val4', `col28` STRING NOT NULL COMMENT '测试5' ) USING paimon PARTITIONED BY (col28) COMMENT '测试6' TBLPROPERTIES( 'bucket' = '400', 'path' = 'hdfs://cluster1/warehouse/db9.db/table13', 'primary-key' = 'col28,col22,col23,col24', 'snapshot.num-retained.max' = '1', 'snapshot.num-retained.min' = '1', 'write-only' = 'true' )",
					StmtType: analyzer.StmtTypeCreateTable,
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						IsSpecifiedCluster:  false,
						DatabaseName:        "db9",
						IsSpecifiedDatabase: true,
						TableName:           "table13",
						ActionType:          analyzer.ActionTypeCreate,
						Columns: []*analyzer.ActionColumn{
							{Name: "col22", Type: "STRING", IsNotNull: true, IsPrimary: true, DefaultValue: "", Comment: "测试3", Action: analyzer.ActionTypeCreate},
							{Name: "col23", Type: "STRING", IsNotNull: true, IsPrimary: true, DefaultValue: "", Comment: "测试4", Action: analyzer.ActionTypeCreate},
							{Name: "col24", Type: "STRING", IsNotNull: true, IsPrimary: true, DefaultValue: "", Comment: "标识1：val1,val2,val3,val4", Action: analyzer.ActionTypeCreate},
							{Name: "col28", Type: "STRING", IsNotNull: true, IsPrimary: true, DefaultValue: "", Comment: "测试5", Action: analyzer.ActionTypeCreate},
						},
						TableInfo: &analyzer.TableInfo{
							PartitionColumnNames:  []string{"col28"},
							Locations:             []string{"'hdfs://cluster1/warehouse/db9.db/table13'"},
							LakehouseTableFormat:  "paimon",
							PrimaryKeyColumnNames: []string{"col28", "col22", "col23", "col24"},
						},
					},
				},
			},
		},
		// USE语句测试
		{
			name: "USE database statement",
			sql:  "USE db1;",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "USE db1;",
					StmtType: analyzer.StmtTypeUseDatabase,
				},
			},
		},
		// CREATE DATABASE
		{
			name: "create database",
			sql:  "CREATE DATABASE db1;",
			expected: []*analyzer.DDLResult{
				{
					Stmt: "CREATE DATABASE db1;",
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						IsSpecifiedCluster:  false,
						DatabaseName:        "db1",
						IsSpecifiedDatabase: true,
						ActionType:          analyzer.ActionTypeCreate,
					},
					StmtType: analyzer.StmtTypeCreateDatabase,
				},
			},
		},
		{
			name: "create database with quotes",
			sql:  "CREATE DATABASE `db1`;",
			expected: []*analyzer.DDLResult{
				{
					Stmt: "CREATE DATABASE `db1`;",
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						IsSpecifiedCluster:  false,
						DatabaseName:        "db1",
						IsSpecifiedDatabase: true,
						ActionType:          analyzer.ActionTypeCreate,
					},
					StmtType: analyzer.StmtTypeCreateDatabase,
				},
			},
		},
		// 主键测试 - 列级PRIMARY KEY约束
		{
			name: "create table with primary key constraint",
			sql:  "CREATE TABLE tb2 (id INT PRIMARY KEY, col1 STRING, col2 INT);",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "CREATE TABLE tb2 (id INT PRIMARY KEY, col1 STRING, col2 INT);",
					StmtType: analyzer.StmtTypeCreateTable,
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						IsSpecifiedCluster:  false,
						DatabaseName:        "default_db",
						IsSpecifiedDatabase: false,
						TableName:           "tb2",
						ActionType:          analyzer.ActionTypeCreate,
						Columns: []*analyzer.ActionColumn{
							{Name: "id", Type: "INT", IsNotNull: false, IsPrimary: true, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
							{Name: "col1", Type: "STRING", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
							{Name: "col2", Type: "INT", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
						},
						TableInfo: &analyzer.TableInfo{
							PrimaryKeyColumnNames: []string{"id"},
						},
					},
				},
			},
		},
		// 主键测试 - Paimon格式
		{
			name: "create table with paimon primary key",
			sql:  "CREATE TABLE tb7 (id INT, col1 STRING) USING paimon TBLPROPERTIES ('primary-key' = 'id');",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "CREATE TABLE tb7 (id INT, col1 STRING) USING paimon TBLPROPERTIES ('primary-key' = 'id');",
					StmtType: analyzer.StmtTypeCreateTable,
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						IsSpecifiedCluster:  false,
						DatabaseName:        "default_db",
						IsSpecifiedDatabase: false,
						TableName:           "tb7",
						ActionType:          analyzer.ActionTypeCreate,
						Columns: []*analyzer.ActionColumn{
							{Name: "id", Type: "INT", IsNotNull: false, IsPrimary: true, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
							{Name: "col1", Type: "STRING", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
						},
						TableInfo: &analyzer.TableInfo{
							PrimaryKeyColumnNames: []string{"id"},
							LakehouseTableFormat:  "paimon",
						},
					},
				},
			},
		},
		// 主键测试 - Hudi格式
		{
			name: "create table with hudi primary key",
			sql:  "CREATE TABLE tb8 (id INT, col1 STRING) USING hudi TBLPROPERTIES ('hoodie.datasource.write.recordkey.field' = 'id');",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "CREATE TABLE tb8 (id INT, col1 STRING) USING hudi TBLPROPERTIES ('hoodie.datasource.write.recordkey.field' = 'id');",
					StmtType: analyzer.StmtTypeCreateTable,
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						IsSpecifiedCluster:  false,
						DatabaseName:        "default_db",
						IsSpecifiedDatabase: false,
						TableName:           "tb8",
						ActionType:          analyzer.ActionTypeCreate,
						Columns: []*analyzer.ActionColumn{
							{Name: "id", Type: "INT", IsNotNull: false, IsPrimary: true, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
							{Name: "col1", Type: "STRING", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
						},
						TableInfo: &analyzer.TableInfo{
							PrimaryKeyColumnNames: []string{"id"},
							LakehouseTableFormat:  "hudi",
						},
					},
				},
			},
		},
		// 主键测试 - Delta Lake格式
		{
			name: "create table with delta primary key",
			sql:  "CREATE TABLE tb9 (id INT, col1 STRING) USING delta TBLPROPERTIES ('primaryKey' = 'id');",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "CREATE TABLE tb9 (id INT, col1 STRING) USING delta TBLPROPERTIES ('primaryKey' = 'id');",
					StmtType: analyzer.StmtTypeCreateTable,
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						IsSpecifiedCluster:  false,
						DatabaseName:        "default_db",
						IsSpecifiedDatabase: false,
						TableName:           "tb9",
						ActionType:          analyzer.ActionTypeCreate,
						Columns: []*analyzer.ActionColumn{
							{Name: "id", Type: "INT", IsNotNull: false, IsPrimary: true, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
							{Name: "col1", Type: "STRING", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
						},
						TableInfo: &analyzer.TableInfo{
							PrimaryKeyColumnNames: []string{"id"},
							LakehouseTableFormat:  "delta",
						},
					},
				},
			},
		},
		// 主键测试 - 复合主键
		{
			name: "create table with composite primary key",
			sql:  "CREATE TABLE tb11 (id INT, col1 STRING, col2 INT) USING paimon TBLPROPERTIES ('primary-key' = 'id,col1');",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "CREATE TABLE tb11 (id INT, col1 STRING, col2 INT) USING paimon TBLPROPERTIES ('primary-key' = 'id,col1');",
					StmtType: analyzer.StmtTypeCreateTable,
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						IsSpecifiedCluster:  false,
						DatabaseName:        "default_db",
						IsSpecifiedDatabase: false,
						TableName:           "tb11",
						ActionType:          analyzer.ActionTypeCreate,
						Columns: []*analyzer.ActionColumn{
							{Name: "id", Type: "INT", IsNotNull: false, IsPrimary: true, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
							{Name: "col1", Type: "STRING", IsNotNull: false, IsPrimary: true, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
							{Name: "col2", Type: "INT", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
						},
						TableInfo: &analyzer.TableInfo{
							PrimaryKeyColumnNames: []string{"id", "col1"},
							LakehouseTableFormat:  "paimon",
						},
					},
				},
			},
		},
		// 主键测试 - Iceberg格式
		{
			name: "create table with iceberg primary key",
			sql:  "CREATE TABLE tb10 (id INT, col1 STRING) USING iceberg TBLPROPERTIES ('primary-key' = 'id');",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "CREATE TABLE tb10 (id INT, col1 STRING) USING iceberg TBLPROPERTIES ('primary-key' = 'id');",
					StmtType: analyzer.StmtTypeCreateTable,
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						IsSpecifiedCluster:  false,
						DatabaseName:        "default_db",
						IsSpecifiedDatabase: false,
						TableName:           "tb10",
						ActionType:          analyzer.ActionTypeCreate,
						Columns: []*analyzer.ActionColumn{
							{Name: "id", Type: "INT", IsNotNull: false, IsPrimary: true, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
							{Name: "col1", Type: "STRING", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
						},
						TableInfo: &analyzer.TableInfo{
							PrimaryKeyColumnNames: []string{"id"},
							LakehouseTableFormat:  "iceberg",
						},
					},
				},
			},
		},
		// 主键测试 - 无主键情况
		{
			name: "create table without primary key",
			sql:  "CREATE TABLE tb12 (id INT, col1 STRING);",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "CREATE TABLE tb12 (id INT, col1 STRING);",
					StmtType: analyzer.StmtTypeCreateTable,
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						IsSpecifiedCluster:  false,
						DatabaseName:        "default_db",
						IsSpecifiedDatabase: false,
						TableName:           "tb12",
						ActionType:          analyzer.ActionTypeCreate,
						Columns: []*analyzer.ActionColumn{
							{Name: "id", Type: "INT", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
							{Name: "col1", Type: "STRING", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
						},
						TableInfo: &analyzer.TableInfo{},
					},
				},
			},
		},
		// 主键测试 - 带分区的主键
		{
			name: "create table with primary key and partition",
			sql:  "CREATE TABLE tb13 (id INT, col1 STRING, col28 STRING) USING paimon PARTITIONED BY (col28) TBLPROPERTIES ('primary-key' = 'id');",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "CREATE TABLE tb13 (id INT, col1 STRING, col28 STRING) USING paimon PARTITIONED BY (col28) TBLPROPERTIES ('primary-key' = 'id');",
					StmtType: analyzer.StmtTypeCreateTable,
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						IsSpecifiedCluster:  false,
						DatabaseName:        "default_db",
						IsSpecifiedDatabase: false,
						TableName:           "tb13",
						ActionType:          analyzer.ActionTypeCreate,
						Columns: []*analyzer.ActionColumn{
							{Name: "id", Type: "INT", IsNotNull: false, IsPrimary: true, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
							{Name: "col1", Type: "STRING", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
							{Name: "col28", Type: "STRING", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
						},
						TableInfo: &analyzer.TableInfo{
							PartitionColumnNames:  []string{"col28"},
							PrimaryKeyColumnNames: []string{"id"},
							LakehouseTableFormat:  "paimon",
						},
					},
				},
			},
		},
		// 测试 ALTER TABLE SET TBLPROPERTIES
		{
			name: "alter table set tblproperties",
			sql:  "alter table db2.table2 set\n  TBLPROPERTIES ('write-only' = 'true');",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "alter table db2.table2 set\n  TBLPROPERTIES ('write-only' = 'true');",
					StmtType: analyzer.StmtTypeAlterTable,
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						IsSpecifiedCluster:  false,
						DatabaseName:        "db2",
						IsSpecifiedDatabase: true,
						TableName:           "table2",
						ActionType:          analyzer.ActionTypeAlter,
						Columns:             []*analyzer.ActionColumn{},
						TableInfo:           &analyzer.TableInfo{},
					},
				},
			},
		},
		// 主键测试 - 复杂复合主键
		{
			name: "create table with complex composite primary key",
			sql:  "CREATE TABLE tb14 (id INT, col1 STRING, col2 INT, col28 STRING) USING paimon PARTITIONED BY (col28) TBLPROPERTIES ('primary-key' = 'id,col1,col2');",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "CREATE TABLE tb14 (id INT, col1 STRING, col2 INT, col28 STRING) USING paimon PARTITIONED BY (col28) TBLPROPERTIES ('primary-key' = 'id,col1,col2');",
					StmtType: analyzer.StmtTypeCreateTable,
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						IsSpecifiedCluster:  false,
						DatabaseName:        "default_db",
						IsSpecifiedDatabase: false,
						TableName:           "tb14",
						ActionType:          analyzer.ActionTypeCreate,
						Columns: []*analyzer.ActionColumn{
							{Name: "id", Type: "INT", IsNotNull: false, IsPrimary: true, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
							{Name: "col1", Type: "STRING", IsNotNull: false, IsPrimary: true, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
							{Name: "col2", Type: "INT", IsNotNull: false, IsPrimary: true, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
							{Name: "col28", Type: "STRING", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
						},
						TableInfo: &analyzer.TableInfo{
							PartitionColumnNames:  []string{"col28"},
							PrimaryKeyColumnNames: []string{"id", "col1", "col2"},
							LakehouseTableFormat:  "paimon",
						},
					},
				},
			},
		},
		// 主键测试 - Iceberg PRIMARY KEY NOT ENFORCED语法
		{
			name: "create table with iceberg primary key not enforced",
			sql:  "CREATE TABLE `cluster2`.`default`.`tb16` ( \n    id BIGINT COMMENT '描述2', \n    data STRING NOT NULL, \n    PRIMARY KEY(`id`) NOT ENFORCED \n) TBLPROPERTIES ('format-version'='2');",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "CREATE TABLE `cluster2`.`default`.`tb16` ( \n    id BIGINT COMMENT '描述2', \n    data STRING NOT NULL, \n    PRIMARY KEY(`id`) NOT ENFORCED \n) TBLPROPERTIES ('format-version'='2');",
					StmtType: analyzer.StmtTypeCreateTable,
					Action: &analyzer.ActionInfo{
						ClusterName:         "cluster2",
						IsSpecifiedCluster:  true,
						DatabaseName:        "default",
						IsSpecifiedDatabase: true,
						TableName:           "tb16",
						ActionType:          analyzer.ActionTypeCreate,
						Columns: []*analyzer.ActionColumn{
							{Name: "id", Type: "BIGINT", IsNotNull: false, IsPrimary: true, DefaultValue: "", Comment: "描述2", Action: analyzer.ActionTypeCreate},
							{Name: "data", Type: "STRING", IsNotNull: true, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
						},
						TableInfo: &analyzer.TableInfo{
							PrimaryKeyColumnNames: []string{"id"},
						},
					},
				},
			},
		},
		// 主键测试 - Iceberg复合主键 NOT ENFORCED语法
		{
			name: "create table with iceberg composite primary key not enforced",
			sql:  "CREATE TABLE tb15 (id INT, col1 STRING, col2 INT, PRIMARY KEY(id, col1) NOT ENFORCED) USING iceberg TBLPROPERTIES ('format-version'='2');",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "CREATE TABLE tb15 (id INT, col1 STRING, col2 INT, PRIMARY KEY(id, col1) NOT ENFORCED) USING iceberg TBLPROPERTIES ('format-version'='2');",
					StmtType: analyzer.StmtTypeCreateTable,
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						IsSpecifiedCluster:  false,
						DatabaseName:        "default_db",
						IsSpecifiedDatabase: false,
						TableName:           "tb15",
						ActionType:          analyzer.ActionTypeCreate,
						Columns: []*analyzer.ActionColumn{
							{Name: "id", Type: "INT", IsNotNull: false, IsPrimary: true, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
							{Name: "col1", Type: "STRING", IsNotNull: false, IsPrimary: true, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
							{Name: "col2", Type: "INT", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
						},
						TableInfo: &analyzer.TableInfo{
							PrimaryKeyColumnNames: []string{"id", "col1"},
							LakehouseTableFormat:  "iceberg",
						},
					},
				},
			},
		},
		// 添加新的测试用例
		{
			name: "create table if not exists like another table",
			sql:  "create table if not exists db10.table14 like db11.table14",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "create table if not exists db10.table14 like db11.table14",
					StmtType: analyzer.StmtTypeCreateTable,
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						IsSpecifiedCluster:  false,
						DatabaseName:        "db10",
						IsSpecifiedDatabase: true,
						TableName:           "table14",
						ActionType:          analyzer.ActionTypeCreate,
						Columns:             []*analyzer.ActionColumn{},
					},
				},
			},
		},
		{
			name: "alter table add column",
			sql:  "alter table db8.table15 add columns (col27 string comment '测试7，1是0否')",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "alter table db8.table15 add columns (col27 string comment '测试7，1是0否')",
					StmtType: analyzer.StmtTypeAlterTable,
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						IsSpecifiedCluster:  false,
						DatabaseName:        "db8",
						IsSpecifiedDatabase: true,
						TableName:           "table15",
						ActionType:          analyzer.ActionTypeAlter,
						Columns: []*analyzer.ActionColumn{
							{
								Name:    "col27",
								Type:    "string",
								Comment: "测试7，1是0否",
								Action:  analyzer.ActionTypeAlter,
							},
						},
					},
				},
			},
		},
		{
			name: "alter table change column",
			sql:  "ALTER TABLE db8.table16 CHANGE COLUMN col25 col25 STRING COMMENT '测试8'",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "ALTER TABLE db8.table16 CHANGE COLUMN col25 col25 STRING COMMENT '测试8'",
					StmtType: analyzer.StmtTypeAlterTable,
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						IsSpecifiedCluster:  false,
						DatabaseName:        "db8",
						IsSpecifiedDatabase: true,
						TableName:           "table16",
						ActionType:          analyzer.ActionTypeAlter,
						Columns: []*analyzer.ActionColumn{
							{
								Name:    "col25",
								Type:    "STRING",
								Comment: "测试8",
								Action:  analyzer.ActionTypeAlter,
							},
						},
					},
				},
			},
		},
		{
			name: "create table with single quotes in comments",
			sql:  "CREATE TABLE tb17 (id INT NOT NULL COMMENT '测试9''s 标识2''' PRIMARY KEY, col1 VARCHAR NOT NULL) COMMENT '这是测试10，存放测试11''s 测试12';",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "CREATE TABLE tb17 (id INT NOT NULL COMMENT '测试9''s 标识2''' PRIMARY KEY, col1 VARCHAR NOT NULL) COMMENT '这是测试10，存放测试11''s 测试12';",
					StmtType: analyzer.StmtTypeCreateTable,
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						IsSpecifiedCluster:  false,
						DatabaseName:        "default_db",
						IsSpecifiedDatabase: false,
						TableName:           "tb17",
						ActionType:          analyzer.ActionTypeCreate,
						Columns: []*analyzer.ActionColumn{
							{Name: "id", Type: "INT", IsNotNull: true, IsPrimary: true, DefaultValue: "", Comment: "测试9''s 标识2''", Action: analyzer.ActionTypeCreate},
							{Name: "col1", Type: "VARCHAR", IsNotNull: true, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
						},
						TableInfo: &analyzer.TableInfo{
							PrimaryKeyColumnNames: []string{"id"},
						},
					},
				},
			},
		},
	}

	sparkAnalyzer := NewSQLAnalyzer()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &analyzer.AnalyzeReq{
				DefaultCluster:  "default_cluster",
				DefaultDatabase: "default_db",
				Type:            analyzer.EngineSpark,
				SQL:             tt.sql,
			}

			results, err := sparkAnalyzer.AnalyzeDDL(req)
			assert.NoError(t, err)
			if assert.Equal(t, len(tt.expected), len(results)) {
				for i, result := range results {
					expected := tt.expected[i]
					assert.Equal(t, expected.Stmt, result.Stmt)
					assert.Equal(t, expected.StmtType, result.StmtType)

					// 验证Action
					if expected.Action != nil {
						assert.NotNil(t, result.Action)
						assert.Equal(t, expected.Action.ClusterName, result.Action.ClusterName)
						assert.Equal(t, expected.Action.DatabaseName, result.Action.DatabaseName)
						assert.Equal(t, expected.Action.TableName, result.Action.TableName)
						assert.Equal(t, expected.Action.ActionType, result.Action.ActionType)
						assert.Equal(t, expected.Action.IsSpecifiedCluster, result.Action.IsSpecifiedCluster)
						assert.Equal(t, expected.Action.IsSpecifiedDatabase, result.Action.IsSpecifiedDatabase)

						// 验证Columns
						if assert.Equal(t, len(expected.Action.Columns), len(result.Action.Columns)) {
							for k, column := range result.Action.Columns {
								expectedColumn := expected.Action.Columns[k]
								assert.Equal(t, expectedColumn.Name, column.Name)
								assert.Equal(t, expectedColumn.Type, column.Type)
								assert.Equal(t, expectedColumn.IsNotNull, column.IsNotNull)
								assert.Equal(t, expectedColumn.IsPrimary, column.IsPrimary)
								assert.Equal(t, expectedColumn.DefaultValue, column.DefaultValue)
								assert.Equal(t, expectedColumn.Comment, column.Comment)
								assert.Equal(t, expectedColumn.Action, column.Action)
							}
						}

						// 验证TableInfo
						if expected.Action.TableInfo != nil {
							assert.NotNil(t, result.Action.TableInfo)
							assert.Equal(t, expected.Action.TableInfo.PartitionColumnNames, result.Action.TableInfo.PartitionColumnNames)
							assert.Equal(t, expected.Action.TableInfo.PrimaryKeyColumnNames, result.Action.TableInfo.PrimaryKeyColumnNames)
							assert.Equal(t, expected.Action.TableInfo.Locations, result.Action.TableInfo.Locations)
							assert.Equal(t, expected.Action.TableInfo.LakehouseTableFormat, result.Action.TableInfo.LakehouseTableFormat)
						} else {
							assert.Nil(t, result.Action.TableInfo)
						}
					} else {
						assert.Nil(t, result.Action)
					}

					// 验证AnotherAction
					if expected.AnotherAction != nil {
						assert.NotNil(t, result.AnotherAction)
						if expected.AnotherAction.TableInfo != nil {
							assert.NotNil(t, result.AnotherAction.TableInfo)
							assert.Equal(t, expected.AnotherAction.TableInfo.PartitionColumnNames, result.AnotherAction.TableInfo.PartitionColumnNames)
							assert.Equal(t, expected.AnotherAction.TableInfo.PrimaryKeyColumnNames, result.AnotherAction.TableInfo.PrimaryKeyColumnNames)
							assert.Equal(t, expected.AnotherAction.TableInfo.Locations, result.AnotherAction.TableInfo.Locations)
							assert.Equal(t, expected.AnotherAction.TableInfo.LakehouseTableFormat, result.AnotherAction.TableInfo.LakehouseTableFormat)
						} else {
							assert.Nil(t, result.AnotherAction.TableInfo)
						}
					} else {
						assert.Nil(t, result.AnotherAction)
					}
				}
			}
		})
	}
}

func TestSparkMakeCommentModification(t *testing.T) {
	tests := []struct {
		name        string
		ddl         string
		columnName  string
		comment     string
		expected    string
		expectedErr bool
	}{
		{
			name:        "Modify column comment",
			ddl:         "CREATE TABLE tb6 (id INT PRIMARY KEY, col1 STRING NOT NULL, col2 INT)",
			columnName:  "col1",
			comment:     "测试13",
			expected:    "ALTER TABLE `tb6` ALTER COLUMN `col1` COMMENT '测试13';",
			expectedErr: false,
		},
		{
			name:        "Modify column comment with database",
			ddl:         "CREATE TABLE db1.tb6 (id INT PRIMARY KEY, col1 STRING)",
			columnName:  "col1",
			comment:     "测试13",
			expected:    "ALTER TABLE `db1`.`tb6` ALTER COLUMN `col1` COMMENT '测试13';",
			expectedErr: false,
		},
		{
			name:        "Column not found",
			ddl:         "CREATE TABLE tb6 (id INT PRIMARY KEY, col1 STRING)",
			columnName:  "col29",
			comment:     "测试2",
			expected:    "",
			expectedErr: true,
		},
	}

	sparkAnalyzer := NewSQLAnalyzer()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &analyzer.MakeCommentModificationReq{
				Type:       analyzer.EngineSpark,
				DDL:        tt.ddl,
				ColumnName: tt.columnName,
				Comment:    tt.comment,
			}

			result, err := sparkAnalyzer.MakeCommentModification(req)
			if tt.expectedErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

// 测试 Split 方法
func TestSparkSplit(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected []string
	}{
		{
			name:     "Single statement",
			sql:      "SELECT * FROM table1",
			expected: []string{"SELECT * FROM table1"},
		},
		{
			name:     "Multiple statements",
			sql:      "SELECT * FROM table1; SELECT * FROM table2",
			expected: []string{"SELECT * FROM table1;", "SELECT * FROM table2"},
		},
		{
			name:     "Multiple statements with semicolons",
			sql:      "SELECT * FROM table1; SELECT * FROM table2;",
			expected: []string{"SELECT * FROM table1;", "SELECT * FROM table2;"},
		},
		{
			name:     "Statements with comments",
			sql:      "-- This is a comment\nSELECT * FROM table1; -- Another comment\nSELECT * FROM table2",
			expected: []string{"-- This is a comment\nSELECT * FROM table1;", "-- Another comment\nSELECT * FROM table2"},
		},
		{
			name:     "Statements with semicolons in strings",
			sql:      "INSERT INTO table1 (name) VALUES ('test;test'); SELECT * FROM table2 WHERE name = 'a;b;c';",
			expected: []string{"INSERT INTO table1 (name) VALUES ('test;test');", "SELECT * FROM table2 WHERE name = 'a;b;c';"},
		},
	}

	sparkAnalyzer := NewSQLAnalyzer()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &analyzer.SplitReq{
				Type: analyzer.EngineSpark,
				SQL:  tt.sql,
			}

			result, err := sparkAnalyzer.Split(req)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}
