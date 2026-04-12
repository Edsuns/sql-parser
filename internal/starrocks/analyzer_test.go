package starrocks

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
			name:          "unsupported specified database RENAME TABLE",
			sql:           "ALTER TABLE db1.`tb1` RENAME db1.`tb2`",
			expectedError: "mismatched input '.'",
		},
		{
			name:          "unsupported ALTER TABLE RENAME TO",
			sql:           "ALTER TABLE `tb1` RENAME TO `tb2`",
			expectedError: "no viable alternative at input 'RENAME TO'",
		},
	}

	starRocksAnalyzer := NewSQLAnalyzer()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &analyzer.AnalyzeReq{
				DefaultCluster:  "default_cluster",
				DefaultDatabase: "default_db",
				Type:            analyzer.EngineSpark,
				SQL:             tt.sql,
			}

			_, err := starRocksAnalyzer.AnalyzeLineage(req)
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

func TestStarRocksLineageAnalyzer(t *testing.T) {
	// StarRocks 3.5.11 常用SQL示例
	tests := []struct {
		name     string
		sql      string
		expected []*analyzer.LineageResult
	}{
		{
			name: "SELECT statement with table",
			sql:  "SELECT id, col1 FROM tb1 WHERE col2 > 18",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "SELECT id, col1 FROM tb1 WHERE col2 > 18",
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
			name: "SELECT statement with table and lower case",
			sql:  "SELECT id, col1 from tb1 WHERE col2 > 18",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "SELECT id, col1 from tb1 WHERE col2 > 18",
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
			name: "SELECT statement with database and table",
			sql:  "SELECT * FROM db1.tb2 WHERE col4 >= '2023-01-01'",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "SELECT * FROM db1.tb2 WHERE col4 >= '2023-01-01'",
					StmtType: analyzer.StmtTypeSelect,
					Reads: []*analyzer.Dependency{
						{
							Cluster:  "default_cluster",
							Database: "db1",
							Table:    "tb2",
						},
					},
					Writes: []*analyzer.Dependency{},
				},
			},
		},
		{
			name: "SELECT statement with function",
			sql:  "SELECT\n/*+ SET_VAR (group_concat_max_len = 1048576) */\nx1.col46, MAX(x1.col47) col47\n, CONCAT('{\"reads\":[\"', GROUP_CONCAT(x1.col48 ORDER BY x1.col49 SEPARATOR '\"],\"writes\":[\"'), '\"]}') col50\nFROM tb3 x1",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "SELECT\n/*+ SET_VAR (group_concat_max_len = 1048576) */\nx1.col46, MAX(x1.col47) col47\n, CONCAT('{\"reads\":[\"', GROUP_CONCAT(x1.col48 ORDER BY x1.col49 SEPARATOR '\"],\"writes\":[\"'), '\"]}') col50\nFROM tb3 x1",
					StmtType: analyzer.StmtTypeSelect,
					Reads: []*analyzer.Dependency{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "tb3",
						},
					},
					Writes: []*analyzer.Dependency{},
				},
			},
		},
		{
			name: "INSERT statement with VALUES",
			sql:  "INSERT INTO tb1 (id, col1, col2) VALUES (1, 'val1', 25), (2, 'val2', 30)",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "INSERT INTO tb1 (id, col1, col2) VALUES (1, 'val1', 25), (2, 'val2', 30)",
					StmtType: analyzer.StmtTypeInsert,
					Reads:    []*analyzer.Dependency{},
					Writes: []*analyzer.Dependency{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "tb1",
						},
					},
				},
			},
		},
		{
			name: "INSERT SELECT statement",
			sql:  "INSERT INTO tb5 SELECT id, col1 FROM tb4 WHERE col2 > 20",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "INSERT INTO tb5 SELECT id, col1 FROM tb4 WHERE col2 > 20",
					StmtType: analyzer.StmtTypeInsert,
					Reads: []*analyzer.Dependency{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "tb4",
						},
					},
					Writes: []*analyzer.Dependency{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "tb5",
						},
					},
				},
			},
		},
		{
			name: "UPDATE statement",
			sql:  "UPDATE tb1 SET col2 = col2 + 1 WHERE id = 1",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "UPDATE tb1 SET col2 = col2 + 1 WHERE id = 1",
					StmtType: analyzer.StmtTypeUpdate,
					Reads:    []*analyzer.Dependency{},
					Writes: []*analyzer.Dependency{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "tb1",
						},
					},
				},
			},
		},
		{
			name: "DELETE statement",
			sql:  "DELETE FROM tb1 WHERE col2 < 18",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "DELETE FROM tb1 WHERE col2 < 18",
					StmtType: analyzer.StmtTypeDelete,
					Reads:    []*analyzer.Dependency{},
					Writes: []*analyzer.Dependency{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "tb1",
						},
					},
				},
			},
		},
		{
			name: "USE statement",
			sql:  "USE db1",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "USE db1",
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
			sql:  "USE db6; INSERT INTO tb5 SELECT * FROM ods.tb4; SELECT * FROM tb5;",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "USE db6;",
					StmtType: analyzer.StmtTypeUseDatabase,
					Reads: []*analyzer.Dependency{
						{
							Cluster:  "default_cluster",
							Database: "db6",
						},
					},
					Writes: []*analyzer.Dependency{},
				},
				{
					Stmt:     "INSERT INTO tb5 SELECT * FROM ods.tb4;",
					StmtType: analyzer.StmtTypeInsert,
					Reads: []*analyzer.Dependency{
						{
							Cluster:  "default_cluster",
							Database: "ods",
							Table:    "tb4",
						},
					},
					Writes: []*analyzer.Dependency{
						{
							Cluster:  "default_cluster",
							Database: "db6",
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
							Database: "db6",
							Table:    "tb5",
						},
					},
					Writes: []*analyzer.Dependency{},
				},
			},
		},
		{
			name: "MULTI TABLE SELECT statement",
			sql:  "SELECT x2.id, x3.col5 FROM tb1 x2 JOIN tb2 x3 ON x2.id = x3.col6",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "SELECT x2.id, x3.col5 FROM tb1 x2 JOIN tb2 x3 ON x2.id = x3.col6",
					StmtType: analyzer.StmtTypeSelect,
					Reads: []*analyzer.Dependency{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "tb1",
						},
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "tb2",
						},
					},
					Writes: []*analyzer.Dependency{},
				},
			},
		},
		{
			name: "CREATE TEMPORARY TABLE statement",
			sql:  "CREATE TEMPORARY TABLE tb6 (id INT, col1 VARCHAR(50)) ENGINE=OLAP DISTRIBUTED BY HASH(id) BUCKETS 10",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "CREATE TEMPORARY TABLE tb6 (id INT, col1 VARCHAR(50)) ENGINE=OLAP DISTRIBUTED BY HASH(id) BUCKETS 10",
					StmtType: analyzer.StmtTypeCreateTemporaryTable,
					Reads:    []*analyzer.Dependency{},
					Writes: []*analyzer.Dependency{
						{
							IsTemp:   true,
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "tb6",
						},
					},
				},
			},
		},
		{
			name: "CREATE TEMPORARY TABLE AS SELECT statement",
			sql:  "CREATE TEMPORARY TABLE tb6 AS SELECT id, col1 FROM tb1",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "CREATE TEMPORARY TABLE tb6 AS SELECT id, col1 FROM tb1",
					StmtType: analyzer.StmtTypeCreateTemporaryTable,
					Reads: []*analyzer.Dependency{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "tb1",
						},
					},
					Writes: []*analyzer.Dependency{
						{
							IsTemp:   true,
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "tb6",
						},
					},
				},
			},
		},
		{
			name: "read temporary table",
			sql:  "CREATE TEMPORARY TABLE tb6 AS SELECT id, col1 FROM tb1; SELECT * from tb6;",
			expected: []*analyzer.LineageResult{
				{
					Stmt:     "CREATE TEMPORARY TABLE tb6 AS SELECT id, col1 FROM tb1;",
					StmtType: analyzer.StmtTypeCreateTemporaryTable,
					Reads: []*analyzer.Dependency{
						{
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "tb1",
						},
					},
					Writes: []*analyzer.Dependency{
						{
							IsTemp:   true,
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "tb6",
						},
					},
				},
				{
					Stmt:     "SELECT * from tb6;",
					StmtType: analyzer.StmtTypeSelect,
					Reads: []*analyzer.Dependency{
						{
							IsTemp:   true,
							Cluster:  "default_cluster",
							Database: "default_db",
							Table:    "tb6",
						},
					},
					Writes: []*analyzer.Dependency{},
				},
			},
		},
	}

	starRocksAnalyzer := NewSQLAnalyzer()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := starRocksAnalyzer.AnalyzeLineage(&analyzer.AnalyzeReq{
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
					assert.Equal(t, len(expected.Reads), len(r.Reads))
					assert.Equal(t, len(expected.Writes), len(r.Writes))

					// 验证读表
					for j, readTable := range r.Reads {
						expectedRead := expected.Reads[j]
						assert.Equal(t, expectedRead.Cluster, readTable.Cluster)
						assert.Equal(t, expectedRead.Database, readTable.Database)
						assert.Equal(t, expectedRead.Table, readTable.Table)
					}

					// 验证写表
					for j, writeTable := range r.Writes {
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

func TestStarRocksDDLAnalyzer(t *testing.T) {
	// StarRocks 3.5.11 常用SQL示例
	tests := []struct {
		name     string
		sql      string
		expected []*analyzer.DDLResult
	}{
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
		{
			name: "CREATE TABLE statement",
			sql:  "CREATE TABLE tb7 (id INT, col1 VARCHAR(50)) ENGINE=OLAP DISTRIBUTED BY HASH(id) BUCKETS 10",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "CREATE TABLE tb7 (id INT, col1 VARCHAR(50)) ENGINE=OLAP DISTRIBUTED BY HASH(id) BUCKETS 10",
					StmtType: analyzer.StmtTypeCreateTable,
					Action: &analyzer.ActionInfo{
						ClusterName:  "default_cluster",
						DatabaseName: "default_db",
						TableName:    "tb7",
						Columns: []*analyzer.ActionColumn{
							{Name: "id", Type: "INT", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
							{Name: "col1", Type: "VARCHAR(50)", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
						},
						ActionType: analyzer.ActionTypeCreate,
						TableInfo: &analyzer.TableInfo{
							Compression:            "",
							DistributedColumnNames: []string{"id"},
							PartitionColumnNames:   []string{},
							DataModel:              "",
						},
					},
				},
			},
		},
		{
			name: "CREATE TABLE statement with TableInfo",
			sql:  "CREATE TABLE tb8 (id INT, col7 DATE, col8 DECIMAL(10,2)) ENGINE=OLAP PRIMARY KEY(id) PARTITION BY RANGE(col7) (PARTITION p2023 VALUES [('2023-01-01'), ('2024-01-01'))) DISTRIBUTED BY HASH(id) BUCKETS 10 PROPERTIES ('compression' = 'LZ4')",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "CREATE TABLE tb8 (id INT, col7 DATE, col8 DECIMAL(10,2)) ENGINE=OLAP PRIMARY KEY(id) PARTITION BY RANGE(col7) (PARTITION p2023 VALUES [('2023-01-01'), ('2024-01-01'))) DISTRIBUTED BY HASH(id) BUCKETS 10 PROPERTIES ('compression' = 'LZ4')",
					StmtType: analyzer.StmtTypeCreateTable,
					Action: &analyzer.ActionInfo{
						ClusterName:  "default_cluster",
						DatabaseName: "default_db",
						TableName:    "tb8",
						Columns: []*analyzer.ActionColumn{
							{Name: "id", Type: "INT", IsNotNull: false, IsPrimary: true, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
							{Name: "col7", Type: "DATE", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
							{Name: "col8", Type: "DECIMAL(10,2)", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
						},
						ActionType: analyzer.ActionTypeCreate,
						TableInfo: &analyzer.TableInfo{
							Compression:            "LZ4",
							DistributedColumnNames: []string{"id"},
							PartitionColumnNames:   []string{"col7"},
							DataModel:              "PRIMARY",
						},
					},
				},
			},
		},
		{
			name: "CREATE TABLE statement with TableInfo (DUPLICATE KEY)",
			sql:  "CREATE TABLE tb9 (id INT, col9 DATETIME, col10 VARCHAR(255)) ENGINE=OLAP DUPLICATE KEY(id, col9) PARTITION BY RANGE(col9) (PARTITION p2023 VALUES [('2023-01-01'), ('2024-01-01'))) DISTRIBUTED BY HASH(id) BUCKETS 10 PROPERTIES ('compression' = 'LZ4')",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "CREATE TABLE tb9 (id INT, col9 DATETIME, col10 VARCHAR(255)) ENGINE=OLAP DUPLICATE KEY(id, col9) PARTITION BY RANGE(col9) (PARTITION p2023 VALUES [('2023-01-01'), ('2024-01-01'))) DISTRIBUTED BY HASH(id) BUCKETS 10 PROPERTIES ('compression' = 'LZ4')",
					StmtType: analyzer.StmtTypeCreateTable,
					Action: &analyzer.ActionInfo{
						ClusterName:  "default_cluster",
						DatabaseName: "default_db",
						TableName:    "tb9",
						Columns: []*analyzer.ActionColumn{
							{Name: "id", Type: "INT", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
							{Name: "col9", Type: "DATETIME", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
							{Name: "col10", Type: "VARCHAR(255)", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
						},
						ActionType: analyzer.ActionTypeCreate,
						TableInfo: &analyzer.TableInfo{
							Compression:            "LZ4",
							DistributedColumnNames: []string{"id"},
							PartitionColumnNames:   []string{"col9"},
							DataModel:              "DUPLICATE",
						},
					},
				},
			},
		},
		{
			name: "CREATE TABLE statement with TableInfo (UNIQUE KEY)",
			sql:  "CREATE TABLE tb1 (id INT, col1 VARCHAR(50), col3 VARCHAR(100)) ENGINE=OLAP UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 10 PROPERTIES ('compression' = 'LZ4')",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "CREATE TABLE tb1 (id INT, col1 VARCHAR(50), col3 VARCHAR(100)) ENGINE=OLAP UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 10 PROPERTIES ('compression' = 'LZ4')",
					StmtType: analyzer.StmtTypeCreateTable,
					Action: &analyzer.ActionInfo{
						ClusterName:  "default_cluster",
						DatabaseName: "default_db",
						TableName:    "tb1",
						Columns: []*analyzer.ActionColumn{
							{Name: "id", Type: "INT", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
							{Name: "col1", Type: "VARCHAR(50)", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
							{Name: "col3", Type: "VARCHAR(100)", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
						},
						ActionType: analyzer.ActionTypeCreate,
						TableInfo: &analyzer.TableInfo{
							Compression:            "LZ4",
							DistributedColumnNames: []string{"id"},
							PartitionColumnNames:   []string{},
							DataModel:              "UNIQUE",
						},
					},
				},
			},
		},
		{
			name: "CREATE TABLE statement with TableInfo (AGGREGATE KEY)",
			sql:  "CREATE TABLE tb10 (col12 DATE, col11 INT, col8 DECIMAL(10,2)) ENGINE=OLAP AGGREGATE KEY(`col12`, col11) PARTITION BY RANGE(`col12`) (PARTITION p2023 VALUES [('2023-01-01'), ('2024-01-01'))) DISTRIBUTED BY HASH(`col11`) BUCKETS 10 PROPERTIES ('compression' = 'LZ4')",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "CREATE TABLE tb10 (col12 DATE, col11 INT, col8 DECIMAL(10,2)) ENGINE=OLAP AGGREGATE KEY(`col12`, col11) PARTITION BY RANGE(`col12`) (PARTITION p2023 VALUES [('2023-01-01'), ('2024-01-01'))) DISTRIBUTED BY HASH(`col11`) BUCKETS 10 PROPERTIES ('compression' = 'LZ4')",
					StmtType: analyzer.StmtTypeCreateTable,
					Action: &analyzer.ActionInfo{
						ClusterName:  "default_cluster",
						DatabaseName: "default_db",
						TableName:    "tb10",
						Columns: []*analyzer.ActionColumn{
							{Name: "col12", Type: "DATE", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
							{Name: "col11", Type: "INT", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
							{Name: "col8", Type: "DECIMAL(10,2)", IsNotNull: false, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
						},
						ActionType: analyzer.ActionTypeCreate,
						TableInfo: &analyzer.TableInfo{
							Compression:            "LZ4",
							DistributedColumnNames: []string{"col11"},
							PartitionColumnNames:   []string{"col12"},
							DataModel:              "AGGREGATE",
						},
					},
				},
			},
		},
		{
			name: "CREATE TABLE",
			sql:  "CREATE TABLE `tb11` (\n  `col13` date NULL COMMENT \"\",\n  `col1` varchar(65533) NULL COMMENT \"\",\n  `col14` varchar(65533) NULL COMMENT \"\",\n  `col19` int(11) NULL COMMENT \"\"\n) ENGINE = OLAP DUPLICATE KEY(`col13`, `col1`) COMMENT \"OLAP\" PARTITION BY RANGE(`col13`) (\n  PARTITION p202406\n  VALUES\n    [(\"2024-06-01\"), (\"2024-07-01\")))\nDISTRIBUTED BY HASH(`col1`) BUCKETS 1\nPROPERTIES (\n\"compression\" = \"ZSTD\",\n\"replication_num\" = \"1\"\n);",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "CREATE TABLE `tb11` (\n  `col13` date NULL COMMENT \"\",\n  `col1` varchar(65533) NULL COMMENT \"\",\n  `col14` varchar(65533) NULL COMMENT \"\",\n  `col19` int(11) NULL COMMENT \"\"\n) ENGINE = OLAP DUPLICATE KEY(`col13`, `col1`) COMMENT \"OLAP\" PARTITION BY RANGE(`col13`) (\n  PARTITION p202406\n  VALUES\n    [(\"2024-06-01\"), (\"2024-07-01\")))\nDISTRIBUTED BY HASH(`col1`) BUCKETS 1\nPROPERTIES (\n\"compression\" = \"ZSTD\",\n\"replication_num\" = \"1\"\n);",
					StmtType: analyzer.StmtTypeCreateTable,
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						IsSpecifiedCluster:  false,
						DatabaseName:        "default_db",
						IsSpecifiedDatabase: false,
						TableName:           "tb11",
						ActionType:          analyzer.ActionTypeCreate,
						Columns: []*analyzer.ActionColumn{
							{
								Name:   "col13",
								Type:   "date",
								Action: analyzer.ActionTypeCreate,
							},
							{
								Name:   "col1",
								Type:   "varchar(65533)",
								Action: analyzer.ActionTypeCreate,
							},
							{
								Name:   "col14",
								Type:   "varchar(65533)",
								Action: analyzer.ActionTypeCreate,
							},
							{
								Name:   "col19",
								Type:   "int(11)",
								Action: analyzer.ActionTypeCreate,
							},
						},
						TableInfo: &analyzer.TableInfo{
							Compression:            "ZSTD",
							DistributedColumnNames: []string{"col1"},
							PartitionColumnNames:   []string{"col13"},
							DataModel:              "DUPLICATE",
						},
					},
				},
			},
		},
		{
			name: "ALTER TABLE statement",
			sql:  "ALTER TABLE tb1 ADD COLUMN col3 VARCHAR(100)",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "ALTER TABLE tb1 ADD COLUMN col3 VARCHAR(100)",
					StmtType: analyzer.StmtTypeAlterTable,
					Action: &analyzer.ActionInfo{
						ClusterName:  "default_cluster",
						DatabaseName: "default_db",
						TableName:    "tb1",
						Columns: []*analyzer.ActionColumn{
							{
								Name:   "col3",
								Type:   "VARCHAR(100)",
								Action: analyzer.ActionTypeCreate,
							},
						},
						ActionType: analyzer.ActionTypeAlter,
					},
				},
			},
		},
		{
			name: "ALTER TABLE statement with multiple columns",
			sql:  "ALTER TABLE tb1 ADD COLUMN (`col3` VARCHAR(100), col2 INT)",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "ALTER TABLE tb1 ADD COLUMN (`col3` VARCHAR(100), col2 INT)",
					StmtType: analyzer.StmtTypeAlterTable,
					Action: &analyzer.ActionInfo{
						ClusterName:  "default_cluster",
						DatabaseName: "default_db",
						TableName:    "tb1",
						Columns: []*analyzer.ActionColumn{
							{
								Name:   "col3",
								Type:   "VARCHAR(100)",
								Action: analyzer.ActionTypeCreate,
							},
							{
								Name:   "col2",
								Type:   "INT",
								Action: analyzer.ActionTypeCreate,
							},
						},
						ActionType: analyzer.ActionTypeAlter,
					},
				},
			},
		},
		{
			name: "CREATE TABLE with single quotes in comments",
			sql:  "CREATE TABLE tb12 (id INT NOT NULL COMMENT '测试10''s 标识1''', col1 VARCHAR NOT NULL) ENGINE=OLAP PRIMARY KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 10;",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "CREATE TABLE tb12 (id INT NOT NULL COMMENT '测试10''s 标识1''', col1 VARCHAR NOT NULL) ENGINE=OLAP PRIMARY KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 10;",
					StmtType: analyzer.StmtTypeCreateTable,
					Action: &analyzer.ActionInfo{
						ClusterName:  "default_cluster",
						DatabaseName: "default_db",
						TableName:    "tb12",
						Columns: []*analyzer.ActionColumn{
							{Name: "id", Type: "INT", IsNotNull: true, IsPrimary: true, DefaultValue: "", Comment: "测试10''s 标识1''", Action: analyzer.ActionTypeCreate},
							{Name: "col1", Type: "VARCHAR", IsNotNull: true, IsPrimary: false, DefaultValue: "", Comment: "", Action: analyzer.ActionTypeCreate},
						},
						ActionType: analyzer.ActionTypeCreate,
						TableInfo: &analyzer.TableInfo{
							Compression:            "",
							DistributedColumnNames: []string{"id"},
							PartitionColumnNames:   []string{},
							DataModel:              "PRIMARY",
						},
					},
				},
			},
		},
		{
			name: "ALTER TABLE drop column",
			sql:  "ALTER TABLE tb1 DROP COLUMN col3",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "ALTER TABLE tb1 DROP COLUMN col3",
					StmtType: analyzer.StmtTypeAlterTable,
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						IsSpecifiedCluster:  false,
						DatabaseName:        "default_db",
						IsSpecifiedDatabase: false,
						TableName:           "tb1",
						Columns: []*analyzer.ActionColumn{
							{
								Name:   "col3",
								Action: analyzer.ActionTypeDrop,
							},
						},
						ActionType: analyzer.ActionTypeAlter,
					},
				},
			},
		},
		{
			name: "ALTER TABLE modify column",
			sql:  "ALTER TABLE db1.tb1 MODIFY COLUMN col2 BIGINT",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "ALTER TABLE db1.tb1 MODIFY COLUMN col2 BIGINT",
					StmtType: analyzer.StmtTypeAlterTable,
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						DatabaseName:        "db1",
						IsSpecifiedDatabase: true,
						TableName:           "tb1",
						Columns: []*analyzer.ActionColumn{
							{
								Name:   "col2",
								Type:   "BIGINT",
								Action: analyzer.ActionTypeAlter,
							},
						},
						ActionType: analyzer.ActionTypeAlter,
					},
				},
			},
		},
		{
			name: "ALTER TABLE SWAP WITH",
			sql:  "ALTER TABLE table14 SWAP WITH table15",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "ALTER TABLE table14 SWAP WITH table15",
					StmtType: analyzer.StmtTypeSwapTable,
					Action: &analyzer.ActionInfo{
						ClusterName:  "default_cluster",
						DatabaseName: "default_db",
						TableName:    "table14",
						ActionType:   analyzer.ActionTypeAlter,
					},
					AnotherAction: &analyzer.ActionInfo{
						ClusterName:  "default_cluster",
						DatabaseName: "default_db",
						TableName:    "table15",
						ActionType:   analyzer.ActionTypeAlter,
					},
				},
			},
		},
		{
			name: "ALTER TABLE RENAME",
			sql:  "ALTER TABLE `tb1` RENAME `tb2`",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "ALTER TABLE `tb1` RENAME `tb2`",
					StmtType: analyzer.StmtTypeRenameTable,
					Action: &analyzer.ActionInfo{
						ClusterName:  "default_cluster",
						DatabaseName: "default_db",
						TableName:    "tb1",
						ActionType:   analyzer.ActionTypeDrop,
					},
					AnotherAction: &analyzer.ActionInfo{
						ClusterName:  "default_cluster",
						DatabaseName: "default_db",
						TableName:    "tb2",
						ActionType:   analyzer.ActionTypeCreate,
					},
				},
			},
		},
		{
			name: "ALTER TABLE RENAME COLUMN",
			sql:  "ALTER TABLE db1.`tb1` RENAME COLUMN `col15` TO `desc`",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "ALTER TABLE db1.`tb1` RENAME COLUMN `col15` TO `desc`",
					StmtType: analyzer.StmtTypeAlterTable,
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						DatabaseName:        "db1",
						IsSpecifiedDatabase: true,
						TableName:           "tb1",
						ActionType:          analyzer.ActionTypeAlter,
						Columns: []*analyzer.ActionColumn{
							{
								Name:   "col15",
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
			name: "DROP TABLE statement",
			sql:  "DROP TABLE IF EXISTS db1.tb7",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "DROP TABLE IF EXISTS db1.tb7",
					StmtType: analyzer.StmtTypeDropTable,
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						DatabaseName:        "db1",
						IsSpecifiedDatabase: true,
						TableName:           "tb7",
						ActionType:          analyzer.ActionTypeDrop,
					},
				},
			},
		},
		{
			name: "USE statement",
			sql:  "USE db1",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "USE db1",
					StmtType: analyzer.StmtTypeUseDatabase,
				},
			},
		},
		{
			name: "CREATE VIEW statement with CTE",
			sql: `create view db1.table1 as with cte1 as ( 
  select 
    a.c1, 
    count( 
      distinct if( 
        a.c2 >= date_sub(curdate(), 30), 
        a.c3, 
        null 
      ) 
    ) c6, 
    ARRAY_DISTINCT( 
      ARRAY_AGG( 
        if( 
          a.c2 >= date_sub(curdate(), 30), 
          a.c3, 
          null 
        ) 
      ) 
    ) as c8, 
    count( 
      distinct if( 
        a.c2 >= date_sub(curdate(), 30), 
        concat(a.c3, a.c4), 
        null 
      ) 
    ) c7, 
    count( 
      distinct if( 
        a.c2 >= date_sub(curdate(), 90), 
        a.c3, 
        null 
      ) 
    ) c9, 
    ARRAY_DISTINCT( 
      ARRAY_AGG( 
        if( 
          a.c2 >= date_sub(curdate(), 90), 
          a.c3, 
          null 
        ) 
      ) 
    ) as c11, 
    count( 
      distinct if( 
        a.c2 >= date_sub(curdate(), 90), 
        concat(a.c3, a.c4), 
        null 
      ) 
    ) c10 
  from 
    db1.table2 a 
  where 
    a.c2 >= date_sub(curdate(), 90) 
    and a.c2 <= date_sub(curdate(), 1) 
    and a.c5 = 0 
  group by 
    a.c1 
) 
select 
  a.c12, 
  a.c13, 
  a.c14, 
  a.c15, 
  a.c16, 
  a.c17, 
  a.c18, 
  a.c19, 
  a.c20, 
  coalesce(b.c6, 0) as c6, 
  coalesce(b.c7, 0) as c7, 
  b.c8 as c8, 
  coalesce(b.c9, 0) as c9, 
  coalesce(b.c10, 0) as c10, 
  b.c11 as c11, 
  if(a.c16 = 'val3', 1, 0) as c21, 
  if(coalesce(b.c10, 0) = 0, 1, 0) as c22, 
  substr(date_sub(curdate(), 1), 1, 10) as col13 
from 
  db1.table3 a 
  left join cte1 b on a.c13 = b.c1 
where 
  a.c23 = 0`,
			expected: []*analyzer.DDLResult{
				{
					Stmt: `create view db1.table1 as with cte1 as ( 
  select 
    a.c1, 
    count( 
      distinct if( 
        a.c2 >= date_sub(curdate(), 30), 
        a.c3, 
        null 
      ) 
    ) c6, 
    ARRAY_DISTINCT( 
      ARRAY_AGG( 
        if( 
          a.c2 >= date_sub(curdate(), 30), 
          a.c3, 
          null 
        ) 
      ) 
    ) as c8, 
    count( 
      distinct if( 
        a.c2 >= date_sub(curdate(), 30), 
        concat(a.c3, a.c4), 
        null 
      ) 
    ) c7, 
    count( 
      distinct if( 
        a.c2 >= date_sub(curdate(), 90), 
        a.c3, 
        null 
      ) 
    ) c9, 
    ARRAY_DISTINCT( 
      ARRAY_AGG( 
        if( 
          a.c2 >= date_sub(curdate(), 90), 
          a.c3, 
          null 
        ) 
      ) 
    ) as c11, 
    count( 
      distinct if( 
        a.c2 >= date_sub(curdate(), 90), 
        concat(a.c3, a.c4), 
        null 
      ) 
    ) c10 
  from 
    db1.table2 a 
  where 
    a.c2 >= date_sub(curdate(), 90) 
    and a.c2 <= date_sub(curdate(), 1) 
    and a.c5 = 0 
  group by 
    a.c1 
) 
select 
  a.c12, 
  a.c13, 
  a.c14, 
  a.c15, 
  a.c16, 
  a.c17, 
  a.c18, 
  a.c19, 
  a.c20, 
  coalesce(b.c6, 0) as c6, 
  coalesce(b.c7, 0) as c7, 
  b.c8 as c8, 
  coalesce(b.c9, 0) as c9, 
  coalesce(b.c10, 0) as c10, 
  b.c11 as c11, 
  if(a.c16 = 'val3', 1, 0) as c21, 
  if(coalesce(b.c10, 0) = 0, 1, 0) as c22, 
  substr(date_sub(curdate(), 1), 1, 10) as col13 
from 
  db1.table3 a 
  left join cte1 b on a.c13 = b.c1 
where 
  a.c23 = 0`,
					StmtType: analyzer.StmtTypeCreateView,
					Action: &analyzer.ActionInfo{
						ClusterName:  "default_cluster",
						DatabaseName: "db1",
						TableName:    "table1",
						ActionType:   analyzer.ActionTypeCreate,
					},
				},
			},
		},
		{
			name: "CREATE OR REPLACE VIEW statement",
			sql: `create or replace view db2.view1 as 
SELECT /*+ SET_VAR(group_concat_max_len = 10240) */ 
    c24, 
    c25, 
    group_concat(distinct concat('@', c26, '@') order by c27 desc, c14 desc SEPARATOR ',') as tags 
FROM table4 WHERE 
     valid = 1 group by 
     c24, 
     c25`,
			expected: []*analyzer.DDLResult{
				{
					Stmt: `create or replace view db2.view1 as 
SELECT /*+ SET_VAR(group_concat_max_len = 10240) */ 
    c24, 
    c25, 
    group_concat(distinct concat('@', c26, '@') order by c27 desc, c14 desc SEPARATOR ',') as tags 
FROM table4 WHERE 
     valid = 1 group by 
     c24, 
     c25`,
					StmtType: analyzer.StmtTypeCreateView,
					Action: &analyzer.ActionInfo{
						ClusterName:  "default_cluster",
						DatabaseName: "db2",
						TableName:    "view1",
						ActionType:   analyzer.ActionTypeCreate,
					},
				},
			},
		},
		{
			name: "CREATE VIEW with backticks",
			sql:  "create view `table6` as \n SELECT \n     `id` \n     ,`c24` \n     ,`c28` \n     ,`c29` \n     ,`c30` \n     ,`c31` \n     ,`c32` \n     ,`c19` \n     ,`c33` \n     ,`c14` \n     ,`c27` \n \n FROM `db5`.`table5` t1",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "create view `table6` as \n SELECT \n     `id` \n     ,`c24` \n     ,`c28` \n     ,`c29` \n     ,`c30` \n     ,`c31` \n     ,`c32` \n     ,`c19` \n     ,`c33` \n     ,`c14` \n     ,`c27` \n \n FROM `db5`.`table5` t1",
					StmtType: analyzer.StmtTypeCreateView,
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						IsSpecifiedCluster:  false,
						DatabaseName:        "default_db",
						IsSpecifiedDatabase: false,
						TableName:           "table6",
						ActionType:          analyzer.ActionTypeCreate,
					},
				},
			},
		},
		{
			name: "ALTER MATERIALIZED VIEW RENAME",
			sql:  "ALTER MATERIALIZED VIEW table7 RENAME table8",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "ALTER MATERIALIZED VIEW table7 RENAME table8",
					StmtType: analyzer.StmtTypeAlterMaterializedView,
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						IsSpecifiedCluster:  false,
						DatabaseName:        "default_db",
						IsSpecifiedDatabase: false,
						TableName:           "table7",
						ActionType:          analyzer.ActionTypeAlter,
					},
				},
			},
		},
		{
			name: "CREATE MATERIALIZED VIEW",
			sql:  "CREATE MATERIALIZED VIEW if not exists db3.table9 \n DISTRIBUTED BY HASH(`c24`) BUCKETS 10 \n REFRESH MANUAL \n AS \n select MONTHS_ADD(c34,1) col13 \n ,c24 \n ,c35/100 c36 \n ,case when c35>100000000 then '1M+' \n when c35>50000000 then '500k-1M' \n when c35>10000000 then '100k-500k' \n when c35>1000000 then '10k-100k' \n else '0-10k' end c37 \n from( \n select date_format(c38,'%Y-%m-01') c34 \n   ,a.c24 \n   ,sum(a.c39) c35 \n from db3.table10 a \n where a.c40=1 \n group by date_format(c38,'%Y-%m-01') \n   ,a.c24) t",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "CREATE MATERIALIZED VIEW if not exists db3.table9 \n DISTRIBUTED BY HASH(`c24`) BUCKETS 10 \n REFRESH MANUAL \n AS \n select MONTHS_ADD(c34,1) col13 \n ,c24 \n ,c35/100 c36 \n ,case when c35>100000000 then '1M+' \n when c35>50000000 then '500k-1M' \n when c35>10000000 then '100k-500k' \n when c35>1000000 then '10k-100k' \n else '0-10k' end c37 \n from( \n select date_format(c38,'%Y-%m-01') c34 \n   ,a.c24 \n   ,sum(a.c39) c35 \n from db3.table10 a \n where a.c40=1 \n group by date_format(c38,'%Y-%m-01') \n   ,a.c24) t",
					StmtType: analyzer.StmtTypeCreateMaterializedView,
					Action: &analyzer.ActionInfo{
						ClusterName:  "default_cluster",
						DatabaseName: "db3",
						TableName:    "table9",
						ActionType:   analyzer.ActionTypeCreate,
					},
				},
			},
		},
		{
			name: "DROP VIEW statement",
			sql:  "drop view table5",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "drop view table5",
					StmtType: analyzer.StmtTypeDropView,
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						IsSpecifiedCluster:  false,
						DatabaseName:        "default_db",
						IsSpecifiedDatabase: false,
						TableName:           "table5",
						ActionType:          analyzer.ActionTypeDrop,
					},
				},
			},
		},
		{
			name: "DROP TABLE statement",
			sql:  "drop table table16",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "drop table table16",
					StmtType: analyzer.StmtTypeDropTable,
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						IsSpecifiedCluster:  false,
						DatabaseName:        "default_db",
						IsSpecifiedDatabase: false,
						TableName:           "table16",
						ActionType:          analyzer.ActionTypeDrop,
					},
				},
			},
		},
		{
			name: "DROP VIEW IF EXISTS statement",
			sql:  "DROP VIEW IF EXISTS view2",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "DROP VIEW IF EXISTS view2",
					StmtType: analyzer.StmtTypeDropView,
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						IsSpecifiedCluster:  false,
						DatabaseName:        "default_db",
						IsSpecifiedDatabase: false,
						TableName:           "view2",
						ActionType:          analyzer.ActionTypeDrop,
					},
				},
			},
		},
		{
			name: "DROP MATERIALIZED VIEW statement",
			sql:  "DROP MATERIALIZED VIEW order_mv1;",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "DROP MATERIALIZED VIEW order_mv1;",
					StmtType: analyzer.StmtTypeDropMaterializedView,
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						IsSpecifiedCluster:  false,
						DatabaseName:        "default_db",
						IsSpecifiedDatabase: false,
						TableName:           "order_mv1",
						ActionType:          analyzer.ActionTypeDrop,
					},
				},
			},
		},
		{
			name: "ALTER VIEW statement",
			sql:  "--测试1 \n alter view db3.table11 ( \n   col13 comment '测试2', \n   c24 comment '测试3', \n   c41 comment '测试4', \n   c42 comment '测试5', \n   c43 comment '测试6', \n   c44 comment '测试7' \n )  as \n select a.col13 \n   ,a.c24 \n   ,b.c41 \n   ,a.c42 \n   ,a.c43 \n   ,b.c44 \n from db4.table12 a \n join db4.table13 b on a.c24=b.c24 and b.c45 not in ('日常测试')",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "--测试1 \n alter view db3.table11 ( \n   col13 comment '测试2', \n   c24 comment '测试3', \n   c41 comment '测试4', \n   c42 comment '测试5', \n   c43 comment '测试6', \n   c44 comment '测试7' \n )  as \n select a.col13 \n   ,a.c24 \n   ,b.c41 \n   ,a.c42 \n   ,a.c43 \n   ,b.c44 \n from db4.table12 a \n join db4.table13 b on a.c24=b.c24 and b.c45 not in ('日常测试')",
					StmtType: analyzer.StmtTypeAlterView,
					Action: &analyzer.ActionInfo{
						ClusterName:  "default_cluster",
						DatabaseName: "db3",
						TableName:    "table11",
						ActionType:   analyzer.ActionTypeAlter,
					},
				},
			},
		},
		{
			name: "CREATE DATABASE statement",
			sql:  "CREATE DATABASE db_test;",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "CREATE DATABASE db_test;",
					StmtType: analyzer.StmtTypeCreateDatabase,
					Action: &analyzer.ActionInfo{
						ClusterName:         "default_cluster",
						IsSpecifiedCluster:  false,
						DatabaseName:        "db_test",
						IsSpecifiedDatabase: true,
						TableName:           "",
						ActionType:          analyzer.ActionTypeCreate,
					},
				},
			},
		},
		{
			name: "DROP DATABASE statement",
			sql:  "DROP DATABASE db_test;",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "DROP DATABASE db_test;",
					StmtType: analyzer.StmtTypeDropDatabase,
					Action: &analyzer.ActionInfo{
						ClusterName:  "default_cluster",
						DatabaseName: "db_test",
						TableName:    "",
						ActionType:   analyzer.ActionTypeDrop,
					},
				},
			},
		},
		{
			name: "ALTER DATABASE RENAME statement",
			sql:  "ALTER DATABASE example_db RENAME example_db2;",
			expected: []*analyzer.DDLResult{
				{
					Stmt:     "ALTER DATABASE example_db RENAME example_db2;",
					StmtType: analyzer.StmtTypeAlterDatabase,
					Action: &analyzer.ActionInfo{
						ClusterName:  "default_cluster",
						DatabaseName: "example_db",
						TableName:    "",
						ActionType:   analyzer.ActionTypeAlter,
					},
				},
			},
		},
	}

	starRocksAnalyzer := NewSQLAnalyzer()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &analyzer.AnalyzeReq{
				DefaultCluster:  "default_cluster",
				DefaultDatabase: "default_db",
				Type:            analyzer.EngineSpark,
				SQL:             tt.sql,
			}

			results, err := starRocksAnalyzer.AnalyzeDDL(req)
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

func TestStarRocksMakeCommentModification(t *testing.T) {
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
			ddl:         "CREATE TABLE tb7 (id INT, col1 VARCHAR(50)) ENGINE=OLAP DISTRIBUTED BY HASH(id) BUCKETS 10",
			columnName:  "col1",
			comment:     "测试8",
			expected:    "ALTER TABLE `tb7` MODIFY COLUMN `col1` VARCHAR(50) COMMENT '测试8';",
			expectedErr: false,
		},
		{
			name:        "Modify column comment with database",
			ddl:         "CREATE TABLE db1.tb7 (id INT, col1 VARCHAR(50) NOT NULL) ENGINE=OLAP DISTRIBUTED BY HASH(id) BUCKETS 10",
			columnName:  "col1",
			comment:     "测试8",
			expected:    "ALTER TABLE `db1`.`tb7` MODIFY COLUMN `col1` VARCHAR(50) NOT NULL COMMENT '测试8';",
			expectedErr: false,
		},
		{
			name:        "Column not found",
			ddl:         "CREATE TABLE tb7 (id INT, col1 VARCHAR(50)) ENGINE=OLAP DISTRIBUTED BY HASH(id) BUCKETS 10",
			columnName:  "c51",
			comment:     "测试9",
			expected:    "",
			expectedErr: true,
		},
	}

	starRocksAnalyzer := NewSQLAnalyzer()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &analyzer.MakeCommentModificationReq{
				Type:       analyzer.EngineStarRocks,
				DDL:        tt.ddl,
				ColumnName: tt.columnName,
				Comment:    tt.comment,
			}

			result, err := starRocksAnalyzer.MakeCommentModification(req)
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
func TestStarRocksSplit(t *testing.T) {
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
			sql:      "INSERT INTO table1 (col1) VALUES ('test;test'); SELECT * FROM table2 WHERE col1 = 'a;b;c';",
			expected: []string{"INSERT INTO table1 (col1) VALUES ('test;test');", "SELECT * FROM table2 WHERE col1 = 'a;b;c';"},
		},
	}

	starRocksAnalyzer := NewSQLAnalyzer()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &analyzer.SplitReq{
				Type: analyzer.EngineStarRocks,
				SQL:  tt.sql,
			}

			result, err := starRocksAnalyzer.Split(req)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}
