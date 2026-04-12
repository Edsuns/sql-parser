package analyzer

import "fmt"

type (
	// AnalyzeReq 分析入参
	AnalyzeReq struct {
		DefaultCluster  string     `json:"defaultCluster"`  // 默认集群，必填
		DefaultDatabase string     `json:"defaultDatabase"` // 默认数据库，必填
		Type            EngineType `json:"type"`            // SQL引擎类型，必填
		SQL             string     `json:"sql"`             // SQL值，必填
	}
	// MakeCommentModificationReq 生成注释修改语句
	MakeCommentModificationReq struct {
		Type       EngineType `json:"type"`       // SQL引擎类型，必填
		DDL        string     `json:"ddl"`        // 建表语句
		ColumnName string     `json:"columnName"` // 要修改的字段
		Comment    string     `json:"comment"`    // 注释要改成的值
	}
	// SplitReq 分割SQL入参
	SplitReq struct {
		Type EngineType `json:"type"` // SQL引擎类型，必填
		SQL  string     `json:"sql"`  // SQL值，必填
	}
)

type (
	// Dependency 读写表
	Dependency struct {
		IsTemp   bool   `json:"isTemp"`
		Cluster  string `json:"cluster"`
		Database string `json:"database"`
		Table    string `json:"table"`
	}
	// ActionColumn DDL操作字段
	ActionColumn struct {
		Name         string     `json:"name"`
		Type         string     `json:"type"`
		IsNotNull    bool       `json:"isNotNull"`
		IsPrimary    bool       `json:"isPrimary"`
		DefaultValue string     `json:"defaultValue"`
		Comment      string     `json:"comment"`
		Action       ActionType `json:"action"`
	}
	// TableInfo DDL表信息
	TableInfo struct {
		// 共有技术信息
		PartitionColumnNames  []string `json:"partitionColumnNames"`
		PrimaryKeyColumnNames []string `json:"primaryKeyColumnNames"`

		// StarRocks 技术信息
		Compression            string   `json:"compression"`
		DistributedColumnNames []string `json:"distributedColumnNames"`
		DataModel              string   `json:"dataModel"` // PRIMARY/DUPLICATE/UNIQUE/AGGREGATE
		// LifecycleValue         int64    `json:"lifecycleValue"`
		// LifecycleTimeUnit      string   `json:"lifecycleTimeUnit"`

		// Spark 技术信息
		Locations            []string `json:"locations"`
		LakehouseTableFormat string   `json:"lakehouseTableFormat"` // hive/paimon/iceberg
		FileFormat           string   `json:"fileFormat"`           // parquet/orc/avro
	}
	// ActionInfo DDL操作信息
	ActionInfo struct {
		ClusterName         string          `json:"clusterName"`
		IsSpecifiedCluster  bool            `json:"isSpecifiedCluster"` // SQL中是否直接指定了集群
		DatabaseName        string          `json:"databaseName"`
		IsSpecifiedDatabase bool            `json:"isSpecifiedDatabase"` // SQL中是否直接指定了数据库
		TableName           string          `json:"tableName"`           // 如果是操作集群/数据库的语句则为空字符串
		Columns             []*ActionColumn `json:"columns"`
		ActionType          ActionType      `json:"actionType"`
		TableInfo           *TableInfo      `json:"tableInfo"` // 如果是建表语句则有值
	}
	// LineageResult 血缘分析结果
	LineageResult struct {
		Stmt     string        `json:"stmt"`
		StmtType StmtType      `json:"stmtType"`
		Reads    []*Dependency `json:"reads"`
		Writes   []*Dependency `json:"writes"`
	}
	// DDLResult DDL分析结果
	DDLResult struct {
		Stmt          string      `json:"stmt"`
		StmtType      StmtType    `json:"stmtType"`
		Action        *ActionInfo `json:"action"`
		AnotherAction *ActionInfo `json:"anotherAction"` // RENAME DATABASE/RENAME TABLE/SWAP TABLE 时有值
	}
)

func (d *Dependency) IsCluster() bool {
	return d.Cluster != "" && d.Database == "" && d.Table == ""
}

func (d *Dependency) IsDatabase() bool {
	return d.Database != "" && d.Table == ""
}

// IsTable 是否是表
func (d *Dependency) IsTable() bool {
	return d.Table != ""
}

// String 读写表转字符串
func (d *Dependency) String() string {
	if d.IsCluster() {
		return d.Cluster
	}
	if d.IsDatabase() {
		return fmt.Sprintf("%s.%s", d.Cluster, d.Database)
	}
	return fmt.Sprintf("%s.%s.%s", d.Cluster, d.Database, d.Table)
}

// SQLAnalyzer SQL分析器接口定义
type SQLAnalyzer interface {
	// Split 分割多句SQL为多个单句SQL
	Split(req *SplitReq) ([]string, error)
	// AnalyzeLineage 分析SQL血缘
	AnalyzeLineage(req *AnalyzeReq) ([]*LineageResult, error)
	// AnalyzeDDL 分析DDL信息
	AnalyzeDDL(req *AnalyzeReq) ([]*DDLResult, error)
	// MakeCommentModification 生成注释修改语句
	MakeCommentModification(req *MakeCommentModificationReq) (string, error)
}
