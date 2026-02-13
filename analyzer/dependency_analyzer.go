package analyzer

import "fmt"

var (
	ActionTypeCreate ActionType = "CREATE"
	ActionTypeAlter  ActionType = "ALTER"
	ActionTypeDrop   ActionType = "DROP"
)

type (
	DependencyAnalyzeReq struct {
		DefaultCluster  string     `json:"defaultCluster"`
		DefaultDatabase string     `json:"defaultDatabase"`
		Type            EngineType `json:"type"`
		SQL             string     `json:"sql"`
	}
)

type (
	ActionType      string
	DependencyTable struct {
		Cluster  string `json:"cluster"`
		Database string `json:"database"`
		Table    string `json:"table"`
	}
	ActionColumn struct {
		Name         string     `json:"name"`
		Type         string     `json:"type"`
		IsNotNull    bool       `json:"isNotNull"`
		IsPrimary    bool       `json:"isPrimary"`
		DefaultValue string     `json:"defaultValue"`
		Comment      string     `json:"comment"`
		Action       ActionType `json:"action"`
	}
	ActionTable struct {
		Cluster  string          `json:"cluster"`
		Database string          `json:"database"`
		Table    string          `json:"table"`
		Columns  []*ActionColumn `json:"columns"`
		Action   ActionType      `json:"action"`
	}
	DependencyResult struct {
		Stmt     string             `json:"stmt"`
		StmtType StmtType           `json:"stmtType"`
		Read     []*DependencyTable `json:"read"`
		Write    []*DependencyTable `json:"write"`
		Actions  []*ActionTable     `json:"actions"`
	}
)

func (d *DependencyTable) String() string {
	return fmt.Sprintf("%s.%s.%s", d.Cluster, d.Database, d.Table)
}

type DependencyAnalyzer interface {
	// Analyze 分析SQL读写表和语句类型 StmtType
	Analyze(req *DependencyAnalyzeReq) ([]*DependencyResult, error)
	// ParseOne 解析单句SQL
	ParseOne(stmt, defaultCluster, defaultDatabase string) (*DependencyResult, error)
}
