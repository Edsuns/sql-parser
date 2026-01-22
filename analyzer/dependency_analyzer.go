package analyzer

import "fmt"

type (
	DependencyAnalyzeReq struct {
		DefaultCluster  string     `json:"defaultCluster"`
		DefaultDatabase string     `json:"defaultDatabase"`
		Type            EngineType `json:"type"`
		SQL             string     `json:"sql"`
	}
)

type (
	DependencyTable struct {
		Cluster  string `json:"cluster"`
		Database string `json:"database"`
		Table    string `json:"table"`
	}
	DependencyResult struct {
		Stmt     string             `json:"stmt"`
		StmtType StmtType           `json:"stmtType"`
		Read     []*DependencyTable `json:"read"`
		Write    []*DependencyTable `json:"write"`
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
