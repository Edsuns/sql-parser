package sqlparser

import (
	"errors"
	"sync"

	"github.com/Edsuns/sql-parser/analyzer"
	"github.com/Edsuns/sql-parser/internal/mysql"
	"github.com/Edsuns/sql-parser/internal/spark"
	"github.com/Edsuns/sql-parser/internal/starrocks"
	"github.com/Edsuns/sql-parser/internal/tidb"
)

type delegateSQLAnalyzer struct {
	once              sync.Once
	mysqlAnalyzer     analyzer.SQLAnalyzer
	sparkAnalyzer     analyzer.SQLAnalyzer
	starrocksAnalyzer analyzer.SQLAnalyzer
	tidbAnalyzer      analyzer.SQLAnalyzer
}

// NewAnalyzer 创建SQL分析器
func NewAnalyzer() analyzer.SQLAnalyzer {
	return &delegateSQLAnalyzer{}
}

func (d *delegateSQLAnalyzer) AnalyzeLineage(req *analyzer.AnalyzeReq) ([]*analyzer.LineageResult, error) {
	impl, err := d.route(req.Type)
	if err != nil {
		return nil, err
	}
	return impl.AnalyzeLineage(req)
}

func (d *delegateSQLAnalyzer) AnalyzeDDL(req *analyzer.AnalyzeReq) ([]*analyzer.DDLResult, error) {
	impl, err := d.route(req.Type)
	if err != nil {
		return nil, err
	}
	return impl.AnalyzeDDL(req)
}

func (d *delegateSQLAnalyzer) MakeCommentModification(req *analyzer.MakeCommentModificationReq) (string, error) {
	impl, err := d.route(req.Type)
	if err != nil {
		return "", err
	}
	return impl.MakeCommentModification(req)
}

func (d *delegateSQLAnalyzer) Split(req *analyzer.SplitReq) ([]string, error) {
	impl, err := d.route(req.Type)
	if err != nil {
		return nil, err
	}
	return impl.Split(req)
}

func (d *delegateSQLAnalyzer) route(t analyzer.EngineType) (analyzer.SQLAnalyzer, error) {
	d.once.Do(func() {
		d.mysqlAnalyzer = mysql.NewSQLAnalyzer()
		d.sparkAnalyzer = spark.NewSQLAnalyzer()
		d.starrocksAnalyzer = starrocks.NewSQLAnalyzer()
		d.tidbAnalyzer = tidb.NewSQLAnalyzer()
	})
	switch t {
	case analyzer.EngineMySQL:
		return d.mysqlAnalyzer, nil
	case analyzer.EngineSpark:
		return d.sparkAnalyzer, nil
	case analyzer.EngineStarRocks:
		return d.starrocksAnalyzer, nil
	case analyzer.EngineTiDB:
		return d.tidbAnalyzer, nil
	default:
		return nil, errors.New("unsupported engine type")
	}
}
