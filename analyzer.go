package parser

import (
	"github.com/Edsuns/sql-parser/analyzer"
	"github.com/Edsuns/sql-parser/internal/hive"
	"github.com/Edsuns/sql-parser/internal/spark"
	"github.com/Edsuns/sql-parser/internal/tidb"
)

func NewHiveDependencyAnalyzer() analyzer.DependencyAnalyzer {
	return hive.NewDependencyAnalyzer()
}

func NewSparkDependencyAnalyzer() analyzer.DependencyAnalyzer {
	return spark.NewDependencyAnalyzer()
}

func NewTiDBDependencyAnalyzer() analyzer.DependencyAnalyzer {
	return tidb.NewDependencyAnalyzer()
}
