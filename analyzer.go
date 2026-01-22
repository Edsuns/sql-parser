package parser

import (
	"sql-parser/analyzer"
	"sql-parser/internal/hive"
	"sql-parser/internal/spark"
	"sql-parser/internal/tidb"
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
