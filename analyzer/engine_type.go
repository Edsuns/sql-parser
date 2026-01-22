package analyzer

type EngineType string

const (
	SQLTypeMySQL     EngineType = "mysql"
	SQLTypeTiDB      EngineType = "tidb"
	SQLTypeSpark     EngineType = "spark"
	SQLTypeHive      EngineType = "hive"
	SQLTypeStarRocks EngineType = "starrocks"
)
