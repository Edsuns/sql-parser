package analyzer

type EngineType string

const (
	EngineMySQL     EngineType = "mysql"
	EngineTiDB      EngineType = "tidb"
	EngineSpark     EngineType = "spark"
	EngineHive      EngineType = "hive"
	EngineStarRocks EngineType = "starrocks"
)
