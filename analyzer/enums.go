package analyzer

type StmtType string

const (
	StmtTypeUnknown                StmtType = "UNKNOWN"
	StmtTypeSelect                 StmtType = "SELECT"
	StmtTypeInsert                 StmtType = "INSERT"
	StmtTypeUpdate                 StmtType = "UPDATE"
	StmtTypeDelete                 StmtType = "DELETE"
	StmtTypeMerge                  StmtType = "MERGE"
	StmtTypeCreateDatabase         StmtType = "CREATE_DATABASE"
	StmtTypeCreateTable            StmtType = "CREATE_TABLE"
	StmtTypeCreateTemporaryTable   StmtType = "CREATE_TEMPORARY_TABLE"
	StmtTypeCreateTableLike        StmtType = "CREATE_TABLE_LIKE"
	StmtTypeCreateView             StmtType = "CREATE_VIEW"
	StmtTypeCreateTemporaryView    StmtType = "CREATE_TEMPORARY_VIEW"
	StmtTypeCreateMaterializedView StmtType = "CREATE_MATERIALIZED_VIEW"
	StmtTypeAlterDatabase          StmtType = "ALTER_DATABASE"
	StmtTypeAlterTable             StmtType = "ALTER_TABLE"
	StmtTypeRenameTable            StmtType = "RENAME_TABLE"
	StmtTypeSwapTable              StmtType = "SWAP_TABLE"
	StmtTypeAlterView              StmtType = "ALTER_VIEW"
	StmtTypeAlterMaterializedView  StmtType = "ALTER_MATERIALIZED_VIEW"
	StmtTypeReplaceTable           StmtType = "REPLACE_TABLE"
	StmtTypeDropDatabase           StmtType = "DROP_DATABASE"
	StmtTypeDropTable              StmtType = "DROP_TABLE"
	StmtTypeDropView               StmtType = "DROP_VIEW"
	StmtTypeDropMaterializedView   StmtType = "DROP_MATERIALIZED_VIEW"
	StmtTypeTruncateTable          StmtType = "TRUNCATE_TABLE"
	StmtTypeUseDatabase            StmtType = "USE_DATABASE"
	StmtTypeUseCatalog             StmtType = "USE_CATALOG"
	StmtTypeSetVar                 StmtType = "SET_VAR"
	StmtTypeTempFunc               StmtType = "TEMP_FUNCTION"
	StmtTypeCall                   StmtType = "CALL"
)

func (s StmtType) IsDDL() bool {
	return s == StmtTypeCreateDatabase ||
		s == StmtTypeCreateTable ||
		s == StmtTypeCreateTemporaryTable ||
		s == StmtTypeCreateView ||
		s == StmtTypeCreateTemporaryView ||
		s == StmtTypeCreateMaterializedView ||
		s == StmtTypeAlterDatabase ||
		s == StmtTypeAlterTable ||
		s == StmtTypeSwapTable ||
		s == StmtTypeAlterView ||
		s == StmtTypeAlterMaterializedView ||
		s == StmtTypeReplaceTable ||
		s == StmtTypeDropDatabase ||
		s == StmtTypeDropTable ||
		s == StmtTypeDropView ||
		s == StmtTypeDropMaterializedView ||
		s == StmtTypeCreateTableLike ||
		s == StmtTypeTruncateTable
}

func (s StmtType) String() string {
	return string(s)
}

// ActionType DDL操作类型
type ActionType string

var (
	ActionTypeCreate ActionType = "CREATE" // 库/表/字段的添加
	ActionTypeAlter  ActionType = "ALTER"  // 库/表/字段的修改
	ActionTypeDrop   ActionType = "DROP"   // 库/表/字段的删除
)

// EngineType 引擎类型
type EngineType string

const (
	EngineMySQL     EngineType = "mysql"
	EngineTiDB      EngineType = "tidb"
	EngineSpark     EngineType = "spark"
	EngineHive      EngineType = "hive"
	EngineStarRocks EngineType = "starrocks"
)
