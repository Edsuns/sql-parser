package analyzer

type StmtType string

const (
	StmtTypeSelect       StmtType = "SELECT"
	StmtTypeInsert       StmtType = "INSERT"
	StmtTypeUpdate       StmtType = "UPDATE"
	StmtTypeDelete       StmtType = "DELETE"
	StmtTypeMerge        StmtType = "MERGE"
	StmtTypeCreateTable  StmtType = "CREATE_TABLE"
	StmtTypeCreateView   StmtType = "CREATE_VIEW"
	StmtTypeAlterTable   StmtType = "ALTER_TABLE"
	StmtTypeReplaceTable StmtType = "REPLACE_TABLE"
	StmtTypeDropTable    StmtType = "DROP_TABLE"
	StmtTypeCreateLike   StmtType = "CREATE_LIKE"
	StmtTypeTruncate     StmtType = "TRUNCATE"
	StmtTypeUseDatabase  StmtType = "USE_DATABASE"
	StmtTypeUseCatalog   StmtType = "USE_CATALOG"
)
