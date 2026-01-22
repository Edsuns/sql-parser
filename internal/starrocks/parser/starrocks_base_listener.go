// Code generated from StarRocks.g4 by ANTLR 4.13.2. DO NOT EDIT.

package parser // StarRocks
import "github.com/antlr4-go/antlr/v4"

// BaseStarRocksListener is a complete listener for a parse tree produced by StarRocksParser.
type BaseStarRocksListener struct{}

var _ StarRocksListener = &BaseStarRocksListener{}

// VisitTerminal is called when a terminal node is visited.
func (s *BaseStarRocksListener) VisitTerminal(node antlr.TerminalNode) {}

// VisitErrorNode is called when an error node is visited.
func (s *BaseStarRocksListener) VisitErrorNode(node antlr.ErrorNode) {}

// EnterEveryRule is called when any rule is entered.
func (s *BaseStarRocksListener) EnterEveryRule(ctx antlr.ParserRuleContext) {}

// ExitEveryRule is called when any rule is exited.
func (s *BaseStarRocksListener) ExitEveryRule(ctx antlr.ParserRuleContext) {}

// EnterSqlStatements is called when production sqlStatements is entered.
func (s *BaseStarRocksListener) EnterSqlStatements(ctx *SqlStatementsContext) {}

// ExitSqlStatements is called when production sqlStatements is exited.
func (s *BaseStarRocksListener) ExitSqlStatements(ctx *SqlStatementsContext) {}

// EnterSingleStatement is called when production singleStatement is entered.
func (s *BaseStarRocksListener) EnterSingleStatement(ctx *SingleStatementContext) {}

// ExitSingleStatement is called when production singleStatement is exited.
func (s *BaseStarRocksListener) ExitSingleStatement(ctx *SingleStatementContext) {}

// EnterEmptyStatement is called when production emptyStatement is entered.
func (s *BaseStarRocksListener) EnterEmptyStatement(ctx *EmptyStatementContext) {}

// ExitEmptyStatement is called when production emptyStatement is exited.
func (s *BaseStarRocksListener) ExitEmptyStatement(ctx *EmptyStatementContext) {}

// EnterStatement is called when production statement is entered.
func (s *BaseStarRocksListener) EnterStatement(ctx *StatementContext) {}

// ExitStatement is called when production statement is exited.
func (s *BaseStarRocksListener) ExitStatement(ctx *StatementContext) {}

// EnterUseDatabaseStatement is called when production useDatabaseStatement is entered.
func (s *BaseStarRocksListener) EnterUseDatabaseStatement(ctx *UseDatabaseStatementContext) {}

// ExitUseDatabaseStatement is called when production useDatabaseStatement is exited.
func (s *BaseStarRocksListener) ExitUseDatabaseStatement(ctx *UseDatabaseStatementContext) {}

// EnterUseCatalogStatement is called when production useCatalogStatement is entered.
func (s *BaseStarRocksListener) EnterUseCatalogStatement(ctx *UseCatalogStatementContext) {}

// ExitUseCatalogStatement is called when production useCatalogStatement is exited.
func (s *BaseStarRocksListener) ExitUseCatalogStatement(ctx *UseCatalogStatementContext) {}

// EnterSetCatalogStatement is called when production setCatalogStatement is entered.
func (s *BaseStarRocksListener) EnterSetCatalogStatement(ctx *SetCatalogStatementContext) {}

// ExitSetCatalogStatement is called when production setCatalogStatement is exited.
func (s *BaseStarRocksListener) ExitSetCatalogStatement(ctx *SetCatalogStatementContext) {}

// EnterShowDatabasesStatement is called when production showDatabasesStatement is entered.
func (s *BaseStarRocksListener) EnterShowDatabasesStatement(ctx *ShowDatabasesStatementContext) {}

// ExitShowDatabasesStatement is called when production showDatabasesStatement is exited.
func (s *BaseStarRocksListener) ExitShowDatabasesStatement(ctx *ShowDatabasesStatementContext) {}

// EnterAlterDbQuotaStatement is called when production alterDbQuotaStatement is entered.
func (s *BaseStarRocksListener) EnterAlterDbQuotaStatement(ctx *AlterDbQuotaStatementContext) {}

// ExitAlterDbQuotaStatement is called when production alterDbQuotaStatement is exited.
func (s *BaseStarRocksListener) ExitAlterDbQuotaStatement(ctx *AlterDbQuotaStatementContext) {}

// EnterCreateDbStatement is called when production createDbStatement is entered.
func (s *BaseStarRocksListener) EnterCreateDbStatement(ctx *CreateDbStatementContext) {}

// ExitCreateDbStatement is called when production createDbStatement is exited.
func (s *BaseStarRocksListener) ExitCreateDbStatement(ctx *CreateDbStatementContext) {}

// EnterDropDbStatement is called when production dropDbStatement is entered.
func (s *BaseStarRocksListener) EnterDropDbStatement(ctx *DropDbStatementContext) {}

// ExitDropDbStatement is called when production dropDbStatement is exited.
func (s *BaseStarRocksListener) ExitDropDbStatement(ctx *DropDbStatementContext) {}

// EnterShowCreateDbStatement is called when production showCreateDbStatement is entered.
func (s *BaseStarRocksListener) EnterShowCreateDbStatement(ctx *ShowCreateDbStatementContext) {}

// ExitShowCreateDbStatement is called when production showCreateDbStatement is exited.
func (s *BaseStarRocksListener) ExitShowCreateDbStatement(ctx *ShowCreateDbStatementContext) {}

// EnterAlterDatabaseRenameStatement is called when production alterDatabaseRenameStatement is entered.
func (s *BaseStarRocksListener) EnterAlterDatabaseRenameStatement(ctx *AlterDatabaseRenameStatementContext) {
}

// ExitAlterDatabaseRenameStatement is called when production alterDatabaseRenameStatement is exited.
func (s *BaseStarRocksListener) ExitAlterDatabaseRenameStatement(ctx *AlterDatabaseRenameStatementContext) {
}

// EnterRecoverDbStmt is called when production recoverDbStmt is entered.
func (s *BaseStarRocksListener) EnterRecoverDbStmt(ctx *RecoverDbStmtContext) {}

// ExitRecoverDbStmt is called when production recoverDbStmt is exited.
func (s *BaseStarRocksListener) ExitRecoverDbStmt(ctx *RecoverDbStmtContext) {}

// EnterShowDataStmt is called when production showDataStmt is entered.
func (s *BaseStarRocksListener) EnterShowDataStmt(ctx *ShowDataStmtContext) {}

// ExitShowDataStmt is called when production showDataStmt is exited.
func (s *BaseStarRocksListener) ExitShowDataStmt(ctx *ShowDataStmtContext) {}

// EnterShowDataDistributionStmt is called when production showDataDistributionStmt is entered.
func (s *BaseStarRocksListener) EnterShowDataDistributionStmt(ctx *ShowDataDistributionStmtContext) {}

// ExitShowDataDistributionStmt is called when production showDataDistributionStmt is exited.
func (s *BaseStarRocksListener) ExitShowDataDistributionStmt(ctx *ShowDataDistributionStmtContext) {}

// EnterCreateTableStatement is called when production createTableStatement is entered.
func (s *BaseStarRocksListener) EnterCreateTableStatement(ctx *CreateTableStatementContext) {}

// ExitCreateTableStatement is called when production createTableStatement is exited.
func (s *BaseStarRocksListener) ExitCreateTableStatement(ctx *CreateTableStatementContext) {}

// EnterColumnDesc is called when production columnDesc is entered.
func (s *BaseStarRocksListener) EnterColumnDesc(ctx *ColumnDescContext) {}

// ExitColumnDesc is called when production columnDesc is exited.
func (s *BaseStarRocksListener) ExitColumnDesc(ctx *ColumnDescContext) {}

// EnterCharsetName is called when production charsetName is entered.
func (s *BaseStarRocksListener) EnterCharsetName(ctx *CharsetNameContext) {}

// ExitCharsetName is called when production charsetName is exited.
func (s *BaseStarRocksListener) ExitCharsetName(ctx *CharsetNameContext) {}

// EnterDefaultDesc is called when production defaultDesc is entered.
func (s *BaseStarRocksListener) EnterDefaultDesc(ctx *DefaultDescContext) {}

// ExitDefaultDesc is called when production defaultDesc is exited.
func (s *BaseStarRocksListener) ExitDefaultDesc(ctx *DefaultDescContext) {}

// EnterGeneratedColumnDesc is called when production generatedColumnDesc is entered.
func (s *BaseStarRocksListener) EnterGeneratedColumnDesc(ctx *GeneratedColumnDescContext) {}

// ExitGeneratedColumnDesc is called when production generatedColumnDesc is exited.
func (s *BaseStarRocksListener) ExitGeneratedColumnDesc(ctx *GeneratedColumnDescContext) {}

// EnterIndexDesc is called when production indexDesc is entered.
func (s *BaseStarRocksListener) EnterIndexDesc(ctx *IndexDescContext) {}

// ExitIndexDesc is called when production indexDesc is exited.
func (s *BaseStarRocksListener) ExitIndexDesc(ctx *IndexDescContext) {}

// EnterEngineDesc is called when production engineDesc is entered.
func (s *BaseStarRocksListener) EnterEngineDesc(ctx *EngineDescContext) {}

// ExitEngineDesc is called when production engineDesc is exited.
func (s *BaseStarRocksListener) ExitEngineDesc(ctx *EngineDescContext) {}

// EnterCharsetDesc is called when production charsetDesc is entered.
func (s *BaseStarRocksListener) EnterCharsetDesc(ctx *CharsetDescContext) {}

// ExitCharsetDesc is called when production charsetDesc is exited.
func (s *BaseStarRocksListener) ExitCharsetDesc(ctx *CharsetDescContext) {}

// EnterCollateDesc is called when production collateDesc is entered.
func (s *BaseStarRocksListener) EnterCollateDesc(ctx *CollateDescContext) {}

// ExitCollateDesc is called when production collateDesc is exited.
func (s *BaseStarRocksListener) ExitCollateDesc(ctx *CollateDescContext) {}

// EnterKeyDesc is called when production keyDesc is entered.
func (s *BaseStarRocksListener) EnterKeyDesc(ctx *KeyDescContext) {}

// ExitKeyDesc is called when production keyDesc is exited.
func (s *BaseStarRocksListener) ExitKeyDesc(ctx *KeyDescContext) {}

// EnterOrderByDesc is called when production orderByDesc is entered.
func (s *BaseStarRocksListener) EnterOrderByDesc(ctx *OrderByDescContext) {}

// ExitOrderByDesc is called when production orderByDesc is exited.
func (s *BaseStarRocksListener) ExitOrderByDesc(ctx *OrderByDescContext) {}

// EnterColumnNullable is called when production columnNullable is entered.
func (s *BaseStarRocksListener) EnterColumnNullable(ctx *ColumnNullableContext) {}

// ExitColumnNullable is called when production columnNullable is exited.
func (s *BaseStarRocksListener) ExitColumnNullable(ctx *ColumnNullableContext) {}

// EnterTypeWithNullable is called when production typeWithNullable is entered.
func (s *BaseStarRocksListener) EnterTypeWithNullable(ctx *TypeWithNullableContext) {}

// ExitTypeWithNullable is called when production typeWithNullable is exited.
func (s *BaseStarRocksListener) ExitTypeWithNullable(ctx *TypeWithNullableContext) {}

// EnterAggStateDesc is called when production aggStateDesc is entered.
func (s *BaseStarRocksListener) EnterAggStateDesc(ctx *AggStateDescContext) {}

// ExitAggStateDesc is called when production aggStateDesc is exited.
func (s *BaseStarRocksListener) ExitAggStateDesc(ctx *AggStateDescContext) {}

// EnterAggDesc is called when production aggDesc is entered.
func (s *BaseStarRocksListener) EnterAggDesc(ctx *AggDescContext) {}

// ExitAggDesc is called when production aggDesc is exited.
func (s *BaseStarRocksListener) ExitAggDesc(ctx *AggDescContext) {}

// EnterRollupDesc is called when production rollupDesc is entered.
func (s *BaseStarRocksListener) EnterRollupDesc(ctx *RollupDescContext) {}

// ExitRollupDesc is called when production rollupDesc is exited.
func (s *BaseStarRocksListener) ExitRollupDesc(ctx *RollupDescContext) {}

// EnterRollupItem is called when production rollupItem is entered.
func (s *BaseStarRocksListener) EnterRollupItem(ctx *RollupItemContext) {}

// ExitRollupItem is called when production rollupItem is exited.
func (s *BaseStarRocksListener) ExitRollupItem(ctx *RollupItemContext) {}

// EnterDupKeys is called when production dupKeys is entered.
func (s *BaseStarRocksListener) EnterDupKeys(ctx *DupKeysContext) {}

// ExitDupKeys is called when production dupKeys is exited.
func (s *BaseStarRocksListener) ExitDupKeys(ctx *DupKeysContext) {}

// EnterFromRollup is called when production fromRollup is entered.
func (s *BaseStarRocksListener) EnterFromRollup(ctx *FromRollupContext) {}

// ExitFromRollup is called when production fromRollup is exited.
func (s *BaseStarRocksListener) ExitFromRollup(ctx *FromRollupContext) {}

// EnterOrReplace is called when production orReplace is entered.
func (s *BaseStarRocksListener) EnterOrReplace(ctx *OrReplaceContext) {}

// ExitOrReplace is called when production orReplace is exited.
func (s *BaseStarRocksListener) ExitOrReplace(ctx *OrReplaceContext) {}

// EnterIfNotExists is called when production ifNotExists is entered.
func (s *BaseStarRocksListener) EnterIfNotExists(ctx *IfNotExistsContext) {}

// ExitIfNotExists is called when production ifNotExists is exited.
func (s *BaseStarRocksListener) ExitIfNotExists(ctx *IfNotExistsContext) {}

// EnterCreateTableAsSelectStatement is called when production createTableAsSelectStatement is entered.
func (s *BaseStarRocksListener) EnterCreateTableAsSelectStatement(ctx *CreateTableAsSelectStatementContext) {
}

// ExitCreateTableAsSelectStatement is called when production createTableAsSelectStatement is exited.
func (s *BaseStarRocksListener) ExitCreateTableAsSelectStatement(ctx *CreateTableAsSelectStatementContext) {
}

// EnterDropTableStatement is called when production dropTableStatement is entered.
func (s *BaseStarRocksListener) EnterDropTableStatement(ctx *DropTableStatementContext) {}

// ExitDropTableStatement is called when production dropTableStatement is exited.
func (s *BaseStarRocksListener) ExitDropTableStatement(ctx *DropTableStatementContext) {}

// EnterCleanTemporaryTableStatement is called when production cleanTemporaryTableStatement is entered.
func (s *BaseStarRocksListener) EnterCleanTemporaryTableStatement(ctx *CleanTemporaryTableStatementContext) {
}

// ExitCleanTemporaryTableStatement is called when production cleanTemporaryTableStatement is exited.
func (s *BaseStarRocksListener) ExitCleanTemporaryTableStatement(ctx *CleanTemporaryTableStatementContext) {
}

// EnterAlterTableStatement is called when production alterTableStatement is entered.
func (s *BaseStarRocksListener) EnterAlterTableStatement(ctx *AlterTableStatementContext) {}

// ExitAlterTableStatement is called when production alterTableStatement is exited.
func (s *BaseStarRocksListener) ExitAlterTableStatement(ctx *AlterTableStatementContext) {}

// EnterCreateIndexStatement is called when production createIndexStatement is entered.
func (s *BaseStarRocksListener) EnterCreateIndexStatement(ctx *CreateIndexStatementContext) {}

// ExitCreateIndexStatement is called when production createIndexStatement is exited.
func (s *BaseStarRocksListener) ExitCreateIndexStatement(ctx *CreateIndexStatementContext) {}

// EnterDropIndexStatement is called when production dropIndexStatement is entered.
func (s *BaseStarRocksListener) EnterDropIndexStatement(ctx *DropIndexStatementContext) {}

// ExitDropIndexStatement is called when production dropIndexStatement is exited.
func (s *BaseStarRocksListener) ExitDropIndexStatement(ctx *DropIndexStatementContext) {}

// EnterIndexType is called when production indexType is entered.
func (s *BaseStarRocksListener) EnterIndexType(ctx *IndexTypeContext) {}

// ExitIndexType is called when production indexType is exited.
func (s *BaseStarRocksListener) ExitIndexType(ctx *IndexTypeContext) {}

// EnterShowTableStatement is called when production showTableStatement is entered.
func (s *BaseStarRocksListener) EnterShowTableStatement(ctx *ShowTableStatementContext) {}

// ExitShowTableStatement is called when production showTableStatement is exited.
func (s *BaseStarRocksListener) ExitShowTableStatement(ctx *ShowTableStatementContext) {}

// EnterShowTemporaryTablesStatement is called when production showTemporaryTablesStatement is entered.
func (s *BaseStarRocksListener) EnterShowTemporaryTablesStatement(ctx *ShowTemporaryTablesStatementContext) {
}

// ExitShowTemporaryTablesStatement is called when production showTemporaryTablesStatement is exited.
func (s *BaseStarRocksListener) ExitShowTemporaryTablesStatement(ctx *ShowTemporaryTablesStatementContext) {
}

// EnterShowCreateTableStatement is called when production showCreateTableStatement is entered.
func (s *BaseStarRocksListener) EnterShowCreateTableStatement(ctx *ShowCreateTableStatementContext) {}

// ExitShowCreateTableStatement is called when production showCreateTableStatement is exited.
func (s *BaseStarRocksListener) ExitShowCreateTableStatement(ctx *ShowCreateTableStatementContext) {}

// EnterShowColumnStatement is called when production showColumnStatement is entered.
func (s *BaseStarRocksListener) EnterShowColumnStatement(ctx *ShowColumnStatementContext) {}

// ExitShowColumnStatement is called when production showColumnStatement is exited.
func (s *BaseStarRocksListener) ExitShowColumnStatement(ctx *ShowColumnStatementContext) {}

// EnterShowTableStatusStatement is called when production showTableStatusStatement is entered.
func (s *BaseStarRocksListener) EnterShowTableStatusStatement(ctx *ShowTableStatusStatementContext) {}

// ExitShowTableStatusStatement is called when production showTableStatusStatement is exited.
func (s *BaseStarRocksListener) ExitShowTableStatusStatement(ctx *ShowTableStatusStatementContext) {}

// EnterRefreshTableStatement is called when production refreshTableStatement is entered.
func (s *BaseStarRocksListener) EnterRefreshTableStatement(ctx *RefreshTableStatementContext) {}

// ExitRefreshTableStatement is called when production refreshTableStatement is exited.
func (s *BaseStarRocksListener) ExitRefreshTableStatement(ctx *RefreshTableStatementContext) {}

// EnterShowAlterStatement is called when production showAlterStatement is entered.
func (s *BaseStarRocksListener) EnterShowAlterStatement(ctx *ShowAlterStatementContext) {}

// ExitShowAlterStatement is called when production showAlterStatement is exited.
func (s *BaseStarRocksListener) ExitShowAlterStatement(ctx *ShowAlterStatementContext) {}

// EnterDescTableStatement is called when production descTableStatement is entered.
func (s *BaseStarRocksListener) EnterDescTableStatement(ctx *DescTableStatementContext) {}

// ExitDescTableStatement is called when production descTableStatement is exited.
func (s *BaseStarRocksListener) ExitDescTableStatement(ctx *DescTableStatementContext) {}

// EnterCreateTableLikeStatement is called when production createTableLikeStatement is entered.
func (s *BaseStarRocksListener) EnterCreateTableLikeStatement(ctx *CreateTableLikeStatementContext) {}

// ExitCreateTableLikeStatement is called when production createTableLikeStatement is exited.
func (s *BaseStarRocksListener) ExitCreateTableLikeStatement(ctx *CreateTableLikeStatementContext) {}

// EnterShowIndexStatement is called when production showIndexStatement is entered.
func (s *BaseStarRocksListener) EnterShowIndexStatement(ctx *ShowIndexStatementContext) {}

// ExitShowIndexStatement is called when production showIndexStatement is exited.
func (s *BaseStarRocksListener) ExitShowIndexStatement(ctx *ShowIndexStatementContext) {}

// EnterRecoverTableStatement is called when production recoverTableStatement is entered.
func (s *BaseStarRocksListener) EnterRecoverTableStatement(ctx *RecoverTableStatementContext) {}

// ExitRecoverTableStatement is called when production recoverTableStatement is exited.
func (s *BaseStarRocksListener) ExitRecoverTableStatement(ctx *RecoverTableStatementContext) {}

// EnterTruncateTableStatement is called when production truncateTableStatement is entered.
func (s *BaseStarRocksListener) EnterTruncateTableStatement(ctx *TruncateTableStatementContext) {}

// ExitTruncateTableStatement is called when production truncateTableStatement is exited.
func (s *BaseStarRocksListener) ExitTruncateTableStatement(ctx *TruncateTableStatementContext) {}

// EnterCancelAlterTableStatement is called when production cancelAlterTableStatement is entered.
func (s *BaseStarRocksListener) EnterCancelAlterTableStatement(ctx *CancelAlterTableStatementContext) {
}

// ExitCancelAlterTableStatement is called when production cancelAlterTableStatement is exited.
func (s *BaseStarRocksListener) ExitCancelAlterTableStatement(ctx *CancelAlterTableStatementContext) {
}

// EnterShowPartitionsStatement is called when production showPartitionsStatement is entered.
func (s *BaseStarRocksListener) EnterShowPartitionsStatement(ctx *ShowPartitionsStatementContext) {}

// ExitShowPartitionsStatement is called when production showPartitionsStatement is exited.
func (s *BaseStarRocksListener) ExitShowPartitionsStatement(ctx *ShowPartitionsStatementContext) {}

// EnterRecoverPartitionStatement is called when production recoverPartitionStatement is entered.
func (s *BaseStarRocksListener) EnterRecoverPartitionStatement(ctx *RecoverPartitionStatementContext) {
}

// ExitRecoverPartitionStatement is called when production recoverPartitionStatement is exited.
func (s *BaseStarRocksListener) ExitRecoverPartitionStatement(ctx *RecoverPartitionStatementContext) {
}

// EnterCreateViewStatement is called when production createViewStatement is entered.
func (s *BaseStarRocksListener) EnterCreateViewStatement(ctx *CreateViewStatementContext) {}

// ExitCreateViewStatement is called when production createViewStatement is exited.
func (s *BaseStarRocksListener) ExitCreateViewStatement(ctx *CreateViewStatementContext) {}

// EnterAlterViewStatement is called when production alterViewStatement is entered.
func (s *BaseStarRocksListener) EnterAlterViewStatement(ctx *AlterViewStatementContext) {}

// ExitAlterViewStatement is called when production alterViewStatement is exited.
func (s *BaseStarRocksListener) ExitAlterViewStatement(ctx *AlterViewStatementContext) {}

// EnterDropViewStatement is called when production dropViewStatement is entered.
func (s *BaseStarRocksListener) EnterDropViewStatement(ctx *DropViewStatementContext) {}

// ExitDropViewStatement is called when production dropViewStatement is exited.
func (s *BaseStarRocksListener) ExitDropViewStatement(ctx *DropViewStatementContext) {}

// EnterColumnNameWithComment is called when production columnNameWithComment is entered.
func (s *BaseStarRocksListener) EnterColumnNameWithComment(ctx *ColumnNameWithCommentContext) {}

// ExitColumnNameWithComment is called when production columnNameWithComment is exited.
func (s *BaseStarRocksListener) ExitColumnNameWithComment(ctx *ColumnNameWithCommentContext) {}

// EnterSubmitTaskStatement is called when production submitTaskStatement is entered.
func (s *BaseStarRocksListener) EnterSubmitTaskStatement(ctx *SubmitTaskStatementContext) {}

// ExitSubmitTaskStatement is called when production submitTaskStatement is exited.
func (s *BaseStarRocksListener) ExitSubmitTaskStatement(ctx *SubmitTaskStatementContext) {}

// EnterTaskClause is called when production taskClause is entered.
func (s *BaseStarRocksListener) EnterTaskClause(ctx *TaskClauseContext) {}

// ExitTaskClause is called when production taskClause is exited.
func (s *BaseStarRocksListener) ExitTaskClause(ctx *TaskClauseContext) {}

// EnterDropTaskStatement is called when production dropTaskStatement is entered.
func (s *BaseStarRocksListener) EnterDropTaskStatement(ctx *DropTaskStatementContext) {}

// ExitDropTaskStatement is called when production dropTaskStatement is exited.
func (s *BaseStarRocksListener) ExitDropTaskStatement(ctx *DropTaskStatementContext) {}

// EnterTaskScheduleDesc is called when production taskScheduleDesc is entered.
func (s *BaseStarRocksListener) EnterTaskScheduleDesc(ctx *TaskScheduleDescContext) {}

// ExitTaskScheduleDesc is called when production taskScheduleDesc is exited.
func (s *BaseStarRocksListener) ExitTaskScheduleDesc(ctx *TaskScheduleDescContext) {}

// EnterCreateMaterializedViewStatement is called when production createMaterializedViewStatement is entered.
func (s *BaseStarRocksListener) EnterCreateMaterializedViewStatement(ctx *CreateMaterializedViewStatementContext) {
}

// ExitCreateMaterializedViewStatement is called when production createMaterializedViewStatement is exited.
func (s *BaseStarRocksListener) ExitCreateMaterializedViewStatement(ctx *CreateMaterializedViewStatementContext) {
}

// EnterMvPartitionExprs is called when production mvPartitionExprs is entered.
func (s *BaseStarRocksListener) EnterMvPartitionExprs(ctx *MvPartitionExprsContext) {}

// ExitMvPartitionExprs is called when production mvPartitionExprs is exited.
func (s *BaseStarRocksListener) ExitMvPartitionExprs(ctx *MvPartitionExprsContext) {}

// EnterMaterializedViewDesc is called when production materializedViewDesc is entered.
func (s *BaseStarRocksListener) EnterMaterializedViewDesc(ctx *MaterializedViewDescContext) {}

// ExitMaterializedViewDesc is called when production materializedViewDesc is exited.
func (s *BaseStarRocksListener) ExitMaterializedViewDesc(ctx *MaterializedViewDescContext) {}

// EnterShowMaterializedViewsStatement is called when production showMaterializedViewsStatement is entered.
func (s *BaseStarRocksListener) EnterShowMaterializedViewsStatement(ctx *ShowMaterializedViewsStatementContext) {
}

// ExitShowMaterializedViewsStatement is called when production showMaterializedViewsStatement is exited.
func (s *BaseStarRocksListener) ExitShowMaterializedViewsStatement(ctx *ShowMaterializedViewsStatementContext) {
}

// EnterDropMaterializedViewStatement is called when production dropMaterializedViewStatement is entered.
func (s *BaseStarRocksListener) EnterDropMaterializedViewStatement(ctx *DropMaterializedViewStatementContext) {
}

// ExitDropMaterializedViewStatement is called when production dropMaterializedViewStatement is exited.
func (s *BaseStarRocksListener) ExitDropMaterializedViewStatement(ctx *DropMaterializedViewStatementContext) {
}

// EnterAlterMaterializedViewStatement is called when production alterMaterializedViewStatement is entered.
func (s *BaseStarRocksListener) EnterAlterMaterializedViewStatement(ctx *AlterMaterializedViewStatementContext) {
}

// ExitAlterMaterializedViewStatement is called when production alterMaterializedViewStatement is exited.
func (s *BaseStarRocksListener) ExitAlterMaterializedViewStatement(ctx *AlterMaterializedViewStatementContext) {
}

// EnterRefreshMaterializedViewStatement is called when production refreshMaterializedViewStatement is entered.
func (s *BaseStarRocksListener) EnterRefreshMaterializedViewStatement(ctx *RefreshMaterializedViewStatementContext) {
}

// ExitRefreshMaterializedViewStatement is called when production refreshMaterializedViewStatement is exited.
func (s *BaseStarRocksListener) ExitRefreshMaterializedViewStatement(ctx *RefreshMaterializedViewStatementContext) {
}

// EnterCancelRefreshMaterializedViewStatement is called when production cancelRefreshMaterializedViewStatement is entered.
func (s *BaseStarRocksListener) EnterCancelRefreshMaterializedViewStatement(ctx *CancelRefreshMaterializedViewStatementContext) {
}

// ExitCancelRefreshMaterializedViewStatement is called when production cancelRefreshMaterializedViewStatement is exited.
func (s *BaseStarRocksListener) ExitCancelRefreshMaterializedViewStatement(ctx *CancelRefreshMaterializedViewStatementContext) {
}

// EnterAdminSetConfigStatement is called when production adminSetConfigStatement is entered.
func (s *BaseStarRocksListener) EnterAdminSetConfigStatement(ctx *AdminSetConfigStatementContext) {}

// ExitAdminSetConfigStatement is called when production adminSetConfigStatement is exited.
func (s *BaseStarRocksListener) ExitAdminSetConfigStatement(ctx *AdminSetConfigStatementContext) {}

// EnterAdminSetReplicaStatusStatement is called when production adminSetReplicaStatusStatement is entered.
func (s *BaseStarRocksListener) EnterAdminSetReplicaStatusStatement(ctx *AdminSetReplicaStatusStatementContext) {
}

// ExitAdminSetReplicaStatusStatement is called when production adminSetReplicaStatusStatement is exited.
func (s *BaseStarRocksListener) ExitAdminSetReplicaStatusStatement(ctx *AdminSetReplicaStatusStatementContext) {
}

// EnterAdminShowConfigStatement is called when production adminShowConfigStatement is entered.
func (s *BaseStarRocksListener) EnterAdminShowConfigStatement(ctx *AdminShowConfigStatementContext) {}

// ExitAdminShowConfigStatement is called when production adminShowConfigStatement is exited.
func (s *BaseStarRocksListener) ExitAdminShowConfigStatement(ctx *AdminShowConfigStatementContext) {}

// EnterAdminShowReplicaDistributionStatement is called when production adminShowReplicaDistributionStatement is entered.
func (s *BaseStarRocksListener) EnterAdminShowReplicaDistributionStatement(ctx *AdminShowReplicaDistributionStatementContext) {
}

// ExitAdminShowReplicaDistributionStatement is called when production adminShowReplicaDistributionStatement is exited.
func (s *BaseStarRocksListener) ExitAdminShowReplicaDistributionStatement(ctx *AdminShowReplicaDistributionStatementContext) {
}

// EnterAdminShowReplicaStatusStatement is called when production adminShowReplicaStatusStatement is entered.
func (s *BaseStarRocksListener) EnterAdminShowReplicaStatusStatement(ctx *AdminShowReplicaStatusStatementContext) {
}

// ExitAdminShowReplicaStatusStatement is called when production adminShowReplicaStatusStatement is exited.
func (s *BaseStarRocksListener) ExitAdminShowReplicaStatusStatement(ctx *AdminShowReplicaStatusStatementContext) {
}

// EnterAdminRepairTableStatement is called when production adminRepairTableStatement is entered.
func (s *BaseStarRocksListener) EnterAdminRepairTableStatement(ctx *AdminRepairTableStatementContext) {
}

// ExitAdminRepairTableStatement is called when production adminRepairTableStatement is exited.
func (s *BaseStarRocksListener) ExitAdminRepairTableStatement(ctx *AdminRepairTableStatementContext) {
}

// EnterAdminCancelRepairTableStatement is called when production adminCancelRepairTableStatement is entered.
func (s *BaseStarRocksListener) EnterAdminCancelRepairTableStatement(ctx *AdminCancelRepairTableStatementContext) {
}

// ExitAdminCancelRepairTableStatement is called when production adminCancelRepairTableStatement is exited.
func (s *BaseStarRocksListener) ExitAdminCancelRepairTableStatement(ctx *AdminCancelRepairTableStatementContext) {
}

// EnterAdminCheckTabletsStatement is called when production adminCheckTabletsStatement is entered.
func (s *BaseStarRocksListener) EnterAdminCheckTabletsStatement(ctx *AdminCheckTabletsStatementContext) {
}

// ExitAdminCheckTabletsStatement is called when production adminCheckTabletsStatement is exited.
func (s *BaseStarRocksListener) ExitAdminCheckTabletsStatement(ctx *AdminCheckTabletsStatementContext) {
}

// EnterAdminSetPartitionVersion is called when production adminSetPartitionVersion is entered.
func (s *BaseStarRocksListener) EnterAdminSetPartitionVersion(ctx *AdminSetPartitionVersionContext) {}

// ExitAdminSetPartitionVersion is called when production adminSetPartitionVersion is exited.
func (s *BaseStarRocksListener) ExitAdminSetPartitionVersion(ctx *AdminSetPartitionVersionContext) {}

// EnterKillStatement is called when production killStatement is entered.
func (s *BaseStarRocksListener) EnterKillStatement(ctx *KillStatementContext) {}

// ExitKillStatement is called when production killStatement is exited.
func (s *BaseStarRocksListener) ExitKillStatement(ctx *KillStatementContext) {}

// EnterSyncStatement is called when production syncStatement is entered.
func (s *BaseStarRocksListener) EnterSyncStatement(ctx *SyncStatementContext) {}

// ExitSyncStatement is called when production syncStatement is exited.
func (s *BaseStarRocksListener) ExitSyncStatement(ctx *SyncStatementContext) {}

// EnterAdminSetAutomatedSnapshotOnStatement is called when production adminSetAutomatedSnapshotOnStatement is entered.
func (s *BaseStarRocksListener) EnterAdminSetAutomatedSnapshotOnStatement(ctx *AdminSetAutomatedSnapshotOnStatementContext) {
}

// ExitAdminSetAutomatedSnapshotOnStatement is called when production adminSetAutomatedSnapshotOnStatement is exited.
func (s *BaseStarRocksListener) ExitAdminSetAutomatedSnapshotOnStatement(ctx *AdminSetAutomatedSnapshotOnStatementContext) {
}

// EnterAdminSetAutomatedSnapshotOffStatement is called when production adminSetAutomatedSnapshotOffStatement is entered.
func (s *BaseStarRocksListener) EnterAdminSetAutomatedSnapshotOffStatement(ctx *AdminSetAutomatedSnapshotOffStatementContext) {
}

// ExitAdminSetAutomatedSnapshotOffStatement is called when production adminSetAutomatedSnapshotOffStatement is exited.
func (s *BaseStarRocksListener) ExitAdminSetAutomatedSnapshotOffStatement(ctx *AdminSetAutomatedSnapshotOffStatementContext) {
}

// EnterAlterSystemStatement is called when production alterSystemStatement is entered.
func (s *BaseStarRocksListener) EnterAlterSystemStatement(ctx *AlterSystemStatementContext) {}

// ExitAlterSystemStatement is called when production alterSystemStatement is exited.
func (s *BaseStarRocksListener) ExitAlterSystemStatement(ctx *AlterSystemStatementContext) {}

// EnterCancelAlterSystemStatement is called when production cancelAlterSystemStatement is entered.
func (s *BaseStarRocksListener) EnterCancelAlterSystemStatement(ctx *CancelAlterSystemStatementContext) {
}

// ExitCancelAlterSystemStatement is called when production cancelAlterSystemStatement is exited.
func (s *BaseStarRocksListener) ExitCancelAlterSystemStatement(ctx *CancelAlterSystemStatementContext) {
}

// EnterShowComputeNodesStatement is called when production showComputeNodesStatement is entered.
func (s *BaseStarRocksListener) EnterShowComputeNodesStatement(ctx *ShowComputeNodesStatementContext) {
}

// ExitShowComputeNodesStatement is called when production showComputeNodesStatement is exited.
func (s *BaseStarRocksListener) ExitShowComputeNodesStatement(ctx *ShowComputeNodesStatementContext) {
}

// EnterCreateExternalCatalogStatement is called when production createExternalCatalogStatement is entered.
func (s *BaseStarRocksListener) EnterCreateExternalCatalogStatement(ctx *CreateExternalCatalogStatementContext) {
}

// ExitCreateExternalCatalogStatement is called when production createExternalCatalogStatement is exited.
func (s *BaseStarRocksListener) ExitCreateExternalCatalogStatement(ctx *CreateExternalCatalogStatementContext) {
}

// EnterShowCreateExternalCatalogStatement is called when production showCreateExternalCatalogStatement is entered.
func (s *BaseStarRocksListener) EnterShowCreateExternalCatalogStatement(ctx *ShowCreateExternalCatalogStatementContext) {
}

// ExitShowCreateExternalCatalogStatement is called when production showCreateExternalCatalogStatement is exited.
func (s *BaseStarRocksListener) ExitShowCreateExternalCatalogStatement(ctx *ShowCreateExternalCatalogStatementContext) {
}

// EnterDropExternalCatalogStatement is called when production dropExternalCatalogStatement is entered.
func (s *BaseStarRocksListener) EnterDropExternalCatalogStatement(ctx *DropExternalCatalogStatementContext) {
}

// ExitDropExternalCatalogStatement is called when production dropExternalCatalogStatement is exited.
func (s *BaseStarRocksListener) ExitDropExternalCatalogStatement(ctx *DropExternalCatalogStatementContext) {
}

// EnterShowCatalogsStatement is called when production showCatalogsStatement is entered.
func (s *BaseStarRocksListener) EnterShowCatalogsStatement(ctx *ShowCatalogsStatementContext) {}

// ExitShowCatalogsStatement is called when production showCatalogsStatement is exited.
func (s *BaseStarRocksListener) ExitShowCatalogsStatement(ctx *ShowCatalogsStatementContext) {}

// EnterAlterCatalogStatement is called when production alterCatalogStatement is entered.
func (s *BaseStarRocksListener) EnterAlterCatalogStatement(ctx *AlterCatalogStatementContext) {}

// ExitAlterCatalogStatement is called when production alterCatalogStatement is exited.
func (s *BaseStarRocksListener) ExitAlterCatalogStatement(ctx *AlterCatalogStatementContext) {}

// EnterCreateStorageVolumeStatement is called when production createStorageVolumeStatement is entered.
func (s *BaseStarRocksListener) EnterCreateStorageVolumeStatement(ctx *CreateStorageVolumeStatementContext) {
}

// ExitCreateStorageVolumeStatement is called when production createStorageVolumeStatement is exited.
func (s *BaseStarRocksListener) ExitCreateStorageVolumeStatement(ctx *CreateStorageVolumeStatementContext) {
}

// EnterTypeDesc is called when production typeDesc is entered.
func (s *BaseStarRocksListener) EnterTypeDesc(ctx *TypeDescContext) {}

// ExitTypeDesc is called when production typeDesc is exited.
func (s *BaseStarRocksListener) ExitTypeDesc(ctx *TypeDescContext) {}

// EnterLocationsDesc is called when production locationsDesc is entered.
func (s *BaseStarRocksListener) EnterLocationsDesc(ctx *LocationsDescContext) {}

// ExitLocationsDesc is called when production locationsDesc is exited.
func (s *BaseStarRocksListener) ExitLocationsDesc(ctx *LocationsDescContext) {}

// EnterShowStorageVolumesStatement is called when production showStorageVolumesStatement is entered.
func (s *BaseStarRocksListener) EnterShowStorageVolumesStatement(ctx *ShowStorageVolumesStatementContext) {
}

// ExitShowStorageVolumesStatement is called when production showStorageVolumesStatement is exited.
func (s *BaseStarRocksListener) ExitShowStorageVolumesStatement(ctx *ShowStorageVolumesStatementContext) {
}

// EnterDropStorageVolumeStatement is called when production dropStorageVolumeStatement is entered.
func (s *BaseStarRocksListener) EnterDropStorageVolumeStatement(ctx *DropStorageVolumeStatementContext) {
}

// ExitDropStorageVolumeStatement is called when production dropStorageVolumeStatement is exited.
func (s *BaseStarRocksListener) ExitDropStorageVolumeStatement(ctx *DropStorageVolumeStatementContext) {
}

// EnterAlterStorageVolumeStatement is called when production alterStorageVolumeStatement is entered.
func (s *BaseStarRocksListener) EnterAlterStorageVolumeStatement(ctx *AlterStorageVolumeStatementContext) {
}

// ExitAlterStorageVolumeStatement is called when production alterStorageVolumeStatement is exited.
func (s *BaseStarRocksListener) ExitAlterStorageVolumeStatement(ctx *AlterStorageVolumeStatementContext) {
}

// EnterAlterStorageVolumeClause is called when production alterStorageVolumeClause is entered.
func (s *BaseStarRocksListener) EnterAlterStorageVolumeClause(ctx *AlterStorageVolumeClauseContext) {}

// ExitAlterStorageVolumeClause is called when production alterStorageVolumeClause is exited.
func (s *BaseStarRocksListener) ExitAlterStorageVolumeClause(ctx *AlterStorageVolumeClauseContext) {}

// EnterModifyStorageVolumePropertiesClause is called when production modifyStorageVolumePropertiesClause is entered.
func (s *BaseStarRocksListener) EnterModifyStorageVolumePropertiesClause(ctx *ModifyStorageVolumePropertiesClauseContext) {
}

// ExitModifyStorageVolumePropertiesClause is called when production modifyStorageVolumePropertiesClause is exited.
func (s *BaseStarRocksListener) ExitModifyStorageVolumePropertiesClause(ctx *ModifyStorageVolumePropertiesClauseContext) {
}

// EnterModifyStorageVolumeCommentClause is called when production modifyStorageVolumeCommentClause is entered.
func (s *BaseStarRocksListener) EnterModifyStorageVolumeCommentClause(ctx *ModifyStorageVolumeCommentClauseContext) {
}

// ExitModifyStorageVolumeCommentClause is called when production modifyStorageVolumeCommentClause is exited.
func (s *BaseStarRocksListener) ExitModifyStorageVolumeCommentClause(ctx *ModifyStorageVolumeCommentClauseContext) {
}

// EnterDescStorageVolumeStatement is called when production descStorageVolumeStatement is entered.
func (s *BaseStarRocksListener) EnterDescStorageVolumeStatement(ctx *DescStorageVolumeStatementContext) {
}

// ExitDescStorageVolumeStatement is called when production descStorageVolumeStatement is exited.
func (s *BaseStarRocksListener) ExitDescStorageVolumeStatement(ctx *DescStorageVolumeStatementContext) {
}

// EnterSetDefaultStorageVolumeStatement is called when production setDefaultStorageVolumeStatement is entered.
func (s *BaseStarRocksListener) EnterSetDefaultStorageVolumeStatement(ctx *SetDefaultStorageVolumeStatementContext) {
}

// ExitSetDefaultStorageVolumeStatement is called when production setDefaultStorageVolumeStatement is exited.
func (s *BaseStarRocksListener) ExitSetDefaultStorageVolumeStatement(ctx *SetDefaultStorageVolumeStatementContext) {
}

// EnterUpdateFailPointStatusStatement is called when production updateFailPointStatusStatement is entered.
func (s *BaseStarRocksListener) EnterUpdateFailPointStatusStatement(ctx *UpdateFailPointStatusStatementContext) {
}

// ExitUpdateFailPointStatusStatement is called when production updateFailPointStatusStatement is exited.
func (s *BaseStarRocksListener) ExitUpdateFailPointStatusStatement(ctx *UpdateFailPointStatusStatementContext) {
}

// EnterShowFailPointStatement is called when production showFailPointStatement is entered.
func (s *BaseStarRocksListener) EnterShowFailPointStatement(ctx *ShowFailPointStatementContext) {}

// ExitShowFailPointStatement is called when production showFailPointStatement is exited.
func (s *BaseStarRocksListener) ExitShowFailPointStatement(ctx *ShowFailPointStatementContext) {}

// EnterCreateDictionaryStatement is called when production createDictionaryStatement is entered.
func (s *BaseStarRocksListener) EnterCreateDictionaryStatement(ctx *CreateDictionaryStatementContext) {
}

// ExitCreateDictionaryStatement is called when production createDictionaryStatement is exited.
func (s *BaseStarRocksListener) ExitCreateDictionaryStatement(ctx *CreateDictionaryStatementContext) {
}

// EnterDropDictionaryStatement is called when production dropDictionaryStatement is entered.
func (s *BaseStarRocksListener) EnterDropDictionaryStatement(ctx *DropDictionaryStatementContext) {}

// ExitDropDictionaryStatement is called when production dropDictionaryStatement is exited.
func (s *BaseStarRocksListener) ExitDropDictionaryStatement(ctx *DropDictionaryStatementContext) {}

// EnterRefreshDictionaryStatement is called when production refreshDictionaryStatement is entered.
func (s *BaseStarRocksListener) EnterRefreshDictionaryStatement(ctx *RefreshDictionaryStatementContext) {
}

// ExitRefreshDictionaryStatement is called when production refreshDictionaryStatement is exited.
func (s *BaseStarRocksListener) ExitRefreshDictionaryStatement(ctx *RefreshDictionaryStatementContext) {
}

// EnterShowDictionaryStatement is called when production showDictionaryStatement is entered.
func (s *BaseStarRocksListener) EnterShowDictionaryStatement(ctx *ShowDictionaryStatementContext) {}

// ExitShowDictionaryStatement is called when production showDictionaryStatement is exited.
func (s *BaseStarRocksListener) ExitShowDictionaryStatement(ctx *ShowDictionaryStatementContext) {}

// EnterCancelRefreshDictionaryStatement is called when production cancelRefreshDictionaryStatement is entered.
func (s *BaseStarRocksListener) EnterCancelRefreshDictionaryStatement(ctx *CancelRefreshDictionaryStatementContext) {
}

// ExitCancelRefreshDictionaryStatement is called when production cancelRefreshDictionaryStatement is exited.
func (s *BaseStarRocksListener) ExitCancelRefreshDictionaryStatement(ctx *CancelRefreshDictionaryStatementContext) {
}

// EnterDictionaryColumnDesc is called when production dictionaryColumnDesc is entered.
func (s *BaseStarRocksListener) EnterDictionaryColumnDesc(ctx *DictionaryColumnDescContext) {}

// ExitDictionaryColumnDesc is called when production dictionaryColumnDesc is exited.
func (s *BaseStarRocksListener) ExitDictionaryColumnDesc(ctx *DictionaryColumnDescContext) {}

// EnterDictionaryName is called when production dictionaryName is entered.
func (s *BaseStarRocksListener) EnterDictionaryName(ctx *DictionaryNameContext) {}

// ExitDictionaryName is called when production dictionaryName is exited.
func (s *BaseStarRocksListener) ExitDictionaryName(ctx *DictionaryNameContext) {}

// EnterAlterClause is called when production alterClause is entered.
func (s *BaseStarRocksListener) EnterAlterClause(ctx *AlterClauseContext) {}

// ExitAlterClause is called when production alterClause is exited.
func (s *BaseStarRocksListener) ExitAlterClause(ctx *AlterClauseContext) {}

// EnterAddFrontendClause is called when production addFrontendClause is entered.
func (s *BaseStarRocksListener) EnterAddFrontendClause(ctx *AddFrontendClauseContext) {}

// ExitAddFrontendClause is called when production addFrontendClause is exited.
func (s *BaseStarRocksListener) ExitAddFrontendClause(ctx *AddFrontendClauseContext) {}

// EnterDropFrontendClause is called when production dropFrontendClause is entered.
func (s *BaseStarRocksListener) EnterDropFrontendClause(ctx *DropFrontendClauseContext) {}

// ExitDropFrontendClause is called when production dropFrontendClause is exited.
func (s *BaseStarRocksListener) ExitDropFrontendClause(ctx *DropFrontendClauseContext) {}

// EnterModifyFrontendHostClause is called when production modifyFrontendHostClause is entered.
func (s *BaseStarRocksListener) EnterModifyFrontendHostClause(ctx *ModifyFrontendHostClauseContext) {}

// ExitModifyFrontendHostClause is called when production modifyFrontendHostClause is exited.
func (s *BaseStarRocksListener) ExitModifyFrontendHostClause(ctx *ModifyFrontendHostClauseContext) {}

// EnterAddBackendClause is called when production addBackendClause is entered.
func (s *BaseStarRocksListener) EnterAddBackendClause(ctx *AddBackendClauseContext) {}

// ExitAddBackendClause is called when production addBackendClause is exited.
func (s *BaseStarRocksListener) ExitAddBackendClause(ctx *AddBackendClauseContext) {}

// EnterDropBackendClause is called when production dropBackendClause is entered.
func (s *BaseStarRocksListener) EnterDropBackendClause(ctx *DropBackendClauseContext) {}

// ExitDropBackendClause is called when production dropBackendClause is exited.
func (s *BaseStarRocksListener) ExitDropBackendClause(ctx *DropBackendClauseContext) {}

// EnterDecommissionBackendClause is called when production decommissionBackendClause is entered.
func (s *BaseStarRocksListener) EnterDecommissionBackendClause(ctx *DecommissionBackendClauseContext) {
}

// ExitDecommissionBackendClause is called when production decommissionBackendClause is exited.
func (s *BaseStarRocksListener) ExitDecommissionBackendClause(ctx *DecommissionBackendClauseContext) {
}

// EnterModifyBackendClause is called when production modifyBackendClause is entered.
func (s *BaseStarRocksListener) EnterModifyBackendClause(ctx *ModifyBackendClauseContext) {}

// ExitModifyBackendClause is called when production modifyBackendClause is exited.
func (s *BaseStarRocksListener) ExitModifyBackendClause(ctx *ModifyBackendClauseContext) {}

// EnterAddComputeNodeClause is called when production addComputeNodeClause is entered.
func (s *BaseStarRocksListener) EnterAddComputeNodeClause(ctx *AddComputeNodeClauseContext) {}

// ExitAddComputeNodeClause is called when production addComputeNodeClause is exited.
func (s *BaseStarRocksListener) ExitAddComputeNodeClause(ctx *AddComputeNodeClauseContext) {}

// EnterDropComputeNodeClause is called when production dropComputeNodeClause is entered.
func (s *BaseStarRocksListener) EnterDropComputeNodeClause(ctx *DropComputeNodeClauseContext) {}

// ExitDropComputeNodeClause is called when production dropComputeNodeClause is exited.
func (s *BaseStarRocksListener) ExitDropComputeNodeClause(ctx *DropComputeNodeClauseContext) {}

// EnterModifyBrokerClause is called when production modifyBrokerClause is entered.
func (s *BaseStarRocksListener) EnterModifyBrokerClause(ctx *ModifyBrokerClauseContext) {}

// ExitModifyBrokerClause is called when production modifyBrokerClause is exited.
func (s *BaseStarRocksListener) ExitModifyBrokerClause(ctx *ModifyBrokerClauseContext) {}

// EnterAlterLoadErrorUrlClause is called when production alterLoadErrorUrlClause is entered.
func (s *BaseStarRocksListener) EnterAlterLoadErrorUrlClause(ctx *AlterLoadErrorUrlClauseContext) {}

// ExitAlterLoadErrorUrlClause is called when production alterLoadErrorUrlClause is exited.
func (s *BaseStarRocksListener) ExitAlterLoadErrorUrlClause(ctx *AlterLoadErrorUrlClauseContext) {}

// EnterCreateImageClause is called when production createImageClause is entered.
func (s *BaseStarRocksListener) EnterCreateImageClause(ctx *CreateImageClauseContext) {}

// ExitCreateImageClause is called when production createImageClause is exited.
func (s *BaseStarRocksListener) ExitCreateImageClause(ctx *CreateImageClauseContext) {}

// EnterCleanTabletSchedQClause is called when production cleanTabletSchedQClause is entered.
func (s *BaseStarRocksListener) EnterCleanTabletSchedQClause(ctx *CleanTabletSchedQClauseContext) {}

// ExitCleanTabletSchedQClause is called when production cleanTabletSchedQClause is exited.
func (s *BaseStarRocksListener) ExitCleanTabletSchedQClause(ctx *CleanTabletSchedQClauseContext) {}

// EnterDecommissionDiskClause is called when production decommissionDiskClause is entered.
func (s *BaseStarRocksListener) EnterDecommissionDiskClause(ctx *DecommissionDiskClauseContext) {}

// ExitDecommissionDiskClause is called when production decommissionDiskClause is exited.
func (s *BaseStarRocksListener) ExitDecommissionDiskClause(ctx *DecommissionDiskClauseContext) {}

// EnterCancelDecommissionDiskClause is called when production cancelDecommissionDiskClause is entered.
func (s *BaseStarRocksListener) EnterCancelDecommissionDiskClause(ctx *CancelDecommissionDiskClauseContext) {
}

// ExitCancelDecommissionDiskClause is called when production cancelDecommissionDiskClause is exited.
func (s *BaseStarRocksListener) ExitCancelDecommissionDiskClause(ctx *CancelDecommissionDiskClauseContext) {
}

// EnterDisableDiskClause is called when production disableDiskClause is entered.
func (s *BaseStarRocksListener) EnterDisableDiskClause(ctx *DisableDiskClauseContext) {}

// ExitDisableDiskClause is called when production disableDiskClause is exited.
func (s *BaseStarRocksListener) ExitDisableDiskClause(ctx *DisableDiskClauseContext) {}

// EnterCancelDisableDiskClause is called when production cancelDisableDiskClause is entered.
func (s *BaseStarRocksListener) EnterCancelDisableDiskClause(ctx *CancelDisableDiskClauseContext) {}

// ExitCancelDisableDiskClause is called when production cancelDisableDiskClause is exited.
func (s *BaseStarRocksListener) ExitCancelDisableDiskClause(ctx *CancelDisableDiskClauseContext) {}

// EnterCreateIndexClause is called when production createIndexClause is entered.
func (s *BaseStarRocksListener) EnterCreateIndexClause(ctx *CreateIndexClauseContext) {}

// ExitCreateIndexClause is called when production createIndexClause is exited.
func (s *BaseStarRocksListener) ExitCreateIndexClause(ctx *CreateIndexClauseContext) {}

// EnterDropIndexClause is called when production dropIndexClause is entered.
func (s *BaseStarRocksListener) EnterDropIndexClause(ctx *DropIndexClauseContext) {}

// ExitDropIndexClause is called when production dropIndexClause is exited.
func (s *BaseStarRocksListener) ExitDropIndexClause(ctx *DropIndexClauseContext) {}

// EnterTableRenameClause is called when production tableRenameClause is entered.
func (s *BaseStarRocksListener) EnterTableRenameClause(ctx *TableRenameClauseContext) {}

// ExitTableRenameClause is called when production tableRenameClause is exited.
func (s *BaseStarRocksListener) ExitTableRenameClause(ctx *TableRenameClauseContext) {}

// EnterSwapTableClause is called when production swapTableClause is entered.
func (s *BaseStarRocksListener) EnterSwapTableClause(ctx *SwapTableClauseContext) {}

// ExitSwapTableClause is called when production swapTableClause is exited.
func (s *BaseStarRocksListener) ExitSwapTableClause(ctx *SwapTableClauseContext) {}

// EnterModifyPropertiesClause is called when production modifyPropertiesClause is entered.
func (s *BaseStarRocksListener) EnterModifyPropertiesClause(ctx *ModifyPropertiesClauseContext) {}

// ExitModifyPropertiesClause is called when production modifyPropertiesClause is exited.
func (s *BaseStarRocksListener) ExitModifyPropertiesClause(ctx *ModifyPropertiesClauseContext) {}

// EnterModifyCommentClause is called when production modifyCommentClause is entered.
func (s *BaseStarRocksListener) EnterModifyCommentClause(ctx *ModifyCommentClauseContext) {}

// ExitModifyCommentClause is called when production modifyCommentClause is exited.
func (s *BaseStarRocksListener) ExitModifyCommentClause(ctx *ModifyCommentClauseContext) {}

// EnterOptimizeRange is called when production optimizeRange is entered.
func (s *BaseStarRocksListener) EnterOptimizeRange(ctx *OptimizeRangeContext) {}

// ExitOptimizeRange is called when production optimizeRange is exited.
func (s *BaseStarRocksListener) ExitOptimizeRange(ctx *OptimizeRangeContext) {}

// EnterOptimizeClause is called when production optimizeClause is entered.
func (s *BaseStarRocksListener) EnterOptimizeClause(ctx *OptimizeClauseContext) {}

// ExitOptimizeClause is called when production optimizeClause is exited.
func (s *BaseStarRocksListener) ExitOptimizeClause(ctx *OptimizeClauseContext) {}

// EnterAddColumnClause is called when production addColumnClause is entered.
func (s *BaseStarRocksListener) EnterAddColumnClause(ctx *AddColumnClauseContext) {}

// ExitAddColumnClause is called when production addColumnClause is exited.
func (s *BaseStarRocksListener) ExitAddColumnClause(ctx *AddColumnClauseContext) {}

// EnterAddColumnsClause is called when production addColumnsClause is entered.
func (s *BaseStarRocksListener) EnterAddColumnsClause(ctx *AddColumnsClauseContext) {}

// ExitAddColumnsClause is called when production addColumnsClause is exited.
func (s *BaseStarRocksListener) ExitAddColumnsClause(ctx *AddColumnsClauseContext) {}

// EnterDropColumnClause is called when production dropColumnClause is entered.
func (s *BaseStarRocksListener) EnterDropColumnClause(ctx *DropColumnClauseContext) {}

// ExitDropColumnClause is called when production dropColumnClause is exited.
func (s *BaseStarRocksListener) ExitDropColumnClause(ctx *DropColumnClauseContext) {}

// EnterModifyColumnClause is called when production modifyColumnClause is entered.
func (s *BaseStarRocksListener) EnterModifyColumnClause(ctx *ModifyColumnClauseContext) {}

// ExitModifyColumnClause is called when production modifyColumnClause is exited.
func (s *BaseStarRocksListener) ExitModifyColumnClause(ctx *ModifyColumnClauseContext) {}

// EnterModifyColumnCommentClause is called when production modifyColumnCommentClause is entered.
func (s *BaseStarRocksListener) EnterModifyColumnCommentClause(ctx *ModifyColumnCommentClauseContext) {
}

// ExitModifyColumnCommentClause is called when production modifyColumnCommentClause is exited.
func (s *BaseStarRocksListener) ExitModifyColumnCommentClause(ctx *ModifyColumnCommentClauseContext) {
}

// EnterColumnRenameClause is called when production columnRenameClause is entered.
func (s *BaseStarRocksListener) EnterColumnRenameClause(ctx *ColumnRenameClauseContext) {}

// ExitColumnRenameClause is called when production columnRenameClause is exited.
func (s *BaseStarRocksListener) ExitColumnRenameClause(ctx *ColumnRenameClauseContext) {}

// EnterReorderColumnsClause is called when production reorderColumnsClause is entered.
func (s *BaseStarRocksListener) EnterReorderColumnsClause(ctx *ReorderColumnsClauseContext) {}

// ExitReorderColumnsClause is called when production reorderColumnsClause is exited.
func (s *BaseStarRocksListener) ExitReorderColumnsClause(ctx *ReorderColumnsClauseContext) {}

// EnterRollupRenameClause is called when production rollupRenameClause is entered.
func (s *BaseStarRocksListener) EnterRollupRenameClause(ctx *RollupRenameClauseContext) {}

// ExitRollupRenameClause is called when production rollupRenameClause is exited.
func (s *BaseStarRocksListener) ExitRollupRenameClause(ctx *RollupRenameClauseContext) {}

// EnterCompactionClause is called when production compactionClause is entered.
func (s *BaseStarRocksListener) EnterCompactionClause(ctx *CompactionClauseContext) {}

// ExitCompactionClause is called when production compactionClause is exited.
func (s *BaseStarRocksListener) ExitCompactionClause(ctx *CompactionClauseContext) {}

// EnterSubfieldName is called when production subfieldName is entered.
func (s *BaseStarRocksListener) EnterSubfieldName(ctx *SubfieldNameContext) {}

// ExitSubfieldName is called when production subfieldName is exited.
func (s *BaseStarRocksListener) ExitSubfieldName(ctx *SubfieldNameContext) {}

// EnterNestedFieldName is called when production nestedFieldName is entered.
func (s *BaseStarRocksListener) EnterNestedFieldName(ctx *NestedFieldNameContext) {}

// ExitNestedFieldName is called when production nestedFieldName is exited.
func (s *BaseStarRocksListener) ExitNestedFieldName(ctx *NestedFieldNameContext) {}

// EnterAddFieldClause is called when production addFieldClause is entered.
func (s *BaseStarRocksListener) EnterAddFieldClause(ctx *AddFieldClauseContext) {}

// ExitAddFieldClause is called when production addFieldClause is exited.
func (s *BaseStarRocksListener) ExitAddFieldClause(ctx *AddFieldClauseContext) {}

// EnterDropFieldClause is called when production dropFieldClause is entered.
func (s *BaseStarRocksListener) EnterDropFieldClause(ctx *DropFieldClauseContext) {}

// ExitDropFieldClause is called when production dropFieldClause is exited.
func (s *BaseStarRocksListener) ExitDropFieldClause(ctx *DropFieldClauseContext) {}

// EnterCreateOrReplaceTagClause is called when production createOrReplaceTagClause is entered.
func (s *BaseStarRocksListener) EnterCreateOrReplaceTagClause(ctx *CreateOrReplaceTagClauseContext) {}

// ExitCreateOrReplaceTagClause is called when production createOrReplaceTagClause is exited.
func (s *BaseStarRocksListener) ExitCreateOrReplaceTagClause(ctx *CreateOrReplaceTagClauseContext) {}

// EnterCreateOrReplaceBranchClause is called when production createOrReplaceBranchClause is entered.
func (s *BaseStarRocksListener) EnterCreateOrReplaceBranchClause(ctx *CreateOrReplaceBranchClauseContext) {
}

// ExitCreateOrReplaceBranchClause is called when production createOrReplaceBranchClause is exited.
func (s *BaseStarRocksListener) ExitCreateOrReplaceBranchClause(ctx *CreateOrReplaceBranchClauseContext) {
}

// EnterDropBranchClause is called when production dropBranchClause is entered.
func (s *BaseStarRocksListener) EnterDropBranchClause(ctx *DropBranchClauseContext) {}

// ExitDropBranchClause is called when production dropBranchClause is exited.
func (s *BaseStarRocksListener) ExitDropBranchClause(ctx *DropBranchClauseContext) {}

// EnterDropTagClause is called when production dropTagClause is entered.
func (s *BaseStarRocksListener) EnterDropTagClause(ctx *DropTagClauseContext) {}

// ExitDropTagClause is called when production dropTagClause is exited.
func (s *BaseStarRocksListener) ExitDropTagClause(ctx *DropTagClauseContext) {}

// EnterTableOperationClause is called when production tableOperationClause is entered.
func (s *BaseStarRocksListener) EnterTableOperationClause(ctx *TableOperationClauseContext) {}

// ExitTableOperationClause is called when production tableOperationClause is exited.
func (s *BaseStarRocksListener) ExitTableOperationClause(ctx *TableOperationClauseContext) {}

// EnterTagOptions is called when production tagOptions is entered.
func (s *BaseStarRocksListener) EnterTagOptions(ctx *TagOptionsContext) {}

// ExitTagOptions is called when production tagOptions is exited.
func (s *BaseStarRocksListener) ExitTagOptions(ctx *TagOptionsContext) {}

// EnterBranchOptions is called when production branchOptions is entered.
func (s *BaseStarRocksListener) EnterBranchOptions(ctx *BranchOptionsContext) {}

// ExitBranchOptions is called when production branchOptions is exited.
func (s *BaseStarRocksListener) ExitBranchOptions(ctx *BranchOptionsContext) {}

// EnterSnapshotRetention is called when production snapshotRetention is entered.
func (s *BaseStarRocksListener) EnterSnapshotRetention(ctx *SnapshotRetentionContext) {}

// ExitSnapshotRetention is called when production snapshotRetention is exited.
func (s *BaseStarRocksListener) ExitSnapshotRetention(ctx *SnapshotRetentionContext) {}

// EnterRefRetain is called when production refRetain is entered.
func (s *BaseStarRocksListener) EnterRefRetain(ctx *RefRetainContext) {}

// ExitRefRetain is called when production refRetain is exited.
func (s *BaseStarRocksListener) ExitRefRetain(ctx *RefRetainContext) {}

// EnterMaxSnapshotAge is called when production maxSnapshotAge is entered.
func (s *BaseStarRocksListener) EnterMaxSnapshotAge(ctx *MaxSnapshotAgeContext) {}

// ExitMaxSnapshotAge is called when production maxSnapshotAge is exited.
func (s *BaseStarRocksListener) ExitMaxSnapshotAge(ctx *MaxSnapshotAgeContext) {}

// EnterMinSnapshotsToKeep is called when production minSnapshotsToKeep is entered.
func (s *BaseStarRocksListener) EnterMinSnapshotsToKeep(ctx *MinSnapshotsToKeepContext) {}

// ExitMinSnapshotsToKeep is called when production minSnapshotsToKeep is exited.
func (s *BaseStarRocksListener) ExitMinSnapshotsToKeep(ctx *MinSnapshotsToKeepContext) {}

// EnterSnapshotId is called when production snapshotId is entered.
func (s *BaseStarRocksListener) EnterSnapshotId(ctx *SnapshotIdContext) {}

// ExitSnapshotId is called when production snapshotId is exited.
func (s *BaseStarRocksListener) ExitSnapshotId(ctx *SnapshotIdContext) {}

// EnterTimeUnit is called when production timeUnit is entered.
func (s *BaseStarRocksListener) EnterTimeUnit(ctx *TimeUnitContext) {}

// ExitTimeUnit is called when production timeUnit is exited.
func (s *BaseStarRocksListener) ExitTimeUnit(ctx *TimeUnitContext) {}

// EnterInteger_list is called when production integer_list is entered.
func (s *BaseStarRocksListener) EnterInteger_list(ctx *Integer_listContext) {}

// ExitInteger_list is called when production integer_list is exited.
func (s *BaseStarRocksListener) ExitInteger_list(ctx *Integer_listContext) {}

// EnterDropPersistentIndexClause is called when production dropPersistentIndexClause is entered.
func (s *BaseStarRocksListener) EnterDropPersistentIndexClause(ctx *DropPersistentIndexClauseContext) {
}

// ExitDropPersistentIndexClause is called when production dropPersistentIndexClause is exited.
func (s *BaseStarRocksListener) ExitDropPersistentIndexClause(ctx *DropPersistentIndexClauseContext) {
}

// EnterAlterTableAutoIncrementClause is called when production alterTableAutoIncrementClause is entered.
func (s *BaseStarRocksListener) EnterAlterTableAutoIncrementClause(ctx *AlterTableAutoIncrementClauseContext) {
}

// ExitAlterTableAutoIncrementClause is called when production alterTableAutoIncrementClause is exited.
func (s *BaseStarRocksListener) ExitAlterTableAutoIncrementClause(ctx *AlterTableAutoIncrementClauseContext) {
}

// EnterAddPartitionClause is called when production addPartitionClause is entered.
func (s *BaseStarRocksListener) EnterAddPartitionClause(ctx *AddPartitionClauseContext) {}

// ExitAddPartitionClause is called when production addPartitionClause is exited.
func (s *BaseStarRocksListener) ExitAddPartitionClause(ctx *AddPartitionClauseContext) {}

// EnterDropPartitionClause is called when production dropPartitionClause is entered.
func (s *BaseStarRocksListener) EnterDropPartitionClause(ctx *DropPartitionClauseContext) {}

// ExitDropPartitionClause is called when production dropPartitionClause is exited.
func (s *BaseStarRocksListener) ExitDropPartitionClause(ctx *DropPartitionClauseContext) {}

// EnterTruncatePartitionClause is called when production truncatePartitionClause is entered.
func (s *BaseStarRocksListener) EnterTruncatePartitionClause(ctx *TruncatePartitionClauseContext) {}

// ExitTruncatePartitionClause is called when production truncatePartitionClause is exited.
func (s *BaseStarRocksListener) ExitTruncatePartitionClause(ctx *TruncatePartitionClauseContext) {}

// EnterModifyPartitionClause is called when production modifyPartitionClause is entered.
func (s *BaseStarRocksListener) EnterModifyPartitionClause(ctx *ModifyPartitionClauseContext) {}

// ExitModifyPartitionClause is called when production modifyPartitionClause is exited.
func (s *BaseStarRocksListener) ExitModifyPartitionClause(ctx *ModifyPartitionClauseContext) {}

// EnterReplacePartitionClause is called when production replacePartitionClause is entered.
func (s *BaseStarRocksListener) EnterReplacePartitionClause(ctx *ReplacePartitionClauseContext) {}

// ExitReplacePartitionClause is called when production replacePartitionClause is exited.
func (s *BaseStarRocksListener) ExitReplacePartitionClause(ctx *ReplacePartitionClauseContext) {}

// EnterPartitionRenameClause is called when production partitionRenameClause is entered.
func (s *BaseStarRocksListener) EnterPartitionRenameClause(ctx *PartitionRenameClauseContext) {}

// ExitPartitionRenameClause is called when production partitionRenameClause is exited.
func (s *BaseStarRocksListener) ExitPartitionRenameClause(ctx *PartitionRenameClauseContext) {}

// EnterInsertStatement is called when production insertStatement is entered.
func (s *BaseStarRocksListener) EnterInsertStatement(ctx *InsertStatementContext) {}

// ExitInsertStatement is called when production insertStatement is exited.
func (s *BaseStarRocksListener) ExitInsertStatement(ctx *InsertStatementContext) {}

// EnterInsertLabelOrColumnAliases is called when production insertLabelOrColumnAliases is entered.
func (s *BaseStarRocksListener) EnterInsertLabelOrColumnAliases(ctx *InsertLabelOrColumnAliasesContext) {
}

// ExitInsertLabelOrColumnAliases is called when production insertLabelOrColumnAliases is exited.
func (s *BaseStarRocksListener) ExitInsertLabelOrColumnAliases(ctx *InsertLabelOrColumnAliasesContext) {
}

// EnterColumnAliasesOrByName is called when production columnAliasesOrByName is entered.
func (s *BaseStarRocksListener) EnterColumnAliasesOrByName(ctx *ColumnAliasesOrByNameContext) {}

// ExitColumnAliasesOrByName is called when production columnAliasesOrByName is exited.
func (s *BaseStarRocksListener) ExitColumnAliasesOrByName(ctx *ColumnAliasesOrByNameContext) {}

// EnterUpdateStatement is called when production updateStatement is entered.
func (s *BaseStarRocksListener) EnterUpdateStatement(ctx *UpdateStatementContext) {}

// ExitUpdateStatement is called when production updateStatement is exited.
func (s *BaseStarRocksListener) ExitUpdateStatement(ctx *UpdateStatementContext) {}

// EnterDeleteStatement is called when production deleteStatement is entered.
func (s *BaseStarRocksListener) EnterDeleteStatement(ctx *DeleteStatementContext) {}

// ExitDeleteStatement is called when production deleteStatement is exited.
func (s *BaseStarRocksListener) ExitDeleteStatement(ctx *DeleteStatementContext) {}

// EnterCreateRoutineLoadStatement is called when production createRoutineLoadStatement is entered.
func (s *BaseStarRocksListener) EnterCreateRoutineLoadStatement(ctx *CreateRoutineLoadStatementContext) {
}

// ExitCreateRoutineLoadStatement is called when production createRoutineLoadStatement is exited.
func (s *BaseStarRocksListener) ExitCreateRoutineLoadStatement(ctx *CreateRoutineLoadStatementContext) {
}

// EnterAlterRoutineLoadStatement is called when production alterRoutineLoadStatement is entered.
func (s *BaseStarRocksListener) EnterAlterRoutineLoadStatement(ctx *AlterRoutineLoadStatementContext) {
}

// ExitAlterRoutineLoadStatement is called when production alterRoutineLoadStatement is exited.
func (s *BaseStarRocksListener) ExitAlterRoutineLoadStatement(ctx *AlterRoutineLoadStatementContext) {
}

// EnterDataSource is called when production dataSource is entered.
func (s *BaseStarRocksListener) EnterDataSource(ctx *DataSourceContext) {}

// ExitDataSource is called when production dataSource is exited.
func (s *BaseStarRocksListener) ExitDataSource(ctx *DataSourceContext) {}

// EnterLoadProperties is called when production loadProperties is entered.
func (s *BaseStarRocksListener) EnterLoadProperties(ctx *LoadPropertiesContext) {}

// ExitLoadProperties is called when production loadProperties is exited.
func (s *BaseStarRocksListener) ExitLoadProperties(ctx *LoadPropertiesContext) {}

// EnterColSeparatorProperty is called when production colSeparatorProperty is entered.
func (s *BaseStarRocksListener) EnterColSeparatorProperty(ctx *ColSeparatorPropertyContext) {}

// ExitColSeparatorProperty is called when production colSeparatorProperty is exited.
func (s *BaseStarRocksListener) ExitColSeparatorProperty(ctx *ColSeparatorPropertyContext) {}

// EnterRowDelimiterProperty is called when production rowDelimiterProperty is entered.
func (s *BaseStarRocksListener) EnterRowDelimiterProperty(ctx *RowDelimiterPropertyContext) {}

// ExitRowDelimiterProperty is called when production rowDelimiterProperty is exited.
func (s *BaseStarRocksListener) ExitRowDelimiterProperty(ctx *RowDelimiterPropertyContext) {}

// EnterImportColumns is called when production importColumns is entered.
func (s *BaseStarRocksListener) EnterImportColumns(ctx *ImportColumnsContext) {}

// ExitImportColumns is called when production importColumns is exited.
func (s *BaseStarRocksListener) ExitImportColumns(ctx *ImportColumnsContext) {}

// EnterColumnProperties is called when production columnProperties is entered.
func (s *BaseStarRocksListener) EnterColumnProperties(ctx *ColumnPropertiesContext) {}

// ExitColumnProperties is called when production columnProperties is exited.
func (s *BaseStarRocksListener) ExitColumnProperties(ctx *ColumnPropertiesContext) {}

// EnterJobProperties is called when production jobProperties is entered.
func (s *BaseStarRocksListener) EnterJobProperties(ctx *JobPropertiesContext) {}

// ExitJobProperties is called when production jobProperties is exited.
func (s *BaseStarRocksListener) ExitJobProperties(ctx *JobPropertiesContext) {}

// EnterDataSourceProperties is called when production dataSourceProperties is entered.
func (s *BaseStarRocksListener) EnterDataSourceProperties(ctx *DataSourcePropertiesContext) {}

// ExitDataSourceProperties is called when production dataSourceProperties is exited.
func (s *BaseStarRocksListener) ExitDataSourceProperties(ctx *DataSourcePropertiesContext) {}

// EnterStopRoutineLoadStatement is called when production stopRoutineLoadStatement is entered.
func (s *BaseStarRocksListener) EnterStopRoutineLoadStatement(ctx *StopRoutineLoadStatementContext) {}

// ExitStopRoutineLoadStatement is called when production stopRoutineLoadStatement is exited.
func (s *BaseStarRocksListener) ExitStopRoutineLoadStatement(ctx *StopRoutineLoadStatementContext) {}

// EnterResumeRoutineLoadStatement is called when production resumeRoutineLoadStatement is entered.
func (s *BaseStarRocksListener) EnterResumeRoutineLoadStatement(ctx *ResumeRoutineLoadStatementContext) {
}

// ExitResumeRoutineLoadStatement is called when production resumeRoutineLoadStatement is exited.
func (s *BaseStarRocksListener) ExitResumeRoutineLoadStatement(ctx *ResumeRoutineLoadStatementContext) {
}

// EnterPauseRoutineLoadStatement is called when production pauseRoutineLoadStatement is entered.
func (s *BaseStarRocksListener) EnterPauseRoutineLoadStatement(ctx *PauseRoutineLoadStatementContext) {
}

// ExitPauseRoutineLoadStatement is called when production pauseRoutineLoadStatement is exited.
func (s *BaseStarRocksListener) ExitPauseRoutineLoadStatement(ctx *PauseRoutineLoadStatementContext) {
}

// EnterShowRoutineLoadStatement is called when production showRoutineLoadStatement is entered.
func (s *BaseStarRocksListener) EnterShowRoutineLoadStatement(ctx *ShowRoutineLoadStatementContext) {}

// ExitShowRoutineLoadStatement is called when production showRoutineLoadStatement is exited.
func (s *BaseStarRocksListener) ExitShowRoutineLoadStatement(ctx *ShowRoutineLoadStatementContext) {}

// EnterShowRoutineLoadTaskStatement is called when production showRoutineLoadTaskStatement is entered.
func (s *BaseStarRocksListener) EnterShowRoutineLoadTaskStatement(ctx *ShowRoutineLoadTaskStatementContext) {
}

// ExitShowRoutineLoadTaskStatement is called when production showRoutineLoadTaskStatement is exited.
func (s *BaseStarRocksListener) ExitShowRoutineLoadTaskStatement(ctx *ShowRoutineLoadTaskStatementContext) {
}

// EnterShowCreateRoutineLoadStatement is called when production showCreateRoutineLoadStatement is entered.
func (s *BaseStarRocksListener) EnterShowCreateRoutineLoadStatement(ctx *ShowCreateRoutineLoadStatementContext) {
}

// ExitShowCreateRoutineLoadStatement is called when production showCreateRoutineLoadStatement is exited.
func (s *BaseStarRocksListener) ExitShowCreateRoutineLoadStatement(ctx *ShowCreateRoutineLoadStatementContext) {
}

// EnterShowStreamLoadStatement is called when production showStreamLoadStatement is entered.
func (s *BaseStarRocksListener) EnterShowStreamLoadStatement(ctx *ShowStreamLoadStatementContext) {}

// ExitShowStreamLoadStatement is called when production showStreamLoadStatement is exited.
func (s *BaseStarRocksListener) ExitShowStreamLoadStatement(ctx *ShowStreamLoadStatementContext) {}

// EnterAnalyzeStatement is called when production analyzeStatement is entered.
func (s *BaseStarRocksListener) EnterAnalyzeStatement(ctx *AnalyzeStatementContext) {}

// ExitAnalyzeStatement is called when production analyzeStatement is exited.
func (s *BaseStarRocksListener) ExitAnalyzeStatement(ctx *AnalyzeStatementContext) {}

// EnterRegularColumns is called when production regularColumns is entered.
func (s *BaseStarRocksListener) EnterRegularColumns(ctx *RegularColumnsContext) {}

// ExitRegularColumns is called when production regularColumns is exited.
func (s *BaseStarRocksListener) ExitRegularColumns(ctx *RegularColumnsContext) {}

// EnterAllColumns is called when production allColumns is entered.
func (s *BaseStarRocksListener) EnterAllColumns(ctx *AllColumnsContext) {}

// ExitAllColumns is called when production allColumns is exited.
func (s *BaseStarRocksListener) ExitAllColumns(ctx *AllColumnsContext) {}

// EnterPredicateColumns is called when production predicateColumns is entered.
func (s *BaseStarRocksListener) EnterPredicateColumns(ctx *PredicateColumnsContext) {}

// ExitPredicateColumns is called when production predicateColumns is exited.
func (s *BaseStarRocksListener) ExitPredicateColumns(ctx *PredicateColumnsContext) {}

// EnterMultiColumnSet is called when production multiColumnSet is entered.
func (s *BaseStarRocksListener) EnterMultiColumnSet(ctx *MultiColumnSetContext) {}

// ExitMultiColumnSet is called when production multiColumnSet is exited.
func (s *BaseStarRocksListener) ExitMultiColumnSet(ctx *MultiColumnSetContext) {}

// EnterDropStatsStatement is called when production dropStatsStatement is entered.
func (s *BaseStarRocksListener) EnterDropStatsStatement(ctx *DropStatsStatementContext) {}

// ExitDropStatsStatement is called when production dropStatsStatement is exited.
func (s *BaseStarRocksListener) ExitDropStatsStatement(ctx *DropStatsStatementContext) {}

// EnterHistogramStatement is called when production histogramStatement is entered.
func (s *BaseStarRocksListener) EnterHistogramStatement(ctx *HistogramStatementContext) {}

// ExitHistogramStatement is called when production histogramStatement is exited.
func (s *BaseStarRocksListener) ExitHistogramStatement(ctx *HistogramStatementContext) {}

// EnterAnalyzeHistogramStatement is called when production analyzeHistogramStatement is entered.
func (s *BaseStarRocksListener) EnterAnalyzeHistogramStatement(ctx *AnalyzeHistogramStatementContext) {
}

// ExitAnalyzeHistogramStatement is called when production analyzeHistogramStatement is exited.
func (s *BaseStarRocksListener) ExitAnalyzeHistogramStatement(ctx *AnalyzeHistogramStatementContext) {
}

// EnterDropHistogramStatement is called when production dropHistogramStatement is entered.
func (s *BaseStarRocksListener) EnterDropHistogramStatement(ctx *DropHistogramStatementContext) {}

// ExitDropHistogramStatement is called when production dropHistogramStatement is exited.
func (s *BaseStarRocksListener) ExitDropHistogramStatement(ctx *DropHistogramStatementContext) {}

// EnterCreateAnalyzeStatement is called when production createAnalyzeStatement is entered.
func (s *BaseStarRocksListener) EnterCreateAnalyzeStatement(ctx *CreateAnalyzeStatementContext) {}

// ExitCreateAnalyzeStatement is called when production createAnalyzeStatement is exited.
func (s *BaseStarRocksListener) ExitCreateAnalyzeStatement(ctx *CreateAnalyzeStatementContext) {}

// EnterDropAnalyzeJobStatement is called when production dropAnalyzeJobStatement is entered.
func (s *BaseStarRocksListener) EnterDropAnalyzeJobStatement(ctx *DropAnalyzeJobStatementContext) {}

// ExitDropAnalyzeJobStatement is called when production dropAnalyzeJobStatement is exited.
func (s *BaseStarRocksListener) ExitDropAnalyzeJobStatement(ctx *DropAnalyzeJobStatementContext) {}

// EnterShowAnalyzeStatement is called when production showAnalyzeStatement is entered.
func (s *BaseStarRocksListener) EnterShowAnalyzeStatement(ctx *ShowAnalyzeStatementContext) {}

// ExitShowAnalyzeStatement is called when production showAnalyzeStatement is exited.
func (s *BaseStarRocksListener) ExitShowAnalyzeStatement(ctx *ShowAnalyzeStatementContext) {}

// EnterShowStatsMetaStatement is called when production showStatsMetaStatement is entered.
func (s *BaseStarRocksListener) EnterShowStatsMetaStatement(ctx *ShowStatsMetaStatementContext) {}

// ExitShowStatsMetaStatement is called when production showStatsMetaStatement is exited.
func (s *BaseStarRocksListener) ExitShowStatsMetaStatement(ctx *ShowStatsMetaStatementContext) {}

// EnterShowHistogramMetaStatement is called when production showHistogramMetaStatement is entered.
func (s *BaseStarRocksListener) EnterShowHistogramMetaStatement(ctx *ShowHistogramMetaStatementContext) {
}

// ExitShowHistogramMetaStatement is called when production showHistogramMetaStatement is exited.
func (s *BaseStarRocksListener) ExitShowHistogramMetaStatement(ctx *ShowHistogramMetaStatementContext) {
}

// EnterKillAnalyzeStatement is called when production killAnalyzeStatement is entered.
func (s *BaseStarRocksListener) EnterKillAnalyzeStatement(ctx *KillAnalyzeStatementContext) {}

// ExitKillAnalyzeStatement is called when production killAnalyzeStatement is exited.
func (s *BaseStarRocksListener) ExitKillAnalyzeStatement(ctx *KillAnalyzeStatementContext) {}

// EnterAnalyzeProfileStatement is called when production analyzeProfileStatement is entered.
func (s *BaseStarRocksListener) EnterAnalyzeProfileStatement(ctx *AnalyzeProfileStatementContext) {}

// ExitAnalyzeProfileStatement is called when production analyzeProfileStatement is exited.
func (s *BaseStarRocksListener) ExitAnalyzeProfileStatement(ctx *AnalyzeProfileStatementContext) {}

// EnterCreateBaselinePlanStatement is called when production createBaselinePlanStatement is entered.
func (s *BaseStarRocksListener) EnterCreateBaselinePlanStatement(ctx *CreateBaselinePlanStatementContext) {
}

// ExitCreateBaselinePlanStatement is called when production createBaselinePlanStatement is exited.
func (s *BaseStarRocksListener) ExitCreateBaselinePlanStatement(ctx *CreateBaselinePlanStatementContext) {
}

// EnterDropBaselinePlanStatement is called when production dropBaselinePlanStatement is entered.
func (s *BaseStarRocksListener) EnterDropBaselinePlanStatement(ctx *DropBaselinePlanStatementContext) {
}

// ExitDropBaselinePlanStatement is called when production dropBaselinePlanStatement is exited.
func (s *BaseStarRocksListener) ExitDropBaselinePlanStatement(ctx *DropBaselinePlanStatementContext) {
}

// EnterShowBaselinePlanStatement is called when production showBaselinePlanStatement is entered.
func (s *BaseStarRocksListener) EnterShowBaselinePlanStatement(ctx *ShowBaselinePlanStatementContext) {
}

// ExitShowBaselinePlanStatement is called when production showBaselinePlanStatement is exited.
func (s *BaseStarRocksListener) ExitShowBaselinePlanStatement(ctx *ShowBaselinePlanStatementContext) {
}

// EnterCreateResourceGroupStatement is called when production createResourceGroupStatement is entered.
func (s *BaseStarRocksListener) EnterCreateResourceGroupStatement(ctx *CreateResourceGroupStatementContext) {
}

// ExitCreateResourceGroupStatement is called when production createResourceGroupStatement is exited.
func (s *BaseStarRocksListener) ExitCreateResourceGroupStatement(ctx *CreateResourceGroupStatementContext) {
}

// EnterDropResourceGroupStatement is called when production dropResourceGroupStatement is entered.
func (s *BaseStarRocksListener) EnterDropResourceGroupStatement(ctx *DropResourceGroupStatementContext) {
}

// ExitDropResourceGroupStatement is called when production dropResourceGroupStatement is exited.
func (s *BaseStarRocksListener) ExitDropResourceGroupStatement(ctx *DropResourceGroupStatementContext) {
}

// EnterAlterResourceGroupStatement is called when production alterResourceGroupStatement is entered.
func (s *BaseStarRocksListener) EnterAlterResourceGroupStatement(ctx *AlterResourceGroupStatementContext) {
}

// ExitAlterResourceGroupStatement is called when production alterResourceGroupStatement is exited.
func (s *BaseStarRocksListener) ExitAlterResourceGroupStatement(ctx *AlterResourceGroupStatementContext) {
}

// EnterShowResourceGroupStatement is called when production showResourceGroupStatement is entered.
func (s *BaseStarRocksListener) EnterShowResourceGroupStatement(ctx *ShowResourceGroupStatementContext) {
}

// ExitShowResourceGroupStatement is called when production showResourceGroupStatement is exited.
func (s *BaseStarRocksListener) ExitShowResourceGroupStatement(ctx *ShowResourceGroupStatementContext) {
}

// EnterShowResourceGroupUsageStatement is called when production showResourceGroupUsageStatement is entered.
func (s *BaseStarRocksListener) EnterShowResourceGroupUsageStatement(ctx *ShowResourceGroupUsageStatementContext) {
}

// ExitShowResourceGroupUsageStatement is called when production showResourceGroupUsageStatement is exited.
func (s *BaseStarRocksListener) ExitShowResourceGroupUsageStatement(ctx *ShowResourceGroupUsageStatementContext) {
}

// EnterCreateResourceStatement is called when production createResourceStatement is entered.
func (s *BaseStarRocksListener) EnterCreateResourceStatement(ctx *CreateResourceStatementContext) {}

// ExitCreateResourceStatement is called when production createResourceStatement is exited.
func (s *BaseStarRocksListener) ExitCreateResourceStatement(ctx *CreateResourceStatementContext) {}

// EnterAlterResourceStatement is called when production alterResourceStatement is entered.
func (s *BaseStarRocksListener) EnterAlterResourceStatement(ctx *AlterResourceStatementContext) {}

// ExitAlterResourceStatement is called when production alterResourceStatement is exited.
func (s *BaseStarRocksListener) ExitAlterResourceStatement(ctx *AlterResourceStatementContext) {}

// EnterDropResourceStatement is called when production dropResourceStatement is entered.
func (s *BaseStarRocksListener) EnterDropResourceStatement(ctx *DropResourceStatementContext) {}

// ExitDropResourceStatement is called when production dropResourceStatement is exited.
func (s *BaseStarRocksListener) ExitDropResourceStatement(ctx *DropResourceStatementContext) {}

// EnterShowResourceStatement is called when production showResourceStatement is entered.
func (s *BaseStarRocksListener) EnterShowResourceStatement(ctx *ShowResourceStatementContext) {}

// ExitShowResourceStatement is called when production showResourceStatement is exited.
func (s *BaseStarRocksListener) ExitShowResourceStatement(ctx *ShowResourceStatementContext) {}

// EnterClassifier is called when production classifier is entered.
func (s *BaseStarRocksListener) EnterClassifier(ctx *ClassifierContext) {}

// ExitClassifier is called when production classifier is exited.
func (s *BaseStarRocksListener) ExitClassifier(ctx *ClassifierContext) {}

// EnterShowFunctionsStatement is called when production showFunctionsStatement is entered.
func (s *BaseStarRocksListener) EnterShowFunctionsStatement(ctx *ShowFunctionsStatementContext) {}

// ExitShowFunctionsStatement is called when production showFunctionsStatement is exited.
func (s *BaseStarRocksListener) ExitShowFunctionsStatement(ctx *ShowFunctionsStatementContext) {}

// EnterDropFunctionStatement is called when production dropFunctionStatement is entered.
func (s *BaseStarRocksListener) EnterDropFunctionStatement(ctx *DropFunctionStatementContext) {}

// ExitDropFunctionStatement is called when production dropFunctionStatement is exited.
func (s *BaseStarRocksListener) ExitDropFunctionStatement(ctx *DropFunctionStatementContext) {}

// EnterCreateFunctionStatement is called when production createFunctionStatement is entered.
func (s *BaseStarRocksListener) EnterCreateFunctionStatement(ctx *CreateFunctionStatementContext) {}

// ExitCreateFunctionStatement is called when production createFunctionStatement is exited.
func (s *BaseStarRocksListener) ExitCreateFunctionStatement(ctx *CreateFunctionStatementContext) {}

// EnterInlineFunction is called when production inlineFunction is entered.
func (s *BaseStarRocksListener) EnterInlineFunction(ctx *InlineFunctionContext) {}

// ExitInlineFunction is called when production inlineFunction is exited.
func (s *BaseStarRocksListener) ExitInlineFunction(ctx *InlineFunctionContext) {}

// EnterTypeList is called when production typeList is entered.
func (s *BaseStarRocksListener) EnterTypeList(ctx *TypeListContext) {}

// ExitTypeList is called when production typeList is exited.
func (s *BaseStarRocksListener) ExitTypeList(ctx *TypeListContext) {}

// EnterLoadStatement is called when production loadStatement is entered.
func (s *BaseStarRocksListener) EnterLoadStatement(ctx *LoadStatementContext) {}

// ExitLoadStatement is called when production loadStatement is exited.
func (s *BaseStarRocksListener) ExitLoadStatement(ctx *LoadStatementContext) {}

// EnterLabelName is called when production labelName is entered.
func (s *BaseStarRocksListener) EnterLabelName(ctx *LabelNameContext) {}

// ExitLabelName is called when production labelName is exited.
func (s *BaseStarRocksListener) ExitLabelName(ctx *LabelNameContext) {}

// EnterDataDescList is called when production dataDescList is entered.
func (s *BaseStarRocksListener) EnterDataDescList(ctx *DataDescListContext) {}

// ExitDataDescList is called when production dataDescList is exited.
func (s *BaseStarRocksListener) ExitDataDescList(ctx *DataDescListContext) {}

// EnterDataDesc is called when production dataDesc is entered.
func (s *BaseStarRocksListener) EnterDataDesc(ctx *DataDescContext) {}

// ExitDataDesc is called when production dataDesc is exited.
func (s *BaseStarRocksListener) ExitDataDesc(ctx *DataDescContext) {}

// EnterFormatProps is called when production formatProps is entered.
func (s *BaseStarRocksListener) EnterFormatProps(ctx *FormatPropsContext) {}

// ExitFormatProps is called when production formatProps is exited.
func (s *BaseStarRocksListener) ExitFormatProps(ctx *FormatPropsContext) {}

// EnterBrokerDesc is called when production brokerDesc is entered.
func (s *BaseStarRocksListener) EnterBrokerDesc(ctx *BrokerDescContext) {}

// ExitBrokerDesc is called when production brokerDesc is exited.
func (s *BaseStarRocksListener) ExitBrokerDesc(ctx *BrokerDescContext) {}

// EnterResourceDesc is called when production resourceDesc is entered.
func (s *BaseStarRocksListener) EnterResourceDesc(ctx *ResourceDescContext) {}

// ExitResourceDesc is called when production resourceDesc is exited.
func (s *BaseStarRocksListener) ExitResourceDesc(ctx *ResourceDescContext) {}

// EnterShowLoadStatement is called when production showLoadStatement is entered.
func (s *BaseStarRocksListener) EnterShowLoadStatement(ctx *ShowLoadStatementContext) {}

// ExitShowLoadStatement is called when production showLoadStatement is exited.
func (s *BaseStarRocksListener) ExitShowLoadStatement(ctx *ShowLoadStatementContext) {}

// EnterShowLoadWarningsStatement is called when production showLoadWarningsStatement is entered.
func (s *BaseStarRocksListener) EnterShowLoadWarningsStatement(ctx *ShowLoadWarningsStatementContext) {
}

// ExitShowLoadWarningsStatement is called when production showLoadWarningsStatement is exited.
func (s *BaseStarRocksListener) ExitShowLoadWarningsStatement(ctx *ShowLoadWarningsStatementContext) {
}

// EnterCancelLoadStatement is called when production cancelLoadStatement is entered.
func (s *BaseStarRocksListener) EnterCancelLoadStatement(ctx *CancelLoadStatementContext) {}

// ExitCancelLoadStatement is called when production cancelLoadStatement is exited.
func (s *BaseStarRocksListener) ExitCancelLoadStatement(ctx *CancelLoadStatementContext) {}

// EnterAlterLoadStatement is called when production alterLoadStatement is entered.
func (s *BaseStarRocksListener) EnterAlterLoadStatement(ctx *AlterLoadStatementContext) {}

// ExitAlterLoadStatement is called when production alterLoadStatement is exited.
func (s *BaseStarRocksListener) ExitAlterLoadStatement(ctx *AlterLoadStatementContext) {}

// EnterCancelCompactionStatement is called when production cancelCompactionStatement is entered.
func (s *BaseStarRocksListener) EnterCancelCompactionStatement(ctx *CancelCompactionStatementContext) {
}

// ExitCancelCompactionStatement is called when production cancelCompactionStatement is exited.
func (s *BaseStarRocksListener) ExitCancelCompactionStatement(ctx *CancelCompactionStatementContext) {
}

// EnterShowAuthorStatement is called when production showAuthorStatement is entered.
func (s *BaseStarRocksListener) EnterShowAuthorStatement(ctx *ShowAuthorStatementContext) {}

// ExitShowAuthorStatement is called when production showAuthorStatement is exited.
func (s *BaseStarRocksListener) ExitShowAuthorStatement(ctx *ShowAuthorStatementContext) {}

// EnterShowBackendsStatement is called when production showBackendsStatement is entered.
func (s *BaseStarRocksListener) EnterShowBackendsStatement(ctx *ShowBackendsStatementContext) {}

// ExitShowBackendsStatement is called when production showBackendsStatement is exited.
func (s *BaseStarRocksListener) ExitShowBackendsStatement(ctx *ShowBackendsStatementContext) {}

// EnterShowBrokerStatement is called when production showBrokerStatement is entered.
func (s *BaseStarRocksListener) EnterShowBrokerStatement(ctx *ShowBrokerStatementContext) {}

// ExitShowBrokerStatement is called when production showBrokerStatement is exited.
func (s *BaseStarRocksListener) ExitShowBrokerStatement(ctx *ShowBrokerStatementContext) {}

// EnterShowCharsetStatement is called when production showCharsetStatement is entered.
func (s *BaseStarRocksListener) EnterShowCharsetStatement(ctx *ShowCharsetStatementContext) {}

// ExitShowCharsetStatement is called when production showCharsetStatement is exited.
func (s *BaseStarRocksListener) ExitShowCharsetStatement(ctx *ShowCharsetStatementContext) {}

// EnterShowCollationStatement is called when production showCollationStatement is entered.
func (s *BaseStarRocksListener) EnterShowCollationStatement(ctx *ShowCollationStatementContext) {}

// ExitShowCollationStatement is called when production showCollationStatement is exited.
func (s *BaseStarRocksListener) ExitShowCollationStatement(ctx *ShowCollationStatementContext) {}

// EnterShowDeleteStatement is called when production showDeleteStatement is entered.
func (s *BaseStarRocksListener) EnterShowDeleteStatement(ctx *ShowDeleteStatementContext) {}

// ExitShowDeleteStatement is called when production showDeleteStatement is exited.
func (s *BaseStarRocksListener) ExitShowDeleteStatement(ctx *ShowDeleteStatementContext) {}

// EnterShowDynamicPartitionStatement is called when production showDynamicPartitionStatement is entered.
func (s *BaseStarRocksListener) EnterShowDynamicPartitionStatement(ctx *ShowDynamicPartitionStatementContext) {
}

// ExitShowDynamicPartitionStatement is called when production showDynamicPartitionStatement is exited.
func (s *BaseStarRocksListener) ExitShowDynamicPartitionStatement(ctx *ShowDynamicPartitionStatementContext) {
}

// EnterShowEventsStatement is called when production showEventsStatement is entered.
func (s *BaseStarRocksListener) EnterShowEventsStatement(ctx *ShowEventsStatementContext) {}

// ExitShowEventsStatement is called when production showEventsStatement is exited.
func (s *BaseStarRocksListener) ExitShowEventsStatement(ctx *ShowEventsStatementContext) {}

// EnterShowEnginesStatement is called when production showEnginesStatement is entered.
func (s *BaseStarRocksListener) EnterShowEnginesStatement(ctx *ShowEnginesStatementContext) {}

// ExitShowEnginesStatement is called when production showEnginesStatement is exited.
func (s *BaseStarRocksListener) ExitShowEnginesStatement(ctx *ShowEnginesStatementContext) {}

// EnterShowFrontendsStatement is called when production showFrontendsStatement is entered.
func (s *BaseStarRocksListener) EnterShowFrontendsStatement(ctx *ShowFrontendsStatementContext) {}

// ExitShowFrontendsStatement is called when production showFrontendsStatement is exited.
func (s *BaseStarRocksListener) ExitShowFrontendsStatement(ctx *ShowFrontendsStatementContext) {}

// EnterShowPluginsStatement is called when production showPluginsStatement is entered.
func (s *BaseStarRocksListener) EnterShowPluginsStatement(ctx *ShowPluginsStatementContext) {}

// ExitShowPluginsStatement is called when production showPluginsStatement is exited.
func (s *BaseStarRocksListener) ExitShowPluginsStatement(ctx *ShowPluginsStatementContext) {}

// EnterShowRepositoriesStatement is called when production showRepositoriesStatement is entered.
func (s *BaseStarRocksListener) EnterShowRepositoriesStatement(ctx *ShowRepositoriesStatementContext) {
}

// ExitShowRepositoriesStatement is called when production showRepositoriesStatement is exited.
func (s *BaseStarRocksListener) ExitShowRepositoriesStatement(ctx *ShowRepositoriesStatementContext) {
}

// EnterShowOpenTableStatement is called when production showOpenTableStatement is entered.
func (s *BaseStarRocksListener) EnterShowOpenTableStatement(ctx *ShowOpenTableStatementContext) {}

// ExitShowOpenTableStatement is called when production showOpenTableStatement is exited.
func (s *BaseStarRocksListener) ExitShowOpenTableStatement(ctx *ShowOpenTableStatementContext) {}

// EnterShowPrivilegesStatement is called when production showPrivilegesStatement is entered.
func (s *BaseStarRocksListener) EnterShowPrivilegesStatement(ctx *ShowPrivilegesStatementContext) {}

// ExitShowPrivilegesStatement is called when production showPrivilegesStatement is exited.
func (s *BaseStarRocksListener) ExitShowPrivilegesStatement(ctx *ShowPrivilegesStatementContext) {}

// EnterShowProcedureStatement is called when production showProcedureStatement is entered.
func (s *BaseStarRocksListener) EnterShowProcedureStatement(ctx *ShowProcedureStatementContext) {}

// ExitShowProcedureStatement is called when production showProcedureStatement is exited.
func (s *BaseStarRocksListener) ExitShowProcedureStatement(ctx *ShowProcedureStatementContext) {}

// EnterShowProcStatement is called when production showProcStatement is entered.
func (s *BaseStarRocksListener) EnterShowProcStatement(ctx *ShowProcStatementContext) {}

// ExitShowProcStatement is called when production showProcStatement is exited.
func (s *BaseStarRocksListener) ExitShowProcStatement(ctx *ShowProcStatementContext) {}

// EnterShowProcesslistStatement is called when production showProcesslistStatement is entered.
func (s *BaseStarRocksListener) EnterShowProcesslistStatement(ctx *ShowProcesslistStatementContext) {}

// ExitShowProcesslistStatement is called when production showProcesslistStatement is exited.
func (s *BaseStarRocksListener) ExitShowProcesslistStatement(ctx *ShowProcesslistStatementContext) {}

// EnterShowProfilelistStatement is called when production showProfilelistStatement is entered.
func (s *BaseStarRocksListener) EnterShowProfilelistStatement(ctx *ShowProfilelistStatementContext) {}

// ExitShowProfilelistStatement is called when production showProfilelistStatement is exited.
func (s *BaseStarRocksListener) ExitShowProfilelistStatement(ctx *ShowProfilelistStatementContext) {}

// EnterShowRunningQueriesStatement is called when production showRunningQueriesStatement is entered.
func (s *BaseStarRocksListener) EnterShowRunningQueriesStatement(ctx *ShowRunningQueriesStatementContext) {
}

// ExitShowRunningQueriesStatement is called when production showRunningQueriesStatement is exited.
func (s *BaseStarRocksListener) ExitShowRunningQueriesStatement(ctx *ShowRunningQueriesStatementContext) {
}

// EnterShowStatusStatement is called when production showStatusStatement is entered.
func (s *BaseStarRocksListener) EnterShowStatusStatement(ctx *ShowStatusStatementContext) {}

// ExitShowStatusStatement is called when production showStatusStatement is exited.
func (s *BaseStarRocksListener) ExitShowStatusStatement(ctx *ShowStatusStatementContext) {}

// EnterShowTabletStatement is called when production showTabletStatement is entered.
func (s *BaseStarRocksListener) EnterShowTabletStatement(ctx *ShowTabletStatementContext) {}

// ExitShowTabletStatement is called when production showTabletStatement is exited.
func (s *BaseStarRocksListener) ExitShowTabletStatement(ctx *ShowTabletStatementContext) {}

// EnterShowTransactionStatement is called when production showTransactionStatement is entered.
func (s *BaseStarRocksListener) EnterShowTransactionStatement(ctx *ShowTransactionStatementContext) {}

// ExitShowTransactionStatement is called when production showTransactionStatement is exited.
func (s *BaseStarRocksListener) ExitShowTransactionStatement(ctx *ShowTransactionStatementContext) {}

// EnterShowTriggersStatement is called when production showTriggersStatement is entered.
func (s *BaseStarRocksListener) EnterShowTriggersStatement(ctx *ShowTriggersStatementContext) {}

// ExitShowTriggersStatement is called when production showTriggersStatement is exited.
func (s *BaseStarRocksListener) ExitShowTriggersStatement(ctx *ShowTriggersStatementContext) {}

// EnterShowUserPropertyStatement is called when production showUserPropertyStatement is entered.
func (s *BaseStarRocksListener) EnterShowUserPropertyStatement(ctx *ShowUserPropertyStatementContext) {
}

// ExitShowUserPropertyStatement is called when production showUserPropertyStatement is exited.
func (s *BaseStarRocksListener) ExitShowUserPropertyStatement(ctx *ShowUserPropertyStatementContext) {
}

// EnterShowVariablesStatement is called when production showVariablesStatement is entered.
func (s *BaseStarRocksListener) EnterShowVariablesStatement(ctx *ShowVariablesStatementContext) {}

// ExitShowVariablesStatement is called when production showVariablesStatement is exited.
func (s *BaseStarRocksListener) ExitShowVariablesStatement(ctx *ShowVariablesStatementContext) {}

// EnterShowWarningStatement is called when production showWarningStatement is entered.
func (s *BaseStarRocksListener) EnterShowWarningStatement(ctx *ShowWarningStatementContext) {}

// ExitShowWarningStatement is called when production showWarningStatement is exited.
func (s *BaseStarRocksListener) ExitShowWarningStatement(ctx *ShowWarningStatementContext) {}

// EnterHelpStatement is called when production helpStatement is entered.
func (s *BaseStarRocksListener) EnterHelpStatement(ctx *HelpStatementContext) {}

// ExitHelpStatement is called when production helpStatement is exited.
func (s *BaseStarRocksListener) ExitHelpStatement(ctx *HelpStatementContext) {}

// EnterCreateUserStatement is called when production createUserStatement is entered.
func (s *BaseStarRocksListener) EnterCreateUserStatement(ctx *CreateUserStatementContext) {}

// ExitCreateUserStatement is called when production createUserStatement is exited.
func (s *BaseStarRocksListener) ExitCreateUserStatement(ctx *CreateUserStatementContext) {}

// EnterDropUserStatement is called when production dropUserStatement is entered.
func (s *BaseStarRocksListener) EnterDropUserStatement(ctx *DropUserStatementContext) {}

// ExitDropUserStatement is called when production dropUserStatement is exited.
func (s *BaseStarRocksListener) ExitDropUserStatement(ctx *DropUserStatementContext) {}

// EnterAlterUserStatement is called when production alterUserStatement is entered.
func (s *BaseStarRocksListener) EnterAlterUserStatement(ctx *AlterUserStatementContext) {}

// ExitAlterUserStatement is called when production alterUserStatement is exited.
func (s *BaseStarRocksListener) ExitAlterUserStatement(ctx *AlterUserStatementContext) {}

// EnterShowUserStatement is called when production showUserStatement is entered.
func (s *BaseStarRocksListener) EnterShowUserStatement(ctx *ShowUserStatementContext) {}

// ExitShowUserStatement is called when production showUserStatement is exited.
func (s *BaseStarRocksListener) ExitShowUserStatement(ctx *ShowUserStatementContext) {}

// EnterShowAllAuthentication is called when production showAllAuthentication is entered.
func (s *BaseStarRocksListener) EnterShowAllAuthentication(ctx *ShowAllAuthenticationContext) {}

// ExitShowAllAuthentication is called when production showAllAuthentication is exited.
func (s *BaseStarRocksListener) ExitShowAllAuthentication(ctx *ShowAllAuthenticationContext) {}

// EnterShowAuthenticationForUser is called when production showAuthenticationForUser is entered.
func (s *BaseStarRocksListener) EnterShowAuthenticationForUser(ctx *ShowAuthenticationForUserContext) {
}

// ExitShowAuthenticationForUser is called when production showAuthenticationForUser is exited.
func (s *BaseStarRocksListener) ExitShowAuthenticationForUser(ctx *ShowAuthenticationForUserContext) {
}

// EnterExecuteAsStatement is called when production executeAsStatement is entered.
func (s *BaseStarRocksListener) EnterExecuteAsStatement(ctx *ExecuteAsStatementContext) {}

// ExitExecuteAsStatement is called when production executeAsStatement is exited.
func (s *BaseStarRocksListener) ExitExecuteAsStatement(ctx *ExecuteAsStatementContext) {}

// EnterCreateRoleStatement is called when production createRoleStatement is entered.
func (s *BaseStarRocksListener) EnterCreateRoleStatement(ctx *CreateRoleStatementContext) {}

// ExitCreateRoleStatement is called when production createRoleStatement is exited.
func (s *BaseStarRocksListener) ExitCreateRoleStatement(ctx *CreateRoleStatementContext) {}

// EnterAlterRoleStatement is called when production alterRoleStatement is entered.
func (s *BaseStarRocksListener) EnterAlterRoleStatement(ctx *AlterRoleStatementContext) {}

// ExitAlterRoleStatement is called when production alterRoleStatement is exited.
func (s *BaseStarRocksListener) ExitAlterRoleStatement(ctx *AlterRoleStatementContext) {}

// EnterDropRoleStatement is called when production dropRoleStatement is entered.
func (s *BaseStarRocksListener) EnterDropRoleStatement(ctx *DropRoleStatementContext) {}

// ExitDropRoleStatement is called when production dropRoleStatement is exited.
func (s *BaseStarRocksListener) ExitDropRoleStatement(ctx *DropRoleStatementContext) {}

// EnterShowRolesStatement is called when production showRolesStatement is entered.
func (s *BaseStarRocksListener) EnterShowRolesStatement(ctx *ShowRolesStatementContext) {}

// ExitShowRolesStatement is called when production showRolesStatement is exited.
func (s *BaseStarRocksListener) ExitShowRolesStatement(ctx *ShowRolesStatementContext) {}

// EnterGrantRoleToUser is called when production grantRoleToUser is entered.
func (s *BaseStarRocksListener) EnterGrantRoleToUser(ctx *GrantRoleToUserContext) {}

// ExitGrantRoleToUser is called when production grantRoleToUser is exited.
func (s *BaseStarRocksListener) ExitGrantRoleToUser(ctx *GrantRoleToUserContext) {}

// EnterGrantRoleToRole is called when production grantRoleToRole is entered.
func (s *BaseStarRocksListener) EnterGrantRoleToRole(ctx *GrantRoleToRoleContext) {}

// ExitGrantRoleToRole is called when production grantRoleToRole is exited.
func (s *BaseStarRocksListener) ExitGrantRoleToRole(ctx *GrantRoleToRoleContext) {}

// EnterRevokeRoleFromUser is called when production revokeRoleFromUser is entered.
func (s *BaseStarRocksListener) EnterRevokeRoleFromUser(ctx *RevokeRoleFromUserContext) {}

// ExitRevokeRoleFromUser is called when production revokeRoleFromUser is exited.
func (s *BaseStarRocksListener) ExitRevokeRoleFromUser(ctx *RevokeRoleFromUserContext) {}

// EnterRevokeRoleFromRole is called when production revokeRoleFromRole is entered.
func (s *BaseStarRocksListener) EnterRevokeRoleFromRole(ctx *RevokeRoleFromRoleContext) {}

// ExitRevokeRoleFromRole is called when production revokeRoleFromRole is exited.
func (s *BaseStarRocksListener) ExitRevokeRoleFromRole(ctx *RevokeRoleFromRoleContext) {}

// EnterSetRoleStatement is called when production setRoleStatement is entered.
func (s *BaseStarRocksListener) EnterSetRoleStatement(ctx *SetRoleStatementContext) {}

// ExitSetRoleStatement is called when production setRoleStatement is exited.
func (s *BaseStarRocksListener) ExitSetRoleStatement(ctx *SetRoleStatementContext) {}

// EnterSetDefaultRoleStatement is called when production setDefaultRoleStatement is entered.
func (s *BaseStarRocksListener) EnterSetDefaultRoleStatement(ctx *SetDefaultRoleStatementContext) {}

// ExitSetDefaultRoleStatement is called when production setDefaultRoleStatement is exited.
func (s *BaseStarRocksListener) ExitSetDefaultRoleStatement(ctx *SetDefaultRoleStatementContext) {}

// EnterGrantRevokeClause is called when production grantRevokeClause is entered.
func (s *BaseStarRocksListener) EnterGrantRevokeClause(ctx *GrantRevokeClauseContext) {}

// ExitGrantRevokeClause is called when production grantRevokeClause is exited.
func (s *BaseStarRocksListener) ExitGrantRevokeClause(ctx *GrantRevokeClauseContext) {}

// EnterGrantOnUser is called when production grantOnUser is entered.
func (s *BaseStarRocksListener) EnterGrantOnUser(ctx *GrantOnUserContext) {}

// ExitGrantOnUser is called when production grantOnUser is exited.
func (s *BaseStarRocksListener) ExitGrantOnUser(ctx *GrantOnUserContext) {}

// EnterGrantOnTableBrief is called when production grantOnTableBrief is entered.
func (s *BaseStarRocksListener) EnterGrantOnTableBrief(ctx *GrantOnTableBriefContext) {}

// ExitGrantOnTableBrief is called when production grantOnTableBrief is exited.
func (s *BaseStarRocksListener) ExitGrantOnTableBrief(ctx *GrantOnTableBriefContext) {}

// EnterGrantOnFunc is called when production grantOnFunc is entered.
func (s *BaseStarRocksListener) EnterGrantOnFunc(ctx *GrantOnFuncContext) {}

// ExitGrantOnFunc is called when production grantOnFunc is exited.
func (s *BaseStarRocksListener) ExitGrantOnFunc(ctx *GrantOnFuncContext) {}

// EnterGrantOnSystem is called when production grantOnSystem is entered.
func (s *BaseStarRocksListener) EnterGrantOnSystem(ctx *GrantOnSystemContext) {}

// ExitGrantOnSystem is called when production grantOnSystem is exited.
func (s *BaseStarRocksListener) ExitGrantOnSystem(ctx *GrantOnSystemContext) {}

// EnterGrantOnPrimaryObj is called when production grantOnPrimaryObj is entered.
func (s *BaseStarRocksListener) EnterGrantOnPrimaryObj(ctx *GrantOnPrimaryObjContext) {}

// ExitGrantOnPrimaryObj is called when production grantOnPrimaryObj is exited.
func (s *BaseStarRocksListener) ExitGrantOnPrimaryObj(ctx *GrantOnPrimaryObjContext) {}

// EnterGrantOnAll is called when production grantOnAll is entered.
func (s *BaseStarRocksListener) EnterGrantOnAll(ctx *GrantOnAllContext) {}

// ExitGrantOnAll is called when production grantOnAll is exited.
func (s *BaseStarRocksListener) ExitGrantOnAll(ctx *GrantOnAllContext) {}

// EnterRevokeOnUser is called when production revokeOnUser is entered.
func (s *BaseStarRocksListener) EnterRevokeOnUser(ctx *RevokeOnUserContext) {}

// ExitRevokeOnUser is called when production revokeOnUser is exited.
func (s *BaseStarRocksListener) ExitRevokeOnUser(ctx *RevokeOnUserContext) {}

// EnterRevokeOnTableBrief is called when production revokeOnTableBrief is entered.
func (s *BaseStarRocksListener) EnterRevokeOnTableBrief(ctx *RevokeOnTableBriefContext) {}

// ExitRevokeOnTableBrief is called when production revokeOnTableBrief is exited.
func (s *BaseStarRocksListener) ExitRevokeOnTableBrief(ctx *RevokeOnTableBriefContext) {}

// EnterRevokeOnFunc is called when production revokeOnFunc is entered.
func (s *BaseStarRocksListener) EnterRevokeOnFunc(ctx *RevokeOnFuncContext) {}

// ExitRevokeOnFunc is called when production revokeOnFunc is exited.
func (s *BaseStarRocksListener) ExitRevokeOnFunc(ctx *RevokeOnFuncContext) {}

// EnterRevokeOnSystem is called when production revokeOnSystem is entered.
func (s *BaseStarRocksListener) EnterRevokeOnSystem(ctx *RevokeOnSystemContext) {}

// ExitRevokeOnSystem is called when production revokeOnSystem is exited.
func (s *BaseStarRocksListener) ExitRevokeOnSystem(ctx *RevokeOnSystemContext) {}

// EnterRevokeOnPrimaryObj is called when production revokeOnPrimaryObj is entered.
func (s *BaseStarRocksListener) EnterRevokeOnPrimaryObj(ctx *RevokeOnPrimaryObjContext) {}

// ExitRevokeOnPrimaryObj is called when production revokeOnPrimaryObj is exited.
func (s *BaseStarRocksListener) ExitRevokeOnPrimaryObj(ctx *RevokeOnPrimaryObjContext) {}

// EnterRevokeOnAll is called when production revokeOnAll is entered.
func (s *BaseStarRocksListener) EnterRevokeOnAll(ctx *RevokeOnAllContext) {}

// ExitRevokeOnAll is called when production revokeOnAll is exited.
func (s *BaseStarRocksListener) ExitRevokeOnAll(ctx *RevokeOnAllContext) {}

// EnterShowGrantsStatement is called when production showGrantsStatement is entered.
func (s *BaseStarRocksListener) EnterShowGrantsStatement(ctx *ShowGrantsStatementContext) {}

// ExitShowGrantsStatement is called when production showGrantsStatement is exited.
func (s *BaseStarRocksListener) ExitShowGrantsStatement(ctx *ShowGrantsStatementContext) {}

// EnterAuthWithoutPlugin is called when production authWithoutPlugin is entered.
func (s *BaseStarRocksListener) EnterAuthWithoutPlugin(ctx *AuthWithoutPluginContext) {}

// ExitAuthWithoutPlugin is called when production authWithoutPlugin is exited.
func (s *BaseStarRocksListener) ExitAuthWithoutPlugin(ctx *AuthWithoutPluginContext) {}

// EnterAuthWithPlugin is called when production authWithPlugin is entered.
func (s *BaseStarRocksListener) EnterAuthWithPlugin(ctx *AuthWithPluginContext) {}

// ExitAuthWithPlugin is called when production authWithPlugin is exited.
func (s *BaseStarRocksListener) ExitAuthWithPlugin(ctx *AuthWithPluginContext) {}

// EnterPrivObjectName is called when production privObjectName is entered.
func (s *BaseStarRocksListener) EnterPrivObjectName(ctx *PrivObjectNameContext) {}

// ExitPrivObjectName is called when production privObjectName is exited.
func (s *BaseStarRocksListener) ExitPrivObjectName(ctx *PrivObjectNameContext) {}

// EnterPrivObjectNameList is called when production privObjectNameList is entered.
func (s *BaseStarRocksListener) EnterPrivObjectNameList(ctx *PrivObjectNameListContext) {}

// ExitPrivObjectNameList is called when production privObjectNameList is exited.
func (s *BaseStarRocksListener) ExitPrivObjectNameList(ctx *PrivObjectNameListContext) {}

// EnterPrivFunctionObjectNameList is called when production privFunctionObjectNameList is entered.
func (s *BaseStarRocksListener) EnterPrivFunctionObjectNameList(ctx *PrivFunctionObjectNameListContext) {
}

// ExitPrivFunctionObjectNameList is called when production privFunctionObjectNameList is exited.
func (s *BaseStarRocksListener) ExitPrivFunctionObjectNameList(ctx *PrivFunctionObjectNameListContext) {
}

// EnterPrivilegeTypeList is called when production privilegeTypeList is entered.
func (s *BaseStarRocksListener) EnterPrivilegeTypeList(ctx *PrivilegeTypeListContext) {}

// ExitPrivilegeTypeList is called when production privilegeTypeList is exited.
func (s *BaseStarRocksListener) ExitPrivilegeTypeList(ctx *PrivilegeTypeListContext) {}

// EnterPrivilegeType is called when production privilegeType is entered.
func (s *BaseStarRocksListener) EnterPrivilegeType(ctx *PrivilegeTypeContext) {}

// ExitPrivilegeType is called when production privilegeType is exited.
func (s *BaseStarRocksListener) ExitPrivilegeType(ctx *PrivilegeTypeContext) {}

// EnterPrivObjectType is called when production privObjectType is entered.
func (s *BaseStarRocksListener) EnterPrivObjectType(ctx *PrivObjectTypeContext) {}

// ExitPrivObjectType is called when production privObjectType is exited.
func (s *BaseStarRocksListener) ExitPrivObjectType(ctx *PrivObjectTypeContext) {}

// EnterPrivObjectTypePlural is called when production privObjectTypePlural is entered.
func (s *BaseStarRocksListener) EnterPrivObjectTypePlural(ctx *PrivObjectTypePluralContext) {}

// ExitPrivObjectTypePlural is called when production privObjectTypePlural is exited.
func (s *BaseStarRocksListener) ExitPrivObjectTypePlural(ctx *PrivObjectTypePluralContext) {}

// EnterCreateSecurityIntegrationStatement is called when production createSecurityIntegrationStatement is entered.
func (s *BaseStarRocksListener) EnterCreateSecurityIntegrationStatement(ctx *CreateSecurityIntegrationStatementContext) {
}

// ExitCreateSecurityIntegrationStatement is called when production createSecurityIntegrationStatement is exited.
func (s *BaseStarRocksListener) ExitCreateSecurityIntegrationStatement(ctx *CreateSecurityIntegrationStatementContext) {
}

// EnterAlterSecurityIntegrationStatement is called when production alterSecurityIntegrationStatement is entered.
func (s *BaseStarRocksListener) EnterAlterSecurityIntegrationStatement(ctx *AlterSecurityIntegrationStatementContext) {
}

// ExitAlterSecurityIntegrationStatement is called when production alterSecurityIntegrationStatement is exited.
func (s *BaseStarRocksListener) ExitAlterSecurityIntegrationStatement(ctx *AlterSecurityIntegrationStatementContext) {
}

// EnterDropSecurityIntegrationStatement is called when production dropSecurityIntegrationStatement is entered.
func (s *BaseStarRocksListener) EnterDropSecurityIntegrationStatement(ctx *DropSecurityIntegrationStatementContext) {
}

// ExitDropSecurityIntegrationStatement is called when production dropSecurityIntegrationStatement is exited.
func (s *BaseStarRocksListener) ExitDropSecurityIntegrationStatement(ctx *DropSecurityIntegrationStatementContext) {
}

// EnterShowSecurityIntegrationStatement is called when production showSecurityIntegrationStatement is entered.
func (s *BaseStarRocksListener) EnterShowSecurityIntegrationStatement(ctx *ShowSecurityIntegrationStatementContext) {
}

// ExitShowSecurityIntegrationStatement is called when production showSecurityIntegrationStatement is exited.
func (s *BaseStarRocksListener) ExitShowSecurityIntegrationStatement(ctx *ShowSecurityIntegrationStatementContext) {
}

// EnterShowCreateSecurityIntegrationStatement is called when production showCreateSecurityIntegrationStatement is entered.
func (s *BaseStarRocksListener) EnterShowCreateSecurityIntegrationStatement(ctx *ShowCreateSecurityIntegrationStatementContext) {
}

// ExitShowCreateSecurityIntegrationStatement is called when production showCreateSecurityIntegrationStatement is exited.
func (s *BaseStarRocksListener) ExitShowCreateSecurityIntegrationStatement(ctx *ShowCreateSecurityIntegrationStatementContext) {
}

// EnterCreateGroupProviderStatement is called when production createGroupProviderStatement is entered.
func (s *BaseStarRocksListener) EnterCreateGroupProviderStatement(ctx *CreateGroupProviderStatementContext) {
}

// ExitCreateGroupProviderStatement is called when production createGroupProviderStatement is exited.
func (s *BaseStarRocksListener) ExitCreateGroupProviderStatement(ctx *CreateGroupProviderStatementContext) {
}

// EnterDropGroupProviderStatement is called when production dropGroupProviderStatement is entered.
func (s *BaseStarRocksListener) EnterDropGroupProviderStatement(ctx *DropGroupProviderStatementContext) {
}

// ExitDropGroupProviderStatement is called when production dropGroupProviderStatement is exited.
func (s *BaseStarRocksListener) ExitDropGroupProviderStatement(ctx *DropGroupProviderStatementContext) {
}

// EnterShowGroupProvidersStatement is called when production showGroupProvidersStatement is entered.
func (s *BaseStarRocksListener) EnterShowGroupProvidersStatement(ctx *ShowGroupProvidersStatementContext) {
}

// ExitShowGroupProvidersStatement is called when production showGroupProvidersStatement is exited.
func (s *BaseStarRocksListener) ExitShowGroupProvidersStatement(ctx *ShowGroupProvidersStatementContext) {
}

// EnterShowCreateGroupProviderStatement is called when production showCreateGroupProviderStatement is entered.
func (s *BaseStarRocksListener) EnterShowCreateGroupProviderStatement(ctx *ShowCreateGroupProviderStatementContext) {
}

// ExitShowCreateGroupProviderStatement is called when production showCreateGroupProviderStatement is exited.
func (s *BaseStarRocksListener) ExitShowCreateGroupProviderStatement(ctx *ShowCreateGroupProviderStatementContext) {
}

// EnterBackupStatement is called when production backupStatement is entered.
func (s *BaseStarRocksListener) EnterBackupStatement(ctx *BackupStatementContext) {}

// ExitBackupStatement is called when production backupStatement is exited.
func (s *BaseStarRocksListener) ExitBackupStatement(ctx *BackupStatementContext) {}

// EnterCancelBackupStatement is called when production cancelBackupStatement is entered.
func (s *BaseStarRocksListener) EnterCancelBackupStatement(ctx *CancelBackupStatementContext) {}

// ExitCancelBackupStatement is called when production cancelBackupStatement is exited.
func (s *BaseStarRocksListener) ExitCancelBackupStatement(ctx *CancelBackupStatementContext) {}

// EnterShowBackupStatement is called when production showBackupStatement is entered.
func (s *BaseStarRocksListener) EnterShowBackupStatement(ctx *ShowBackupStatementContext) {}

// ExitShowBackupStatement is called when production showBackupStatement is exited.
func (s *BaseStarRocksListener) ExitShowBackupStatement(ctx *ShowBackupStatementContext) {}

// EnterRestoreStatement is called when production restoreStatement is entered.
func (s *BaseStarRocksListener) EnterRestoreStatement(ctx *RestoreStatementContext) {}

// ExitRestoreStatement is called when production restoreStatement is exited.
func (s *BaseStarRocksListener) ExitRestoreStatement(ctx *RestoreStatementContext) {}

// EnterCancelRestoreStatement is called when production cancelRestoreStatement is entered.
func (s *BaseStarRocksListener) EnterCancelRestoreStatement(ctx *CancelRestoreStatementContext) {}

// ExitCancelRestoreStatement is called when production cancelRestoreStatement is exited.
func (s *BaseStarRocksListener) ExitCancelRestoreStatement(ctx *CancelRestoreStatementContext) {}

// EnterShowRestoreStatement is called when production showRestoreStatement is entered.
func (s *BaseStarRocksListener) EnterShowRestoreStatement(ctx *ShowRestoreStatementContext) {}

// ExitShowRestoreStatement is called when production showRestoreStatement is exited.
func (s *BaseStarRocksListener) ExitShowRestoreStatement(ctx *ShowRestoreStatementContext) {}

// EnterShowSnapshotStatement is called when production showSnapshotStatement is entered.
func (s *BaseStarRocksListener) EnterShowSnapshotStatement(ctx *ShowSnapshotStatementContext) {}

// ExitShowSnapshotStatement is called when production showSnapshotStatement is exited.
func (s *BaseStarRocksListener) ExitShowSnapshotStatement(ctx *ShowSnapshotStatementContext) {}

// EnterCreateRepositoryStatement is called when production createRepositoryStatement is entered.
func (s *BaseStarRocksListener) EnterCreateRepositoryStatement(ctx *CreateRepositoryStatementContext) {
}

// ExitCreateRepositoryStatement is called when production createRepositoryStatement is exited.
func (s *BaseStarRocksListener) ExitCreateRepositoryStatement(ctx *CreateRepositoryStatementContext) {
}

// EnterDropRepositoryStatement is called when production dropRepositoryStatement is entered.
func (s *BaseStarRocksListener) EnterDropRepositoryStatement(ctx *DropRepositoryStatementContext) {}

// ExitDropRepositoryStatement is called when production dropRepositoryStatement is exited.
func (s *BaseStarRocksListener) ExitDropRepositoryStatement(ctx *DropRepositoryStatementContext) {}

// EnterAddSqlBlackListStatement is called when production addSqlBlackListStatement is entered.
func (s *BaseStarRocksListener) EnterAddSqlBlackListStatement(ctx *AddSqlBlackListStatementContext) {}

// ExitAddSqlBlackListStatement is called when production addSqlBlackListStatement is exited.
func (s *BaseStarRocksListener) ExitAddSqlBlackListStatement(ctx *AddSqlBlackListStatementContext) {}

// EnterDelSqlBlackListStatement is called when production delSqlBlackListStatement is entered.
func (s *BaseStarRocksListener) EnterDelSqlBlackListStatement(ctx *DelSqlBlackListStatementContext) {}

// ExitDelSqlBlackListStatement is called when production delSqlBlackListStatement is exited.
func (s *BaseStarRocksListener) ExitDelSqlBlackListStatement(ctx *DelSqlBlackListStatementContext) {}

// EnterShowSqlBlackListStatement is called when production showSqlBlackListStatement is entered.
func (s *BaseStarRocksListener) EnterShowSqlBlackListStatement(ctx *ShowSqlBlackListStatementContext) {
}

// ExitShowSqlBlackListStatement is called when production showSqlBlackListStatement is exited.
func (s *BaseStarRocksListener) ExitShowSqlBlackListStatement(ctx *ShowSqlBlackListStatementContext) {
}

// EnterShowWhiteListStatement is called when production showWhiteListStatement is entered.
func (s *BaseStarRocksListener) EnterShowWhiteListStatement(ctx *ShowWhiteListStatementContext) {}

// ExitShowWhiteListStatement is called when production showWhiteListStatement is exited.
func (s *BaseStarRocksListener) ExitShowWhiteListStatement(ctx *ShowWhiteListStatementContext) {}

// EnterAddBackendBlackListStatement is called when production addBackendBlackListStatement is entered.
func (s *BaseStarRocksListener) EnterAddBackendBlackListStatement(ctx *AddBackendBlackListStatementContext) {
}

// ExitAddBackendBlackListStatement is called when production addBackendBlackListStatement is exited.
func (s *BaseStarRocksListener) ExitAddBackendBlackListStatement(ctx *AddBackendBlackListStatementContext) {
}

// EnterDelBackendBlackListStatement is called when production delBackendBlackListStatement is entered.
func (s *BaseStarRocksListener) EnterDelBackendBlackListStatement(ctx *DelBackendBlackListStatementContext) {
}

// ExitDelBackendBlackListStatement is called when production delBackendBlackListStatement is exited.
func (s *BaseStarRocksListener) ExitDelBackendBlackListStatement(ctx *DelBackendBlackListStatementContext) {
}

// EnterShowBackendBlackListStatement is called when production showBackendBlackListStatement is entered.
func (s *BaseStarRocksListener) EnterShowBackendBlackListStatement(ctx *ShowBackendBlackListStatementContext) {
}

// ExitShowBackendBlackListStatement is called when production showBackendBlackListStatement is exited.
func (s *BaseStarRocksListener) ExitShowBackendBlackListStatement(ctx *ShowBackendBlackListStatementContext) {
}

// EnterDataCacheTarget is called when production dataCacheTarget is entered.
func (s *BaseStarRocksListener) EnterDataCacheTarget(ctx *DataCacheTargetContext) {}

// ExitDataCacheTarget is called when production dataCacheTarget is exited.
func (s *BaseStarRocksListener) ExitDataCacheTarget(ctx *DataCacheTargetContext) {}

// EnterCreateDataCacheRuleStatement is called when production createDataCacheRuleStatement is entered.
func (s *BaseStarRocksListener) EnterCreateDataCacheRuleStatement(ctx *CreateDataCacheRuleStatementContext) {
}

// ExitCreateDataCacheRuleStatement is called when production createDataCacheRuleStatement is exited.
func (s *BaseStarRocksListener) ExitCreateDataCacheRuleStatement(ctx *CreateDataCacheRuleStatementContext) {
}

// EnterShowDataCacheRulesStatement is called when production showDataCacheRulesStatement is entered.
func (s *BaseStarRocksListener) EnterShowDataCacheRulesStatement(ctx *ShowDataCacheRulesStatementContext) {
}

// ExitShowDataCacheRulesStatement is called when production showDataCacheRulesStatement is exited.
func (s *BaseStarRocksListener) ExitShowDataCacheRulesStatement(ctx *ShowDataCacheRulesStatementContext) {
}

// EnterDropDataCacheRuleStatement is called when production dropDataCacheRuleStatement is entered.
func (s *BaseStarRocksListener) EnterDropDataCacheRuleStatement(ctx *DropDataCacheRuleStatementContext) {
}

// ExitDropDataCacheRuleStatement is called when production dropDataCacheRuleStatement is exited.
func (s *BaseStarRocksListener) ExitDropDataCacheRuleStatement(ctx *DropDataCacheRuleStatementContext) {
}

// EnterClearDataCacheRulesStatement is called when production clearDataCacheRulesStatement is entered.
func (s *BaseStarRocksListener) EnterClearDataCacheRulesStatement(ctx *ClearDataCacheRulesStatementContext) {
}

// ExitClearDataCacheRulesStatement is called when production clearDataCacheRulesStatement is exited.
func (s *BaseStarRocksListener) ExitClearDataCacheRulesStatement(ctx *ClearDataCacheRulesStatementContext) {
}

// EnterDataCacheSelectStatement is called when production dataCacheSelectStatement is entered.
func (s *BaseStarRocksListener) EnterDataCacheSelectStatement(ctx *DataCacheSelectStatementContext) {}

// ExitDataCacheSelectStatement is called when production dataCacheSelectStatement is exited.
func (s *BaseStarRocksListener) ExitDataCacheSelectStatement(ctx *DataCacheSelectStatementContext) {}

// EnterExportStatement is called when production exportStatement is entered.
func (s *BaseStarRocksListener) EnterExportStatement(ctx *ExportStatementContext) {}

// ExitExportStatement is called when production exportStatement is exited.
func (s *BaseStarRocksListener) ExitExportStatement(ctx *ExportStatementContext) {}

// EnterCancelExportStatement is called when production cancelExportStatement is entered.
func (s *BaseStarRocksListener) EnterCancelExportStatement(ctx *CancelExportStatementContext) {}

// ExitCancelExportStatement is called when production cancelExportStatement is exited.
func (s *BaseStarRocksListener) ExitCancelExportStatement(ctx *CancelExportStatementContext) {}

// EnterShowExportStatement is called when production showExportStatement is entered.
func (s *BaseStarRocksListener) EnterShowExportStatement(ctx *ShowExportStatementContext) {}

// ExitShowExportStatement is called when production showExportStatement is exited.
func (s *BaseStarRocksListener) ExitShowExportStatement(ctx *ShowExportStatementContext) {}

// EnterInstallPluginStatement is called when production installPluginStatement is entered.
func (s *BaseStarRocksListener) EnterInstallPluginStatement(ctx *InstallPluginStatementContext) {}

// ExitInstallPluginStatement is called when production installPluginStatement is exited.
func (s *BaseStarRocksListener) ExitInstallPluginStatement(ctx *InstallPluginStatementContext) {}

// EnterUninstallPluginStatement is called when production uninstallPluginStatement is entered.
func (s *BaseStarRocksListener) EnterUninstallPluginStatement(ctx *UninstallPluginStatementContext) {}

// ExitUninstallPluginStatement is called when production uninstallPluginStatement is exited.
func (s *BaseStarRocksListener) ExitUninstallPluginStatement(ctx *UninstallPluginStatementContext) {}

// EnterCreateFileStatement is called when production createFileStatement is entered.
func (s *BaseStarRocksListener) EnterCreateFileStatement(ctx *CreateFileStatementContext) {}

// ExitCreateFileStatement is called when production createFileStatement is exited.
func (s *BaseStarRocksListener) ExitCreateFileStatement(ctx *CreateFileStatementContext) {}

// EnterDropFileStatement is called when production dropFileStatement is entered.
func (s *BaseStarRocksListener) EnterDropFileStatement(ctx *DropFileStatementContext) {}

// ExitDropFileStatement is called when production dropFileStatement is exited.
func (s *BaseStarRocksListener) ExitDropFileStatement(ctx *DropFileStatementContext) {}

// EnterShowSmallFilesStatement is called when production showSmallFilesStatement is entered.
func (s *BaseStarRocksListener) EnterShowSmallFilesStatement(ctx *ShowSmallFilesStatementContext) {}

// ExitShowSmallFilesStatement is called when production showSmallFilesStatement is exited.
func (s *BaseStarRocksListener) ExitShowSmallFilesStatement(ctx *ShowSmallFilesStatementContext) {}

// EnterCreatePipeStatement is called when production createPipeStatement is entered.
func (s *BaseStarRocksListener) EnterCreatePipeStatement(ctx *CreatePipeStatementContext) {}

// ExitCreatePipeStatement is called when production createPipeStatement is exited.
func (s *BaseStarRocksListener) ExitCreatePipeStatement(ctx *CreatePipeStatementContext) {}

// EnterDropPipeStatement is called when production dropPipeStatement is entered.
func (s *BaseStarRocksListener) EnterDropPipeStatement(ctx *DropPipeStatementContext) {}

// ExitDropPipeStatement is called when production dropPipeStatement is exited.
func (s *BaseStarRocksListener) ExitDropPipeStatement(ctx *DropPipeStatementContext) {}

// EnterAlterPipeClause is called when production alterPipeClause is entered.
func (s *BaseStarRocksListener) EnterAlterPipeClause(ctx *AlterPipeClauseContext) {}

// ExitAlterPipeClause is called when production alterPipeClause is exited.
func (s *BaseStarRocksListener) ExitAlterPipeClause(ctx *AlterPipeClauseContext) {}

// EnterAlterPipeStatement is called when production alterPipeStatement is entered.
func (s *BaseStarRocksListener) EnterAlterPipeStatement(ctx *AlterPipeStatementContext) {}

// ExitAlterPipeStatement is called when production alterPipeStatement is exited.
func (s *BaseStarRocksListener) ExitAlterPipeStatement(ctx *AlterPipeStatementContext) {}

// EnterDescPipeStatement is called when production descPipeStatement is entered.
func (s *BaseStarRocksListener) EnterDescPipeStatement(ctx *DescPipeStatementContext) {}

// ExitDescPipeStatement is called when production descPipeStatement is exited.
func (s *BaseStarRocksListener) ExitDescPipeStatement(ctx *DescPipeStatementContext) {}

// EnterShowPipeStatement is called when production showPipeStatement is entered.
func (s *BaseStarRocksListener) EnterShowPipeStatement(ctx *ShowPipeStatementContext) {}

// ExitShowPipeStatement is called when production showPipeStatement is exited.
func (s *BaseStarRocksListener) ExitShowPipeStatement(ctx *ShowPipeStatementContext) {}

// EnterSetStatement is called when production setStatement is entered.
func (s *BaseStarRocksListener) EnterSetStatement(ctx *SetStatementContext) {}

// ExitSetStatement is called when production setStatement is exited.
func (s *BaseStarRocksListener) ExitSetStatement(ctx *SetStatementContext) {}

// EnterSetNames is called when production setNames is entered.
func (s *BaseStarRocksListener) EnterSetNames(ctx *SetNamesContext) {}

// ExitSetNames is called when production setNames is exited.
func (s *BaseStarRocksListener) ExitSetNames(ctx *SetNamesContext) {}

// EnterSetPassword is called when production setPassword is entered.
func (s *BaseStarRocksListener) EnterSetPassword(ctx *SetPasswordContext) {}

// ExitSetPassword is called when production setPassword is exited.
func (s *BaseStarRocksListener) ExitSetPassword(ctx *SetPasswordContext) {}

// EnterSetUserVar is called when production setUserVar is entered.
func (s *BaseStarRocksListener) EnterSetUserVar(ctx *SetUserVarContext) {}

// ExitSetUserVar is called when production setUserVar is exited.
func (s *BaseStarRocksListener) ExitSetUserVar(ctx *SetUserVarContext) {}

// EnterSetSystemVar is called when production setSystemVar is entered.
func (s *BaseStarRocksListener) EnterSetSystemVar(ctx *SetSystemVarContext) {}

// ExitSetSystemVar is called when production setSystemVar is exited.
func (s *BaseStarRocksListener) ExitSetSystemVar(ctx *SetSystemVarContext) {}

// EnterSetTransaction is called when production setTransaction is entered.
func (s *BaseStarRocksListener) EnterSetTransaction(ctx *SetTransactionContext) {}

// ExitSetTransaction is called when production setTransaction is exited.
func (s *BaseStarRocksListener) ExitSetTransaction(ctx *SetTransactionContext) {}

// EnterTransaction_characteristics is called when production transaction_characteristics is entered.
func (s *BaseStarRocksListener) EnterTransaction_characteristics(ctx *Transaction_characteristicsContext) {
}

// ExitTransaction_characteristics is called when production transaction_characteristics is exited.
func (s *BaseStarRocksListener) ExitTransaction_characteristics(ctx *Transaction_characteristicsContext) {
}

// EnterTransaction_access_mode is called when production transaction_access_mode is entered.
func (s *BaseStarRocksListener) EnterTransaction_access_mode(ctx *Transaction_access_modeContext) {}

// ExitTransaction_access_mode is called when production transaction_access_mode is exited.
func (s *BaseStarRocksListener) ExitTransaction_access_mode(ctx *Transaction_access_modeContext) {}

// EnterIsolation_level is called when production isolation_level is entered.
func (s *BaseStarRocksListener) EnterIsolation_level(ctx *Isolation_levelContext) {}

// ExitIsolation_level is called when production isolation_level is exited.
func (s *BaseStarRocksListener) ExitIsolation_level(ctx *Isolation_levelContext) {}

// EnterIsolation_types is called when production isolation_types is entered.
func (s *BaseStarRocksListener) EnterIsolation_types(ctx *Isolation_typesContext) {}

// ExitIsolation_types is called when production isolation_types is exited.
func (s *BaseStarRocksListener) ExitIsolation_types(ctx *Isolation_typesContext) {}

// EnterSetExprOrDefault is called when production setExprOrDefault is entered.
func (s *BaseStarRocksListener) EnterSetExprOrDefault(ctx *SetExprOrDefaultContext) {}

// ExitSetExprOrDefault is called when production setExprOrDefault is exited.
func (s *BaseStarRocksListener) ExitSetExprOrDefault(ctx *SetExprOrDefaultContext) {}

// EnterSetUserPropertyStatement is called when production setUserPropertyStatement is entered.
func (s *BaseStarRocksListener) EnterSetUserPropertyStatement(ctx *SetUserPropertyStatementContext) {}

// ExitSetUserPropertyStatement is called when production setUserPropertyStatement is exited.
func (s *BaseStarRocksListener) ExitSetUserPropertyStatement(ctx *SetUserPropertyStatementContext) {}

// EnterRoleList is called when production roleList is entered.
func (s *BaseStarRocksListener) EnterRoleList(ctx *RoleListContext) {}

// ExitRoleList is called when production roleList is exited.
func (s *BaseStarRocksListener) ExitRoleList(ctx *RoleListContext) {}

// EnterExecuteScriptStatement is called when production executeScriptStatement is entered.
func (s *BaseStarRocksListener) EnterExecuteScriptStatement(ctx *ExecuteScriptStatementContext) {}

// ExitExecuteScriptStatement is called when production executeScriptStatement is exited.
func (s *BaseStarRocksListener) ExitExecuteScriptStatement(ctx *ExecuteScriptStatementContext) {}

// EnterUnsupportedStatement is called when production unsupportedStatement is entered.
func (s *BaseStarRocksListener) EnterUnsupportedStatement(ctx *UnsupportedStatementContext) {}

// ExitUnsupportedStatement is called when production unsupportedStatement is exited.
func (s *BaseStarRocksListener) ExitUnsupportedStatement(ctx *UnsupportedStatementContext) {}

// EnterLock_item is called when production lock_item is entered.
func (s *BaseStarRocksListener) EnterLock_item(ctx *Lock_itemContext) {}

// ExitLock_item is called when production lock_item is exited.
func (s *BaseStarRocksListener) ExitLock_item(ctx *Lock_itemContext) {}

// EnterLock_type is called when production lock_type is entered.
func (s *BaseStarRocksListener) EnterLock_type(ctx *Lock_typeContext) {}

// ExitLock_type is called when production lock_type is exited.
func (s *BaseStarRocksListener) ExitLock_type(ctx *Lock_typeContext) {}

// EnterAlterPlanAdvisorAddStatement is called when production alterPlanAdvisorAddStatement is entered.
func (s *BaseStarRocksListener) EnterAlterPlanAdvisorAddStatement(ctx *AlterPlanAdvisorAddStatementContext) {
}

// ExitAlterPlanAdvisorAddStatement is called when production alterPlanAdvisorAddStatement is exited.
func (s *BaseStarRocksListener) ExitAlterPlanAdvisorAddStatement(ctx *AlterPlanAdvisorAddStatementContext) {
}

// EnterTruncatePlanAdvisorStatement is called when production truncatePlanAdvisorStatement is entered.
func (s *BaseStarRocksListener) EnterTruncatePlanAdvisorStatement(ctx *TruncatePlanAdvisorStatementContext) {
}

// ExitTruncatePlanAdvisorStatement is called when production truncatePlanAdvisorStatement is exited.
func (s *BaseStarRocksListener) ExitTruncatePlanAdvisorStatement(ctx *TruncatePlanAdvisorStatementContext) {
}

// EnterAlterPlanAdvisorDropStatement is called when production alterPlanAdvisorDropStatement is entered.
func (s *BaseStarRocksListener) EnterAlterPlanAdvisorDropStatement(ctx *AlterPlanAdvisorDropStatementContext) {
}

// ExitAlterPlanAdvisorDropStatement is called when production alterPlanAdvisorDropStatement is exited.
func (s *BaseStarRocksListener) ExitAlterPlanAdvisorDropStatement(ctx *AlterPlanAdvisorDropStatementContext) {
}

// EnterShowPlanAdvisorStatement is called when production showPlanAdvisorStatement is entered.
func (s *BaseStarRocksListener) EnterShowPlanAdvisorStatement(ctx *ShowPlanAdvisorStatementContext) {}

// ExitShowPlanAdvisorStatement is called when production showPlanAdvisorStatement is exited.
func (s *BaseStarRocksListener) ExitShowPlanAdvisorStatement(ctx *ShowPlanAdvisorStatementContext) {}

// EnterCreateWarehouseStatement is called when production createWarehouseStatement is entered.
func (s *BaseStarRocksListener) EnterCreateWarehouseStatement(ctx *CreateWarehouseStatementContext) {}

// ExitCreateWarehouseStatement is called when production createWarehouseStatement is exited.
func (s *BaseStarRocksListener) ExitCreateWarehouseStatement(ctx *CreateWarehouseStatementContext) {}

// EnterDropWarehouseStatement is called when production dropWarehouseStatement is entered.
func (s *BaseStarRocksListener) EnterDropWarehouseStatement(ctx *DropWarehouseStatementContext) {}

// ExitDropWarehouseStatement is called when production dropWarehouseStatement is exited.
func (s *BaseStarRocksListener) ExitDropWarehouseStatement(ctx *DropWarehouseStatementContext) {}

// EnterSuspendWarehouseStatement is called when production suspendWarehouseStatement is entered.
func (s *BaseStarRocksListener) EnterSuspendWarehouseStatement(ctx *SuspendWarehouseStatementContext) {
}

// ExitSuspendWarehouseStatement is called when production suspendWarehouseStatement is exited.
func (s *BaseStarRocksListener) ExitSuspendWarehouseStatement(ctx *SuspendWarehouseStatementContext) {
}

// EnterResumeWarehouseStatement is called when production resumeWarehouseStatement is entered.
func (s *BaseStarRocksListener) EnterResumeWarehouseStatement(ctx *ResumeWarehouseStatementContext) {}

// ExitResumeWarehouseStatement is called when production resumeWarehouseStatement is exited.
func (s *BaseStarRocksListener) ExitResumeWarehouseStatement(ctx *ResumeWarehouseStatementContext) {}

// EnterSetWarehouseStatement is called when production setWarehouseStatement is entered.
func (s *BaseStarRocksListener) EnterSetWarehouseStatement(ctx *SetWarehouseStatementContext) {}

// ExitSetWarehouseStatement is called when production setWarehouseStatement is exited.
func (s *BaseStarRocksListener) ExitSetWarehouseStatement(ctx *SetWarehouseStatementContext) {}

// EnterShowWarehousesStatement is called when production showWarehousesStatement is entered.
func (s *BaseStarRocksListener) EnterShowWarehousesStatement(ctx *ShowWarehousesStatementContext) {}

// ExitShowWarehousesStatement is called when production showWarehousesStatement is exited.
func (s *BaseStarRocksListener) ExitShowWarehousesStatement(ctx *ShowWarehousesStatementContext) {}

// EnterShowClustersStatement is called when production showClustersStatement is entered.
func (s *BaseStarRocksListener) EnterShowClustersStatement(ctx *ShowClustersStatementContext) {}

// ExitShowClustersStatement is called when production showClustersStatement is exited.
func (s *BaseStarRocksListener) ExitShowClustersStatement(ctx *ShowClustersStatementContext) {}

// EnterShowNodesStatement is called when production showNodesStatement is entered.
func (s *BaseStarRocksListener) EnterShowNodesStatement(ctx *ShowNodesStatementContext) {}

// ExitShowNodesStatement is called when production showNodesStatement is exited.
func (s *BaseStarRocksListener) ExitShowNodesStatement(ctx *ShowNodesStatementContext) {}

// EnterAlterWarehouseStatement is called when production alterWarehouseStatement is entered.
func (s *BaseStarRocksListener) EnterAlterWarehouseStatement(ctx *AlterWarehouseStatementContext) {}

// ExitAlterWarehouseStatement is called when production alterWarehouseStatement is exited.
func (s *BaseStarRocksListener) ExitAlterWarehouseStatement(ctx *AlterWarehouseStatementContext) {}

// EnterBeginStatement is called when production beginStatement is entered.
func (s *BaseStarRocksListener) EnterBeginStatement(ctx *BeginStatementContext) {}

// ExitBeginStatement is called when production beginStatement is exited.
func (s *BaseStarRocksListener) ExitBeginStatement(ctx *BeginStatementContext) {}

// EnterCommitStatement is called when production commitStatement is entered.
func (s *BaseStarRocksListener) EnterCommitStatement(ctx *CommitStatementContext) {}

// ExitCommitStatement is called when production commitStatement is exited.
func (s *BaseStarRocksListener) ExitCommitStatement(ctx *CommitStatementContext) {}

// EnterRollbackStatement is called when production rollbackStatement is entered.
func (s *BaseStarRocksListener) EnterRollbackStatement(ctx *RollbackStatementContext) {}

// ExitRollbackStatement is called when production rollbackStatement is exited.
func (s *BaseStarRocksListener) ExitRollbackStatement(ctx *RollbackStatementContext) {}

// EnterTranslateStatement is called when production translateStatement is entered.
func (s *BaseStarRocksListener) EnterTranslateStatement(ctx *TranslateStatementContext) {}

// ExitTranslateStatement is called when production translateStatement is exited.
func (s *BaseStarRocksListener) ExitTranslateStatement(ctx *TranslateStatementContext) {}

// EnterDialect is called when production dialect is entered.
func (s *BaseStarRocksListener) EnterDialect(ctx *DialectContext) {}

// ExitDialect is called when production dialect is exited.
func (s *BaseStarRocksListener) ExitDialect(ctx *DialectContext) {}

// EnterTranslateSQL is called when production translateSQL is entered.
func (s *BaseStarRocksListener) EnterTranslateSQL(ctx *TranslateSQLContext) {}

// ExitTranslateSQL is called when production translateSQL is exited.
func (s *BaseStarRocksListener) ExitTranslateSQL(ctx *TranslateSQLContext) {}

// EnterQueryStatement is called when production queryStatement is entered.
func (s *BaseStarRocksListener) EnterQueryStatement(ctx *QueryStatementContext) {}

// ExitQueryStatement is called when production queryStatement is exited.
func (s *BaseStarRocksListener) ExitQueryStatement(ctx *QueryStatementContext) {}

// EnterQueryRelation is called when production queryRelation is entered.
func (s *BaseStarRocksListener) EnterQueryRelation(ctx *QueryRelationContext) {}

// ExitQueryRelation is called when production queryRelation is exited.
func (s *BaseStarRocksListener) ExitQueryRelation(ctx *QueryRelationContext) {}

// EnterWithClause is called when production withClause is entered.
func (s *BaseStarRocksListener) EnterWithClause(ctx *WithClauseContext) {}

// ExitWithClause is called when production withClause is exited.
func (s *BaseStarRocksListener) ExitWithClause(ctx *WithClauseContext) {}

// EnterQueryNoWith is called when production queryNoWith is entered.
func (s *BaseStarRocksListener) EnterQueryNoWith(ctx *QueryNoWithContext) {}

// ExitQueryNoWith is called when production queryNoWith is exited.
func (s *BaseStarRocksListener) ExitQueryNoWith(ctx *QueryNoWithContext) {}

// EnterQueryPeriod is called when production queryPeriod is entered.
func (s *BaseStarRocksListener) EnterQueryPeriod(ctx *QueryPeriodContext) {}

// ExitQueryPeriod is called when production queryPeriod is exited.
func (s *BaseStarRocksListener) ExitQueryPeriod(ctx *QueryPeriodContext) {}

// EnterPeriodType is called when production periodType is entered.
func (s *BaseStarRocksListener) EnterPeriodType(ctx *PeriodTypeContext) {}

// ExitPeriodType is called when production periodType is exited.
func (s *BaseStarRocksListener) ExitPeriodType(ctx *PeriodTypeContext) {}

// EnterQueryWithParentheses is called when production queryWithParentheses is entered.
func (s *BaseStarRocksListener) EnterQueryWithParentheses(ctx *QueryWithParenthesesContext) {}

// ExitQueryWithParentheses is called when production queryWithParentheses is exited.
func (s *BaseStarRocksListener) ExitQueryWithParentheses(ctx *QueryWithParenthesesContext) {}

// EnterSetOperation is called when production setOperation is entered.
func (s *BaseStarRocksListener) EnterSetOperation(ctx *SetOperationContext) {}

// ExitSetOperation is called when production setOperation is exited.
func (s *BaseStarRocksListener) ExitSetOperation(ctx *SetOperationContext) {}

// EnterQueryPrimaryDefault is called when production queryPrimaryDefault is entered.
func (s *BaseStarRocksListener) EnterQueryPrimaryDefault(ctx *QueryPrimaryDefaultContext) {}

// ExitQueryPrimaryDefault is called when production queryPrimaryDefault is exited.
func (s *BaseStarRocksListener) ExitQueryPrimaryDefault(ctx *QueryPrimaryDefaultContext) {}

// EnterSubquery is called when production subquery is entered.
func (s *BaseStarRocksListener) EnterSubquery(ctx *SubqueryContext) {}

// ExitSubquery is called when production subquery is exited.
func (s *BaseStarRocksListener) ExitSubquery(ctx *SubqueryContext) {}

// EnterRowConstructor is called when production rowConstructor is entered.
func (s *BaseStarRocksListener) EnterRowConstructor(ctx *RowConstructorContext) {}

// ExitRowConstructor is called when production rowConstructor is exited.
func (s *BaseStarRocksListener) ExitRowConstructor(ctx *RowConstructorContext) {}

// EnterSortItem is called when production sortItem is entered.
func (s *BaseStarRocksListener) EnterSortItem(ctx *SortItemContext) {}

// ExitSortItem is called when production sortItem is exited.
func (s *BaseStarRocksListener) ExitSortItem(ctx *SortItemContext) {}

// EnterLimitConstExpr is called when production limitConstExpr is entered.
func (s *BaseStarRocksListener) EnterLimitConstExpr(ctx *LimitConstExprContext) {}

// ExitLimitConstExpr is called when production limitConstExpr is exited.
func (s *BaseStarRocksListener) ExitLimitConstExpr(ctx *LimitConstExprContext) {}

// EnterLimitElement is called when production limitElement is entered.
func (s *BaseStarRocksListener) EnterLimitElement(ctx *LimitElementContext) {}

// ExitLimitElement is called when production limitElement is exited.
func (s *BaseStarRocksListener) ExitLimitElement(ctx *LimitElementContext) {}

// EnterQuerySpecification is called when production querySpecification is entered.
func (s *BaseStarRocksListener) EnterQuerySpecification(ctx *QuerySpecificationContext) {}

// ExitQuerySpecification is called when production querySpecification is exited.
func (s *BaseStarRocksListener) ExitQuerySpecification(ctx *QuerySpecificationContext) {}

// EnterFrom is called when production from is entered.
func (s *BaseStarRocksListener) EnterFrom(ctx *FromContext) {}

// ExitFrom is called when production from is exited.
func (s *BaseStarRocksListener) ExitFrom(ctx *FromContext) {}

// EnterDual is called when production dual is entered.
func (s *BaseStarRocksListener) EnterDual(ctx *DualContext) {}

// ExitDual is called when production dual is exited.
func (s *BaseStarRocksListener) ExitDual(ctx *DualContext) {}

// EnterRollup is called when production rollup is entered.
func (s *BaseStarRocksListener) EnterRollup(ctx *RollupContext) {}

// ExitRollup is called when production rollup is exited.
func (s *BaseStarRocksListener) ExitRollup(ctx *RollupContext) {}

// EnterCube is called when production cube is entered.
func (s *BaseStarRocksListener) EnterCube(ctx *CubeContext) {}

// ExitCube is called when production cube is exited.
func (s *BaseStarRocksListener) ExitCube(ctx *CubeContext) {}

// EnterMultipleGroupingSets is called when production multipleGroupingSets is entered.
func (s *BaseStarRocksListener) EnterMultipleGroupingSets(ctx *MultipleGroupingSetsContext) {}

// ExitMultipleGroupingSets is called when production multipleGroupingSets is exited.
func (s *BaseStarRocksListener) ExitMultipleGroupingSets(ctx *MultipleGroupingSetsContext) {}

// EnterSingleGroupingSet is called when production singleGroupingSet is entered.
func (s *BaseStarRocksListener) EnterSingleGroupingSet(ctx *SingleGroupingSetContext) {}

// ExitSingleGroupingSet is called when production singleGroupingSet is exited.
func (s *BaseStarRocksListener) ExitSingleGroupingSet(ctx *SingleGroupingSetContext) {}

// EnterGroupingSet is called when production groupingSet is entered.
func (s *BaseStarRocksListener) EnterGroupingSet(ctx *GroupingSetContext) {}

// ExitGroupingSet is called when production groupingSet is exited.
func (s *BaseStarRocksListener) ExitGroupingSet(ctx *GroupingSetContext) {}

// EnterCommonTableExpression is called when production commonTableExpression is entered.
func (s *BaseStarRocksListener) EnterCommonTableExpression(ctx *CommonTableExpressionContext) {}

// ExitCommonTableExpression is called when production commonTableExpression is exited.
func (s *BaseStarRocksListener) ExitCommonTableExpression(ctx *CommonTableExpressionContext) {}

// EnterSetQuantifier is called when production setQuantifier is entered.
func (s *BaseStarRocksListener) EnterSetQuantifier(ctx *SetQuantifierContext) {}

// ExitSetQuantifier is called when production setQuantifier is exited.
func (s *BaseStarRocksListener) ExitSetQuantifier(ctx *SetQuantifierContext) {}

// EnterSelectSingle is called when production selectSingle is entered.
func (s *BaseStarRocksListener) EnterSelectSingle(ctx *SelectSingleContext) {}

// ExitSelectSingle is called when production selectSingle is exited.
func (s *BaseStarRocksListener) ExitSelectSingle(ctx *SelectSingleContext) {}

// EnterSelectAll is called when production selectAll is entered.
func (s *BaseStarRocksListener) EnterSelectAll(ctx *SelectAllContext) {}

// ExitSelectAll is called when production selectAll is exited.
func (s *BaseStarRocksListener) ExitSelectAll(ctx *SelectAllContext) {}

// EnterRelations is called when production relations is entered.
func (s *BaseStarRocksListener) EnterRelations(ctx *RelationsContext) {}

// ExitRelations is called when production relations is exited.
func (s *BaseStarRocksListener) ExitRelations(ctx *RelationsContext) {}

// EnterRelation is called when production relation is entered.
func (s *BaseStarRocksListener) EnterRelation(ctx *RelationContext) {}

// ExitRelation is called when production relation is exited.
func (s *BaseStarRocksListener) ExitRelation(ctx *RelationContext) {}

// EnterTableAtom is called when production tableAtom is entered.
func (s *BaseStarRocksListener) EnterTableAtom(ctx *TableAtomContext) {}

// ExitTableAtom is called when production tableAtom is exited.
func (s *BaseStarRocksListener) ExitTableAtom(ctx *TableAtomContext) {}

// EnterInlineTable is called when production inlineTable is entered.
func (s *BaseStarRocksListener) EnterInlineTable(ctx *InlineTableContext) {}

// ExitInlineTable is called when production inlineTable is exited.
func (s *BaseStarRocksListener) ExitInlineTable(ctx *InlineTableContext) {}

// EnterSubqueryWithAlias is called when production subqueryWithAlias is entered.
func (s *BaseStarRocksListener) EnterSubqueryWithAlias(ctx *SubqueryWithAliasContext) {}

// ExitSubqueryWithAlias is called when production subqueryWithAlias is exited.
func (s *BaseStarRocksListener) ExitSubqueryWithAlias(ctx *SubqueryWithAliasContext) {}

// EnterTableFunction is called when production tableFunction is entered.
func (s *BaseStarRocksListener) EnterTableFunction(ctx *TableFunctionContext) {}

// ExitTableFunction is called when production tableFunction is exited.
func (s *BaseStarRocksListener) ExitTableFunction(ctx *TableFunctionContext) {}

// EnterNormalizedTableFunction is called when production normalizedTableFunction is entered.
func (s *BaseStarRocksListener) EnterNormalizedTableFunction(ctx *NormalizedTableFunctionContext) {}

// ExitNormalizedTableFunction is called when production normalizedTableFunction is exited.
func (s *BaseStarRocksListener) ExitNormalizedTableFunction(ctx *NormalizedTableFunctionContext) {}

// EnterFileTableFunction is called when production fileTableFunction is entered.
func (s *BaseStarRocksListener) EnterFileTableFunction(ctx *FileTableFunctionContext) {}

// ExitFileTableFunction is called when production fileTableFunction is exited.
func (s *BaseStarRocksListener) ExitFileTableFunction(ctx *FileTableFunctionContext) {}

// EnterParenthesizedRelation is called when production parenthesizedRelation is entered.
func (s *BaseStarRocksListener) EnterParenthesizedRelation(ctx *ParenthesizedRelationContext) {}

// ExitParenthesizedRelation is called when production parenthesizedRelation is exited.
func (s *BaseStarRocksListener) ExitParenthesizedRelation(ctx *ParenthesizedRelationContext) {}

// EnterPivotClause is called when production pivotClause is entered.
func (s *BaseStarRocksListener) EnterPivotClause(ctx *PivotClauseContext) {}

// ExitPivotClause is called when production pivotClause is exited.
func (s *BaseStarRocksListener) ExitPivotClause(ctx *PivotClauseContext) {}

// EnterPivotAggregationExpression is called when production pivotAggregationExpression is entered.
func (s *BaseStarRocksListener) EnterPivotAggregationExpression(ctx *PivotAggregationExpressionContext) {
}

// ExitPivotAggregationExpression is called when production pivotAggregationExpression is exited.
func (s *BaseStarRocksListener) ExitPivotAggregationExpression(ctx *PivotAggregationExpressionContext) {
}

// EnterPivotValue is called when production pivotValue is entered.
func (s *BaseStarRocksListener) EnterPivotValue(ctx *PivotValueContext) {}

// ExitPivotValue is called when production pivotValue is exited.
func (s *BaseStarRocksListener) ExitPivotValue(ctx *PivotValueContext) {}

// EnterSampleClause is called when production sampleClause is entered.
func (s *BaseStarRocksListener) EnterSampleClause(ctx *SampleClauseContext) {}

// ExitSampleClause is called when production sampleClause is exited.
func (s *BaseStarRocksListener) ExitSampleClause(ctx *SampleClauseContext) {}

// EnterArgumentList is called when production argumentList is entered.
func (s *BaseStarRocksListener) EnterArgumentList(ctx *ArgumentListContext) {}

// ExitArgumentList is called when production argumentList is exited.
func (s *BaseStarRocksListener) ExitArgumentList(ctx *ArgumentListContext) {}

// EnterNamedArgumentList is called when production namedArgumentList is entered.
func (s *BaseStarRocksListener) EnterNamedArgumentList(ctx *NamedArgumentListContext) {}

// ExitNamedArgumentList is called when production namedArgumentList is exited.
func (s *BaseStarRocksListener) ExitNamedArgumentList(ctx *NamedArgumentListContext) {}

// EnterNamedArguments is called when production namedArguments is entered.
func (s *BaseStarRocksListener) EnterNamedArguments(ctx *NamedArgumentsContext) {}

// ExitNamedArguments is called when production namedArguments is exited.
func (s *BaseStarRocksListener) ExitNamedArguments(ctx *NamedArgumentsContext) {}

// EnterJoinRelation is called when production joinRelation is entered.
func (s *BaseStarRocksListener) EnterJoinRelation(ctx *JoinRelationContext) {}

// ExitJoinRelation is called when production joinRelation is exited.
func (s *BaseStarRocksListener) ExitJoinRelation(ctx *JoinRelationContext) {}

// EnterCrossOrInnerJoinType is called when production crossOrInnerJoinType is entered.
func (s *BaseStarRocksListener) EnterCrossOrInnerJoinType(ctx *CrossOrInnerJoinTypeContext) {}

// ExitCrossOrInnerJoinType is called when production crossOrInnerJoinType is exited.
func (s *BaseStarRocksListener) ExitCrossOrInnerJoinType(ctx *CrossOrInnerJoinTypeContext) {}

// EnterOuterAndSemiJoinType is called when production outerAndSemiJoinType is entered.
func (s *BaseStarRocksListener) EnterOuterAndSemiJoinType(ctx *OuterAndSemiJoinTypeContext) {}

// ExitOuterAndSemiJoinType is called when production outerAndSemiJoinType is exited.
func (s *BaseStarRocksListener) ExitOuterAndSemiJoinType(ctx *OuterAndSemiJoinTypeContext) {}

// EnterBracketHint is called when production bracketHint is entered.
func (s *BaseStarRocksListener) EnterBracketHint(ctx *BracketHintContext) {}

// ExitBracketHint is called when production bracketHint is exited.
func (s *BaseStarRocksListener) ExitBracketHint(ctx *BracketHintContext) {}

// EnterHintMap is called when production hintMap is entered.
func (s *BaseStarRocksListener) EnterHintMap(ctx *HintMapContext) {}

// ExitHintMap is called when production hintMap is exited.
func (s *BaseStarRocksListener) ExitHintMap(ctx *HintMapContext) {}

// EnterJoinCriteria is called when production joinCriteria is entered.
func (s *BaseStarRocksListener) EnterJoinCriteria(ctx *JoinCriteriaContext) {}

// ExitJoinCriteria is called when production joinCriteria is exited.
func (s *BaseStarRocksListener) ExitJoinCriteria(ctx *JoinCriteriaContext) {}

// EnterColumnAliases is called when production columnAliases is entered.
func (s *BaseStarRocksListener) EnterColumnAliases(ctx *ColumnAliasesContext) {}

// ExitColumnAliases is called when production columnAliases is exited.
func (s *BaseStarRocksListener) ExitColumnAliases(ctx *ColumnAliasesContext) {}

// EnterPartitionNames is called when production partitionNames is entered.
func (s *BaseStarRocksListener) EnterPartitionNames(ctx *PartitionNamesContext) {}

// ExitPartitionNames is called when production partitionNames is exited.
func (s *BaseStarRocksListener) ExitPartitionNames(ctx *PartitionNamesContext) {}

// EnterKeyPartitionList is called when production keyPartitionList is entered.
func (s *BaseStarRocksListener) EnterKeyPartitionList(ctx *KeyPartitionListContext) {}

// ExitKeyPartitionList is called when production keyPartitionList is exited.
func (s *BaseStarRocksListener) ExitKeyPartitionList(ctx *KeyPartitionListContext) {}

// EnterTabletList is called when production tabletList is entered.
func (s *BaseStarRocksListener) EnterTabletList(ctx *TabletListContext) {}

// ExitTabletList is called when production tabletList is exited.
func (s *BaseStarRocksListener) ExitTabletList(ctx *TabletListContext) {}

// EnterPrepareStatement is called when production prepareStatement is entered.
func (s *BaseStarRocksListener) EnterPrepareStatement(ctx *PrepareStatementContext) {}

// ExitPrepareStatement is called when production prepareStatement is exited.
func (s *BaseStarRocksListener) ExitPrepareStatement(ctx *PrepareStatementContext) {}

// EnterPrepareSql is called when production prepareSql is entered.
func (s *BaseStarRocksListener) EnterPrepareSql(ctx *PrepareSqlContext) {}

// ExitPrepareSql is called when production prepareSql is exited.
func (s *BaseStarRocksListener) ExitPrepareSql(ctx *PrepareSqlContext) {}

// EnterExecuteStatement is called when production executeStatement is entered.
func (s *BaseStarRocksListener) EnterExecuteStatement(ctx *ExecuteStatementContext) {}

// ExitExecuteStatement is called when production executeStatement is exited.
func (s *BaseStarRocksListener) ExitExecuteStatement(ctx *ExecuteStatementContext) {}

// EnterDeallocateStatement is called when production deallocateStatement is entered.
func (s *BaseStarRocksListener) EnterDeallocateStatement(ctx *DeallocateStatementContext) {}

// ExitDeallocateStatement is called when production deallocateStatement is exited.
func (s *BaseStarRocksListener) ExitDeallocateStatement(ctx *DeallocateStatementContext) {}

// EnterReplicaList is called when production replicaList is entered.
func (s *BaseStarRocksListener) EnterReplicaList(ctx *ReplicaListContext) {}

// ExitReplicaList is called when production replicaList is exited.
func (s *BaseStarRocksListener) ExitReplicaList(ctx *ReplicaListContext) {}

// EnterExpressionsWithDefault is called when production expressionsWithDefault is entered.
func (s *BaseStarRocksListener) EnterExpressionsWithDefault(ctx *ExpressionsWithDefaultContext) {}

// ExitExpressionsWithDefault is called when production expressionsWithDefault is exited.
func (s *BaseStarRocksListener) ExitExpressionsWithDefault(ctx *ExpressionsWithDefaultContext) {}

// EnterExpressionOrDefault is called when production expressionOrDefault is entered.
func (s *BaseStarRocksListener) EnterExpressionOrDefault(ctx *ExpressionOrDefaultContext) {}

// ExitExpressionOrDefault is called when production expressionOrDefault is exited.
func (s *BaseStarRocksListener) ExitExpressionOrDefault(ctx *ExpressionOrDefaultContext) {}

// EnterMapExpressionList is called when production mapExpressionList is entered.
func (s *BaseStarRocksListener) EnterMapExpressionList(ctx *MapExpressionListContext) {}

// ExitMapExpressionList is called when production mapExpressionList is exited.
func (s *BaseStarRocksListener) ExitMapExpressionList(ctx *MapExpressionListContext) {}

// EnterMapExpression is called when production mapExpression is entered.
func (s *BaseStarRocksListener) EnterMapExpression(ctx *MapExpressionContext) {}

// ExitMapExpression is called when production mapExpression is exited.
func (s *BaseStarRocksListener) ExitMapExpression(ctx *MapExpressionContext) {}

// EnterExpressionSingleton is called when production expressionSingleton is entered.
func (s *BaseStarRocksListener) EnterExpressionSingleton(ctx *ExpressionSingletonContext) {}

// ExitExpressionSingleton is called when production expressionSingleton is exited.
func (s *BaseStarRocksListener) ExitExpressionSingleton(ctx *ExpressionSingletonContext) {}

// EnterExpressionDefault is called when production expressionDefault is entered.
func (s *BaseStarRocksListener) EnterExpressionDefault(ctx *ExpressionDefaultContext) {}

// ExitExpressionDefault is called when production expressionDefault is exited.
func (s *BaseStarRocksListener) ExitExpressionDefault(ctx *ExpressionDefaultContext) {}

// EnterLogicalNot is called when production logicalNot is entered.
func (s *BaseStarRocksListener) EnterLogicalNot(ctx *LogicalNotContext) {}

// ExitLogicalNot is called when production logicalNot is exited.
func (s *BaseStarRocksListener) ExitLogicalNot(ctx *LogicalNotContext) {}

// EnterLogicalBinary is called when production logicalBinary is entered.
func (s *BaseStarRocksListener) EnterLogicalBinary(ctx *LogicalBinaryContext) {}

// ExitLogicalBinary is called when production logicalBinary is exited.
func (s *BaseStarRocksListener) ExitLogicalBinary(ctx *LogicalBinaryContext) {}

// EnterExpressionList is called when production expressionList is entered.
func (s *BaseStarRocksListener) EnterExpressionList(ctx *ExpressionListContext) {}

// ExitExpressionList is called when production expressionList is exited.
func (s *BaseStarRocksListener) ExitExpressionList(ctx *ExpressionListContext) {}

// EnterComparison is called when production comparison is entered.
func (s *BaseStarRocksListener) EnterComparison(ctx *ComparisonContext) {}

// ExitComparison is called when production comparison is exited.
func (s *BaseStarRocksListener) ExitComparison(ctx *ComparisonContext) {}

// EnterBooleanExpressionDefault is called when production booleanExpressionDefault is entered.
func (s *BaseStarRocksListener) EnterBooleanExpressionDefault(ctx *BooleanExpressionDefaultContext) {}

// ExitBooleanExpressionDefault is called when production booleanExpressionDefault is exited.
func (s *BaseStarRocksListener) ExitBooleanExpressionDefault(ctx *BooleanExpressionDefaultContext) {}

// EnterIsNull is called when production isNull is entered.
func (s *BaseStarRocksListener) EnterIsNull(ctx *IsNullContext) {}

// ExitIsNull is called when production isNull is exited.
func (s *BaseStarRocksListener) ExitIsNull(ctx *IsNullContext) {}

// EnterScalarSubquery is called when production scalarSubquery is entered.
func (s *BaseStarRocksListener) EnterScalarSubquery(ctx *ScalarSubqueryContext) {}

// ExitScalarSubquery is called when production scalarSubquery is exited.
func (s *BaseStarRocksListener) ExitScalarSubquery(ctx *ScalarSubqueryContext) {}

// EnterPredicate is called when production predicate is entered.
func (s *BaseStarRocksListener) EnterPredicate(ctx *PredicateContext) {}

// ExitPredicate is called when production predicate is exited.
func (s *BaseStarRocksListener) ExitPredicate(ctx *PredicateContext) {}

// EnterTupleInSubquery is called when production tupleInSubquery is entered.
func (s *BaseStarRocksListener) EnterTupleInSubquery(ctx *TupleInSubqueryContext) {}

// ExitTupleInSubquery is called when production tupleInSubquery is exited.
func (s *BaseStarRocksListener) ExitTupleInSubquery(ctx *TupleInSubqueryContext) {}

// EnterInIntegerList is called when production inIntegerList is entered.
func (s *BaseStarRocksListener) EnterInIntegerList(ctx *InIntegerListContext) {}

// ExitInIntegerList is called when production inIntegerList is exited.
func (s *BaseStarRocksListener) ExitInIntegerList(ctx *InIntegerListContext) {}

// EnterInStringList is called when production inStringList is entered.
func (s *BaseStarRocksListener) EnterInStringList(ctx *InStringListContext) {}

// ExitInStringList is called when production inStringList is exited.
func (s *BaseStarRocksListener) ExitInStringList(ctx *InStringListContext) {}

// EnterInSubquery is called when production inSubquery is entered.
func (s *BaseStarRocksListener) EnterInSubquery(ctx *InSubqueryContext) {}

// ExitInSubquery is called when production inSubquery is exited.
func (s *BaseStarRocksListener) ExitInSubquery(ctx *InSubqueryContext) {}

// EnterInList is called when production inList is entered.
func (s *BaseStarRocksListener) EnterInList(ctx *InListContext) {}

// ExitInList is called when production inList is exited.
func (s *BaseStarRocksListener) ExitInList(ctx *InListContext) {}

// EnterBetween is called when production between is entered.
func (s *BaseStarRocksListener) EnterBetween(ctx *BetweenContext) {}

// ExitBetween is called when production between is exited.
func (s *BaseStarRocksListener) ExitBetween(ctx *BetweenContext) {}

// EnterLike is called when production like is entered.
func (s *BaseStarRocksListener) EnterLike(ctx *LikeContext) {}

// ExitLike is called when production like is exited.
func (s *BaseStarRocksListener) ExitLike(ctx *LikeContext) {}

// EnterValueExpressionDefault is called when production valueExpressionDefault is entered.
func (s *BaseStarRocksListener) EnterValueExpressionDefault(ctx *ValueExpressionDefaultContext) {}

// ExitValueExpressionDefault is called when production valueExpressionDefault is exited.
func (s *BaseStarRocksListener) ExitValueExpressionDefault(ctx *ValueExpressionDefaultContext) {}

// EnterArithmeticBinary is called when production arithmeticBinary is entered.
func (s *BaseStarRocksListener) EnterArithmeticBinary(ctx *ArithmeticBinaryContext) {}

// ExitArithmeticBinary is called when production arithmeticBinary is exited.
func (s *BaseStarRocksListener) ExitArithmeticBinary(ctx *ArithmeticBinaryContext) {}

// EnterDereference is called when production dereference is entered.
func (s *BaseStarRocksListener) EnterDereference(ctx *DereferenceContext) {}

// ExitDereference is called when production dereference is exited.
func (s *BaseStarRocksListener) ExitDereference(ctx *DereferenceContext) {}

// EnterOdbcFunctionCallExpression is called when production odbcFunctionCallExpression is entered.
func (s *BaseStarRocksListener) EnterOdbcFunctionCallExpression(ctx *OdbcFunctionCallExpressionContext) {
}

// ExitOdbcFunctionCallExpression is called when production odbcFunctionCallExpression is exited.
func (s *BaseStarRocksListener) ExitOdbcFunctionCallExpression(ctx *OdbcFunctionCallExpressionContext) {
}

// EnterMatchExpr is called when production matchExpr is entered.
func (s *BaseStarRocksListener) EnterMatchExpr(ctx *MatchExprContext) {}

// ExitMatchExpr is called when production matchExpr is exited.
func (s *BaseStarRocksListener) ExitMatchExpr(ctx *MatchExprContext) {}

// EnterColumnRef is called when production columnRef is entered.
func (s *BaseStarRocksListener) EnterColumnRef(ctx *ColumnRefContext) {}

// ExitColumnRef is called when production columnRef is exited.
func (s *BaseStarRocksListener) ExitColumnRef(ctx *ColumnRefContext) {}

// EnterConvert is called when production convert is entered.
func (s *BaseStarRocksListener) EnterConvert(ctx *ConvertContext) {}

// ExitConvert is called when production convert is exited.
func (s *BaseStarRocksListener) ExitConvert(ctx *ConvertContext) {}

// EnterCollectionSubscript is called when production collectionSubscript is entered.
func (s *BaseStarRocksListener) EnterCollectionSubscript(ctx *CollectionSubscriptContext) {}

// ExitCollectionSubscript is called when production collectionSubscript is exited.
func (s *BaseStarRocksListener) ExitCollectionSubscript(ctx *CollectionSubscriptContext) {}

// EnterLiteral is called when production literal is entered.
func (s *BaseStarRocksListener) EnterLiteral(ctx *LiteralContext) {}

// ExitLiteral is called when production literal is exited.
func (s *BaseStarRocksListener) ExitLiteral(ctx *LiteralContext) {}

// EnterCast is called when production cast is entered.
func (s *BaseStarRocksListener) EnterCast(ctx *CastContext) {}

// ExitCast is called when production cast is exited.
func (s *BaseStarRocksListener) ExitCast(ctx *CastContext) {}

// EnterParenthesizedExpression is called when production parenthesizedExpression is entered.
func (s *BaseStarRocksListener) EnterParenthesizedExpression(ctx *ParenthesizedExpressionContext) {}

// ExitParenthesizedExpression is called when production parenthesizedExpression is exited.
func (s *BaseStarRocksListener) ExitParenthesizedExpression(ctx *ParenthesizedExpressionContext) {}

// EnterUserVariableExpression is called when production userVariableExpression is entered.
func (s *BaseStarRocksListener) EnterUserVariableExpression(ctx *UserVariableExpressionContext) {}

// ExitUserVariableExpression is called when production userVariableExpression is exited.
func (s *BaseStarRocksListener) ExitUserVariableExpression(ctx *UserVariableExpressionContext) {}

// EnterFunctionCallExpression is called when production functionCallExpression is entered.
func (s *BaseStarRocksListener) EnterFunctionCallExpression(ctx *FunctionCallExpressionContext) {}

// ExitFunctionCallExpression is called when production functionCallExpression is exited.
func (s *BaseStarRocksListener) ExitFunctionCallExpression(ctx *FunctionCallExpressionContext) {}

// EnterSimpleCase is called when production simpleCase is entered.
func (s *BaseStarRocksListener) EnterSimpleCase(ctx *SimpleCaseContext) {}

// ExitSimpleCase is called when production simpleCase is exited.
func (s *BaseStarRocksListener) ExitSimpleCase(ctx *SimpleCaseContext) {}

// EnterArrowExpression is called when production arrowExpression is entered.
func (s *BaseStarRocksListener) EnterArrowExpression(ctx *ArrowExpressionContext) {}

// ExitArrowExpression is called when production arrowExpression is exited.
func (s *BaseStarRocksListener) ExitArrowExpression(ctx *ArrowExpressionContext) {}

// EnterSystemVariableExpression is called when production systemVariableExpression is entered.
func (s *BaseStarRocksListener) EnterSystemVariableExpression(ctx *SystemVariableExpressionContext) {}

// ExitSystemVariableExpression is called when production systemVariableExpression is exited.
func (s *BaseStarRocksListener) ExitSystemVariableExpression(ctx *SystemVariableExpressionContext) {}

// EnterConcat is called when production concat is entered.
func (s *BaseStarRocksListener) EnterConcat(ctx *ConcatContext) {}

// ExitConcat is called when production concat is exited.
func (s *BaseStarRocksListener) ExitConcat(ctx *ConcatContext) {}

// EnterSubqueryExpression is called when production subqueryExpression is entered.
func (s *BaseStarRocksListener) EnterSubqueryExpression(ctx *SubqueryExpressionContext) {}

// ExitSubqueryExpression is called when production subqueryExpression is exited.
func (s *BaseStarRocksListener) ExitSubqueryExpression(ctx *SubqueryExpressionContext) {}

// EnterLambdaFunctionExpr is called when production lambdaFunctionExpr is entered.
func (s *BaseStarRocksListener) EnterLambdaFunctionExpr(ctx *LambdaFunctionExprContext) {}

// ExitLambdaFunctionExpr is called when production lambdaFunctionExpr is exited.
func (s *BaseStarRocksListener) ExitLambdaFunctionExpr(ctx *LambdaFunctionExprContext) {}

// EnterDictionaryGetExpr is called when production dictionaryGetExpr is entered.
func (s *BaseStarRocksListener) EnterDictionaryGetExpr(ctx *DictionaryGetExprContext) {}

// ExitDictionaryGetExpr is called when production dictionaryGetExpr is exited.
func (s *BaseStarRocksListener) ExitDictionaryGetExpr(ctx *DictionaryGetExprContext) {}

// EnterCollate is called when production collate is entered.
func (s *BaseStarRocksListener) EnterCollate(ctx *CollateContext) {}

// ExitCollate is called when production collate is exited.
func (s *BaseStarRocksListener) ExitCollate(ctx *CollateContext) {}

// EnterArrayConstructor is called when production arrayConstructor is entered.
func (s *BaseStarRocksListener) EnterArrayConstructor(ctx *ArrayConstructorContext) {}

// ExitArrayConstructor is called when production arrayConstructor is exited.
func (s *BaseStarRocksListener) ExitArrayConstructor(ctx *ArrayConstructorContext) {}

// EnterMapConstructor is called when production mapConstructor is entered.
func (s *BaseStarRocksListener) EnterMapConstructor(ctx *MapConstructorContext) {}

// ExitMapConstructor is called when production mapConstructor is exited.
func (s *BaseStarRocksListener) ExitMapConstructor(ctx *MapConstructorContext) {}

// EnterArraySlice is called when production arraySlice is entered.
func (s *BaseStarRocksListener) EnterArraySlice(ctx *ArraySliceContext) {}

// ExitArraySlice is called when production arraySlice is exited.
func (s *BaseStarRocksListener) ExitArraySlice(ctx *ArraySliceContext) {}

// EnterExists is called when production exists is entered.
func (s *BaseStarRocksListener) EnterExists(ctx *ExistsContext) {}

// ExitExists is called when production exists is exited.
func (s *BaseStarRocksListener) ExitExists(ctx *ExistsContext) {}

// EnterSearchedCase is called when production searchedCase is entered.
func (s *BaseStarRocksListener) EnterSearchedCase(ctx *SearchedCaseContext) {}

// ExitSearchedCase is called when production searchedCase is exited.
func (s *BaseStarRocksListener) ExitSearchedCase(ctx *SearchedCaseContext) {}

// EnterArithmeticUnary is called when production arithmeticUnary is entered.
func (s *BaseStarRocksListener) EnterArithmeticUnary(ctx *ArithmeticUnaryContext) {}

// ExitArithmeticUnary is called when production arithmeticUnary is exited.
func (s *BaseStarRocksListener) ExitArithmeticUnary(ctx *ArithmeticUnaryContext) {}

// EnterNumericLiteral is called when production numericLiteral is entered.
func (s *BaseStarRocksListener) EnterNumericLiteral(ctx *NumericLiteralContext) {}

// ExitNumericLiteral is called when production numericLiteral is exited.
func (s *BaseStarRocksListener) ExitNumericLiteral(ctx *NumericLiteralContext) {}

// EnterNullLiteral is called when production nullLiteral is entered.
func (s *BaseStarRocksListener) EnterNullLiteral(ctx *NullLiteralContext) {}

// ExitNullLiteral is called when production nullLiteral is exited.
func (s *BaseStarRocksListener) ExitNullLiteral(ctx *NullLiteralContext) {}

// EnterBooleanLiteral is called when production booleanLiteral is entered.
func (s *BaseStarRocksListener) EnterBooleanLiteral(ctx *BooleanLiteralContext) {}

// ExitBooleanLiteral is called when production booleanLiteral is exited.
func (s *BaseStarRocksListener) ExitBooleanLiteral(ctx *BooleanLiteralContext) {}

// EnterDateLiteral is called when production dateLiteral is entered.
func (s *BaseStarRocksListener) EnterDateLiteral(ctx *DateLiteralContext) {}

// ExitDateLiteral is called when production dateLiteral is exited.
func (s *BaseStarRocksListener) ExitDateLiteral(ctx *DateLiteralContext) {}

// EnterStringLiteral is called when production stringLiteral is entered.
func (s *BaseStarRocksListener) EnterStringLiteral(ctx *StringLiteralContext) {}

// ExitStringLiteral is called when production stringLiteral is exited.
func (s *BaseStarRocksListener) ExitStringLiteral(ctx *StringLiteralContext) {}

// EnterIntervalLiteral is called when production intervalLiteral is entered.
func (s *BaseStarRocksListener) EnterIntervalLiteral(ctx *IntervalLiteralContext) {}

// ExitIntervalLiteral is called when production intervalLiteral is exited.
func (s *BaseStarRocksListener) ExitIntervalLiteral(ctx *IntervalLiteralContext) {}

// EnterUnitBoundaryLiteral is called when production unitBoundaryLiteral is entered.
func (s *BaseStarRocksListener) EnterUnitBoundaryLiteral(ctx *UnitBoundaryLiteralContext) {}

// ExitUnitBoundaryLiteral is called when production unitBoundaryLiteral is exited.
func (s *BaseStarRocksListener) ExitUnitBoundaryLiteral(ctx *UnitBoundaryLiteralContext) {}

// EnterBinaryLiteral is called when production binaryLiteral is entered.
func (s *BaseStarRocksListener) EnterBinaryLiteral(ctx *BinaryLiteralContext) {}

// ExitBinaryLiteral is called when production binaryLiteral is exited.
func (s *BaseStarRocksListener) ExitBinaryLiteral(ctx *BinaryLiteralContext) {}

// EnterParameter is called when production Parameter is entered.
func (s *BaseStarRocksListener) EnterParameter(ctx *ParameterContext) {}

// ExitParameter is called when production Parameter is exited.
func (s *BaseStarRocksListener) ExitParameter(ctx *ParameterContext) {}

// EnterExtract is called when production extract is entered.
func (s *BaseStarRocksListener) EnterExtract(ctx *ExtractContext) {}

// ExitExtract is called when production extract is exited.
func (s *BaseStarRocksListener) ExitExtract(ctx *ExtractContext) {}

// EnterGroupingOperation is called when production groupingOperation is entered.
func (s *BaseStarRocksListener) EnterGroupingOperation(ctx *GroupingOperationContext) {}

// ExitGroupingOperation is called when production groupingOperation is exited.
func (s *BaseStarRocksListener) ExitGroupingOperation(ctx *GroupingOperationContext) {}

// EnterInformationFunction is called when production informationFunction is entered.
func (s *BaseStarRocksListener) EnterInformationFunction(ctx *InformationFunctionContext) {}

// ExitInformationFunction is called when production informationFunction is exited.
func (s *BaseStarRocksListener) ExitInformationFunction(ctx *InformationFunctionContext) {}

// EnterSpecialDateTime is called when production specialDateTime is entered.
func (s *BaseStarRocksListener) EnterSpecialDateTime(ctx *SpecialDateTimeContext) {}

// ExitSpecialDateTime is called when production specialDateTime is exited.
func (s *BaseStarRocksListener) ExitSpecialDateTime(ctx *SpecialDateTimeContext) {}

// EnterSpecialFunction is called when production specialFunction is entered.
func (s *BaseStarRocksListener) EnterSpecialFunction(ctx *SpecialFunctionContext) {}

// ExitSpecialFunction is called when production specialFunction is exited.
func (s *BaseStarRocksListener) ExitSpecialFunction(ctx *SpecialFunctionContext) {}

// EnterAggregationFunctionCall is called when production aggregationFunctionCall is entered.
func (s *BaseStarRocksListener) EnterAggregationFunctionCall(ctx *AggregationFunctionCallContext) {}

// ExitAggregationFunctionCall is called when production aggregationFunctionCall is exited.
func (s *BaseStarRocksListener) ExitAggregationFunctionCall(ctx *AggregationFunctionCallContext) {}

// EnterWindowFunctionCall is called when production windowFunctionCall is entered.
func (s *BaseStarRocksListener) EnterWindowFunctionCall(ctx *WindowFunctionCallContext) {}

// ExitWindowFunctionCall is called when production windowFunctionCall is exited.
func (s *BaseStarRocksListener) ExitWindowFunctionCall(ctx *WindowFunctionCallContext) {}

// EnterTranslateFunctionCall is called when production translateFunctionCall is entered.
func (s *BaseStarRocksListener) EnterTranslateFunctionCall(ctx *TranslateFunctionCallContext) {}

// ExitTranslateFunctionCall is called when production translateFunctionCall is exited.
func (s *BaseStarRocksListener) ExitTranslateFunctionCall(ctx *TranslateFunctionCallContext) {}

// EnterSimpleFunctionCall is called when production simpleFunctionCall is entered.
func (s *BaseStarRocksListener) EnterSimpleFunctionCall(ctx *SimpleFunctionCallContext) {}

// ExitSimpleFunctionCall is called when production simpleFunctionCall is exited.
func (s *BaseStarRocksListener) ExitSimpleFunctionCall(ctx *SimpleFunctionCallContext) {}

// EnterAggregationFunction is called when production aggregationFunction is entered.
func (s *BaseStarRocksListener) EnterAggregationFunction(ctx *AggregationFunctionContext) {}

// ExitAggregationFunction is called when production aggregationFunction is exited.
func (s *BaseStarRocksListener) ExitAggregationFunction(ctx *AggregationFunctionContext) {}

// EnterUserVariable is called when production userVariable is entered.
func (s *BaseStarRocksListener) EnterUserVariable(ctx *UserVariableContext) {}

// ExitUserVariable is called when production userVariable is exited.
func (s *BaseStarRocksListener) ExitUserVariable(ctx *UserVariableContext) {}

// EnterSystemVariable is called when production systemVariable is entered.
func (s *BaseStarRocksListener) EnterSystemVariable(ctx *SystemVariableContext) {}

// ExitSystemVariable is called when production systemVariable is exited.
func (s *BaseStarRocksListener) ExitSystemVariable(ctx *SystemVariableContext) {}

// EnterColumnReference is called when production columnReference is entered.
func (s *BaseStarRocksListener) EnterColumnReference(ctx *ColumnReferenceContext) {}

// ExitColumnReference is called when production columnReference is exited.
func (s *BaseStarRocksListener) ExitColumnReference(ctx *ColumnReferenceContext) {}

// EnterInformationFunctionExpression is called when production informationFunctionExpression is entered.
func (s *BaseStarRocksListener) EnterInformationFunctionExpression(ctx *InformationFunctionExpressionContext) {
}

// ExitInformationFunctionExpression is called when production informationFunctionExpression is exited.
func (s *BaseStarRocksListener) ExitInformationFunctionExpression(ctx *InformationFunctionExpressionContext) {
}

// EnterSpecialDateTimeExpression is called when production specialDateTimeExpression is entered.
func (s *BaseStarRocksListener) EnterSpecialDateTimeExpression(ctx *SpecialDateTimeExpressionContext) {
}

// ExitSpecialDateTimeExpression is called when production specialDateTimeExpression is exited.
func (s *BaseStarRocksListener) ExitSpecialDateTimeExpression(ctx *SpecialDateTimeExpressionContext) {
}

// EnterSpecialFunctionExpression is called when production specialFunctionExpression is entered.
func (s *BaseStarRocksListener) EnterSpecialFunctionExpression(ctx *SpecialFunctionExpressionContext) {
}

// ExitSpecialFunctionExpression is called when production specialFunctionExpression is exited.
func (s *BaseStarRocksListener) ExitSpecialFunctionExpression(ctx *SpecialFunctionExpressionContext) {
}

// EnterWindowFunction is called when production windowFunction is entered.
func (s *BaseStarRocksListener) EnterWindowFunction(ctx *WindowFunctionContext) {}

// ExitWindowFunction is called when production windowFunction is exited.
func (s *BaseStarRocksListener) ExitWindowFunction(ctx *WindowFunctionContext) {}

// EnterWhenClause is called when production whenClause is entered.
func (s *BaseStarRocksListener) EnterWhenClause(ctx *WhenClauseContext) {}

// ExitWhenClause is called when production whenClause is exited.
func (s *BaseStarRocksListener) ExitWhenClause(ctx *WhenClauseContext) {}

// EnterOver is called when production over is entered.
func (s *BaseStarRocksListener) EnterOver(ctx *OverContext) {}

// ExitOver is called when production over is exited.
func (s *BaseStarRocksListener) ExitOver(ctx *OverContext) {}

// EnterIgnoreNulls is called when production ignoreNulls is entered.
func (s *BaseStarRocksListener) EnterIgnoreNulls(ctx *IgnoreNullsContext) {}

// ExitIgnoreNulls is called when production ignoreNulls is exited.
func (s *BaseStarRocksListener) ExitIgnoreNulls(ctx *IgnoreNullsContext) {}

// EnterWindowFrame is called when production windowFrame is entered.
func (s *BaseStarRocksListener) EnterWindowFrame(ctx *WindowFrameContext) {}

// ExitWindowFrame is called when production windowFrame is exited.
func (s *BaseStarRocksListener) ExitWindowFrame(ctx *WindowFrameContext) {}

// EnterUnboundedFrame is called when production unboundedFrame is entered.
func (s *BaseStarRocksListener) EnterUnboundedFrame(ctx *UnboundedFrameContext) {}

// ExitUnboundedFrame is called when production unboundedFrame is exited.
func (s *BaseStarRocksListener) ExitUnboundedFrame(ctx *UnboundedFrameContext) {}

// EnterCurrentRowBound is called when production currentRowBound is entered.
func (s *BaseStarRocksListener) EnterCurrentRowBound(ctx *CurrentRowBoundContext) {}

// ExitCurrentRowBound is called when production currentRowBound is exited.
func (s *BaseStarRocksListener) ExitCurrentRowBound(ctx *CurrentRowBoundContext) {}

// EnterBoundedFrame is called when production boundedFrame is entered.
func (s *BaseStarRocksListener) EnterBoundedFrame(ctx *BoundedFrameContext) {}

// ExitBoundedFrame is called when production boundedFrame is exited.
func (s *BaseStarRocksListener) ExitBoundedFrame(ctx *BoundedFrameContext) {}

// EnterBackupRestoreObjectDesc is called when production backupRestoreObjectDesc is entered.
func (s *BaseStarRocksListener) EnterBackupRestoreObjectDesc(ctx *BackupRestoreObjectDescContext) {}

// ExitBackupRestoreObjectDesc is called when production backupRestoreObjectDesc is exited.
func (s *BaseStarRocksListener) ExitBackupRestoreObjectDesc(ctx *BackupRestoreObjectDescContext) {}

// EnterTableDesc is called when production tableDesc is entered.
func (s *BaseStarRocksListener) EnterTableDesc(ctx *TableDescContext) {}

// ExitTableDesc is called when production tableDesc is exited.
func (s *BaseStarRocksListener) ExitTableDesc(ctx *TableDescContext) {}

// EnterBackupRestoreTableDesc is called when production backupRestoreTableDesc is entered.
func (s *BaseStarRocksListener) EnterBackupRestoreTableDesc(ctx *BackupRestoreTableDescContext) {}

// ExitBackupRestoreTableDesc is called when production backupRestoreTableDesc is exited.
func (s *BaseStarRocksListener) ExitBackupRestoreTableDesc(ctx *BackupRestoreTableDescContext) {}

// EnterExplainDesc is called when production explainDesc is entered.
func (s *BaseStarRocksListener) EnterExplainDesc(ctx *ExplainDescContext) {}

// ExitExplainDesc is called when production explainDesc is exited.
func (s *BaseStarRocksListener) ExitExplainDesc(ctx *ExplainDescContext) {}

// EnterOptimizerTrace is called when production optimizerTrace is entered.
func (s *BaseStarRocksListener) EnterOptimizerTrace(ctx *OptimizerTraceContext) {}

// ExitOptimizerTrace is called when production optimizerTrace is exited.
func (s *BaseStarRocksListener) ExitOptimizerTrace(ctx *OptimizerTraceContext) {}

// EnterPartitionExpr is called when production partitionExpr is entered.
func (s *BaseStarRocksListener) EnterPartitionExpr(ctx *PartitionExprContext) {}

// ExitPartitionExpr is called when production partitionExpr is exited.
func (s *BaseStarRocksListener) ExitPartitionExpr(ctx *PartitionExprContext) {}

// EnterPartitionDesc is called when production partitionDesc is entered.
func (s *BaseStarRocksListener) EnterPartitionDesc(ctx *PartitionDescContext) {}

// ExitPartitionDesc is called when production partitionDesc is exited.
func (s *BaseStarRocksListener) ExitPartitionDesc(ctx *PartitionDescContext) {}

// EnterListPartitionDesc is called when production listPartitionDesc is entered.
func (s *BaseStarRocksListener) EnterListPartitionDesc(ctx *ListPartitionDescContext) {}

// ExitListPartitionDesc is called when production listPartitionDesc is exited.
func (s *BaseStarRocksListener) ExitListPartitionDesc(ctx *ListPartitionDescContext) {}

// EnterSingleItemListPartitionDesc is called when production singleItemListPartitionDesc is entered.
func (s *BaseStarRocksListener) EnterSingleItemListPartitionDesc(ctx *SingleItemListPartitionDescContext) {
}

// ExitSingleItemListPartitionDesc is called when production singleItemListPartitionDesc is exited.
func (s *BaseStarRocksListener) ExitSingleItemListPartitionDesc(ctx *SingleItemListPartitionDescContext) {
}

// EnterMultiItemListPartitionDesc is called when production multiItemListPartitionDesc is entered.
func (s *BaseStarRocksListener) EnterMultiItemListPartitionDesc(ctx *MultiItemListPartitionDescContext) {
}

// ExitMultiItemListPartitionDesc is called when production multiItemListPartitionDesc is exited.
func (s *BaseStarRocksListener) ExitMultiItemListPartitionDesc(ctx *MultiItemListPartitionDescContext) {
}

// EnterMultiListPartitionValues is called when production multiListPartitionValues is entered.
func (s *BaseStarRocksListener) EnterMultiListPartitionValues(ctx *MultiListPartitionValuesContext) {}

// ExitMultiListPartitionValues is called when production multiListPartitionValues is exited.
func (s *BaseStarRocksListener) ExitMultiListPartitionValues(ctx *MultiListPartitionValuesContext) {}

// EnterSingleListPartitionValues is called when production singleListPartitionValues is entered.
func (s *BaseStarRocksListener) EnterSingleListPartitionValues(ctx *SingleListPartitionValuesContext) {
}

// ExitSingleListPartitionValues is called when production singleListPartitionValues is exited.
func (s *BaseStarRocksListener) ExitSingleListPartitionValues(ctx *SingleListPartitionValuesContext) {
}

// EnterListPartitionValues is called when production listPartitionValues is entered.
func (s *BaseStarRocksListener) EnterListPartitionValues(ctx *ListPartitionValuesContext) {}

// ExitListPartitionValues is called when production listPartitionValues is exited.
func (s *BaseStarRocksListener) ExitListPartitionValues(ctx *ListPartitionValuesContext) {}

// EnterListPartitionValue is called when production listPartitionValue is entered.
func (s *BaseStarRocksListener) EnterListPartitionValue(ctx *ListPartitionValueContext) {}

// ExitListPartitionValue is called when production listPartitionValue is exited.
func (s *BaseStarRocksListener) ExitListPartitionValue(ctx *ListPartitionValueContext) {}

// EnterStringList is called when production stringList is entered.
func (s *BaseStarRocksListener) EnterStringList(ctx *StringListContext) {}

// ExitStringList is called when production stringList is exited.
func (s *BaseStarRocksListener) ExitStringList(ctx *StringListContext) {}

// EnterIntegerList is called when production integerList is entered.
func (s *BaseStarRocksListener) EnterIntegerList(ctx *IntegerListContext) {}

// ExitIntegerList is called when production integerList is exited.
func (s *BaseStarRocksListener) ExitIntegerList(ctx *IntegerListContext) {}

// EnterLiteralExpressionList is called when production literalExpressionList is entered.
func (s *BaseStarRocksListener) EnterLiteralExpressionList(ctx *LiteralExpressionListContext) {}

// ExitLiteralExpressionList is called when production literalExpressionList is exited.
func (s *BaseStarRocksListener) ExitLiteralExpressionList(ctx *LiteralExpressionListContext) {}

// EnterRangePartitionDesc is called when production rangePartitionDesc is entered.
func (s *BaseStarRocksListener) EnterRangePartitionDesc(ctx *RangePartitionDescContext) {}

// ExitRangePartitionDesc is called when production rangePartitionDesc is exited.
func (s *BaseStarRocksListener) ExitRangePartitionDesc(ctx *RangePartitionDescContext) {}

// EnterSingleRangePartition is called when production singleRangePartition is entered.
func (s *BaseStarRocksListener) EnterSingleRangePartition(ctx *SingleRangePartitionContext) {}

// ExitSingleRangePartition is called when production singleRangePartition is exited.
func (s *BaseStarRocksListener) ExitSingleRangePartition(ctx *SingleRangePartitionContext) {}

// EnterMultiRangePartition is called when production multiRangePartition is entered.
func (s *BaseStarRocksListener) EnterMultiRangePartition(ctx *MultiRangePartitionContext) {}

// ExitMultiRangePartition is called when production multiRangePartition is exited.
func (s *BaseStarRocksListener) ExitMultiRangePartition(ctx *MultiRangePartitionContext) {}

// EnterPartitionRangeDesc is called when production partitionRangeDesc is entered.
func (s *BaseStarRocksListener) EnterPartitionRangeDesc(ctx *PartitionRangeDescContext) {}

// ExitPartitionRangeDesc is called when production partitionRangeDesc is exited.
func (s *BaseStarRocksListener) ExitPartitionRangeDesc(ctx *PartitionRangeDescContext) {}

// EnterPartitionKeyDesc is called when production partitionKeyDesc is entered.
func (s *BaseStarRocksListener) EnterPartitionKeyDesc(ctx *PartitionKeyDescContext) {}

// ExitPartitionKeyDesc is called when production partitionKeyDesc is exited.
func (s *BaseStarRocksListener) ExitPartitionKeyDesc(ctx *PartitionKeyDescContext) {}

// EnterPartitionValueList is called when production partitionValueList is entered.
func (s *BaseStarRocksListener) EnterPartitionValueList(ctx *PartitionValueListContext) {}

// ExitPartitionValueList is called when production partitionValueList is exited.
func (s *BaseStarRocksListener) ExitPartitionValueList(ctx *PartitionValueListContext) {}

// EnterKeyPartition is called when production keyPartition is entered.
func (s *BaseStarRocksListener) EnterKeyPartition(ctx *KeyPartitionContext) {}

// ExitKeyPartition is called when production keyPartition is exited.
func (s *BaseStarRocksListener) ExitKeyPartition(ctx *KeyPartitionContext) {}

// EnterPartitionValue is called when production partitionValue is entered.
func (s *BaseStarRocksListener) EnterPartitionValue(ctx *PartitionValueContext) {}

// ExitPartitionValue is called when production partitionValue is exited.
func (s *BaseStarRocksListener) ExitPartitionValue(ctx *PartitionValueContext) {}

// EnterDistributionClause is called when production distributionClause is entered.
func (s *BaseStarRocksListener) EnterDistributionClause(ctx *DistributionClauseContext) {}

// ExitDistributionClause is called when production distributionClause is exited.
func (s *BaseStarRocksListener) ExitDistributionClause(ctx *DistributionClauseContext) {}

// EnterDistributionDesc is called when production distributionDesc is entered.
func (s *BaseStarRocksListener) EnterDistributionDesc(ctx *DistributionDescContext) {}

// ExitDistributionDesc is called when production distributionDesc is exited.
func (s *BaseStarRocksListener) ExitDistributionDesc(ctx *DistributionDescContext) {}

// EnterAlterModifyDefaultBuckets is called when production alterModifyDefaultBuckets is entered.
func (s *BaseStarRocksListener) EnterAlterModifyDefaultBuckets(ctx *AlterModifyDefaultBucketsContext) {
}

// ExitAlterModifyDefaultBuckets is called when production alterModifyDefaultBuckets is exited.
func (s *BaseStarRocksListener) ExitAlterModifyDefaultBuckets(ctx *AlterModifyDefaultBucketsContext) {
}

// EnterRefreshSchemeDesc is called when production refreshSchemeDesc is entered.
func (s *BaseStarRocksListener) EnterRefreshSchemeDesc(ctx *RefreshSchemeDescContext) {}

// ExitRefreshSchemeDesc is called when production refreshSchemeDesc is exited.
func (s *BaseStarRocksListener) ExitRefreshSchemeDesc(ctx *RefreshSchemeDescContext) {}

// EnterStatusDesc is called when production statusDesc is entered.
func (s *BaseStarRocksListener) EnterStatusDesc(ctx *StatusDescContext) {}

// ExitStatusDesc is called when production statusDesc is exited.
func (s *BaseStarRocksListener) ExitStatusDesc(ctx *StatusDescContext) {}

// EnterProperties is called when production properties is entered.
func (s *BaseStarRocksListener) EnterProperties(ctx *PropertiesContext) {}

// ExitProperties is called when production properties is exited.
func (s *BaseStarRocksListener) ExitProperties(ctx *PropertiesContext) {}

// EnterExtProperties is called when production extProperties is entered.
func (s *BaseStarRocksListener) EnterExtProperties(ctx *ExtPropertiesContext) {}

// ExitExtProperties is called when production extProperties is exited.
func (s *BaseStarRocksListener) ExitExtProperties(ctx *ExtPropertiesContext) {}

// EnterPropertyList is called when production propertyList is entered.
func (s *BaseStarRocksListener) EnterPropertyList(ctx *PropertyListContext) {}

// ExitPropertyList is called when production propertyList is exited.
func (s *BaseStarRocksListener) ExitPropertyList(ctx *PropertyListContext) {}

// EnterUserPropertyList is called when production userPropertyList is entered.
func (s *BaseStarRocksListener) EnterUserPropertyList(ctx *UserPropertyListContext) {}

// ExitUserPropertyList is called when production userPropertyList is exited.
func (s *BaseStarRocksListener) ExitUserPropertyList(ctx *UserPropertyListContext) {}

// EnterProperty is called when production property is entered.
func (s *BaseStarRocksListener) EnterProperty(ctx *PropertyContext) {}

// ExitProperty is called when production property is exited.
func (s *BaseStarRocksListener) ExitProperty(ctx *PropertyContext) {}

// EnterInlineProperties is called when production inlineProperties is entered.
func (s *BaseStarRocksListener) EnterInlineProperties(ctx *InlinePropertiesContext) {}

// ExitInlineProperties is called when production inlineProperties is exited.
func (s *BaseStarRocksListener) ExitInlineProperties(ctx *InlinePropertiesContext) {}

// EnterInlineProperty is called when production inlineProperty is entered.
func (s *BaseStarRocksListener) EnterInlineProperty(ctx *InlinePropertyContext) {}

// ExitInlineProperty is called when production inlineProperty is exited.
func (s *BaseStarRocksListener) ExitInlineProperty(ctx *InlinePropertyContext) {}

// EnterVarType is called when production varType is entered.
func (s *BaseStarRocksListener) EnterVarType(ctx *VarTypeContext) {}

// ExitVarType is called when production varType is exited.
func (s *BaseStarRocksListener) ExitVarType(ctx *VarTypeContext) {}

// EnterComment is called when production comment is entered.
func (s *BaseStarRocksListener) EnterComment(ctx *CommentContext) {}

// ExitComment is called when production comment is exited.
func (s *BaseStarRocksListener) ExitComment(ctx *CommentContext) {}

// EnterOutfile is called when production outfile is entered.
func (s *BaseStarRocksListener) EnterOutfile(ctx *OutfileContext) {}

// ExitOutfile is called when production outfile is exited.
func (s *BaseStarRocksListener) ExitOutfile(ctx *OutfileContext) {}

// EnterFileFormat is called when production fileFormat is entered.
func (s *BaseStarRocksListener) EnterFileFormat(ctx *FileFormatContext) {}

// ExitFileFormat is called when production fileFormat is exited.
func (s *BaseStarRocksListener) ExitFileFormat(ctx *FileFormatContext) {}

// EnterString is called when production string is entered.
func (s *BaseStarRocksListener) EnterString(ctx *StringContext) {}

// ExitString is called when production string is exited.
func (s *BaseStarRocksListener) ExitString(ctx *StringContext) {}

// EnterBinary is called when production binary is entered.
func (s *BaseStarRocksListener) EnterBinary(ctx *BinaryContext) {}

// ExitBinary is called when production binary is exited.
func (s *BaseStarRocksListener) ExitBinary(ctx *BinaryContext) {}

// EnterComparisonOperator is called when production comparisonOperator is entered.
func (s *BaseStarRocksListener) EnterComparisonOperator(ctx *ComparisonOperatorContext) {}

// ExitComparisonOperator is called when production comparisonOperator is exited.
func (s *BaseStarRocksListener) ExitComparisonOperator(ctx *ComparisonOperatorContext) {}

// EnterBooleanValue is called when production booleanValue is entered.
func (s *BaseStarRocksListener) EnterBooleanValue(ctx *BooleanValueContext) {}

// ExitBooleanValue is called when production booleanValue is exited.
func (s *BaseStarRocksListener) ExitBooleanValue(ctx *BooleanValueContext) {}

// EnterInterval is called when production interval is entered.
func (s *BaseStarRocksListener) EnterInterval(ctx *IntervalContext) {}

// ExitInterval is called when production interval is exited.
func (s *BaseStarRocksListener) ExitInterval(ctx *IntervalContext) {}

// EnterTaskInterval is called when production taskInterval is entered.
func (s *BaseStarRocksListener) EnterTaskInterval(ctx *TaskIntervalContext) {}

// ExitTaskInterval is called when production taskInterval is exited.
func (s *BaseStarRocksListener) ExitTaskInterval(ctx *TaskIntervalContext) {}

// EnterTaskUnitIdentifier is called when production taskUnitIdentifier is entered.
func (s *BaseStarRocksListener) EnterTaskUnitIdentifier(ctx *TaskUnitIdentifierContext) {}

// ExitTaskUnitIdentifier is called when production taskUnitIdentifier is exited.
func (s *BaseStarRocksListener) ExitTaskUnitIdentifier(ctx *TaskUnitIdentifierContext) {}

// EnterUnitIdentifier is called when production unitIdentifier is entered.
func (s *BaseStarRocksListener) EnterUnitIdentifier(ctx *UnitIdentifierContext) {}

// ExitUnitIdentifier is called when production unitIdentifier is exited.
func (s *BaseStarRocksListener) ExitUnitIdentifier(ctx *UnitIdentifierContext) {}

// EnterUnitBoundary is called when production unitBoundary is entered.
func (s *BaseStarRocksListener) EnterUnitBoundary(ctx *UnitBoundaryContext) {}

// ExitUnitBoundary is called when production unitBoundary is exited.
func (s *BaseStarRocksListener) ExitUnitBoundary(ctx *UnitBoundaryContext) {}

// EnterType is called when production type is entered.
func (s *BaseStarRocksListener) EnterType(ctx *TypeContext) {}

// ExitType is called when production type is exited.
func (s *BaseStarRocksListener) ExitType(ctx *TypeContext) {}

// EnterArrayType is called when production arrayType is entered.
func (s *BaseStarRocksListener) EnterArrayType(ctx *ArrayTypeContext) {}

// ExitArrayType is called when production arrayType is exited.
func (s *BaseStarRocksListener) ExitArrayType(ctx *ArrayTypeContext) {}

// EnterMapType is called when production mapType is entered.
func (s *BaseStarRocksListener) EnterMapType(ctx *MapTypeContext) {}

// ExitMapType is called when production mapType is exited.
func (s *BaseStarRocksListener) ExitMapType(ctx *MapTypeContext) {}

// EnterSubfieldDesc is called when production subfieldDesc is entered.
func (s *BaseStarRocksListener) EnterSubfieldDesc(ctx *SubfieldDescContext) {}

// ExitSubfieldDesc is called when production subfieldDesc is exited.
func (s *BaseStarRocksListener) ExitSubfieldDesc(ctx *SubfieldDescContext) {}

// EnterSubfieldDescs is called when production subfieldDescs is entered.
func (s *BaseStarRocksListener) EnterSubfieldDescs(ctx *SubfieldDescsContext) {}

// ExitSubfieldDescs is called when production subfieldDescs is exited.
func (s *BaseStarRocksListener) ExitSubfieldDescs(ctx *SubfieldDescsContext) {}

// EnterStructType is called when production structType is entered.
func (s *BaseStarRocksListener) EnterStructType(ctx *StructTypeContext) {}

// ExitStructType is called when production structType is exited.
func (s *BaseStarRocksListener) ExitStructType(ctx *StructTypeContext) {}

// EnterTypeParameter is called when production typeParameter is entered.
func (s *BaseStarRocksListener) EnterTypeParameter(ctx *TypeParameterContext) {}

// ExitTypeParameter is called when production typeParameter is exited.
func (s *BaseStarRocksListener) ExitTypeParameter(ctx *TypeParameterContext) {}

// EnterBaseType is called when production baseType is entered.
func (s *BaseStarRocksListener) EnterBaseType(ctx *BaseTypeContext) {}

// ExitBaseType is called when production baseType is exited.
func (s *BaseStarRocksListener) ExitBaseType(ctx *BaseTypeContext) {}

// EnterDecimalType is called when production decimalType is entered.
func (s *BaseStarRocksListener) EnterDecimalType(ctx *DecimalTypeContext) {}

// ExitDecimalType is called when production decimalType is exited.
func (s *BaseStarRocksListener) ExitDecimalType(ctx *DecimalTypeContext) {}

// EnterQualifiedName is called when production qualifiedName is entered.
func (s *BaseStarRocksListener) EnterQualifiedName(ctx *QualifiedNameContext) {}

// ExitQualifiedName is called when production qualifiedName is exited.
func (s *BaseStarRocksListener) ExitQualifiedName(ctx *QualifiedNameContext) {}

// EnterTableName is called when production tableName is entered.
func (s *BaseStarRocksListener) EnterTableName(ctx *TableNameContext) {}

// ExitTableName is called when production tableName is exited.
func (s *BaseStarRocksListener) ExitTableName(ctx *TableNameContext) {}

// EnterWriteBranch is called when production writeBranch is entered.
func (s *BaseStarRocksListener) EnterWriteBranch(ctx *WriteBranchContext) {}

// ExitWriteBranch is called when production writeBranch is exited.
func (s *BaseStarRocksListener) ExitWriteBranch(ctx *WriteBranchContext) {}

// EnterUnquotedIdentifier is called when production unquotedIdentifier is entered.
func (s *BaseStarRocksListener) EnterUnquotedIdentifier(ctx *UnquotedIdentifierContext) {}

// ExitUnquotedIdentifier is called when production unquotedIdentifier is exited.
func (s *BaseStarRocksListener) ExitUnquotedIdentifier(ctx *UnquotedIdentifierContext) {}

// EnterDigitIdentifier is called when production digitIdentifier is entered.
func (s *BaseStarRocksListener) EnterDigitIdentifier(ctx *DigitIdentifierContext) {}

// ExitDigitIdentifier is called when production digitIdentifier is exited.
func (s *BaseStarRocksListener) ExitDigitIdentifier(ctx *DigitIdentifierContext) {}

// EnterBackQuotedIdentifier is called when production backQuotedIdentifier is entered.
func (s *BaseStarRocksListener) EnterBackQuotedIdentifier(ctx *BackQuotedIdentifierContext) {}

// ExitBackQuotedIdentifier is called when production backQuotedIdentifier is exited.
func (s *BaseStarRocksListener) ExitBackQuotedIdentifier(ctx *BackQuotedIdentifierContext) {}

// EnterIdentifierWithAlias is called when production identifierWithAlias is entered.
func (s *BaseStarRocksListener) EnterIdentifierWithAlias(ctx *IdentifierWithAliasContext) {}

// ExitIdentifierWithAlias is called when production identifierWithAlias is exited.
func (s *BaseStarRocksListener) ExitIdentifierWithAlias(ctx *IdentifierWithAliasContext) {}

// EnterIdentifierWithAliasList is called when production identifierWithAliasList is entered.
func (s *BaseStarRocksListener) EnterIdentifierWithAliasList(ctx *IdentifierWithAliasListContext) {}

// ExitIdentifierWithAliasList is called when production identifierWithAliasList is exited.
func (s *BaseStarRocksListener) ExitIdentifierWithAliasList(ctx *IdentifierWithAliasListContext) {}

// EnterIdentifierList is called when production identifierList is entered.
func (s *BaseStarRocksListener) EnterIdentifierList(ctx *IdentifierListContext) {}

// ExitIdentifierList is called when production identifierList is exited.
func (s *BaseStarRocksListener) ExitIdentifierList(ctx *IdentifierListContext) {}

// EnterIdentifierOrString is called when production identifierOrString is entered.
func (s *BaseStarRocksListener) EnterIdentifierOrString(ctx *IdentifierOrStringContext) {}

// ExitIdentifierOrString is called when production identifierOrString is exited.
func (s *BaseStarRocksListener) ExitIdentifierOrString(ctx *IdentifierOrStringContext) {}

// EnterIdentifierOrStringList is called when production identifierOrStringList is entered.
func (s *BaseStarRocksListener) EnterIdentifierOrStringList(ctx *IdentifierOrStringListContext) {}

// ExitIdentifierOrStringList is called when production identifierOrStringList is exited.
func (s *BaseStarRocksListener) ExitIdentifierOrStringList(ctx *IdentifierOrStringListContext) {}

// EnterIdentifierOrStringOrStar is called when production identifierOrStringOrStar is entered.
func (s *BaseStarRocksListener) EnterIdentifierOrStringOrStar(ctx *IdentifierOrStringOrStarContext) {}

// ExitIdentifierOrStringOrStar is called when production identifierOrStringOrStar is exited.
func (s *BaseStarRocksListener) ExitIdentifierOrStringOrStar(ctx *IdentifierOrStringOrStarContext) {}

// EnterUserWithoutHost is called when production userWithoutHost is entered.
func (s *BaseStarRocksListener) EnterUserWithoutHost(ctx *UserWithoutHostContext) {}

// ExitUserWithoutHost is called when production userWithoutHost is exited.
func (s *BaseStarRocksListener) ExitUserWithoutHost(ctx *UserWithoutHostContext) {}

// EnterUserWithHost is called when production userWithHost is entered.
func (s *BaseStarRocksListener) EnterUserWithHost(ctx *UserWithHostContext) {}

// ExitUserWithHost is called when production userWithHost is exited.
func (s *BaseStarRocksListener) ExitUserWithHost(ctx *UserWithHostContext) {}

// EnterUserWithHostAndBlanket is called when production userWithHostAndBlanket is entered.
func (s *BaseStarRocksListener) EnterUserWithHostAndBlanket(ctx *UserWithHostAndBlanketContext) {}

// ExitUserWithHostAndBlanket is called when production userWithHostAndBlanket is exited.
func (s *BaseStarRocksListener) ExitUserWithHostAndBlanket(ctx *UserWithHostAndBlanketContext) {}

// EnterAssignment is called when production assignment is entered.
func (s *BaseStarRocksListener) EnterAssignment(ctx *AssignmentContext) {}

// ExitAssignment is called when production assignment is exited.
func (s *BaseStarRocksListener) ExitAssignment(ctx *AssignmentContext) {}

// EnterAssignmentList is called when production assignmentList is entered.
func (s *BaseStarRocksListener) EnterAssignmentList(ctx *AssignmentListContext) {}

// ExitAssignmentList is called when production assignmentList is exited.
func (s *BaseStarRocksListener) ExitAssignmentList(ctx *AssignmentListContext) {}

// EnterIntegerValue is called when production integerValue is entered.
func (s *BaseStarRocksListener) EnterIntegerValue(ctx *IntegerValueContext) {}

// ExitIntegerValue is called when production integerValue is exited.
func (s *BaseStarRocksListener) ExitIntegerValue(ctx *IntegerValueContext) {}

// EnterDecimalValue is called when production decimalValue is entered.
func (s *BaseStarRocksListener) EnterDecimalValue(ctx *DecimalValueContext) {}

// ExitDecimalValue is called when production decimalValue is exited.
func (s *BaseStarRocksListener) ExitDecimalValue(ctx *DecimalValueContext) {}

// EnterDoubleValue is called when production doubleValue is entered.
func (s *BaseStarRocksListener) EnterDoubleValue(ctx *DoubleValueContext) {}

// ExitDoubleValue is called when production doubleValue is exited.
func (s *BaseStarRocksListener) ExitDoubleValue(ctx *DoubleValueContext) {}

// EnterNonReserved is called when production nonReserved is entered.
func (s *BaseStarRocksListener) EnterNonReserved(ctx *NonReservedContext) {}

// ExitNonReserved is called when production nonReserved is exited.
func (s *BaseStarRocksListener) ExitNonReserved(ctx *NonReservedContext) {}
