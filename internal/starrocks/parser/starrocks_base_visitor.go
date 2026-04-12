// Code generated from StarRocks.g4 by ANTLR 4.13.2. DO NOT EDIT.

package parser // StarRocks
import "github.com/antlr4-go/antlr/v4"

type BaseStarRocksVisitor struct {
	*antlr.BaseParseTreeVisitor
}

func (v *BaseStarRocksVisitor) VisitSqlStatements(ctx *SqlStatementsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitSingleStatement(ctx *SingleStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitEmptyStatement(ctx *EmptyStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitStatement(ctx *StatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitUseDatabaseStatement(ctx *UseDatabaseStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitUseCatalogStatement(ctx *UseCatalogStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitSetCatalogStatement(ctx *SetCatalogStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowDatabasesStatement(ctx *ShowDatabasesStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitAlterDbQuotaStatement(ctx *AlterDbQuotaStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitCreateDbStatement(ctx *CreateDbStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDropDbStatement(ctx *DropDbStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowCreateDbStatement(ctx *ShowCreateDbStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitAlterDatabaseRenameStatement(ctx *AlterDatabaseRenameStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitRecoverDbStmt(ctx *RecoverDbStmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowDataStmt(ctx *ShowDataStmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowDataDistributionStmt(ctx *ShowDataDistributionStmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitCreateTableStatement(ctx *CreateTableStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitColumnDesc(ctx *ColumnDescContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitCharsetName(ctx *CharsetNameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDefaultDesc(ctx *DefaultDescContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitGeneratedColumnDesc(ctx *GeneratedColumnDescContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitIndexDesc(ctx *IndexDescContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitEngineDesc(ctx *EngineDescContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitCharsetDesc(ctx *CharsetDescContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitCollateDesc(ctx *CollateDescContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitKeyDesc(ctx *KeyDescContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitOrderByDesc(ctx *OrderByDescContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitColumnNullable(ctx *ColumnNullableContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitTypeWithNullable(ctx *TypeWithNullableContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitAggStateDesc(ctx *AggStateDescContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitAggDesc(ctx *AggDescContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitRollupDesc(ctx *RollupDescContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitRollupItem(ctx *RollupItemContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDupKeys(ctx *DupKeysContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitFromRollup(ctx *FromRollupContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitOrReplace(ctx *OrReplaceContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitIfNotExists(ctx *IfNotExistsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitCreateTableAsSelectStatement(ctx *CreateTableAsSelectStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDropTableStatement(ctx *DropTableStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitCleanTemporaryTableStatement(ctx *CleanTemporaryTableStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitAlterTableStatement(ctx *AlterTableStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitCreateIndexStatement(ctx *CreateIndexStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDropIndexStatement(ctx *DropIndexStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitIndexType(ctx *IndexTypeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowTableStatement(ctx *ShowTableStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowTemporaryTablesStatement(ctx *ShowTemporaryTablesStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowCreateTableStatement(ctx *ShowCreateTableStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowColumnStatement(ctx *ShowColumnStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowTableStatusStatement(ctx *ShowTableStatusStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitRefreshTableStatement(ctx *RefreshTableStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowAlterStatement(ctx *ShowAlterStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDescTableStatement(ctx *DescTableStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitCreateTableLikeStatement(ctx *CreateTableLikeStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowIndexStatement(ctx *ShowIndexStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitRecoverTableStatement(ctx *RecoverTableStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitTruncateTableStatement(ctx *TruncateTableStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitCancelAlterTableStatement(ctx *CancelAlterTableStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowPartitionsStatement(ctx *ShowPartitionsStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitRecoverPartitionStatement(ctx *RecoverPartitionStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitCreateViewStatement(ctx *CreateViewStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitAlterViewStatement(ctx *AlterViewStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDropViewStatement(ctx *DropViewStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitColumnNameWithComment(ctx *ColumnNameWithCommentContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitSubmitTaskStatement(ctx *SubmitTaskStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitTaskClause(ctx *TaskClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDropTaskStatement(ctx *DropTaskStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitTaskScheduleDesc(ctx *TaskScheduleDescContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitCreateMaterializedViewStatement(ctx *CreateMaterializedViewStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitMvPartitionExprs(ctx *MvPartitionExprsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitMaterializedViewDesc(ctx *MaterializedViewDescContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowMaterializedViewsStatement(ctx *ShowMaterializedViewsStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDropMaterializedViewStatement(ctx *DropMaterializedViewStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitAlterMaterializedViewStatement(ctx *AlterMaterializedViewStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitRefreshMaterializedViewStatement(ctx *RefreshMaterializedViewStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitCancelRefreshMaterializedViewStatement(ctx *CancelRefreshMaterializedViewStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitAdminSetConfigStatement(ctx *AdminSetConfigStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitAdminSetReplicaStatusStatement(ctx *AdminSetReplicaStatusStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitAdminShowConfigStatement(ctx *AdminShowConfigStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitAdminShowReplicaDistributionStatement(ctx *AdminShowReplicaDistributionStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitAdminShowReplicaStatusStatement(ctx *AdminShowReplicaStatusStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitAdminRepairTableStatement(ctx *AdminRepairTableStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitAdminCancelRepairTableStatement(ctx *AdminCancelRepairTableStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitAdminCheckTabletsStatement(ctx *AdminCheckTabletsStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitAdminSetPartitionVersion(ctx *AdminSetPartitionVersionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitKillStatement(ctx *KillStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitSyncStatement(ctx *SyncStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitAdminSetAutomatedSnapshotOnStatement(ctx *AdminSetAutomatedSnapshotOnStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitAdminSetAutomatedSnapshotOffStatement(ctx *AdminSetAutomatedSnapshotOffStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitAlterSystemStatement(ctx *AlterSystemStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitCancelAlterSystemStatement(ctx *CancelAlterSystemStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowComputeNodesStatement(ctx *ShowComputeNodesStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitCreateExternalCatalogStatement(ctx *CreateExternalCatalogStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowCreateExternalCatalogStatement(ctx *ShowCreateExternalCatalogStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDropExternalCatalogStatement(ctx *DropExternalCatalogStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowCatalogsStatement(ctx *ShowCatalogsStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitAlterCatalogStatement(ctx *AlterCatalogStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitCreateStorageVolumeStatement(ctx *CreateStorageVolumeStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitTypeDesc(ctx *TypeDescContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitLocationsDesc(ctx *LocationsDescContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowStorageVolumesStatement(ctx *ShowStorageVolumesStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDropStorageVolumeStatement(ctx *DropStorageVolumeStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitAlterStorageVolumeStatement(ctx *AlterStorageVolumeStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitAlterStorageVolumeClause(ctx *AlterStorageVolumeClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitModifyStorageVolumePropertiesClause(ctx *ModifyStorageVolumePropertiesClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitModifyStorageVolumeCommentClause(ctx *ModifyStorageVolumeCommentClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDescStorageVolumeStatement(ctx *DescStorageVolumeStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitSetDefaultStorageVolumeStatement(ctx *SetDefaultStorageVolumeStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitUpdateFailPointStatusStatement(ctx *UpdateFailPointStatusStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowFailPointStatement(ctx *ShowFailPointStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitCreateDictionaryStatement(ctx *CreateDictionaryStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDropDictionaryStatement(ctx *DropDictionaryStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitRefreshDictionaryStatement(ctx *RefreshDictionaryStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowDictionaryStatement(ctx *ShowDictionaryStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitCancelRefreshDictionaryStatement(ctx *CancelRefreshDictionaryStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDictionaryColumnDesc(ctx *DictionaryColumnDescContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDictionaryName(ctx *DictionaryNameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitAlterClause(ctx *AlterClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitAddFrontendClause(ctx *AddFrontendClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDropFrontendClause(ctx *DropFrontendClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitModifyFrontendHostClause(ctx *ModifyFrontendHostClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitAddBackendClause(ctx *AddBackendClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDropBackendClause(ctx *DropBackendClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDecommissionBackendClause(ctx *DecommissionBackendClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitModifyBackendClause(ctx *ModifyBackendClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitAddComputeNodeClause(ctx *AddComputeNodeClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDropComputeNodeClause(ctx *DropComputeNodeClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitModifyBrokerClause(ctx *ModifyBrokerClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitAlterLoadErrorUrlClause(ctx *AlterLoadErrorUrlClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitCreateImageClause(ctx *CreateImageClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitCleanTabletSchedQClause(ctx *CleanTabletSchedQClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDecommissionDiskClause(ctx *DecommissionDiskClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitCancelDecommissionDiskClause(ctx *CancelDecommissionDiskClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDisableDiskClause(ctx *DisableDiskClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitCancelDisableDiskClause(ctx *CancelDisableDiskClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitCreateIndexClause(ctx *CreateIndexClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDropIndexClause(ctx *DropIndexClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitTableRenameClause(ctx *TableRenameClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitSwapTableClause(ctx *SwapTableClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitModifyPropertiesClause(ctx *ModifyPropertiesClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitModifyCommentClause(ctx *ModifyCommentClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitOptimizeRange(ctx *OptimizeRangeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitOptimizeClause(ctx *OptimizeClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitAddColumnClause(ctx *AddColumnClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitAddColumnsClause(ctx *AddColumnsClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDropColumnClause(ctx *DropColumnClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitModifyColumnClause(ctx *ModifyColumnClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitModifyColumnCommentClause(ctx *ModifyColumnCommentClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitColumnRenameClause(ctx *ColumnRenameClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitReorderColumnsClause(ctx *ReorderColumnsClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitRollupRenameClause(ctx *RollupRenameClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitCompactionClause(ctx *CompactionClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitSubfieldName(ctx *SubfieldNameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitNestedFieldName(ctx *NestedFieldNameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitAddFieldClause(ctx *AddFieldClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDropFieldClause(ctx *DropFieldClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitCreateOrReplaceTagClause(ctx *CreateOrReplaceTagClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitCreateOrReplaceBranchClause(ctx *CreateOrReplaceBranchClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDropBranchClause(ctx *DropBranchClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDropTagClause(ctx *DropTagClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitTableOperationClause(ctx *TableOperationClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitTagOptions(ctx *TagOptionsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitBranchOptions(ctx *BranchOptionsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitSnapshotRetention(ctx *SnapshotRetentionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitRefRetain(ctx *RefRetainContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitMaxSnapshotAge(ctx *MaxSnapshotAgeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitMinSnapshotsToKeep(ctx *MinSnapshotsToKeepContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitSnapshotId(ctx *SnapshotIdContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitTimeUnit(ctx *TimeUnitContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitInteger_list(ctx *Integer_listContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDropPersistentIndexClause(ctx *DropPersistentIndexClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitAlterTableAutoIncrementClause(ctx *AlterTableAutoIncrementClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitAddPartitionClause(ctx *AddPartitionClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDropPartitionClause(ctx *DropPartitionClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitTruncatePartitionClause(ctx *TruncatePartitionClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitModifyPartitionClause(ctx *ModifyPartitionClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitReplacePartitionClause(ctx *ReplacePartitionClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitPartitionRenameClause(ctx *PartitionRenameClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitInsertStatement(ctx *InsertStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitInsertLabelOrColumnAliases(ctx *InsertLabelOrColumnAliasesContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitColumnAliasesOrByName(ctx *ColumnAliasesOrByNameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitUpdateStatement(ctx *UpdateStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDeleteStatement(ctx *DeleteStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitCreateRoutineLoadStatement(ctx *CreateRoutineLoadStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitAlterRoutineLoadStatement(ctx *AlterRoutineLoadStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDataSource(ctx *DataSourceContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitLoadProperties(ctx *LoadPropertiesContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitColSeparatorProperty(ctx *ColSeparatorPropertyContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitRowDelimiterProperty(ctx *RowDelimiterPropertyContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitImportColumns(ctx *ImportColumnsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitColumnProperties(ctx *ColumnPropertiesContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitJobProperties(ctx *JobPropertiesContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDataSourceProperties(ctx *DataSourcePropertiesContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitStopRoutineLoadStatement(ctx *StopRoutineLoadStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitResumeRoutineLoadStatement(ctx *ResumeRoutineLoadStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitPauseRoutineLoadStatement(ctx *PauseRoutineLoadStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowRoutineLoadStatement(ctx *ShowRoutineLoadStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowRoutineLoadTaskStatement(ctx *ShowRoutineLoadTaskStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowCreateRoutineLoadStatement(ctx *ShowCreateRoutineLoadStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowStreamLoadStatement(ctx *ShowStreamLoadStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitAnalyzeStatement(ctx *AnalyzeStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitRegularColumns(ctx *RegularColumnsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitAllColumns(ctx *AllColumnsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitPredicateColumns(ctx *PredicateColumnsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitMultiColumnSet(ctx *MultiColumnSetContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDropStatsStatement(ctx *DropStatsStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitHistogramStatement(ctx *HistogramStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitAnalyzeHistogramStatement(ctx *AnalyzeHistogramStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDropHistogramStatement(ctx *DropHistogramStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitCreateAnalyzeStatement(ctx *CreateAnalyzeStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDropAnalyzeJobStatement(ctx *DropAnalyzeJobStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowAnalyzeStatement(ctx *ShowAnalyzeStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowStatsMetaStatement(ctx *ShowStatsMetaStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowHistogramMetaStatement(ctx *ShowHistogramMetaStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitKillAnalyzeStatement(ctx *KillAnalyzeStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitAnalyzeProfileStatement(ctx *AnalyzeProfileStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitCreateBaselinePlanStatement(ctx *CreateBaselinePlanStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDropBaselinePlanStatement(ctx *DropBaselinePlanStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowBaselinePlanStatement(ctx *ShowBaselinePlanStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitCreateResourceGroupStatement(ctx *CreateResourceGroupStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDropResourceGroupStatement(ctx *DropResourceGroupStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitAlterResourceGroupStatement(ctx *AlterResourceGroupStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowResourceGroupStatement(ctx *ShowResourceGroupStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowResourceGroupUsageStatement(ctx *ShowResourceGroupUsageStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitCreateResourceStatement(ctx *CreateResourceStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitAlterResourceStatement(ctx *AlterResourceStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDropResourceStatement(ctx *DropResourceStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowResourceStatement(ctx *ShowResourceStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitClassifier(ctx *ClassifierContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowFunctionsStatement(ctx *ShowFunctionsStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDropFunctionStatement(ctx *DropFunctionStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitCreateFunctionStatement(ctx *CreateFunctionStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitInlineFunction(ctx *InlineFunctionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitTypeList(ctx *TypeListContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitLoadStatement(ctx *LoadStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitLabelName(ctx *LabelNameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDataDescList(ctx *DataDescListContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDataDesc(ctx *DataDescContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitFormatProps(ctx *FormatPropsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitBrokerDesc(ctx *BrokerDescContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitResourceDesc(ctx *ResourceDescContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowLoadStatement(ctx *ShowLoadStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowLoadWarningsStatement(ctx *ShowLoadWarningsStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitCancelLoadStatement(ctx *CancelLoadStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitAlterLoadStatement(ctx *AlterLoadStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitCancelCompactionStatement(ctx *CancelCompactionStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowAuthorStatement(ctx *ShowAuthorStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowBackendsStatement(ctx *ShowBackendsStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowBrokerStatement(ctx *ShowBrokerStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowCharsetStatement(ctx *ShowCharsetStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowCollationStatement(ctx *ShowCollationStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowDeleteStatement(ctx *ShowDeleteStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowDynamicPartitionStatement(ctx *ShowDynamicPartitionStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowEventsStatement(ctx *ShowEventsStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowEnginesStatement(ctx *ShowEnginesStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowFrontendsStatement(ctx *ShowFrontendsStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowPluginsStatement(ctx *ShowPluginsStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowRepositoriesStatement(ctx *ShowRepositoriesStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowOpenTableStatement(ctx *ShowOpenTableStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowPrivilegesStatement(ctx *ShowPrivilegesStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowProcedureStatement(ctx *ShowProcedureStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowProcStatement(ctx *ShowProcStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowProcesslistStatement(ctx *ShowProcesslistStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowProfilelistStatement(ctx *ShowProfilelistStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowRunningQueriesStatement(ctx *ShowRunningQueriesStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowStatusStatement(ctx *ShowStatusStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowTabletStatement(ctx *ShowTabletStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowTransactionStatement(ctx *ShowTransactionStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowTriggersStatement(ctx *ShowTriggersStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowUserPropertyStatement(ctx *ShowUserPropertyStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowVariablesStatement(ctx *ShowVariablesStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowWarningStatement(ctx *ShowWarningStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitHelpStatement(ctx *HelpStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitCreateUserStatement(ctx *CreateUserStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDropUserStatement(ctx *DropUserStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitAlterUserStatement(ctx *AlterUserStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowUserStatement(ctx *ShowUserStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowAllAuthentication(ctx *ShowAllAuthenticationContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowAuthenticationForUser(ctx *ShowAuthenticationForUserContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitExecuteAsStatement(ctx *ExecuteAsStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitCreateRoleStatement(ctx *CreateRoleStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitAlterRoleStatement(ctx *AlterRoleStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDropRoleStatement(ctx *DropRoleStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowRolesStatement(ctx *ShowRolesStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitGrantRoleToUser(ctx *GrantRoleToUserContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitGrantRoleToRole(ctx *GrantRoleToRoleContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitRevokeRoleFromUser(ctx *RevokeRoleFromUserContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitRevokeRoleFromRole(ctx *RevokeRoleFromRoleContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitSetRoleStatement(ctx *SetRoleStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitSetDefaultRoleStatement(ctx *SetDefaultRoleStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitGrantRevokeClause(ctx *GrantRevokeClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitGrantOnUser(ctx *GrantOnUserContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitGrantOnTableBrief(ctx *GrantOnTableBriefContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitGrantOnFunc(ctx *GrantOnFuncContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitGrantOnSystem(ctx *GrantOnSystemContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitGrantOnPrimaryObj(ctx *GrantOnPrimaryObjContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitGrantOnAll(ctx *GrantOnAllContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitRevokeOnUser(ctx *RevokeOnUserContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitRevokeOnTableBrief(ctx *RevokeOnTableBriefContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitRevokeOnFunc(ctx *RevokeOnFuncContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitRevokeOnSystem(ctx *RevokeOnSystemContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitRevokeOnPrimaryObj(ctx *RevokeOnPrimaryObjContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitRevokeOnAll(ctx *RevokeOnAllContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowGrantsStatement(ctx *ShowGrantsStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitAuthWithoutPlugin(ctx *AuthWithoutPluginContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitAuthWithPlugin(ctx *AuthWithPluginContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitPrivObjectName(ctx *PrivObjectNameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitPrivObjectNameList(ctx *PrivObjectNameListContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitPrivFunctionObjectNameList(ctx *PrivFunctionObjectNameListContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitPrivilegeTypeList(ctx *PrivilegeTypeListContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitPrivilegeType(ctx *PrivilegeTypeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitPrivObjectType(ctx *PrivObjectTypeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitPrivObjectTypePlural(ctx *PrivObjectTypePluralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitCreateSecurityIntegrationStatement(ctx *CreateSecurityIntegrationStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitAlterSecurityIntegrationStatement(ctx *AlterSecurityIntegrationStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDropSecurityIntegrationStatement(ctx *DropSecurityIntegrationStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowSecurityIntegrationStatement(ctx *ShowSecurityIntegrationStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowCreateSecurityIntegrationStatement(ctx *ShowCreateSecurityIntegrationStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitCreateGroupProviderStatement(ctx *CreateGroupProviderStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDropGroupProviderStatement(ctx *DropGroupProviderStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowGroupProvidersStatement(ctx *ShowGroupProvidersStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowCreateGroupProviderStatement(ctx *ShowCreateGroupProviderStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitBackupStatement(ctx *BackupStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitCancelBackupStatement(ctx *CancelBackupStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowBackupStatement(ctx *ShowBackupStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitRestoreStatement(ctx *RestoreStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitCancelRestoreStatement(ctx *CancelRestoreStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowRestoreStatement(ctx *ShowRestoreStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowSnapshotStatement(ctx *ShowSnapshotStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitCreateRepositoryStatement(ctx *CreateRepositoryStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDropRepositoryStatement(ctx *DropRepositoryStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitAddSqlBlackListStatement(ctx *AddSqlBlackListStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDelSqlBlackListStatement(ctx *DelSqlBlackListStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowSqlBlackListStatement(ctx *ShowSqlBlackListStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowWhiteListStatement(ctx *ShowWhiteListStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitAddBackendBlackListStatement(ctx *AddBackendBlackListStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDelBackendBlackListStatement(ctx *DelBackendBlackListStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowBackendBlackListStatement(ctx *ShowBackendBlackListStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDataCacheTarget(ctx *DataCacheTargetContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitCreateDataCacheRuleStatement(ctx *CreateDataCacheRuleStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowDataCacheRulesStatement(ctx *ShowDataCacheRulesStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDropDataCacheRuleStatement(ctx *DropDataCacheRuleStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitClearDataCacheRulesStatement(ctx *ClearDataCacheRulesStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDataCacheSelectStatement(ctx *DataCacheSelectStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitExportStatement(ctx *ExportStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitCancelExportStatement(ctx *CancelExportStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowExportStatement(ctx *ShowExportStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitInstallPluginStatement(ctx *InstallPluginStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitUninstallPluginStatement(ctx *UninstallPluginStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitCreateFileStatement(ctx *CreateFileStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDropFileStatement(ctx *DropFileStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowSmallFilesStatement(ctx *ShowSmallFilesStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitCreatePipeStatement(ctx *CreatePipeStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDropPipeStatement(ctx *DropPipeStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitAlterPipeClause(ctx *AlterPipeClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitAlterPipeStatement(ctx *AlterPipeStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDescPipeStatement(ctx *DescPipeStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowPipeStatement(ctx *ShowPipeStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitSetStatement(ctx *SetStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitSetNames(ctx *SetNamesContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitSetPassword(ctx *SetPasswordContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitSetUserVar(ctx *SetUserVarContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitSetSystemVar(ctx *SetSystemVarContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitSetTransaction(ctx *SetTransactionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitTransaction_characteristics(ctx *Transaction_characteristicsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitTransaction_access_mode(ctx *Transaction_access_modeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitIsolation_level(ctx *Isolation_levelContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitIsolation_types(ctx *Isolation_typesContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitSetExprOrDefault(ctx *SetExprOrDefaultContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitSetUserPropertyStatement(ctx *SetUserPropertyStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitRoleList(ctx *RoleListContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitExecuteScriptStatement(ctx *ExecuteScriptStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitUnsupportedStatement(ctx *UnsupportedStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitLock_item(ctx *Lock_itemContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitLock_type(ctx *Lock_typeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitAlterPlanAdvisorAddStatement(ctx *AlterPlanAdvisorAddStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitTruncatePlanAdvisorStatement(ctx *TruncatePlanAdvisorStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitAlterPlanAdvisorDropStatement(ctx *AlterPlanAdvisorDropStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowPlanAdvisorStatement(ctx *ShowPlanAdvisorStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitCreateWarehouseStatement(ctx *CreateWarehouseStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDropWarehouseStatement(ctx *DropWarehouseStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitSuspendWarehouseStatement(ctx *SuspendWarehouseStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitResumeWarehouseStatement(ctx *ResumeWarehouseStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitSetWarehouseStatement(ctx *SetWarehouseStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowWarehousesStatement(ctx *ShowWarehousesStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowClustersStatement(ctx *ShowClustersStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitShowNodesStatement(ctx *ShowNodesStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitAlterWarehouseStatement(ctx *AlterWarehouseStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitBeginStatement(ctx *BeginStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitCommitStatement(ctx *CommitStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitRollbackStatement(ctx *RollbackStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitTranslateStatement(ctx *TranslateStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDialect(ctx *DialectContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitTranslateSQL(ctx *TranslateSQLContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitQueryStatement(ctx *QueryStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitQueryRelation(ctx *QueryRelationContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitWithClause(ctx *WithClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitQueryNoWith(ctx *QueryNoWithContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitQueryPeriod(ctx *QueryPeriodContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitPeriodType(ctx *PeriodTypeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitQueryWithParentheses(ctx *QueryWithParenthesesContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitSetOperation(ctx *SetOperationContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitQueryPrimaryDefault(ctx *QueryPrimaryDefaultContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitSubquery(ctx *SubqueryContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitRowConstructor(ctx *RowConstructorContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitSortItem(ctx *SortItemContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitLimitConstExpr(ctx *LimitConstExprContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitLimitElement(ctx *LimitElementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitQuerySpecification(ctx *QuerySpecificationContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitFrom(ctx *FromContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDual(ctx *DualContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitRollup(ctx *RollupContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitCube(ctx *CubeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitMultipleGroupingSets(ctx *MultipleGroupingSetsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitSingleGroupingSet(ctx *SingleGroupingSetContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitGroupingSet(ctx *GroupingSetContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitCommonTableExpression(ctx *CommonTableExpressionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitSetQuantifier(ctx *SetQuantifierContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitSelectSingle(ctx *SelectSingleContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitSelectAll(ctx *SelectAllContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitRelations(ctx *RelationsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitRelation(ctx *RelationContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitTableAtom(ctx *TableAtomContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitInlineTable(ctx *InlineTableContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitSubqueryWithAlias(ctx *SubqueryWithAliasContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitTableFunction(ctx *TableFunctionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitNormalizedTableFunction(ctx *NormalizedTableFunctionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitFileTableFunction(ctx *FileTableFunctionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitParenthesizedRelation(ctx *ParenthesizedRelationContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitPivotClause(ctx *PivotClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitPivotAggregationExpression(ctx *PivotAggregationExpressionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitPivotValue(ctx *PivotValueContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitSampleClause(ctx *SampleClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitArgumentList(ctx *ArgumentListContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitNamedArgumentList(ctx *NamedArgumentListContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitNamedArguments(ctx *NamedArgumentsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitJoinRelation(ctx *JoinRelationContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitCrossOrInnerJoinType(ctx *CrossOrInnerJoinTypeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitOuterAndSemiJoinType(ctx *OuterAndSemiJoinTypeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitBracketHint(ctx *BracketHintContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitHintMap(ctx *HintMapContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitJoinCriteria(ctx *JoinCriteriaContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitColumnAliases(ctx *ColumnAliasesContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitPartitionNames(ctx *PartitionNamesContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitKeyPartitionList(ctx *KeyPartitionListContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitTabletList(ctx *TabletListContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitPrepareStatement(ctx *PrepareStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitPrepareSql(ctx *PrepareSqlContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitExecuteStatement(ctx *ExecuteStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDeallocateStatement(ctx *DeallocateStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitReplicaList(ctx *ReplicaListContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitExpressionsWithDefault(ctx *ExpressionsWithDefaultContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitExpressionOrDefault(ctx *ExpressionOrDefaultContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitMapExpressionList(ctx *MapExpressionListContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitMapExpression(ctx *MapExpressionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitExpressionSingleton(ctx *ExpressionSingletonContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitExpressionDefault(ctx *ExpressionDefaultContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitLogicalNot(ctx *LogicalNotContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitLogicalBinary(ctx *LogicalBinaryContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitExpressionList(ctx *ExpressionListContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitComparison(ctx *ComparisonContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitBooleanExpressionDefault(ctx *BooleanExpressionDefaultContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitIsNull(ctx *IsNullContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitScalarSubquery(ctx *ScalarSubqueryContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitPredicate(ctx *PredicateContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitTupleInSubquery(ctx *TupleInSubqueryContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitInIntegerList(ctx *InIntegerListContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitInStringList(ctx *InStringListContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitInSubquery(ctx *InSubqueryContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitInList(ctx *InListContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitBetween(ctx *BetweenContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitLike(ctx *LikeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitValueExpressionDefault(ctx *ValueExpressionDefaultContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitArithmeticBinary(ctx *ArithmeticBinaryContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDereference(ctx *DereferenceContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitOdbcFunctionCallExpression(ctx *OdbcFunctionCallExpressionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitMatchExpr(ctx *MatchExprContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitColumnRef(ctx *ColumnRefContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitConvert(ctx *ConvertContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitCollectionSubscript(ctx *CollectionSubscriptContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitLiteral(ctx *LiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitCast(ctx *CastContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitParenthesizedExpression(ctx *ParenthesizedExpressionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitUserVariableExpression(ctx *UserVariableExpressionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitFunctionCallExpression(ctx *FunctionCallExpressionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitSimpleCase(ctx *SimpleCaseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitArrowExpression(ctx *ArrowExpressionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitSystemVariableExpression(ctx *SystemVariableExpressionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitConcat(ctx *ConcatContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitSubqueryExpression(ctx *SubqueryExpressionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitLambdaFunctionExpr(ctx *LambdaFunctionExprContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDictionaryGetExpr(ctx *DictionaryGetExprContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitCollate(ctx *CollateContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitArrayConstructor(ctx *ArrayConstructorContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitMapConstructor(ctx *MapConstructorContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitArraySlice(ctx *ArraySliceContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitExists(ctx *ExistsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitSearchedCase(ctx *SearchedCaseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitArithmeticUnary(ctx *ArithmeticUnaryContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitNumericLiteral(ctx *NumericLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitNullLiteral(ctx *NullLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitBooleanLiteral(ctx *BooleanLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDateLiteral(ctx *DateLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitStringLiteral(ctx *StringLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitIntervalLiteral(ctx *IntervalLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitUnitBoundaryLiteral(ctx *UnitBoundaryLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitBinaryLiteral(ctx *BinaryLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitParameter(ctx *ParameterContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitExtract(ctx *ExtractContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitGroupingOperation(ctx *GroupingOperationContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitInformationFunction(ctx *InformationFunctionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitSpecialDateTime(ctx *SpecialDateTimeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitSpecialFunction(ctx *SpecialFunctionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitAggregationFunctionCall(ctx *AggregationFunctionCallContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitWindowFunctionCall(ctx *WindowFunctionCallContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitTranslateFunctionCall(ctx *TranslateFunctionCallContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitSimpleFunctionCall(ctx *SimpleFunctionCallContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitAggregationFunction(ctx *AggregationFunctionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitUserVariable(ctx *UserVariableContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitSystemVariable(ctx *SystemVariableContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitColumnReference(ctx *ColumnReferenceContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitInformationFunctionExpression(ctx *InformationFunctionExpressionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitSpecialDateTimeExpression(ctx *SpecialDateTimeExpressionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitSpecialFunctionExpression(ctx *SpecialFunctionExpressionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitWindowFunction(ctx *WindowFunctionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitWhenClause(ctx *WhenClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitOver(ctx *OverContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitIgnoreNulls(ctx *IgnoreNullsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitWindowFrame(ctx *WindowFrameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitUnboundedFrame(ctx *UnboundedFrameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitCurrentRowBound(ctx *CurrentRowBoundContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitBoundedFrame(ctx *BoundedFrameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitBackupRestoreObjectDesc(ctx *BackupRestoreObjectDescContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitTableDesc(ctx *TableDescContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitBackupRestoreTableDesc(ctx *BackupRestoreTableDescContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitExplainDesc(ctx *ExplainDescContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitOptimizerTrace(ctx *OptimizerTraceContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitPartitionExpr(ctx *PartitionExprContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitPartitionDesc(ctx *PartitionDescContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitListPartitionDesc(ctx *ListPartitionDescContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitSingleItemListPartitionDesc(ctx *SingleItemListPartitionDescContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitMultiItemListPartitionDesc(ctx *MultiItemListPartitionDescContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitMultiListPartitionValues(ctx *MultiListPartitionValuesContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitSingleListPartitionValues(ctx *SingleListPartitionValuesContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitListPartitionValues(ctx *ListPartitionValuesContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitListPartitionValue(ctx *ListPartitionValueContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitStringList(ctx *StringListContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitIntegerList(ctx *IntegerListContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitLiteralExpressionList(ctx *LiteralExpressionListContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitRangePartitionDesc(ctx *RangePartitionDescContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitSingleRangePartition(ctx *SingleRangePartitionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitMultiRangePartition(ctx *MultiRangePartitionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitPartitionRangeDesc(ctx *PartitionRangeDescContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitPartitionKeyDesc(ctx *PartitionKeyDescContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitPartitionValueList(ctx *PartitionValueListContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitKeyPartition(ctx *KeyPartitionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitPartitionValue(ctx *PartitionValueContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDistributionClause(ctx *DistributionClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDistributionDesc(ctx *DistributionDescContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitAlterModifyDefaultBuckets(ctx *AlterModifyDefaultBucketsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitRefreshSchemeDesc(ctx *RefreshSchemeDescContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitStatusDesc(ctx *StatusDescContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitProperties(ctx *PropertiesContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitExtProperties(ctx *ExtPropertiesContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitPropertyList(ctx *PropertyListContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitUserPropertyList(ctx *UserPropertyListContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitProperty(ctx *PropertyContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitInlineProperties(ctx *InlinePropertiesContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitInlineProperty(ctx *InlinePropertyContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitVarType(ctx *VarTypeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitComment(ctx *CommentContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitOutfile(ctx *OutfileContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitFileFormat(ctx *FileFormatContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitString(ctx *StringContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitBinary(ctx *BinaryContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitComparisonOperator(ctx *ComparisonOperatorContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitBooleanValue(ctx *BooleanValueContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitInterval(ctx *IntervalContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitTaskInterval(ctx *TaskIntervalContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitTaskUnitIdentifier(ctx *TaskUnitIdentifierContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitUnitIdentifier(ctx *UnitIdentifierContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitUnitBoundary(ctx *UnitBoundaryContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitType(ctx *TypeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitArrayType(ctx *ArrayTypeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitMapType(ctx *MapTypeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitSubfieldDesc(ctx *SubfieldDescContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitSubfieldDescs(ctx *SubfieldDescsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitStructType(ctx *StructTypeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitTypeParameter(ctx *TypeParameterContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitBaseType(ctx *BaseTypeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDecimalType(ctx *DecimalTypeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitQualifiedName(ctx *QualifiedNameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitTableName(ctx *TableNameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitWriteBranch(ctx *WriteBranchContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitUnquotedIdentifier(ctx *UnquotedIdentifierContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDigitIdentifier(ctx *DigitIdentifierContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitBackQuotedIdentifier(ctx *BackQuotedIdentifierContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitIdentifierWithAlias(ctx *IdentifierWithAliasContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitIdentifierWithAliasList(ctx *IdentifierWithAliasListContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitIdentifierList(ctx *IdentifierListContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitIdentifierOrString(ctx *IdentifierOrStringContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitIdentifierOrStringList(ctx *IdentifierOrStringListContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitIdentifierOrStringOrStar(ctx *IdentifierOrStringOrStarContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitUserWithoutHost(ctx *UserWithoutHostContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitUserWithHost(ctx *UserWithHostContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitUserWithHostAndBlanket(ctx *UserWithHostAndBlanketContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitAssignment(ctx *AssignmentContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitAssignmentList(ctx *AssignmentListContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitIntegerValue(ctx *IntegerValueContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDecimalValue(ctx *DecimalValueContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitDoubleValue(ctx *DoubleValueContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseStarRocksVisitor) VisitNonReserved(ctx *NonReservedContext) interface{} {
	return v.VisitChildren(ctx)
}
