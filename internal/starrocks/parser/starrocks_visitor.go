// Code generated from StarRocks.g4 by ANTLR 4.13.2. DO NOT EDIT.

package parser // StarRocks
import "github.com/antlr4-go/antlr/v4"

// A complete Visitor for a parse tree produced by StarRocksParser.
type StarRocksVisitor interface {
	antlr.ParseTreeVisitor

	// Visit a parse tree produced by StarRocksParser#sqlStatements.
	VisitSqlStatements(ctx *SqlStatementsContext) interface{}

	// Visit a parse tree produced by StarRocksParser#singleStatement.
	VisitSingleStatement(ctx *SingleStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#emptyStatement.
	VisitEmptyStatement(ctx *EmptyStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#statement.
	VisitStatement(ctx *StatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#useDatabaseStatement.
	VisitUseDatabaseStatement(ctx *UseDatabaseStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#useCatalogStatement.
	VisitUseCatalogStatement(ctx *UseCatalogStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#setCatalogStatement.
	VisitSetCatalogStatement(ctx *SetCatalogStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showDatabasesStatement.
	VisitShowDatabasesStatement(ctx *ShowDatabasesStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#alterDbQuotaStatement.
	VisitAlterDbQuotaStatement(ctx *AlterDbQuotaStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#createDbStatement.
	VisitCreateDbStatement(ctx *CreateDbStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#dropDbStatement.
	VisitDropDbStatement(ctx *DropDbStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showCreateDbStatement.
	VisitShowCreateDbStatement(ctx *ShowCreateDbStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#alterDatabaseRenameStatement.
	VisitAlterDatabaseRenameStatement(ctx *AlterDatabaseRenameStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#recoverDbStmt.
	VisitRecoverDbStmt(ctx *RecoverDbStmtContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showDataStmt.
	VisitShowDataStmt(ctx *ShowDataStmtContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showDataDistributionStmt.
	VisitShowDataDistributionStmt(ctx *ShowDataDistributionStmtContext) interface{}

	// Visit a parse tree produced by StarRocksParser#createTableStatement.
	VisitCreateTableStatement(ctx *CreateTableStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#columnDesc.
	VisitColumnDesc(ctx *ColumnDescContext) interface{}

	// Visit a parse tree produced by StarRocksParser#charsetName.
	VisitCharsetName(ctx *CharsetNameContext) interface{}

	// Visit a parse tree produced by StarRocksParser#defaultDesc.
	VisitDefaultDesc(ctx *DefaultDescContext) interface{}

	// Visit a parse tree produced by StarRocksParser#generatedColumnDesc.
	VisitGeneratedColumnDesc(ctx *GeneratedColumnDescContext) interface{}

	// Visit a parse tree produced by StarRocksParser#indexDesc.
	VisitIndexDesc(ctx *IndexDescContext) interface{}

	// Visit a parse tree produced by StarRocksParser#engineDesc.
	VisitEngineDesc(ctx *EngineDescContext) interface{}

	// Visit a parse tree produced by StarRocksParser#charsetDesc.
	VisitCharsetDesc(ctx *CharsetDescContext) interface{}

	// Visit a parse tree produced by StarRocksParser#collateDesc.
	VisitCollateDesc(ctx *CollateDescContext) interface{}

	// Visit a parse tree produced by StarRocksParser#keyDesc.
	VisitKeyDesc(ctx *KeyDescContext) interface{}

	// Visit a parse tree produced by StarRocksParser#orderByDesc.
	VisitOrderByDesc(ctx *OrderByDescContext) interface{}

	// Visit a parse tree produced by StarRocksParser#columnNullable.
	VisitColumnNullable(ctx *ColumnNullableContext) interface{}

	// Visit a parse tree produced by StarRocksParser#typeWithNullable.
	VisitTypeWithNullable(ctx *TypeWithNullableContext) interface{}

	// Visit a parse tree produced by StarRocksParser#aggStateDesc.
	VisitAggStateDesc(ctx *AggStateDescContext) interface{}

	// Visit a parse tree produced by StarRocksParser#aggDesc.
	VisitAggDesc(ctx *AggDescContext) interface{}

	// Visit a parse tree produced by StarRocksParser#rollupDesc.
	VisitRollupDesc(ctx *RollupDescContext) interface{}

	// Visit a parse tree produced by StarRocksParser#rollupItem.
	VisitRollupItem(ctx *RollupItemContext) interface{}

	// Visit a parse tree produced by StarRocksParser#dupKeys.
	VisitDupKeys(ctx *DupKeysContext) interface{}

	// Visit a parse tree produced by StarRocksParser#fromRollup.
	VisitFromRollup(ctx *FromRollupContext) interface{}

	// Visit a parse tree produced by StarRocksParser#orReplace.
	VisitOrReplace(ctx *OrReplaceContext) interface{}

	// Visit a parse tree produced by StarRocksParser#ifNotExists.
	VisitIfNotExists(ctx *IfNotExistsContext) interface{}

	// Visit a parse tree produced by StarRocksParser#createTableAsSelectStatement.
	VisitCreateTableAsSelectStatement(ctx *CreateTableAsSelectStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#dropTableStatement.
	VisitDropTableStatement(ctx *DropTableStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#cleanTemporaryTableStatement.
	VisitCleanTemporaryTableStatement(ctx *CleanTemporaryTableStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#alterTableStatement.
	VisitAlterTableStatement(ctx *AlterTableStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#createIndexStatement.
	VisitCreateIndexStatement(ctx *CreateIndexStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#dropIndexStatement.
	VisitDropIndexStatement(ctx *DropIndexStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#indexType.
	VisitIndexType(ctx *IndexTypeContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showTableStatement.
	VisitShowTableStatement(ctx *ShowTableStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showTemporaryTablesStatement.
	VisitShowTemporaryTablesStatement(ctx *ShowTemporaryTablesStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showCreateTableStatement.
	VisitShowCreateTableStatement(ctx *ShowCreateTableStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showColumnStatement.
	VisitShowColumnStatement(ctx *ShowColumnStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showTableStatusStatement.
	VisitShowTableStatusStatement(ctx *ShowTableStatusStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#refreshTableStatement.
	VisitRefreshTableStatement(ctx *RefreshTableStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showAlterStatement.
	VisitShowAlterStatement(ctx *ShowAlterStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#descTableStatement.
	VisitDescTableStatement(ctx *DescTableStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#createTableLikeStatement.
	VisitCreateTableLikeStatement(ctx *CreateTableLikeStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showIndexStatement.
	VisitShowIndexStatement(ctx *ShowIndexStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#recoverTableStatement.
	VisitRecoverTableStatement(ctx *RecoverTableStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#truncateTableStatement.
	VisitTruncateTableStatement(ctx *TruncateTableStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#cancelAlterTableStatement.
	VisitCancelAlterTableStatement(ctx *CancelAlterTableStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showPartitionsStatement.
	VisitShowPartitionsStatement(ctx *ShowPartitionsStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#recoverPartitionStatement.
	VisitRecoverPartitionStatement(ctx *RecoverPartitionStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#createViewStatement.
	VisitCreateViewStatement(ctx *CreateViewStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#alterViewStatement.
	VisitAlterViewStatement(ctx *AlterViewStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#dropViewStatement.
	VisitDropViewStatement(ctx *DropViewStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#columnNameWithComment.
	VisitColumnNameWithComment(ctx *ColumnNameWithCommentContext) interface{}

	// Visit a parse tree produced by StarRocksParser#submitTaskStatement.
	VisitSubmitTaskStatement(ctx *SubmitTaskStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#taskClause.
	VisitTaskClause(ctx *TaskClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#dropTaskStatement.
	VisitDropTaskStatement(ctx *DropTaskStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#taskScheduleDesc.
	VisitTaskScheduleDesc(ctx *TaskScheduleDescContext) interface{}

	// Visit a parse tree produced by StarRocksParser#createMaterializedViewStatement.
	VisitCreateMaterializedViewStatement(ctx *CreateMaterializedViewStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#mvPartitionExprs.
	VisitMvPartitionExprs(ctx *MvPartitionExprsContext) interface{}

	// Visit a parse tree produced by StarRocksParser#materializedViewDesc.
	VisitMaterializedViewDesc(ctx *MaterializedViewDescContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showMaterializedViewsStatement.
	VisitShowMaterializedViewsStatement(ctx *ShowMaterializedViewsStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#dropMaterializedViewStatement.
	VisitDropMaterializedViewStatement(ctx *DropMaterializedViewStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#alterMaterializedViewStatement.
	VisitAlterMaterializedViewStatement(ctx *AlterMaterializedViewStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#refreshMaterializedViewStatement.
	VisitRefreshMaterializedViewStatement(ctx *RefreshMaterializedViewStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#cancelRefreshMaterializedViewStatement.
	VisitCancelRefreshMaterializedViewStatement(ctx *CancelRefreshMaterializedViewStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#adminSetConfigStatement.
	VisitAdminSetConfigStatement(ctx *AdminSetConfigStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#adminSetReplicaStatusStatement.
	VisitAdminSetReplicaStatusStatement(ctx *AdminSetReplicaStatusStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#adminShowConfigStatement.
	VisitAdminShowConfigStatement(ctx *AdminShowConfigStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#adminShowReplicaDistributionStatement.
	VisitAdminShowReplicaDistributionStatement(ctx *AdminShowReplicaDistributionStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#adminShowReplicaStatusStatement.
	VisitAdminShowReplicaStatusStatement(ctx *AdminShowReplicaStatusStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#adminRepairTableStatement.
	VisitAdminRepairTableStatement(ctx *AdminRepairTableStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#adminCancelRepairTableStatement.
	VisitAdminCancelRepairTableStatement(ctx *AdminCancelRepairTableStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#adminCheckTabletsStatement.
	VisitAdminCheckTabletsStatement(ctx *AdminCheckTabletsStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#adminSetPartitionVersion.
	VisitAdminSetPartitionVersion(ctx *AdminSetPartitionVersionContext) interface{}

	// Visit a parse tree produced by StarRocksParser#killStatement.
	VisitKillStatement(ctx *KillStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#syncStatement.
	VisitSyncStatement(ctx *SyncStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#adminSetAutomatedSnapshotOnStatement.
	VisitAdminSetAutomatedSnapshotOnStatement(ctx *AdminSetAutomatedSnapshotOnStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#adminSetAutomatedSnapshotOffStatement.
	VisitAdminSetAutomatedSnapshotOffStatement(ctx *AdminSetAutomatedSnapshotOffStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#alterSystemStatement.
	VisitAlterSystemStatement(ctx *AlterSystemStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#cancelAlterSystemStatement.
	VisitCancelAlterSystemStatement(ctx *CancelAlterSystemStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showComputeNodesStatement.
	VisitShowComputeNodesStatement(ctx *ShowComputeNodesStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#createExternalCatalogStatement.
	VisitCreateExternalCatalogStatement(ctx *CreateExternalCatalogStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showCreateExternalCatalogStatement.
	VisitShowCreateExternalCatalogStatement(ctx *ShowCreateExternalCatalogStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#dropExternalCatalogStatement.
	VisitDropExternalCatalogStatement(ctx *DropExternalCatalogStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showCatalogsStatement.
	VisitShowCatalogsStatement(ctx *ShowCatalogsStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#alterCatalogStatement.
	VisitAlterCatalogStatement(ctx *AlterCatalogStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#createStorageVolumeStatement.
	VisitCreateStorageVolumeStatement(ctx *CreateStorageVolumeStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#typeDesc.
	VisitTypeDesc(ctx *TypeDescContext) interface{}

	// Visit a parse tree produced by StarRocksParser#locationsDesc.
	VisitLocationsDesc(ctx *LocationsDescContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showStorageVolumesStatement.
	VisitShowStorageVolumesStatement(ctx *ShowStorageVolumesStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#dropStorageVolumeStatement.
	VisitDropStorageVolumeStatement(ctx *DropStorageVolumeStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#alterStorageVolumeStatement.
	VisitAlterStorageVolumeStatement(ctx *AlterStorageVolumeStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#alterStorageVolumeClause.
	VisitAlterStorageVolumeClause(ctx *AlterStorageVolumeClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#modifyStorageVolumePropertiesClause.
	VisitModifyStorageVolumePropertiesClause(ctx *ModifyStorageVolumePropertiesClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#modifyStorageVolumeCommentClause.
	VisitModifyStorageVolumeCommentClause(ctx *ModifyStorageVolumeCommentClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#descStorageVolumeStatement.
	VisitDescStorageVolumeStatement(ctx *DescStorageVolumeStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#setDefaultStorageVolumeStatement.
	VisitSetDefaultStorageVolumeStatement(ctx *SetDefaultStorageVolumeStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#updateFailPointStatusStatement.
	VisitUpdateFailPointStatusStatement(ctx *UpdateFailPointStatusStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showFailPointStatement.
	VisitShowFailPointStatement(ctx *ShowFailPointStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#createDictionaryStatement.
	VisitCreateDictionaryStatement(ctx *CreateDictionaryStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#dropDictionaryStatement.
	VisitDropDictionaryStatement(ctx *DropDictionaryStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#refreshDictionaryStatement.
	VisitRefreshDictionaryStatement(ctx *RefreshDictionaryStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showDictionaryStatement.
	VisitShowDictionaryStatement(ctx *ShowDictionaryStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#cancelRefreshDictionaryStatement.
	VisitCancelRefreshDictionaryStatement(ctx *CancelRefreshDictionaryStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#dictionaryColumnDesc.
	VisitDictionaryColumnDesc(ctx *DictionaryColumnDescContext) interface{}

	// Visit a parse tree produced by StarRocksParser#dictionaryName.
	VisitDictionaryName(ctx *DictionaryNameContext) interface{}

	// Visit a parse tree produced by StarRocksParser#alterClause.
	VisitAlterClause(ctx *AlterClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#addFrontendClause.
	VisitAddFrontendClause(ctx *AddFrontendClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#dropFrontendClause.
	VisitDropFrontendClause(ctx *DropFrontendClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#modifyFrontendHostClause.
	VisitModifyFrontendHostClause(ctx *ModifyFrontendHostClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#addBackendClause.
	VisitAddBackendClause(ctx *AddBackendClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#dropBackendClause.
	VisitDropBackendClause(ctx *DropBackendClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#decommissionBackendClause.
	VisitDecommissionBackendClause(ctx *DecommissionBackendClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#modifyBackendClause.
	VisitModifyBackendClause(ctx *ModifyBackendClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#addComputeNodeClause.
	VisitAddComputeNodeClause(ctx *AddComputeNodeClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#dropComputeNodeClause.
	VisitDropComputeNodeClause(ctx *DropComputeNodeClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#modifyBrokerClause.
	VisitModifyBrokerClause(ctx *ModifyBrokerClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#alterLoadErrorUrlClause.
	VisitAlterLoadErrorUrlClause(ctx *AlterLoadErrorUrlClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#createImageClause.
	VisitCreateImageClause(ctx *CreateImageClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#cleanTabletSchedQClause.
	VisitCleanTabletSchedQClause(ctx *CleanTabletSchedQClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#decommissionDiskClause.
	VisitDecommissionDiskClause(ctx *DecommissionDiskClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#cancelDecommissionDiskClause.
	VisitCancelDecommissionDiskClause(ctx *CancelDecommissionDiskClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#disableDiskClause.
	VisitDisableDiskClause(ctx *DisableDiskClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#cancelDisableDiskClause.
	VisitCancelDisableDiskClause(ctx *CancelDisableDiskClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#createIndexClause.
	VisitCreateIndexClause(ctx *CreateIndexClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#dropIndexClause.
	VisitDropIndexClause(ctx *DropIndexClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#tableRenameClause.
	VisitTableRenameClause(ctx *TableRenameClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#swapTableClause.
	VisitSwapTableClause(ctx *SwapTableClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#modifyPropertiesClause.
	VisitModifyPropertiesClause(ctx *ModifyPropertiesClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#modifyCommentClause.
	VisitModifyCommentClause(ctx *ModifyCommentClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#optimizeRange.
	VisitOptimizeRange(ctx *OptimizeRangeContext) interface{}

	// Visit a parse tree produced by StarRocksParser#optimizeClause.
	VisitOptimizeClause(ctx *OptimizeClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#addColumnClause.
	VisitAddColumnClause(ctx *AddColumnClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#addColumnsClause.
	VisitAddColumnsClause(ctx *AddColumnsClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#dropColumnClause.
	VisitDropColumnClause(ctx *DropColumnClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#modifyColumnClause.
	VisitModifyColumnClause(ctx *ModifyColumnClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#modifyColumnCommentClause.
	VisitModifyColumnCommentClause(ctx *ModifyColumnCommentClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#columnRenameClause.
	VisitColumnRenameClause(ctx *ColumnRenameClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#reorderColumnsClause.
	VisitReorderColumnsClause(ctx *ReorderColumnsClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#rollupRenameClause.
	VisitRollupRenameClause(ctx *RollupRenameClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#compactionClause.
	VisitCompactionClause(ctx *CompactionClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#subfieldName.
	VisitSubfieldName(ctx *SubfieldNameContext) interface{}

	// Visit a parse tree produced by StarRocksParser#nestedFieldName.
	VisitNestedFieldName(ctx *NestedFieldNameContext) interface{}

	// Visit a parse tree produced by StarRocksParser#addFieldClause.
	VisitAddFieldClause(ctx *AddFieldClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#dropFieldClause.
	VisitDropFieldClause(ctx *DropFieldClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#createOrReplaceTagClause.
	VisitCreateOrReplaceTagClause(ctx *CreateOrReplaceTagClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#createOrReplaceBranchClause.
	VisitCreateOrReplaceBranchClause(ctx *CreateOrReplaceBranchClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#dropBranchClause.
	VisitDropBranchClause(ctx *DropBranchClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#dropTagClause.
	VisitDropTagClause(ctx *DropTagClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#tableOperationClause.
	VisitTableOperationClause(ctx *TableOperationClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#tagOptions.
	VisitTagOptions(ctx *TagOptionsContext) interface{}

	// Visit a parse tree produced by StarRocksParser#branchOptions.
	VisitBranchOptions(ctx *BranchOptionsContext) interface{}

	// Visit a parse tree produced by StarRocksParser#snapshotRetention.
	VisitSnapshotRetention(ctx *SnapshotRetentionContext) interface{}

	// Visit a parse tree produced by StarRocksParser#refRetain.
	VisitRefRetain(ctx *RefRetainContext) interface{}

	// Visit a parse tree produced by StarRocksParser#maxSnapshotAge.
	VisitMaxSnapshotAge(ctx *MaxSnapshotAgeContext) interface{}

	// Visit a parse tree produced by StarRocksParser#minSnapshotsToKeep.
	VisitMinSnapshotsToKeep(ctx *MinSnapshotsToKeepContext) interface{}

	// Visit a parse tree produced by StarRocksParser#snapshotId.
	VisitSnapshotId(ctx *SnapshotIdContext) interface{}

	// Visit a parse tree produced by StarRocksParser#timeUnit.
	VisitTimeUnit(ctx *TimeUnitContext) interface{}

	// Visit a parse tree produced by StarRocksParser#integer_list.
	VisitInteger_list(ctx *Integer_listContext) interface{}

	// Visit a parse tree produced by StarRocksParser#dropPersistentIndexClause.
	VisitDropPersistentIndexClause(ctx *DropPersistentIndexClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#alterTableAutoIncrementClause.
	VisitAlterTableAutoIncrementClause(ctx *AlterTableAutoIncrementClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#addPartitionClause.
	VisitAddPartitionClause(ctx *AddPartitionClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#dropPartitionClause.
	VisitDropPartitionClause(ctx *DropPartitionClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#truncatePartitionClause.
	VisitTruncatePartitionClause(ctx *TruncatePartitionClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#modifyPartitionClause.
	VisitModifyPartitionClause(ctx *ModifyPartitionClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#replacePartitionClause.
	VisitReplacePartitionClause(ctx *ReplacePartitionClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#partitionRenameClause.
	VisitPartitionRenameClause(ctx *PartitionRenameClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#insertStatement.
	VisitInsertStatement(ctx *InsertStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#insertLabelOrColumnAliases.
	VisitInsertLabelOrColumnAliases(ctx *InsertLabelOrColumnAliasesContext) interface{}

	// Visit a parse tree produced by StarRocksParser#columnAliasesOrByName.
	VisitColumnAliasesOrByName(ctx *ColumnAliasesOrByNameContext) interface{}

	// Visit a parse tree produced by StarRocksParser#updateStatement.
	VisitUpdateStatement(ctx *UpdateStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#deleteStatement.
	VisitDeleteStatement(ctx *DeleteStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#createRoutineLoadStatement.
	VisitCreateRoutineLoadStatement(ctx *CreateRoutineLoadStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#alterRoutineLoadStatement.
	VisitAlterRoutineLoadStatement(ctx *AlterRoutineLoadStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#dataSource.
	VisitDataSource(ctx *DataSourceContext) interface{}

	// Visit a parse tree produced by StarRocksParser#loadProperties.
	VisitLoadProperties(ctx *LoadPropertiesContext) interface{}

	// Visit a parse tree produced by StarRocksParser#colSeparatorProperty.
	VisitColSeparatorProperty(ctx *ColSeparatorPropertyContext) interface{}

	// Visit a parse tree produced by StarRocksParser#rowDelimiterProperty.
	VisitRowDelimiterProperty(ctx *RowDelimiterPropertyContext) interface{}

	// Visit a parse tree produced by StarRocksParser#importColumns.
	VisitImportColumns(ctx *ImportColumnsContext) interface{}

	// Visit a parse tree produced by StarRocksParser#columnProperties.
	VisitColumnProperties(ctx *ColumnPropertiesContext) interface{}

	// Visit a parse tree produced by StarRocksParser#jobProperties.
	VisitJobProperties(ctx *JobPropertiesContext) interface{}

	// Visit a parse tree produced by StarRocksParser#dataSourceProperties.
	VisitDataSourceProperties(ctx *DataSourcePropertiesContext) interface{}

	// Visit a parse tree produced by StarRocksParser#stopRoutineLoadStatement.
	VisitStopRoutineLoadStatement(ctx *StopRoutineLoadStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#resumeRoutineLoadStatement.
	VisitResumeRoutineLoadStatement(ctx *ResumeRoutineLoadStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#pauseRoutineLoadStatement.
	VisitPauseRoutineLoadStatement(ctx *PauseRoutineLoadStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showRoutineLoadStatement.
	VisitShowRoutineLoadStatement(ctx *ShowRoutineLoadStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showRoutineLoadTaskStatement.
	VisitShowRoutineLoadTaskStatement(ctx *ShowRoutineLoadTaskStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showCreateRoutineLoadStatement.
	VisitShowCreateRoutineLoadStatement(ctx *ShowCreateRoutineLoadStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showStreamLoadStatement.
	VisitShowStreamLoadStatement(ctx *ShowStreamLoadStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#analyzeStatement.
	VisitAnalyzeStatement(ctx *AnalyzeStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#regularColumns.
	VisitRegularColumns(ctx *RegularColumnsContext) interface{}

	// Visit a parse tree produced by StarRocksParser#allColumns.
	VisitAllColumns(ctx *AllColumnsContext) interface{}

	// Visit a parse tree produced by StarRocksParser#predicateColumns.
	VisitPredicateColumns(ctx *PredicateColumnsContext) interface{}

	// Visit a parse tree produced by StarRocksParser#multiColumnSet.
	VisitMultiColumnSet(ctx *MultiColumnSetContext) interface{}

	// Visit a parse tree produced by StarRocksParser#dropStatsStatement.
	VisitDropStatsStatement(ctx *DropStatsStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#histogramStatement.
	VisitHistogramStatement(ctx *HistogramStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#analyzeHistogramStatement.
	VisitAnalyzeHistogramStatement(ctx *AnalyzeHistogramStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#dropHistogramStatement.
	VisitDropHistogramStatement(ctx *DropHistogramStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#createAnalyzeStatement.
	VisitCreateAnalyzeStatement(ctx *CreateAnalyzeStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#dropAnalyzeJobStatement.
	VisitDropAnalyzeJobStatement(ctx *DropAnalyzeJobStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showAnalyzeStatement.
	VisitShowAnalyzeStatement(ctx *ShowAnalyzeStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showStatsMetaStatement.
	VisitShowStatsMetaStatement(ctx *ShowStatsMetaStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showHistogramMetaStatement.
	VisitShowHistogramMetaStatement(ctx *ShowHistogramMetaStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#killAnalyzeStatement.
	VisitKillAnalyzeStatement(ctx *KillAnalyzeStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#analyzeProfileStatement.
	VisitAnalyzeProfileStatement(ctx *AnalyzeProfileStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#createBaselinePlanStatement.
	VisitCreateBaselinePlanStatement(ctx *CreateBaselinePlanStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#dropBaselinePlanStatement.
	VisitDropBaselinePlanStatement(ctx *DropBaselinePlanStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showBaselinePlanStatement.
	VisitShowBaselinePlanStatement(ctx *ShowBaselinePlanStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#createResourceGroupStatement.
	VisitCreateResourceGroupStatement(ctx *CreateResourceGroupStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#dropResourceGroupStatement.
	VisitDropResourceGroupStatement(ctx *DropResourceGroupStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#alterResourceGroupStatement.
	VisitAlterResourceGroupStatement(ctx *AlterResourceGroupStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showResourceGroupStatement.
	VisitShowResourceGroupStatement(ctx *ShowResourceGroupStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showResourceGroupUsageStatement.
	VisitShowResourceGroupUsageStatement(ctx *ShowResourceGroupUsageStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#createResourceStatement.
	VisitCreateResourceStatement(ctx *CreateResourceStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#alterResourceStatement.
	VisitAlterResourceStatement(ctx *AlterResourceStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#dropResourceStatement.
	VisitDropResourceStatement(ctx *DropResourceStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showResourceStatement.
	VisitShowResourceStatement(ctx *ShowResourceStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#classifier.
	VisitClassifier(ctx *ClassifierContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showFunctionsStatement.
	VisitShowFunctionsStatement(ctx *ShowFunctionsStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#dropFunctionStatement.
	VisitDropFunctionStatement(ctx *DropFunctionStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#createFunctionStatement.
	VisitCreateFunctionStatement(ctx *CreateFunctionStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#inlineFunction.
	VisitInlineFunction(ctx *InlineFunctionContext) interface{}

	// Visit a parse tree produced by StarRocksParser#typeList.
	VisitTypeList(ctx *TypeListContext) interface{}

	// Visit a parse tree produced by StarRocksParser#loadStatement.
	VisitLoadStatement(ctx *LoadStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#labelName.
	VisitLabelName(ctx *LabelNameContext) interface{}

	// Visit a parse tree produced by StarRocksParser#dataDescList.
	VisitDataDescList(ctx *DataDescListContext) interface{}

	// Visit a parse tree produced by StarRocksParser#dataDesc.
	VisitDataDesc(ctx *DataDescContext) interface{}

	// Visit a parse tree produced by StarRocksParser#formatProps.
	VisitFormatProps(ctx *FormatPropsContext) interface{}

	// Visit a parse tree produced by StarRocksParser#brokerDesc.
	VisitBrokerDesc(ctx *BrokerDescContext) interface{}

	// Visit a parse tree produced by StarRocksParser#resourceDesc.
	VisitResourceDesc(ctx *ResourceDescContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showLoadStatement.
	VisitShowLoadStatement(ctx *ShowLoadStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showLoadWarningsStatement.
	VisitShowLoadWarningsStatement(ctx *ShowLoadWarningsStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#cancelLoadStatement.
	VisitCancelLoadStatement(ctx *CancelLoadStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#alterLoadStatement.
	VisitAlterLoadStatement(ctx *AlterLoadStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#cancelCompactionStatement.
	VisitCancelCompactionStatement(ctx *CancelCompactionStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showAuthorStatement.
	VisitShowAuthorStatement(ctx *ShowAuthorStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showBackendsStatement.
	VisitShowBackendsStatement(ctx *ShowBackendsStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showBrokerStatement.
	VisitShowBrokerStatement(ctx *ShowBrokerStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showCharsetStatement.
	VisitShowCharsetStatement(ctx *ShowCharsetStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showCollationStatement.
	VisitShowCollationStatement(ctx *ShowCollationStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showDeleteStatement.
	VisitShowDeleteStatement(ctx *ShowDeleteStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showDynamicPartitionStatement.
	VisitShowDynamicPartitionStatement(ctx *ShowDynamicPartitionStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showEventsStatement.
	VisitShowEventsStatement(ctx *ShowEventsStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showEnginesStatement.
	VisitShowEnginesStatement(ctx *ShowEnginesStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showFrontendsStatement.
	VisitShowFrontendsStatement(ctx *ShowFrontendsStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showPluginsStatement.
	VisitShowPluginsStatement(ctx *ShowPluginsStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showRepositoriesStatement.
	VisitShowRepositoriesStatement(ctx *ShowRepositoriesStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showOpenTableStatement.
	VisitShowOpenTableStatement(ctx *ShowOpenTableStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showPrivilegesStatement.
	VisitShowPrivilegesStatement(ctx *ShowPrivilegesStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showProcedureStatement.
	VisitShowProcedureStatement(ctx *ShowProcedureStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showProcStatement.
	VisitShowProcStatement(ctx *ShowProcStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showProcesslistStatement.
	VisitShowProcesslistStatement(ctx *ShowProcesslistStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showProfilelistStatement.
	VisitShowProfilelistStatement(ctx *ShowProfilelistStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showRunningQueriesStatement.
	VisitShowRunningQueriesStatement(ctx *ShowRunningQueriesStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showStatusStatement.
	VisitShowStatusStatement(ctx *ShowStatusStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showTabletStatement.
	VisitShowTabletStatement(ctx *ShowTabletStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showTransactionStatement.
	VisitShowTransactionStatement(ctx *ShowTransactionStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showTriggersStatement.
	VisitShowTriggersStatement(ctx *ShowTriggersStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showUserPropertyStatement.
	VisitShowUserPropertyStatement(ctx *ShowUserPropertyStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showVariablesStatement.
	VisitShowVariablesStatement(ctx *ShowVariablesStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showWarningStatement.
	VisitShowWarningStatement(ctx *ShowWarningStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#helpStatement.
	VisitHelpStatement(ctx *HelpStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#createUserStatement.
	VisitCreateUserStatement(ctx *CreateUserStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#dropUserStatement.
	VisitDropUserStatement(ctx *DropUserStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#alterUserStatement.
	VisitAlterUserStatement(ctx *AlterUserStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showUserStatement.
	VisitShowUserStatement(ctx *ShowUserStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showAllAuthentication.
	VisitShowAllAuthentication(ctx *ShowAllAuthenticationContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showAuthenticationForUser.
	VisitShowAuthenticationForUser(ctx *ShowAuthenticationForUserContext) interface{}

	// Visit a parse tree produced by StarRocksParser#executeAsStatement.
	VisitExecuteAsStatement(ctx *ExecuteAsStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#createRoleStatement.
	VisitCreateRoleStatement(ctx *CreateRoleStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#alterRoleStatement.
	VisitAlterRoleStatement(ctx *AlterRoleStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#dropRoleStatement.
	VisitDropRoleStatement(ctx *DropRoleStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showRolesStatement.
	VisitShowRolesStatement(ctx *ShowRolesStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#grantRoleToUser.
	VisitGrantRoleToUser(ctx *GrantRoleToUserContext) interface{}

	// Visit a parse tree produced by StarRocksParser#grantRoleToRole.
	VisitGrantRoleToRole(ctx *GrantRoleToRoleContext) interface{}

	// Visit a parse tree produced by StarRocksParser#revokeRoleFromUser.
	VisitRevokeRoleFromUser(ctx *RevokeRoleFromUserContext) interface{}

	// Visit a parse tree produced by StarRocksParser#revokeRoleFromRole.
	VisitRevokeRoleFromRole(ctx *RevokeRoleFromRoleContext) interface{}

	// Visit a parse tree produced by StarRocksParser#setRoleStatement.
	VisitSetRoleStatement(ctx *SetRoleStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#setDefaultRoleStatement.
	VisitSetDefaultRoleStatement(ctx *SetDefaultRoleStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#grantRevokeClause.
	VisitGrantRevokeClause(ctx *GrantRevokeClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#grantOnUser.
	VisitGrantOnUser(ctx *GrantOnUserContext) interface{}

	// Visit a parse tree produced by StarRocksParser#grantOnTableBrief.
	VisitGrantOnTableBrief(ctx *GrantOnTableBriefContext) interface{}

	// Visit a parse tree produced by StarRocksParser#grantOnFunc.
	VisitGrantOnFunc(ctx *GrantOnFuncContext) interface{}

	// Visit a parse tree produced by StarRocksParser#grantOnSystem.
	VisitGrantOnSystem(ctx *GrantOnSystemContext) interface{}

	// Visit a parse tree produced by StarRocksParser#grantOnPrimaryObj.
	VisitGrantOnPrimaryObj(ctx *GrantOnPrimaryObjContext) interface{}

	// Visit a parse tree produced by StarRocksParser#grantOnAll.
	VisitGrantOnAll(ctx *GrantOnAllContext) interface{}

	// Visit a parse tree produced by StarRocksParser#revokeOnUser.
	VisitRevokeOnUser(ctx *RevokeOnUserContext) interface{}

	// Visit a parse tree produced by StarRocksParser#revokeOnTableBrief.
	VisitRevokeOnTableBrief(ctx *RevokeOnTableBriefContext) interface{}

	// Visit a parse tree produced by StarRocksParser#revokeOnFunc.
	VisitRevokeOnFunc(ctx *RevokeOnFuncContext) interface{}

	// Visit a parse tree produced by StarRocksParser#revokeOnSystem.
	VisitRevokeOnSystem(ctx *RevokeOnSystemContext) interface{}

	// Visit a parse tree produced by StarRocksParser#revokeOnPrimaryObj.
	VisitRevokeOnPrimaryObj(ctx *RevokeOnPrimaryObjContext) interface{}

	// Visit a parse tree produced by StarRocksParser#revokeOnAll.
	VisitRevokeOnAll(ctx *RevokeOnAllContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showGrantsStatement.
	VisitShowGrantsStatement(ctx *ShowGrantsStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#authWithoutPlugin.
	VisitAuthWithoutPlugin(ctx *AuthWithoutPluginContext) interface{}

	// Visit a parse tree produced by StarRocksParser#authWithPlugin.
	VisitAuthWithPlugin(ctx *AuthWithPluginContext) interface{}

	// Visit a parse tree produced by StarRocksParser#privObjectName.
	VisitPrivObjectName(ctx *PrivObjectNameContext) interface{}

	// Visit a parse tree produced by StarRocksParser#privObjectNameList.
	VisitPrivObjectNameList(ctx *PrivObjectNameListContext) interface{}

	// Visit a parse tree produced by StarRocksParser#privFunctionObjectNameList.
	VisitPrivFunctionObjectNameList(ctx *PrivFunctionObjectNameListContext) interface{}

	// Visit a parse tree produced by StarRocksParser#privilegeTypeList.
	VisitPrivilegeTypeList(ctx *PrivilegeTypeListContext) interface{}

	// Visit a parse tree produced by StarRocksParser#privilegeType.
	VisitPrivilegeType(ctx *PrivilegeTypeContext) interface{}

	// Visit a parse tree produced by StarRocksParser#privObjectType.
	VisitPrivObjectType(ctx *PrivObjectTypeContext) interface{}

	// Visit a parse tree produced by StarRocksParser#privObjectTypePlural.
	VisitPrivObjectTypePlural(ctx *PrivObjectTypePluralContext) interface{}

	// Visit a parse tree produced by StarRocksParser#createSecurityIntegrationStatement.
	VisitCreateSecurityIntegrationStatement(ctx *CreateSecurityIntegrationStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#alterSecurityIntegrationStatement.
	VisitAlterSecurityIntegrationStatement(ctx *AlterSecurityIntegrationStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#dropSecurityIntegrationStatement.
	VisitDropSecurityIntegrationStatement(ctx *DropSecurityIntegrationStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showSecurityIntegrationStatement.
	VisitShowSecurityIntegrationStatement(ctx *ShowSecurityIntegrationStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showCreateSecurityIntegrationStatement.
	VisitShowCreateSecurityIntegrationStatement(ctx *ShowCreateSecurityIntegrationStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#createGroupProviderStatement.
	VisitCreateGroupProviderStatement(ctx *CreateGroupProviderStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#dropGroupProviderStatement.
	VisitDropGroupProviderStatement(ctx *DropGroupProviderStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showGroupProvidersStatement.
	VisitShowGroupProvidersStatement(ctx *ShowGroupProvidersStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showCreateGroupProviderStatement.
	VisitShowCreateGroupProviderStatement(ctx *ShowCreateGroupProviderStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#backupStatement.
	VisitBackupStatement(ctx *BackupStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#cancelBackupStatement.
	VisitCancelBackupStatement(ctx *CancelBackupStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showBackupStatement.
	VisitShowBackupStatement(ctx *ShowBackupStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#restoreStatement.
	VisitRestoreStatement(ctx *RestoreStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#cancelRestoreStatement.
	VisitCancelRestoreStatement(ctx *CancelRestoreStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showRestoreStatement.
	VisitShowRestoreStatement(ctx *ShowRestoreStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showSnapshotStatement.
	VisitShowSnapshotStatement(ctx *ShowSnapshotStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#createRepositoryStatement.
	VisitCreateRepositoryStatement(ctx *CreateRepositoryStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#dropRepositoryStatement.
	VisitDropRepositoryStatement(ctx *DropRepositoryStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#addSqlBlackListStatement.
	VisitAddSqlBlackListStatement(ctx *AddSqlBlackListStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#delSqlBlackListStatement.
	VisitDelSqlBlackListStatement(ctx *DelSqlBlackListStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showSqlBlackListStatement.
	VisitShowSqlBlackListStatement(ctx *ShowSqlBlackListStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showWhiteListStatement.
	VisitShowWhiteListStatement(ctx *ShowWhiteListStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#addBackendBlackListStatement.
	VisitAddBackendBlackListStatement(ctx *AddBackendBlackListStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#delBackendBlackListStatement.
	VisitDelBackendBlackListStatement(ctx *DelBackendBlackListStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showBackendBlackListStatement.
	VisitShowBackendBlackListStatement(ctx *ShowBackendBlackListStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#dataCacheTarget.
	VisitDataCacheTarget(ctx *DataCacheTargetContext) interface{}

	// Visit a parse tree produced by StarRocksParser#createDataCacheRuleStatement.
	VisitCreateDataCacheRuleStatement(ctx *CreateDataCacheRuleStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showDataCacheRulesStatement.
	VisitShowDataCacheRulesStatement(ctx *ShowDataCacheRulesStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#dropDataCacheRuleStatement.
	VisitDropDataCacheRuleStatement(ctx *DropDataCacheRuleStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#clearDataCacheRulesStatement.
	VisitClearDataCacheRulesStatement(ctx *ClearDataCacheRulesStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#dataCacheSelectStatement.
	VisitDataCacheSelectStatement(ctx *DataCacheSelectStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#exportStatement.
	VisitExportStatement(ctx *ExportStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#cancelExportStatement.
	VisitCancelExportStatement(ctx *CancelExportStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showExportStatement.
	VisitShowExportStatement(ctx *ShowExportStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#installPluginStatement.
	VisitInstallPluginStatement(ctx *InstallPluginStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#uninstallPluginStatement.
	VisitUninstallPluginStatement(ctx *UninstallPluginStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#createFileStatement.
	VisitCreateFileStatement(ctx *CreateFileStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#dropFileStatement.
	VisitDropFileStatement(ctx *DropFileStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showSmallFilesStatement.
	VisitShowSmallFilesStatement(ctx *ShowSmallFilesStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#createPipeStatement.
	VisitCreatePipeStatement(ctx *CreatePipeStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#dropPipeStatement.
	VisitDropPipeStatement(ctx *DropPipeStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#alterPipeClause.
	VisitAlterPipeClause(ctx *AlterPipeClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#alterPipeStatement.
	VisitAlterPipeStatement(ctx *AlterPipeStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#descPipeStatement.
	VisitDescPipeStatement(ctx *DescPipeStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showPipeStatement.
	VisitShowPipeStatement(ctx *ShowPipeStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#setStatement.
	VisitSetStatement(ctx *SetStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#setNames.
	VisitSetNames(ctx *SetNamesContext) interface{}

	// Visit a parse tree produced by StarRocksParser#setPassword.
	VisitSetPassword(ctx *SetPasswordContext) interface{}

	// Visit a parse tree produced by StarRocksParser#setUserVar.
	VisitSetUserVar(ctx *SetUserVarContext) interface{}

	// Visit a parse tree produced by StarRocksParser#setSystemVar.
	VisitSetSystemVar(ctx *SetSystemVarContext) interface{}

	// Visit a parse tree produced by StarRocksParser#setTransaction.
	VisitSetTransaction(ctx *SetTransactionContext) interface{}

	// Visit a parse tree produced by StarRocksParser#transaction_characteristics.
	VisitTransaction_characteristics(ctx *Transaction_characteristicsContext) interface{}

	// Visit a parse tree produced by StarRocksParser#transaction_access_mode.
	VisitTransaction_access_mode(ctx *Transaction_access_modeContext) interface{}

	// Visit a parse tree produced by StarRocksParser#isolation_level.
	VisitIsolation_level(ctx *Isolation_levelContext) interface{}

	// Visit a parse tree produced by StarRocksParser#isolation_types.
	VisitIsolation_types(ctx *Isolation_typesContext) interface{}

	// Visit a parse tree produced by StarRocksParser#setExprOrDefault.
	VisitSetExprOrDefault(ctx *SetExprOrDefaultContext) interface{}

	// Visit a parse tree produced by StarRocksParser#setUserPropertyStatement.
	VisitSetUserPropertyStatement(ctx *SetUserPropertyStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#roleList.
	VisitRoleList(ctx *RoleListContext) interface{}

	// Visit a parse tree produced by StarRocksParser#executeScriptStatement.
	VisitExecuteScriptStatement(ctx *ExecuteScriptStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#unsupportedStatement.
	VisitUnsupportedStatement(ctx *UnsupportedStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#lock_item.
	VisitLock_item(ctx *Lock_itemContext) interface{}

	// Visit a parse tree produced by StarRocksParser#lock_type.
	VisitLock_type(ctx *Lock_typeContext) interface{}

	// Visit a parse tree produced by StarRocksParser#alterPlanAdvisorAddStatement.
	VisitAlterPlanAdvisorAddStatement(ctx *AlterPlanAdvisorAddStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#truncatePlanAdvisorStatement.
	VisitTruncatePlanAdvisorStatement(ctx *TruncatePlanAdvisorStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#alterPlanAdvisorDropStatement.
	VisitAlterPlanAdvisorDropStatement(ctx *AlterPlanAdvisorDropStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showPlanAdvisorStatement.
	VisitShowPlanAdvisorStatement(ctx *ShowPlanAdvisorStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#createWarehouseStatement.
	VisitCreateWarehouseStatement(ctx *CreateWarehouseStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#dropWarehouseStatement.
	VisitDropWarehouseStatement(ctx *DropWarehouseStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#suspendWarehouseStatement.
	VisitSuspendWarehouseStatement(ctx *SuspendWarehouseStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#resumeWarehouseStatement.
	VisitResumeWarehouseStatement(ctx *ResumeWarehouseStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#setWarehouseStatement.
	VisitSetWarehouseStatement(ctx *SetWarehouseStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showWarehousesStatement.
	VisitShowWarehousesStatement(ctx *ShowWarehousesStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showClustersStatement.
	VisitShowClustersStatement(ctx *ShowClustersStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#showNodesStatement.
	VisitShowNodesStatement(ctx *ShowNodesStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#alterWarehouseStatement.
	VisitAlterWarehouseStatement(ctx *AlterWarehouseStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#beginStatement.
	VisitBeginStatement(ctx *BeginStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#commitStatement.
	VisitCommitStatement(ctx *CommitStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#rollbackStatement.
	VisitRollbackStatement(ctx *RollbackStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#translateStatement.
	VisitTranslateStatement(ctx *TranslateStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#dialect.
	VisitDialect(ctx *DialectContext) interface{}

	// Visit a parse tree produced by StarRocksParser#translateSQL.
	VisitTranslateSQL(ctx *TranslateSQLContext) interface{}

	// Visit a parse tree produced by StarRocksParser#queryStatement.
	VisitQueryStatement(ctx *QueryStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#queryRelation.
	VisitQueryRelation(ctx *QueryRelationContext) interface{}

	// Visit a parse tree produced by StarRocksParser#withClause.
	VisitWithClause(ctx *WithClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#queryNoWith.
	VisitQueryNoWith(ctx *QueryNoWithContext) interface{}

	// Visit a parse tree produced by StarRocksParser#queryPeriod.
	VisitQueryPeriod(ctx *QueryPeriodContext) interface{}

	// Visit a parse tree produced by StarRocksParser#periodType.
	VisitPeriodType(ctx *PeriodTypeContext) interface{}

	// Visit a parse tree produced by StarRocksParser#queryWithParentheses.
	VisitQueryWithParentheses(ctx *QueryWithParenthesesContext) interface{}

	// Visit a parse tree produced by StarRocksParser#setOperation.
	VisitSetOperation(ctx *SetOperationContext) interface{}

	// Visit a parse tree produced by StarRocksParser#queryPrimaryDefault.
	VisitQueryPrimaryDefault(ctx *QueryPrimaryDefaultContext) interface{}

	// Visit a parse tree produced by StarRocksParser#subquery.
	VisitSubquery(ctx *SubqueryContext) interface{}

	// Visit a parse tree produced by StarRocksParser#rowConstructor.
	VisitRowConstructor(ctx *RowConstructorContext) interface{}

	// Visit a parse tree produced by StarRocksParser#sortItem.
	VisitSortItem(ctx *SortItemContext) interface{}

	// Visit a parse tree produced by StarRocksParser#limitConstExpr.
	VisitLimitConstExpr(ctx *LimitConstExprContext) interface{}

	// Visit a parse tree produced by StarRocksParser#limitElement.
	VisitLimitElement(ctx *LimitElementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#querySpecification.
	VisitQuerySpecification(ctx *QuerySpecificationContext) interface{}

	// Visit a parse tree produced by StarRocksParser#from.
	VisitFrom(ctx *FromContext) interface{}

	// Visit a parse tree produced by StarRocksParser#dual.
	VisitDual(ctx *DualContext) interface{}

	// Visit a parse tree produced by StarRocksParser#rollup.
	VisitRollup(ctx *RollupContext) interface{}

	// Visit a parse tree produced by StarRocksParser#cube.
	VisitCube(ctx *CubeContext) interface{}

	// Visit a parse tree produced by StarRocksParser#multipleGroupingSets.
	VisitMultipleGroupingSets(ctx *MultipleGroupingSetsContext) interface{}

	// Visit a parse tree produced by StarRocksParser#singleGroupingSet.
	VisitSingleGroupingSet(ctx *SingleGroupingSetContext) interface{}

	// Visit a parse tree produced by StarRocksParser#groupingSet.
	VisitGroupingSet(ctx *GroupingSetContext) interface{}

	// Visit a parse tree produced by StarRocksParser#commonTableExpression.
	VisitCommonTableExpression(ctx *CommonTableExpressionContext) interface{}

	// Visit a parse tree produced by StarRocksParser#setQuantifier.
	VisitSetQuantifier(ctx *SetQuantifierContext) interface{}

	// Visit a parse tree produced by StarRocksParser#selectSingle.
	VisitSelectSingle(ctx *SelectSingleContext) interface{}

	// Visit a parse tree produced by StarRocksParser#selectAll.
	VisitSelectAll(ctx *SelectAllContext) interface{}

	// Visit a parse tree produced by StarRocksParser#relations.
	VisitRelations(ctx *RelationsContext) interface{}

	// Visit a parse tree produced by StarRocksParser#relation.
	VisitRelation(ctx *RelationContext) interface{}

	// Visit a parse tree produced by StarRocksParser#tableAtom.
	VisitTableAtom(ctx *TableAtomContext) interface{}

	// Visit a parse tree produced by StarRocksParser#inlineTable.
	VisitInlineTable(ctx *InlineTableContext) interface{}

	// Visit a parse tree produced by StarRocksParser#subqueryWithAlias.
	VisitSubqueryWithAlias(ctx *SubqueryWithAliasContext) interface{}

	// Visit a parse tree produced by StarRocksParser#tableFunction.
	VisitTableFunction(ctx *TableFunctionContext) interface{}

	// Visit a parse tree produced by StarRocksParser#normalizedTableFunction.
	VisitNormalizedTableFunction(ctx *NormalizedTableFunctionContext) interface{}

	// Visit a parse tree produced by StarRocksParser#fileTableFunction.
	VisitFileTableFunction(ctx *FileTableFunctionContext) interface{}

	// Visit a parse tree produced by StarRocksParser#parenthesizedRelation.
	VisitParenthesizedRelation(ctx *ParenthesizedRelationContext) interface{}

	// Visit a parse tree produced by StarRocksParser#pivotClause.
	VisitPivotClause(ctx *PivotClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#pivotAggregationExpression.
	VisitPivotAggregationExpression(ctx *PivotAggregationExpressionContext) interface{}

	// Visit a parse tree produced by StarRocksParser#pivotValue.
	VisitPivotValue(ctx *PivotValueContext) interface{}

	// Visit a parse tree produced by StarRocksParser#sampleClause.
	VisitSampleClause(ctx *SampleClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#argumentList.
	VisitArgumentList(ctx *ArgumentListContext) interface{}

	// Visit a parse tree produced by StarRocksParser#namedArgumentList.
	VisitNamedArgumentList(ctx *NamedArgumentListContext) interface{}

	// Visit a parse tree produced by StarRocksParser#namedArguments.
	VisitNamedArguments(ctx *NamedArgumentsContext) interface{}

	// Visit a parse tree produced by StarRocksParser#joinRelation.
	VisitJoinRelation(ctx *JoinRelationContext) interface{}

	// Visit a parse tree produced by StarRocksParser#crossOrInnerJoinType.
	VisitCrossOrInnerJoinType(ctx *CrossOrInnerJoinTypeContext) interface{}

	// Visit a parse tree produced by StarRocksParser#outerAndSemiJoinType.
	VisitOuterAndSemiJoinType(ctx *OuterAndSemiJoinTypeContext) interface{}

	// Visit a parse tree produced by StarRocksParser#bracketHint.
	VisitBracketHint(ctx *BracketHintContext) interface{}

	// Visit a parse tree produced by StarRocksParser#hintMap.
	VisitHintMap(ctx *HintMapContext) interface{}

	// Visit a parse tree produced by StarRocksParser#joinCriteria.
	VisitJoinCriteria(ctx *JoinCriteriaContext) interface{}

	// Visit a parse tree produced by StarRocksParser#columnAliases.
	VisitColumnAliases(ctx *ColumnAliasesContext) interface{}

	// Visit a parse tree produced by StarRocksParser#partitionNames.
	VisitPartitionNames(ctx *PartitionNamesContext) interface{}

	// Visit a parse tree produced by StarRocksParser#keyPartitionList.
	VisitKeyPartitionList(ctx *KeyPartitionListContext) interface{}

	// Visit a parse tree produced by StarRocksParser#tabletList.
	VisitTabletList(ctx *TabletListContext) interface{}

	// Visit a parse tree produced by StarRocksParser#prepareStatement.
	VisitPrepareStatement(ctx *PrepareStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#prepareSql.
	VisitPrepareSql(ctx *PrepareSqlContext) interface{}

	// Visit a parse tree produced by StarRocksParser#executeStatement.
	VisitExecuteStatement(ctx *ExecuteStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#deallocateStatement.
	VisitDeallocateStatement(ctx *DeallocateStatementContext) interface{}

	// Visit a parse tree produced by StarRocksParser#replicaList.
	VisitReplicaList(ctx *ReplicaListContext) interface{}

	// Visit a parse tree produced by StarRocksParser#expressionsWithDefault.
	VisitExpressionsWithDefault(ctx *ExpressionsWithDefaultContext) interface{}

	// Visit a parse tree produced by StarRocksParser#expressionOrDefault.
	VisitExpressionOrDefault(ctx *ExpressionOrDefaultContext) interface{}

	// Visit a parse tree produced by StarRocksParser#mapExpressionList.
	VisitMapExpressionList(ctx *MapExpressionListContext) interface{}

	// Visit a parse tree produced by StarRocksParser#mapExpression.
	VisitMapExpression(ctx *MapExpressionContext) interface{}

	// Visit a parse tree produced by StarRocksParser#expressionSingleton.
	VisitExpressionSingleton(ctx *ExpressionSingletonContext) interface{}

	// Visit a parse tree produced by StarRocksParser#expressionDefault.
	VisitExpressionDefault(ctx *ExpressionDefaultContext) interface{}

	// Visit a parse tree produced by StarRocksParser#logicalNot.
	VisitLogicalNot(ctx *LogicalNotContext) interface{}

	// Visit a parse tree produced by StarRocksParser#logicalBinary.
	VisitLogicalBinary(ctx *LogicalBinaryContext) interface{}

	// Visit a parse tree produced by StarRocksParser#expressionList.
	VisitExpressionList(ctx *ExpressionListContext) interface{}

	// Visit a parse tree produced by StarRocksParser#comparison.
	VisitComparison(ctx *ComparisonContext) interface{}

	// Visit a parse tree produced by StarRocksParser#booleanExpressionDefault.
	VisitBooleanExpressionDefault(ctx *BooleanExpressionDefaultContext) interface{}

	// Visit a parse tree produced by StarRocksParser#isNull.
	VisitIsNull(ctx *IsNullContext) interface{}

	// Visit a parse tree produced by StarRocksParser#scalarSubquery.
	VisitScalarSubquery(ctx *ScalarSubqueryContext) interface{}

	// Visit a parse tree produced by StarRocksParser#predicate.
	VisitPredicate(ctx *PredicateContext) interface{}

	// Visit a parse tree produced by StarRocksParser#tupleInSubquery.
	VisitTupleInSubquery(ctx *TupleInSubqueryContext) interface{}

	// Visit a parse tree produced by StarRocksParser#inIntegerList.
	VisitInIntegerList(ctx *InIntegerListContext) interface{}

	// Visit a parse tree produced by StarRocksParser#inStringList.
	VisitInStringList(ctx *InStringListContext) interface{}

	// Visit a parse tree produced by StarRocksParser#inSubquery.
	VisitInSubquery(ctx *InSubqueryContext) interface{}

	// Visit a parse tree produced by StarRocksParser#inList.
	VisitInList(ctx *InListContext) interface{}

	// Visit a parse tree produced by StarRocksParser#between.
	VisitBetween(ctx *BetweenContext) interface{}

	// Visit a parse tree produced by StarRocksParser#like.
	VisitLike(ctx *LikeContext) interface{}

	// Visit a parse tree produced by StarRocksParser#valueExpressionDefault.
	VisitValueExpressionDefault(ctx *ValueExpressionDefaultContext) interface{}

	// Visit a parse tree produced by StarRocksParser#arithmeticBinary.
	VisitArithmeticBinary(ctx *ArithmeticBinaryContext) interface{}

	// Visit a parse tree produced by StarRocksParser#dereference.
	VisitDereference(ctx *DereferenceContext) interface{}

	// Visit a parse tree produced by StarRocksParser#odbcFunctionCallExpression.
	VisitOdbcFunctionCallExpression(ctx *OdbcFunctionCallExpressionContext) interface{}

	// Visit a parse tree produced by StarRocksParser#matchExpr.
	VisitMatchExpr(ctx *MatchExprContext) interface{}

	// Visit a parse tree produced by StarRocksParser#columnRef.
	VisitColumnRef(ctx *ColumnRefContext) interface{}

	// Visit a parse tree produced by StarRocksParser#convert.
	VisitConvert(ctx *ConvertContext) interface{}

	// Visit a parse tree produced by StarRocksParser#collectionSubscript.
	VisitCollectionSubscript(ctx *CollectionSubscriptContext) interface{}

	// Visit a parse tree produced by StarRocksParser#literal.
	VisitLiteral(ctx *LiteralContext) interface{}

	// Visit a parse tree produced by StarRocksParser#cast.
	VisitCast(ctx *CastContext) interface{}

	// Visit a parse tree produced by StarRocksParser#parenthesizedExpression.
	VisitParenthesizedExpression(ctx *ParenthesizedExpressionContext) interface{}

	// Visit a parse tree produced by StarRocksParser#userVariableExpression.
	VisitUserVariableExpression(ctx *UserVariableExpressionContext) interface{}

	// Visit a parse tree produced by StarRocksParser#functionCallExpression.
	VisitFunctionCallExpression(ctx *FunctionCallExpressionContext) interface{}

	// Visit a parse tree produced by StarRocksParser#simpleCase.
	VisitSimpleCase(ctx *SimpleCaseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#arrowExpression.
	VisitArrowExpression(ctx *ArrowExpressionContext) interface{}

	// Visit a parse tree produced by StarRocksParser#systemVariableExpression.
	VisitSystemVariableExpression(ctx *SystemVariableExpressionContext) interface{}

	// Visit a parse tree produced by StarRocksParser#concat.
	VisitConcat(ctx *ConcatContext) interface{}

	// Visit a parse tree produced by StarRocksParser#subqueryExpression.
	VisitSubqueryExpression(ctx *SubqueryExpressionContext) interface{}

	// Visit a parse tree produced by StarRocksParser#lambdaFunctionExpr.
	VisitLambdaFunctionExpr(ctx *LambdaFunctionExprContext) interface{}

	// Visit a parse tree produced by StarRocksParser#dictionaryGetExpr.
	VisitDictionaryGetExpr(ctx *DictionaryGetExprContext) interface{}

	// Visit a parse tree produced by StarRocksParser#collate.
	VisitCollate(ctx *CollateContext) interface{}

	// Visit a parse tree produced by StarRocksParser#arrayConstructor.
	VisitArrayConstructor(ctx *ArrayConstructorContext) interface{}

	// Visit a parse tree produced by StarRocksParser#mapConstructor.
	VisitMapConstructor(ctx *MapConstructorContext) interface{}

	// Visit a parse tree produced by StarRocksParser#arraySlice.
	VisitArraySlice(ctx *ArraySliceContext) interface{}

	// Visit a parse tree produced by StarRocksParser#exists.
	VisitExists(ctx *ExistsContext) interface{}

	// Visit a parse tree produced by StarRocksParser#searchedCase.
	VisitSearchedCase(ctx *SearchedCaseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#arithmeticUnary.
	VisitArithmeticUnary(ctx *ArithmeticUnaryContext) interface{}

	// Visit a parse tree produced by StarRocksParser#numericLiteral.
	VisitNumericLiteral(ctx *NumericLiteralContext) interface{}

	// Visit a parse tree produced by StarRocksParser#nullLiteral.
	VisitNullLiteral(ctx *NullLiteralContext) interface{}

	// Visit a parse tree produced by StarRocksParser#booleanLiteral.
	VisitBooleanLiteral(ctx *BooleanLiteralContext) interface{}

	// Visit a parse tree produced by StarRocksParser#dateLiteral.
	VisitDateLiteral(ctx *DateLiteralContext) interface{}

	// Visit a parse tree produced by StarRocksParser#stringLiteral.
	VisitStringLiteral(ctx *StringLiteralContext) interface{}

	// Visit a parse tree produced by StarRocksParser#intervalLiteral.
	VisitIntervalLiteral(ctx *IntervalLiteralContext) interface{}

	// Visit a parse tree produced by StarRocksParser#unitBoundaryLiteral.
	VisitUnitBoundaryLiteral(ctx *UnitBoundaryLiteralContext) interface{}

	// Visit a parse tree produced by StarRocksParser#binaryLiteral.
	VisitBinaryLiteral(ctx *BinaryLiteralContext) interface{}

	// Visit a parse tree produced by StarRocksParser#Parameter.
	VisitParameter(ctx *ParameterContext) interface{}

	// Visit a parse tree produced by StarRocksParser#extract.
	VisitExtract(ctx *ExtractContext) interface{}

	// Visit a parse tree produced by StarRocksParser#groupingOperation.
	VisitGroupingOperation(ctx *GroupingOperationContext) interface{}

	// Visit a parse tree produced by StarRocksParser#informationFunction.
	VisitInformationFunction(ctx *InformationFunctionContext) interface{}

	// Visit a parse tree produced by StarRocksParser#specialDateTime.
	VisitSpecialDateTime(ctx *SpecialDateTimeContext) interface{}

	// Visit a parse tree produced by StarRocksParser#specialFunction.
	VisitSpecialFunction(ctx *SpecialFunctionContext) interface{}

	// Visit a parse tree produced by StarRocksParser#aggregationFunctionCall.
	VisitAggregationFunctionCall(ctx *AggregationFunctionCallContext) interface{}

	// Visit a parse tree produced by StarRocksParser#windowFunctionCall.
	VisitWindowFunctionCall(ctx *WindowFunctionCallContext) interface{}

	// Visit a parse tree produced by StarRocksParser#translateFunctionCall.
	VisitTranslateFunctionCall(ctx *TranslateFunctionCallContext) interface{}

	// Visit a parse tree produced by StarRocksParser#simpleFunctionCall.
	VisitSimpleFunctionCall(ctx *SimpleFunctionCallContext) interface{}

	// Visit a parse tree produced by StarRocksParser#aggregationFunction.
	VisitAggregationFunction(ctx *AggregationFunctionContext) interface{}

	// Visit a parse tree produced by StarRocksParser#userVariable.
	VisitUserVariable(ctx *UserVariableContext) interface{}

	// Visit a parse tree produced by StarRocksParser#systemVariable.
	VisitSystemVariable(ctx *SystemVariableContext) interface{}

	// Visit a parse tree produced by StarRocksParser#columnReference.
	VisitColumnReference(ctx *ColumnReferenceContext) interface{}

	// Visit a parse tree produced by StarRocksParser#informationFunctionExpression.
	VisitInformationFunctionExpression(ctx *InformationFunctionExpressionContext) interface{}

	// Visit a parse tree produced by StarRocksParser#specialDateTimeExpression.
	VisitSpecialDateTimeExpression(ctx *SpecialDateTimeExpressionContext) interface{}

	// Visit a parse tree produced by StarRocksParser#specialFunctionExpression.
	VisitSpecialFunctionExpression(ctx *SpecialFunctionExpressionContext) interface{}

	// Visit a parse tree produced by StarRocksParser#windowFunction.
	VisitWindowFunction(ctx *WindowFunctionContext) interface{}

	// Visit a parse tree produced by StarRocksParser#whenClause.
	VisitWhenClause(ctx *WhenClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#over.
	VisitOver(ctx *OverContext) interface{}

	// Visit a parse tree produced by StarRocksParser#ignoreNulls.
	VisitIgnoreNulls(ctx *IgnoreNullsContext) interface{}

	// Visit a parse tree produced by StarRocksParser#windowFrame.
	VisitWindowFrame(ctx *WindowFrameContext) interface{}

	// Visit a parse tree produced by StarRocksParser#unboundedFrame.
	VisitUnboundedFrame(ctx *UnboundedFrameContext) interface{}

	// Visit a parse tree produced by StarRocksParser#currentRowBound.
	VisitCurrentRowBound(ctx *CurrentRowBoundContext) interface{}

	// Visit a parse tree produced by StarRocksParser#boundedFrame.
	VisitBoundedFrame(ctx *BoundedFrameContext) interface{}

	// Visit a parse tree produced by StarRocksParser#backupRestoreObjectDesc.
	VisitBackupRestoreObjectDesc(ctx *BackupRestoreObjectDescContext) interface{}

	// Visit a parse tree produced by StarRocksParser#tableDesc.
	VisitTableDesc(ctx *TableDescContext) interface{}

	// Visit a parse tree produced by StarRocksParser#backupRestoreTableDesc.
	VisitBackupRestoreTableDesc(ctx *BackupRestoreTableDescContext) interface{}

	// Visit a parse tree produced by StarRocksParser#explainDesc.
	VisitExplainDesc(ctx *ExplainDescContext) interface{}

	// Visit a parse tree produced by StarRocksParser#optimizerTrace.
	VisitOptimizerTrace(ctx *OptimizerTraceContext) interface{}

	// Visit a parse tree produced by StarRocksParser#partitionExpr.
	VisitPartitionExpr(ctx *PartitionExprContext) interface{}

	// Visit a parse tree produced by StarRocksParser#partitionDesc.
	VisitPartitionDesc(ctx *PartitionDescContext) interface{}

	// Visit a parse tree produced by StarRocksParser#listPartitionDesc.
	VisitListPartitionDesc(ctx *ListPartitionDescContext) interface{}

	// Visit a parse tree produced by StarRocksParser#singleItemListPartitionDesc.
	VisitSingleItemListPartitionDesc(ctx *SingleItemListPartitionDescContext) interface{}

	// Visit a parse tree produced by StarRocksParser#multiItemListPartitionDesc.
	VisitMultiItemListPartitionDesc(ctx *MultiItemListPartitionDescContext) interface{}

	// Visit a parse tree produced by StarRocksParser#multiListPartitionValues.
	VisitMultiListPartitionValues(ctx *MultiListPartitionValuesContext) interface{}

	// Visit a parse tree produced by StarRocksParser#singleListPartitionValues.
	VisitSingleListPartitionValues(ctx *SingleListPartitionValuesContext) interface{}

	// Visit a parse tree produced by StarRocksParser#listPartitionValues.
	VisitListPartitionValues(ctx *ListPartitionValuesContext) interface{}

	// Visit a parse tree produced by StarRocksParser#listPartitionValue.
	VisitListPartitionValue(ctx *ListPartitionValueContext) interface{}

	// Visit a parse tree produced by StarRocksParser#stringList.
	VisitStringList(ctx *StringListContext) interface{}

	// Visit a parse tree produced by StarRocksParser#integerList.
	VisitIntegerList(ctx *IntegerListContext) interface{}

	// Visit a parse tree produced by StarRocksParser#literalExpressionList.
	VisitLiteralExpressionList(ctx *LiteralExpressionListContext) interface{}

	// Visit a parse tree produced by StarRocksParser#rangePartitionDesc.
	VisitRangePartitionDesc(ctx *RangePartitionDescContext) interface{}

	// Visit a parse tree produced by StarRocksParser#singleRangePartition.
	VisitSingleRangePartition(ctx *SingleRangePartitionContext) interface{}

	// Visit a parse tree produced by StarRocksParser#multiRangePartition.
	VisitMultiRangePartition(ctx *MultiRangePartitionContext) interface{}

	// Visit a parse tree produced by StarRocksParser#partitionRangeDesc.
	VisitPartitionRangeDesc(ctx *PartitionRangeDescContext) interface{}

	// Visit a parse tree produced by StarRocksParser#partitionKeyDesc.
	VisitPartitionKeyDesc(ctx *PartitionKeyDescContext) interface{}

	// Visit a parse tree produced by StarRocksParser#partitionValueList.
	VisitPartitionValueList(ctx *PartitionValueListContext) interface{}

	// Visit a parse tree produced by StarRocksParser#keyPartition.
	VisitKeyPartition(ctx *KeyPartitionContext) interface{}

	// Visit a parse tree produced by StarRocksParser#partitionValue.
	VisitPartitionValue(ctx *PartitionValueContext) interface{}

	// Visit a parse tree produced by StarRocksParser#distributionClause.
	VisitDistributionClause(ctx *DistributionClauseContext) interface{}

	// Visit a parse tree produced by StarRocksParser#distributionDesc.
	VisitDistributionDesc(ctx *DistributionDescContext) interface{}

	// Visit a parse tree produced by StarRocksParser#alterModifyDefaultBuckets.
	VisitAlterModifyDefaultBuckets(ctx *AlterModifyDefaultBucketsContext) interface{}

	// Visit a parse tree produced by StarRocksParser#refreshSchemeDesc.
	VisitRefreshSchemeDesc(ctx *RefreshSchemeDescContext) interface{}

	// Visit a parse tree produced by StarRocksParser#statusDesc.
	VisitStatusDesc(ctx *StatusDescContext) interface{}

	// Visit a parse tree produced by StarRocksParser#properties.
	VisitProperties(ctx *PropertiesContext) interface{}

	// Visit a parse tree produced by StarRocksParser#extProperties.
	VisitExtProperties(ctx *ExtPropertiesContext) interface{}

	// Visit a parse tree produced by StarRocksParser#propertyList.
	VisitPropertyList(ctx *PropertyListContext) interface{}

	// Visit a parse tree produced by StarRocksParser#userPropertyList.
	VisitUserPropertyList(ctx *UserPropertyListContext) interface{}

	// Visit a parse tree produced by StarRocksParser#property.
	VisitProperty(ctx *PropertyContext) interface{}

	// Visit a parse tree produced by StarRocksParser#inlineProperties.
	VisitInlineProperties(ctx *InlinePropertiesContext) interface{}

	// Visit a parse tree produced by StarRocksParser#inlineProperty.
	VisitInlineProperty(ctx *InlinePropertyContext) interface{}

	// Visit a parse tree produced by StarRocksParser#varType.
	VisitVarType(ctx *VarTypeContext) interface{}

	// Visit a parse tree produced by StarRocksParser#comment.
	VisitComment(ctx *CommentContext) interface{}

	// Visit a parse tree produced by StarRocksParser#outfile.
	VisitOutfile(ctx *OutfileContext) interface{}

	// Visit a parse tree produced by StarRocksParser#fileFormat.
	VisitFileFormat(ctx *FileFormatContext) interface{}

	// Visit a parse tree produced by StarRocksParser#string.
	VisitString(ctx *StringContext) interface{}

	// Visit a parse tree produced by StarRocksParser#binary.
	VisitBinary(ctx *BinaryContext) interface{}

	// Visit a parse tree produced by StarRocksParser#comparisonOperator.
	VisitComparisonOperator(ctx *ComparisonOperatorContext) interface{}

	// Visit a parse tree produced by StarRocksParser#booleanValue.
	VisitBooleanValue(ctx *BooleanValueContext) interface{}

	// Visit a parse tree produced by StarRocksParser#interval.
	VisitInterval(ctx *IntervalContext) interface{}

	// Visit a parse tree produced by StarRocksParser#taskInterval.
	VisitTaskInterval(ctx *TaskIntervalContext) interface{}

	// Visit a parse tree produced by StarRocksParser#taskUnitIdentifier.
	VisitTaskUnitIdentifier(ctx *TaskUnitIdentifierContext) interface{}

	// Visit a parse tree produced by StarRocksParser#unitIdentifier.
	VisitUnitIdentifier(ctx *UnitIdentifierContext) interface{}

	// Visit a parse tree produced by StarRocksParser#unitBoundary.
	VisitUnitBoundary(ctx *UnitBoundaryContext) interface{}

	// Visit a parse tree produced by StarRocksParser#type.
	VisitType(ctx *TypeContext) interface{}

	// Visit a parse tree produced by StarRocksParser#arrayType.
	VisitArrayType(ctx *ArrayTypeContext) interface{}

	// Visit a parse tree produced by StarRocksParser#mapType.
	VisitMapType(ctx *MapTypeContext) interface{}

	// Visit a parse tree produced by StarRocksParser#subfieldDesc.
	VisitSubfieldDesc(ctx *SubfieldDescContext) interface{}

	// Visit a parse tree produced by StarRocksParser#subfieldDescs.
	VisitSubfieldDescs(ctx *SubfieldDescsContext) interface{}

	// Visit a parse tree produced by StarRocksParser#structType.
	VisitStructType(ctx *StructTypeContext) interface{}

	// Visit a parse tree produced by StarRocksParser#typeParameter.
	VisitTypeParameter(ctx *TypeParameterContext) interface{}

	// Visit a parse tree produced by StarRocksParser#baseType.
	VisitBaseType(ctx *BaseTypeContext) interface{}

	// Visit a parse tree produced by StarRocksParser#decimalType.
	VisitDecimalType(ctx *DecimalTypeContext) interface{}

	// Visit a parse tree produced by StarRocksParser#qualifiedName.
	VisitQualifiedName(ctx *QualifiedNameContext) interface{}

	// Visit a parse tree produced by StarRocksParser#tableName.
	VisitTableName(ctx *TableNameContext) interface{}

	// Visit a parse tree produced by StarRocksParser#writeBranch.
	VisitWriteBranch(ctx *WriteBranchContext) interface{}

	// Visit a parse tree produced by StarRocksParser#unquotedIdentifier.
	VisitUnquotedIdentifier(ctx *UnquotedIdentifierContext) interface{}

	// Visit a parse tree produced by StarRocksParser#digitIdentifier.
	VisitDigitIdentifier(ctx *DigitIdentifierContext) interface{}

	// Visit a parse tree produced by StarRocksParser#backQuotedIdentifier.
	VisitBackQuotedIdentifier(ctx *BackQuotedIdentifierContext) interface{}

	// Visit a parse tree produced by StarRocksParser#identifierWithAlias.
	VisitIdentifierWithAlias(ctx *IdentifierWithAliasContext) interface{}

	// Visit a parse tree produced by StarRocksParser#identifierWithAliasList.
	VisitIdentifierWithAliasList(ctx *IdentifierWithAliasListContext) interface{}

	// Visit a parse tree produced by StarRocksParser#identifierList.
	VisitIdentifierList(ctx *IdentifierListContext) interface{}

	// Visit a parse tree produced by StarRocksParser#identifierOrString.
	VisitIdentifierOrString(ctx *IdentifierOrStringContext) interface{}

	// Visit a parse tree produced by StarRocksParser#identifierOrStringList.
	VisitIdentifierOrStringList(ctx *IdentifierOrStringListContext) interface{}

	// Visit a parse tree produced by StarRocksParser#identifierOrStringOrStar.
	VisitIdentifierOrStringOrStar(ctx *IdentifierOrStringOrStarContext) interface{}

	// Visit a parse tree produced by StarRocksParser#userWithoutHost.
	VisitUserWithoutHost(ctx *UserWithoutHostContext) interface{}

	// Visit a parse tree produced by StarRocksParser#userWithHost.
	VisitUserWithHost(ctx *UserWithHostContext) interface{}

	// Visit a parse tree produced by StarRocksParser#userWithHostAndBlanket.
	VisitUserWithHostAndBlanket(ctx *UserWithHostAndBlanketContext) interface{}

	// Visit a parse tree produced by StarRocksParser#assignment.
	VisitAssignment(ctx *AssignmentContext) interface{}

	// Visit a parse tree produced by StarRocksParser#assignmentList.
	VisitAssignmentList(ctx *AssignmentListContext) interface{}

	// Visit a parse tree produced by StarRocksParser#integerValue.
	VisitIntegerValue(ctx *IntegerValueContext) interface{}

	// Visit a parse tree produced by StarRocksParser#decimalValue.
	VisitDecimalValue(ctx *DecimalValueContext) interface{}

	// Visit a parse tree produced by StarRocksParser#doubleValue.
	VisitDoubleValue(ctx *DoubleValueContext) interface{}

	// Visit a parse tree produced by StarRocksParser#nonReserved.
	VisitNonReserved(ctx *NonReservedContext) interface{}
}
