// Code generated from SqlBaseParser.g4 by ANTLR 4.13.2. DO NOT EDIT.

package parser // SqlBaseParser
import "github.com/antlr4-go/antlr/v4"

// SqlBaseParserListener is a complete listener for a parse tree produced by SqlBaseParser.
type SqlBaseParserListener interface {
	antlr.ParseTreeListener

	// EnterCompoundOrSingleStatement is called when entering the compoundOrSingleStatement production.
	EnterCompoundOrSingleStatement(c *CompoundOrSingleStatementContext)

	// EnterSingleCompoundStatement is called when entering the singleCompoundStatement production.
	EnterSingleCompoundStatement(c *SingleCompoundStatementContext)

	// EnterBeginEndCompoundBlock is called when entering the beginEndCompoundBlock production.
	EnterBeginEndCompoundBlock(c *BeginEndCompoundBlockContext)

	// EnterCompoundBody is called when entering the compoundBody production.
	EnterCompoundBody(c *CompoundBodyContext)

	// EnterCompoundStatement is called when entering the compoundStatement production.
	EnterCompoundStatement(c *CompoundStatementContext)

	// EnterSetVariableInsideSqlScript is called when entering the setVariableInsideSqlScript production.
	EnterSetVariableInsideSqlScript(c *SetVariableInsideSqlScriptContext)

	// EnterSqlStateValue is called when entering the sqlStateValue production.
	EnterSqlStateValue(c *SqlStateValueContext)

	// EnterDeclareConditionStatement is called when entering the declareConditionStatement production.
	EnterDeclareConditionStatement(c *DeclareConditionStatementContext)

	// EnterConditionValue is called when entering the conditionValue production.
	EnterConditionValue(c *ConditionValueContext)

	// EnterConditionValues is called when entering the conditionValues production.
	EnterConditionValues(c *ConditionValuesContext)

	// EnterDeclareHandlerStatement is called when entering the declareHandlerStatement production.
	EnterDeclareHandlerStatement(c *DeclareHandlerStatementContext)

	// EnterWhileStatement is called when entering the whileStatement production.
	EnterWhileStatement(c *WhileStatementContext)

	// EnterIfElseStatement is called when entering the ifElseStatement production.
	EnterIfElseStatement(c *IfElseStatementContext)

	// EnterRepeatStatement is called when entering the repeatStatement production.
	EnterRepeatStatement(c *RepeatStatementContext)

	// EnterLeaveStatement is called when entering the leaveStatement production.
	EnterLeaveStatement(c *LeaveStatementContext)

	// EnterIterateStatement is called when entering the iterateStatement production.
	EnterIterateStatement(c *IterateStatementContext)

	// EnterSearchedCaseStatement is called when entering the searchedCaseStatement production.
	EnterSearchedCaseStatement(c *SearchedCaseStatementContext)

	// EnterSimpleCaseStatement is called when entering the simpleCaseStatement production.
	EnterSimpleCaseStatement(c *SimpleCaseStatementContext)

	// EnterLoopStatement is called when entering the loopStatement production.
	EnterLoopStatement(c *LoopStatementContext)

	// EnterForStatement is called when entering the forStatement production.
	EnterForStatement(c *ForStatementContext)

	// EnterSingleStatement is called when entering the singleStatement production.
	EnterSingleStatement(c *SingleStatementContext)

	// EnterBeginLabel is called when entering the beginLabel production.
	EnterBeginLabel(c *BeginLabelContext)

	// EnterEndLabel is called when entering the endLabel production.
	EnterEndLabel(c *EndLabelContext)

	// EnterSingleExpression is called when entering the singleExpression production.
	EnterSingleExpression(c *SingleExpressionContext)

	// EnterSingleTableIdentifier is called when entering the singleTableIdentifier production.
	EnterSingleTableIdentifier(c *SingleTableIdentifierContext)

	// EnterSingleMultipartIdentifier is called when entering the singleMultipartIdentifier production.
	EnterSingleMultipartIdentifier(c *SingleMultipartIdentifierContext)

	// EnterSingleFunctionIdentifier is called when entering the singleFunctionIdentifier production.
	EnterSingleFunctionIdentifier(c *SingleFunctionIdentifierContext)

	// EnterSingleDataType is called when entering the singleDataType production.
	EnterSingleDataType(c *SingleDataTypeContext)

	// EnterSingleTableSchema is called when entering the singleTableSchema production.
	EnterSingleTableSchema(c *SingleTableSchemaContext)

	// EnterSingleRoutineParamList is called when entering the singleRoutineParamList production.
	EnterSingleRoutineParamList(c *SingleRoutineParamListContext)

	// EnterStatementDefault is called when entering the statementDefault production.
	EnterStatementDefault(c *StatementDefaultContext)

	// EnterVisitExecuteImmediate is called when entering the visitExecuteImmediate production.
	EnterVisitExecuteImmediate(c *VisitExecuteImmediateContext)

	// EnterDmlStatement is called when entering the dmlStatement production.
	EnterDmlStatement(c *DmlStatementContext)

	// EnterUse is called when entering the use production.
	EnterUse(c *UseContext)

	// EnterUseNamespace is called when entering the useNamespace production.
	EnterUseNamespace(c *UseNamespaceContext)

	// EnterSetCatalog is called when entering the setCatalog production.
	EnterSetCatalog(c *SetCatalogContext)

	// EnterCreateNamespace is called when entering the createNamespace production.
	EnterCreateNamespace(c *CreateNamespaceContext)

	// EnterSetNamespaceProperties is called when entering the setNamespaceProperties production.
	EnterSetNamespaceProperties(c *SetNamespacePropertiesContext)

	// EnterUnsetNamespaceProperties is called when entering the unsetNamespaceProperties production.
	EnterUnsetNamespaceProperties(c *UnsetNamespacePropertiesContext)

	// EnterSetNamespaceCollation is called when entering the setNamespaceCollation production.
	EnterSetNamespaceCollation(c *SetNamespaceCollationContext)

	// EnterSetNamespaceLocation is called when entering the setNamespaceLocation production.
	EnterSetNamespaceLocation(c *SetNamespaceLocationContext)

	// EnterDropNamespace is called when entering the dropNamespace production.
	EnterDropNamespace(c *DropNamespaceContext)

	// EnterShowNamespaces is called when entering the showNamespaces production.
	EnterShowNamespaces(c *ShowNamespacesContext)

	// EnterCreateTable is called when entering the createTable production.
	EnterCreateTable(c *CreateTableContext)

	// EnterCreateTableLike is called when entering the createTableLike production.
	EnterCreateTableLike(c *CreateTableLikeContext)

	// EnterReplaceTable is called when entering the replaceTable production.
	EnterReplaceTable(c *ReplaceTableContext)

	// EnterAnalyze is called when entering the analyze production.
	EnterAnalyze(c *AnalyzeContext)

	// EnterAnalyzeTables is called when entering the analyzeTables production.
	EnterAnalyzeTables(c *AnalyzeTablesContext)

	// EnterAddTableColumns is called when entering the addTableColumns production.
	EnterAddTableColumns(c *AddTableColumnsContext)

	// EnterRenameTableColumn is called when entering the renameTableColumn production.
	EnterRenameTableColumn(c *RenameTableColumnContext)

	// EnterDropTableColumns is called when entering the dropTableColumns production.
	EnterDropTableColumns(c *DropTableColumnsContext)

	// EnterRenameTable is called when entering the renameTable production.
	EnterRenameTable(c *RenameTableContext)

	// EnterSetTableProperties is called when entering the setTableProperties production.
	EnterSetTableProperties(c *SetTablePropertiesContext)

	// EnterUnsetTableProperties is called when entering the unsetTableProperties production.
	EnterUnsetTableProperties(c *UnsetTablePropertiesContext)

	// EnterAlterTableAlterColumn is called when entering the alterTableAlterColumn production.
	EnterAlterTableAlterColumn(c *AlterTableAlterColumnContext)

	// EnterHiveChangeColumn is called when entering the hiveChangeColumn production.
	EnterHiveChangeColumn(c *HiveChangeColumnContext)

	// EnterHiveReplaceColumns is called when entering the hiveReplaceColumns production.
	EnterHiveReplaceColumns(c *HiveReplaceColumnsContext)

	// EnterSetTableSerDe is called when entering the setTableSerDe production.
	EnterSetTableSerDe(c *SetTableSerDeContext)

	// EnterAddTablePartition is called when entering the addTablePartition production.
	EnterAddTablePartition(c *AddTablePartitionContext)

	// EnterRenameTablePartition is called when entering the renameTablePartition production.
	EnterRenameTablePartition(c *RenameTablePartitionContext)

	// EnterDropTablePartitions is called when entering the dropTablePartitions production.
	EnterDropTablePartitions(c *DropTablePartitionsContext)

	// EnterSetTableLocation is called when entering the setTableLocation production.
	EnterSetTableLocation(c *SetTableLocationContext)

	// EnterRecoverPartitions is called when entering the recoverPartitions production.
	EnterRecoverPartitions(c *RecoverPartitionsContext)

	// EnterAlterClusterBy is called when entering the alterClusterBy production.
	EnterAlterClusterBy(c *AlterClusterByContext)

	// EnterAlterTableCollation is called when entering the alterTableCollation production.
	EnterAlterTableCollation(c *AlterTableCollationContext)

	// EnterAddTableConstraint is called when entering the addTableConstraint production.
	EnterAddTableConstraint(c *AddTableConstraintContext)

	// EnterDropTableConstraint is called when entering the dropTableConstraint production.
	EnterDropTableConstraint(c *DropTableConstraintContext)

	// EnterDropTable is called when entering the dropTable production.
	EnterDropTable(c *DropTableContext)

	// EnterDropView is called when entering the dropView production.
	EnterDropView(c *DropViewContext)

	// EnterCreateView is called when entering the createView production.
	EnterCreateView(c *CreateViewContext)

	// EnterCreateTempViewUsing is called when entering the createTempViewUsing production.
	EnterCreateTempViewUsing(c *CreateTempViewUsingContext)

	// EnterAlterViewQuery is called when entering the alterViewQuery production.
	EnterAlterViewQuery(c *AlterViewQueryContext)

	// EnterAlterViewSchemaBinding is called when entering the alterViewSchemaBinding production.
	EnterAlterViewSchemaBinding(c *AlterViewSchemaBindingContext)

	// EnterCreateFunction is called when entering the createFunction production.
	EnterCreateFunction(c *CreateFunctionContext)

	// EnterCreateUserDefinedFunction is called when entering the createUserDefinedFunction production.
	EnterCreateUserDefinedFunction(c *CreateUserDefinedFunctionContext)

	// EnterDropFunction is called when entering the dropFunction production.
	EnterDropFunction(c *DropFunctionContext)

	// EnterCreateVariable is called when entering the createVariable production.
	EnterCreateVariable(c *CreateVariableContext)

	// EnterDropVariable is called when entering the dropVariable production.
	EnterDropVariable(c *DropVariableContext)

	// EnterExplain is called when entering the explain production.
	EnterExplain(c *ExplainContext)

	// EnterShowTables is called when entering the showTables production.
	EnterShowTables(c *ShowTablesContext)

	// EnterShowTableExtended is called when entering the showTableExtended production.
	EnterShowTableExtended(c *ShowTableExtendedContext)

	// EnterShowTblProperties is called when entering the showTblProperties production.
	EnterShowTblProperties(c *ShowTblPropertiesContext)

	// EnterShowColumns is called when entering the showColumns production.
	EnterShowColumns(c *ShowColumnsContext)

	// EnterShowViews is called when entering the showViews production.
	EnterShowViews(c *ShowViewsContext)

	// EnterShowPartitions is called when entering the showPartitions production.
	EnterShowPartitions(c *ShowPartitionsContext)

	// EnterShowFunctions is called when entering the showFunctions production.
	EnterShowFunctions(c *ShowFunctionsContext)

	// EnterShowProcedures is called when entering the showProcedures production.
	EnterShowProcedures(c *ShowProceduresContext)

	// EnterShowCreateTable is called when entering the showCreateTable production.
	EnterShowCreateTable(c *ShowCreateTableContext)

	// EnterShowCurrentNamespace is called when entering the showCurrentNamespace production.
	EnterShowCurrentNamespace(c *ShowCurrentNamespaceContext)

	// EnterShowCatalogs is called when entering the showCatalogs production.
	EnterShowCatalogs(c *ShowCatalogsContext)

	// EnterDescribeFunction is called when entering the describeFunction production.
	EnterDescribeFunction(c *DescribeFunctionContext)

	// EnterDescribeProcedure is called when entering the describeProcedure production.
	EnterDescribeProcedure(c *DescribeProcedureContext)

	// EnterDescribeNamespace is called when entering the describeNamespace production.
	EnterDescribeNamespace(c *DescribeNamespaceContext)

	// EnterDescribeRelation is called when entering the describeRelation production.
	EnterDescribeRelation(c *DescribeRelationContext)

	// EnterDescribeQuery is called when entering the describeQuery production.
	EnterDescribeQuery(c *DescribeQueryContext)

	// EnterCommentNamespace is called when entering the commentNamespace production.
	EnterCommentNamespace(c *CommentNamespaceContext)

	// EnterCommentTable is called when entering the commentTable production.
	EnterCommentTable(c *CommentTableContext)

	// EnterRefreshTable is called when entering the refreshTable production.
	EnterRefreshTable(c *RefreshTableContext)

	// EnterRefreshFunction is called when entering the refreshFunction production.
	EnterRefreshFunction(c *RefreshFunctionContext)

	// EnterRefreshResource is called when entering the refreshResource production.
	EnterRefreshResource(c *RefreshResourceContext)

	// EnterCacheTable is called when entering the cacheTable production.
	EnterCacheTable(c *CacheTableContext)

	// EnterUncacheTable is called when entering the uncacheTable production.
	EnterUncacheTable(c *UncacheTableContext)

	// EnterClearCache is called when entering the clearCache production.
	EnterClearCache(c *ClearCacheContext)

	// EnterLoadData is called when entering the loadData production.
	EnterLoadData(c *LoadDataContext)

	// EnterTruncateTable is called when entering the truncateTable production.
	EnterTruncateTable(c *TruncateTableContext)

	// EnterRepairTable is called when entering the repairTable production.
	EnterRepairTable(c *RepairTableContext)

	// EnterManageResource is called when entering the manageResource production.
	EnterManageResource(c *ManageResourceContext)

	// EnterCreateIndex is called when entering the createIndex production.
	EnterCreateIndex(c *CreateIndexContext)

	// EnterDropIndex is called when entering the dropIndex production.
	EnterDropIndex(c *DropIndexContext)

	// EnterCall is called when entering the call production.
	EnterCall(c *CallContext)

	// EnterFailNativeCommand is called when entering the failNativeCommand production.
	EnterFailNativeCommand(c *FailNativeCommandContext)

	// EnterCreatePipelineDataset is called when entering the createPipelineDataset production.
	EnterCreatePipelineDataset(c *CreatePipelineDatasetContext)

	// EnterCreatePipelineInsertIntoFlow is called when entering the createPipelineInsertIntoFlow production.
	EnterCreatePipelineInsertIntoFlow(c *CreatePipelineInsertIntoFlowContext)

	// EnterMaterializedView is called when entering the materializedView production.
	EnterMaterializedView(c *MaterializedViewContext)

	// EnterStreamingTable is called when entering the streamingTable production.
	EnterStreamingTable(c *StreamingTableContext)

	// EnterCreatePipelineDatasetHeader is called when entering the createPipelineDatasetHeader production.
	EnterCreatePipelineDatasetHeader(c *CreatePipelineDatasetHeaderContext)

	// EnterStreamTableName is called when entering the streamTableName production.
	EnterStreamTableName(c *StreamTableNameContext)

	// EnterFailSetRole is called when entering the failSetRole production.
	EnterFailSetRole(c *FailSetRoleContext)

	// EnterSetTimeZone is called when entering the setTimeZone production.
	EnterSetTimeZone(c *SetTimeZoneContext)

	// EnterSetVariable is called when entering the setVariable production.
	EnterSetVariable(c *SetVariableContext)

	// EnterSetQuotedConfiguration is called when entering the setQuotedConfiguration production.
	EnterSetQuotedConfiguration(c *SetQuotedConfigurationContext)

	// EnterSetConfiguration is called when entering the setConfiguration production.
	EnterSetConfiguration(c *SetConfigurationContext)

	// EnterResetQuotedConfiguration is called when entering the resetQuotedConfiguration production.
	EnterResetQuotedConfiguration(c *ResetQuotedConfigurationContext)

	// EnterResetConfiguration is called when entering the resetConfiguration production.
	EnterResetConfiguration(c *ResetConfigurationContext)

	// EnterExecuteImmediate is called when entering the executeImmediate production.
	EnterExecuteImmediate(c *ExecuteImmediateContext)

	// EnterExecuteImmediateUsing is called when entering the executeImmediateUsing production.
	EnterExecuteImmediateUsing(c *ExecuteImmediateUsingContext)

	// EnterTimezone is called when entering the timezone production.
	EnterTimezone(c *TimezoneContext)

	// EnterConfigKey is called when entering the configKey production.
	EnterConfigKey(c *ConfigKeyContext)

	// EnterConfigValue is called when entering the configValue production.
	EnterConfigValue(c *ConfigValueContext)

	// EnterUnsupportedHiveNativeCommands is called when entering the unsupportedHiveNativeCommands production.
	EnterUnsupportedHiveNativeCommands(c *UnsupportedHiveNativeCommandsContext)

	// EnterCreateTableHeader is called when entering the createTableHeader production.
	EnterCreateTableHeader(c *CreateTableHeaderContext)

	// EnterReplaceTableHeader is called when entering the replaceTableHeader production.
	EnterReplaceTableHeader(c *ReplaceTableHeaderContext)

	// EnterClusterBySpec is called when entering the clusterBySpec production.
	EnterClusterBySpec(c *ClusterBySpecContext)

	// EnterBucketSpec is called when entering the bucketSpec production.
	EnterBucketSpec(c *BucketSpecContext)

	// EnterSkewSpec is called when entering the skewSpec production.
	EnterSkewSpec(c *SkewSpecContext)

	// EnterLocationSpec is called when entering the locationSpec production.
	EnterLocationSpec(c *LocationSpecContext)

	// EnterSchemaBinding is called when entering the schemaBinding production.
	EnterSchemaBinding(c *SchemaBindingContext)

	// EnterCommentSpec is called when entering the commentSpec production.
	EnterCommentSpec(c *CommentSpecContext)

	// EnterSingleQuery is called when entering the singleQuery production.
	EnterSingleQuery(c *SingleQueryContext)

	// EnterQuery is called when entering the query production.
	EnterQuery(c *QueryContext)

	// EnterInsertOverwriteTable is called when entering the insertOverwriteTable production.
	EnterInsertOverwriteTable(c *InsertOverwriteTableContext)

	// EnterInsertIntoTable is called when entering the insertIntoTable production.
	EnterInsertIntoTable(c *InsertIntoTableContext)

	// EnterInsertIntoReplaceWhere is called when entering the insertIntoReplaceWhere production.
	EnterInsertIntoReplaceWhere(c *InsertIntoReplaceWhereContext)

	// EnterInsertOverwriteHiveDir is called when entering the insertOverwriteHiveDir production.
	EnterInsertOverwriteHiveDir(c *InsertOverwriteHiveDirContext)

	// EnterInsertOverwriteDir is called when entering the insertOverwriteDir production.
	EnterInsertOverwriteDir(c *InsertOverwriteDirContext)

	// EnterPartitionSpecLocation is called when entering the partitionSpecLocation production.
	EnterPartitionSpecLocation(c *PartitionSpecLocationContext)

	// EnterPartitionSpec is called when entering the partitionSpec production.
	EnterPartitionSpec(c *PartitionSpecContext)

	// EnterPartitionVal is called when entering the partitionVal production.
	EnterPartitionVal(c *PartitionValContext)

	// EnterCreatePipelineFlowHeader is called when entering the createPipelineFlowHeader production.
	EnterCreatePipelineFlowHeader(c *CreatePipelineFlowHeaderContext)

	// EnterNamespace is called when entering the namespace production.
	EnterNamespace(c *NamespaceContext)

	// EnterNamespaces is called when entering the namespaces production.
	EnterNamespaces(c *NamespacesContext)

	// EnterVariable is called when entering the variable production.
	EnterVariable(c *VariableContext)

	// EnterDescribeFuncName is called when entering the describeFuncName production.
	EnterDescribeFuncName(c *DescribeFuncNameContext)

	// EnterDescribeColName is called when entering the describeColName production.
	EnterDescribeColName(c *DescribeColNameContext)

	// EnterCtes is called when entering the ctes production.
	EnterCtes(c *CtesContext)

	// EnterNamedQuery is called when entering the namedQuery production.
	EnterNamedQuery(c *NamedQueryContext)

	// EnterTableProvider is called when entering the tableProvider production.
	EnterTableProvider(c *TableProviderContext)

	// EnterCreateTableClauses is called when entering the createTableClauses production.
	EnterCreateTableClauses(c *CreateTableClausesContext)

	// EnterPropertyList is called when entering the propertyList production.
	EnterPropertyList(c *PropertyListContext)

	// EnterPropertyWithKeyAndEquals is called when entering the propertyWithKeyAndEquals production.
	EnterPropertyWithKeyAndEquals(c *PropertyWithKeyAndEqualsContext)

	// EnterPropertyWithKeyNoEquals is called when entering the propertyWithKeyNoEquals production.
	EnterPropertyWithKeyNoEquals(c *PropertyWithKeyNoEqualsContext)

	// EnterPropertyKey is called when entering the propertyKey production.
	EnterPropertyKey(c *PropertyKeyContext)

	// EnterPropertyKeyOrStringLit is called when entering the propertyKeyOrStringLit production.
	EnterPropertyKeyOrStringLit(c *PropertyKeyOrStringLitContext)

	// EnterPropertyKeyOrStringLitNoCoalesce is called when entering the propertyKeyOrStringLitNoCoalesce production.
	EnterPropertyKeyOrStringLitNoCoalesce(c *PropertyKeyOrStringLitNoCoalesceContext)

	// EnterPropertyValue is called when entering the propertyValue production.
	EnterPropertyValue(c *PropertyValueContext)

	// EnterExpressionPropertyList is called when entering the expressionPropertyList production.
	EnterExpressionPropertyList(c *ExpressionPropertyListContext)

	// EnterExpressionPropertyWithKeyAndEquals is called when entering the expressionPropertyWithKeyAndEquals production.
	EnterExpressionPropertyWithKeyAndEquals(c *ExpressionPropertyWithKeyAndEqualsContext)

	// EnterExpressionPropertyWithKeyNoEquals is called when entering the expressionPropertyWithKeyNoEquals production.
	EnterExpressionPropertyWithKeyNoEquals(c *ExpressionPropertyWithKeyNoEqualsContext)

	// EnterConstantList is called when entering the constantList production.
	EnterConstantList(c *ConstantListContext)

	// EnterNestedConstantList is called when entering the nestedConstantList production.
	EnterNestedConstantList(c *NestedConstantListContext)

	// EnterCreateFileFormat is called when entering the createFileFormat production.
	EnterCreateFileFormat(c *CreateFileFormatContext)

	// EnterTableFileFormat is called when entering the tableFileFormat production.
	EnterTableFileFormat(c *TableFileFormatContext)

	// EnterGenericFileFormat is called when entering the genericFileFormat production.
	EnterGenericFileFormat(c *GenericFileFormatContext)

	// EnterStorageHandler is called when entering the storageHandler production.
	EnterStorageHandler(c *StorageHandlerContext)

	// EnterResource is called when entering the resource production.
	EnterResource(c *ResourceContext)

	// EnterSingleInsertQuery is called when entering the singleInsertQuery production.
	EnterSingleInsertQuery(c *SingleInsertQueryContext)

	// EnterMultiInsertQuery is called when entering the multiInsertQuery production.
	EnterMultiInsertQuery(c *MultiInsertQueryContext)

	// EnterDeleteFromTable is called when entering the deleteFromTable production.
	EnterDeleteFromTable(c *DeleteFromTableContext)

	// EnterUpdateTable is called when entering the updateTable production.
	EnterUpdateTable(c *UpdateTableContext)

	// EnterMergeIntoTable is called when entering the mergeIntoTable production.
	EnterMergeIntoTable(c *MergeIntoTableContext)

	// EnterIdentifierReference is called when entering the identifierReference production.
	EnterIdentifierReference(c *IdentifierReferenceContext)

	// EnterCatalogIdentifierReference is called when entering the catalogIdentifierReference production.
	EnterCatalogIdentifierReference(c *CatalogIdentifierReferenceContext)

	// EnterQueryOrganization is called when entering the queryOrganization production.
	EnterQueryOrganization(c *QueryOrganizationContext)

	// EnterMultiInsertQueryBody is called when entering the multiInsertQueryBody production.
	EnterMultiInsertQueryBody(c *MultiInsertQueryBodyContext)

	// EnterOperatorPipeStatement is called when entering the operatorPipeStatement production.
	EnterOperatorPipeStatement(c *OperatorPipeStatementContext)

	// EnterQueryTermDefault is called when entering the queryTermDefault production.
	EnterQueryTermDefault(c *QueryTermDefaultContext)

	// EnterSetOperation is called when entering the setOperation production.
	EnterSetOperation(c *SetOperationContext)

	// EnterQueryPrimaryDefault is called when entering the queryPrimaryDefault production.
	EnterQueryPrimaryDefault(c *QueryPrimaryDefaultContext)

	// EnterFromStmt is called when entering the fromStmt production.
	EnterFromStmt(c *FromStmtContext)

	// EnterTable is called when entering the table production.
	EnterTable(c *TableContext)

	// EnterInlineTableDefault1 is called when entering the inlineTableDefault1 production.
	EnterInlineTableDefault1(c *InlineTableDefault1Context)

	// EnterSubquery is called when entering the subquery production.
	EnterSubquery(c *SubqueryContext)

	// EnterSortItem is called when entering the sortItem production.
	EnterSortItem(c *SortItemContext)

	// EnterFromStatement is called when entering the fromStatement production.
	EnterFromStatement(c *FromStatementContext)

	// EnterFromStatementBody is called when entering the fromStatementBody production.
	EnterFromStatementBody(c *FromStatementBodyContext)

	// EnterTransformQuerySpecification is called when entering the transformQuerySpecification production.
	EnterTransformQuerySpecification(c *TransformQuerySpecificationContext)

	// EnterRegularQuerySpecification is called when entering the regularQuerySpecification production.
	EnterRegularQuerySpecification(c *RegularQuerySpecificationContext)

	// EnterTransformClause is called when entering the transformClause production.
	EnterTransformClause(c *TransformClauseContext)

	// EnterSelectClause is called when entering the selectClause production.
	EnterSelectClause(c *SelectClauseContext)

	// EnterSetClause is called when entering the setClause production.
	EnterSetClause(c *SetClauseContext)

	// EnterMatchedClause is called when entering the matchedClause production.
	EnterMatchedClause(c *MatchedClauseContext)

	// EnterNotMatchedClause is called when entering the notMatchedClause production.
	EnterNotMatchedClause(c *NotMatchedClauseContext)

	// EnterNotMatchedBySourceClause is called when entering the notMatchedBySourceClause production.
	EnterNotMatchedBySourceClause(c *NotMatchedBySourceClauseContext)

	// EnterMatchedAction is called when entering the matchedAction production.
	EnterMatchedAction(c *MatchedActionContext)

	// EnterNotMatchedAction is called when entering the notMatchedAction production.
	EnterNotMatchedAction(c *NotMatchedActionContext)

	// EnterNotMatchedBySourceAction is called when entering the notMatchedBySourceAction production.
	EnterNotMatchedBySourceAction(c *NotMatchedBySourceActionContext)

	// EnterExceptClause is called when entering the exceptClause production.
	EnterExceptClause(c *ExceptClauseContext)

	// EnterAssignmentList is called when entering the assignmentList production.
	EnterAssignmentList(c *AssignmentListContext)

	// EnterAssignment is called when entering the assignment production.
	EnterAssignment(c *AssignmentContext)

	// EnterWhereClause is called when entering the whereClause production.
	EnterWhereClause(c *WhereClauseContext)

	// EnterHavingClause is called when entering the havingClause production.
	EnterHavingClause(c *HavingClauseContext)

	// EnterHint is called when entering the hint production.
	EnterHint(c *HintContext)

	// EnterHintStatement is called when entering the hintStatement production.
	EnterHintStatement(c *HintStatementContext)

	// EnterFromClause is called when entering the fromClause production.
	EnterFromClause(c *FromClauseContext)

	// EnterTemporalClause is called when entering the temporalClause production.
	EnterTemporalClause(c *TemporalClauseContext)

	// EnterAggregationClause is called when entering the aggregationClause production.
	EnterAggregationClause(c *AggregationClauseContext)

	// EnterGroupByClause is called when entering the groupByClause production.
	EnterGroupByClause(c *GroupByClauseContext)

	// EnterGroupingAnalytics is called when entering the groupingAnalytics production.
	EnterGroupingAnalytics(c *GroupingAnalyticsContext)

	// EnterGroupingElement is called when entering the groupingElement production.
	EnterGroupingElement(c *GroupingElementContext)

	// EnterGroupingSet is called when entering the groupingSet production.
	EnterGroupingSet(c *GroupingSetContext)

	// EnterPivotClause is called when entering the pivotClause production.
	EnterPivotClause(c *PivotClauseContext)

	// EnterPivotColumn is called when entering the pivotColumn production.
	EnterPivotColumn(c *PivotColumnContext)

	// EnterPivotValue is called when entering the pivotValue production.
	EnterPivotValue(c *PivotValueContext)

	// EnterUnpivotClause is called when entering the unpivotClause production.
	EnterUnpivotClause(c *UnpivotClauseContext)

	// EnterUnpivotNullClause is called when entering the unpivotNullClause production.
	EnterUnpivotNullClause(c *UnpivotNullClauseContext)

	// EnterUnpivotOperator is called when entering the unpivotOperator production.
	EnterUnpivotOperator(c *UnpivotOperatorContext)

	// EnterUnpivotSingleValueColumnClause is called when entering the unpivotSingleValueColumnClause production.
	EnterUnpivotSingleValueColumnClause(c *UnpivotSingleValueColumnClauseContext)

	// EnterUnpivotMultiValueColumnClause is called when entering the unpivotMultiValueColumnClause production.
	EnterUnpivotMultiValueColumnClause(c *UnpivotMultiValueColumnClauseContext)

	// EnterUnpivotColumnSet is called when entering the unpivotColumnSet production.
	EnterUnpivotColumnSet(c *UnpivotColumnSetContext)

	// EnterUnpivotValueColumn is called when entering the unpivotValueColumn production.
	EnterUnpivotValueColumn(c *UnpivotValueColumnContext)

	// EnterUnpivotNameColumn is called when entering the unpivotNameColumn production.
	EnterUnpivotNameColumn(c *UnpivotNameColumnContext)

	// EnterUnpivotColumnAndAlias is called when entering the unpivotColumnAndAlias production.
	EnterUnpivotColumnAndAlias(c *UnpivotColumnAndAliasContext)

	// EnterUnpivotColumn is called when entering the unpivotColumn production.
	EnterUnpivotColumn(c *UnpivotColumnContext)

	// EnterUnpivotAlias is called when entering the unpivotAlias production.
	EnterUnpivotAlias(c *UnpivotAliasContext)

	// EnterLateralView is called when entering the lateralView production.
	EnterLateralView(c *LateralViewContext)

	// EnterWatermarkClause is called when entering the watermarkClause production.
	EnterWatermarkClause(c *WatermarkClauseContext)

	// EnterSetQuantifier is called when entering the setQuantifier production.
	EnterSetQuantifier(c *SetQuantifierContext)

	// EnterRelation is called when entering the relation production.
	EnterRelation(c *RelationContext)

	// EnterRelationExtension is called when entering the relationExtension production.
	EnterRelationExtension(c *RelationExtensionContext)

	// EnterJoinRelation is called when entering the joinRelation production.
	EnterJoinRelation(c *JoinRelationContext)

	// EnterJoinType is called when entering the joinType production.
	EnterJoinType(c *JoinTypeContext)

	// EnterJoinCriteria is called when entering the joinCriteria production.
	EnterJoinCriteria(c *JoinCriteriaContext)

	// EnterSample is called when entering the sample production.
	EnterSample(c *SampleContext)

	// EnterSampleByPercentile is called when entering the sampleByPercentile production.
	EnterSampleByPercentile(c *SampleByPercentileContext)

	// EnterSampleByRows is called when entering the sampleByRows production.
	EnterSampleByRows(c *SampleByRowsContext)

	// EnterSampleByBucket is called when entering the sampleByBucket production.
	EnterSampleByBucket(c *SampleByBucketContext)

	// EnterSampleByBytes is called when entering the sampleByBytes production.
	EnterSampleByBytes(c *SampleByBytesContext)

	// EnterIdentifierList is called when entering the identifierList production.
	EnterIdentifierList(c *IdentifierListContext)

	// EnterIdentifierSeq is called when entering the identifierSeq production.
	EnterIdentifierSeq(c *IdentifierSeqContext)

	// EnterOrderedIdentifierList is called when entering the orderedIdentifierList production.
	EnterOrderedIdentifierList(c *OrderedIdentifierListContext)

	// EnterOrderedIdentifier is called when entering the orderedIdentifier production.
	EnterOrderedIdentifier(c *OrderedIdentifierContext)

	// EnterIdentifierCommentList is called when entering the identifierCommentList production.
	EnterIdentifierCommentList(c *IdentifierCommentListContext)

	// EnterIdentifierComment is called when entering the identifierComment production.
	EnterIdentifierComment(c *IdentifierCommentContext)

	// EnterStreamRelation is called when entering the streamRelation production.
	EnterStreamRelation(c *StreamRelationContext)

	// EnterTableName is called when entering the tableName production.
	EnterTableName(c *TableNameContext)

	// EnterAliasedQuery is called when entering the aliasedQuery production.
	EnterAliasedQuery(c *AliasedQueryContext)

	// EnterAliasedRelation is called when entering the aliasedRelation production.
	EnterAliasedRelation(c *AliasedRelationContext)

	// EnterInlineTableDefault2 is called when entering the inlineTableDefault2 production.
	EnterInlineTableDefault2(c *InlineTableDefault2Context)

	// EnterTableValuedFunction is called when entering the tableValuedFunction production.
	EnterTableValuedFunction(c *TableValuedFunctionContext)

	// EnterOptionsClause is called when entering the optionsClause production.
	EnterOptionsClause(c *OptionsClauseContext)

	// EnterInlineTable is called when entering the inlineTable production.
	EnterInlineTable(c *InlineTableContext)

	// EnterFunctionTableSubqueryArgument is called when entering the functionTableSubqueryArgument production.
	EnterFunctionTableSubqueryArgument(c *FunctionTableSubqueryArgumentContext)

	// EnterTableArgumentPartitioning is called when entering the tableArgumentPartitioning production.
	EnterTableArgumentPartitioning(c *TableArgumentPartitioningContext)

	// EnterFunctionTableNamedArgumentExpression is called when entering the functionTableNamedArgumentExpression production.
	EnterFunctionTableNamedArgumentExpression(c *FunctionTableNamedArgumentExpressionContext)

	// EnterFunctionTableReferenceArgument is called when entering the functionTableReferenceArgument production.
	EnterFunctionTableReferenceArgument(c *FunctionTableReferenceArgumentContext)

	// EnterFunctionTableArgument is called when entering the functionTableArgument production.
	EnterFunctionTableArgument(c *FunctionTableArgumentContext)

	// EnterFunctionTable is called when entering the functionTable production.
	EnterFunctionTable(c *FunctionTableContext)

	// EnterTableAlias is called when entering the tableAlias production.
	EnterTableAlias(c *TableAliasContext)

	// EnterRowFormatSerde is called when entering the rowFormatSerde production.
	EnterRowFormatSerde(c *RowFormatSerdeContext)

	// EnterRowFormatDelimited is called when entering the rowFormatDelimited production.
	EnterRowFormatDelimited(c *RowFormatDelimitedContext)

	// EnterMultipartIdentifierList is called when entering the multipartIdentifierList production.
	EnterMultipartIdentifierList(c *MultipartIdentifierListContext)

	// EnterMultipartIdentifier is called when entering the multipartIdentifier production.
	EnterMultipartIdentifier(c *MultipartIdentifierContext)

	// EnterMultipartIdentifierPropertyList is called when entering the multipartIdentifierPropertyList production.
	EnterMultipartIdentifierPropertyList(c *MultipartIdentifierPropertyListContext)

	// EnterMultipartIdentifierProperty is called when entering the multipartIdentifierProperty production.
	EnterMultipartIdentifierProperty(c *MultipartIdentifierPropertyContext)

	// EnterTableIdentifier is called when entering the tableIdentifier production.
	EnterTableIdentifier(c *TableIdentifierContext)

	// EnterFunctionIdentifier is called when entering the functionIdentifier production.
	EnterFunctionIdentifier(c *FunctionIdentifierContext)

	// EnterNamedExpression is called when entering the namedExpression production.
	EnterNamedExpression(c *NamedExpressionContext)

	// EnterNamedExpressionSeq is called when entering the namedExpressionSeq production.
	EnterNamedExpressionSeq(c *NamedExpressionSeqContext)

	// EnterPartitionFieldList is called when entering the partitionFieldList production.
	EnterPartitionFieldList(c *PartitionFieldListContext)

	// EnterPartitionTransform is called when entering the partitionTransform production.
	EnterPartitionTransform(c *PartitionTransformContext)

	// EnterPartitionColumn is called when entering the partitionColumn production.
	EnterPartitionColumn(c *PartitionColumnContext)

	// EnterIdentityTransform is called when entering the identityTransform production.
	EnterIdentityTransform(c *IdentityTransformContext)

	// EnterApplyTransform is called when entering the applyTransform production.
	EnterApplyTransform(c *ApplyTransformContext)

	// EnterTransformArgument is called when entering the transformArgument production.
	EnterTransformArgument(c *TransformArgumentContext)

	// EnterExpression is called when entering the expression production.
	EnterExpression(c *ExpressionContext)

	// EnterNamedArgumentExpression is called when entering the namedArgumentExpression production.
	EnterNamedArgumentExpression(c *NamedArgumentExpressionContext)

	// EnterFunctionArgument is called when entering the functionArgument production.
	EnterFunctionArgument(c *FunctionArgumentContext)

	// EnterExpressionSeq is called when entering the expressionSeq production.
	EnterExpressionSeq(c *ExpressionSeqContext)

	// EnterLogicalNot is called when entering the logicalNot production.
	EnterLogicalNot(c *LogicalNotContext)

	// EnterPredicated is called when entering the predicated production.
	EnterPredicated(c *PredicatedContext)

	// EnterExists is called when entering the exists production.
	EnterExists(c *ExistsContext)

	// EnterLogicalBinary is called when entering the logicalBinary production.
	EnterLogicalBinary(c *LogicalBinaryContext)

	// EnterPredicate is called when entering the predicate production.
	EnterPredicate(c *PredicateContext)

	// EnterErrorCapturingNot is called when entering the errorCapturingNot production.
	EnterErrorCapturingNot(c *ErrorCapturingNotContext)

	// EnterValueExpressionDefault is called when entering the valueExpressionDefault production.
	EnterValueExpressionDefault(c *ValueExpressionDefaultContext)

	// EnterComparison is called when entering the comparison production.
	EnterComparison(c *ComparisonContext)

	// EnterShiftExpression is called when entering the shiftExpression production.
	EnterShiftExpression(c *ShiftExpressionContext)

	// EnterArithmeticBinary is called when entering the arithmeticBinary production.
	EnterArithmeticBinary(c *ArithmeticBinaryContext)

	// EnterArithmeticUnary is called when entering the arithmeticUnary production.
	EnterArithmeticUnary(c *ArithmeticUnaryContext)

	// EnterShiftOperator is called when entering the shiftOperator production.
	EnterShiftOperator(c *ShiftOperatorContext)

	// EnterDatetimeUnit is called when entering the datetimeUnit production.
	EnterDatetimeUnit(c *DatetimeUnitContext)

	// EnterStruct is called when entering the struct production.
	EnterStruct(c *StructContext)

	// EnterDereference is called when entering the dereference production.
	EnterDereference(c *DereferenceContext)

	// EnterCastByColon is called when entering the castByColon production.
	EnterCastByColon(c *CastByColonContext)

	// EnterTimestampadd is called when entering the timestampadd production.
	EnterTimestampadd(c *TimestampaddContext)

	// EnterSubstring is called when entering the substring production.
	EnterSubstring(c *SubstringContext)

	// EnterCast is called when entering the cast production.
	EnterCast(c *CastContext)

	// EnterLambda is called when entering the lambda production.
	EnterLambda(c *LambdaContext)

	// EnterParenthesizedExpression is called when entering the parenthesizedExpression production.
	EnterParenthesizedExpression(c *ParenthesizedExpressionContext)

	// EnterAny_value is called when entering the any_value production.
	EnterAny_value(c *Any_valueContext)

	// EnterTrim is called when entering the trim production.
	EnterTrim(c *TrimContext)

	// EnterSemiStructuredExtract is called when entering the semiStructuredExtract production.
	EnterSemiStructuredExtract(c *SemiStructuredExtractContext)

	// EnterSimpleCase is called when entering the simpleCase production.
	EnterSimpleCase(c *SimpleCaseContext)

	// EnterCurrentLike is called when entering the currentLike production.
	EnterCurrentLike(c *CurrentLikeContext)

	// EnterColumnReference is called when entering the columnReference production.
	EnterColumnReference(c *ColumnReferenceContext)

	// EnterRowConstructor is called when entering the rowConstructor production.
	EnterRowConstructor(c *RowConstructorContext)

	// EnterLast is called when entering the last production.
	EnterLast(c *LastContext)

	// EnterStar is called when entering the star production.
	EnterStar(c *StarContext)

	// EnterOverlay is called when entering the overlay production.
	EnterOverlay(c *OverlayContext)

	// EnterSubscript is called when entering the subscript production.
	EnterSubscript(c *SubscriptContext)

	// EnterTimestampdiff is called when entering the timestampdiff production.
	EnterTimestampdiff(c *TimestampdiffContext)

	// EnterSubqueryExpression is called when entering the subqueryExpression production.
	EnterSubqueryExpression(c *SubqueryExpressionContext)

	// EnterCollate is called when entering the collate production.
	EnterCollate(c *CollateContext)

	// EnterConstantDefault is called when entering the constantDefault production.
	EnterConstantDefault(c *ConstantDefaultContext)

	// EnterExtract is called when entering the extract production.
	EnterExtract(c *ExtractContext)

	// EnterFunctionCall is called when entering the functionCall production.
	EnterFunctionCall(c *FunctionCallContext)

	// EnterSearchedCase is called when entering the searchedCase production.
	EnterSearchedCase(c *SearchedCaseContext)

	// EnterPosition is called when entering the position production.
	EnterPosition(c *PositionContext)

	// EnterFirst is called when entering the first production.
	EnterFirst(c *FirstContext)

	// EnterSemiStructuredExtractionPath is called when entering the semiStructuredExtractionPath production.
	EnterSemiStructuredExtractionPath(c *SemiStructuredExtractionPathContext)

	// EnterJsonPathIdentifier is called when entering the jsonPathIdentifier production.
	EnterJsonPathIdentifier(c *JsonPathIdentifierContext)

	// EnterJsonPathBracketedIdentifier is called when entering the jsonPathBracketedIdentifier production.
	EnterJsonPathBracketedIdentifier(c *JsonPathBracketedIdentifierContext)

	// EnterJsonPathFirstPart is called when entering the jsonPathFirstPart production.
	EnterJsonPathFirstPart(c *JsonPathFirstPartContext)

	// EnterJsonPathParts is called when entering the jsonPathParts production.
	EnterJsonPathParts(c *JsonPathPartsContext)

	// EnterLiteralType is called when entering the literalType production.
	EnterLiteralType(c *LiteralTypeContext)

	// EnterNullLiteral is called when entering the nullLiteral production.
	EnterNullLiteral(c *NullLiteralContext)

	// EnterPosParameterLiteral is called when entering the posParameterLiteral production.
	EnterPosParameterLiteral(c *PosParameterLiteralContext)

	// EnterNamedParameterLiteral is called when entering the namedParameterLiteral production.
	EnterNamedParameterLiteral(c *NamedParameterLiteralContext)

	// EnterIntervalLiteral is called when entering the intervalLiteral production.
	EnterIntervalLiteral(c *IntervalLiteralContext)

	// EnterTypeConstructor is called when entering the typeConstructor production.
	EnterTypeConstructor(c *TypeConstructorContext)

	// EnterNumericLiteral is called when entering the numericLiteral production.
	EnterNumericLiteral(c *NumericLiteralContext)

	// EnterBooleanLiteral is called when entering the booleanLiteral production.
	EnterBooleanLiteral(c *BooleanLiteralContext)

	// EnterStringLiteral is called when entering the stringLiteral production.
	EnterStringLiteral(c *StringLiteralContext)

	// EnterNamedParameterMarker is called when entering the namedParameterMarker production.
	EnterNamedParameterMarker(c *NamedParameterMarkerContext)

	// EnterComparisonOperator is called when entering the comparisonOperator production.
	EnterComparisonOperator(c *ComparisonOperatorContext)

	// EnterArithmeticOperator is called when entering the arithmeticOperator production.
	EnterArithmeticOperator(c *ArithmeticOperatorContext)

	// EnterPredicateOperator is called when entering the predicateOperator production.
	EnterPredicateOperator(c *PredicateOperatorContext)

	// EnterBooleanValue is called when entering the booleanValue production.
	EnterBooleanValue(c *BooleanValueContext)

	// EnterInterval is called when entering the interval production.
	EnterInterval(c *IntervalContext)

	// EnterErrorCapturingMultiUnitsInterval is called when entering the errorCapturingMultiUnitsInterval production.
	EnterErrorCapturingMultiUnitsInterval(c *ErrorCapturingMultiUnitsIntervalContext)

	// EnterMultiUnitsInterval is called when entering the multiUnitsInterval production.
	EnterMultiUnitsInterval(c *MultiUnitsIntervalContext)

	// EnterErrorCapturingUnitToUnitInterval is called when entering the errorCapturingUnitToUnitInterval production.
	EnterErrorCapturingUnitToUnitInterval(c *ErrorCapturingUnitToUnitIntervalContext)

	// EnterUnitToUnitInterval is called when entering the unitToUnitInterval production.
	EnterUnitToUnitInterval(c *UnitToUnitIntervalContext)

	// EnterIntervalValue is called when entering the intervalValue production.
	EnterIntervalValue(c *IntervalValueContext)

	// EnterUnitInMultiUnits is called when entering the unitInMultiUnits production.
	EnterUnitInMultiUnits(c *UnitInMultiUnitsContext)

	// EnterUnitInUnitToUnit is called when entering the unitInUnitToUnit production.
	EnterUnitInUnitToUnit(c *UnitInUnitToUnitContext)

	// EnterColPosition is called when entering the colPosition production.
	EnterColPosition(c *ColPositionContext)

	// EnterCollationSpec is called when entering the collationSpec production.
	EnterCollationSpec(c *CollationSpecContext)

	// EnterCollateClause is called when entering the collateClause production.
	EnterCollateClause(c *CollateClauseContext)

	// EnterNonTrivialPrimitiveType is called when entering the nonTrivialPrimitiveType production.
	EnterNonTrivialPrimitiveType(c *NonTrivialPrimitiveTypeContext)

	// EnterTrivialPrimitiveType is called when entering the trivialPrimitiveType production.
	EnterTrivialPrimitiveType(c *TrivialPrimitiveTypeContext)

	// EnterPrimitiveType is called when entering the primitiveType production.
	EnterPrimitiveType(c *PrimitiveTypeContext)

	// EnterComplexDataType is called when entering the complexDataType production.
	EnterComplexDataType(c *ComplexDataTypeContext)

	// EnterPrimitiveDataType is called when entering the primitiveDataType production.
	EnterPrimitiveDataType(c *PrimitiveDataTypeContext)

	// EnterQualifiedColTypeWithPositionList is called when entering the qualifiedColTypeWithPositionList production.
	EnterQualifiedColTypeWithPositionList(c *QualifiedColTypeWithPositionListContext)

	// EnterQualifiedColTypeWithPosition is called when entering the qualifiedColTypeWithPosition production.
	EnterQualifiedColTypeWithPosition(c *QualifiedColTypeWithPositionContext)

	// EnterColDefinitionDescriptorWithPosition is called when entering the colDefinitionDescriptorWithPosition production.
	EnterColDefinitionDescriptorWithPosition(c *ColDefinitionDescriptorWithPositionContext)

	// EnterDefaultExpression is called when entering the defaultExpression production.
	EnterDefaultExpression(c *DefaultExpressionContext)

	// EnterVariableDefaultExpression is called when entering the variableDefaultExpression production.
	EnterVariableDefaultExpression(c *VariableDefaultExpressionContext)

	// EnterColTypeList is called when entering the colTypeList production.
	EnterColTypeList(c *ColTypeListContext)

	// EnterColType is called when entering the colType production.
	EnterColType(c *ColTypeContext)

	// EnterTableElementList is called when entering the tableElementList production.
	EnterTableElementList(c *TableElementListContext)

	// EnterTableElement is called when entering the tableElement production.
	EnterTableElement(c *TableElementContext)

	// EnterColDefinitionList is called when entering the colDefinitionList production.
	EnterColDefinitionList(c *ColDefinitionListContext)

	// EnterColDefinition is called when entering the colDefinition production.
	EnterColDefinition(c *ColDefinitionContext)

	// EnterColDefinitionOption is called when entering the colDefinitionOption production.
	EnterColDefinitionOption(c *ColDefinitionOptionContext)

	// EnterGeneratedColumn is called when entering the generatedColumn production.
	EnterGeneratedColumn(c *GeneratedColumnContext)

	// EnterIdentityColumn is called when entering the identityColumn production.
	EnterIdentityColumn(c *IdentityColumnContext)

	// EnterIdentityColSpec is called when entering the identityColSpec production.
	EnterIdentityColSpec(c *IdentityColSpecContext)

	// EnterSequenceGeneratorOption is called when entering the sequenceGeneratorOption production.
	EnterSequenceGeneratorOption(c *SequenceGeneratorOptionContext)

	// EnterSequenceGeneratorStartOrStep is called when entering the sequenceGeneratorStartOrStep production.
	EnterSequenceGeneratorStartOrStep(c *SequenceGeneratorStartOrStepContext)

	// EnterComplexColTypeList is called when entering the complexColTypeList production.
	EnterComplexColTypeList(c *ComplexColTypeListContext)

	// EnterComplexColType is called when entering the complexColType production.
	EnterComplexColType(c *ComplexColTypeContext)

	// EnterRoutineCharacteristics is called when entering the routineCharacteristics production.
	EnterRoutineCharacteristics(c *RoutineCharacteristicsContext)

	// EnterRoutineLanguage is called when entering the routineLanguage production.
	EnterRoutineLanguage(c *RoutineLanguageContext)

	// EnterSpecificName is called when entering the specificName production.
	EnterSpecificName(c *SpecificNameContext)

	// EnterDeterministic is called when entering the deterministic production.
	EnterDeterministic(c *DeterministicContext)

	// EnterSqlDataAccess is called when entering the sqlDataAccess production.
	EnterSqlDataAccess(c *SqlDataAccessContext)

	// EnterNullCall is called when entering the nullCall production.
	EnterNullCall(c *NullCallContext)

	// EnterRightsClause is called when entering the rightsClause production.
	EnterRightsClause(c *RightsClauseContext)

	// EnterWhenClause is called when entering the whenClause production.
	EnterWhenClause(c *WhenClauseContext)

	// EnterWindowClause is called when entering the windowClause production.
	EnterWindowClause(c *WindowClauseContext)

	// EnterNamedWindow is called when entering the namedWindow production.
	EnterNamedWindow(c *NamedWindowContext)

	// EnterWindowRef is called when entering the windowRef production.
	EnterWindowRef(c *WindowRefContext)

	// EnterWindowDef is called when entering the windowDef production.
	EnterWindowDef(c *WindowDefContext)

	// EnterWindowFrame is called when entering the windowFrame production.
	EnterWindowFrame(c *WindowFrameContext)

	// EnterFrameBound is called when entering the frameBound production.
	EnterFrameBound(c *FrameBoundContext)

	// EnterQualifiedNameList is called when entering the qualifiedNameList production.
	EnterQualifiedNameList(c *QualifiedNameListContext)

	// EnterFunctionName is called when entering the functionName production.
	EnterFunctionName(c *FunctionNameContext)

	// EnterQualifiedName is called when entering the qualifiedName production.
	EnterQualifiedName(c *QualifiedNameContext)

	// EnterErrorCapturingIdentifier is called when entering the errorCapturingIdentifier production.
	EnterErrorCapturingIdentifier(c *ErrorCapturingIdentifierContext)

	// EnterErrorIdent is called when entering the errorIdent production.
	EnterErrorIdent(c *ErrorIdentContext)

	// EnterRealIdent is called when entering the realIdent production.
	EnterRealIdent(c *RealIdentContext)

	// EnterIdentifier is called when entering the identifier production.
	EnterIdentifier(c *IdentifierContext)

	// EnterSimpleIdentifier is called when entering the simpleIdentifier production.
	EnterSimpleIdentifier(c *SimpleIdentifierContext)

	// EnterUnquotedIdentifier is called when entering the unquotedIdentifier production.
	EnterUnquotedIdentifier(c *UnquotedIdentifierContext)

	// EnterQuotedIdentifierAlternative is called when entering the quotedIdentifierAlternative production.
	EnterQuotedIdentifierAlternative(c *QuotedIdentifierAlternativeContext)

	// EnterIdentifierLiteral is called when entering the identifierLiteral production.
	EnterIdentifierLiteral(c *IdentifierLiteralContext)

	// EnterSimpleUnquotedIdentifier is called when entering the simpleUnquotedIdentifier production.
	EnterSimpleUnquotedIdentifier(c *SimpleUnquotedIdentifierContext)

	// EnterSimpleQuotedIdentifierAlternative is called when entering the simpleQuotedIdentifierAlternative production.
	EnterSimpleQuotedIdentifierAlternative(c *SimpleQuotedIdentifierAlternativeContext)

	// EnterQuotedIdentifier is called when entering the quotedIdentifier production.
	EnterQuotedIdentifier(c *QuotedIdentifierContext)

	// EnterBackQuotedIdentifier is called when entering the backQuotedIdentifier production.
	EnterBackQuotedIdentifier(c *BackQuotedIdentifierContext)

	// EnterExponentLiteral is called when entering the exponentLiteral production.
	EnterExponentLiteral(c *ExponentLiteralContext)

	// EnterDecimalLiteral is called when entering the decimalLiteral production.
	EnterDecimalLiteral(c *DecimalLiteralContext)

	// EnterLegacyDecimalLiteral is called when entering the legacyDecimalLiteral production.
	EnterLegacyDecimalLiteral(c *LegacyDecimalLiteralContext)

	// EnterIntegerLiteral is called when entering the integerLiteral production.
	EnterIntegerLiteral(c *IntegerLiteralContext)

	// EnterBigIntLiteral is called when entering the bigIntLiteral production.
	EnterBigIntLiteral(c *BigIntLiteralContext)

	// EnterSmallIntLiteral is called when entering the smallIntLiteral production.
	EnterSmallIntLiteral(c *SmallIntLiteralContext)

	// EnterTinyIntLiteral is called when entering the tinyIntLiteral production.
	EnterTinyIntLiteral(c *TinyIntLiteralContext)

	// EnterDoubleLiteral is called when entering the doubleLiteral production.
	EnterDoubleLiteral(c *DoubleLiteralContext)

	// EnterFloatLiteral is called when entering the floatLiteral production.
	EnterFloatLiteral(c *FloatLiteralContext)

	// EnterBigDecimalLiteral is called when entering the bigDecimalLiteral production.
	EnterBigDecimalLiteral(c *BigDecimalLiteralContext)

	// EnterIntegerVal is called when entering the integerVal production.
	EnterIntegerVal(c *IntegerValContext)

	// EnterParameterIntegerValue is called when entering the parameterIntegerValue production.
	EnterParameterIntegerValue(c *ParameterIntegerValueContext)

	// EnterColumnConstraintDefinition is called when entering the columnConstraintDefinition production.
	EnterColumnConstraintDefinition(c *ColumnConstraintDefinitionContext)

	// EnterColumnConstraint is called when entering the columnConstraint production.
	EnterColumnConstraint(c *ColumnConstraintContext)

	// EnterTableConstraintDefinition is called when entering the tableConstraintDefinition production.
	EnterTableConstraintDefinition(c *TableConstraintDefinitionContext)

	// EnterTableConstraint is called when entering the tableConstraint production.
	EnterTableConstraint(c *TableConstraintContext)

	// EnterCheckConstraint is called when entering the checkConstraint production.
	EnterCheckConstraint(c *CheckConstraintContext)

	// EnterUniqueSpec is called when entering the uniqueSpec production.
	EnterUniqueSpec(c *UniqueSpecContext)

	// EnterUniqueConstraint is called when entering the uniqueConstraint production.
	EnterUniqueConstraint(c *UniqueConstraintContext)

	// EnterReferenceSpec is called when entering the referenceSpec production.
	EnterReferenceSpec(c *ReferenceSpecContext)

	// EnterForeignKeyConstraint is called when entering the foreignKeyConstraint production.
	EnterForeignKeyConstraint(c *ForeignKeyConstraintContext)

	// EnterConstraintCharacteristic is called when entering the constraintCharacteristic production.
	EnterConstraintCharacteristic(c *ConstraintCharacteristicContext)

	// EnterEnforcedCharacteristic is called when entering the enforcedCharacteristic production.
	EnterEnforcedCharacteristic(c *EnforcedCharacteristicContext)

	// EnterRelyCharacteristic is called when entering the relyCharacteristic production.
	EnterRelyCharacteristic(c *RelyCharacteristicContext)

	// EnterAlterColumnSpecList is called when entering the alterColumnSpecList production.
	EnterAlterColumnSpecList(c *AlterColumnSpecListContext)

	// EnterAlterColumnSpec is called when entering the alterColumnSpec production.
	EnterAlterColumnSpec(c *AlterColumnSpecContext)

	// EnterAlterColumnAction is called when entering the alterColumnAction production.
	EnterAlterColumnAction(c *AlterColumnActionContext)

	// EnterSingleStringLiteralValue is called when entering the singleStringLiteralValue production.
	EnterSingleStringLiteralValue(c *SingleStringLiteralValueContext)

	// EnterSingleDoubleQuotedStringLiteralValue is called when entering the singleDoubleQuotedStringLiteralValue production.
	EnterSingleDoubleQuotedStringLiteralValue(c *SingleDoubleQuotedStringLiteralValueContext)

	// EnterSingleStringLit is called when entering the singleStringLit production.
	EnterSingleStringLit(c *SingleStringLitContext)

	// EnterNamedParameterMarkerRule is called when entering the namedParameterMarkerRule production.
	EnterNamedParameterMarkerRule(c *NamedParameterMarkerRuleContext)

	// EnterPositionalParameterMarkerRule is called when entering the positionalParameterMarkerRule production.
	EnterPositionalParameterMarkerRule(c *PositionalParameterMarkerRuleContext)

	// EnterStringLit is called when entering the stringLit production.
	EnterStringLit(c *StringLitContext)

	// EnterComment is called when entering the comment production.
	EnterComment(c *CommentContext)

	// EnterVersion is called when entering the version production.
	EnterVersion(c *VersionContext)

	// EnterOperatorPipeRightSide is called when entering the operatorPipeRightSide production.
	EnterOperatorPipeRightSide(c *OperatorPipeRightSideContext)

	// EnterOperatorPipeSetAssignmentSeq is called when entering the operatorPipeSetAssignmentSeq production.
	EnterOperatorPipeSetAssignmentSeq(c *OperatorPipeSetAssignmentSeqContext)

	// EnterAnsiNonReserved is called when entering the ansiNonReserved production.
	EnterAnsiNonReserved(c *AnsiNonReservedContext)

	// EnterStrictNonReserved is called when entering the strictNonReserved production.
	EnterStrictNonReserved(c *StrictNonReservedContext)

	// EnterNonReserved is called when entering the nonReserved production.
	EnterNonReserved(c *NonReservedContext)

	// ExitCompoundOrSingleStatement is called when exiting the compoundOrSingleStatement production.
	ExitCompoundOrSingleStatement(c *CompoundOrSingleStatementContext)

	// ExitSingleCompoundStatement is called when exiting the singleCompoundStatement production.
	ExitSingleCompoundStatement(c *SingleCompoundStatementContext)

	// ExitBeginEndCompoundBlock is called when exiting the beginEndCompoundBlock production.
	ExitBeginEndCompoundBlock(c *BeginEndCompoundBlockContext)

	// ExitCompoundBody is called when exiting the compoundBody production.
	ExitCompoundBody(c *CompoundBodyContext)

	// ExitCompoundStatement is called when exiting the compoundStatement production.
	ExitCompoundStatement(c *CompoundStatementContext)

	// ExitSetVariableInsideSqlScript is called when exiting the setVariableInsideSqlScript production.
	ExitSetVariableInsideSqlScript(c *SetVariableInsideSqlScriptContext)

	// ExitSqlStateValue is called when exiting the sqlStateValue production.
	ExitSqlStateValue(c *SqlStateValueContext)

	// ExitDeclareConditionStatement is called when exiting the declareConditionStatement production.
	ExitDeclareConditionStatement(c *DeclareConditionStatementContext)

	// ExitConditionValue is called when exiting the conditionValue production.
	ExitConditionValue(c *ConditionValueContext)

	// ExitConditionValues is called when exiting the conditionValues production.
	ExitConditionValues(c *ConditionValuesContext)

	// ExitDeclareHandlerStatement is called when exiting the declareHandlerStatement production.
	ExitDeclareHandlerStatement(c *DeclareHandlerStatementContext)

	// ExitWhileStatement is called when exiting the whileStatement production.
	ExitWhileStatement(c *WhileStatementContext)

	// ExitIfElseStatement is called when exiting the ifElseStatement production.
	ExitIfElseStatement(c *IfElseStatementContext)

	// ExitRepeatStatement is called when exiting the repeatStatement production.
	ExitRepeatStatement(c *RepeatStatementContext)

	// ExitLeaveStatement is called when exiting the leaveStatement production.
	ExitLeaveStatement(c *LeaveStatementContext)

	// ExitIterateStatement is called when exiting the iterateStatement production.
	ExitIterateStatement(c *IterateStatementContext)

	// ExitSearchedCaseStatement is called when exiting the searchedCaseStatement production.
	ExitSearchedCaseStatement(c *SearchedCaseStatementContext)

	// ExitSimpleCaseStatement is called when exiting the simpleCaseStatement production.
	ExitSimpleCaseStatement(c *SimpleCaseStatementContext)

	// ExitLoopStatement is called when exiting the loopStatement production.
	ExitLoopStatement(c *LoopStatementContext)

	// ExitForStatement is called when exiting the forStatement production.
	ExitForStatement(c *ForStatementContext)

	// ExitSingleStatement is called when exiting the singleStatement production.
	ExitSingleStatement(c *SingleStatementContext)

	// ExitBeginLabel is called when exiting the beginLabel production.
	ExitBeginLabel(c *BeginLabelContext)

	// ExitEndLabel is called when exiting the endLabel production.
	ExitEndLabel(c *EndLabelContext)

	// ExitSingleExpression is called when exiting the singleExpression production.
	ExitSingleExpression(c *SingleExpressionContext)

	// ExitSingleTableIdentifier is called when exiting the singleTableIdentifier production.
	ExitSingleTableIdentifier(c *SingleTableIdentifierContext)

	// ExitSingleMultipartIdentifier is called when exiting the singleMultipartIdentifier production.
	ExitSingleMultipartIdentifier(c *SingleMultipartIdentifierContext)

	// ExitSingleFunctionIdentifier is called when exiting the singleFunctionIdentifier production.
	ExitSingleFunctionIdentifier(c *SingleFunctionIdentifierContext)

	// ExitSingleDataType is called when exiting the singleDataType production.
	ExitSingleDataType(c *SingleDataTypeContext)

	// ExitSingleTableSchema is called when exiting the singleTableSchema production.
	ExitSingleTableSchema(c *SingleTableSchemaContext)

	// ExitSingleRoutineParamList is called when exiting the singleRoutineParamList production.
	ExitSingleRoutineParamList(c *SingleRoutineParamListContext)

	// ExitStatementDefault is called when exiting the statementDefault production.
	ExitStatementDefault(c *StatementDefaultContext)

	// ExitVisitExecuteImmediate is called when exiting the visitExecuteImmediate production.
	ExitVisitExecuteImmediate(c *VisitExecuteImmediateContext)

	// ExitDmlStatement is called when exiting the dmlStatement production.
	ExitDmlStatement(c *DmlStatementContext)

	// ExitUse is called when exiting the use production.
	ExitUse(c *UseContext)

	// ExitUseNamespace is called when exiting the useNamespace production.
	ExitUseNamespace(c *UseNamespaceContext)

	// ExitSetCatalog is called when exiting the setCatalog production.
	ExitSetCatalog(c *SetCatalogContext)

	// ExitCreateNamespace is called when exiting the createNamespace production.
	ExitCreateNamespace(c *CreateNamespaceContext)

	// ExitSetNamespaceProperties is called when exiting the setNamespaceProperties production.
	ExitSetNamespaceProperties(c *SetNamespacePropertiesContext)

	// ExitUnsetNamespaceProperties is called when exiting the unsetNamespaceProperties production.
	ExitUnsetNamespaceProperties(c *UnsetNamespacePropertiesContext)

	// ExitSetNamespaceCollation is called when exiting the setNamespaceCollation production.
	ExitSetNamespaceCollation(c *SetNamespaceCollationContext)

	// ExitSetNamespaceLocation is called when exiting the setNamespaceLocation production.
	ExitSetNamespaceLocation(c *SetNamespaceLocationContext)

	// ExitDropNamespace is called when exiting the dropNamespace production.
	ExitDropNamespace(c *DropNamespaceContext)

	// ExitShowNamespaces is called when exiting the showNamespaces production.
	ExitShowNamespaces(c *ShowNamespacesContext)

	// ExitCreateTable is called when exiting the createTable production.
	ExitCreateTable(c *CreateTableContext)

	// ExitCreateTableLike is called when exiting the createTableLike production.
	ExitCreateTableLike(c *CreateTableLikeContext)

	// ExitReplaceTable is called when exiting the replaceTable production.
	ExitReplaceTable(c *ReplaceTableContext)

	// ExitAnalyze is called when exiting the analyze production.
	ExitAnalyze(c *AnalyzeContext)

	// ExitAnalyzeTables is called when exiting the analyzeTables production.
	ExitAnalyzeTables(c *AnalyzeTablesContext)

	// ExitAddTableColumns is called when exiting the addTableColumns production.
	ExitAddTableColumns(c *AddTableColumnsContext)

	// ExitRenameTableColumn is called when exiting the renameTableColumn production.
	ExitRenameTableColumn(c *RenameTableColumnContext)

	// ExitDropTableColumns is called when exiting the dropTableColumns production.
	ExitDropTableColumns(c *DropTableColumnsContext)

	// ExitRenameTable is called when exiting the renameTable production.
	ExitRenameTable(c *RenameTableContext)

	// ExitSetTableProperties is called when exiting the setTableProperties production.
	ExitSetTableProperties(c *SetTablePropertiesContext)

	// ExitUnsetTableProperties is called when exiting the unsetTableProperties production.
	ExitUnsetTableProperties(c *UnsetTablePropertiesContext)

	// ExitAlterTableAlterColumn is called when exiting the alterTableAlterColumn production.
	ExitAlterTableAlterColumn(c *AlterTableAlterColumnContext)

	// ExitHiveChangeColumn is called when exiting the hiveChangeColumn production.
	ExitHiveChangeColumn(c *HiveChangeColumnContext)

	// ExitHiveReplaceColumns is called when exiting the hiveReplaceColumns production.
	ExitHiveReplaceColumns(c *HiveReplaceColumnsContext)

	// ExitSetTableSerDe is called when exiting the setTableSerDe production.
	ExitSetTableSerDe(c *SetTableSerDeContext)

	// ExitAddTablePartition is called when exiting the addTablePartition production.
	ExitAddTablePartition(c *AddTablePartitionContext)

	// ExitRenameTablePartition is called when exiting the renameTablePartition production.
	ExitRenameTablePartition(c *RenameTablePartitionContext)

	// ExitDropTablePartitions is called when exiting the dropTablePartitions production.
	ExitDropTablePartitions(c *DropTablePartitionsContext)

	// ExitSetTableLocation is called when exiting the setTableLocation production.
	ExitSetTableLocation(c *SetTableLocationContext)

	// ExitRecoverPartitions is called when exiting the recoverPartitions production.
	ExitRecoverPartitions(c *RecoverPartitionsContext)

	// ExitAlterClusterBy is called when exiting the alterClusterBy production.
	ExitAlterClusterBy(c *AlterClusterByContext)

	// ExitAlterTableCollation is called when exiting the alterTableCollation production.
	ExitAlterTableCollation(c *AlterTableCollationContext)

	// ExitAddTableConstraint is called when exiting the addTableConstraint production.
	ExitAddTableConstraint(c *AddTableConstraintContext)

	// ExitDropTableConstraint is called when exiting the dropTableConstraint production.
	ExitDropTableConstraint(c *DropTableConstraintContext)

	// ExitDropTable is called when exiting the dropTable production.
	ExitDropTable(c *DropTableContext)

	// ExitDropView is called when exiting the dropView production.
	ExitDropView(c *DropViewContext)

	// ExitCreateView is called when exiting the createView production.
	ExitCreateView(c *CreateViewContext)

	// ExitCreateTempViewUsing is called when exiting the createTempViewUsing production.
	ExitCreateTempViewUsing(c *CreateTempViewUsingContext)

	// ExitAlterViewQuery is called when exiting the alterViewQuery production.
	ExitAlterViewQuery(c *AlterViewQueryContext)

	// ExitAlterViewSchemaBinding is called when exiting the alterViewSchemaBinding production.
	ExitAlterViewSchemaBinding(c *AlterViewSchemaBindingContext)

	// ExitCreateFunction is called when exiting the createFunction production.
	ExitCreateFunction(c *CreateFunctionContext)

	// ExitCreateUserDefinedFunction is called when exiting the createUserDefinedFunction production.
	ExitCreateUserDefinedFunction(c *CreateUserDefinedFunctionContext)

	// ExitDropFunction is called when exiting the dropFunction production.
	ExitDropFunction(c *DropFunctionContext)

	// ExitCreateVariable is called when exiting the createVariable production.
	ExitCreateVariable(c *CreateVariableContext)

	// ExitDropVariable is called when exiting the dropVariable production.
	ExitDropVariable(c *DropVariableContext)

	// ExitExplain is called when exiting the explain production.
	ExitExplain(c *ExplainContext)

	// ExitShowTables is called when exiting the showTables production.
	ExitShowTables(c *ShowTablesContext)

	// ExitShowTableExtended is called when exiting the showTableExtended production.
	ExitShowTableExtended(c *ShowTableExtendedContext)

	// ExitShowTblProperties is called when exiting the showTblProperties production.
	ExitShowTblProperties(c *ShowTblPropertiesContext)

	// ExitShowColumns is called when exiting the showColumns production.
	ExitShowColumns(c *ShowColumnsContext)

	// ExitShowViews is called when exiting the showViews production.
	ExitShowViews(c *ShowViewsContext)

	// ExitShowPartitions is called when exiting the showPartitions production.
	ExitShowPartitions(c *ShowPartitionsContext)

	// ExitShowFunctions is called when exiting the showFunctions production.
	ExitShowFunctions(c *ShowFunctionsContext)

	// ExitShowProcedures is called when exiting the showProcedures production.
	ExitShowProcedures(c *ShowProceduresContext)

	// ExitShowCreateTable is called when exiting the showCreateTable production.
	ExitShowCreateTable(c *ShowCreateTableContext)

	// ExitShowCurrentNamespace is called when exiting the showCurrentNamespace production.
	ExitShowCurrentNamespace(c *ShowCurrentNamespaceContext)

	// ExitShowCatalogs is called when exiting the showCatalogs production.
	ExitShowCatalogs(c *ShowCatalogsContext)

	// ExitDescribeFunction is called when exiting the describeFunction production.
	ExitDescribeFunction(c *DescribeFunctionContext)

	// ExitDescribeProcedure is called when exiting the describeProcedure production.
	ExitDescribeProcedure(c *DescribeProcedureContext)

	// ExitDescribeNamespace is called when exiting the describeNamespace production.
	ExitDescribeNamespace(c *DescribeNamespaceContext)

	// ExitDescribeRelation is called when exiting the describeRelation production.
	ExitDescribeRelation(c *DescribeRelationContext)

	// ExitDescribeQuery is called when exiting the describeQuery production.
	ExitDescribeQuery(c *DescribeQueryContext)

	// ExitCommentNamespace is called when exiting the commentNamespace production.
	ExitCommentNamespace(c *CommentNamespaceContext)

	// ExitCommentTable is called when exiting the commentTable production.
	ExitCommentTable(c *CommentTableContext)

	// ExitRefreshTable is called when exiting the refreshTable production.
	ExitRefreshTable(c *RefreshTableContext)

	// ExitRefreshFunction is called when exiting the refreshFunction production.
	ExitRefreshFunction(c *RefreshFunctionContext)

	// ExitRefreshResource is called when exiting the refreshResource production.
	ExitRefreshResource(c *RefreshResourceContext)

	// ExitCacheTable is called when exiting the cacheTable production.
	ExitCacheTable(c *CacheTableContext)

	// ExitUncacheTable is called when exiting the uncacheTable production.
	ExitUncacheTable(c *UncacheTableContext)

	// ExitClearCache is called when exiting the clearCache production.
	ExitClearCache(c *ClearCacheContext)

	// ExitLoadData is called when exiting the loadData production.
	ExitLoadData(c *LoadDataContext)

	// ExitTruncateTable is called when exiting the truncateTable production.
	ExitTruncateTable(c *TruncateTableContext)

	// ExitRepairTable is called when exiting the repairTable production.
	ExitRepairTable(c *RepairTableContext)

	// ExitManageResource is called when exiting the manageResource production.
	ExitManageResource(c *ManageResourceContext)

	// ExitCreateIndex is called when exiting the createIndex production.
	ExitCreateIndex(c *CreateIndexContext)

	// ExitDropIndex is called when exiting the dropIndex production.
	ExitDropIndex(c *DropIndexContext)

	// ExitCall is called when exiting the call production.
	ExitCall(c *CallContext)

	// ExitFailNativeCommand is called when exiting the failNativeCommand production.
	ExitFailNativeCommand(c *FailNativeCommandContext)

	// ExitCreatePipelineDataset is called when exiting the createPipelineDataset production.
	ExitCreatePipelineDataset(c *CreatePipelineDatasetContext)

	// ExitCreatePipelineInsertIntoFlow is called when exiting the createPipelineInsertIntoFlow production.
	ExitCreatePipelineInsertIntoFlow(c *CreatePipelineInsertIntoFlowContext)

	// ExitMaterializedView is called when exiting the materializedView production.
	ExitMaterializedView(c *MaterializedViewContext)

	// ExitStreamingTable is called when exiting the streamingTable production.
	ExitStreamingTable(c *StreamingTableContext)

	// ExitCreatePipelineDatasetHeader is called when exiting the createPipelineDatasetHeader production.
	ExitCreatePipelineDatasetHeader(c *CreatePipelineDatasetHeaderContext)

	// ExitStreamTableName is called when exiting the streamTableName production.
	ExitStreamTableName(c *StreamTableNameContext)

	// ExitFailSetRole is called when exiting the failSetRole production.
	ExitFailSetRole(c *FailSetRoleContext)

	// ExitSetTimeZone is called when exiting the setTimeZone production.
	ExitSetTimeZone(c *SetTimeZoneContext)

	// ExitSetVariable is called when exiting the setVariable production.
	ExitSetVariable(c *SetVariableContext)

	// ExitSetQuotedConfiguration is called when exiting the setQuotedConfiguration production.
	ExitSetQuotedConfiguration(c *SetQuotedConfigurationContext)

	// ExitSetConfiguration is called when exiting the setConfiguration production.
	ExitSetConfiguration(c *SetConfigurationContext)

	// ExitResetQuotedConfiguration is called when exiting the resetQuotedConfiguration production.
	ExitResetQuotedConfiguration(c *ResetQuotedConfigurationContext)

	// ExitResetConfiguration is called when exiting the resetConfiguration production.
	ExitResetConfiguration(c *ResetConfigurationContext)

	// ExitExecuteImmediate is called when exiting the executeImmediate production.
	ExitExecuteImmediate(c *ExecuteImmediateContext)

	// ExitExecuteImmediateUsing is called when exiting the executeImmediateUsing production.
	ExitExecuteImmediateUsing(c *ExecuteImmediateUsingContext)

	// ExitTimezone is called when exiting the timezone production.
	ExitTimezone(c *TimezoneContext)

	// ExitConfigKey is called when exiting the configKey production.
	ExitConfigKey(c *ConfigKeyContext)

	// ExitConfigValue is called when exiting the configValue production.
	ExitConfigValue(c *ConfigValueContext)

	// ExitUnsupportedHiveNativeCommands is called when exiting the unsupportedHiveNativeCommands production.
	ExitUnsupportedHiveNativeCommands(c *UnsupportedHiveNativeCommandsContext)

	// ExitCreateTableHeader is called when exiting the createTableHeader production.
	ExitCreateTableHeader(c *CreateTableHeaderContext)

	// ExitReplaceTableHeader is called when exiting the replaceTableHeader production.
	ExitReplaceTableHeader(c *ReplaceTableHeaderContext)

	// ExitClusterBySpec is called when exiting the clusterBySpec production.
	ExitClusterBySpec(c *ClusterBySpecContext)

	// ExitBucketSpec is called when exiting the bucketSpec production.
	ExitBucketSpec(c *BucketSpecContext)

	// ExitSkewSpec is called when exiting the skewSpec production.
	ExitSkewSpec(c *SkewSpecContext)

	// ExitLocationSpec is called when exiting the locationSpec production.
	ExitLocationSpec(c *LocationSpecContext)

	// ExitSchemaBinding is called when exiting the schemaBinding production.
	ExitSchemaBinding(c *SchemaBindingContext)

	// ExitCommentSpec is called when exiting the commentSpec production.
	ExitCommentSpec(c *CommentSpecContext)

	// ExitSingleQuery is called when exiting the singleQuery production.
	ExitSingleQuery(c *SingleQueryContext)

	// ExitQuery is called when exiting the query production.
	ExitQuery(c *QueryContext)

	// ExitInsertOverwriteTable is called when exiting the insertOverwriteTable production.
	ExitInsertOverwriteTable(c *InsertOverwriteTableContext)

	// ExitInsertIntoTable is called when exiting the insertIntoTable production.
	ExitInsertIntoTable(c *InsertIntoTableContext)

	// ExitInsertIntoReplaceWhere is called when exiting the insertIntoReplaceWhere production.
	ExitInsertIntoReplaceWhere(c *InsertIntoReplaceWhereContext)

	// ExitInsertOverwriteHiveDir is called when exiting the insertOverwriteHiveDir production.
	ExitInsertOverwriteHiveDir(c *InsertOverwriteHiveDirContext)

	// ExitInsertOverwriteDir is called when exiting the insertOverwriteDir production.
	ExitInsertOverwriteDir(c *InsertOverwriteDirContext)

	// ExitPartitionSpecLocation is called when exiting the partitionSpecLocation production.
	ExitPartitionSpecLocation(c *PartitionSpecLocationContext)

	// ExitPartitionSpec is called when exiting the partitionSpec production.
	ExitPartitionSpec(c *PartitionSpecContext)

	// ExitPartitionVal is called when exiting the partitionVal production.
	ExitPartitionVal(c *PartitionValContext)

	// ExitCreatePipelineFlowHeader is called when exiting the createPipelineFlowHeader production.
	ExitCreatePipelineFlowHeader(c *CreatePipelineFlowHeaderContext)

	// ExitNamespace is called when exiting the namespace production.
	ExitNamespace(c *NamespaceContext)

	// ExitNamespaces is called when exiting the namespaces production.
	ExitNamespaces(c *NamespacesContext)

	// ExitVariable is called when exiting the variable production.
	ExitVariable(c *VariableContext)

	// ExitDescribeFuncName is called when exiting the describeFuncName production.
	ExitDescribeFuncName(c *DescribeFuncNameContext)

	// ExitDescribeColName is called when exiting the describeColName production.
	ExitDescribeColName(c *DescribeColNameContext)

	// ExitCtes is called when exiting the ctes production.
	ExitCtes(c *CtesContext)

	// ExitNamedQuery is called when exiting the namedQuery production.
	ExitNamedQuery(c *NamedQueryContext)

	// ExitTableProvider is called when exiting the tableProvider production.
	ExitTableProvider(c *TableProviderContext)

	// ExitCreateTableClauses is called when exiting the createTableClauses production.
	ExitCreateTableClauses(c *CreateTableClausesContext)

	// ExitPropertyList is called when exiting the propertyList production.
	ExitPropertyList(c *PropertyListContext)

	// ExitPropertyWithKeyAndEquals is called when exiting the propertyWithKeyAndEquals production.
	ExitPropertyWithKeyAndEquals(c *PropertyWithKeyAndEqualsContext)

	// ExitPropertyWithKeyNoEquals is called when exiting the propertyWithKeyNoEquals production.
	ExitPropertyWithKeyNoEquals(c *PropertyWithKeyNoEqualsContext)

	// ExitPropertyKey is called when exiting the propertyKey production.
	ExitPropertyKey(c *PropertyKeyContext)

	// ExitPropertyKeyOrStringLit is called when exiting the propertyKeyOrStringLit production.
	ExitPropertyKeyOrStringLit(c *PropertyKeyOrStringLitContext)

	// ExitPropertyKeyOrStringLitNoCoalesce is called when exiting the propertyKeyOrStringLitNoCoalesce production.
	ExitPropertyKeyOrStringLitNoCoalesce(c *PropertyKeyOrStringLitNoCoalesceContext)

	// ExitPropertyValue is called when exiting the propertyValue production.
	ExitPropertyValue(c *PropertyValueContext)

	// ExitExpressionPropertyList is called when exiting the expressionPropertyList production.
	ExitExpressionPropertyList(c *ExpressionPropertyListContext)

	// ExitExpressionPropertyWithKeyAndEquals is called when exiting the expressionPropertyWithKeyAndEquals production.
	ExitExpressionPropertyWithKeyAndEquals(c *ExpressionPropertyWithKeyAndEqualsContext)

	// ExitExpressionPropertyWithKeyNoEquals is called when exiting the expressionPropertyWithKeyNoEquals production.
	ExitExpressionPropertyWithKeyNoEquals(c *ExpressionPropertyWithKeyNoEqualsContext)

	// ExitConstantList is called when exiting the constantList production.
	ExitConstantList(c *ConstantListContext)

	// ExitNestedConstantList is called when exiting the nestedConstantList production.
	ExitNestedConstantList(c *NestedConstantListContext)

	// ExitCreateFileFormat is called when exiting the createFileFormat production.
	ExitCreateFileFormat(c *CreateFileFormatContext)

	// ExitTableFileFormat is called when exiting the tableFileFormat production.
	ExitTableFileFormat(c *TableFileFormatContext)

	// ExitGenericFileFormat is called when exiting the genericFileFormat production.
	ExitGenericFileFormat(c *GenericFileFormatContext)

	// ExitStorageHandler is called when exiting the storageHandler production.
	ExitStorageHandler(c *StorageHandlerContext)

	// ExitResource is called when exiting the resource production.
	ExitResource(c *ResourceContext)

	// ExitSingleInsertQuery is called when exiting the singleInsertQuery production.
	ExitSingleInsertQuery(c *SingleInsertQueryContext)

	// ExitMultiInsertQuery is called when exiting the multiInsertQuery production.
	ExitMultiInsertQuery(c *MultiInsertQueryContext)

	// ExitDeleteFromTable is called when exiting the deleteFromTable production.
	ExitDeleteFromTable(c *DeleteFromTableContext)

	// ExitUpdateTable is called when exiting the updateTable production.
	ExitUpdateTable(c *UpdateTableContext)

	// ExitMergeIntoTable is called when exiting the mergeIntoTable production.
	ExitMergeIntoTable(c *MergeIntoTableContext)

	// ExitIdentifierReference is called when exiting the identifierReference production.
	ExitIdentifierReference(c *IdentifierReferenceContext)

	// ExitCatalogIdentifierReference is called when exiting the catalogIdentifierReference production.
	ExitCatalogIdentifierReference(c *CatalogIdentifierReferenceContext)

	// ExitQueryOrganization is called when exiting the queryOrganization production.
	ExitQueryOrganization(c *QueryOrganizationContext)

	// ExitMultiInsertQueryBody is called when exiting the multiInsertQueryBody production.
	ExitMultiInsertQueryBody(c *MultiInsertQueryBodyContext)

	// ExitOperatorPipeStatement is called when exiting the operatorPipeStatement production.
	ExitOperatorPipeStatement(c *OperatorPipeStatementContext)

	// ExitQueryTermDefault is called when exiting the queryTermDefault production.
	ExitQueryTermDefault(c *QueryTermDefaultContext)

	// ExitSetOperation is called when exiting the setOperation production.
	ExitSetOperation(c *SetOperationContext)

	// ExitQueryPrimaryDefault is called when exiting the queryPrimaryDefault production.
	ExitQueryPrimaryDefault(c *QueryPrimaryDefaultContext)

	// ExitFromStmt is called when exiting the fromStmt production.
	ExitFromStmt(c *FromStmtContext)

	// ExitTable is called when exiting the table production.
	ExitTable(c *TableContext)

	// ExitInlineTableDefault1 is called when exiting the inlineTableDefault1 production.
	ExitInlineTableDefault1(c *InlineTableDefault1Context)

	// ExitSubquery is called when exiting the subquery production.
	ExitSubquery(c *SubqueryContext)

	// ExitSortItem is called when exiting the sortItem production.
	ExitSortItem(c *SortItemContext)

	// ExitFromStatement is called when exiting the fromStatement production.
	ExitFromStatement(c *FromStatementContext)

	// ExitFromStatementBody is called when exiting the fromStatementBody production.
	ExitFromStatementBody(c *FromStatementBodyContext)

	// ExitTransformQuerySpecification is called when exiting the transformQuerySpecification production.
	ExitTransformQuerySpecification(c *TransformQuerySpecificationContext)

	// ExitRegularQuerySpecification is called when exiting the regularQuerySpecification production.
	ExitRegularQuerySpecification(c *RegularQuerySpecificationContext)

	// ExitTransformClause is called when exiting the transformClause production.
	ExitTransformClause(c *TransformClauseContext)

	// ExitSelectClause is called when exiting the selectClause production.
	ExitSelectClause(c *SelectClauseContext)

	// ExitSetClause is called when exiting the setClause production.
	ExitSetClause(c *SetClauseContext)

	// ExitMatchedClause is called when exiting the matchedClause production.
	ExitMatchedClause(c *MatchedClauseContext)

	// ExitNotMatchedClause is called when exiting the notMatchedClause production.
	ExitNotMatchedClause(c *NotMatchedClauseContext)

	// ExitNotMatchedBySourceClause is called when exiting the notMatchedBySourceClause production.
	ExitNotMatchedBySourceClause(c *NotMatchedBySourceClauseContext)

	// ExitMatchedAction is called when exiting the matchedAction production.
	ExitMatchedAction(c *MatchedActionContext)

	// ExitNotMatchedAction is called when exiting the notMatchedAction production.
	ExitNotMatchedAction(c *NotMatchedActionContext)

	// ExitNotMatchedBySourceAction is called when exiting the notMatchedBySourceAction production.
	ExitNotMatchedBySourceAction(c *NotMatchedBySourceActionContext)

	// ExitExceptClause is called when exiting the exceptClause production.
	ExitExceptClause(c *ExceptClauseContext)

	// ExitAssignmentList is called when exiting the assignmentList production.
	ExitAssignmentList(c *AssignmentListContext)

	// ExitAssignment is called when exiting the assignment production.
	ExitAssignment(c *AssignmentContext)

	// ExitWhereClause is called when exiting the whereClause production.
	ExitWhereClause(c *WhereClauseContext)

	// ExitHavingClause is called when exiting the havingClause production.
	ExitHavingClause(c *HavingClauseContext)

	// ExitHint is called when exiting the hint production.
	ExitHint(c *HintContext)

	// ExitHintStatement is called when exiting the hintStatement production.
	ExitHintStatement(c *HintStatementContext)

	// ExitFromClause is called when exiting the fromClause production.
	ExitFromClause(c *FromClauseContext)

	// ExitTemporalClause is called when exiting the temporalClause production.
	ExitTemporalClause(c *TemporalClauseContext)

	// ExitAggregationClause is called when exiting the aggregationClause production.
	ExitAggregationClause(c *AggregationClauseContext)

	// ExitGroupByClause is called when exiting the groupByClause production.
	ExitGroupByClause(c *GroupByClauseContext)

	// ExitGroupingAnalytics is called when exiting the groupingAnalytics production.
	ExitGroupingAnalytics(c *GroupingAnalyticsContext)

	// ExitGroupingElement is called when exiting the groupingElement production.
	ExitGroupingElement(c *GroupingElementContext)

	// ExitGroupingSet is called when exiting the groupingSet production.
	ExitGroupingSet(c *GroupingSetContext)

	// ExitPivotClause is called when exiting the pivotClause production.
	ExitPivotClause(c *PivotClauseContext)

	// ExitPivotColumn is called when exiting the pivotColumn production.
	ExitPivotColumn(c *PivotColumnContext)

	// ExitPivotValue is called when exiting the pivotValue production.
	ExitPivotValue(c *PivotValueContext)

	// ExitUnpivotClause is called when exiting the unpivotClause production.
	ExitUnpivotClause(c *UnpivotClauseContext)

	// ExitUnpivotNullClause is called when exiting the unpivotNullClause production.
	ExitUnpivotNullClause(c *UnpivotNullClauseContext)

	// ExitUnpivotOperator is called when exiting the unpivotOperator production.
	ExitUnpivotOperator(c *UnpivotOperatorContext)

	// ExitUnpivotSingleValueColumnClause is called when exiting the unpivotSingleValueColumnClause production.
	ExitUnpivotSingleValueColumnClause(c *UnpivotSingleValueColumnClauseContext)

	// ExitUnpivotMultiValueColumnClause is called when exiting the unpivotMultiValueColumnClause production.
	ExitUnpivotMultiValueColumnClause(c *UnpivotMultiValueColumnClauseContext)

	// ExitUnpivotColumnSet is called when exiting the unpivotColumnSet production.
	ExitUnpivotColumnSet(c *UnpivotColumnSetContext)

	// ExitUnpivotValueColumn is called when exiting the unpivotValueColumn production.
	ExitUnpivotValueColumn(c *UnpivotValueColumnContext)

	// ExitUnpivotNameColumn is called when exiting the unpivotNameColumn production.
	ExitUnpivotNameColumn(c *UnpivotNameColumnContext)

	// ExitUnpivotColumnAndAlias is called when exiting the unpivotColumnAndAlias production.
	ExitUnpivotColumnAndAlias(c *UnpivotColumnAndAliasContext)

	// ExitUnpivotColumn is called when exiting the unpivotColumn production.
	ExitUnpivotColumn(c *UnpivotColumnContext)

	// ExitUnpivotAlias is called when exiting the unpivotAlias production.
	ExitUnpivotAlias(c *UnpivotAliasContext)

	// ExitLateralView is called when exiting the lateralView production.
	ExitLateralView(c *LateralViewContext)

	// ExitWatermarkClause is called when exiting the watermarkClause production.
	ExitWatermarkClause(c *WatermarkClauseContext)

	// ExitSetQuantifier is called when exiting the setQuantifier production.
	ExitSetQuantifier(c *SetQuantifierContext)

	// ExitRelation is called when exiting the relation production.
	ExitRelation(c *RelationContext)

	// ExitRelationExtension is called when exiting the relationExtension production.
	ExitRelationExtension(c *RelationExtensionContext)

	// ExitJoinRelation is called when exiting the joinRelation production.
	ExitJoinRelation(c *JoinRelationContext)

	// ExitJoinType is called when exiting the joinType production.
	ExitJoinType(c *JoinTypeContext)

	// ExitJoinCriteria is called when exiting the joinCriteria production.
	ExitJoinCriteria(c *JoinCriteriaContext)

	// ExitSample is called when exiting the sample production.
	ExitSample(c *SampleContext)

	// ExitSampleByPercentile is called when exiting the sampleByPercentile production.
	ExitSampleByPercentile(c *SampleByPercentileContext)

	// ExitSampleByRows is called when exiting the sampleByRows production.
	ExitSampleByRows(c *SampleByRowsContext)

	// ExitSampleByBucket is called when exiting the sampleByBucket production.
	ExitSampleByBucket(c *SampleByBucketContext)

	// ExitSampleByBytes is called when exiting the sampleByBytes production.
	ExitSampleByBytes(c *SampleByBytesContext)

	// ExitIdentifierList is called when exiting the identifierList production.
	ExitIdentifierList(c *IdentifierListContext)

	// ExitIdentifierSeq is called when exiting the identifierSeq production.
	ExitIdentifierSeq(c *IdentifierSeqContext)

	// ExitOrderedIdentifierList is called when exiting the orderedIdentifierList production.
	ExitOrderedIdentifierList(c *OrderedIdentifierListContext)

	// ExitOrderedIdentifier is called when exiting the orderedIdentifier production.
	ExitOrderedIdentifier(c *OrderedIdentifierContext)

	// ExitIdentifierCommentList is called when exiting the identifierCommentList production.
	ExitIdentifierCommentList(c *IdentifierCommentListContext)

	// ExitIdentifierComment is called when exiting the identifierComment production.
	ExitIdentifierComment(c *IdentifierCommentContext)

	// ExitStreamRelation is called when exiting the streamRelation production.
	ExitStreamRelation(c *StreamRelationContext)

	// ExitTableName is called when exiting the tableName production.
	ExitTableName(c *TableNameContext)

	// ExitAliasedQuery is called when exiting the aliasedQuery production.
	ExitAliasedQuery(c *AliasedQueryContext)

	// ExitAliasedRelation is called when exiting the aliasedRelation production.
	ExitAliasedRelation(c *AliasedRelationContext)

	// ExitInlineTableDefault2 is called when exiting the inlineTableDefault2 production.
	ExitInlineTableDefault2(c *InlineTableDefault2Context)

	// ExitTableValuedFunction is called when exiting the tableValuedFunction production.
	ExitTableValuedFunction(c *TableValuedFunctionContext)

	// ExitOptionsClause is called when exiting the optionsClause production.
	ExitOptionsClause(c *OptionsClauseContext)

	// ExitInlineTable is called when exiting the inlineTable production.
	ExitInlineTable(c *InlineTableContext)

	// ExitFunctionTableSubqueryArgument is called when exiting the functionTableSubqueryArgument production.
	ExitFunctionTableSubqueryArgument(c *FunctionTableSubqueryArgumentContext)

	// ExitTableArgumentPartitioning is called when exiting the tableArgumentPartitioning production.
	ExitTableArgumentPartitioning(c *TableArgumentPartitioningContext)

	// ExitFunctionTableNamedArgumentExpression is called when exiting the functionTableNamedArgumentExpression production.
	ExitFunctionTableNamedArgumentExpression(c *FunctionTableNamedArgumentExpressionContext)

	// ExitFunctionTableReferenceArgument is called when exiting the functionTableReferenceArgument production.
	ExitFunctionTableReferenceArgument(c *FunctionTableReferenceArgumentContext)

	// ExitFunctionTableArgument is called when exiting the functionTableArgument production.
	ExitFunctionTableArgument(c *FunctionTableArgumentContext)

	// ExitFunctionTable is called when exiting the functionTable production.
	ExitFunctionTable(c *FunctionTableContext)

	// ExitTableAlias is called when exiting the tableAlias production.
	ExitTableAlias(c *TableAliasContext)

	// ExitRowFormatSerde is called when exiting the rowFormatSerde production.
	ExitRowFormatSerde(c *RowFormatSerdeContext)

	// ExitRowFormatDelimited is called when exiting the rowFormatDelimited production.
	ExitRowFormatDelimited(c *RowFormatDelimitedContext)

	// ExitMultipartIdentifierList is called when exiting the multipartIdentifierList production.
	ExitMultipartIdentifierList(c *MultipartIdentifierListContext)

	// ExitMultipartIdentifier is called when exiting the multipartIdentifier production.
	ExitMultipartIdentifier(c *MultipartIdentifierContext)

	// ExitMultipartIdentifierPropertyList is called when exiting the multipartIdentifierPropertyList production.
	ExitMultipartIdentifierPropertyList(c *MultipartIdentifierPropertyListContext)

	// ExitMultipartIdentifierProperty is called when exiting the multipartIdentifierProperty production.
	ExitMultipartIdentifierProperty(c *MultipartIdentifierPropertyContext)

	// ExitTableIdentifier is called when exiting the tableIdentifier production.
	ExitTableIdentifier(c *TableIdentifierContext)

	// ExitFunctionIdentifier is called when exiting the functionIdentifier production.
	ExitFunctionIdentifier(c *FunctionIdentifierContext)

	// ExitNamedExpression is called when exiting the namedExpression production.
	ExitNamedExpression(c *NamedExpressionContext)

	// ExitNamedExpressionSeq is called when exiting the namedExpressionSeq production.
	ExitNamedExpressionSeq(c *NamedExpressionSeqContext)

	// ExitPartitionFieldList is called when exiting the partitionFieldList production.
	ExitPartitionFieldList(c *PartitionFieldListContext)

	// ExitPartitionTransform is called when exiting the partitionTransform production.
	ExitPartitionTransform(c *PartitionTransformContext)

	// ExitPartitionColumn is called when exiting the partitionColumn production.
	ExitPartitionColumn(c *PartitionColumnContext)

	// ExitIdentityTransform is called when exiting the identityTransform production.
	ExitIdentityTransform(c *IdentityTransformContext)

	// ExitApplyTransform is called when exiting the applyTransform production.
	ExitApplyTransform(c *ApplyTransformContext)

	// ExitTransformArgument is called when exiting the transformArgument production.
	ExitTransformArgument(c *TransformArgumentContext)

	// ExitExpression is called when exiting the expression production.
	ExitExpression(c *ExpressionContext)

	// ExitNamedArgumentExpression is called when exiting the namedArgumentExpression production.
	ExitNamedArgumentExpression(c *NamedArgumentExpressionContext)

	// ExitFunctionArgument is called when exiting the functionArgument production.
	ExitFunctionArgument(c *FunctionArgumentContext)

	// ExitExpressionSeq is called when exiting the expressionSeq production.
	ExitExpressionSeq(c *ExpressionSeqContext)

	// ExitLogicalNot is called when exiting the logicalNot production.
	ExitLogicalNot(c *LogicalNotContext)

	// ExitPredicated is called when exiting the predicated production.
	ExitPredicated(c *PredicatedContext)

	// ExitExists is called when exiting the exists production.
	ExitExists(c *ExistsContext)

	// ExitLogicalBinary is called when exiting the logicalBinary production.
	ExitLogicalBinary(c *LogicalBinaryContext)

	// ExitPredicate is called when exiting the predicate production.
	ExitPredicate(c *PredicateContext)

	// ExitErrorCapturingNot is called when exiting the errorCapturingNot production.
	ExitErrorCapturingNot(c *ErrorCapturingNotContext)

	// ExitValueExpressionDefault is called when exiting the valueExpressionDefault production.
	ExitValueExpressionDefault(c *ValueExpressionDefaultContext)

	// ExitComparison is called when exiting the comparison production.
	ExitComparison(c *ComparisonContext)

	// ExitShiftExpression is called when exiting the shiftExpression production.
	ExitShiftExpression(c *ShiftExpressionContext)

	// ExitArithmeticBinary is called when exiting the arithmeticBinary production.
	ExitArithmeticBinary(c *ArithmeticBinaryContext)

	// ExitArithmeticUnary is called when exiting the arithmeticUnary production.
	ExitArithmeticUnary(c *ArithmeticUnaryContext)

	// ExitShiftOperator is called when exiting the shiftOperator production.
	ExitShiftOperator(c *ShiftOperatorContext)

	// ExitDatetimeUnit is called when exiting the datetimeUnit production.
	ExitDatetimeUnit(c *DatetimeUnitContext)

	// ExitStruct is called when exiting the struct production.
	ExitStruct(c *StructContext)

	// ExitDereference is called when exiting the dereference production.
	ExitDereference(c *DereferenceContext)

	// ExitCastByColon is called when exiting the castByColon production.
	ExitCastByColon(c *CastByColonContext)

	// ExitTimestampadd is called when exiting the timestampadd production.
	ExitTimestampadd(c *TimestampaddContext)

	// ExitSubstring is called when exiting the substring production.
	ExitSubstring(c *SubstringContext)

	// ExitCast is called when exiting the cast production.
	ExitCast(c *CastContext)

	// ExitLambda is called when exiting the lambda production.
	ExitLambda(c *LambdaContext)

	// ExitParenthesizedExpression is called when exiting the parenthesizedExpression production.
	ExitParenthesizedExpression(c *ParenthesizedExpressionContext)

	// ExitAny_value is called when exiting the any_value production.
	ExitAny_value(c *Any_valueContext)

	// ExitTrim is called when exiting the trim production.
	ExitTrim(c *TrimContext)

	// ExitSemiStructuredExtract is called when exiting the semiStructuredExtract production.
	ExitSemiStructuredExtract(c *SemiStructuredExtractContext)

	// ExitSimpleCase is called when exiting the simpleCase production.
	ExitSimpleCase(c *SimpleCaseContext)

	// ExitCurrentLike is called when exiting the currentLike production.
	ExitCurrentLike(c *CurrentLikeContext)

	// ExitColumnReference is called when exiting the columnReference production.
	ExitColumnReference(c *ColumnReferenceContext)

	// ExitRowConstructor is called when exiting the rowConstructor production.
	ExitRowConstructor(c *RowConstructorContext)

	// ExitLast is called when exiting the last production.
	ExitLast(c *LastContext)

	// ExitStar is called when exiting the star production.
	ExitStar(c *StarContext)

	// ExitOverlay is called when exiting the overlay production.
	ExitOverlay(c *OverlayContext)

	// ExitSubscript is called when exiting the subscript production.
	ExitSubscript(c *SubscriptContext)

	// ExitTimestampdiff is called when exiting the timestampdiff production.
	ExitTimestampdiff(c *TimestampdiffContext)

	// ExitSubqueryExpression is called when exiting the subqueryExpression production.
	ExitSubqueryExpression(c *SubqueryExpressionContext)

	// ExitCollate is called when exiting the collate production.
	ExitCollate(c *CollateContext)

	// ExitConstantDefault is called when exiting the constantDefault production.
	ExitConstantDefault(c *ConstantDefaultContext)

	// ExitExtract is called when exiting the extract production.
	ExitExtract(c *ExtractContext)

	// ExitFunctionCall is called when exiting the functionCall production.
	ExitFunctionCall(c *FunctionCallContext)

	// ExitSearchedCase is called when exiting the searchedCase production.
	ExitSearchedCase(c *SearchedCaseContext)

	// ExitPosition is called when exiting the position production.
	ExitPosition(c *PositionContext)

	// ExitFirst is called when exiting the first production.
	ExitFirst(c *FirstContext)

	// ExitSemiStructuredExtractionPath is called when exiting the semiStructuredExtractionPath production.
	ExitSemiStructuredExtractionPath(c *SemiStructuredExtractionPathContext)

	// ExitJsonPathIdentifier is called when exiting the jsonPathIdentifier production.
	ExitJsonPathIdentifier(c *JsonPathIdentifierContext)

	// ExitJsonPathBracketedIdentifier is called when exiting the jsonPathBracketedIdentifier production.
	ExitJsonPathBracketedIdentifier(c *JsonPathBracketedIdentifierContext)

	// ExitJsonPathFirstPart is called when exiting the jsonPathFirstPart production.
	ExitJsonPathFirstPart(c *JsonPathFirstPartContext)

	// ExitJsonPathParts is called when exiting the jsonPathParts production.
	ExitJsonPathParts(c *JsonPathPartsContext)

	// ExitLiteralType is called when exiting the literalType production.
	ExitLiteralType(c *LiteralTypeContext)

	// ExitNullLiteral is called when exiting the nullLiteral production.
	ExitNullLiteral(c *NullLiteralContext)

	// ExitPosParameterLiteral is called when exiting the posParameterLiteral production.
	ExitPosParameterLiteral(c *PosParameterLiteralContext)

	// ExitNamedParameterLiteral is called when exiting the namedParameterLiteral production.
	ExitNamedParameterLiteral(c *NamedParameterLiteralContext)

	// ExitIntervalLiteral is called when exiting the intervalLiteral production.
	ExitIntervalLiteral(c *IntervalLiteralContext)

	// ExitTypeConstructor is called when exiting the typeConstructor production.
	ExitTypeConstructor(c *TypeConstructorContext)

	// ExitNumericLiteral is called when exiting the numericLiteral production.
	ExitNumericLiteral(c *NumericLiteralContext)

	// ExitBooleanLiteral is called when exiting the booleanLiteral production.
	ExitBooleanLiteral(c *BooleanLiteralContext)

	// ExitStringLiteral is called when exiting the stringLiteral production.
	ExitStringLiteral(c *StringLiteralContext)

	// ExitNamedParameterMarker is called when exiting the namedParameterMarker production.
	ExitNamedParameterMarker(c *NamedParameterMarkerContext)

	// ExitComparisonOperator is called when exiting the comparisonOperator production.
	ExitComparisonOperator(c *ComparisonOperatorContext)

	// ExitArithmeticOperator is called when exiting the arithmeticOperator production.
	ExitArithmeticOperator(c *ArithmeticOperatorContext)

	// ExitPredicateOperator is called when exiting the predicateOperator production.
	ExitPredicateOperator(c *PredicateOperatorContext)

	// ExitBooleanValue is called when exiting the booleanValue production.
	ExitBooleanValue(c *BooleanValueContext)

	// ExitInterval is called when exiting the interval production.
	ExitInterval(c *IntervalContext)

	// ExitErrorCapturingMultiUnitsInterval is called when exiting the errorCapturingMultiUnitsInterval production.
	ExitErrorCapturingMultiUnitsInterval(c *ErrorCapturingMultiUnitsIntervalContext)

	// ExitMultiUnitsInterval is called when exiting the multiUnitsInterval production.
	ExitMultiUnitsInterval(c *MultiUnitsIntervalContext)

	// ExitErrorCapturingUnitToUnitInterval is called when exiting the errorCapturingUnitToUnitInterval production.
	ExitErrorCapturingUnitToUnitInterval(c *ErrorCapturingUnitToUnitIntervalContext)

	// ExitUnitToUnitInterval is called when exiting the unitToUnitInterval production.
	ExitUnitToUnitInterval(c *UnitToUnitIntervalContext)

	// ExitIntervalValue is called when exiting the intervalValue production.
	ExitIntervalValue(c *IntervalValueContext)

	// ExitUnitInMultiUnits is called when exiting the unitInMultiUnits production.
	ExitUnitInMultiUnits(c *UnitInMultiUnitsContext)

	// ExitUnitInUnitToUnit is called when exiting the unitInUnitToUnit production.
	ExitUnitInUnitToUnit(c *UnitInUnitToUnitContext)

	// ExitColPosition is called when exiting the colPosition production.
	ExitColPosition(c *ColPositionContext)

	// ExitCollationSpec is called when exiting the collationSpec production.
	ExitCollationSpec(c *CollationSpecContext)

	// ExitCollateClause is called when exiting the collateClause production.
	ExitCollateClause(c *CollateClauseContext)

	// ExitNonTrivialPrimitiveType is called when exiting the nonTrivialPrimitiveType production.
	ExitNonTrivialPrimitiveType(c *NonTrivialPrimitiveTypeContext)

	// ExitTrivialPrimitiveType is called when exiting the trivialPrimitiveType production.
	ExitTrivialPrimitiveType(c *TrivialPrimitiveTypeContext)

	// ExitPrimitiveType is called when exiting the primitiveType production.
	ExitPrimitiveType(c *PrimitiveTypeContext)

	// ExitComplexDataType is called when exiting the complexDataType production.
	ExitComplexDataType(c *ComplexDataTypeContext)

	// ExitPrimitiveDataType is called when exiting the primitiveDataType production.
	ExitPrimitiveDataType(c *PrimitiveDataTypeContext)

	// ExitQualifiedColTypeWithPositionList is called when exiting the qualifiedColTypeWithPositionList production.
	ExitQualifiedColTypeWithPositionList(c *QualifiedColTypeWithPositionListContext)

	// ExitQualifiedColTypeWithPosition is called when exiting the qualifiedColTypeWithPosition production.
	ExitQualifiedColTypeWithPosition(c *QualifiedColTypeWithPositionContext)

	// ExitColDefinitionDescriptorWithPosition is called when exiting the colDefinitionDescriptorWithPosition production.
	ExitColDefinitionDescriptorWithPosition(c *ColDefinitionDescriptorWithPositionContext)

	// ExitDefaultExpression is called when exiting the defaultExpression production.
	ExitDefaultExpression(c *DefaultExpressionContext)

	// ExitVariableDefaultExpression is called when exiting the variableDefaultExpression production.
	ExitVariableDefaultExpression(c *VariableDefaultExpressionContext)

	// ExitColTypeList is called when exiting the colTypeList production.
	ExitColTypeList(c *ColTypeListContext)

	// ExitColType is called when exiting the colType production.
	ExitColType(c *ColTypeContext)

	// ExitTableElementList is called when exiting the tableElementList production.
	ExitTableElementList(c *TableElementListContext)

	// ExitTableElement is called when exiting the tableElement production.
	ExitTableElement(c *TableElementContext)

	// ExitColDefinitionList is called when exiting the colDefinitionList production.
	ExitColDefinitionList(c *ColDefinitionListContext)

	// ExitColDefinition is called when exiting the colDefinition production.
	ExitColDefinition(c *ColDefinitionContext)

	// ExitColDefinitionOption is called when exiting the colDefinitionOption production.
	ExitColDefinitionOption(c *ColDefinitionOptionContext)

	// ExitGeneratedColumn is called when exiting the generatedColumn production.
	ExitGeneratedColumn(c *GeneratedColumnContext)

	// ExitIdentityColumn is called when exiting the identityColumn production.
	ExitIdentityColumn(c *IdentityColumnContext)

	// ExitIdentityColSpec is called when exiting the identityColSpec production.
	ExitIdentityColSpec(c *IdentityColSpecContext)

	// ExitSequenceGeneratorOption is called when exiting the sequenceGeneratorOption production.
	ExitSequenceGeneratorOption(c *SequenceGeneratorOptionContext)

	// ExitSequenceGeneratorStartOrStep is called when exiting the sequenceGeneratorStartOrStep production.
	ExitSequenceGeneratorStartOrStep(c *SequenceGeneratorStartOrStepContext)

	// ExitComplexColTypeList is called when exiting the complexColTypeList production.
	ExitComplexColTypeList(c *ComplexColTypeListContext)

	// ExitComplexColType is called when exiting the complexColType production.
	ExitComplexColType(c *ComplexColTypeContext)

	// ExitRoutineCharacteristics is called when exiting the routineCharacteristics production.
	ExitRoutineCharacteristics(c *RoutineCharacteristicsContext)

	// ExitRoutineLanguage is called when exiting the routineLanguage production.
	ExitRoutineLanguage(c *RoutineLanguageContext)

	// ExitSpecificName is called when exiting the specificName production.
	ExitSpecificName(c *SpecificNameContext)

	// ExitDeterministic is called when exiting the deterministic production.
	ExitDeterministic(c *DeterministicContext)

	// ExitSqlDataAccess is called when exiting the sqlDataAccess production.
	ExitSqlDataAccess(c *SqlDataAccessContext)

	// ExitNullCall is called when exiting the nullCall production.
	ExitNullCall(c *NullCallContext)

	// ExitRightsClause is called when exiting the rightsClause production.
	ExitRightsClause(c *RightsClauseContext)

	// ExitWhenClause is called when exiting the whenClause production.
	ExitWhenClause(c *WhenClauseContext)

	// ExitWindowClause is called when exiting the windowClause production.
	ExitWindowClause(c *WindowClauseContext)

	// ExitNamedWindow is called when exiting the namedWindow production.
	ExitNamedWindow(c *NamedWindowContext)

	// ExitWindowRef is called when exiting the windowRef production.
	ExitWindowRef(c *WindowRefContext)

	// ExitWindowDef is called when exiting the windowDef production.
	ExitWindowDef(c *WindowDefContext)

	// ExitWindowFrame is called when exiting the windowFrame production.
	ExitWindowFrame(c *WindowFrameContext)

	// ExitFrameBound is called when exiting the frameBound production.
	ExitFrameBound(c *FrameBoundContext)

	// ExitQualifiedNameList is called when exiting the qualifiedNameList production.
	ExitQualifiedNameList(c *QualifiedNameListContext)

	// ExitFunctionName is called when exiting the functionName production.
	ExitFunctionName(c *FunctionNameContext)

	// ExitQualifiedName is called when exiting the qualifiedName production.
	ExitQualifiedName(c *QualifiedNameContext)

	// ExitErrorCapturingIdentifier is called when exiting the errorCapturingIdentifier production.
	ExitErrorCapturingIdentifier(c *ErrorCapturingIdentifierContext)

	// ExitErrorIdent is called when exiting the errorIdent production.
	ExitErrorIdent(c *ErrorIdentContext)

	// ExitRealIdent is called when exiting the realIdent production.
	ExitRealIdent(c *RealIdentContext)

	// ExitIdentifier is called when exiting the identifier production.
	ExitIdentifier(c *IdentifierContext)

	// ExitSimpleIdentifier is called when exiting the simpleIdentifier production.
	ExitSimpleIdentifier(c *SimpleIdentifierContext)

	// ExitUnquotedIdentifier is called when exiting the unquotedIdentifier production.
	ExitUnquotedIdentifier(c *UnquotedIdentifierContext)

	// ExitQuotedIdentifierAlternative is called when exiting the quotedIdentifierAlternative production.
	ExitQuotedIdentifierAlternative(c *QuotedIdentifierAlternativeContext)

	// ExitIdentifierLiteral is called when exiting the identifierLiteral production.
	ExitIdentifierLiteral(c *IdentifierLiteralContext)

	// ExitSimpleUnquotedIdentifier is called when exiting the simpleUnquotedIdentifier production.
	ExitSimpleUnquotedIdentifier(c *SimpleUnquotedIdentifierContext)

	// ExitSimpleQuotedIdentifierAlternative is called when exiting the simpleQuotedIdentifierAlternative production.
	ExitSimpleQuotedIdentifierAlternative(c *SimpleQuotedIdentifierAlternativeContext)

	// ExitQuotedIdentifier is called when exiting the quotedIdentifier production.
	ExitQuotedIdentifier(c *QuotedIdentifierContext)

	// ExitBackQuotedIdentifier is called when exiting the backQuotedIdentifier production.
	ExitBackQuotedIdentifier(c *BackQuotedIdentifierContext)

	// ExitExponentLiteral is called when exiting the exponentLiteral production.
	ExitExponentLiteral(c *ExponentLiteralContext)

	// ExitDecimalLiteral is called when exiting the decimalLiteral production.
	ExitDecimalLiteral(c *DecimalLiteralContext)

	// ExitLegacyDecimalLiteral is called when exiting the legacyDecimalLiteral production.
	ExitLegacyDecimalLiteral(c *LegacyDecimalLiteralContext)

	// ExitIntegerLiteral is called when exiting the integerLiteral production.
	ExitIntegerLiteral(c *IntegerLiteralContext)

	// ExitBigIntLiteral is called when exiting the bigIntLiteral production.
	ExitBigIntLiteral(c *BigIntLiteralContext)

	// ExitSmallIntLiteral is called when exiting the smallIntLiteral production.
	ExitSmallIntLiteral(c *SmallIntLiteralContext)

	// ExitTinyIntLiteral is called when exiting the tinyIntLiteral production.
	ExitTinyIntLiteral(c *TinyIntLiteralContext)

	// ExitDoubleLiteral is called when exiting the doubleLiteral production.
	ExitDoubleLiteral(c *DoubleLiteralContext)

	// ExitFloatLiteral is called when exiting the floatLiteral production.
	ExitFloatLiteral(c *FloatLiteralContext)

	// ExitBigDecimalLiteral is called when exiting the bigDecimalLiteral production.
	ExitBigDecimalLiteral(c *BigDecimalLiteralContext)

	// ExitIntegerVal is called when exiting the integerVal production.
	ExitIntegerVal(c *IntegerValContext)

	// ExitParameterIntegerValue is called when exiting the parameterIntegerValue production.
	ExitParameterIntegerValue(c *ParameterIntegerValueContext)

	// ExitColumnConstraintDefinition is called when exiting the columnConstraintDefinition production.
	ExitColumnConstraintDefinition(c *ColumnConstraintDefinitionContext)

	// ExitColumnConstraint is called when exiting the columnConstraint production.
	ExitColumnConstraint(c *ColumnConstraintContext)

	// ExitTableConstraintDefinition is called when exiting the tableConstraintDefinition production.
	ExitTableConstraintDefinition(c *TableConstraintDefinitionContext)

	// ExitTableConstraint is called when exiting the tableConstraint production.
	ExitTableConstraint(c *TableConstraintContext)

	// ExitCheckConstraint is called when exiting the checkConstraint production.
	ExitCheckConstraint(c *CheckConstraintContext)

	// ExitUniqueSpec is called when exiting the uniqueSpec production.
	ExitUniqueSpec(c *UniqueSpecContext)

	// ExitUniqueConstraint is called when exiting the uniqueConstraint production.
	ExitUniqueConstraint(c *UniqueConstraintContext)

	// ExitReferenceSpec is called when exiting the referenceSpec production.
	ExitReferenceSpec(c *ReferenceSpecContext)

	// ExitForeignKeyConstraint is called when exiting the foreignKeyConstraint production.
	ExitForeignKeyConstraint(c *ForeignKeyConstraintContext)

	// ExitConstraintCharacteristic is called when exiting the constraintCharacteristic production.
	ExitConstraintCharacteristic(c *ConstraintCharacteristicContext)

	// ExitEnforcedCharacteristic is called when exiting the enforcedCharacteristic production.
	ExitEnforcedCharacteristic(c *EnforcedCharacteristicContext)

	// ExitRelyCharacteristic is called when exiting the relyCharacteristic production.
	ExitRelyCharacteristic(c *RelyCharacteristicContext)

	// ExitAlterColumnSpecList is called when exiting the alterColumnSpecList production.
	ExitAlterColumnSpecList(c *AlterColumnSpecListContext)

	// ExitAlterColumnSpec is called when exiting the alterColumnSpec production.
	ExitAlterColumnSpec(c *AlterColumnSpecContext)

	// ExitAlterColumnAction is called when exiting the alterColumnAction production.
	ExitAlterColumnAction(c *AlterColumnActionContext)

	// ExitSingleStringLiteralValue is called when exiting the singleStringLiteralValue production.
	ExitSingleStringLiteralValue(c *SingleStringLiteralValueContext)

	// ExitSingleDoubleQuotedStringLiteralValue is called when exiting the singleDoubleQuotedStringLiteralValue production.
	ExitSingleDoubleQuotedStringLiteralValue(c *SingleDoubleQuotedStringLiteralValueContext)

	// ExitSingleStringLit is called when exiting the singleStringLit production.
	ExitSingleStringLit(c *SingleStringLitContext)

	// ExitNamedParameterMarkerRule is called when exiting the namedParameterMarkerRule production.
	ExitNamedParameterMarkerRule(c *NamedParameterMarkerRuleContext)

	// ExitPositionalParameterMarkerRule is called when exiting the positionalParameterMarkerRule production.
	ExitPositionalParameterMarkerRule(c *PositionalParameterMarkerRuleContext)

	// ExitStringLit is called when exiting the stringLit production.
	ExitStringLit(c *StringLitContext)

	// ExitComment is called when exiting the comment production.
	ExitComment(c *CommentContext)

	// ExitVersion is called when exiting the version production.
	ExitVersion(c *VersionContext)

	// ExitOperatorPipeRightSide is called when exiting the operatorPipeRightSide production.
	ExitOperatorPipeRightSide(c *OperatorPipeRightSideContext)

	// ExitOperatorPipeSetAssignmentSeq is called when exiting the operatorPipeSetAssignmentSeq production.
	ExitOperatorPipeSetAssignmentSeq(c *OperatorPipeSetAssignmentSeqContext)

	// ExitAnsiNonReserved is called when exiting the ansiNonReserved production.
	ExitAnsiNonReserved(c *AnsiNonReservedContext)

	// ExitStrictNonReserved is called when exiting the strictNonReserved production.
	ExitStrictNonReserved(c *StrictNonReservedContext)

	// ExitNonReserved is called when exiting the nonReserved production.
	ExitNonReserved(c *NonReservedContext)
}
