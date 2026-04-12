// Code generated from SqlBaseParser.g4 by ANTLR 4.13.2. DO NOT EDIT.

package parser // SqlBaseParser
import "github.com/antlr4-go/antlr/v4"

// A complete Visitor for a parse tree produced by SqlBaseParser.
type SqlBaseParserVisitor interface {
	antlr.ParseTreeVisitor

	// Visit a parse tree produced by SqlBaseParser#compoundOrSingleStatement.
	VisitCompoundOrSingleStatement(ctx *CompoundOrSingleStatementContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#singleCompoundStatement.
	VisitSingleCompoundStatement(ctx *SingleCompoundStatementContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#beginEndCompoundBlock.
	VisitBeginEndCompoundBlock(ctx *BeginEndCompoundBlockContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#compoundBody.
	VisitCompoundBody(ctx *CompoundBodyContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#compoundStatement.
	VisitCompoundStatement(ctx *CompoundStatementContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#setVariableInsideSqlScript.
	VisitSetVariableInsideSqlScript(ctx *SetVariableInsideSqlScriptContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#sqlStateValue.
	VisitSqlStateValue(ctx *SqlStateValueContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#declareConditionStatement.
	VisitDeclareConditionStatement(ctx *DeclareConditionStatementContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#conditionValue.
	VisitConditionValue(ctx *ConditionValueContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#conditionValues.
	VisitConditionValues(ctx *ConditionValuesContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#declareHandlerStatement.
	VisitDeclareHandlerStatement(ctx *DeclareHandlerStatementContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#whileStatement.
	VisitWhileStatement(ctx *WhileStatementContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#ifElseStatement.
	VisitIfElseStatement(ctx *IfElseStatementContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#repeatStatement.
	VisitRepeatStatement(ctx *RepeatStatementContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#leaveStatement.
	VisitLeaveStatement(ctx *LeaveStatementContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#iterateStatement.
	VisitIterateStatement(ctx *IterateStatementContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#searchedCaseStatement.
	VisitSearchedCaseStatement(ctx *SearchedCaseStatementContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#simpleCaseStatement.
	VisitSimpleCaseStatement(ctx *SimpleCaseStatementContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#loopStatement.
	VisitLoopStatement(ctx *LoopStatementContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#forStatement.
	VisitForStatement(ctx *ForStatementContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#singleStatement.
	VisitSingleStatement(ctx *SingleStatementContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#beginLabel.
	VisitBeginLabel(ctx *BeginLabelContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#endLabel.
	VisitEndLabel(ctx *EndLabelContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#singleExpression.
	VisitSingleExpression(ctx *SingleExpressionContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#singleTableIdentifier.
	VisitSingleTableIdentifier(ctx *SingleTableIdentifierContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#singleMultipartIdentifier.
	VisitSingleMultipartIdentifier(ctx *SingleMultipartIdentifierContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#singleFunctionIdentifier.
	VisitSingleFunctionIdentifier(ctx *SingleFunctionIdentifierContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#singleDataType.
	VisitSingleDataType(ctx *SingleDataTypeContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#singleTableSchema.
	VisitSingleTableSchema(ctx *SingleTableSchemaContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#singleRoutineParamList.
	VisitSingleRoutineParamList(ctx *SingleRoutineParamListContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#statementDefault.
	VisitStatementDefault(ctx *StatementDefaultContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#visitExecuteImmediate.
	VisitVisitExecuteImmediate(ctx *VisitExecuteImmediateContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#dmlStatement.
	VisitDmlStatement(ctx *DmlStatementContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#use.
	VisitUse(ctx *UseContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#useNamespace.
	VisitUseNamespace(ctx *UseNamespaceContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#setCatalog.
	VisitSetCatalog(ctx *SetCatalogContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#createNamespace.
	VisitCreateNamespace(ctx *CreateNamespaceContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#setNamespaceProperties.
	VisitSetNamespaceProperties(ctx *SetNamespacePropertiesContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#unsetNamespaceProperties.
	VisitUnsetNamespaceProperties(ctx *UnsetNamespacePropertiesContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#setNamespaceCollation.
	VisitSetNamespaceCollation(ctx *SetNamespaceCollationContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#setNamespaceLocation.
	VisitSetNamespaceLocation(ctx *SetNamespaceLocationContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#dropNamespace.
	VisitDropNamespace(ctx *DropNamespaceContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#showNamespaces.
	VisitShowNamespaces(ctx *ShowNamespacesContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#createTable.
	VisitCreateTable(ctx *CreateTableContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#createTableLike.
	VisitCreateTableLike(ctx *CreateTableLikeContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#replaceTable.
	VisitReplaceTable(ctx *ReplaceTableContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#analyze.
	VisitAnalyze(ctx *AnalyzeContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#analyzeTables.
	VisitAnalyzeTables(ctx *AnalyzeTablesContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#addTableColumns.
	VisitAddTableColumns(ctx *AddTableColumnsContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#renameTableColumn.
	VisitRenameTableColumn(ctx *RenameTableColumnContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#dropTableColumns.
	VisitDropTableColumns(ctx *DropTableColumnsContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#renameTable.
	VisitRenameTable(ctx *RenameTableContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#setTableProperties.
	VisitSetTableProperties(ctx *SetTablePropertiesContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#unsetTableProperties.
	VisitUnsetTableProperties(ctx *UnsetTablePropertiesContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#alterTableAlterColumn.
	VisitAlterTableAlterColumn(ctx *AlterTableAlterColumnContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#hiveChangeColumn.
	VisitHiveChangeColumn(ctx *HiveChangeColumnContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#hiveReplaceColumns.
	VisitHiveReplaceColumns(ctx *HiveReplaceColumnsContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#setTableSerDe.
	VisitSetTableSerDe(ctx *SetTableSerDeContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#addTablePartition.
	VisitAddTablePartition(ctx *AddTablePartitionContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#renameTablePartition.
	VisitRenameTablePartition(ctx *RenameTablePartitionContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#dropTablePartitions.
	VisitDropTablePartitions(ctx *DropTablePartitionsContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#setTableLocation.
	VisitSetTableLocation(ctx *SetTableLocationContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#recoverPartitions.
	VisitRecoverPartitions(ctx *RecoverPartitionsContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#alterClusterBy.
	VisitAlterClusterBy(ctx *AlterClusterByContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#alterTableCollation.
	VisitAlterTableCollation(ctx *AlterTableCollationContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#addTableConstraint.
	VisitAddTableConstraint(ctx *AddTableConstraintContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#dropTableConstraint.
	VisitDropTableConstraint(ctx *DropTableConstraintContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#dropTable.
	VisitDropTable(ctx *DropTableContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#dropView.
	VisitDropView(ctx *DropViewContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#createView.
	VisitCreateView(ctx *CreateViewContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#createTempViewUsing.
	VisitCreateTempViewUsing(ctx *CreateTempViewUsingContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#alterViewQuery.
	VisitAlterViewQuery(ctx *AlterViewQueryContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#alterViewSchemaBinding.
	VisitAlterViewSchemaBinding(ctx *AlterViewSchemaBindingContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#createFunction.
	VisitCreateFunction(ctx *CreateFunctionContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#createUserDefinedFunction.
	VisitCreateUserDefinedFunction(ctx *CreateUserDefinedFunctionContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#dropFunction.
	VisitDropFunction(ctx *DropFunctionContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#createVariable.
	VisitCreateVariable(ctx *CreateVariableContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#dropVariable.
	VisitDropVariable(ctx *DropVariableContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#explain.
	VisitExplain(ctx *ExplainContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#showTables.
	VisitShowTables(ctx *ShowTablesContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#showTableExtended.
	VisitShowTableExtended(ctx *ShowTableExtendedContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#showTblProperties.
	VisitShowTblProperties(ctx *ShowTblPropertiesContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#showColumns.
	VisitShowColumns(ctx *ShowColumnsContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#showViews.
	VisitShowViews(ctx *ShowViewsContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#showPartitions.
	VisitShowPartitions(ctx *ShowPartitionsContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#showFunctions.
	VisitShowFunctions(ctx *ShowFunctionsContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#showProcedures.
	VisitShowProcedures(ctx *ShowProceduresContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#showCreateTable.
	VisitShowCreateTable(ctx *ShowCreateTableContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#showCurrentNamespace.
	VisitShowCurrentNamespace(ctx *ShowCurrentNamespaceContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#showCatalogs.
	VisitShowCatalogs(ctx *ShowCatalogsContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#describeFunction.
	VisitDescribeFunction(ctx *DescribeFunctionContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#describeProcedure.
	VisitDescribeProcedure(ctx *DescribeProcedureContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#describeNamespace.
	VisitDescribeNamespace(ctx *DescribeNamespaceContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#describeRelation.
	VisitDescribeRelation(ctx *DescribeRelationContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#describeQuery.
	VisitDescribeQuery(ctx *DescribeQueryContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#commentNamespace.
	VisitCommentNamespace(ctx *CommentNamespaceContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#commentTable.
	VisitCommentTable(ctx *CommentTableContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#refreshTable.
	VisitRefreshTable(ctx *RefreshTableContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#refreshFunction.
	VisitRefreshFunction(ctx *RefreshFunctionContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#refreshResource.
	VisitRefreshResource(ctx *RefreshResourceContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#cacheTable.
	VisitCacheTable(ctx *CacheTableContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#uncacheTable.
	VisitUncacheTable(ctx *UncacheTableContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#clearCache.
	VisitClearCache(ctx *ClearCacheContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#loadData.
	VisitLoadData(ctx *LoadDataContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#truncateTable.
	VisitTruncateTable(ctx *TruncateTableContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#repairTable.
	VisitRepairTable(ctx *RepairTableContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#manageResource.
	VisitManageResource(ctx *ManageResourceContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#createIndex.
	VisitCreateIndex(ctx *CreateIndexContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#dropIndex.
	VisitDropIndex(ctx *DropIndexContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#call.
	VisitCall(ctx *CallContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#failNativeCommand.
	VisitFailNativeCommand(ctx *FailNativeCommandContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#createPipelineDataset.
	VisitCreatePipelineDataset(ctx *CreatePipelineDatasetContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#createPipelineInsertIntoFlow.
	VisitCreatePipelineInsertIntoFlow(ctx *CreatePipelineInsertIntoFlowContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#materializedView.
	VisitMaterializedView(ctx *MaterializedViewContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#streamingTable.
	VisitStreamingTable(ctx *StreamingTableContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#createPipelineDatasetHeader.
	VisitCreatePipelineDatasetHeader(ctx *CreatePipelineDatasetHeaderContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#streamTableName.
	VisitStreamTableName(ctx *StreamTableNameContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#failSetRole.
	VisitFailSetRole(ctx *FailSetRoleContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#setTimeZone.
	VisitSetTimeZone(ctx *SetTimeZoneContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#setVariable.
	VisitSetVariable(ctx *SetVariableContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#setQuotedConfiguration.
	VisitSetQuotedConfiguration(ctx *SetQuotedConfigurationContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#setConfiguration.
	VisitSetConfiguration(ctx *SetConfigurationContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#resetQuotedConfiguration.
	VisitResetQuotedConfiguration(ctx *ResetQuotedConfigurationContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#resetConfiguration.
	VisitResetConfiguration(ctx *ResetConfigurationContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#executeImmediate.
	VisitExecuteImmediate(ctx *ExecuteImmediateContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#executeImmediateUsing.
	VisitExecuteImmediateUsing(ctx *ExecuteImmediateUsingContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#timezone.
	VisitTimezone(ctx *TimezoneContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#configKey.
	VisitConfigKey(ctx *ConfigKeyContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#configValue.
	VisitConfigValue(ctx *ConfigValueContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#unsupportedHiveNativeCommands.
	VisitUnsupportedHiveNativeCommands(ctx *UnsupportedHiveNativeCommandsContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#createTableHeader.
	VisitCreateTableHeader(ctx *CreateTableHeaderContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#replaceTableHeader.
	VisitReplaceTableHeader(ctx *ReplaceTableHeaderContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#clusterBySpec.
	VisitClusterBySpec(ctx *ClusterBySpecContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#bucketSpec.
	VisitBucketSpec(ctx *BucketSpecContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#skewSpec.
	VisitSkewSpec(ctx *SkewSpecContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#locationSpec.
	VisitLocationSpec(ctx *LocationSpecContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#schemaBinding.
	VisitSchemaBinding(ctx *SchemaBindingContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#commentSpec.
	VisitCommentSpec(ctx *CommentSpecContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#singleQuery.
	VisitSingleQuery(ctx *SingleQueryContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#query.
	VisitQuery(ctx *QueryContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#insertOverwriteTable.
	VisitInsertOverwriteTable(ctx *InsertOverwriteTableContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#insertIntoTable.
	VisitInsertIntoTable(ctx *InsertIntoTableContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#insertIntoReplaceWhere.
	VisitInsertIntoReplaceWhere(ctx *InsertIntoReplaceWhereContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#insertOverwriteHiveDir.
	VisitInsertOverwriteHiveDir(ctx *InsertOverwriteHiveDirContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#insertOverwriteDir.
	VisitInsertOverwriteDir(ctx *InsertOverwriteDirContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#partitionSpecLocation.
	VisitPartitionSpecLocation(ctx *PartitionSpecLocationContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#partitionSpec.
	VisitPartitionSpec(ctx *PartitionSpecContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#partitionVal.
	VisitPartitionVal(ctx *PartitionValContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#createPipelineFlowHeader.
	VisitCreatePipelineFlowHeader(ctx *CreatePipelineFlowHeaderContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#namespace.
	VisitNamespace(ctx *NamespaceContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#namespaces.
	VisitNamespaces(ctx *NamespacesContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#variable.
	VisitVariable(ctx *VariableContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#describeFuncName.
	VisitDescribeFuncName(ctx *DescribeFuncNameContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#describeColName.
	VisitDescribeColName(ctx *DescribeColNameContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#ctes.
	VisitCtes(ctx *CtesContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#namedQuery.
	VisitNamedQuery(ctx *NamedQueryContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#tableProvider.
	VisitTableProvider(ctx *TableProviderContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#createTableClauses.
	VisitCreateTableClauses(ctx *CreateTableClausesContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#propertyList.
	VisitPropertyList(ctx *PropertyListContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#propertyWithKeyAndEquals.
	VisitPropertyWithKeyAndEquals(ctx *PropertyWithKeyAndEqualsContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#propertyWithKeyNoEquals.
	VisitPropertyWithKeyNoEquals(ctx *PropertyWithKeyNoEqualsContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#propertyKey.
	VisitPropertyKey(ctx *PropertyKeyContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#propertyKeyOrStringLit.
	VisitPropertyKeyOrStringLit(ctx *PropertyKeyOrStringLitContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#propertyKeyOrStringLitNoCoalesce.
	VisitPropertyKeyOrStringLitNoCoalesce(ctx *PropertyKeyOrStringLitNoCoalesceContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#propertyValue.
	VisitPropertyValue(ctx *PropertyValueContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#expressionPropertyList.
	VisitExpressionPropertyList(ctx *ExpressionPropertyListContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#expressionPropertyWithKeyAndEquals.
	VisitExpressionPropertyWithKeyAndEquals(ctx *ExpressionPropertyWithKeyAndEqualsContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#expressionPropertyWithKeyNoEquals.
	VisitExpressionPropertyWithKeyNoEquals(ctx *ExpressionPropertyWithKeyNoEqualsContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#constantList.
	VisitConstantList(ctx *ConstantListContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#nestedConstantList.
	VisitNestedConstantList(ctx *NestedConstantListContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#createFileFormat.
	VisitCreateFileFormat(ctx *CreateFileFormatContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#tableFileFormat.
	VisitTableFileFormat(ctx *TableFileFormatContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#genericFileFormat.
	VisitGenericFileFormat(ctx *GenericFileFormatContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#storageHandler.
	VisitStorageHandler(ctx *StorageHandlerContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#resource.
	VisitResource(ctx *ResourceContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#singleInsertQuery.
	VisitSingleInsertQuery(ctx *SingleInsertQueryContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#multiInsertQuery.
	VisitMultiInsertQuery(ctx *MultiInsertQueryContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#deleteFromTable.
	VisitDeleteFromTable(ctx *DeleteFromTableContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#updateTable.
	VisitUpdateTable(ctx *UpdateTableContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#mergeIntoTable.
	VisitMergeIntoTable(ctx *MergeIntoTableContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#identifierReference.
	VisitIdentifierReference(ctx *IdentifierReferenceContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#catalogIdentifierReference.
	VisitCatalogIdentifierReference(ctx *CatalogIdentifierReferenceContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#queryOrganization.
	VisitQueryOrganization(ctx *QueryOrganizationContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#multiInsertQueryBody.
	VisitMultiInsertQueryBody(ctx *MultiInsertQueryBodyContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#operatorPipeStatement.
	VisitOperatorPipeStatement(ctx *OperatorPipeStatementContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#queryTermDefault.
	VisitQueryTermDefault(ctx *QueryTermDefaultContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#setOperation.
	VisitSetOperation(ctx *SetOperationContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#queryPrimaryDefault.
	VisitQueryPrimaryDefault(ctx *QueryPrimaryDefaultContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#fromStmt.
	VisitFromStmt(ctx *FromStmtContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#table.
	VisitTable(ctx *TableContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#inlineTableDefault1.
	VisitInlineTableDefault1(ctx *InlineTableDefault1Context) interface{}

	// Visit a parse tree produced by SqlBaseParser#subquery.
	VisitSubquery(ctx *SubqueryContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#sortItem.
	VisitSortItem(ctx *SortItemContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#fromStatement.
	VisitFromStatement(ctx *FromStatementContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#fromStatementBody.
	VisitFromStatementBody(ctx *FromStatementBodyContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#transformQuerySpecification.
	VisitTransformQuerySpecification(ctx *TransformQuerySpecificationContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#regularQuerySpecification.
	VisitRegularQuerySpecification(ctx *RegularQuerySpecificationContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#transformClause.
	VisitTransformClause(ctx *TransformClauseContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#selectClause.
	VisitSelectClause(ctx *SelectClauseContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#setClause.
	VisitSetClause(ctx *SetClauseContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#matchedClause.
	VisitMatchedClause(ctx *MatchedClauseContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#notMatchedClause.
	VisitNotMatchedClause(ctx *NotMatchedClauseContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#notMatchedBySourceClause.
	VisitNotMatchedBySourceClause(ctx *NotMatchedBySourceClauseContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#matchedAction.
	VisitMatchedAction(ctx *MatchedActionContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#notMatchedAction.
	VisitNotMatchedAction(ctx *NotMatchedActionContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#notMatchedBySourceAction.
	VisitNotMatchedBySourceAction(ctx *NotMatchedBySourceActionContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#exceptClause.
	VisitExceptClause(ctx *ExceptClauseContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#assignmentList.
	VisitAssignmentList(ctx *AssignmentListContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#assignment.
	VisitAssignment(ctx *AssignmentContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#whereClause.
	VisitWhereClause(ctx *WhereClauseContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#havingClause.
	VisitHavingClause(ctx *HavingClauseContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#hint.
	VisitHint(ctx *HintContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#hintStatement.
	VisitHintStatement(ctx *HintStatementContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#fromClause.
	VisitFromClause(ctx *FromClauseContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#temporalClause.
	VisitTemporalClause(ctx *TemporalClauseContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#aggregationClause.
	VisitAggregationClause(ctx *AggregationClauseContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#groupByClause.
	VisitGroupByClause(ctx *GroupByClauseContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#groupingAnalytics.
	VisitGroupingAnalytics(ctx *GroupingAnalyticsContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#groupingElement.
	VisitGroupingElement(ctx *GroupingElementContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#groupingSet.
	VisitGroupingSet(ctx *GroupingSetContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#pivotClause.
	VisitPivotClause(ctx *PivotClauseContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#pivotColumn.
	VisitPivotColumn(ctx *PivotColumnContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#pivotValue.
	VisitPivotValue(ctx *PivotValueContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#unpivotClause.
	VisitUnpivotClause(ctx *UnpivotClauseContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#unpivotNullClause.
	VisitUnpivotNullClause(ctx *UnpivotNullClauseContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#unpivotOperator.
	VisitUnpivotOperator(ctx *UnpivotOperatorContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#unpivotSingleValueColumnClause.
	VisitUnpivotSingleValueColumnClause(ctx *UnpivotSingleValueColumnClauseContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#unpivotMultiValueColumnClause.
	VisitUnpivotMultiValueColumnClause(ctx *UnpivotMultiValueColumnClauseContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#unpivotColumnSet.
	VisitUnpivotColumnSet(ctx *UnpivotColumnSetContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#unpivotValueColumn.
	VisitUnpivotValueColumn(ctx *UnpivotValueColumnContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#unpivotNameColumn.
	VisitUnpivotNameColumn(ctx *UnpivotNameColumnContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#unpivotColumnAndAlias.
	VisitUnpivotColumnAndAlias(ctx *UnpivotColumnAndAliasContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#unpivotColumn.
	VisitUnpivotColumn(ctx *UnpivotColumnContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#unpivotAlias.
	VisitUnpivotAlias(ctx *UnpivotAliasContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#lateralView.
	VisitLateralView(ctx *LateralViewContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#watermarkClause.
	VisitWatermarkClause(ctx *WatermarkClauseContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#setQuantifier.
	VisitSetQuantifier(ctx *SetQuantifierContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#relation.
	VisitRelation(ctx *RelationContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#relationExtension.
	VisitRelationExtension(ctx *RelationExtensionContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#joinRelation.
	VisitJoinRelation(ctx *JoinRelationContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#joinType.
	VisitJoinType(ctx *JoinTypeContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#joinCriteria.
	VisitJoinCriteria(ctx *JoinCriteriaContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#sample.
	VisitSample(ctx *SampleContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#sampleByPercentile.
	VisitSampleByPercentile(ctx *SampleByPercentileContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#sampleByRows.
	VisitSampleByRows(ctx *SampleByRowsContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#sampleByBucket.
	VisitSampleByBucket(ctx *SampleByBucketContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#sampleByBytes.
	VisitSampleByBytes(ctx *SampleByBytesContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#identifierList.
	VisitIdentifierList(ctx *IdentifierListContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#identifierSeq.
	VisitIdentifierSeq(ctx *IdentifierSeqContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#orderedIdentifierList.
	VisitOrderedIdentifierList(ctx *OrderedIdentifierListContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#orderedIdentifier.
	VisitOrderedIdentifier(ctx *OrderedIdentifierContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#identifierCommentList.
	VisitIdentifierCommentList(ctx *IdentifierCommentListContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#identifierComment.
	VisitIdentifierComment(ctx *IdentifierCommentContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#streamRelation.
	VisitStreamRelation(ctx *StreamRelationContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#tableName.
	VisitTableName(ctx *TableNameContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#aliasedQuery.
	VisitAliasedQuery(ctx *AliasedQueryContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#aliasedRelation.
	VisitAliasedRelation(ctx *AliasedRelationContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#inlineTableDefault2.
	VisitInlineTableDefault2(ctx *InlineTableDefault2Context) interface{}

	// Visit a parse tree produced by SqlBaseParser#tableValuedFunction.
	VisitTableValuedFunction(ctx *TableValuedFunctionContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#optionsClause.
	VisitOptionsClause(ctx *OptionsClauseContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#inlineTable.
	VisitInlineTable(ctx *InlineTableContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#functionTableSubqueryArgument.
	VisitFunctionTableSubqueryArgument(ctx *FunctionTableSubqueryArgumentContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#tableArgumentPartitioning.
	VisitTableArgumentPartitioning(ctx *TableArgumentPartitioningContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#functionTableNamedArgumentExpression.
	VisitFunctionTableNamedArgumentExpression(ctx *FunctionTableNamedArgumentExpressionContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#functionTableReferenceArgument.
	VisitFunctionTableReferenceArgument(ctx *FunctionTableReferenceArgumentContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#functionTableArgument.
	VisitFunctionTableArgument(ctx *FunctionTableArgumentContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#functionTable.
	VisitFunctionTable(ctx *FunctionTableContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#tableAlias.
	VisitTableAlias(ctx *TableAliasContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#rowFormatSerde.
	VisitRowFormatSerde(ctx *RowFormatSerdeContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#rowFormatDelimited.
	VisitRowFormatDelimited(ctx *RowFormatDelimitedContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#multipartIdentifierList.
	VisitMultipartIdentifierList(ctx *MultipartIdentifierListContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#multipartIdentifier.
	VisitMultipartIdentifier(ctx *MultipartIdentifierContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#multipartIdentifierPropertyList.
	VisitMultipartIdentifierPropertyList(ctx *MultipartIdentifierPropertyListContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#multipartIdentifierProperty.
	VisitMultipartIdentifierProperty(ctx *MultipartIdentifierPropertyContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#tableIdentifier.
	VisitTableIdentifier(ctx *TableIdentifierContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#functionIdentifier.
	VisitFunctionIdentifier(ctx *FunctionIdentifierContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#namedExpression.
	VisitNamedExpression(ctx *NamedExpressionContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#namedExpressionSeq.
	VisitNamedExpressionSeq(ctx *NamedExpressionSeqContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#partitionFieldList.
	VisitPartitionFieldList(ctx *PartitionFieldListContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#partitionTransform.
	VisitPartitionTransform(ctx *PartitionTransformContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#partitionColumn.
	VisitPartitionColumn(ctx *PartitionColumnContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#identityTransform.
	VisitIdentityTransform(ctx *IdentityTransformContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#applyTransform.
	VisitApplyTransform(ctx *ApplyTransformContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#transformArgument.
	VisitTransformArgument(ctx *TransformArgumentContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#expression.
	VisitExpression(ctx *ExpressionContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#namedArgumentExpression.
	VisitNamedArgumentExpression(ctx *NamedArgumentExpressionContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#functionArgument.
	VisitFunctionArgument(ctx *FunctionArgumentContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#expressionSeq.
	VisitExpressionSeq(ctx *ExpressionSeqContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#logicalNot.
	VisitLogicalNot(ctx *LogicalNotContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#predicated.
	VisitPredicated(ctx *PredicatedContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#exists.
	VisitExists(ctx *ExistsContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#logicalBinary.
	VisitLogicalBinary(ctx *LogicalBinaryContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#predicate.
	VisitPredicate(ctx *PredicateContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#errorCapturingNot.
	VisitErrorCapturingNot(ctx *ErrorCapturingNotContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#valueExpressionDefault.
	VisitValueExpressionDefault(ctx *ValueExpressionDefaultContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#comparison.
	VisitComparison(ctx *ComparisonContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#shiftExpression.
	VisitShiftExpression(ctx *ShiftExpressionContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#arithmeticBinary.
	VisitArithmeticBinary(ctx *ArithmeticBinaryContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#arithmeticUnary.
	VisitArithmeticUnary(ctx *ArithmeticUnaryContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#shiftOperator.
	VisitShiftOperator(ctx *ShiftOperatorContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#datetimeUnit.
	VisitDatetimeUnit(ctx *DatetimeUnitContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#struct.
	VisitStruct(ctx *StructContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#dereference.
	VisitDereference(ctx *DereferenceContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#castByColon.
	VisitCastByColon(ctx *CastByColonContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#timestampadd.
	VisitTimestampadd(ctx *TimestampaddContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#substring.
	VisitSubstring(ctx *SubstringContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#cast.
	VisitCast(ctx *CastContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#lambda.
	VisitLambda(ctx *LambdaContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#parenthesizedExpression.
	VisitParenthesizedExpression(ctx *ParenthesizedExpressionContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#any_value.
	VisitAny_value(ctx *Any_valueContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#trim.
	VisitTrim(ctx *TrimContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#semiStructuredExtract.
	VisitSemiStructuredExtract(ctx *SemiStructuredExtractContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#simpleCase.
	VisitSimpleCase(ctx *SimpleCaseContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#currentLike.
	VisitCurrentLike(ctx *CurrentLikeContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#columnReference.
	VisitColumnReference(ctx *ColumnReferenceContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#rowConstructor.
	VisitRowConstructor(ctx *RowConstructorContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#last.
	VisitLast(ctx *LastContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#star.
	VisitStar(ctx *StarContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#overlay.
	VisitOverlay(ctx *OverlayContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#subscript.
	VisitSubscript(ctx *SubscriptContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#timestampdiff.
	VisitTimestampdiff(ctx *TimestampdiffContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#subqueryExpression.
	VisitSubqueryExpression(ctx *SubqueryExpressionContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#collate.
	VisitCollate(ctx *CollateContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#constantDefault.
	VisitConstantDefault(ctx *ConstantDefaultContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#extract.
	VisitExtract(ctx *ExtractContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#functionCall.
	VisitFunctionCall(ctx *FunctionCallContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#searchedCase.
	VisitSearchedCase(ctx *SearchedCaseContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#position.
	VisitPosition(ctx *PositionContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#first.
	VisitFirst(ctx *FirstContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#semiStructuredExtractionPath.
	VisitSemiStructuredExtractionPath(ctx *SemiStructuredExtractionPathContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#jsonPathIdentifier.
	VisitJsonPathIdentifier(ctx *JsonPathIdentifierContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#jsonPathBracketedIdentifier.
	VisitJsonPathBracketedIdentifier(ctx *JsonPathBracketedIdentifierContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#jsonPathFirstPart.
	VisitJsonPathFirstPart(ctx *JsonPathFirstPartContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#jsonPathParts.
	VisitJsonPathParts(ctx *JsonPathPartsContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#literalType.
	VisitLiteralType(ctx *LiteralTypeContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#nullLiteral.
	VisitNullLiteral(ctx *NullLiteralContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#posParameterLiteral.
	VisitPosParameterLiteral(ctx *PosParameterLiteralContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#namedParameterLiteral.
	VisitNamedParameterLiteral(ctx *NamedParameterLiteralContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#intervalLiteral.
	VisitIntervalLiteral(ctx *IntervalLiteralContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#typeConstructor.
	VisitTypeConstructor(ctx *TypeConstructorContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#numericLiteral.
	VisitNumericLiteral(ctx *NumericLiteralContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#booleanLiteral.
	VisitBooleanLiteral(ctx *BooleanLiteralContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#stringLiteral.
	VisitStringLiteral(ctx *StringLiteralContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#namedParameterMarker.
	VisitNamedParameterMarker(ctx *NamedParameterMarkerContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#comparisonOperator.
	VisitComparisonOperator(ctx *ComparisonOperatorContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#arithmeticOperator.
	VisitArithmeticOperator(ctx *ArithmeticOperatorContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#predicateOperator.
	VisitPredicateOperator(ctx *PredicateOperatorContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#booleanValue.
	VisitBooleanValue(ctx *BooleanValueContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#interval.
	VisitInterval(ctx *IntervalContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#errorCapturingMultiUnitsInterval.
	VisitErrorCapturingMultiUnitsInterval(ctx *ErrorCapturingMultiUnitsIntervalContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#multiUnitsInterval.
	VisitMultiUnitsInterval(ctx *MultiUnitsIntervalContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#errorCapturingUnitToUnitInterval.
	VisitErrorCapturingUnitToUnitInterval(ctx *ErrorCapturingUnitToUnitIntervalContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#unitToUnitInterval.
	VisitUnitToUnitInterval(ctx *UnitToUnitIntervalContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#intervalValue.
	VisitIntervalValue(ctx *IntervalValueContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#unitInMultiUnits.
	VisitUnitInMultiUnits(ctx *UnitInMultiUnitsContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#unitInUnitToUnit.
	VisitUnitInUnitToUnit(ctx *UnitInUnitToUnitContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#colPosition.
	VisitColPosition(ctx *ColPositionContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#collationSpec.
	VisitCollationSpec(ctx *CollationSpecContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#collateClause.
	VisitCollateClause(ctx *CollateClauseContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#nonTrivialPrimitiveType.
	VisitNonTrivialPrimitiveType(ctx *NonTrivialPrimitiveTypeContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#trivialPrimitiveType.
	VisitTrivialPrimitiveType(ctx *TrivialPrimitiveTypeContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#primitiveType.
	VisitPrimitiveType(ctx *PrimitiveTypeContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#complexDataType.
	VisitComplexDataType(ctx *ComplexDataTypeContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#primitiveDataType.
	VisitPrimitiveDataType(ctx *PrimitiveDataTypeContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#qualifiedColTypeWithPositionList.
	VisitQualifiedColTypeWithPositionList(ctx *QualifiedColTypeWithPositionListContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#qualifiedColTypeWithPosition.
	VisitQualifiedColTypeWithPosition(ctx *QualifiedColTypeWithPositionContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#colDefinitionDescriptorWithPosition.
	VisitColDefinitionDescriptorWithPosition(ctx *ColDefinitionDescriptorWithPositionContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#defaultExpression.
	VisitDefaultExpression(ctx *DefaultExpressionContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#variableDefaultExpression.
	VisitVariableDefaultExpression(ctx *VariableDefaultExpressionContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#colTypeList.
	VisitColTypeList(ctx *ColTypeListContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#colType.
	VisitColType(ctx *ColTypeContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#tableElementList.
	VisitTableElementList(ctx *TableElementListContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#tableElement.
	VisitTableElement(ctx *TableElementContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#colDefinitionList.
	VisitColDefinitionList(ctx *ColDefinitionListContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#colDefinition.
	VisitColDefinition(ctx *ColDefinitionContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#colDefinitionOption.
	VisitColDefinitionOption(ctx *ColDefinitionOptionContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#generatedColumn.
	VisitGeneratedColumn(ctx *GeneratedColumnContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#identityColumn.
	VisitIdentityColumn(ctx *IdentityColumnContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#identityColSpec.
	VisitIdentityColSpec(ctx *IdentityColSpecContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#sequenceGeneratorOption.
	VisitSequenceGeneratorOption(ctx *SequenceGeneratorOptionContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#sequenceGeneratorStartOrStep.
	VisitSequenceGeneratorStartOrStep(ctx *SequenceGeneratorStartOrStepContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#complexColTypeList.
	VisitComplexColTypeList(ctx *ComplexColTypeListContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#complexColType.
	VisitComplexColType(ctx *ComplexColTypeContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#routineCharacteristics.
	VisitRoutineCharacteristics(ctx *RoutineCharacteristicsContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#routineLanguage.
	VisitRoutineLanguage(ctx *RoutineLanguageContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#specificName.
	VisitSpecificName(ctx *SpecificNameContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#deterministic.
	VisitDeterministic(ctx *DeterministicContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#sqlDataAccess.
	VisitSqlDataAccess(ctx *SqlDataAccessContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#nullCall.
	VisitNullCall(ctx *NullCallContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#rightsClause.
	VisitRightsClause(ctx *RightsClauseContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#whenClause.
	VisitWhenClause(ctx *WhenClauseContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#windowClause.
	VisitWindowClause(ctx *WindowClauseContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#namedWindow.
	VisitNamedWindow(ctx *NamedWindowContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#windowRef.
	VisitWindowRef(ctx *WindowRefContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#windowDef.
	VisitWindowDef(ctx *WindowDefContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#windowFrame.
	VisitWindowFrame(ctx *WindowFrameContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#frameBound.
	VisitFrameBound(ctx *FrameBoundContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#qualifiedNameList.
	VisitQualifiedNameList(ctx *QualifiedNameListContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#functionName.
	VisitFunctionName(ctx *FunctionNameContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#qualifiedName.
	VisitQualifiedName(ctx *QualifiedNameContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#errorCapturingIdentifier.
	VisitErrorCapturingIdentifier(ctx *ErrorCapturingIdentifierContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#errorIdent.
	VisitErrorIdent(ctx *ErrorIdentContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#realIdent.
	VisitRealIdent(ctx *RealIdentContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#identifier.
	VisitIdentifier(ctx *IdentifierContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#simpleIdentifier.
	VisitSimpleIdentifier(ctx *SimpleIdentifierContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#unquotedIdentifier.
	VisitUnquotedIdentifier(ctx *UnquotedIdentifierContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#quotedIdentifierAlternative.
	VisitQuotedIdentifierAlternative(ctx *QuotedIdentifierAlternativeContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#identifierLiteral.
	VisitIdentifierLiteral(ctx *IdentifierLiteralContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#simpleUnquotedIdentifier.
	VisitSimpleUnquotedIdentifier(ctx *SimpleUnquotedIdentifierContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#simpleQuotedIdentifierAlternative.
	VisitSimpleQuotedIdentifierAlternative(ctx *SimpleQuotedIdentifierAlternativeContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#quotedIdentifier.
	VisitQuotedIdentifier(ctx *QuotedIdentifierContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#backQuotedIdentifier.
	VisitBackQuotedIdentifier(ctx *BackQuotedIdentifierContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#exponentLiteral.
	VisitExponentLiteral(ctx *ExponentLiteralContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#decimalLiteral.
	VisitDecimalLiteral(ctx *DecimalLiteralContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#legacyDecimalLiteral.
	VisitLegacyDecimalLiteral(ctx *LegacyDecimalLiteralContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#integerLiteral.
	VisitIntegerLiteral(ctx *IntegerLiteralContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#bigIntLiteral.
	VisitBigIntLiteral(ctx *BigIntLiteralContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#smallIntLiteral.
	VisitSmallIntLiteral(ctx *SmallIntLiteralContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#tinyIntLiteral.
	VisitTinyIntLiteral(ctx *TinyIntLiteralContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#doubleLiteral.
	VisitDoubleLiteral(ctx *DoubleLiteralContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#floatLiteral.
	VisitFloatLiteral(ctx *FloatLiteralContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#bigDecimalLiteral.
	VisitBigDecimalLiteral(ctx *BigDecimalLiteralContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#integerVal.
	VisitIntegerVal(ctx *IntegerValContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#parameterIntegerValue.
	VisitParameterIntegerValue(ctx *ParameterIntegerValueContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#columnConstraintDefinition.
	VisitColumnConstraintDefinition(ctx *ColumnConstraintDefinitionContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#columnConstraint.
	VisitColumnConstraint(ctx *ColumnConstraintContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#tableConstraintDefinition.
	VisitTableConstraintDefinition(ctx *TableConstraintDefinitionContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#tableConstraint.
	VisitTableConstraint(ctx *TableConstraintContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#checkConstraint.
	VisitCheckConstraint(ctx *CheckConstraintContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#uniqueSpec.
	VisitUniqueSpec(ctx *UniqueSpecContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#uniqueConstraint.
	VisitUniqueConstraint(ctx *UniqueConstraintContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#referenceSpec.
	VisitReferenceSpec(ctx *ReferenceSpecContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#foreignKeyConstraint.
	VisitForeignKeyConstraint(ctx *ForeignKeyConstraintContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#constraintCharacteristic.
	VisitConstraintCharacteristic(ctx *ConstraintCharacteristicContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#enforcedCharacteristic.
	VisitEnforcedCharacteristic(ctx *EnforcedCharacteristicContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#relyCharacteristic.
	VisitRelyCharacteristic(ctx *RelyCharacteristicContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#alterColumnSpecList.
	VisitAlterColumnSpecList(ctx *AlterColumnSpecListContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#alterColumnSpec.
	VisitAlterColumnSpec(ctx *AlterColumnSpecContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#alterColumnAction.
	VisitAlterColumnAction(ctx *AlterColumnActionContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#singleStringLiteralValue.
	VisitSingleStringLiteralValue(ctx *SingleStringLiteralValueContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#singleDoubleQuotedStringLiteralValue.
	VisitSingleDoubleQuotedStringLiteralValue(ctx *SingleDoubleQuotedStringLiteralValueContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#singleStringLit.
	VisitSingleStringLit(ctx *SingleStringLitContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#namedParameterMarkerRule.
	VisitNamedParameterMarkerRule(ctx *NamedParameterMarkerRuleContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#positionalParameterMarkerRule.
	VisitPositionalParameterMarkerRule(ctx *PositionalParameterMarkerRuleContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#stringLit.
	VisitStringLit(ctx *StringLitContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#comment.
	VisitComment(ctx *CommentContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#version.
	VisitVersion(ctx *VersionContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#operatorPipeRightSide.
	VisitOperatorPipeRightSide(ctx *OperatorPipeRightSideContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#operatorPipeSetAssignmentSeq.
	VisitOperatorPipeSetAssignmentSeq(ctx *OperatorPipeSetAssignmentSeqContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#ansiNonReserved.
	VisitAnsiNonReserved(ctx *AnsiNonReservedContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#strictNonReserved.
	VisitStrictNonReserved(ctx *StrictNonReservedContext) interface{}

	// Visit a parse tree produced by SqlBaseParser#nonReserved.
	VisitNonReserved(ctx *NonReservedContext) interface{}
}
