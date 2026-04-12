// Code generated from SqlBaseParser.g4 by ANTLR 4.13.2. DO NOT EDIT.

package parser // SqlBaseParser
import "github.com/antlr4-go/antlr/v4"

type BaseSqlBaseParserVisitor struct {
	*antlr.BaseParseTreeVisitor
}

func (v *BaseSqlBaseParserVisitor) VisitCompoundOrSingleStatement(ctx *CompoundOrSingleStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitSingleCompoundStatement(ctx *SingleCompoundStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitBeginEndCompoundBlock(ctx *BeginEndCompoundBlockContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitCompoundBody(ctx *CompoundBodyContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitCompoundStatement(ctx *CompoundStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitSetVariableInsideSqlScript(ctx *SetVariableInsideSqlScriptContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitSqlStateValue(ctx *SqlStateValueContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitDeclareConditionStatement(ctx *DeclareConditionStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitConditionValue(ctx *ConditionValueContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitConditionValues(ctx *ConditionValuesContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitDeclareHandlerStatement(ctx *DeclareHandlerStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitWhileStatement(ctx *WhileStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitIfElseStatement(ctx *IfElseStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitRepeatStatement(ctx *RepeatStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitLeaveStatement(ctx *LeaveStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitIterateStatement(ctx *IterateStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitSearchedCaseStatement(ctx *SearchedCaseStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitSimpleCaseStatement(ctx *SimpleCaseStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitLoopStatement(ctx *LoopStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitForStatement(ctx *ForStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitSingleStatement(ctx *SingleStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitBeginLabel(ctx *BeginLabelContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitEndLabel(ctx *EndLabelContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitSingleExpression(ctx *SingleExpressionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitSingleTableIdentifier(ctx *SingleTableIdentifierContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitSingleMultipartIdentifier(ctx *SingleMultipartIdentifierContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitSingleFunctionIdentifier(ctx *SingleFunctionIdentifierContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitSingleDataType(ctx *SingleDataTypeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitSingleTableSchema(ctx *SingleTableSchemaContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitSingleRoutineParamList(ctx *SingleRoutineParamListContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitStatementDefault(ctx *StatementDefaultContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitVisitExecuteImmediate(ctx *VisitExecuteImmediateContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitDmlStatement(ctx *DmlStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitUse(ctx *UseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitUseNamespace(ctx *UseNamespaceContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitSetCatalog(ctx *SetCatalogContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitCreateNamespace(ctx *CreateNamespaceContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitSetNamespaceProperties(ctx *SetNamespacePropertiesContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitUnsetNamespaceProperties(ctx *UnsetNamespacePropertiesContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitSetNamespaceCollation(ctx *SetNamespaceCollationContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitSetNamespaceLocation(ctx *SetNamespaceLocationContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitDropNamespace(ctx *DropNamespaceContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitShowNamespaces(ctx *ShowNamespacesContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitCreateTable(ctx *CreateTableContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitCreateTableLike(ctx *CreateTableLikeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitReplaceTable(ctx *ReplaceTableContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitAnalyze(ctx *AnalyzeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitAnalyzeTables(ctx *AnalyzeTablesContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitAddTableColumns(ctx *AddTableColumnsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitRenameTableColumn(ctx *RenameTableColumnContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitDropTableColumns(ctx *DropTableColumnsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitRenameTable(ctx *RenameTableContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitSetTableProperties(ctx *SetTablePropertiesContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitUnsetTableProperties(ctx *UnsetTablePropertiesContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitAlterTableAlterColumn(ctx *AlterTableAlterColumnContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitHiveChangeColumn(ctx *HiveChangeColumnContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitHiveReplaceColumns(ctx *HiveReplaceColumnsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitSetTableSerDe(ctx *SetTableSerDeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitAddTablePartition(ctx *AddTablePartitionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitRenameTablePartition(ctx *RenameTablePartitionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitDropTablePartitions(ctx *DropTablePartitionsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitSetTableLocation(ctx *SetTableLocationContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitRecoverPartitions(ctx *RecoverPartitionsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitAlterClusterBy(ctx *AlterClusterByContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitAlterTableCollation(ctx *AlterTableCollationContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitAddTableConstraint(ctx *AddTableConstraintContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitDropTableConstraint(ctx *DropTableConstraintContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitDropTable(ctx *DropTableContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitDropView(ctx *DropViewContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitCreateView(ctx *CreateViewContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitCreateTempViewUsing(ctx *CreateTempViewUsingContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitAlterViewQuery(ctx *AlterViewQueryContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitAlterViewSchemaBinding(ctx *AlterViewSchemaBindingContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitCreateFunction(ctx *CreateFunctionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitCreateUserDefinedFunction(ctx *CreateUserDefinedFunctionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitDropFunction(ctx *DropFunctionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitCreateVariable(ctx *CreateVariableContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitDropVariable(ctx *DropVariableContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitExplain(ctx *ExplainContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitShowTables(ctx *ShowTablesContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitShowTableExtended(ctx *ShowTableExtendedContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitShowTblProperties(ctx *ShowTblPropertiesContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitShowColumns(ctx *ShowColumnsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitShowViews(ctx *ShowViewsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitShowPartitions(ctx *ShowPartitionsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitShowFunctions(ctx *ShowFunctionsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitShowProcedures(ctx *ShowProceduresContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitShowCreateTable(ctx *ShowCreateTableContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitShowCurrentNamespace(ctx *ShowCurrentNamespaceContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitShowCatalogs(ctx *ShowCatalogsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitDescribeFunction(ctx *DescribeFunctionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitDescribeProcedure(ctx *DescribeProcedureContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitDescribeNamespace(ctx *DescribeNamespaceContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitDescribeRelation(ctx *DescribeRelationContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitDescribeQuery(ctx *DescribeQueryContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitCommentNamespace(ctx *CommentNamespaceContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitCommentTable(ctx *CommentTableContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitRefreshTable(ctx *RefreshTableContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitRefreshFunction(ctx *RefreshFunctionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitRefreshResource(ctx *RefreshResourceContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitCacheTable(ctx *CacheTableContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitUncacheTable(ctx *UncacheTableContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitClearCache(ctx *ClearCacheContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitLoadData(ctx *LoadDataContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitTruncateTable(ctx *TruncateTableContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitRepairTable(ctx *RepairTableContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitManageResource(ctx *ManageResourceContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitCreateIndex(ctx *CreateIndexContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitDropIndex(ctx *DropIndexContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitCall(ctx *CallContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitFailNativeCommand(ctx *FailNativeCommandContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitCreatePipelineDataset(ctx *CreatePipelineDatasetContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitCreatePipelineInsertIntoFlow(ctx *CreatePipelineInsertIntoFlowContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitMaterializedView(ctx *MaterializedViewContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitStreamingTable(ctx *StreamingTableContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitCreatePipelineDatasetHeader(ctx *CreatePipelineDatasetHeaderContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitStreamTableName(ctx *StreamTableNameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitFailSetRole(ctx *FailSetRoleContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitSetTimeZone(ctx *SetTimeZoneContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitSetVariable(ctx *SetVariableContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitSetQuotedConfiguration(ctx *SetQuotedConfigurationContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitSetConfiguration(ctx *SetConfigurationContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitResetQuotedConfiguration(ctx *ResetQuotedConfigurationContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitResetConfiguration(ctx *ResetConfigurationContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitExecuteImmediate(ctx *ExecuteImmediateContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitExecuteImmediateUsing(ctx *ExecuteImmediateUsingContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitTimezone(ctx *TimezoneContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitConfigKey(ctx *ConfigKeyContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitConfigValue(ctx *ConfigValueContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitUnsupportedHiveNativeCommands(ctx *UnsupportedHiveNativeCommandsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitCreateTableHeader(ctx *CreateTableHeaderContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitReplaceTableHeader(ctx *ReplaceTableHeaderContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitClusterBySpec(ctx *ClusterBySpecContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitBucketSpec(ctx *BucketSpecContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitSkewSpec(ctx *SkewSpecContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitLocationSpec(ctx *LocationSpecContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitSchemaBinding(ctx *SchemaBindingContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitCommentSpec(ctx *CommentSpecContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitSingleQuery(ctx *SingleQueryContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitQuery(ctx *QueryContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitInsertOverwriteTable(ctx *InsertOverwriteTableContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitInsertIntoTable(ctx *InsertIntoTableContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitInsertIntoReplaceWhere(ctx *InsertIntoReplaceWhereContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitInsertOverwriteHiveDir(ctx *InsertOverwriteHiveDirContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitInsertOverwriteDir(ctx *InsertOverwriteDirContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitPartitionSpecLocation(ctx *PartitionSpecLocationContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitPartitionSpec(ctx *PartitionSpecContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitPartitionVal(ctx *PartitionValContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitCreatePipelineFlowHeader(ctx *CreatePipelineFlowHeaderContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitNamespace(ctx *NamespaceContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitNamespaces(ctx *NamespacesContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitVariable(ctx *VariableContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitDescribeFuncName(ctx *DescribeFuncNameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitDescribeColName(ctx *DescribeColNameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitCtes(ctx *CtesContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitNamedQuery(ctx *NamedQueryContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitTableProvider(ctx *TableProviderContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitCreateTableClauses(ctx *CreateTableClausesContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitPropertyList(ctx *PropertyListContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitPropertyWithKeyAndEquals(ctx *PropertyWithKeyAndEqualsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitPropertyWithKeyNoEquals(ctx *PropertyWithKeyNoEqualsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitPropertyKey(ctx *PropertyKeyContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitPropertyKeyOrStringLit(ctx *PropertyKeyOrStringLitContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitPropertyKeyOrStringLitNoCoalesce(ctx *PropertyKeyOrStringLitNoCoalesceContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitPropertyValue(ctx *PropertyValueContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitExpressionPropertyList(ctx *ExpressionPropertyListContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitExpressionPropertyWithKeyAndEquals(ctx *ExpressionPropertyWithKeyAndEqualsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitExpressionPropertyWithKeyNoEquals(ctx *ExpressionPropertyWithKeyNoEqualsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitConstantList(ctx *ConstantListContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitNestedConstantList(ctx *NestedConstantListContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitCreateFileFormat(ctx *CreateFileFormatContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitTableFileFormat(ctx *TableFileFormatContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitGenericFileFormat(ctx *GenericFileFormatContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitStorageHandler(ctx *StorageHandlerContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitResource(ctx *ResourceContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitSingleInsertQuery(ctx *SingleInsertQueryContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitMultiInsertQuery(ctx *MultiInsertQueryContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitDeleteFromTable(ctx *DeleteFromTableContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitUpdateTable(ctx *UpdateTableContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitMergeIntoTable(ctx *MergeIntoTableContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitIdentifierReference(ctx *IdentifierReferenceContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitCatalogIdentifierReference(ctx *CatalogIdentifierReferenceContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitQueryOrganization(ctx *QueryOrganizationContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitMultiInsertQueryBody(ctx *MultiInsertQueryBodyContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitOperatorPipeStatement(ctx *OperatorPipeStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitQueryTermDefault(ctx *QueryTermDefaultContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitSetOperation(ctx *SetOperationContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitQueryPrimaryDefault(ctx *QueryPrimaryDefaultContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitFromStmt(ctx *FromStmtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitTable(ctx *TableContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitInlineTableDefault1(ctx *InlineTableDefault1Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitSubquery(ctx *SubqueryContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitSortItem(ctx *SortItemContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitFromStatement(ctx *FromStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitFromStatementBody(ctx *FromStatementBodyContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitTransformQuerySpecification(ctx *TransformQuerySpecificationContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitRegularQuerySpecification(ctx *RegularQuerySpecificationContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitTransformClause(ctx *TransformClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitSelectClause(ctx *SelectClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitSetClause(ctx *SetClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitMatchedClause(ctx *MatchedClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitNotMatchedClause(ctx *NotMatchedClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitNotMatchedBySourceClause(ctx *NotMatchedBySourceClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitMatchedAction(ctx *MatchedActionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitNotMatchedAction(ctx *NotMatchedActionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitNotMatchedBySourceAction(ctx *NotMatchedBySourceActionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitExceptClause(ctx *ExceptClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitAssignmentList(ctx *AssignmentListContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitAssignment(ctx *AssignmentContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitWhereClause(ctx *WhereClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitHavingClause(ctx *HavingClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitHint(ctx *HintContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitHintStatement(ctx *HintStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitFromClause(ctx *FromClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitTemporalClause(ctx *TemporalClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitAggregationClause(ctx *AggregationClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitGroupByClause(ctx *GroupByClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitGroupingAnalytics(ctx *GroupingAnalyticsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitGroupingElement(ctx *GroupingElementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitGroupingSet(ctx *GroupingSetContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitPivotClause(ctx *PivotClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitPivotColumn(ctx *PivotColumnContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitPivotValue(ctx *PivotValueContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitUnpivotClause(ctx *UnpivotClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitUnpivotNullClause(ctx *UnpivotNullClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitUnpivotOperator(ctx *UnpivotOperatorContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitUnpivotSingleValueColumnClause(ctx *UnpivotSingleValueColumnClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitUnpivotMultiValueColumnClause(ctx *UnpivotMultiValueColumnClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitUnpivotColumnSet(ctx *UnpivotColumnSetContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitUnpivotValueColumn(ctx *UnpivotValueColumnContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitUnpivotNameColumn(ctx *UnpivotNameColumnContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitUnpivotColumnAndAlias(ctx *UnpivotColumnAndAliasContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitUnpivotColumn(ctx *UnpivotColumnContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitUnpivotAlias(ctx *UnpivotAliasContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitLateralView(ctx *LateralViewContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitWatermarkClause(ctx *WatermarkClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitSetQuantifier(ctx *SetQuantifierContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitRelation(ctx *RelationContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitRelationExtension(ctx *RelationExtensionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitJoinRelation(ctx *JoinRelationContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitJoinType(ctx *JoinTypeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitJoinCriteria(ctx *JoinCriteriaContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitSample(ctx *SampleContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitSampleByPercentile(ctx *SampleByPercentileContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitSampleByRows(ctx *SampleByRowsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitSampleByBucket(ctx *SampleByBucketContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitSampleByBytes(ctx *SampleByBytesContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitIdentifierList(ctx *IdentifierListContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitIdentifierSeq(ctx *IdentifierSeqContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitOrderedIdentifierList(ctx *OrderedIdentifierListContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitOrderedIdentifier(ctx *OrderedIdentifierContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitIdentifierCommentList(ctx *IdentifierCommentListContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitIdentifierComment(ctx *IdentifierCommentContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitStreamRelation(ctx *StreamRelationContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitTableName(ctx *TableNameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitAliasedQuery(ctx *AliasedQueryContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitAliasedRelation(ctx *AliasedRelationContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitInlineTableDefault2(ctx *InlineTableDefault2Context) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitTableValuedFunction(ctx *TableValuedFunctionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitOptionsClause(ctx *OptionsClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitInlineTable(ctx *InlineTableContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitFunctionTableSubqueryArgument(ctx *FunctionTableSubqueryArgumentContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitTableArgumentPartitioning(ctx *TableArgumentPartitioningContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitFunctionTableNamedArgumentExpression(ctx *FunctionTableNamedArgumentExpressionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitFunctionTableReferenceArgument(ctx *FunctionTableReferenceArgumentContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitFunctionTableArgument(ctx *FunctionTableArgumentContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitFunctionTable(ctx *FunctionTableContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitTableAlias(ctx *TableAliasContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitRowFormatSerde(ctx *RowFormatSerdeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitRowFormatDelimited(ctx *RowFormatDelimitedContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitMultipartIdentifierList(ctx *MultipartIdentifierListContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitMultipartIdentifier(ctx *MultipartIdentifierContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitMultipartIdentifierPropertyList(ctx *MultipartIdentifierPropertyListContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitMultipartIdentifierProperty(ctx *MultipartIdentifierPropertyContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitTableIdentifier(ctx *TableIdentifierContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitFunctionIdentifier(ctx *FunctionIdentifierContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitNamedExpression(ctx *NamedExpressionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitNamedExpressionSeq(ctx *NamedExpressionSeqContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitPartitionFieldList(ctx *PartitionFieldListContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitPartitionTransform(ctx *PartitionTransformContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitPartitionColumn(ctx *PartitionColumnContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitIdentityTransform(ctx *IdentityTransformContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitApplyTransform(ctx *ApplyTransformContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitTransformArgument(ctx *TransformArgumentContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitExpression(ctx *ExpressionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitNamedArgumentExpression(ctx *NamedArgumentExpressionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitFunctionArgument(ctx *FunctionArgumentContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitExpressionSeq(ctx *ExpressionSeqContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitLogicalNot(ctx *LogicalNotContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitPredicated(ctx *PredicatedContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitExists(ctx *ExistsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitLogicalBinary(ctx *LogicalBinaryContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitPredicate(ctx *PredicateContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitErrorCapturingNot(ctx *ErrorCapturingNotContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitValueExpressionDefault(ctx *ValueExpressionDefaultContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitComparison(ctx *ComparisonContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitShiftExpression(ctx *ShiftExpressionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitArithmeticBinary(ctx *ArithmeticBinaryContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitArithmeticUnary(ctx *ArithmeticUnaryContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitShiftOperator(ctx *ShiftOperatorContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitDatetimeUnit(ctx *DatetimeUnitContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitStruct(ctx *StructContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitDereference(ctx *DereferenceContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitCastByColon(ctx *CastByColonContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitTimestampadd(ctx *TimestampaddContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitSubstring(ctx *SubstringContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitCast(ctx *CastContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitLambda(ctx *LambdaContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitParenthesizedExpression(ctx *ParenthesizedExpressionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitAny_value(ctx *Any_valueContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitTrim(ctx *TrimContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitSemiStructuredExtract(ctx *SemiStructuredExtractContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitSimpleCase(ctx *SimpleCaseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitCurrentLike(ctx *CurrentLikeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitColumnReference(ctx *ColumnReferenceContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitRowConstructor(ctx *RowConstructorContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitLast(ctx *LastContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitStar(ctx *StarContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitOverlay(ctx *OverlayContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitSubscript(ctx *SubscriptContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitTimestampdiff(ctx *TimestampdiffContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitSubqueryExpression(ctx *SubqueryExpressionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitCollate(ctx *CollateContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitConstantDefault(ctx *ConstantDefaultContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitExtract(ctx *ExtractContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitFunctionCall(ctx *FunctionCallContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitSearchedCase(ctx *SearchedCaseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitPosition(ctx *PositionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitFirst(ctx *FirstContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitSemiStructuredExtractionPath(ctx *SemiStructuredExtractionPathContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitJsonPathIdentifier(ctx *JsonPathIdentifierContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitJsonPathBracketedIdentifier(ctx *JsonPathBracketedIdentifierContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitJsonPathFirstPart(ctx *JsonPathFirstPartContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitJsonPathParts(ctx *JsonPathPartsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitLiteralType(ctx *LiteralTypeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitNullLiteral(ctx *NullLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitPosParameterLiteral(ctx *PosParameterLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitNamedParameterLiteral(ctx *NamedParameterLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitIntervalLiteral(ctx *IntervalLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitTypeConstructor(ctx *TypeConstructorContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitNumericLiteral(ctx *NumericLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitBooleanLiteral(ctx *BooleanLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitStringLiteral(ctx *StringLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitNamedParameterMarker(ctx *NamedParameterMarkerContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitComparisonOperator(ctx *ComparisonOperatorContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitArithmeticOperator(ctx *ArithmeticOperatorContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitPredicateOperator(ctx *PredicateOperatorContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitBooleanValue(ctx *BooleanValueContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitInterval(ctx *IntervalContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitErrorCapturingMultiUnitsInterval(ctx *ErrorCapturingMultiUnitsIntervalContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitMultiUnitsInterval(ctx *MultiUnitsIntervalContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitErrorCapturingUnitToUnitInterval(ctx *ErrorCapturingUnitToUnitIntervalContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitUnitToUnitInterval(ctx *UnitToUnitIntervalContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitIntervalValue(ctx *IntervalValueContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitUnitInMultiUnits(ctx *UnitInMultiUnitsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitUnitInUnitToUnit(ctx *UnitInUnitToUnitContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitColPosition(ctx *ColPositionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitCollationSpec(ctx *CollationSpecContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitCollateClause(ctx *CollateClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitNonTrivialPrimitiveType(ctx *NonTrivialPrimitiveTypeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitTrivialPrimitiveType(ctx *TrivialPrimitiveTypeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitPrimitiveType(ctx *PrimitiveTypeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitComplexDataType(ctx *ComplexDataTypeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitPrimitiveDataType(ctx *PrimitiveDataTypeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitQualifiedColTypeWithPositionList(ctx *QualifiedColTypeWithPositionListContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitQualifiedColTypeWithPosition(ctx *QualifiedColTypeWithPositionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitColDefinitionDescriptorWithPosition(ctx *ColDefinitionDescriptorWithPositionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitDefaultExpression(ctx *DefaultExpressionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitVariableDefaultExpression(ctx *VariableDefaultExpressionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitColTypeList(ctx *ColTypeListContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitColType(ctx *ColTypeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitTableElementList(ctx *TableElementListContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitTableElement(ctx *TableElementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitColDefinitionList(ctx *ColDefinitionListContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitColDefinition(ctx *ColDefinitionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitColDefinitionOption(ctx *ColDefinitionOptionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitGeneratedColumn(ctx *GeneratedColumnContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitIdentityColumn(ctx *IdentityColumnContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitIdentityColSpec(ctx *IdentityColSpecContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitSequenceGeneratorOption(ctx *SequenceGeneratorOptionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitSequenceGeneratorStartOrStep(ctx *SequenceGeneratorStartOrStepContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitComplexColTypeList(ctx *ComplexColTypeListContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitComplexColType(ctx *ComplexColTypeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitRoutineCharacteristics(ctx *RoutineCharacteristicsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitRoutineLanguage(ctx *RoutineLanguageContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitSpecificName(ctx *SpecificNameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitDeterministic(ctx *DeterministicContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitSqlDataAccess(ctx *SqlDataAccessContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitNullCall(ctx *NullCallContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitRightsClause(ctx *RightsClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitWhenClause(ctx *WhenClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitWindowClause(ctx *WindowClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitNamedWindow(ctx *NamedWindowContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitWindowRef(ctx *WindowRefContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitWindowDef(ctx *WindowDefContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitWindowFrame(ctx *WindowFrameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitFrameBound(ctx *FrameBoundContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitQualifiedNameList(ctx *QualifiedNameListContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitFunctionName(ctx *FunctionNameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitQualifiedName(ctx *QualifiedNameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitErrorCapturingIdentifier(ctx *ErrorCapturingIdentifierContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitErrorIdent(ctx *ErrorIdentContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitRealIdent(ctx *RealIdentContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitIdentifier(ctx *IdentifierContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitSimpleIdentifier(ctx *SimpleIdentifierContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitUnquotedIdentifier(ctx *UnquotedIdentifierContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitQuotedIdentifierAlternative(ctx *QuotedIdentifierAlternativeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitIdentifierLiteral(ctx *IdentifierLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitSimpleUnquotedIdentifier(ctx *SimpleUnquotedIdentifierContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitSimpleQuotedIdentifierAlternative(ctx *SimpleQuotedIdentifierAlternativeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitQuotedIdentifier(ctx *QuotedIdentifierContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitBackQuotedIdentifier(ctx *BackQuotedIdentifierContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitExponentLiteral(ctx *ExponentLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitDecimalLiteral(ctx *DecimalLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitLegacyDecimalLiteral(ctx *LegacyDecimalLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitIntegerLiteral(ctx *IntegerLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitBigIntLiteral(ctx *BigIntLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitSmallIntLiteral(ctx *SmallIntLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitTinyIntLiteral(ctx *TinyIntLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitDoubleLiteral(ctx *DoubleLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitFloatLiteral(ctx *FloatLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitBigDecimalLiteral(ctx *BigDecimalLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitIntegerVal(ctx *IntegerValContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitParameterIntegerValue(ctx *ParameterIntegerValueContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitColumnConstraintDefinition(ctx *ColumnConstraintDefinitionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitColumnConstraint(ctx *ColumnConstraintContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitTableConstraintDefinition(ctx *TableConstraintDefinitionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitTableConstraint(ctx *TableConstraintContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitCheckConstraint(ctx *CheckConstraintContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitUniqueSpec(ctx *UniqueSpecContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitUniqueConstraint(ctx *UniqueConstraintContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitReferenceSpec(ctx *ReferenceSpecContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitForeignKeyConstraint(ctx *ForeignKeyConstraintContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitConstraintCharacteristic(ctx *ConstraintCharacteristicContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitEnforcedCharacteristic(ctx *EnforcedCharacteristicContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitRelyCharacteristic(ctx *RelyCharacteristicContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitAlterColumnSpecList(ctx *AlterColumnSpecListContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitAlterColumnSpec(ctx *AlterColumnSpecContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitAlterColumnAction(ctx *AlterColumnActionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitSingleStringLiteralValue(ctx *SingleStringLiteralValueContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitSingleDoubleQuotedStringLiteralValue(ctx *SingleDoubleQuotedStringLiteralValueContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitSingleStringLit(ctx *SingleStringLitContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitNamedParameterMarkerRule(ctx *NamedParameterMarkerRuleContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitPositionalParameterMarkerRule(ctx *PositionalParameterMarkerRuleContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitStringLit(ctx *StringLitContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitComment(ctx *CommentContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitVersion(ctx *VersionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitOperatorPipeRightSide(ctx *OperatorPipeRightSideContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitOperatorPipeSetAssignmentSeq(ctx *OperatorPipeSetAssignmentSeqContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitAnsiNonReserved(ctx *AnsiNonReservedContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitStrictNonReserved(ctx *StrictNonReservedContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseSqlBaseParserVisitor) VisitNonReserved(ctx *NonReservedContext) interface{} {
	return v.VisitChildren(ctx)
}
