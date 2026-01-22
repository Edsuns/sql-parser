// Code generated from SqlBaseParser.g4 by ANTLR 4.13.2. DO NOT EDIT.

package parser // SqlBaseParser
import "github.com/antlr4-go/antlr/v4"

// BaseSqlBaseParserListener is a complete listener for a parse tree produced by SqlBaseParser.
type BaseSqlBaseParserListener struct{}

var _ SqlBaseParserListener = &BaseSqlBaseParserListener{}

// VisitTerminal is called when a terminal node is visited.
func (s *BaseSqlBaseParserListener) VisitTerminal(node antlr.TerminalNode) {}

// VisitErrorNode is called when an error node is visited.
func (s *BaseSqlBaseParserListener) VisitErrorNode(node antlr.ErrorNode) {}

// EnterEveryRule is called when any rule is entered.
func (s *BaseSqlBaseParserListener) EnterEveryRule(ctx antlr.ParserRuleContext) {}

// ExitEveryRule is called when any rule is exited.
func (s *BaseSqlBaseParserListener) ExitEveryRule(ctx antlr.ParserRuleContext) {}

// EnterCompoundOrSingleStatement is called when production compoundOrSingleStatement is entered.
func (s *BaseSqlBaseParserListener) EnterCompoundOrSingleStatement(ctx *CompoundOrSingleStatementContext) {
}

// ExitCompoundOrSingleStatement is called when production compoundOrSingleStatement is exited.
func (s *BaseSqlBaseParserListener) ExitCompoundOrSingleStatement(ctx *CompoundOrSingleStatementContext) {
}

// EnterSingleCompoundStatement is called when production singleCompoundStatement is entered.
func (s *BaseSqlBaseParserListener) EnterSingleCompoundStatement(ctx *SingleCompoundStatementContext) {
}

// ExitSingleCompoundStatement is called when production singleCompoundStatement is exited.
func (s *BaseSqlBaseParserListener) ExitSingleCompoundStatement(ctx *SingleCompoundStatementContext) {
}

// EnterBeginEndCompoundBlock is called when production beginEndCompoundBlock is entered.
func (s *BaseSqlBaseParserListener) EnterBeginEndCompoundBlock(ctx *BeginEndCompoundBlockContext) {}

// ExitBeginEndCompoundBlock is called when production beginEndCompoundBlock is exited.
func (s *BaseSqlBaseParserListener) ExitBeginEndCompoundBlock(ctx *BeginEndCompoundBlockContext) {}

// EnterCompoundBody is called when production compoundBody is entered.
func (s *BaseSqlBaseParserListener) EnterCompoundBody(ctx *CompoundBodyContext) {}

// ExitCompoundBody is called when production compoundBody is exited.
func (s *BaseSqlBaseParserListener) ExitCompoundBody(ctx *CompoundBodyContext) {}

// EnterCompoundStatement is called when production compoundStatement is entered.
func (s *BaseSqlBaseParserListener) EnterCompoundStatement(ctx *CompoundStatementContext) {}

// ExitCompoundStatement is called when production compoundStatement is exited.
func (s *BaseSqlBaseParserListener) ExitCompoundStatement(ctx *CompoundStatementContext) {}

// EnterSetVariableInsideSqlScript is called when production setVariableInsideSqlScript is entered.
func (s *BaseSqlBaseParserListener) EnterSetVariableInsideSqlScript(ctx *SetVariableInsideSqlScriptContext) {
}

// ExitSetVariableInsideSqlScript is called when production setVariableInsideSqlScript is exited.
func (s *BaseSqlBaseParserListener) ExitSetVariableInsideSqlScript(ctx *SetVariableInsideSqlScriptContext) {
}

// EnterSqlStateValue is called when production sqlStateValue is entered.
func (s *BaseSqlBaseParserListener) EnterSqlStateValue(ctx *SqlStateValueContext) {}

// ExitSqlStateValue is called when production sqlStateValue is exited.
func (s *BaseSqlBaseParserListener) ExitSqlStateValue(ctx *SqlStateValueContext) {}

// EnterDeclareConditionStatement is called when production declareConditionStatement is entered.
func (s *BaseSqlBaseParserListener) EnterDeclareConditionStatement(ctx *DeclareConditionStatementContext) {
}

// ExitDeclareConditionStatement is called when production declareConditionStatement is exited.
func (s *BaseSqlBaseParserListener) ExitDeclareConditionStatement(ctx *DeclareConditionStatementContext) {
}

// EnterConditionValue is called when production conditionValue is entered.
func (s *BaseSqlBaseParserListener) EnterConditionValue(ctx *ConditionValueContext) {}

// ExitConditionValue is called when production conditionValue is exited.
func (s *BaseSqlBaseParserListener) ExitConditionValue(ctx *ConditionValueContext) {}

// EnterConditionValues is called when production conditionValues is entered.
func (s *BaseSqlBaseParserListener) EnterConditionValues(ctx *ConditionValuesContext) {}

// ExitConditionValues is called when production conditionValues is exited.
func (s *BaseSqlBaseParserListener) ExitConditionValues(ctx *ConditionValuesContext) {}

// EnterDeclareHandlerStatement is called when production declareHandlerStatement is entered.
func (s *BaseSqlBaseParserListener) EnterDeclareHandlerStatement(ctx *DeclareHandlerStatementContext) {
}

// ExitDeclareHandlerStatement is called when production declareHandlerStatement is exited.
func (s *BaseSqlBaseParserListener) ExitDeclareHandlerStatement(ctx *DeclareHandlerStatementContext) {
}

// EnterWhileStatement is called when production whileStatement is entered.
func (s *BaseSqlBaseParserListener) EnterWhileStatement(ctx *WhileStatementContext) {}

// ExitWhileStatement is called when production whileStatement is exited.
func (s *BaseSqlBaseParserListener) ExitWhileStatement(ctx *WhileStatementContext) {}

// EnterIfElseStatement is called when production ifElseStatement is entered.
func (s *BaseSqlBaseParserListener) EnterIfElseStatement(ctx *IfElseStatementContext) {}

// ExitIfElseStatement is called when production ifElseStatement is exited.
func (s *BaseSqlBaseParserListener) ExitIfElseStatement(ctx *IfElseStatementContext) {}

// EnterRepeatStatement is called when production repeatStatement is entered.
func (s *BaseSqlBaseParserListener) EnterRepeatStatement(ctx *RepeatStatementContext) {}

// ExitRepeatStatement is called when production repeatStatement is exited.
func (s *BaseSqlBaseParserListener) ExitRepeatStatement(ctx *RepeatStatementContext) {}

// EnterLeaveStatement is called when production leaveStatement is entered.
func (s *BaseSqlBaseParserListener) EnterLeaveStatement(ctx *LeaveStatementContext) {}

// ExitLeaveStatement is called when production leaveStatement is exited.
func (s *BaseSqlBaseParserListener) ExitLeaveStatement(ctx *LeaveStatementContext) {}

// EnterIterateStatement is called when production iterateStatement is entered.
func (s *BaseSqlBaseParserListener) EnterIterateStatement(ctx *IterateStatementContext) {}

// ExitIterateStatement is called when production iterateStatement is exited.
func (s *BaseSqlBaseParserListener) ExitIterateStatement(ctx *IterateStatementContext) {}

// EnterSearchedCaseStatement is called when production searchedCaseStatement is entered.
func (s *BaseSqlBaseParserListener) EnterSearchedCaseStatement(ctx *SearchedCaseStatementContext) {}

// ExitSearchedCaseStatement is called when production searchedCaseStatement is exited.
func (s *BaseSqlBaseParserListener) ExitSearchedCaseStatement(ctx *SearchedCaseStatementContext) {}

// EnterSimpleCaseStatement is called when production simpleCaseStatement is entered.
func (s *BaseSqlBaseParserListener) EnterSimpleCaseStatement(ctx *SimpleCaseStatementContext) {}

// ExitSimpleCaseStatement is called when production simpleCaseStatement is exited.
func (s *BaseSqlBaseParserListener) ExitSimpleCaseStatement(ctx *SimpleCaseStatementContext) {}

// EnterLoopStatement is called when production loopStatement is entered.
func (s *BaseSqlBaseParserListener) EnterLoopStatement(ctx *LoopStatementContext) {}

// ExitLoopStatement is called when production loopStatement is exited.
func (s *BaseSqlBaseParserListener) ExitLoopStatement(ctx *LoopStatementContext) {}

// EnterForStatement is called when production forStatement is entered.
func (s *BaseSqlBaseParserListener) EnterForStatement(ctx *ForStatementContext) {}

// ExitForStatement is called when production forStatement is exited.
func (s *BaseSqlBaseParserListener) ExitForStatement(ctx *ForStatementContext) {}

// EnterSingleStatement is called when production singleStatement is entered.
func (s *BaseSqlBaseParserListener) EnterSingleStatement(ctx *SingleStatementContext) {}

// ExitSingleStatement is called when production singleStatement is exited.
func (s *BaseSqlBaseParserListener) ExitSingleStatement(ctx *SingleStatementContext) {}

// EnterBeginLabel is called when production beginLabel is entered.
func (s *BaseSqlBaseParserListener) EnterBeginLabel(ctx *BeginLabelContext) {}

// ExitBeginLabel is called when production beginLabel is exited.
func (s *BaseSqlBaseParserListener) ExitBeginLabel(ctx *BeginLabelContext) {}

// EnterEndLabel is called when production endLabel is entered.
func (s *BaseSqlBaseParserListener) EnterEndLabel(ctx *EndLabelContext) {}

// ExitEndLabel is called when production endLabel is exited.
func (s *BaseSqlBaseParserListener) ExitEndLabel(ctx *EndLabelContext) {}

// EnterSingleExpression is called when production singleExpression is entered.
func (s *BaseSqlBaseParserListener) EnterSingleExpression(ctx *SingleExpressionContext) {}

// ExitSingleExpression is called when production singleExpression is exited.
func (s *BaseSqlBaseParserListener) ExitSingleExpression(ctx *SingleExpressionContext) {}

// EnterSingleTableIdentifier is called when production singleTableIdentifier is entered.
func (s *BaseSqlBaseParserListener) EnterSingleTableIdentifier(ctx *SingleTableIdentifierContext) {}

// ExitSingleTableIdentifier is called when production singleTableIdentifier is exited.
func (s *BaseSqlBaseParserListener) ExitSingleTableIdentifier(ctx *SingleTableIdentifierContext) {}

// EnterSingleMultipartIdentifier is called when production singleMultipartIdentifier is entered.
func (s *BaseSqlBaseParserListener) EnterSingleMultipartIdentifier(ctx *SingleMultipartIdentifierContext) {
}

// ExitSingleMultipartIdentifier is called when production singleMultipartIdentifier is exited.
func (s *BaseSqlBaseParserListener) ExitSingleMultipartIdentifier(ctx *SingleMultipartIdentifierContext) {
}

// EnterSingleFunctionIdentifier is called when production singleFunctionIdentifier is entered.
func (s *BaseSqlBaseParserListener) EnterSingleFunctionIdentifier(ctx *SingleFunctionIdentifierContext) {
}

// ExitSingleFunctionIdentifier is called when production singleFunctionIdentifier is exited.
func (s *BaseSqlBaseParserListener) ExitSingleFunctionIdentifier(ctx *SingleFunctionIdentifierContext) {
}

// EnterSingleDataType is called when production singleDataType is entered.
func (s *BaseSqlBaseParserListener) EnterSingleDataType(ctx *SingleDataTypeContext) {}

// ExitSingleDataType is called when production singleDataType is exited.
func (s *BaseSqlBaseParserListener) ExitSingleDataType(ctx *SingleDataTypeContext) {}

// EnterSingleTableSchema is called when production singleTableSchema is entered.
func (s *BaseSqlBaseParserListener) EnterSingleTableSchema(ctx *SingleTableSchemaContext) {}

// ExitSingleTableSchema is called when production singleTableSchema is exited.
func (s *BaseSqlBaseParserListener) ExitSingleTableSchema(ctx *SingleTableSchemaContext) {}

// EnterSingleRoutineParamList is called when production singleRoutineParamList is entered.
func (s *BaseSqlBaseParserListener) EnterSingleRoutineParamList(ctx *SingleRoutineParamListContext) {}

// ExitSingleRoutineParamList is called when production singleRoutineParamList is exited.
func (s *BaseSqlBaseParserListener) ExitSingleRoutineParamList(ctx *SingleRoutineParamListContext) {}

// EnterStatementDefault is called when production statementDefault is entered.
func (s *BaseSqlBaseParserListener) EnterStatementDefault(ctx *StatementDefaultContext) {}

// ExitStatementDefault is called when production statementDefault is exited.
func (s *BaseSqlBaseParserListener) ExitStatementDefault(ctx *StatementDefaultContext) {}

// EnterVisitExecuteImmediate is called when production visitExecuteImmediate is entered.
func (s *BaseSqlBaseParserListener) EnterVisitExecuteImmediate(ctx *VisitExecuteImmediateContext) {}

// ExitVisitExecuteImmediate is called when production visitExecuteImmediate is exited.
func (s *BaseSqlBaseParserListener) ExitVisitExecuteImmediate(ctx *VisitExecuteImmediateContext) {}

// EnterDmlStatement is called when production dmlStatement is entered.
func (s *BaseSqlBaseParserListener) EnterDmlStatement(ctx *DmlStatementContext) {}

// ExitDmlStatement is called when production dmlStatement is exited.
func (s *BaseSqlBaseParserListener) ExitDmlStatement(ctx *DmlStatementContext) {}

// EnterUse is called when production use is entered.
func (s *BaseSqlBaseParserListener) EnterUse(ctx *UseContext) {}

// ExitUse is called when production use is exited.
func (s *BaseSqlBaseParserListener) ExitUse(ctx *UseContext) {}

// EnterUseNamespace is called when production useNamespace is entered.
func (s *BaseSqlBaseParserListener) EnterUseNamespace(ctx *UseNamespaceContext) {}

// ExitUseNamespace is called when production useNamespace is exited.
func (s *BaseSqlBaseParserListener) ExitUseNamespace(ctx *UseNamespaceContext) {}

// EnterSetCatalog is called when production setCatalog is entered.
func (s *BaseSqlBaseParserListener) EnterSetCatalog(ctx *SetCatalogContext) {}

// ExitSetCatalog is called when production setCatalog is exited.
func (s *BaseSqlBaseParserListener) ExitSetCatalog(ctx *SetCatalogContext) {}

// EnterCreateNamespace is called when production createNamespace is entered.
func (s *BaseSqlBaseParserListener) EnterCreateNamespace(ctx *CreateNamespaceContext) {}

// ExitCreateNamespace is called when production createNamespace is exited.
func (s *BaseSqlBaseParserListener) ExitCreateNamespace(ctx *CreateNamespaceContext) {}

// EnterSetNamespaceProperties is called when production setNamespaceProperties is entered.
func (s *BaseSqlBaseParserListener) EnterSetNamespaceProperties(ctx *SetNamespacePropertiesContext) {}

// ExitSetNamespaceProperties is called when production setNamespaceProperties is exited.
func (s *BaseSqlBaseParserListener) ExitSetNamespaceProperties(ctx *SetNamespacePropertiesContext) {}

// EnterUnsetNamespaceProperties is called when production unsetNamespaceProperties is entered.
func (s *BaseSqlBaseParserListener) EnterUnsetNamespaceProperties(ctx *UnsetNamespacePropertiesContext) {
}

// ExitUnsetNamespaceProperties is called when production unsetNamespaceProperties is exited.
func (s *BaseSqlBaseParserListener) ExitUnsetNamespaceProperties(ctx *UnsetNamespacePropertiesContext) {
}

// EnterSetNamespaceCollation is called when production setNamespaceCollation is entered.
func (s *BaseSqlBaseParserListener) EnterSetNamespaceCollation(ctx *SetNamespaceCollationContext) {}

// ExitSetNamespaceCollation is called when production setNamespaceCollation is exited.
func (s *BaseSqlBaseParserListener) ExitSetNamespaceCollation(ctx *SetNamespaceCollationContext) {}

// EnterSetNamespaceLocation is called when production setNamespaceLocation is entered.
func (s *BaseSqlBaseParserListener) EnterSetNamespaceLocation(ctx *SetNamespaceLocationContext) {}

// ExitSetNamespaceLocation is called when production setNamespaceLocation is exited.
func (s *BaseSqlBaseParserListener) ExitSetNamespaceLocation(ctx *SetNamespaceLocationContext) {}

// EnterDropNamespace is called when production dropNamespace is entered.
func (s *BaseSqlBaseParserListener) EnterDropNamespace(ctx *DropNamespaceContext) {}

// ExitDropNamespace is called when production dropNamespace is exited.
func (s *BaseSqlBaseParserListener) ExitDropNamespace(ctx *DropNamespaceContext) {}

// EnterShowNamespaces is called when production showNamespaces is entered.
func (s *BaseSqlBaseParserListener) EnterShowNamespaces(ctx *ShowNamespacesContext) {}

// ExitShowNamespaces is called when production showNamespaces is exited.
func (s *BaseSqlBaseParserListener) ExitShowNamespaces(ctx *ShowNamespacesContext) {}

// EnterCreateTable is called when production createTable is entered.
func (s *BaseSqlBaseParserListener) EnterCreateTable(ctx *CreateTableContext) {}

// ExitCreateTable is called when production createTable is exited.
func (s *BaseSqlBaseParserListener) ExitCreateTable(ctx *CreateTableContext) {}

// EnterCreateTableLike is called when production createTableLike is entered.
func (s *BaseSqlBaseParserListener) EnterCreateTableLike(ctx *CreateTableLikeContext) {}

// ExitCreateTableLike is called when production createTableLike is exited.
func (s *BaseSqlBaseParserListener) ExitCreateTableLike(ctx *CreateTableLikeContext) {}

// EnterReplaceTable is called when production replaceTable is entered.
func (s *BaseSqlBaseParserListener) EnterReplaceTable(ctx *ReplaceTableContext) {}

// ExitReplaceTable is called when production replaceTable is exited.
func (s *BaseSqlBaseParserListener) ExitReplaceTable(ctx *ReplaceTableContext) {}

// EnterAnalyze is called when production analyze is entered.
func (s *BaseSqlBaseParserListener) EnterAnalyze(ctx *AnalyzeContext) {}

// ExitAnalyze is called when production analyze is exited.
func (s *BaseSqlBaseParserListener) ExitAnalyze(ctx *AnalyzeContext) {}

// EnterAnalyzeTables is called when production analyzeTables is entered.
func (s *BaseSqlBaseParserListener) EnterAnalyzeTables(ctx *AnalyzeTablesContext) {}

// ExitAnalyzeTables is called when production analyzeTables is exited.
func (s *BaseSqlBaseParserListener) ExitAnalyzeTables(ctx *AnalyzeTablesContext) {}

// EnterAddTableColumns is called when production addTableColumns is entered.
func (s *BaseSqlBaseParserListener) EnterAddTableColumns(ctx *AddTableColumnsContext) {}

// ExitAddTableColumns is called when production addTableColumns is exited.
func (s *BaseSqlBaseParserListener) ExitAddTableColumns(ctx *AddTableColumnsContext) {}

// EnterRenameTableColumn is called when production renameTableColumn is entered.
func (s *BaseSqlBaseParserListener) EnterRenameTableColumn(ctx *RenameTableColumnContext) {}

// ExitRenameTableColumn is called when production renameTableColumn is exited.
func (s *BaseSqlBaseParserListener) ExitRenameTableColumn(ctx *RenameTableColumnContext) {}

// EnterDropTableColumns is called when production dropTableColumns is entered.
func (s *BaseSqlBaseParserListener) EnterDropTableColumns(ctx *DropTableColumnsContext) {}

// ExitDropTableColumns is called when production dropTableColumns is exited.
func (s *BaseSqlBaseParserListener) ExitDropTableColumns(ctx *DropTableColumnsContext) {}

// EnterRenameTable is called when production renameTable is entered.
func (s *BaseSqlBaseParserListener) EnterRenameTable(ctx *RenameTableContext) {}

// ExitRenameTable is called when production renameTable is exited.
func (s *BaseSqlBaseParserListener) ExitRenameTable(ctx *RenameTableContext) {}

// EnterSetTableProperties is called when production setTableProperties is entered.
func (s *BaseSqlBaseParserListener) EnterSetTableProperties(ctx *SetTablePropertiesContext) {}

// ExitSetTableProperties is called when production setTableProperties is exited.
func (s *BaseSqlBaseParserListener) ExitSetTableProperties(ctx *SetTablePropertiesContext) {}

// EnterUnsetTableProperties is called when production unsetTableProperties is entered.
func (s *BaseSqlBaseParserListener) EnterUnsetTableProperties(ctx *UnsetTablePropertiesContext) {}

// ExitUnsetTableProperties is called when production unsetTableProperties is exited.
func (s *BaseSqlBaseParserListener) ExitUnsetTableProperties(ctx *UnsetTablePropertiesContext) {}

// EnterAlterTableAlterColumn is called when production alterTableAlterColumn is entered.
func (s *BaseSqlBaseParserListener) EnterAlterTableAlterColumn(ctx *AlterTableAlterColumnContext) {}

// ExitAlterTableAlterColumn is called when production alterTableAlterColumn is exited.
func (s *BaseSqlBaseParserListener) ExitAlterTableAlterColumn(ctx *AlterTableAlterColumnContext) {}

// EnterHiveChangeColumn is called when production hiveChangeColumn is entered.
func (s *BaseSqlBaseParserListener) EnterHiveChangeColumn(ctx *HiveChangeColumnContext) {}

// ExitHiveChangeColumn is called when production hiveChangeColumn is exited.
func (s *BaseSqlBaseParserListener) ExitHiveChangeColumn(ctx *HiveChangeColumnContext) {}

// EnterHiveReplaceColumns is called when production hiveReplaceColumns is entered.
func (s *BaseSqlBaseParserListener) EnterHiveReplaceColumns(ctx *HiveReplaceColumnsContext) {}

// ExitHiveReplaceColumns is called when production hiveReplaceColumns is exited.
func (s *BaseSqlBaseParserListener) ExitHiveReplaceColumns(ctx *HiveReplaceColumnsContext) {}

// EnterSetTableSerDe is called when production setTableSerDe is entered.
func (s *BaseSqlBaseParserListener) EnterSetTableSerDe(ctx *SetTableSerDeContext) {}

// ExitSetTableSerDe is called when production setTableSerDe is exited.
func (s *BaseSqlBaseParserListener) ExitSetTableSerDe(ctx *SetTableSerDeContext) {}

// EnterAddTablePartition is called when production addTablePartition is entered.
func (s *BaseSqlBaseParserListener) EnterAddTablePartition(ctx *AddTablePartitionContext) {}

// ExitAddTablePartition is called when production addTablePartition is exited.
func (s *BaseSqlBaseParserListener) ExitAddTablePartition(ctx *AddTablePartitionContext) {}

// EnterRenameTablePartition is called when production renameTablePartition is entered.
func (s *BaseSqlBaseParserListener) EnterRenameTablePartition(ctx *RenameTablePartitionContext) {}

// ExitRenameTablePartition is called when production renameTablePartition is exited.
func (s *BaseSqlBaseParserListener) ExitRenameTablePartition(ctx *RenameTablePartitionContext) {}

// EnterDropTablePartitions is called when production dropTablePartitions is entered.
func (s *BaseSqlBaseParserListener) EnterDropTablePartitions(ctx *DropTablePartitionsContext) {}

// ExitDropTablePartitions is called when production dropTablePartitions is exited.
func (s *BaseSqlBaseParserListener) ExitDropTablePartitions(ctx *DropTablePartitionsContext) {}

// EnterSetTableLocation is called when production setTableLocation is entered.
func (s *BaseSqlBaseParserListener) EnterSetTableLocation(ctx *SetTableLocationContext) {}

// ExitSetTableLocation is called when production setTableLocation is exited.
func (s *BaseSqlBaseParserListener) ExitSetTableLocation(ctx *SetTableLocationContext) {}

// EnterRecoverPartitions is called when production recoverPartitions is entered.
func (s *BaseSqlBaseParserListener) EnterRecoverPartitions(ctx *RecoverPartitionsContext) {}

// ExitRecoverPartitions is called when production recoverPartitions is exited.
func (s *BaseSqlBaseParserListener) ExitRecoverPartitions(ctx *RecoverPartitionsContext) {}

// EnterAlterClusterBy is called when production alterClusterBy is entered.
func (s *BaseSqlBaseParserListener) EnterAlterClusterBy(ctx *AlterClusterByContext) {}

// ExitAlterClusterBy is called when production alterClusterBy is exited.
func (s *BaseSqlBaseParserListener) ExitAlterClusterBy(ctx *AlterClusterByContext) {}

// EnterAlterTableCollation is called when production alterTableCollation is entered.
func (s *BaseSqlBaseParserListener) EnterAlterTableCollation(ctx *AlterTableCollationContext) {}

// ExitAlterTableCollation is called when production alterTableCollation is exited.
func (s *BaseSqlBaseParserListener) ExitAlterTableCollation(ctx *AlterTableCollationContext) {}

// EnterAddTableConstraint is called when production addTableConstraint is entered.
func (s *BaseSqlBaseParserListener) EnterAddTableConstraint(ctx *AddTableConstraintContext) {}

// ExitAddTableConstraint is called when production addTableConstraint is exited.
func (s *BaseSqlBaseParserListener) ExitAddTableConstraint(ctx *AddTableConstraintContext) {}

// EnterDropTableConstraint is called when production dropTableConstraint is entered.
func (s *BaseSqlBaseParserListener) EnterDropTableConstraint(ctx *DropTableConstraintContext) {}

// ExitDropTableConstraint is called when production dropTableConstraint is exited.
func (s *BaseSqlBaseParserListener) ExitDropTableConstraint(ctx *DropTableConstraintContext) {}

// EnterDropTable is called when production dropTable is entered.
func (s *BaseSqlBaseParserListener) EnterDropTable(ctx *DropTableContext) {}

// ExitDropTable is called when production dropTable is exited.
func (s *BaseSqlBaseParserListener) ExitDropTable(ctx *DropTableContext) {}

// EnterDropView is called when production dropView is entered.
func (s *BaseSqlBaseParserListener) EnterDropView(ctx *DropViewContext) {}

// ExitDropView is called when production dropView is exited.
func (s *BaseSqlBaseParserListener) ExitDropView(ctx *DropViewContext) {}

// EnterCreateView is called when production createView is entered.
func (s *BaseSqlBaseParserListener) EnterCreateView(ctx *CreateViewContext) {}

// ExitCreateView is called when production createView is exited.
func (s *BaseSqlBaseParserListener) ExitCreateView(ctx *CreateViewContext) {}

// EnterCreateTempViewUsing is called when production createTempViewUsing is entered.
func (s *BaseSqlBaseParserListener) EnterCreateTempViewUsing(ctx *CreateTempViewUsingContext) {}

// ExitCreateTempViewUsing is called when production createTempViewUsing is exited.
func (s *BaseSqlBaseParserListener) ExitCreateTempViewUsing(ctx *CreateTempViewUsingContext) {}

// EnterAlterViewQuery is called when production alterViewQuery is entered.
func (s *BaseSqlBaseParserListener) EnterAlterViewQuery(ctx *AlterViewQueryContext) {}

// ExitAlterViewQuery is called when production alterViewQuery is exited.
func (s *BaseSqlBaseParserListener) ExitAlterViewQuery(ctx *AlterViewQueryContext) {}

// EnterAlterViewSchemaBinding is called when production alterViewSchemaBinding is entered.
func (s *BaseSqlBaseParserListener) EnterAlterViewSchemaBinding(ctx *AlterViewSchemaBindingContext) {}

// ExitAlterViewSchemaBinding is called when production alterViewSchemaBinding is exited.
func (s *BaseSqlBaseParserListener) ExitAlterViewSchemaBinding(ctx *AlterViewSchemaBindingContext) {}

// EnterCreateFunction is called when production createFunction is entered.
func (s *BaseSqlBaseParserListener) EnterCreateFunction(ctx *CreateFunctionContext) {}

// ExitCreateFunction is called when production createFunction is exited.
func (s *BaseSqlBaseParserListener) ExitCreateFunction(ctx *CreateFunctionContext) {}

// EnterCreateUserDefinedFunction is called when production createUserDefinedFunction is entered.
func (s *BaseSqlBaseParserListener) EnterCreateUserDefinedFunction(ctx *CreateUserDefinedFunctionContext) {
}

// ExitCreateUserDefinedFunction is called when production createUserDefinedFunction is exited.
func (s *BaseSqlBaseParserListener) ExitCreateUserDefinedFunction(ctx *CreateUserDefinedFunctionContext) {
}

// EnterDropFunction is called when production dropFunction is entered.
func (s *BaseSqlBaseParserListener) EnterDropFunction(ctx *DropFunctionContext) {}

// ExitDropFunction is called when production dropFunction is exited.
func (s *BaseSqlBaseParserListener) ExitDropFunction(ctx *DropFunctionContext) {}

// EnterCreateVariable is called when production createVariable is entered.
func (s *BaseSqlBaseParserListener) EnterCreateVariable(ctx *CreateVariableContext) {}

// ExitCreateVariable is called when production createVariable is exited.
func (s *BaseSqlBaseParserListener) ExitCreateVariable(ctx *CreateVariableContext) {}

// EnterDropVariable is called when production dropVariable is entered.
func (s *BaseSqlBaseParserListener) EnterDropVariable(ctx *DropVariableContext) {}

// ExitDropVariable is called when production dropVariable is exited.
func (s *BaseSqlBaseParserListener) ExitDropVariable(ctx *DropVariableContext) {}

// EnterExplain is called when production explain is entered.
func (s *BaseSqlBaseParserListener) EnterExplain(ctx *ExplainContext) {}

// ExitExplain is called when production explain is exited.
func (s *BaseSqlBaseParserListener) ExitExplain(ctx *ExplainContext) {}

// EnterShowTables is called when production showTables is entered.
func (s *BaseSqlBaseParserListener) EnterShowTables(ctx *ShowTablesContext) {}

// ExitShowTables is called when production showTables is exited.
func (s *BaseSqlBaseParserListener) ExitShowTables(ctx *ShowTablesContext) {}

// EnterShowTableExtended is called when production showTableExtended is entered.
func (s *BaseSqlBaseParserListener) EnterShowTableExtended(ctx *ShowTableExtendedContext) {}

// ExitShowTableExtended is called when production showTableExtended is exited.
func (s *BaseSqlBaseParserListener) ExitShowTableExtended(ctx *ShowTableExtendedContext) {}

// EnterShowTblProperties is called when production showTblProperties is entered.
func (s *BaseSqlBaseParserListener) EnterShowTblProperties(ctx *ShowTblPropertiesContext) {}

// ExitShowTblProperties is called when production showTblProperties is exited.
func (s *BaseSqlBaseParserListener) ExitShowTblProperties(ctx *ShowTblPropertiesContext) {}

// EnterShowColumns is called when production showColumns is entered.
func (s *BaseSqlBaseParserListener) EnterShowColumns(ctx *ShowColumnsContext) {}

// ExitShowColumns is called when production showColumns is exited.
func (s *BaseSqlBaseParserListener) ExitShowColumns(ctx *ShowColumnsContext) {}

// EnterShowViews is called when production showViews is entered.
func (s *BaseSqlBaseParserListener) EnterShowViews(ctx *ShowViewsContext) {}

// ExitShowViews is called when production showViews is exited.
func (s *BaseSqlBaseParserListener) ExitShowViews(ctx *ShowViewsContext) {}

// EnterShowPartitions is called when production showPartitions is entered.
func (s *BaseSqlBaseParserListener) EnterShowPartitions(ctx *ShowPartitionsContext) {}

// ExitShowPartitions is called when production showPartitions is exited.
func (s *BaseSqlBaseParserListener) ExitShowPartitions(ctx *ShowPartitionsContext) {}

// EnterShowFunctions is called when production showFunctions is entered.
func (s *BaseSqlBaseParserListener) EnterShowFunctions(ctx *ShowFunctionsContext) {}

// ExitShowFunctions is called when production showFunctions is exited.
func (s *BaseSqlBaseParserListener) ExitShowFunctions(ctx *ShowFunctionsContext) {}

// EnterShowProcedures is called when production showProcedures is entered.
func (s *BaseSqlBaseParserListener) EnterShowProcedures(ctx *ShowProceduresContext) {}

// ExitShowProcedures is called when production showProcedures is exited.
func (s *BaseSqlBaseParserListener) ExitShowProcedures(ctx *ShowProceduresContext) {}

// EnterShowCreateTable is called when production showCreateTable is entered.
func (s *BaseSqlBaseParserListener) EnterShowCreateTable(ctx *ShowCreateTableContext) {}

// ExitShowCreateTable is called when production showCreateTable is exited.
func (s *BaseSqlBaseParserListener) ExitShowCreateTable(ctx *ShowCreateTableContext) {}

// EnterShowCurrentNamespace is called when production showCurrentNamespace is entered.
func (s *BaseSqlBaseParserListener) EnterShowCurrentNamespace(ctx *ShowCurrentNamespaceContext) {}

// ExitShowCurrentNamespace is called when production showCurrentNamespace is exited.
func (s *BaseSqlBaseParserListener) ExitShowCurrentNamespace(ctx *ShowCurrentNamespaceContext) {}

// EnterShowCatalogs is called when production showCatalogs is entered.
func (s *BaseSqlBaseParserListener) EnterShowCatalogs(ctx *ShowCatalogsContext) {}

// ExitShowCatalogs is called when production showCatalogs is exited.
func (s *BaseSqlBaseParserListener) ExitShowCatalogs(ctx *ShowCatalogsContext) {}

// EnterDescribeFunction is called when production describeFunction is entered.
func (s *BaseSqlBaseParserListener) EnterDescribeFunction(ctx *DescribeFunctionContext) {}

// ExitDescribeFunction is called when production describeFunction is exited.
func (s *BaseSqlBaseParserListener) ExitDescribeFunction(ctx *DescribeFunctionContext) {}

// EnterDescribeProcedure is called when production describeProcedure is entered.
func (s *BaseSqlBaseParserListener) EnterDescribeProcedure(ctx *DescribeProcedureContext) {}

// ExitDescribeProcedure is called when production describeProcedure is exited.
func (s *BaseSqlBaseParserListener) ExitDescribeProcedure(ctx *DescribeProcedureContext) {}

// EnterDescribeNamespace is called when production describeNamespace is entered.
func (s *BaseSqlBaseParserListener) EnterDescribeNamespace(ctx *DescribeNamespaceContext) {}

// ExitDescribeNamespace is called when production describeNamespace is exited.
func (s *BaseSqlBaseParserListener) ExitDescribeNamespace(ctx *DescribeNamespaceContext) {}

// EnterDescribeRelation is called when production describeRelation is entered.
func (s *BaseSqlBaseParserListener) EnterDescribeRelation(ctx *DescribeRelationContext) {}

// ExitDescribeRelation is called when production describeRelation is exited.
func (s *BaseSqlBaseParserListener) ExitDescribeRelation(ctx *DescribeRelationContext) {}

// EnterDescribeQuery is called when production describeQuery is entered.
func (s *BaseSqlBaseParserListener) EnterDescribeQuery(ctx *DescribeQueryContext) {}

// ExitDescribeQuery is called when production describeQuery is exited.
func (s *BaseSqlBaseParserListener) ExitDescribeQuery(ctx *DescribeQueryContext) {}

// EnterCommentNamespace is called when production commentNamespace is entered.
func (s *BaseSqlBaseParserListener) EnterCommentNamespace(ctx *CommentNamespaceContext) {}

// ExitCommentNamespace is called when production commentNamespace is exited.
func (s *BaseSqlBaseParserListener) ExitCommentNamespace(ctx *CommentNamespaceContext) {}

// EnterCommentTable is called when production commentTable is entered.
func (s *BaseSqlBaseParserListener) EnterCommentTable(ctx *CommentTableContext) {}

// ExitCommentTable is called when production commentTable is exited.
func (s *BaseSqlBaseParserListener) ExitCommentTable(ctx *CommentTableContext) {}

// EnterRefreshTable is called when production refreshTable is entered.
func (s *BaseSqlBaseParserListener) EnterRefreshTable(ctx *RefreshTableContext) {}

// ExitRefreshTable is called when production refreshTable is exited.
func (s *BaseSqlBaseParserListener) ExitRefreshTable(ctx *RefreshTableContext) {}

// EnterRefreshFunction is called when production refreshFunction is entered.
func (s *BaseSqlBaseParserListener) EnterRefreshFunction(ctx *RefreshFunctionContext) {}

// ExitRefreshFunction is called when production refreshFunction is exited.
func (s *BaseSqlBaseParserListener) ExitRefreshFunction(ctx *RefreshFunctionContext) {}

// EnterRefreshResource is called when production refreshResource is entered.
func (s *BaseSqlBaseParserListener) EnterRefreshResource(ctx *RefreshResourceContext) {}

// ExitRefreshResource is called when production refreshResource is exited.
func (s *BaseSqlBaseParserListener) ExitRefreshResource(ctx *RefreshResourceContext) {}

// EnterCacheTable is called when production cacheTable is entered.
func (s *BaseSqlBaseParserListener) EnterCacheTable(ctx *CacheTableContext) {}

// ExitCacheTable is called when production cacheTable is exited.
func (s *BaseSqlBaseParserListener) ExitCacheTable(ctx *CacheTableContext) {}

// EnterUncacheTable is called when production uncacheTable is entered.
func (s *BaseSqlBaseParserListener) EnterUncacheTable(ctx *UncacheTableContext) {}

// ExitUncacheTable is called when production uncacheTable is exited.
func (s *BaseSqlBaseParserListener) ExitUncacheTable(ctx *UncacheTableContext) {}

// EnterClearCache is called when production clearCache is entered.
func (s *BaseSqlBaseParserListener) EnterClearCache(ctx *ClearCacheContext) {}

// ExitClearCache is called when production clearCache is exited.
func (s *BaseSqlBaseParserListener) ExitClearCache(ctx *ClearCacheContext) {}

// EnterLoadData is called when production loadData is entered.
func (s *BaseSqlBaseParserListener) EnterLoadData(ctx *LoadDataContext) {}

// ExitLoadData is called when production loadData is exited.
func (s *BaseSqlBaseParserListener) ExitLoadData(ctx *LoadDataContext) {}

// EnterTruncateTable is called when production truncateTable is entered.
func (s *BaseSqlBaseParserListener) EnterTruncateTable(ctx *TruncateTableContext) {}

// ExitTruncateTable is called when production truncateTable is exited.
func (s *BaseSqlBaseParserListener) ExitTruncateTable(ctx *TruncateTableContext) {}

// EnterRepairTable is called when production repairTable is entered.
func (s *BaseSqlBaseParserListener) EnterRepairTable(ctx *RepairTableContext) {}

// ExitRepairTable is called when production repairTable is exited.
func (s *BaseSqlBaseParserListener) ExitRepairTable(ctx *RepairTableContext) {}

// EnterManageResource is called when production manageResource is entered.
func (s *BaseSqlBaseParserListener) EnterManageResource(ctx *ManageResourceContext) {}

// ExitManageResource is called when production manageResource is exited.
func (s *BaseSqlBaseParserListener) ExitManageResource(ctx *ManageResourceContext) {}

// EnterCreateIndex is called when production createIndex is entered.
func (s *BaseSqlBaseParserListener) EnterCreateIndex(ctx *CreateIndexContext) {}

// ExitCreateIndex is called when production createIndex is exited.
func (s *BaseSqlBaseParserListener) ExitCreateIndex(ctx *CreateIndexContext) {}

// EnterDropIndex is called when production dropIndex is entered.
func (s *BaseSqlBaseParserListener) EnterDropIndex(ctx *DropIndexContext) {}

// ExitDropIndex is called when production dropIndex is exited.
func (s *BaseSqlBaseParserListener) ExitDropIndex(ctx *DropIndexContext) {}

// EnterCall is called when production call is entered.
func (s *BaseSqlBaseParserListener) EnterCall(ctx *CallContext) {}

// ExitCall is called when production call is exited.
func (s *BaseSqlBaseParserListener) ExitCall(ctx *CallContext) {}

// EnterFailNativeCommand is called when production failNativeCommand is entered.
func (s *BaseSqlBaseParserListener) EnterFailNativeCommand(ctx *FailNativeCommandContext) {}

// ExitFailNativeCommand is called when production failNativeCommand is exited.
func (s *BaseSqlBaseParserListener) ExitFailNativeCommand(ctx *FailNativeCommandContext) {}

// EnterCreatePipelineDataset is called when production createPipelineDataset is entered.
func (s *BaseSqlBaseParserListener) EnterCreatePipelineDataset(ctx *CreatePipelineDatasetContext) {}

// ExitCreatePipelineDataset is called when production createPipelineDataset is exited.
func (s *BaseSqlBaseParserListener) ExitCreatePipelineDataset(ctx *CreatePipelineDatasetContext) {}

// EnterCreatePipelineInsertIntoFlow is called when production createPipelineInsertIntoFlow is entered.
func (s *BaseSqlBaseParserListener) EnterCreatePipelineInsertIntoFlow(ctx *CreatePipelineInsertIntoFlowContext) {
}

// ExitCreatePipelineInsertIntoFlow is called when production createPipelineInsertIntoFlow is exited.
func (s *BaseSqlBaseParserListener) ExitCreatePipelineInsertIntoFlow(ctx *CreatePipelineInsertIntoFlowContext) {
}

// EnterMaterializedView is called when production materializedView is entered.
func (s *BaseSqlBaseParserListener) EnterMaterializedView(ctx *MaterializedViewContext) {}

// ExitMaterializedView is called when production materializedView is exited.
func (s *BaseSqlBaseParserListener) ExitMaterializedView(ctx *MaterializedViewContext) {}

// EnterStreamingTable is called when production streamingTable is entered.
func (s *BaseSqlBaseParserListener) EnterStreamingTable(ctx *StreamingTableContext) {}

// ExitStreamingTable is called when production streamingTable is exited.
func (s *BaseSqlBaseParserListener) ExitStreamingTable(ctx *StreamingTableContext) {}

// EnterCreatePipelineDatasetHeader is called when production createPipelineDatasetHeader is entered.
func (s *BaseSqlBaseParserListener) EnterCreatePipelineDatasetHeader(ctx *CreatePipelineDatasetHeaderContext) {
}

// ExitCreatePipelineDatasetHeader is called when production createPipelineDatasetHeader is exited.
func (s *BaseSqlBaseParserListener) ExitCreatePipelineDatasetHeader(ctx *CreatePipelineDatasetHeaderContext) {
}

// EnterStreamTableName is called when production streamTableName is entered.
func (s *BaseSqlBaseParserListener) EnterStreamTableName(ctx *StreamTableNameContext) {}

// ExitStreamTableName is called when production streamTableName is exited.
func (s *BaseSqlBaseParserListener) ExitStreamTableName(ctx *StreamTableNameContext) {}

// EnterFailSetRole is called when production failSetRole is entered.
func (s *BaseSqlBaseParserListener) EnterFailSetRole(ctx *FailSetRoleContext) {}

// ExitFailSetRole is called when production failSetRole is exited.
func (s *BaseSqlBaseParserListener) ExitFailSetRole(ctx *FailSetRoleContext) {}

// EnterSetTimeZone is called when production setTimeZone is entered.
func (s *BaseSqlBaseParserListener) EnterSetTimeZone(ctx *SetTimeZoneContext) {}

// ExitSetTimeZone is called when production setTimeZone is exited.
func (s *BaseSqlBaseParserListener) ExitSetTimeZone(ctx *SetTimeZoneContext) {}

// EnterSetVariable is called when production setVariable is entered.
func (s *BaseSqlBaseParserListener) EnterSetVariable(ctx *SetVariableContext) {}

// ExitSetVariable is called when production setVariable is exited.
func (s *BaseSqlBaseParserListener) ExitSetVariable(ctx *SetVariableContext) {}

// EnterSetQuotedConfiguration is called when production setQuotedConfiguration is entered.
func (s *BaseSqlBaseParserListener) EnterSetQuotedConfiguration(ctx *SetQuotedConfigurationContext) {}

// ExitSetQuotedConfiguration is called when production setQuotedConfiguration is exited.
func (s *BaseSqlBaseParserListener) ExitSetQuotedConfiguration(ctx *SetQuotedConfigurationContext) {}

// EnterSetConfiguration is called when production setConfiguration is entered.
func (s *BaseSqlBaseParserListener) EnterSetConfiguration(ctx *SetConfigurationContext) {}

// ExitSetConfiguration is called when production setConfiguration is exited.
func (s *BaseSqlBaseParserListener) ExitSetConfiguration(ctx *SetConfigurationContext) {}

// EnterResetQuotedConfiguration is called when production resetQuotedConfiguration is entered.
func (s *BaseSqlBaseParserListener) EnterResetQuotedConfiguration(ctx *ResetQuotedConfigurationContext) {
}

// ExitResetQuotedConfiguration is called when production resetQuotedConfiguration is exited.
func (s *BaseSqlBaseParserListener) ExitResetQuotedConfiguration(ctx *ResetQuotedConfigurationContext) {
}

// EnterResetConfiguration is called when production resetConfiguration is entered.
func (s *BaseSqlBaseParserListener) EnterResetConfiguration(ctx *ResetConfigurationContext) {}

// ExitResetConfiguration is called when production resetConfiguration is exited.
func (s *BaseSqlBaseParserListener) ExitResetConfiguration(ctx *ResetConfigurationContext) {}

// EnterExecuteImmediate is called when production executeImmediate is entered.
func (s *BaseSqlBaseParserListener) EnterExecuteImmediate(ctx *ExecuteImmediateContext) {}

// ExitExecuteImmediate is called when production executeImmediate is exited.
func (s *BaseSqlBaseParserListener) ExitExecuteImmediate(ctx *ExecuteImmediateContext) {}

// EnterExecuteImmediateUsing is called when production executeImmediateUsing is entered.
func (s *BaseSqlBaseParserListener) EnterExecuteImmediateUsing(ctx *ExecuteImmediateUsingContext) {}

// ExitExecuteImmediateUsing is called when production executeImmediateUsing is exited.
func (s *BaseSqlBaseParserListener) ExitExecuteImmediateUsing(ctx *ExecuteImmediateUsingContext) {}

// EnterTimezone is called when production timezone is entered.
func (s *BaseSqlBaseParserListener) EnterTimezone(ctx *TimezoneContext) {}

// ExitTimezone is called when production timezone is exited.
func (s *BaseSqlBaseParserListener) ExitTimezone(ctx *TimezoneContext) {}

// EnterConfigKey is called when production configKey is entered.
func (s *BaseSqlBaseParserListener) EnterConfigKey(ctx *ConfigKeyContext) {}

// ExitConfigKey is called when production configKey is exited.
func (s *BaseSqlBaseParserListener) ExitConfigKey(ctx *ConfigKeyContext) {}

// EnterConfigValue is called when production configValue is entered.
func (s *BaseSqlBaseParserListener) EnterConfigValue(ctx *ConfigValueContext) {}

// ExitConfigValue is called when production configValue is exited.
func (s *BaseSqlBaseParserListener) ExitConfigValue(ctx *ConfigValueContext) {}

// EnterUnsupportedHiveNativeCommands is called when production unsupportedHiveNativeCommands is entered.
func (s *BaseSqlBaseParserListener) EnterUnsupportedHiveNativeCommands(ctx *UnsupportedHiveNativeCommandsContext) {
}

// ExitUnsupportedHiveNativeCommands is called when production unsupportedHiveNativeCommands is exited.
func (s *BaseSqlBaseParserListener) ExitUnsupportedHiveNativeCommands(ctx *UnsupportedHiveNativeCommandsContext) {
}

// EnterCreateTableHeader is called when production createTableHeader is entered.
func (s *BaseSqlBaseParserListener) EnterCreateTableHeader(ctx *CreateTableHeaderContext) {}

// ExitCreateTableHeader is called when production createTableHeader is exited.
func (s *BaseSqlBaseParserListener) ExitCreateTableHeader(ctx *CreateTableHeaderContext) {}

// EnterReplaceTableHeader is called when production replaceTableHeader is entered.
func (s *BaseSqlBaseParserListener) EnterReplaceTableHeader(ctx *ReplaceTableHeaderContext) {}

// ExitReplaceTableHeader is called when production replaceTableHeader is exited.
func (s *BaseSqlBaseParserListener) ExitReplaceTableHeader(ctx *ReplaceTableHeaderContext) {}

// EnterClusterBySpec is called when production clusterBySpec is entered.
func (s *BaseSqlBaseParserListener) EnterClusterBySpec(ctx *ClusterBySpecContext) {}

// ExitClusterBySpec is called when production clusterBySpec is exited.
func (s *BaseSqlBaseParserListener) ExitClusterBySpec(ctx *ClusterBySpecContext) {}

// EnterBucketSpec is called when production bucketSpec is entered.
func (s *BaseSqlBaseParserListener) EnterBucketSpec(ctx *BucketSpecContext) {}

// ExitBucketSpec is called when production bucketSpec is exited.
func (s *BaseSqlBaseParserListener) ExitBucketSpec(ctx *BucketSpecContext) {}

// EnterSkewSpec is called when production skewSpec is entered.
func (s *BaseSqlBaseParserListener) EnterSkewSpec(ctx *SkewSpecContext) {}

// ExitSkewSpec is called when production skewSpec is exited.
func (s *BaseSqlBaseParserListener) ExitSkewSpec(ctx *SkewSpecContext) {}

// EnterLocationSpec is called when production locationSpec is entered.
func (s *BaseSqlBaseParserListener) EnterLocationSpec(ctx *LocationSpecContext) {}

// ExitLocationSpec is called when production locationSpec is exited.
func (s *BaseSqlBaseParserListener) ExitLocationSpec(ctx *LocationSpecContext) {}

// EnterSchemaBinding is called when production schemaBinding is entered.
func (s *BaseSqlBaseParserListener) EnterSchemaBinding(ctx *SchemaBindingContext) {}

// ExitSchemaBinding is called when production schemaBinding is exited.
func (s *BaseSqlBaseParserListener) ExitSchemaBinding(ctx *SchemaBindingContext) {}

// EnterCommentSpec is called when production commentSpec is entered.
func (s *BaseSqlBaseParserListener) EnterCommentSpec(ctx *CommentSpecContext) {}

// ExitCommentSpec is called when production commentSpec is exited.
func (s *BaseSqlBaseParserListener) ExitCommentSpec(ctx *CommentSpecContext) {}

// EnterSingleQuery is called when production singleQuery is entered.
func (s *BaseSqlBaseParserListener) EnterSingleQuery(ctx *SingleQueryContext) {}

// ExitSingleQuery is called when production singleQuery is exited.
func (s *BaseSqlBaseParserListener) ExitSingleQuery(ctx *SingleQueryContext) {}

// EnterQuery is called when production query is entered.
func (s *BaseSqlBaseParserListener) EnterQuery(ctx *QueryContext) {}

// ExitQuery is called when production query is exited.
func (s *BaseSqlBaseParserListener) ExitQuery(ctx *QueryContext) {}

// EnterInsertOverwriteTable is called when production insertOverwriteTable is entered.
func (s *BaseSqlBaseParserListener) EnterInsertOverwriteTable(ctx *InsertOverwriteTableContext) {}

// ExitInsertOverwriteTable is called when production insertOverwriteTable is exited.
func (s *BaseSqlBaseParserListener) ExitInsertOverwriteTable(ctx *InsertOverwriteTableContext) {}

// EnterInsertIntoTable is called when production insertIntoTable is entered.
func (s *BaseSqlBaseParserListener) EnterInsertIntoTable(ctx *InsertIntoTableContext) {}

// ExitInsertIntoTable is called when production insertIntoTable is exited.
func (s *BaseSqlBaseParserListener) ExitInsertIntoTable(ctx *InsertIntoTableContext) {}

// EnterInsertIntoReplaceWhere is called when production insertIntoReplaceWhere is entered.
func (s *BaseSqlBaseParserListener) EnterInsertIntoReplaceWhere(ctx *InsertIntoReplaceWhereContext) {}

// ExitInsertIntoReplaceWhere is called when production insertIntoReplaceWhere is exited.
func (s *BaseSqlBaseParserListener) ExitInsertIntoReplaceWhere(ctx *InsertIntoReplaceWhereContext) {}

// EnterInsertOverwriteHiveDir is called when production insertOverwriteHiveDir is entered.
func (s *BaseSqlBaseParserListener) EnterInsertOverwriteHiveDir(ctx *InsertOverwriteHiveDirContext) {}

// ExitInsertOverwriteHiveDir is called when production insertOverwriteHiveDir is exited.
func (s *BaseSqlBaseParserListener) ExitInsertOverwriteHiveDir(ctx *InsertOverwriteHiveDirContext) {}

// EnterInsertOverwriteDir is called when production insertOverwriteDir is entered.
func (s *BaseSqlBaseParserListener) EnterInsertOverwriteDir(ctx *InsertOverwriteDirContext) {}

// ExitInsertOverwriteDir is called when production insertOverwriteDir is exited.
func (s *BaseSqlBaseParserListener) ExitInsertOverwriteDir(ctx *InsertOverwriteDirContext) {}

// EnterPartitionSpecLocation is called when production partitionSpecLocation is entered.
func (s *BaseSqlBaseParserListener) EnterPartitionSpecLocation(ctx *PartitionSpecLocationContext) {}

// ExitPartitionSpecLocation is called when production partitionSpecLocation is exited.
func (s *BaseSqlBaseParserListener) ExitPartitionSpecLocation(ctx *PartitionSpecLocationContext) {}

// EnterPartitionSpec is called when production partitionSpec is entered.
func (s *BaseSqlBaseParserListener) EnterPartitionSpec(ctx *PartitionSpecContext) {}

// ExitPartitionSpec is called when production partitionSpec is exited.
func (s *BaseSqlBaseParserListener) ExitPartitionSpec(ctx *PartitionSpecContext) {}

// EnterPartitionVal is called when production partitionVal is entered.
func (s *BaseSqlBaseParserListener) EnterPartitionVal(ctx *PartitionValContext) {}

// ExitPartitionVal is called when production partitionVal is exited.
func (s *BaseSqlBaseParserListener) ExitPartitionVal(ctx *PartitionValContext) {}

// EnterCreatePipelineFlowHeader is called when production createPipelineFlowHeader is entered.
func (s *BaseSqlBaseParserListener) EnterCreatePipelineFlowHeader(ctx *CreatePipelineFlowHeaderContext) {
}

// ExitCreatePipelineFlowHeader is called when production createPipelineFlowHeader is exited.
func (s *BaseSqlBaseParserListener) ExitCreatePipelineFlowHeader(ctx *CreatePipelineFlowHeaderContext) {
}

// EnterNamespace is called when production namespace is entered.
func (s *BaseSqlBaseParserListener) EnterNamespace(ctx *NamespaceContext) {}

// ExitNamespace is called when production namespace is exited.
func (s *BaseSqlBaseParserListener) ExitNamespace(ctx *NamespaceContext) {}

// EnterNamespaces is called when production namespaces is entered.
func (s *BaseSqlBaseParserListener) EnterNamespaces(ctx *NamespacesContext) {}

// ExitNamespaces is called when production namespaces is exited.
func (s *BaseSqlBaseParserListener) ExitNamespaces(ctx *NamespacesContext) {}

// EnterVariable is called when production variable is entered.
func (s *BaseSqlBaseParserListener) EnterVariable(ctx *VariableContext) {}

// ExitVariable is called when production variable is exited.
func (s *BaseSqlBaseParserListener) ExitVariable(ctx *VariableContext) {}

// EnterDescribeFuncName is called when production describeFuncName is entered.
func (s *BaseSqlBaseParserListener) EnterDescribeFuncName(ctx *DescribeFuncNameContext) {}

// ExitDescribeFuncName is called when production describeFuncName is exited.
func (s *BaseSqlBaseParserListener) ExitDescribeFuncName(ctx *DescribeFuncNameContext) {}

// EnterDescribeColName is called when production describeColName is entered.
func (s *BaseSqlBaseParserListener) EnterDescribeColName(ctx *DescribeColNameContext) {}

// ExitDescribeColName is called when production describeColName is exited.
func (s *BaseSqlBaseParserListener) ExitDescribeColName(ctx *DescribeColNameContext) {}

// EnterCtes is called when production ctes is entered.
func (s *BaseSqlBaseParserListener) EnterCtes(ctx *CtesContext) {}

// ExitCtes is called when production ctes is exited.
func (s *BaseSqlBaseParserListener) ExitCtes(ctx *CtesContext) {}

// EnterNamedQuery is called when production namedQuery is entered.
func (s *BaseSqlBaseParserListener) EnterNamedQuery(ctx *NamedQueryContext) {}

// ExitNamedQuery is called when production namedQuery is exited.
func (s *BaseSqlBaseParserListener) ExitNamedQuery(ctx *NamedQueryContext) {}

// EnterTableProvider is called when production tableProvider is entered.
func (s *BaseSqlBaseParserListener) EnterTableProvider(ctx *TableProviderContext) {}

// ExitTableProvider is called when production tableProvider is exited.
func (s *BaseSqlBaseParserListener) ExitTableProvider(ctx *TableProviderContext) {}

// EnterCreateTableClauses is called when production createTableClauses is entered.
func (s *BaseSqlBaseParserListener) EnterCreateTableClauses(ctx *CreateTableClausesContext) {}

// ExitCreateTableClauses is called when production createTableClauses is exited.
func (s *BaseSqlBaseParserListener) ExitCreateTableClauses(ctx *CreateTableClausesContext) {}

// EnterPropertyList is called when production propertyList is entered.
func (s *BaseSqlBaseParserListener) EnterPropertyList(ctx *PropertyListContext) {}

// ExitPropertyList is called when production propertyList is exited.
func (s *BaseSqlBaseParserListener) ExitPropertyList(ctx *PropertyListContext) {}

// EnterPropertyWithKeyAndEquals is called when production propertyWithKeyAndEquals is entered.
func (s *BaseSqlBaseParserListener) EnterPropertyWithKeyAndEquals(ctx *PropertyWithKeyAndEqualsContext) {
}

// ExitPropertyWithKeyAndEquals is called when production propertyWithKeyAndEquals is exited.
func (s *BaseSqlBaseParserListener) ExitPropertyWithKeyAndEquals(ctx *PropertyWithKeyAndEqualsContext) {
}

// EnterPropertyWithKeyNoEquals is called when production propertyWithKeyNoEquals is entered.
func (s *BaseSqlBaseParserListener) EnterPropertyWithKeyNoEquals(ctx *PropertyWithKeyNoEqualsContext) {
}

// ExitPropertyWithKeyNoEquals is called when production propertyWithKeyNoEquals is exited.
func (s *BaseSqlBaseParserListener) ExitPropertyWithKeyNoEquals(ctx *PropertyWithKeyNoEqualsContext) {
}

// EnterPropertyKey is called when production propertyKey is entered.
func (s *BaseSqlBaseParserListener) EnterPropertyKey(ctx *PropertyKeyContext) {}

// ExitPropertyKey is called when production propertyKey is exited.
func (s *BaseSqlBaseParserListener) ExitPropertyKey(ctx *PropertyKeyContext) {}

// EnterPropertyKeyOrStringLit is called when production propertyKeyOrStringLit is entered.
func (s *BaseSqlBaseParserListener) EnterPropertyKeyOrStringLit(ctx *PropertyKeyOrStringLitContext) {}

// ExitPropertyKeyOrStringLit is called when production propertyKeyOrStringLit is exited.
func (s *BaseSqlBaseParserListener) ExitPropertyKeyOrStringLit(ctx *PropertyKeyOrStringLitContext) {}

// EnterPropertyKeyOrStringLitNoCoalesce is called when production propertyKeyOrStringLitNoCoalesce is entered.
func (s *BaseSqlBaseParserListener) EnterPropertyKeyOrStringLitNoCoalesce(ctx *PropertyKeyOrStringLitNoCoalesceContext) {
}

// ExitPropertyKeyOrStringLitNoCoalesce is called when production propertyKeyOrStringLitNoCoalesce is exited.
func (s *BaseSqlBaseParserListener) ExitPropertyKeyOrStringLitNoCoalesce(ctx *PropertyKeyOrStringLitNoCoalesceContext) {
}

// EnterPropertyValue is called when production propertyValue is entered.
func (s *BaseSqlBaseParserListener) EnterPropertyValue(ctx *PropertyValueContext) {}

// ExitPropertyValue is called when production propertyValue is exited.
func (s *BaseSqlBaseParserListener) ExitPropertyValue(ctx *PropertyValueContext) {}

// EnterExpressionPropertyList is called when production expressionPropertyList is entered.
func (s *BaseSqlBaseParserListener) EnterExpressionPropertyList(ctx *ExpressionPropertyListContext) {}

// ExitExpressionPropertyList is called when production expressionPropertyList is exited.
func (s *BaseSqlBaseParserListener) ExitExpressionPropertyList(ctx *ExpressionPropertyListContext) {}

// EnterExpressionPropertyWithKeyAndEquals is called when production expressionPropertyWithKeyAndEquals is entered.
func (s *BaseSqlBaseParserListener) EnterExpressionPropertyWithKeyAndEquals(ctx *ExpressionPropertyWithKeyAndEqualsContext) {
}

// ExitExpressionPropertyWithKeyAndEquals is called when production expressionPropertyWithKeyAndEquals is exited.
func (s *BaseSqlBaseParserListener) ExitExpressionPropertyWithKeyAndEquals(ctx *ExpressionPropertyWithKeyAndEqualsContext) {
}

// EnterExpressionPropertyWithKeyNoEquals is called when production expressionPropertyWithKeyNoEquals is entered.
func (s *BaseSqlBaseParserListener) EnterExpressionPropertyWithKeyNoEquals(ctx *ExpressionPropertyWithKeyNoEqualsContext) {
}

// ExitExpressionPropertyWithKeyNoEquals is called when production expressionPropertyWithKeyNoEquals is exited.
func (s *BaseSqlBaseParserListener) ExitExpressionPropertyWithKeyNoEquals(ctx *ExpressionPropertyWithKeyNoEqualsContext) {
}

// EnterConstantList is called when production constantList is entered.
func (s *BaseSqlBaseParserListener) EnterConstantList(ctx *ConstantListContext) {}

// ExitConstantList is called when production constantList is exited.
func (s *BaseSqlBaseParserListener) ExitConstantList(ctx *ConstantListContext) {}

// EnterNestedConstantList is called when production nestedConstantList is entered.
func (s *BaseSqlBaseParserListener) EnterNestedConstantList(ctx *NestedConstantListContext) {}

// ExitNestedConstantList is called when production nestedConstantList is exited.
func (s *BaseSqlBaseParserListener) ExitNestedConstantList(ctx *NestedConstantListContext) {}

// EnterCreateFileFormat is called when production createFileFormat is entered.
func (s *BaseSqlBaseParserListener) EnterCreateFileFormat(ctx *CreateFileFormatContext) {}

// ExitCreateFileFormat is called when production createFileFormat is exited.
func (s *BaseSqlBaseParserListener) ExitCreateFileFormat(ctx *CreateFileFormatContext) {}

// EnterTableFileFormat is called when production tableFileFormat is entered.
func (s *BaseSqlBaseParserListener) EnterTableFileFormat(ctx *TableFileFormatContext) {}

// ExitTableFileFormat is called when production tableFileFormat is exited.
func (s *BaseSqlBaseParserListener) ExitTableFileFormat(ctx *TableFileFormatContext) {}

// EnterGenericFileFormat is called when production genericFileFormat is entered.
func (s *BaseSqlBaseParserListener) EnterGenericFileFormat(ctx *GenericFileFormatContext) {}

// ExitGenericFileFormat is called when production genericFileFormat is exited.
func (s *BaseSqlBaseParserListener) ExitGenericFileFormat(ctx *GenericFileFormatContext) {}

// EnterStorageHandler is called when production storageHandler is entered.
func (s *BaseSqlBaseParserListener) EnterStorageHandler(ctx *StorageHandlerContext) {}

// ExitStorageHandler is called when production storageHandler is exited.
func (s *BaseSqlBaseParserListener) ExitStorageHandler(ctx *StorageHandlerContext) {}

// EnterResource is called when production resource is entered.
func (s *BaseSqlBaseParserListener) EnterResource(ctx *ResourceContext) {}

// ExitResource is called when production resource is exited.
func (s *BaseSqlBaseParserListener) ExitResource(ctx *ResourceContext) {}

// EnterSingleInsertQuery is called when production singleInsertQuery is entered.
func (s *BaseSqlBaseParserListener) EnterSingleInsertQuery(ctx *SingleInsertQueryContext) {}

// ExitSingleInsertQuery is called when production singleInsertQuery is exited.
func (s *BaseSqlBaseParserListener) ExitSingleInsertQuery(ctx *SingleInsertQueryContext) {}

// EnterMultiInsertQuery is called when production multiInsertQuery is entered.
func (s *BaseSqlBaseParserListener) EnterMultiInsertQuery(ctx *MultiInsertQueryContext) {}

// ExitMultiInsertQuery is called when production multiInsertQuery is exited.
func (s *BaseSqlBaseParserListener) ExitMultiInsertQuery(ctx *MultiInsertQueryContext) {}

// EnterDeleteFromTable is called when production deleteFromTable is entered.
func (s *BaseSqlBaseParserListener) EnterDeleteFromTable(ctx *DeleteFromTableContext) {}

// ExitDeleteFromTable is called when production deleteFromTable is exited.
func (s *BaseSqlBaseParserListener) ExitDeleteFromTable(ctx *DeleteFromTableContext) {}

// EnterUpdateTable is called when production updateTable is entered.
func (s *BaseSqlBaseParserListener) EnterUpdateTable(ctx *UpdateTableContext) {}

// ExitUpdateTable is called when production updateTable is exited.
func (s *BaseSqlBaseParserListener) ExitUpdateTable(ctx *UpdateTableContext) {}

// EnterMergeIntoTable is called when production mergeIntoTable is entered.
func (s *BaseSqlBaseParserListener) EnterMergeIntoTable(ctx *MergeIntoTableContext) {}

// ExitMergeIntoTable is called when production mergeIntoTable is exited.
func (s *BaseSqlBaseParserListener) ExitMergeIntoTable(ctx *MergeIntoTableContext) {}

// EnterIdentifierReference is called when production identifierReference is entered.
func (s *BaseSqlBaseParserListener) EnterIdentifierReference(ctx *IdentifierReferenceContext) {}

// ExitIdentifierReference is called when production identifierReference is exited.
func (s *BaseSqlBaseParserListener) ExitIdentifierReference(ctx *IdentifierReferenceContext) {}

// EnterCatalogIdentifierReference is called when production catalogIdentifierReference is entered.
func (s *BaseSqlBaseParserListener) EnterCatalogIdentifierReference(ctx *CatalogIdentifierReferenceContext) {
}

// ExitCatalogIdentifierReference is called when production catalogIdentifierReference is exited.
func (s *BaseSqlBaseParserListener) ExitCatalogIdentifierReference(ctx *CatalogIdentifierReferenceContext) {
}

// EnterQueryOrganization is called when production queryOrganization is entered.
func (s *BaseSqlBaseParserListener) EnterQueryOrganization(ctx *QueryOrganizationContext) {}

// ExitQueryOrganization is called when production queryOrganization is exited.
func (s *BaseSqlBaseParserListener) ExitQueryOrganization(ctx *QueryOrganizationContext) {}

// EnterMultiInsertQueryBody is called when production multiInsertQueryBody is entered.
func (s *BaseSqlBaseParserListener) EnterMultiInsertQueryBody(ctx *MultiInsertQueryBodyContext) {}

// ExitMultiInsertQueryBody is called when production multiInsertQueryBody is exited.
func (s *BaseSqlBaseParserListener) ExitMultiInsertQueryBody(ctx *MultiInsertQueryBodyContext) {}

// EnterOperatorPipeStatement is called when production operatorPipeStatement is entered.
func (s *BaseSqlBaseParserListener) EnterOperatorPipeStatement(ctx *OperatorPipeStatementContext) {}

// ExitOperatorPipeStatement is called when production operatorPipeStatement is exited.
func (s *BaseSqlBaseParserListener) ExitOperatorPipeStatement(ctx *OperatorPipeStatementContext) {}

// EnterQueryTermDefault is called when production queryTermDefault is entered.
func (s *BaseSqlBaseParserListener) EnterQueryTermDefault(ctx *QueryTermDefaultContext) {}

// ExitQueryTermDefault is called when production queryTermDefault is exited.
func (s *BaseSqlBaseParserListener) ExitQueryTermDefault(ctx *QueryTermDefaultContext) {}

// EnterSetOperation is called when production setOperation is entered.
func (s *BaseSqlBaseParserListener) EnterSetOperation(ctx *SetOperationContext) {}

// ExitSetOperation is called when production setOperation is exited.
func (s *BaseSqlBaseParserListener) ExitSetOperation(ctx *SetOperationContext) {}

// EnterQueryPrimaryDefault is called when production queryPrimaryDefault is entered.
func (s *BaseSqlBaseParserListener) EnterQueryPrimaryDefault(ctx *QueryPrimaryDefaultContext) {}

// ExitQueryPrimaryDefault is called when production queryPrimaryDefault is exited.
func (s *BaseSqlBaseParserListener) ExitQueryPrimaryDefault(ctx *QueryPrimaryDefaultContext) {}

// EnterFromStmt is called when production fromStmt is entered.
func (s *BaseSqlBaseParserListener) EnterFromStmt(ctx *FromStmtContext) {}

// ExitFromStmt is called when production fromStmt is exited.
func (s *BaseSqlBaseParserListener) ExitFromStmt(ctx *FromStmtContext) {}

// EnterTable is called when production table is entered.
func (s *BaseSqlBaseParserListener) EnterTable(ctx *TableContext) {}

// ExitTable is called when production table is exited.
func (s *BaseSqlBaseParserListener) ExitTable(ctx *TableContext) {}

// EnterInlineTableDefault1 is called when production inlineTableDefault1 is entered.
func (s *BaseSqlBaseParserListener) EnterInlineTableDefault1(ctx *InlineTableDefault1Context) {}

// ExitInlineTableDefault1 is called when production inlineTableDefault1 is exited.
func (s *BaseSqlBaseParserListener) ExitInlineTableDefault1(ctx *InlineTableDefault1Context) {}

// EnterSubquery is called when production subquery is entered.
func (s *BaseSqlBaseParserListener) EnterSubquery(ctx *SubqueryContext) {}

// ExitSubquery is called when production subquery is exited.
func (s *BaseSqlBaseParserListener) ExitSubquery(ctx *SubqueryContext) {}

// EnterSortItem is called when production sortItem is entered.
func (s *BaseSqlBaseParserListener) EnterSortItem(ctx *SortItemContext) {}

// ExitSortItem is called when production sortItem is exited.
func (s *BaseSqlBaseParserListener) ExitSortItem(ctx *SortItemContext) {}

// EnterFromStatement is called when production fromStatement is entered.
func (s *BaseSqlBaseParserListener) EnterFromStatement(ctx *FromStatementContext) {}

// ExitFromStatement is called when production fromStatement is exited.
func (s *BaseSqlBaseParserListener) ExitFromStatement(ctx *FromStatementContext) {}

// EnterFromStatementBody is called when production fromStatementBody is entered.
func (s *BaseSqlBaseParserListener) EnterFromStatementBody(ctx *FromStatementBodyContext) {}

// ExitFromStatementBody is called when production fromStatementBody is exited.
func (s *BaseSqlBaseParserListener) ExitFromStatementBody(ctx *FromStatementBodyContext) {}

// EnterTransformQuerySpecification is called when production transformQuerySpecification is entered.
func (s *BaseSqlBaseParserListener) EnterTransformQuerySpecification(ctx *TransformQuerySpecificationContext) {
}

// ExitTransformQuerySpecification is called when production transformQuerySpecification is exited.
func (s *BaseSqlBaseParserListener) ExitTransformQuerySpecification(ctx *TransformQuerySpecificationContext) {
}

// EnterRegularQuerySpecification is called when production regularQuerySpecification is entered.
func (s *BaseSqlBaseParserListener) EnterRegularQuerySpecification(ctx *RegularQuerySpecificationContext) {
}

// ExitRegularQuerySpecification is called when production regularQuerySpecification is exited.
func (s *BaseSqlBaseParserListener) ExitRegularQuerySpecification(ctx *RegularQuerySpecificationContext) {
}

// EnterTransformClause is called when production transformClause is entered.
func (s *BaseSqlBaseParserListener) EnterTransformClause(ctx *TransformClauseContext) {}

// ExitTransformClause is called when production transformClause is exited.
func (s *BaseSqlBaseParserListener) ExitTransformClause(ctx *TransformClauseContext) {}

// EnterSelectClause is called when production selectClause is entered.
func (s *BaseSqlBaseParserListener) EnterSelectClause(ctx *SelectClauseContext) {}

// ExitSelectClause is called when production selectClause is exited.
func (s *BaseSqlBaseParserListener) ExitSelectClause(ctx *SelectClauseContext) {}

// EnterSetClause is called when production setClause is entered.
func (s *BaseSqlBaseParserListener) EnterSetClause(ctx *SetClauseContext) {}

// ExitSetClause is called when production setClause is exited.
func (s *BaseSqlBaseParserListener) ExitSetClause(ctx *SetClauseContext) {}

// EnterMatchedClause is called when production matchedClause is entered.
func (s *BaseSqlBaseParserListener) EnterMatchedClause(ctx *MatchedClauseContext) {}

// ExitMatchedClause is called when production matchedClause is exited.
func (s *BaseSqlBaseParserListener) ExitMatchedClause(ctx *MatchedClauseContext) {}

// EnterNotMatchedClause is called when production notMatchedClause is entered.
func (s *BaseSqlBaseParserListener) EnterNotMatchedClause(ctx *NotMatchedClauseContext) {}

// ExitNotMatchedClause is called when production notMatchedClause is exited.
func (s *BaseSqlBaseParserListener) ExitNotMatchedClause(ctx *NotMatchedClauseContext) {}

// EnterNotMatchedBySourceClause is called when production notMatchedBySourceClause is entered.
func (s *BaseSqlBaseParserListener) EnterNotMatchedBySourceClause(ctx *NotMatchedBySourceClauseContext) {
}

// ExitNotMatchedBySourceClause is called when production notMatchedBySourceClause is exited.
func (s *BaseSqlBaseParserListener) ExitNotMatchedBySourceClause(ctx *NotMatchedBySourceClauseContext) {
}

// EnterMatchedAction is called when production matchedAction is entered.
func (s *BaseSqlBaseParserListener) EnterMatchedAction(ctx *MatchedActionContext) {}

// ExitMatchedAction is called when production matchedAction is exited.
func (s *BaseSqlBaseParserListener) ExitMatchedAction(ctx *MatchedActionContext) {}

// EnterNotMatchedAction is called when production notMatchedAction is entered.
func (s *BaseSqlBaseParserListener) EnterNotMatchedAction(ctx *NotMatchedActionContext) {}

// ExitNotMatchedAction is called when production notMatchedAction is exited.
func (s *BaseSqlBaseParserListener) ExitNotMatchedAction(ctx *NotMatchedActionContext) {}

// EnterNotMatchedBySourceAction is called when production notMatchedBySourceAction is entered.
func (s *BaseSqlBaseParserListener) EnterNotMatchedBySourceAction(ctx *NotMatchedBySourceActionContext) {
}

// ExitNotMatchedBySourceAction is called when production notMatchedBySourceAction is exited.
func (s *BaseSqlBaseParserListener) ExitNotMatchedBySourceAction(ctx *NotMatchedBySourceActionContext) {
}

// EnterExceptClause is called when production exceptClause is entered.
func (s *BaseSqlBaseParserListener) EnterExceptClause(ctx *ExceptClauseContext) {}

// ExitExceptClause is called when production exceptClause is exited.
func (s *BaseSqlBaseParserListener) ExitExceptClause(ctx *ExceptClauseContext) {}

// EnterAssignmentList is called when production assignmentList is entered.
func (s *BaseSqlBaseParserListener) EnterAssignmentList(ctx *AssignmentListContext) {}

// ExitAssignmentList is called when production assignmentList is exited.
func (s *BaseSqlBaseParserListener) ExitAssignmentList(ctx *AssignmentListContext) {}

// EnterAssignment is called when production assignment is entered.
func (s *BaseSqlBaseParserListener) EnterAssignment(ctx *AssignmentContext) {}

// ExitAssignment is called when production assignment is exited.
func (s *BaseSqlBaseParserListener) ExitAssignment(ctx *AssignmentContext) {}

// EnterWhereClause is called when production whereClause is entered.
func (s *BaseSqlBaseParserListener) EnterWhereClause(ctx *WhereClauseContext) {}

// ExitWhereClause is called when production whereClause is exited.
func (s *BaseSqlBaseParserListener) ExitWhereClause(ctx *WhereClauseContext) {}

// EnterHavingClause is called when production havingClause is entered.
func (s *BaseSqlBaseParserListener) EnterHavingClause(ctx *HavingClauseContext) {}

// ExitHavingClause is called when production havingClause is exited.
func (s *BaseSqlBaseParserListener) ExitHavingClause(ctx *HavingClauseContext) {}

// EnterHint is called when production hint is entered.
func (s *BaseSqlBaseParserListener) EnterHint(ctx *HintContext) {}

// ExitHint is called when production hint is exited.
func (s *BaseSqlBaseParserListener) ExitHint(ctx *HintContext) {}

// EnterHintStatement is called when production hintStatement is entered.
func (s *BaseSqlBaseParserListener) EnterHintStatement(ctx *HintStatementContext) {}

// ExitHintStatement is called when production hintStatement is exited.
func (s *BaseSqlBaseParserListener) ExitHintStatement(ctx *HintStatementContext) {}

// EnterFromClause is called when production fromClause is entered.
func (s *BaseSqlBaseParserListener) EnterFromClause(ctx *FromClauseContext) {}

// ExitFromClause is called when production fromClause is exited.
func (s *BaseSqlBaseParserListener) ExitFromClause(ctx *FromClauseContext) {}

// EnterTemporalClause is called when production temporalClause is entered.
func (s *BaseSqlBaseParserListener) EnterTemporalClause(ctx *TemporalClauseContext) {}

// ExitTemporalClause is called when production temporalClause is exited.
func (s *BaseSqlBaseParserListener) ExitTemporalClause(ctx *TemporalClauseContext) {}

// EnterAggregationClause is called when production aggregationClause is entered.
func (s *BaseSqlBaseParserListener) EnterAggregationClause(ctx *AggregationClauseContext) {}

// ExitAggregationClause is called when production aggregationClause is exited.
func (s *BaseSqlBaseParserListener) ExitAggregationClause(ctx *AggregationClauseContext) {}

// EnterGroupByClause is called when production groupByClause is entered.
func (s *BaseSqlBaseParserListener) EnterGroupByClause(ctx *GroupByClauseContext) {}

// ExitGroupByClause is called when production groupByClause is exited.
func (s *BaseSqlBaseParserListener) ExitGroupByClause(ctx *GroupByClauseContext) {}

// EnterGroupingAnalytics is called when production groupingAnalytics is entered.
func (s *BaseSqlBaseParserListener) EnterGroupingAnalytics(ctx *GroupingAnalyticsContext) {}

// ExitGroupingAnalytics is called when production groupingAnalytics is exited.
func (s *BaseSqlBaseParserListener) ExitGroupingAnalytics(ctx *GroupingAnalyticsContext) {}

// EnterGroupingElement is called when production groupingElement is entered.
func (s *BaseSqlBaseParserListener) EnterGroupingElement(ctx *GroupingElementContext) {}

// ExitGroupingElement is called when production groupingElement is exited.
func (s *BaseSqlBaseParserListener) ExitGroupingElement(ctx *GroupingElementContext) {}

// EnterGroupingSet is called when production groupingSet is entered.
func (s *BaseSqlBaseParserListener) EnterGroupingSet(ctx *GroupingSetContext) {}

// ExitGroupingSet is called when production groupingSet is exited.
func (s *BaseSqlBaseParserListener) ExitGroupingSet(ctx *GroupingSetContext) {}

// EnterPivotClause is called when production pivotClause is entered.
func (s *BaseSqlBaseParserListener) EnterPivotClause(ctx *PivotClauseContext) {}

// ExitPivotClause is called when production pivotClause is exited.
func (s *BaseSqlBaseParserListener) ExitPivotClause(ctx *PivotClauseContext) {}

// EnterPivotColumn is called when production pivotColumn is entered.
func (s *BaseSqlBaseParserListener) EnterPivotColumn(ctx *PivotColumnContext) {}

// ExitPivotColumn is called when production pivotColumn is exited.
func (s *BaseSqlBaseParserListener) ExitPivotColumn(ctx *PivotColumnContext) {}

// EnterPivotValue is called when production pivotValue is entered.
func (s *BaseSqlBaseParserListener) EnterPivotValue(ctx *PivotValueContext) {}

// ExitPivotValue is called when production pivotValue is exited.
func (s *BaseSqlBaseParserListener) ExitPivotValue(ctx *PivotValueContext) {}

// EnterUnpivotClause is called when production unpivotClause is entered.
func (s *BaseSqlBaseParserListener) EnterUnpivotClause(ctx *UnpivotClauseContext) {}

// ExitUnpivotClause is called when production unpivotClause is exited.
func (s *BaseSqlBaseParserListener) ExitUnpivotClause(ctx *UnpivotClauseContext) {}

// EnterUnpivotNullClause is called when production unpivotNullClause is entered.
func (s *BaseSqlBaseParserListener) EnterUnpivotNullClause(ctx *UnpivotNullClauseContext) {}

// ExitUnpivotNullClause is called when production unpivotNullClause is exited.
func (s *BaseSqlBaseParserListener) ExitUnpivotNullClause(ctx *UnpivotNullClauseContext) {}

// EnterUnpivotOperator is called when production unpivotOperator is entered.
func (s *BaseSqlBaseParserListener) EnterUnpivotOperator(ctx *UnpivotOperatorContext) {}

// ExitUnpivotOperator is called when production unpivotOperator is exited.
func (s *BaseSqlBaseParserListener) ExitUnpivotOperator(ctx *UnpivotOperatorContext) {}

// EnterUnpivotSingleValueColumnClause is called when production unpivotSingleValueColumnClause is entered.
func (s *BaseSqlBaseParserListener) EnterUnpivotSingleValueColumnClause(ctx *UnpivotSingleValueColumnClauseContext) {
}

// ExitUnpivotSingleValueColumnClause is called when production unpivotSingleValueColumnClause is exited.
func (s *BaseSqlBaseParserListener) ExitUnpivotSingleValueColumnClause(ctx *UnpivotSingleValueColumnClauseContext) {
}

// EnterUnpivotMultiValueColumnClause is called when production unpivotMultiValueColumnClause is entered.
func (s *BaseSqlBaseParserListener) EnterUnpivotMultiValueColumnClause(ctx *UnpivotMultiValueColumnClauseContext) {
}

// ExitUnpivotMultiValueColumnClause is called when production unpivotMultiValueColumnClause is exited.
func (s *BaseSqlBaseParserListener) ExitUnpivotMultiValueColumnClause(ctx *UnpivotMultiValueColumnClauseContext) {
}

// EnterUnpivotColumnSet is called when production unpivotColumnSet is entered.
func (s *BaseSqlBaseParserListener) EnterUnpivotColumnSet(ctx *UnpivotColumnSetContext) {}

// ExitUnpivotColumnSet is called when production unpivotColumnSet is exited.
func (s *BaseSqlBaseParserListener) ExitUnpivotColumnSet(ctx *UnpivotColumnSetContext) {}

// EnterUnpivotValueColumn is called when production unpivotValueColumn is entered.
func (s *BaseSqlBaseParserListener) EnterUnpivotValueColumn(ctx *UnpivotValueColumnContext) {}

// ExitUnpivotValueColumn is called when production unpivotValueColumn is exited.
func (s *BaseSqlBaseParserListener) ExitUnpivotValueColumn(ctx *UnpivotValueColumnContext) {}

// EnterUnpivotNameColumn is called when production unpivotNameColumn is entered.
func (s *BaseSqlBaseParserListener) EnterUnpivotNameColumn(ctx *UnpivotNameColumnContext) {}

// ExitUnpivotNameColumn is called when production unpivotNameColumn is exited.
func (s *BaseSqlBaseParserListener) ExitUnpivotNameColumn(ctx *UnpivotNameColumnContext) {}

// EnterUnpivotColumnAndAlias is called when production unpivotColumnAndAlias is entered.
func (s *BaseSqlBaseParserListener) EnterUnpivotColumnAndAlias(ctx *UnpivotColumnAndAliasContext) {}

// ExitUnpivotColumnAndAlias is called when production unpivotColumnAndAlias is exited.
func (s *BaseSqlBaseParserListener) ExitUnpivotColumnAndAlias(ctx *UnpivotColumnAndAliasContext) {}

// EnterUnpivotColumn is called when production unpivotColumn is entered.
func (s *BaseSqlBaseParserListener) EnterUnpivotColumn(ctx *UnpivotColumnContext) {}

// ExitUnpivotColumn is called when production unpivotColumn is exited.
func (s *BaseSqlBaseParserListener) ExitUnpivotColumn(ctx *UnpivotColumnContext) {}

// EnterUnpivotAlias is called when production unpivotAlias is entered.
func (s *BaseSqlBaseParserListener) EnterUnpivotAlias(ctx *UnpivotAliasContext) {}

// ExitUnpivotAlias is called when production unpivotAlias is exited.
func (s *BaseSqlBaseParserListener) ExitUnpivotAlias(ctx *UnpivotAliasContext) {}

// EnterLateralView is called when production lateralView is entered.
func (s *BaseSqlBaseParserListener) EnterLateralView(ctx *LateralViewContext) {}

// ExitLateralView is called when production lateralView is exited.
func (s *BaseSqlBaseParserListener) ExitLateralView(ctx *LateralViewContext) {}

// EnterWatermarkClause is called when production watermarkClause is entered.
func (s *BaseSqlBaseParserListener) EnterWatermarkClause(ctx *WatermarkClauseContext) {}

// ExitWatermarkClause is called when production watermarkClause is exited.
func (s *BaseSqlBaseParserListener) ExitWatermarkClause(ctx *WatermarkClauseContext) {}

// EnterSetQuantifier is called when production setQuantifier is entered.
func (s *BaseSqlBaseParserListener) EnterSetQuantifier(ctx *SetQuantifierContext) {}

// ExitSetQuantifier is called when production setQuantifier is exited.
func (s *BaseSqlBaseParserListener) ExitSetQuantifier(ctx *SetQuantifierContext) {}

// EnterRelation is called when production relation is entered.
func (s *BaseSqlBaseParserListener) EnterRelation(ctx *RelationContext) {}

// ExitRelation is called when production relation is exited.
func (s *BaseSqlBaseParserListener) ExitRelation(ctx *RelationContext) {}

// EnterRelationExtension is called when production relationExtension is entered.
func (s *BaseSqlBaseParserListener) EnterRelationExtension(ctx *RelationExtensionContext) {}

// ExitRelationExtension is called when production relationExtension is exited.
func (s *BaseSqlBaseParserListener) ExitRelationExtension(ctx *RelationExtensionContext) {}

// EnterJoinRelation is called when production joinRelation is entered.
func (s *BaseSqlBaseParserListener) EnterJoinRelation(ctx *JoinRelationContext) {}

// ExitJoinRelation is called when production joinRelation is exited.
func (s *BaseSqlBaseParserListener) ExitJoinRelation(ctx *JoinRelationContext) {}

// EnterJoinType is called when production joinType is entered.
func (s *BaseSqlBaseParserListener) EnterJoinType(ctx *JoinTypeContext) {}

// ExitJoinType is called when production joinType is exited.
func (s *BaseSqlBaseParserListener) ExitJoinType(ctx *JoinTypeContext) {}

// EnterJoinCriteria is called when production joinCriteria is entered.
func (s *BaseSqlBaseParserListener) EnterJoinCriteria(ctx *JoinCriteriaContext) {}

// ExitJoinCriteria is called when production joinCriteria is exited.
func (s *BaseSqlBaseParserListener) ExitJoinCriteria(ctx *JoinCriteriaContext) {}

// EnterSample is called when production sample is entered.
func (s *BaseSqlBaseParserListener) EnterSample(ctx *SampleContext) {}

// ExitSample is called when production sample is exited.
func (s *BaseSqlBaseParserListener) ExitSample(ctx *SampleContext) {}

// EnterSampleByPercentile is called when production sampleByPercentile is entered.
func (s *BaseSqlBaseParserListener) EnterSampleByPercentile(ctx *SampleByPercentileContext) {}

// ExitSampleByPercentile is called when production sampleByPercentile is exited.
func (s *BaseSqlBaseParserListener) ExitSampleByPercentile(ctx *SampleByPercentileContext) {}

// EnterSampleByRows is called when production sampleByRows is entered.
func (s *BaseSqlBaseParserListener) EnterSampleByRows(ctx *SampleByRowsContext) {}

// ExitSampleByRows is called when production sampleByRows is exited.
func (s *BaseSqlBaseParserListener) ExitSampleByRows(ctx *SampleByRowsContext) {}

// EnterSampleByBucket is called when production sampleByBucket is entered.
func (s *BaseSqlBaseParserListener) EnterSampleByBucket(ctx *SampleByBucketContext) {}

// ExitSampleByBucket is called when production sampleByBucket is exited.
func (s *BaseSqlBaseParserListener) ExitSampleByBucket(ctx *SampleByBucketContext) {}

// EnterSampleByBytes is called when production sampleByBytes is entered.
func (s *BaseSqlBaseParserListener) EnterSampleByBytes(ctx *SampleByBytesContext) {}

// ExitSampleByBytes is called when production sampleByBytes is exited.
func (s *BaseSqlBaseParserListener) ExitSampleByBytes(ctx *SampleByBytesContext) {}

// EnterIdentifierList is called when production identifierList is entered.
func (s *BaseSqlBaseParserListener) EnterIdentifierList(ctx *IdentifierListContext) {}

// ExitIdentifierList is called when production identifierList is exited.
func (s *BaseSqlBaseParserListener) ExitIdentifierList(ctx *IdentifierListContext) {}

// EnterIdentifierSeq is called when production identifierSeq is entered.
func (s *BaseSqlBaseParserListener) EnterIdentifierSeq(ctx *IdentifierSeqContext) {}

// ExitIdentifierSeq is called when production identifierSeq is exited.
func (s *BaseSqlBaseParserListener) ExitIdentifierSeq(ctx *IdentifierSeqContext) {}

// EnterOrderedIdentifierList is called when production orderedIdentifierList is entered.
func (s *BaseSqlBaseParserListener) EnterOrderedIdentifierList(ctx *OrderedIdentifierListContext) {}

// ExitOrderedIdentifierList is called when production orderedIdentifierList is exited.
func (s *BaseSqlBaseParserListener) ExitOrderedIdentifierList(ctx *OrderedIdentifierListContext) {}

// EnterOrderedIdentifier is called when production orderedIdentifier is entered.
func (s *BaseSqlBaseParserListener) EnterOrderedIdentifier(ctx *OrderedIdentifierContext) {}

// ExitOrderedIdentifier is called when production orderedIdentifier is exited.
func (s *BaseSqlBaseParserListener) ExitOrderedIdentifier(ctx *OrderedIdentifierContext) {}

// EnterIdentifierCommentList is called when production identifierCommentList is entered.
func (s *BaseSqlBaseParserListener) EnterIdentifierCommentList(ctx *IdentifierCommentListContext) {}

// ExitIdentifierCommentList is called when production identifierCommentList is exited.
func (s *BaseSqlBaseParserListener) ExitIdentifierCommentList(ctx *IdentifierCommentListContext) {}

// EnterIdentifierComment is called when production identifierComment is entered.
func (s *BaseSqlBaseParserListener) EnterIdentifierComment(ctx *IdentifierCommentContext) {}

// ExitIdentifierComment is called when production identifierComment is exited.
func (s *BaseSqlBaseParserListener) ExitIdentifierComment(ctx *IdentifierCommentContext) {}

// EnterStreamRelation is called when production streamRelation is entered.
func (s *BaseSqlBaseParserListener) EnterStreamRelation(ctx *StreamRelationContext) {}

// ExitStreamRelation is called when production streamRelation is exited.
func (s *BaseSqlBaseParserListener) ExitStreamRelation(ctx *StreamRelationContext) {}

// EnterTableName is called when production tableName is entered.
func (s *BaseSqlBaseParserListener) EnterTableName(ctx *TableNameContext) {}

// ExitTableName is called when production tableName is exited.
func (s *BaseSqlBaseParserListener) ExitTableName(ctx *TableNameContext) {}

// EnterAliasedQuery is called when production aliasedQuery is entered.
func (s *BaseSqlBaseParserListener) EnterAliasedQuery(ctx *AliasedQueryContext) {}

// ExitAliasedQuery is called when production aliasedQuery is exited.
func (s *BaseSqlBaseParserListener) ExitAliasedQuery(ctx *AliasedQueryContext) {}

// EnterAliasedRelation is called when production aliasedRelation is entered.
func (s *BaseSqlBaseParserListener) EnterAliasedRelation(ctx *AliasedRelationContext) {}

// ExitAliasedRelation is called when production aliasedRelation is exited.
func (s *BaseSqlBaseParserListener) ExitAliasedRelation(ctx *AliasedRelationContext) {}

// EnterInlineTableDefault2 is called when production inlineTableDefault2 is entered.
func (s *BaseSqlBaseParserListener) EnterInlineTableDefault2(ctx *InlineTableDefault2Context) {}

// ExitInlineTableDefault2 is called when production inlineTableDefault2 is exited.
func (s *BaseSqlBaseParserListener) ExitInlineTableDefault2(ctx *InlineTableDefault2Context) {}

// EnterTableValuedFunction is called when production tableValuedFunction is entered.
func (s *BaseSqlBaseParserListener) EnterTableValuedFunction(ctx *TableValuedFunctionContext) {}

// ExitTableValuedFunction is called when production tableValuedFunction is exited.
func (s *BaseSqlBaseParserListener) ExitTableValuedFunction(ctx *TableValuedFunctionContext) {}

// EnterOptionsClause is called when production optionsClause is entered.
func (s *BaseSqlBaseParserListener) EnterOptionsClause(ctx *OptionsClauseContext) {}

// ExitOptionsClause is called when production optionsClause is exited.
func (s *BaseSqlBaseParserListener) ExitOptionsClause(ctx *OptionsClauseContext) {}

// EnterInlineTable is called when production inlineTable is entered.
func (s *BaseSqlBaseParserListener) EnterInlineTable(ctx *InlineTableContext) {}

// ExitInlineTable is called when production inlineTable is exited.
func (s *BaseSqlBaseParserListener) ExitInlineTable(ctx *InlineTableContext) {}

// EnterFunctionTableSubqueryArgument is called when production functionTableSubqueryArgument is entered.
func (s *BaseSqlBaseParserListener) EnterFunctionTableSubqueryArgument(ctx *FunctionTableSubqueryArgumentContext) {
}

// ExitFunctionTableSubqueryArgument is called when production functionTableSubqueryArgument is exited.
func (s *BaseSqlBaseParserListener) ExitFunctionTableSubqueryArgument(ctx *FunctionTableSubqueryArgumentContext) {
}

// EnterTableArgumentPartitioning is called when production tableArgumentPartitioning is entered.
func (s *BaseSqlBaseParserListener) EnterTableArgumentPartitioning(ctx *TableArgumentPartitioningContext) {
}

// ExitTableArgumentPartitioning is called when production tableArgumentPartitioning is exited.
func (s *BaseSqlBaseParserListener) ExitTableArgumentPartitioning(ctx *TableArgumentPartitioningContext) {
}

// EnterFunctionTableNamedArgumentExpression is called when production functionTableNamedArgumentExpression is entered.
func (s *BaseSqlBaseParserListener) EnterFunctionTableNamedArgumentExpression(ctx *FunctionTableNamedArgumentExpressionContext) {
}

// ExitFunctionTableNamedArgumentExpression is called when production functionTableNamedArgumentExpression is exited.
func (s *BaseSqlBaseParserListener) ExitFunctionTableNamedArgumentExpression(ctx *FunctionTableNamedArgumentExpressionContext) {
}

// EnterFunctionTableReferenceArgument is called when production functionTableReferenceArgument is entered.
func (s *BaseSqlBaseParserListener) EnterFunctionTableReferenceArgument(ctx *FunctionTableReferenceArgumentContext) {
}

// ExitFunctionTableReferenceArgument is called when production functionTableReferenceArgument is exited.
func (s *BaseSqlBaseParserListener) ExitFunctionTableReferenceArgument(ctx *FunctionTableReferenceArgumentContext) {
}

// EnterFunctionTableArgument is called when production functionTableArgument is entered.
func (s *BaseSqlBaseParserListener) EnterFunctionTableArgument(ctx *FunctionTableArgumentContext) {}

// ExitFunctionTableArgument is called when production functionTableArgument is exited.
func (s *BaseSqlBaseParserListener) ExitFunctionTableArgument(ctx *FunctionTableArgumentContext) {}

// EnterFunctionTable is called when production functionTable is entered.
func (s *BaseSqlBaseParserListener) EnterFunctionTable(ctx *FunctionTableContext) {}

// ExitFunctionTable is called when production functionTable is exited.
func (s *BaseSqlBaseParserListener) ExitFunctionTable(ctx *FunctionTableContext) {}

// EnterTableAlias is called when production tableAlias is entered.
func (s *BaseSqlBaseParserListener) EnterTableAlias(ctx *TableAliasContext) {}

// ExitTableAlias is called when production tableAlias is exited.
func (s *BaseSqlBaseParserListener) ExitTableAlias(ctx *TableAliasContext) {}

// EnterRowFormatSerde is called when production rowFormatSerde is entered.
func (s *BaseSqlBaseParserListener) EnterRowFormatSerde(ctx *RowFormatSerdeContext) {}

// ExitRowFormatSerde is called when production rowFormatSerde is exited.
func (s *BaseSqlBaseParserListener) ExitRowFormatSerde(ctx *RowFormatSerdeContext) {}

// EnterRowFormatDelimited is called when production rowFormatDelimited is entered.
func (s *BaseSqlBaseParserListener) EnterRowFormatDelimited(ctx *RowFormatDelimitedContext) {}

// ExitRowFormatDelimited is called when production rowFormatDelimited is exited.
func (s *BaseSqlBaseParserListener) ExitRowFormatDelimited(ctx *RowFormatDelimitedContext) {}

// EnterMultipartIdentifierList is called when production multipartIdentifierList is entered.
func (s *BaseSqlBaseParserListener) EnterMultipartIdentifierList(ctx *MultipartIdentifierListContext) {
}

// ExitMultipartIdentifierList is called when production multipartIdentifierList is exited.
func (s *BaseSqlBaseParserListener) ExitMultipartIdentifierList(ctx *MultipartIdentifierListContext) {
}

// EnterMultipartIdentifier is called when production multipartIdentifier is entered.
func (s *BaseSqlBaseParserListener) EnterMultipartIdentifier(ctx *MultipartIdentifierContext) {}

// ExitMultipartIdentifier is called when production multipartIdentifier is exited.
func (s *BaseSqlBaseParserListener) ExitMultipartIdentifier(ctx *MultipartIdentifierContext) {}

// EnterMultipartIdentifierPropertyList is called when production multipartIdentifierPropertyList is entered.
func (s *BaseSqlBaseParserListener) EnterMultipartIdentifierPropertyList(ctx *MultipartIdentifierPropertyListContext) {
}

// ExitMultipartIdentifierPropertyList is called when production multipartIdentifierPropertyList is exited.
func (s *BaseSqlBaseParserListener) ExitMultipartIdentifierPropertyList(ctx *MultipartIdentifierPropertyListContext) {
}

// EnterMultipartIdentifierProperty is called when production multipartIdentifierProperty is entered.
func (s *BaseSqlBaseParserListener) EnterMultipartIdentifierProperty(ctx *MultipartIdentifierPropertyContext) {
}

// ExitMultipartIdentifierProperty is called when production multipartIdentifierProperty is exited.
func (s *BaseSqlBaseParserListener) ExitMultipartIdentifierProperty(ctx *MultipartIdentifierPropertyContext) {
}

// EnterTableIdentifier is called when production tableIdentifier is entered.
func (s *BaseSqlBaseParserListener) EnterTableIdentifier(ctx *TableIdentifierContext) {}

// ExitTableIdentifier is called when production tableIdentifier is exited.
func (s *BaseSqlBaseParserListener) ExitTableIdentifier(ctx *TableIdentifierContext) {}

// EnterFunctionIdentifier is called when production functionIdentifier is entered.
func (s *BaseSqlBaseParserListener) EnterFunctionIdentifier(ctx *FunctionIdentifierContext) {}

// ExitFunctionIdentifier is called when production functionIdentifier is exited.
func (s *BaseSqlBaseParserListener) ExitFunctionIdentifier(ctx *FunctionIdentifierContext) {}

// EnterNamedExpression is called when production namedExpression is entered.
func (s *BaseSqlBaseParserListener) EnterNamedExpression(ctx *NamedExpressionContext) {}

// ExitNamedExpression is called when production namedExpression is exited.
func (s *BaseSqlBaseParserListener) ExitNamedExpression(ctx *NamedExpressionContext) {}

// EnterNamedExpressionSeq is called when production namedExpressionSeq is entered.
func (s *BaseSqlBaseParserListener) EnterNamedExpressionSeq(ctx *NamedExpressionSeqContext) {}

// ExitNamedExpressionSeq is called when production namedExpressionSeq is exited.
func (s *BaseSqlBaseParserListener) ExitNamedExpressionSeq(ctx *NamedExpressionSeqContext) {}

// EnterPartitionFieldList is called when production partitionFieldList is entered.
func (s *BaseSqlBaseParserListener) EnterPartitionFieldList(ctx *PartitionFieldListContext) {}

// ExitPartitionFieldList is called when production partitionFieldList is exited.
func (s *BaseSqlBaseParserListener) ExitPartitionFieldList(ctx *PartitionFieldListContext) {}

// EnterPartitionTransform is called when production partitionTransform is entered.
func (s *BaseSqlBaseParserListener) EnterPartitionTransform(ctx *PartitionTransformContext) {}

// ExitPartitionTransform is called when production partitionTransform is exited.
func (s *BaseSqlBaseParserListener) ExitPartitionTransform(ctx *PartitionTransformContext) {}

// EnterPartitionColumn is called when production partitionColumn is entered.
func (s *BaseSqlBaseParserListener) EnterPartitionColumn(ctx *PartitionColumnContext) {}

// ExitPartitionColumn is called when production partitionColumn is exited.
func (s *BaseSqlBaseParserListener) ExitPartitionColumn(ctx *PartitionColumnContext) {}

// EnterIdentityTransform is called when production identityTransform is entered.
func (s *BaseSqlBaseParserListener) EnterIdentityTransform(ctx *IdentityTransformContext) {}

// ExitIdentityTransform is called when production identityTransform is exited.
func (s *BaseSqlBaseParserListener) ExitIdentityTransform(ctx *IdentityTransformContext) {}

// EnterApplyTransform is called when production applyTransform is entered.
func (s *BaseSqlBaseParserListener) EnterApplyTransform(ctx *ApplyTransformContext) {}

// ExitApplyTransform is called when production applyTransform is exited.
func (s *BaseSqlBaseParserListener) ExitApplyTransform(ctx *ApplyTransformContext) {}

// EnterTransformArgument is called when production transformArgument is entered.
func (s *BaseSqlBaseParserListener) EnterTransformArgument(ctx *TransformArgumentContext) {}

// ExitTransformArgument is called when production transformArgument is exited.
func (s *BaseSqlBaseParserListener) ExitTransformArgument(ctx *TransformArgumentContext) {}

// EnterExpression is called when production expression is entered.
func (s *BaseSqlBaseParserListener) EnterExpression(ctx *ExpressionContext) {}

// ExitExpression is called when production expression is exited.
func (s *BaseSqlBaseParserListener) ExitExpression(ctx *ExpressionContext) {}

// EnterNamedArgumentExpression is called when production namedArgumentExpression is entered.
func (s *BaseSqlBaseParserListener) EnterNamedArgumentExpression(ctx *NamedArgumentExpressionContext) {
}

// ExitNamedArgumentExpression is called when production namedArgumentExpression is exited.
func (s *BaseSqlBaseParserListener) ExitNamedArgumentExpression(ctx *NamedArgumentExpressionContext) {
}

// EnterFunctionArgument is called when production functionArgument is entered.
func (s *BaseSqlBaseParserListener) EnterFunctionArgument(ctx *FunctionArgumentContext) {}

// ExitFunctionArgument is called when production functionArgument is exited.
func (s *BaseSqlBaseParserListener) ExitFunctionArgument(ctx *FunctionArgumentContext) {}

// EnterExpressionSeq is called when production expressionSeq is entered.
func (s *BaseSqlBaseParserListener) EnterExpressionSeq(ctx *ExpressionSeqContext) {}

// ExitExpressionSeq is called when production expressionSeq is exited.
func (s *BaseSqlBaseParserListener) ExitExpressionSeq(ctx *ExpressionSeqContext) {}

// EnterLogicalNot is called when production logicalNot is entered.
func (s *BaseSqlBaseParserListener) EnterLogicalNot(ctx *LogicalNotContext) {}

// ExitLogicalNot is called when production logicalNot is exited.
func (s *BaseSqlBaseParserListener) ExitLogicalNot(ctx *LogicalNotContext) {}

// EnterPredicated is called when production predicated is entered.
func (s *BaseSqlBaseParserListener) EnterPredicated(ctx *PredicatedContext) {}

// ExitPredicated is called when production predicated is exited.
func (s *BaseSqlBaseParserListener) ExitPredicated(ctx *PredicatedContext) {}

// EnterExists is called when production exists is entered.
func (s *BaseSqlBaseParserListener) EnterExists(ctx *ExistsContext) {}

// ExitExists is called when production exists is exited.
func (s *BaseSqlBaseParserListener) ExitExists(ctx *ExistsContext) {}

// EnterLogicalBinary is called when production logicalBinary is entered.
func (s *BaseSqlBaseParserListener) EnterLogicalBinary(ctx *LogicalBinaryContext) {}

// ExitLogicalBinary is called when production logicalBinary is exited.
func (s *BaseSqlBaseParserListener) ExitLogicalBinary(ctx *LogicalBinaryContext) {}

// EnterPredicate is called when production predicate is entered.
func (s *BaseSqlBaseParserListener) EnterPredicate(ctx *PredicateContext) {}

// ExitPredicate is called when production predicate is exited.
func (s *BaseSqlBaseParserListener) ExitPredicate(ctx *PredicateContext) {}

// EnterErrorCapturingNot is called when production errorCapturingNot is entered.
func (s *BaseSqlBaseParserListener) EnterErrorCapturingNot(ctx *ErrorCapturingNotContext) {}

// ExitErrorCapturingNot is called when production errorCapturingNot is exited.
func (s *BaseSqlBaseParserListener) ExitErrorCapturingNot(ctx *ErrorCapturingNotContext) {}

// EnterValueExpressionDefault is called when production valueExpressionDefault is entered.
func (s *BaseSqlBaseParserListener) EnterValueExpressionDefault(ctx *ValueExpressionDefaultContext) {}

// ExitValueExpressionDefault is called when production valueExpressionDefault is exited.
func (s *BaseSqlBaseParserListener) ExitValueExpressionDefault(ctx *ValueExpressionDefaultContext) {}

// EnterComparison is called when production comparison is entered.
func (s *BaseSqlBaseParserListener) EnterComparison(ctx *ComparisonContext) {}

// ExitComparison is called when production comparison is exited.
func (s *BaseSqlBaseParserListener) ExitComparison(ctx *ComparisonContext) {}

// EnterShiftExpression is called when production shiftExpression is entered.
func (s *BaseSqlBaseParserListener) EnterShiftExpression(ctx *ShiftExpressionContext) {}

// ExitShiftExpression is called when production shiftExpression is exited.
func (s *BaseSqlBaseParserListener) ExitShiftExpression(ctx *ShiftExpressionContext) {}

// EnterArithmeticBinary is called when production arithmeticBinary is entered.
func (s *BaseSqlBaseParserListener) EnterArithmeticBinary(ctx *ArithmeticBinaryContext) {}

// ExitArithmeticBinary is called when production arithmeticBinary is exited.
func (s *BaseSqlBaseParserListener) ExitArithmeticBinary(ctx *ArithmeticBinaryContext) {}

// EnterArithmeticUnary is called when production arithmeticUnary is entered.
func (s *BaseSqlBaseParserListener) EnterArithmeticUnary(ctx *ArithmeticUnaryContext) {}

// ExitArithmeticUnary is called when production arithmeticUnary is exited.
func (s *BaseSqlBaseParserListener) ExitArithmeticUnary(ctx *ArithmeticUnaryContext) {}

// EnterShiftOperator is called when production shiftOperator is entered.
func (s *BaseSqlBaseParserListener) EnterShiftOperator(ctx *ShiftOperatorContext) {}

// ExitShiftOperator is called when production shiftOperator is exited.
func (s *BaseSqlBaseParserListener) ExitShiftOperator(ctx *ShiftOperatorContext) {}

// EnterDatetimeUnit is called when production datetimeUnit is entered.
func (s *BaseSqlBaseParserListener) EnterDatetimeUnit(ctx *DatetimeUnitContext) {}

// ExitDatetimeUnit is called when production datetimeUnit is exited.
func (s *BaseSqlBaseParserListener) ExitDatetimeUnit(ctx *DatetimeUnitContext) {}

// EnterStruct is called when production struct is entered.
func (s *BaseSqlBaseParserListener) EnterStruct(ctx *StructContext) {}

// ExitStruct is called when production struct is exited.
func (s *BaseSqlBaseParserListener) ExitStruct(ctx *StructContext) {}

// EnterDereference is called when production dereference is entered.
func (s *BaseSqlBaseParserListener) EnterDereference(ctx *DereferenceContext) {}

// ExitDereference is called when production dereference is exited.
func (s *BaseSqlBaseParserListener) ExitDereference(ctx *DereferenceContext) {}

// EnterCastByColon is called when production castByColon is entered.
func (s *BaseSqlBaseParserListener) EnterCastByColon(ctx *CastByColonContext) {}

// ExitCastByColon is called when production castByColon is exited.
func (s *BaseSqlBaseParserListener) ExitCastByColon(ctx *CastByColonContext) {}

// EnterTimestampadd is called when production timestampadd is entered.
func (s *BaseSqlBaseParserListener) EnterTimestampadd(ctx *TimestampaddContext) {}

// ExitTimestampadd is called when production timestampadd is exited.
func (s *BaseSqlBaseParserListener) ExitTimestampadd(ctx *TimestampaddContext) {}

// EnterSubstring is called when production substring is entered.
func (s *BaseSqlBaseParserListener) EnterSubstring(ctx *SubstringContext) {}

// ExitSubstring is called when production substring is exited.
func (s *BaseSqlBaseParserListener) ExitSubstring(ctx *SubstringContext) {}

// EnterCast is called when production cast is entered.
func (s *BaseSqlBaseParserListener) EnterCast(ctx *CastContext) {}

// ExitCast is called when production cast is exited.
func (s *BaseSqlBaseParserListener) ExitCast(ctx *CastContext) {}

// EnterLambda is called when production lambda is entered.
func (s *BaseSqlBaseParserListener) EnterLambda(ctx *LambdaContext) {}

// ExitLambda is called when production lambda is exited.
func (s *BaseSqlBaseParserListener) ExitLambda(ctx *LambdaContext) {}

// EnterParenthesizedExpression is called when production parenthesizedExpression is entered.
func (s *BaseSqlBaseParserListener) EnterParenthesizedExpression(ctx *ParenthesizedExpressionContext) {
}

// ExitParenthesizedExpression is called when production parenthesizedExpression is exited.
func (s *BaseSqlBaseParserListener) ExitParenthesizedExpression(ctx *ParenthesizedExpressionContext) {
}

// EnterAny_value is called when production any_value is entered.
func (s *BaseSqlBaseParserListener) EnterAny_value(ctx *Any_valueContext) {}

// ExitAny_value is called when production any_value is exited.
func (s *BaseSqlBaseParserListener) ExitAny_value(ctx *Any_valueContext) {}

// EnterTrim is called when production trim is entered.
func (s *BaseSqlBaseParserListener) EnterTrim(ctx *TrimContext) {}

// ExitTrim is called when production trim is exited.
func (s *BaseSqlBaseParserListener) ExitTrim(ctx *TrimContext) {}

// EnterSemiStructuredExtract is called when production semiStructuredExtract is entered.
func (s *BaseSqlBaseParserListener) EnterSemiStructuredExtract(ctx *SemiStructuredExtractContext) {}

// ExitSemiStructuredExtract is called when production semiStructuredExtract is exited.
func (s *BaseSqlBaseParserListener) ExitSemiStructuredExtract(ctx *SemiStructuredExtractContext) {}

// EnterSimpleCase is called when production simpleCase is entered.
func (s *BaseSqlBaseParserListener) EnterSimpleCase(ctx *SimpleCaseContext) {}

// ExitSimpleCase is called when production simpleCase is exited.
func (s *BaseSqlBaseParserListener) ExitSimpleCase(ctx *SimpleCaseContext) {}

// EnterCurrentLike is called when production currentLike is entered.
func (s *BaseSqlBaseParserListener) EnterCurrentLike(ctx *CurrentLikeContext) {}

// ExitCurrentLike is called when production currentLike is exited.
func (s *BaseSqlBaseParserListener) ExitCurrentLike(ctx *CurrentLikeContext) {}

// EnterColumnReference is called when production columnReference is entered.
func (s *BaseSqlBaseParserListener) EnterColumnReference(ctx *ColumnReferenceContext) {}

// ExitColumnReference is called when production columnReference is exited.
func (s *BaseSqlBaseParserListener) ExitColumnReference(ctx *ColumnReferenceContext) {}

// EnterRowConstructor is called when production rowConstructor is entered.
func (s *BaseSqlBaseParserListener) EnterRowConstructor(ctx *RowConstructorContext) {}

// ExitRowConstructor is called when production rowConstructor is exited.
func (s *BaseSqlBaseParserListener) ExitRowConstructor(ctx *RowConstructorContext) {}

// EnterLast is called when production last is entered.
func (s *BaseSqlBaseParserListener) EnterLast(ctx *LastContext) {}

// ExitLast is called when production last is exited.
func (s *BaseSqlBaseParserListener) ExitLast(ctx *LastContext) {}

// EnterStar is called when production star is entered.
func (s *BaseSqlBaseParserListener) EnterStar(ctx *StarContext) {}

// ExitStar is called when production star is exited.
func (s *BaseSqlBaseParserListener) ExitStar(ctx *StarContext) {}

// EnterOverlay is called when production overlay is entered.
func (s *BaseSqlBaseParserListener) EnterOverlay(ctx *OverlayContext) {}

// ExitOverlay is called when production overlay is exited.
func (s *BaseSqlBaseParserListener) ExitOverlay(ctx *OverlayContext) {}

// EnterSubscript is called when production subscript is entered.
func (s *BaseSqlBaseParserListener) EnterSubscript(ctx *SubscriptContext) {}

// ExitSubscript is called when production subscript is exited.
func (s *BaseSqlBaseParserListener) ExitSubscript(ctx *SubscriptContext) {}

// EnterTimestampdiff is called when production timestampdiff is entered.
func (s *BaseSqlBaseParserListener) EnterTimestampdiff(ctx *TimestampdiffContext) {}

// ExitTimestampdiff is called when production timestampdiff is exited.
func (s *BaseSqlBaseParserListener) ExitTimestampdiff(ctx *TimestampdiffContext) {}

// EnterSubqueryExpression is called when production subqueryExpression is entered.
func (s *BaseSqlBaseParserListener) EnterSubqueryExpression(ctx *SubqueryExpressionContext) {}

// ExitSubqueryExpression is called when production subqueryExpression is exited.
func (s *BaseSqlBaseParserListener) ExitSubqueryExpression(ctx *SubqueryExpressionContext) {}

// EnterCollate is called when production collate is entered.
func (s *BaseSqlBaseParserListener) EnterCollate(ctx *CollateContext) {}

// ExitCollate is called when production collate is exited.
func (s *BaseSqlBaseParserListener) ExitCollate(ctx *CollateContext) {}

// EnterConstantDefault is called when production constantDefault is entered.
func (s *BaseSqlBaseParserListener) EnterConstantDefault(ctx *ConstantDefaultContext) {}

// ExitConstantDefault is called when production constantDefault is exited.
func (s *BaseSqlBaseParserListener) ExitConstantDefault(ctx *ConstantDefaultContext) {}

// EnterExtract is called when production extract is entered.
func (s *BaseSqlBaseParserListener) EnterExtract(ctx *ExtractContext) {}

// ExitExtract is called when production extract is exited.
func (s *BaseSqlBaseParserListener) ExitExtract(ctx *ExtractContext) {}

// EnterFunctionCall is called when production functionCall is entered.
func (s *BaseSqlBaseParserListener) EnterFunctionCall(ctx *FunctionCallContext) {}

// ExitFunctionCall is called when production functionCall is exited.
func (s *BaseSqlBaseParserListener) ExitFunctionCall(ctx *FunctionCallContext) {}

// EnterSearchedCase is called when production searchedCase is entered.
func (s *BaseSqlBaseParserListener) EnterSearchedCase(ctx *SearchedCaseContext) {}

// ExitSearchedCase is called when production searchedCase is exited.
func (s *BaseSqlBaseParserListener) ExitSearchedCase(ctx *SearchedCaseContext) {}

// EnterPosition is called when production position is entered.
func (s *BaseSqlBaseParserListener) EnterPosition(ctx *PositionContext) {}

// ExitPosition is called when production position is exited.
func (s *BaseSqlBaseParserListener) ExitPosition(ctx *PositionContext) {}

// EnterFirst is called when production first is entered.
func (s *BaseSqlBaseParserListener) EnterFirst(ctx *FirstContext) {}

// ExitFirst is called when production first is exited.
func (s *BaseSqlBaseParserListener) ExitFirst(ctx *FirstContext) {}

// EnterSemiStructuredExtractionPath is called when production semiStructuredExtractionPath is entered.
func (s *BaseSqlBaseParserListener) EnterSemiStructuredExtractionPath(ctx *SemiStructuredExtractionPathContext) {
}

// ExitSemiStructuredExtractionPath is called when production semiStructuredExtractionPath is exited.
func (s *BaseSqlBaseParserListener) ExitSemiStructuredExtractionPath(ctx *SemiStructuredExtractionPathContext) {
}

// EnterJsonPathIdentifier is called when production jsonPathIdentifier is entered.
func (s *BaseSqlBaseParserListener) EnterJsonPathIdentifier(ctx *JsonPathIdentifierContext) {}

// ExitJsonPathIdentifier is called when production jsonPathIdentifier is exited.
func (s *BaseSqlBaseParserListener) ExitJsonPathIdentifier(ctx *JsonPathIdentifierContext) {}

// EnterJsonPathBracketedIdentifier is called when production jsonPathBracketedIdentifier is entered.
func (s *BaseSqlBaseParserListener) EnterJsonPathBracketedIdentifier(ctx *JsonPathBracketedIdentifierContext) {
}

// ExitJsonPathBracketedIdentifier is called when production jsonPathBracketedIdentifier is exited.
func (s *BaseSqlBaseParserListener) ExitJsonPathBracketedIdentifier(ctx *JsonPathBracketedIdentifierContext) {
}

// EnterJsonPathFirstPart is called when production jsonPathFirstPart is entered.
func (s *BaseSqlBaseParserListener) EnterJsonPathFirstPart(ctx *JsonPathFirstPartContext) {}

// ExitJsonPathFirstPart is called when production jsonPathFirstPart is exited.
func (s *BaseSqlBaseParserListener) ExitJsonPathFirstPart(ctx *JsonPathFirstPartContext) {}

// EnterJsonPathParts is called when production jsonPathParts is entered.
func (s *BaseSqlBaseParserListener) EnterJsonPathParts(ctx *JsonPathPartsContext) {}

// ExitJsonPathParts is called when production jsonPathParts is exited.
func (s *BaseSqlBaseParserListener) ExitJsonPathParts(ctx *JsonPathPartsContext) {}

// EnterLiteralType is called when production literalType is entered.
func (s *BaseSqlBaseParserListener) EnterLiteralType(ctx *LiteralTypeContext) {}

// ExitLiteralType is called when production literalType is exited.
func (s *BaseSqlBaseParserListener) ExitLiteralType(ctx *LiteralTypeContext) {}

// EnterNullLiteral is called when production nullLiteral is entered.
func (s *BaseSqlBaseParserListener) EnterNullLiteral(ctx *NullLiteralContext) {}

// ExitNullLiteral is called when production nullLiteral is exited.
func (s *BaseSqlBaseParserListener) ExitNullLiteral(ctx *NullLiteralContext) {}

// EnterPosParameterLiteral is called when production posParameterLiteral is entered.
func (s *BaseSqlBaseParserListener) EnterPosParameterLiteral(ctx *PosParameterLiteralContext) {}

// ExitPosParameterLiteral is called when production posParameterLiteral is exited.
func (s *BaseSqlBaseParserListener) ExitPosParameterLiteral(ctx *PosParameterLiteralContext) {}

// EnterNamedParameterLiteral is called when production namedParameterLiteral is entered.
func (s *BaseSqlBaseParserListener) EnterNamedParameterLiteral(ctx *NamedParameterLiteralContext) {}

// ExitNamedParameterLiteral is called when production namedParameterLiteral is exited.
func (s *BaseSqlBaseParserListener) ExitNamedParameterLiteral(ctx *NamedParameterLiteralContext) {}

// EnterIntervalLiteral is called when production intervalLiteral is entered.
func (s *BaseSqlBaseParserListener) EnterIntervalLiteral(ctx *IntervalLiteralContext) {}

// ExitIntervalLiteral is called when production intervalLiteral is exited.
func (s *BaseSqlBaseParserListener) ExitIntervalLiteral(ctx *IntervalLiteralContext) {}

// EnterTypeConstructor is called when production typeConstructor is entered.
func (s *BaseSqlBaseParserListener) EnterTypeConstructor(ctx *TypeConstructorContext) {}

// ExitTypeConstructor is called when production typeConstructor is exited.
func (s *BaseSqlBaseParserListener) ExitTypeConstructor(ctx *TypeConstructorContext) {}

// EnterNumericLiteral is called when production numericLiteral is entered.
func (s *BaseSqlBaseParserListener) EnterNumericLiteral(ctx *NumericLiteralContext) {}

// ExitNumericLiteral is called when production numericLiteral is exited.
func (s *BaseSqlBaseParserListener) ExitNumericLiteral(ctx *NumericLiteralContext) {}

// EnterBooleanLiteral is called when production booleanLiteral is entered.
func (s *BaseSqlBaseParserListener) EnterBooleanLiteral(ctx *BooleanLiteralContext) {}

// ExitBooleanLiteral is called when production booleanLiteral is exited.
func (s *BaseSqlBaseParserListener) ExitBooleanLiteral(ctx *BooleanLiteralContext) {}

// EnterStringLiteral is called when production stringLiteral is entered.
func (s *BaseSqlBaseParserListener) EnterStringLiteral(ctx *StringLiteralContext) {}

// ExitStringLiteral is called when production stringLiteral is exited.
func (s *BaseSqlBaseParserListener) ExitStringLiteral(ctx *StringLiteralContext) {}

// EnterNamedParameterMarker is called when production namedParameterMarker is entered.
func (s *BaseSqlBaseParserListener) EnterNamedParameterMarker(ctx *NamedParameterMarkerContext) {}

// ExitNamedParameterMarker is called when production namedParameterMarker is exited.
func (s *BaseSqlBaseParserListener) ExitNamedParameterMarker(ctx *NamedParameterMarkerContext) {}

// EnterComparisonOperator is called when production comparisonOperator is entered.
func (s *BaseSqlBaseParserListener) EnterComparisonOperator(ctx *ComparisonOperatorContext) {}

// ExitComparisonOperator is called when production comparisonOperator is exited.
func (s *BaseSqlBaseParserListener) ExitComparisonOperator(ctx *ComparisonOperatorContext) {}

// EnterArithmeticOperator is called when production arithmeticOperator is entered.
func (s *BaseSqlBaseParserListener) EnterArithmeticOperator(ctx *ArithmeticOperatorContext) {}

// ExitArithmeticOperator is called when production arithmeticOperator is exited.
func (s *BaseSqlBaseParserListener) ExitArithmeticOperator(ctx *ArithmeticOperatorContext) {}

// EnterPredicateOperator is called when production predicateOperator is entered.
func (s *BaseSqlBaseParserListener) EnterPredicateOperator(ctx *PredicateOperatorContext) {}

// ExitPredicateOperator is called when production predicateOperator is exited.
func (s *BaseSqlBaseParserListener) ExitPredicateOperator(ctx *PredicateOperatorContext) {}

// EnterBooleanValue is called when production booleanValue is entered.
func (s *BaseSqlBaseParserListener) EnterBooleanValue(ctx *BooleanValueContext) {}

// ExitBooleanValue is called when production booleanValue is exited.
func (s *BaseSqlBaseParserListener) ExitBooleanValue(ctx *BooleanValueContext) {}

// EnterInterval is called when production interval is entered.
func (s *BaseSqlBaseParserListener) EnterInterval(ctx *IntervalContext) {}

// ExitInterval is called when production interval is exited.
func (s *BaseSqlBaseParserListener) ExitInterval(ctx *IntervalContext) {}

// EnterErrorCapturingMultiUnitsInterval is called when production errorCapturingMultiUnitsInterval is entered.
func (s *BaseSqlBaseParserListener) EnterErrorCapturingMultiUnitsInterval(ctx *ErrorCapturingMultiUnitsIntervalContext) {
}

// ExitErrorCapturingMultiUnitsInterval is called when production errorCapturingMultiUnitsInterval is exited.
func (s *BaseSqlBaseParserListener) ExitErrorCapturingMultiUnitsInterval(ctx *ErrorCapturingMultiUnitsIntervalContext) {
}

// EnterMultiUnitsInterval is called when production multiUnitsInterval is entered.
func (s *BaseSqlBaseParserListener) EnterMultiUnitsInterval(ctx *MultiUnitsIntervalContext) {}

// ExitMultiUnitsInterval is called when production multiUnitsInterval is exited.
func (s *BaseSqlBaseParserListener) ExitMultiUnitsInterval(ctx *MultiUnitsIntervalContext) {}

// EnterErrorCapturingUnitToUnitInterval is called when production errorCapturingUnitToUnitInterval is entered.
func (s *BaseSqlBaseParserListener) EnterErrorCapturingUnitToUnitInterval(ctx *ErrorCapturingUnitToUnitIntervalContext) {
}

// ExitErrorCapturingUnitToUnitInterval is called when production errorCapturingUnitToUnitInterval is exited.
func (s *BaseSqlBaseParserListener) ExitErrorCapturingUnitToUnitInterval(ctx *ErrorCapturingUnitToUnitIntervalContext) {
}

// EnterUnitToUnitInterval is called when production unitToUnitInterval is entered.
func (s *BaseSqlBaseParserListener) EnterUnitToUnitInterval(ctx *UnitToUnitIntervalContext) {}

// ExitUnitToUnitInterval is called when production unitToUnitInterval is exited.
func (s *BaseSqlBaseParserListener) ExitUnitToUnitInterval(ctx *UnitToUnitIntervalContext) {}

// EnterIntervalValue is called when production intervalValue is entered.
func (s *BaseSqlBaseParserListener) EnterIntervalValue(ctx *IntervalValueContext) {}

// ExitIntervalValue is called when production intervalValue is exited.
func (s *BaseSqlBaseParserListener) ExitIntervalValue(ctx *IntervalValueContext) {}

// EnterUnitInMultiUnits is called when production unitInMultiUnits is entered.
func (s *BaseSqlBaseParserListener) EnterUnitInMultiUnits(ctx *UnitInMultiUnitsContext) {}

// ExitUnitInMultiUnits is called when production unitInMultiUnits is exited.
func (s *BaseSqlBaseParserListener) ExitUnitInMultiUnits(ctx *UnitInMultiUnitsContext) {}

// EnterUnitInUnitToUnit is called when production unitInUnitToUnit is entered.
func (s *BaseSqlBaseParserListener) EnterUnitInUnitToUnit(ctx *UnitInUnitToUnitContext) {}

// ExitUnitInUnitToUnit is called when production unitInUnitToUnit is exited.
func (s *BaseSqlBaseParserListener) ExitUnitInUnitToUnit(ctx *UnitInUnitToUnitContext) {}

// EnterColPosition is called when production colPosition is entered.
func (s *BaseSqlBaseParserListener) EnterColPosition(ctx *ColPositionContext) {}

// ExitColPosition is called when production colPosition is exited.
func (s *BaseSqlBaseParserListener) ExitColPosition(ctx *ColPositionContext) {}

// EnterCollationSpec is called when production collationSpec is entered.
func (s *BaseSqlBaseParserListener) EnterCollationSpec(ctx *CollationSpecContext) {}

// ExitCollationSpec is called when production collationSpec is exited.
func (s *BaseSqlBaseParserListener) ExitCollationSpec(ctx *CollationSpecContext) {}

// EnterCollateClause is called when production collateClause is entered.
func (s *BaseSqlBaseParserListener) EnterCollateClause(ctx *CollateClauseContext) {}

// ExitCollateClause is called when production collateClause is exited.
func (s *BaseSqlBaseParserListener) ExitCollateClause(ctx *CollateClauseContext) {}

// EnterNonTrivialPrimitiveType is called when production nonTrivialPrimitiveType is entered.
func (s *BaseSqlBaseParserListener) EnterNonTrivialPrimitiveType(ctx *NonTrivialPrimitiveTypeContext) {
}

// ExitNonTrivialPrimitiveType is called when production nonTrivialPrimitiveType is exited.
func (s *BaseSqlBaseParserListener) ExitNonTrivialPrimitiveType(ctx *NonTrivialPrimitiveTypeContext) {
}

// EnterTrivialPrimitiveType is called when production trivialPrimitiveType is entered.
func (s *BaseSqlBaseParserListener) EnterTrivialPrimitiveType(ctx *TrivialPrimitiveTypeContext) {}

// ExitTrivialPrimitiveType is called when production trivialPrimitiveType is exited.
func (s *BaseSqlBaseParserListener) ExitTrivialPrimitiveType(ctx *TrivialPrimitiveTypeContext) {}

// EnterPrimitiveType is called when production primitiveType is entered.
func (s *BaseSqlBaseParserListener) EnterPrimitiveType(ctx *PrimitiveTypeContext) {}

// ExitPrimitiveType is called when production primitiveType is exited.
func (s *BaseSqlBaseParserListener) ExitPrimitiveType(ctx *PrimitiveTypeContext) {}

// EnterComplexDataType is called when production complexDataType is entered.
func (s *BaseSqlBaseParserListener) EnterComplexDataType(ctx *ComplexDataTypeContext) {}

// ExitComplexDataType is called when production complexDataType is exited.
func (s *BaseSqlBaseParserListener) ExitComplexDataType(ctx *ComplexDataTypeContext) {}

// EnterPrimitiveDataType is called when production primitiveDataType is entered.
func (s *BaseSqlBaseParserListener) EnterPrimitiveDataType(ctx *PrimitiveDataTypeContext) {}

// ExitPrimitiveDataType is called when production primitiveDataType is exited.
func (s *BaseSqlBaseParserListener) ExitPrimitiveDataType(ctx *PrimitiveDataTypeContext) {}

// EnterQualifiedColTypeWithPositionList is called when production qualifiedColTypeWithPositionList is entered.
func (s *BaseSqlBaseParserListener) EnterQualifiedColTypeWithPositionList(ctx *QualifiedColTypeWithPositionListContext) {
}

// ExitQualifiedColTypeWithPositionList is called when production qualifiedColTypeWithPositionList is exited.
func (s *BaseSqlBaseParserListener) ExitQualifiedColTypeWithPositionList(ctx *QualifiedColTypeWithPositionListContext) {
}

// EnterQualifiedColTypeWithPosition is called when production qualifiedColTypeWithPosition is entered.
func (s *BaseSqlBaseParserListener) EnterQualifiedColTypeWithPosition(ctx *QualifiedColTypeWithPositionContext) {
}

// ExitQualifiedColTypeWithPosition is called when production qualifiedColTypeWithPosition is exited.
func (s *BaseSqlBaseParserListener) ExitQualifiedColTypeWithPosition(ctx *QualifiedColTypeWithPositionContext) {
}

// EnterColDefinitionDescriptorWithPosition is called when production colDefinitionDescriptorWithPosition is entered.
func (s *BaseSqlBaseParserListener) EnterColDefinitionDescriptorWithPosition(ctx *ColDefinitionDescriptorWithPositionContext) {
}

// ExitColDefinitionDescriptorWithPosition is called when production colDefinitionDescriptorWithPosition is exited.
func (s *BaseSqlBaseParserListener) ExitColDefinitionDescriptorWithPosition(ctx *ColDefinitionDescriptorWithPositionContext) {
}

// EnterDefaultExpression is called when production defaultExpression is entered.
func (s *BaseSqlBaseParserListener) EnterDefaultExpression(ctx *DefaultExpressionContext) {}

// ExitDefaultExpression is called when production defaultExpression is exited.
func (s *BaseSqlBaseParserListener) ExitDefaultExpression(ctx *DefaultExpressionContext) {}

// EnterVariableDefaultExpression is called when production variableDefaultExpression is entered.
func (s *BaseSqlBaseParserListener) EnterVariableDefaultExpression(ctx *VariableDefaultExpressionContext) {
}

// ExitVariableDefaultExpression is called when production variableDefaultExpression is exited.
func (s *BaseSqlBaseParserListener) ExitVariableDefaultExpression(ctx *VariableDefaultExpressionContext) {
}

// EnterColTypeList is called when production colTypeList is entered.
func (s *BaseSqlBaseParserListener) EnterColTypeList(ctx *ColTypeListContext) {}

// ExitColTypeList is called when production colTypeList is exited.
func (s *BaseSqlBaseParserListener) ExitColTypeList(ctx *ColTypeListContext) {}

// EnterColType is called when production colType is entered.
func (s *BaseSqlBaseParserListener) EnterColType(ctx *ColTypeContext) {}

// ExitColType is called when production colType is exited.
func (s *BaseSqlBaseParserListener) ExitColType(ctx *ColTypeContext) {}

// EnterTableElementList is called when production tableElementList is entered.
func (s *BaseSqlBaseParserListener) EnterTableElementList(ctx *TableElementListContext) {}

// ExitTableElementList is called when production tableElementList is exited.
func (s *BaseSqlBaseParserListener) ExitTableElementList(ctx *TableElementListContext) {}

// EnterTableElement is called when production tableElement is entered.
func (s *BaseSqlBaseParserListener) EnterTableElement(ctx *TableElementContext) {}

// ExitTableElement is called when production tableElement is exited.
func (s *BaseSqlBaseParserListener) ExitTableElement(ctx *TableElementContext) {}

// EnterColDefinitionList is called when production colDefinitionList is entered.
func (s *BaseSqlBaseParserListener) EnterColDefinitionList(ctx *ColDefinitionListContext) {}

// ExitColDefinitionList is called when production colDefinitionList is exited.
func (s *BaseSqlBaseParserListener) ExitColDefinitionList(ctx *ColDefinitionListContext) {}

// EnterColDefinition is called when production colDefinition is entered.
func (s *BaseSqlBaseParserListener) EnterColDefinition(ctx *ColDefinitionContext) {}

// ExitColDefinition is called when production colDefinition is exited.
func (s *BaseSqlBaseParserListener) ExitColDefinition(ctx *ColDefinitionContext) {}

// EnterColDefinitionOption is called when production colDefinitionOption is entered.
func (s *BaseSqlBaseParserListener) EnterColDefinitionOption(ctx *ColDefinitionOptionContext) {}

// ExitColDefinitionOption is called when production colDefinitionOption is exited.
func (s *BaseSqlBaseParserListener) ExitColDefinitionOption(ctx *ColDefinitionOptionContext) {}

// EnterGeneratedColumn is called when production generatedColumn is entered.
func (s *BaseSqlBaseParserListener) EnterGeneratedColumn(ctx *GeneratedColumnContext) {}

// ExitGeneratedColumn is called when production generatedColumn is exited.
func (s *BaseSqlBaseParserListener) ExitGeneratedColumn(ctx *GeneratedColumnContext) {}

// EnterIdentityColumn is called when production identityColumn is entered.
func (s *BaseSqlBaseParserListener) EnterIdentityColumn(ctx *IdentityColumnContext) {}

// ExitIdentityColumn is called when production identityColumn is exited.
func (s *BaseSqlBaseParserListener) ExitIdentityColumn(ctx *IdentityColumnContext) {}

// EnterIdentityColSpec is called when production identityColSpec is entered.
func (s *BaseSqlBaseParserListener) EnterIdentityColSpec(ctx *IdentityColSpecContext) {}

// ExitIdentityColSpec is called when production identityColSpec is exited.
func (s *BaseSqlBaseParserListener) ExitIdentityColSpec(ctx *IdentityColSpecContext) {}

// EnterSequenceGeneratorOption is called when production sequenceGeneratorOption is entered.
func (s *BaseSqlBaseParserListener) EnterSequenceGeneratorOption(ctx *SequenceGeneratorOptionContext) {
}

// ExitSequenceGeneratorOption is called when production sequenceGeneratorOption is exited.
func (s *BaseSqlBaseParserListener) ExitSequenceGeneratorOption(ctx *SequenceGeneratorOptionContext) {
}

// EnterSequenceGeneratorStartOrStep is called when production sequenceGeneratorStartOrStep is entered.
func (s *BaseSqlBaseParserListener) EnterSequenceGeneratorStartOrStep(ctx *SequenceGeneratorStartOrStepContext) {
}

// ExitSequenceGeneratorStartOrStep is called when production sequenceGeneratorStartOrStep is exited.
func (s *BaseSqlBaseParserListener) ExitSequenceGeneratorStartOrStep(ctx *SequenceGeneratorStartOrStepContext) {
}

// EnterComplexColTypeList is called when production complexColTypeList is entered.
func (s *BaseSqlBaseParserListener) EnterComplexColTypeList(ctx *ComplexColTypeListContext) {}

// ExitComplexColTypeList is called when production complexColTypeList is exited.
func (s *BaseSqlBaseParserListener) ExitComplexColTypeList(ctx *ComplexColTypeListContext) {}

// EnterComplexColType is called when production complexColType is entered.
func (s *BaseSqlBaseParserListener) EnterComplexColType(ctx *ComplexColTypeContext) {}

// ExitComplexColType is called when production complexColType is exited.
func (s *BaseSqlBaseParserListener) ExitComplexColType(ctx *ComplexColTypeContext) {}

// EnterRoutineCharacteristics is called when production routineCharacteristics is entered.
func (s *BaseSqlBaseParserListener) EnterRoutineCharacteristics(ctx *RoutineCharacteristicsContext) {}

// ExitRoutineCharacteristics is called when production routineCharacteristics is exited.
func (s *BaseSqlBaseParserListener) ExitRoutineCharacteristics(ctx *RoutineCharacteristicsContext) {}

// EnterRoutineLanguage is called when production routineLanguage is entered.
func (s *BaseSqlBaseParserListener) EnterRoutineLanguage(ctx *RoutineLanguageContext) {}

// ExitRoutineLanguage is called when production routineLanguage is exited.
func (s *BaseSqlBaseParserListener) ExitRoutineLanguage(ctx *RoutineLanguageContext) {}

// EnterSpecificName is called when production specificName is entered.
func (s *BaseSqlBaseParserListener) EnterSpecificName(ctx *SpecificNameContext) {}

// ExitSpecificName is called when production specificName is exited.
func (s *BaseSqlBaseParserListener) ExitSpecificName(ctx *SpecificNameContext) {}

// EnterDeterministic is called when production deterministic is entered.
func (s *BaseSqlBaseParserListener) EnterDeterministic(ctx *DeterministicContext) {}

// ExitDeterministic is called when production deterministic is exited.
func (s *BaseSqlBaseParserListener) ExitDeterministic(ctx *DeterministicContext) {}

// EnterSqlDataAccess is called when production sqlDataAccess is entered.
func (s *BaseSqlBaseParserListener) EnterSqlDataAccess(ctx *SqlDataAccessContext) {}

// ExitSqlDataAccess is called when production sqlDataAccess is exited.
func (s *BaseSqlBaseParserListener) ExitSqlDataAccess(ctx *SqlDataAccessContext) {}

// EnterNullCall is called when production nullCall is entered.
func (s *BaseSqlBaseParserListener) EnterNullCall(ctx *NullCallContext) {}

// ExitNullCall is called when production nullCall is exited.
func (s *BaseSqlBaseParserListener) ExitNullCall(ctx *NullCallContext) {}

// EnterRightsClause is called when production rightsClause is entered.
func (s *BaseSqlBaseParserListener) EnterRightsClause(ctx *RightsClauseContext) {}

// ExitRightsClause is called when production rightsClause is exited.
func (s *BaseSqlBaseParserListener) ExitRightsClause(ctx *RightsClauseContext) {}

// EnterWhenClause is called when production whenClause is entered.
func (s *BaseSqlBaseParserListener) EnterWhenClause(ctx *WhenClauseContext) {}

// ExitWhenClause is called when production whenClause is exited.
func (s *BaseSqlBaseParserListener) ExitWhenClause(ctx *WhenClauseContext) {}

// EnterWindowClause is called when production windowClause is entered.
func (s *BaseSqlBaseParserListener) EnterWindowClause(ctx *WindowClauseContext) {}

// ExitWindowClause is called when production windowClause is exited.
func (s *BaseSqlBaseParserListener) ExitWindowClause(ctx *WindowClauseContext) {}

// EnterNamedWindow is called when production namedWindow is entered.
func (s *BaseSqlBaseParserListener) EnterNamedWindow(ctx *NamedWindowContext) {}

// ExitNamedWindow is called when production namedWindow is exited.
func (s *BaseSqlBaseParserListener) ExitNamedWindow(ctx *NamedWindowContext) {}

// EnterWindowRef is called when production windowRef is entered.
func (s *BaseSqlBaseParserListener) EnterWindowRef(ctx *WindowRefContext) {}

// ExitWindowRef is called when production windowRef is exited.
func (s *BaseSqlBaseParserListener) ExitWindowRef(ctx *WindowRefContext) {}

// EnterWindowDef is called when production windowDef is entered.
func (s *BaseSqlBaseParserListener) EnterWindowDef(ctx *WindowDefContext) {}

// ExitWindowDef is called when production windowDef is exited.
func (s *BaseSqlBaseParserListener) ExitWindowDef(ctx *WindowDefContext) {}

// EnterWindowFrame is called when production windowFrame is entered.
func (s *BaseSqlBaseParserListener) EnterWindowFrame(ctx *WindowFrameContext) {}

// ExitWindowFrame is called when production windowFrame is exited.
func (s *BaseSqlBaseParserListener) ExitWindowFrame(ctx *WindowFrameContext) {}

// EnterFrameBound is called when production frameBound is entered.
func (s *BaseSqlBaseParserListener) EnterFrameBound(ctx *FrameBoundContext) {}

// ExitFrameBound is called when production frameBound is exited.
func (s *BaseSqlBaseParserListener) ExitFrameBound(ctx *FrameBoundContext) {}

// EnterQualifiedNameList is called when production qualifiedNameList is entered.
func (s *BaseSqlBaseParserListener) EnterQualifiedNameList(ctx *QualifiedNameListContext) {}

// ExitQualifiedNameList is called when production qualifiedNameList is exited.
func (s *BaseSqlBaseParserListener) ExitQualifiedNameList(ctx *QualifiedNameListContext) {}

// EnterFunctionName is called when production functionName is entered.
func (s *BaseSqlBaseParserListener) EnterFunctionName(ctx *FunctionNameContext) {}

// ExitFunctionName is called when production functionName is exited.
func (s *BaseSqlBaseParserListener) ExitFunctionName(ctx *FunctionNameContext) {}

// EnterQualifiedName is called when production qualifiedName is entered.
func (s *BaseSqlBaseParserListener) EnterQualifiedName(ctx *QualifiedNameContext) {}

// ExitQualifiedName is called when production qualifiedName is exited.
func (s *BaseSqlBaseParserListener) ExitQualifiedName(ctx *QualifiedNameContext) {}

// EnterErrorCapturingIdentifier is called when production errorCapturingIdentifier is entered.
func (s *BaseSqlBaseParserListener) EnterErrorCapturingIdentifier(ctx *ErrorCapturingIdentifierContext) {
}

// ExitErrorCapturingIdentifier is called when production errorCapturingIdentifier is exited.
func (s *BaseSqlBaseParserListener) ExitErrorCapturingIdentifier(ctx *ErrorCapturingIdentifierContext) {
}

// EnterErrorIdent is called when production errorIdent is entered.
func (s *BaseSqlBaseParserListener) EnterErrorIdent(ctx *ErrorIdentContext) {}

// ExitErrorIdent is called when production errorIdent is exited.
func (s *BaseSqlBaseParserListener) ExitErrorIdent(ctx *ErrorIdentContext) {}

// EnterRealIdent is called when production realIdent is entered.
func (s *BaseSqlBaseParserListener) EnterRealIdent(ctx *RealIdentContext) {}

// ExitRealIdent is called when production realIdent is exited.
func (s *BaseSqlBaseParserListener) ExitRealIdent(ctx *RealIdentContext) {}

// EnterIdentifier is called when production identifier is entered.
func (s *BaseSqlBaseParserListener) EnterIdentifier(ctx *IdentifierContext) {}

// ExitIdentifier is called when production identifier is exited.
func (s *BaseSqlBaseParserListener) ExitIdentifier(ctx *IdentifierContext) {}

// EnterSimpleIdentifier is called when production simpleIdentifier is entered.
func (s *BaseSqlBaseParserListener) EnterSimpleIdentifier(ctx *SimpleIdentifierContext) {}

// ExitSimpleIdentifier is called when production simpleIdentifier is exited.
func (s *BaseSqlBaseParserListener) ExitSimpleIdentifier(ctx *SimpleIdentifierContext) {}

// EnterUnquotedIdentifier is called when production unquotedIdentifier is entered.
func (s *BaseSqlBaseParserListener) EnterUnquotedIdentifier(ctx *UnquotedIdentifierContext) {}

// ExitUnquotedIdentifier is called when production unquotedIdentifier is exited.
func (s *BaseSqlBaseParserListener) ExitUnquotedIdentifier(ctx *UnquotedIdentifierContext) {}

// EnterQuotedIdentifierAlternative is called when production quotedIdentifierAlternative is entered.
func (s *BaseSqlBaseParserListener) EnterQuotedIdentifierAlternative(ctx *QuotedIdentifierAlternativeContext) {
}

// ExitQuotedIdentifierAlternative is called when production quotedIdentifierAlternative is exited.
func (s *BaseSqlBaseParserListener) ExitQuotedIdentifierAlternative(ctx *QuotedIdentifierAlternativeContext) {
}

// EnterIdentifierLiteral is called when production identifierLiteral is entered.
func (s *BaseSqlBaseParserListener) EnterIdentifierLiteral(ctx *IdentifierLiteralContext) {}

// ExitIdentifierLiteral is called when production identifierLiteral is exited.
func (s *BaseSqlBaseParserListener) ExitIdentifierLiteral(ctx *IdentifierLiteralContext) {}

// EnterSimpleUnquotedIdentifier is called when production simpleUnquotedIdentifier is entered.
func (s *BaseSqlBaseParserListener) EnterSimpleUnquotedIdentifier(ctx *SimpleUnquotedIdentifierContext) {
}

// ExitSimpleUnquotedIdentifier is called when production simpleUnquotedIdentifier is exited.
func (s *BaseSqlBaseParserListener) ExitSimpleUnquotedIdentifier(ctx *SimpleUnquotedIdentifierContext) {
}

// EnterSimpleQuotedIdentifierAlternative is called when production simpleQuotedIdentifierAlternative is entered.
func (s *BaseSqlBaseParserListener) EnterSimpleQuotedIdentifierAlternative(ctx *SimpleQuotedIdentifierAlternativeContext) {
}

// ExitSimpleQuotedIdentifierAlternative is called when production simpleQuotedIdentifierAlternative is exited.
func (s *BaseSqlBaseParserListener) ExitSimpleQuotedIdentifierAlternative(ctx *SimpleQuotedIdentifierAlternativeContext) {
}

// EnterQuotedIdentifier is called when production quotedIdentifier is entered.
func (s *BaseSqlBaseParserListener) EnterQuotedIdentifier(ctx *QuotedIdentifierContext) {}

// ExitQuotedIdentifier is called when production quotedIdentifier is exited.
func (s *BaseSqlBaseParserListener) ExitQuotedIdentifier(ctx *QuotedIdentifierContext) {}

// EnterBackQuotedIdentifier is called when production backQuotedIdentifier is entered.
func (s *BaseSqlBaseParserListener) EnterBackQuotedIdentifier(ctx *BackQuotedIdentifierContext) {}

// ExitBackQuotedIdentifier is called when production backQuotedIdentifier is exited.
func (s *BaseSqlBaseParserListener) ExitBackQuotedIdentifier(ctx *BackQuotedIdentifierContext) {}

// EnterExponentLiteral is called when production exponentLiteral is entered.
func (s *BaseSqlBaseParserListener) EnterExponentLiteral(ctx *ExponentLiteralContext) {}

// ExitExponentLiteral is called when production exponentLiteral is exited.
func (s *BaseSqlBaseParserListener) ExitExponentLiteral(ctx *ExponentLiteralContext) {}

// EnterDecimalLiteral is called when production decimalLiteral is entered.
func (s *BaseSqlBaseParserListener) EnterDecimalLiteral(ctx *DecimalLiteralContext) {}

// ExitDecimalLiteral is called when production decimalLiteral is exited.
func (s *BaseSqlBaseParserListener) ExitDecimalLiteral(ctx *DecimalLiteralContext) {}

// EnterLegacyDecimalLiteral is called when production legacyDecimalLiteral is entered.
func (s *BaseSqlBaseParserListener) EnterLegacyDecimalLiteral(ctx *LegacyDecimalLiteralContext) {}

// ExitLegacyDecimalLiteral is called when production legacyDecimalLiteral is exited.
func (s *BaseSqlBaseParserListener) ExitLegacyDecimalLiteral(ctx *LegacyDecimalLiteralContext) {}

// EnterIntegerLiteral is called when production integerLiteral is entered.
func (s *BaseSqlBaseParserListener) EnterIntegerLiteral(ctx *IntegerLiteralContext) {}

// ExitIntegerLiteral is called when production integerLiteral is exited.
func (s *BaseSqlBaseParserListener) ExitIntegerLiteral(ctx *IntegerLiteralContext) {}

// EnterBigIntLiteral is called when production bigIntLiteral is entered.
func (s *BaseSqlBaseParserListener) EnterBigIntLiteral(ctx *BigIntLiteralContext) {}

// ExitBigIntLiteral is called when production bigIntLiteral is exited.
func (s *BaseSqlBaseParserListener) ExitBigIntLiteral(ctx *BigIntLiteralContext) {}

// EnterSmallIntLiteral is called when production smallIntLiteral is entered.
func (s *BaseSqlBaseParserListener) EnterSmallIntLiteral(ctx *SmallIntLiteralContext) {}

// ExitSmallIntLiteral is called when production smallIntLiteral is exited.
func (s *BaseSqlBaseParserListener) ExitSmallIntLiteral(ctx *SmallIntLiteralContext) {}

// EnterTinyIntLiteral is called when production tinyIntLiteral is entered.
func (s *BaseSqlBaseParserListener) EnterTinyIntLiteral(ctx *TinyIntLiteralContext) {}

// ExitTinyIntLiteral is called when production tinyIntLiteral is exited.
func (s *BaseSqlBaseParserListener) ExitTinyIntLiteral(ctx *TinyIntLiteralContext) {}

// EnterDoubleLiteral is called when production doubleLiteral is entered.
func (s *BaseSqlBaseParserListener) EnterDoubleLiteral(ctx *DoubleLiteralContext) {}

// ExitDoubleLiteral is called when production doubleLiteral is exited.
func (s *BaseSqlBaseParserListener) ExitDoubleLiteral(ctx *DoubleLiteralContext) {}

// EnterFloatLiteral is called when production floatLiteral is entered.
func (s *BaseSqlBaseParserListener) EnterFloatLiteral(ctx *FloatLiteralContext) {}

// ExitFloatLiteral is called when production floatLiteral is exited.
func (s *BaseSqlBaseParserListener) ExitFloatLiteral(ctx *FloatLiteralContext) {}

// EnterBigDecimalLiteral is called when production bigDecimalLiteral is entered.
func (s *BaseSqlBaseParserListener) EnterBigDecimalLiteral(ctx *BigDecimalLiteralContext) {}

// ExitBigDecimalLiteral is called when production bigDecimalLiteral is exited.
func (s *BaseSqlBaseParserListener) ExitBigDecimalLiteral(ctx *BigDecimalLiteralContext) {}

// EnterIntegerVal is called when production integerVal is entered.
func (s *BaseSqlBaseParserListener) EnterIntegerVal(ctx *IntegerValContext) {}

// ExitIntegerVal is called when production integerVal is exited.
func (s *BaseSqlBaseParserListener) ExitIntegerVal(ctx *IntegerValContext) {}

// EnterParameterIntegerValue is called when production parameterIntegerValue is entered.
func (s *BaseSqlBaseParserListener) EnterParameterIntegerValue(ctx *ParameterIntegerValueContext) {}

// ExitParameterIntegerValue is called when production parameterIntegerValue is exited.
func (s *BaseSqlBaseParserListener) ExitParameterIntegerValue(ctx *ParameterIntegerValueContext) {}

// EnterColumnConstraintDefinition is called when production columnConstraintDefinition is entered.
func (s *BaseSqlBaseParserListener) EnterColumnConstraintDefinition(ctx *ColumnConstraintDefinitionContext) {
}

// ExitColumnConstraintDefinition is called when production columnConstraintDefinition is exited.
func (s *BaseSqlBaseParserListener) ExitColumnConstraintDefinition(ctx *ColumnConstraintDefinitionContext) {
}

// EnterColumnConstraint is called when production columnConstraint is entered.
func (s *BaseSqlBaseParserListener) EnterColumnConstraint(ctx *ColumnConstraintContext) {}

// ExitColumnConstraint is called when production columnConstraint is exited.
func (s *BaseSqlBaseParserListener) ExitColumnConstraint(ctx *ColumnConstraintContext) {}

// EnterTableConstraintDefinition is called when production tableConstraintDefinition is entered.
func (s *BaseSqlBaseParserListener) EnterTableConstraintDefinition(ctx *TableConstraintDefinitionContext) {
}

// ExitTableConstraintDefinition is called when production tableConstraintDefinition is exited.
func (s *BaseSqlBaseParserListener) ExitTableConstraintDefinition(ctx *TableConstraintDefinitionContext) {
}

// EnterTableConstraint is called when production tableConstraint is entered.
func (s *BaseSqlBaseParserListener) EnterTableConstraint(ctx *TableConstraintContext) {}

// ExitTableConstraint is called when production tableConstraint is exited.
func (s *BaseSqlBaseParserListener) ExitTableConstraint(ctx *TableConstraintContext) {}

// EnterCheckConstraint is called when production checkConstraint is entered.
func (s *BaseSqlBaseParserListener) EnterCheckConstraint(ctx *CheckConstraintContext) {}

// ExitCheckConstraint is called when production checkConstraint is exited.
func (s *BaseSqlBaseParserListener) ExitCheckConstraint(ctx *CheckConstraintContext) {}

// EnterUniqueSpec is called when production uniqueSpec is entered.
func (s *BaseSqlBaseParserListener) EnterUniqueSpec(ctx *UniqueSpecContext) {}

// ExitUniqueSpec is called when production uniqueSpec is exited.
func (s *BaseSqlBaseParserListener) ExitUniqueSpec(ctx *UniqueSpecContext) {}

// EnterUniqueConstraint is called when production uniqueConstraint is entered.
func (s *BaseSqlBaseParserListener) EnterUniqueConstraint(ctx *UniqueConstraintContext) {}

// ExitUniqueConstraint is called when production uniqueConstraint is exited.
func (s *BaseSqlBaseParserListener) ExitUniqueConstraint(ctx *UniqueConstraintContext) {}

// EnterReferenceSpec is called when production referenceSpec is entered.
func (s *BaseSqlBaseParserListener) EnterReferenceSpec(ctx *ReferenceSpecContext) {}

// ExitReferenceSpec is called when production referenceSpec is exited.
func (s *BaseSqlBaseParserListener) ExitReferenceSpec(ctx *ReferenceSpecContext) {}

// EnterForeignKeyConstraint is called when production foreignKeyConstraint is entered.
func (s *BaseSqlBaseParserListener) EnterForeignKeyConstraint(ctx *ForeignKeyConstraintContext) {}

// ExitForeignKeyConstraint is called when production foreignKeyConstraint is exited.
func (s *BaseSqlBaseParserListener) ExitForeignKeyConstraint(ctx *ForeignKeyConstraintContext) {}

// EnterConstraintCharacteristic is called when production constraintCharacteristic is entered.
func (s *BaseSqlBaseParserListener) EnterConstraintCharacteristic(ctx *ConstraintCharacteristicContext) {
}

// ExitConstraintCharacteristic is called when production constraintCharacteristic is exited.
func (s *BaseSqlBaseParserListener) ExitConstraintCharacteristic(ctx *ConstraintCharacteristicContext) {
}

// EnterEnforcedCharacteristic is called when production enforcedCharacteristic is entered.
func (s *BaseSqlBaseParserListener) EnterEnforcedCharacteristic(ctx *EnforcedCharacteristicContext) {}

// ExitEnforcedCharacteristic is called when production enforcedCharacteristic is exited.
func (s *BaseSqlBaseParserListener) ExitEnforcedCharacteristic(ctx *EnforcedCharacteristicContext) {}

// EnterRelyCharacteristic is called when production relyCharacteristic is entered.
func (s *BaseSqlBaseParserListener) EnterRelyCharacteristic(ctx *RelyCharacteristicContext) {}

// ExitRelyCharacteristic is called when production relyCharacteristic is exited.
func (s *BaseSqlBaseParserListener) ExitRelyCharacteristic(ctx *RelyCharacteristicContext) {}

// EnterAlterColumnSpecList is called when production alterColumnSpecList is entered.
func (s *BaseSqlBaseParserListener) EnterAlterColumnSpecList(ctx *AlterColumnSpecListContext) {}

// ExitAlterColumnSpecList is called when production alterColumnSpecList is exited.
func (s *BaseSqlBaseParserListener) ExitAlterColumnSpecList(ctx *AlterColumnSpecListContext) {}

// EnterAlterColumnSpec is called when production alterColumnSpec is entered.
func (s *BaseSqlBaseParserListener) EnterAlterColumnSpec(ctx *AlterColumnSpecContext) {}

// ExitAlterColumnSpec is called when production alterColumnSpec is exited.
func (s *BaseSqlBaseParserListener) ExitAlterColumnSpec(ctx *AlterColumnSpecContext) {}

// EnterAlterColumnAction is called when production alterColumnAction is entered.
func (s *BaseSqlBaseParserListener) EnterAlterColumnAction(ctx *AlterColumnActionContext) {}

// ExitAlterColumnAction is called when production alterColumnAction is exited.
func (s *BaseSqlBaseParserListener) ExitAlterColumnAction(ctx *AlterColumnActionContext) {}

// EnterSingleStringLiteralValue is called when production singleStringLiteralValue is entered.
func (s *BaseSqlBaseParserListener) EnterSingleStringLiteralValue(ctx *SingleStringLiteralValueContext) {
}

// ExitSingleStringLiteralValue is called when production singleStringLiteralValue is exited.
func (s *BaseSqlBaseParserListener) ExitSingleStringLiteralValue(ctx *SingleStringLiteralValueContext) {
}

// EnterSingleDoubleQuotedStringLiteralValue is called when production singleDoubleQuotedStringLiteralValue is entered.
func (s *BaseSqlBaseParserListener) EnterSingleDoubleQuotedStringLiteralValue(ctx *SingleDoubleQuotedStringLiteralValueContext) {
}

// ExitSingleDoubleQuotedStringLiteralValue is called when production singleDoubleQuotedStringLiteralValue is exited.
func (s *BaseSqlBaseParserListener) ExitSingleDoubleQuotedStringLiteralValue(ctx *SingleDoubleQuotedStringLiteralValueContext) {
}

// EnterSingleStringLit is called when production singleStringLit is entered.
func (s *BaseSqlBaseParserListener) EnterSingleStringLit(ctx *SingleStringLitContext) {}

// ExitSingleStringLit is called when production singleStringLit is exited.
func (s *BaseSqlBaseParserListener) ExitSingleStringLit(ctx *SingleStringLitContext) {}

// EnterNamedParameterMarkerRule is called when production namedParameterMarkerRule is entered.
func (s *BaseSqlBaseParserListener) EnterNamedParameterMarkerRule(ctx *NamedParameterMarkerRuleContext) {
}

// ExitNamedParameterMarkerRule is called when production namedParameterMarkerRule is exited.
func (s *BaseSqlBaseParserListener) ExitNamedParameterMarkerRule(ctx *NamedParameterMarkerRuleContext) {
}

// EnterPositionalParameterMarkerRule is called when production positionalParameterMarkerRule is entered.
func (s *BaseSqlBaseParserListener) EnterPositionalParameterMarkerRule(ctx *PositionalParameterMarkerRuleContext) {
}

// ExitPositionalParameterMarkerRule is called when production positionalParameterMarkerRule is exited.
func (s *BaseSqlBaseParserListener) ExitPositionalParameterMarkerRule(ctx *PositionalParameterMarkerRuleContext) {
}

// EnterStringLit is called when production stringLit is entered.
func (s *BaseSqlBaseParserListener) EnterStringLit(ctx *StringLitContext) {}

// ExitStringLit is called when production stringLit is exited.
func (s *BaseSqlBaseParserListener) ExitStringLit(ctx *StringLitContext) {}

// EnterComment is called when production comment is entered.
func (s *BaseSqlBaseParserListener) EnterComment(ctx *CommentContext) {}

// ExitComment is called when production comment is exited.
func (s *BaseSqlBaseParserListener) ExitComment(ctx *CommentContext) {}

// EnterVersion is called when production version is entered.
func (s *BaseSqlBaseParserListener) EnterVersion(ctx *VersionContext) {}

// ExitVersion is called when production version is exited.
func (s *BaseSqlBaseParserListener) ExitVersion(ctx *VersionContext) {}

// EnterOperatorPipeRightSide is called when production operatorPipeRightSide is entered.
func (s *BaseSqlBaseParserListener) EnterOperatorPipeRightSide(ctx *OperatorPipeRightSideContext) {}

// ExitOperatorPipeRightSide is called when production operatorPipeRightSide is exited.
func (s *BaseSqlBaseParserListener) ExitOperatorPipeRightSide(ctx *OperatorPipeRightSideContext) {}

// EnterOperatorPipeSetAssignmentSeq is called when production operatorPipeSetAssignmentSeq is entered.
func (s *BaseSqlBaseParserListener) EnterOperatorPipeSetAssignmentSeq(ctx *OperatorPipeSetAssignmentSeqContext) {
}

// ExitOperatorPipeSetAssignmentSeq is called when production operatorPipeSetAssignmentSeq is exited.
func (s *BaseSqlBaseParserListener) ExitOperatorPipeSetAssignmentSeq(ctx *OperatorPipeSetAssignmentSeqContext) {
}

// EnterAnsiNonReserved is called when production ansiNonReserved is entered.
func (s *BaseSqlBaseParserListener) EnterAnsiNonReserved(ctx *AnsiNonReservedContext) {}

// ExitAnsiNonReserved is called when production ansiNonReserved is exited.
func (s *BaseSqlBaseParserListener) ExitAnsiNonReserved(ctx *AnsiNonReservedContext) {}

// EnterStrictNonReserved is called when production strictNonReserved is entered.
func (s *BaseSqlBaseParserListener) EnterStrictNonReserved(ctx *StrictNonReservedContext) {}

// ExitStrictNonReserved is called when production strictNonReserved is exited.
func (s *BaseSqlBaseParserListener) ExitStrictNonReserved(ctx *StrictNonReservedContext) {}

// EnterNonReserved is called when production nonReserved is entered.
func (s *BaseSqlBaseParserListener) EnterNonReserved(ctx *NonReservedContext) {}

// ExitNonReserved is called when production nonReserved is exited.
func (s *BaseSqlBaseParserListener) ExitNonReserved(ctx *NonReservedContext) {}
