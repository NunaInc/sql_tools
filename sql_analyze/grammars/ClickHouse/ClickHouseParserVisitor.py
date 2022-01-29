# Generated from ClickHouseParser.g4 by ANTLR 4.9.3
from antlr4 import *
if __name__ is not None and "." in __name__:
    from .ClickHouseParser import ClickHouseParser
else:
    from ClickHouseParser import ClickHouseParser

# This class defines a complete generic visitor for a parse tree produced by ClickHouseParser.

class ClickHouseParserVisitor(ParseTreeVisitor):

    # Visit a parse tree produced by ClickHouseParser#multiQueryStmt.
    def visitMultiQueryStmt(self, ctx:ClickHouseParser.MultiQueryStmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#queryStmt.
    def visitQueryStmt(self, ctx:ClickHouseParser.QueryStmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#query.
    def visitQuery(self, ctx:ClickHouseParser.QueryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#AlterTableStmt.
    def visitAlterTableStmt(self, ctx:ClickHouseParser.AlterTableStmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#AlterTableClauseAddColumn.
    def visitAlterTableClauseAddColumn(self, ctx:ClickHouseParser.AlterTableClauseAddColumnContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#AlterTableClauseAddIndex.
    def visitAlterTableClauseAddIndex(self, ctx:ClickHouseParser.AlterTableClauseAddIndexContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#AlterTableClauseAddProjection.
    def visitAlterTableClauseAddProjection(self, ctx:ClickHouseParser.AlterTableClauseAddProjectionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#AlterTableClauseAttach.
    def visitAlterTableClauseAttach(self, ctx:ClickHouseParser.AlterTableClauseAttachContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#AlterTableClauseClearColumn.
    def visitAlterTableClauseClearColumn(self, ctx:ClickHouseParser.AlterTableClauseClearColumnContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#AlterTableClauseClearIndex.
    def visitAlterTableClauseClearIndex(self, ctx:ClickHouseParser.AlterTableClauseClearIndexContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#AlterTableClauseClearProjection.
    def visitAlterTableClauseClearProjection(self, ctx:ClickHouseParser.AlterTableClauseClearProjectionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#AlterTableClauseComment.
    def visitAlterTableClauseComment(self, ctx:ClickHouseParser.AlterTableClauseCommentContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#AlterTableClauseDelete.
    def visitAlterTableClauseDelete(self, ctx:ClickHouseParser.AlterTableClauseDeleteContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#AlterTableClauseDetach.
    def visitAlterTableClauseDetach(self, ctx:ClickHouseParser.AlterTableClauseDetachContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#AlterTableClauseDropColumn.
    def visitAlterTableClauseDropColumn(self, ctx:ClickHouseParser.AlterTableClauseDropColumnContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#AlterTableClauseDropIndex.
    def visitAlterTableClauseDropIndex(self, ctx:ClickHouseParser.AlterTableClauseDropIndexContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#AlterTableClauseDropProjection.
    def visitAlterTableClauseDropProjection(self, ctx:ClickHouseParser.AlterTableClauseDropProjectionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#AlterTableClauseDropPartition.
    def visitAlterTableClauseDropPartition(self, ctx:ClickHouseParser.AlterTableClauseDropPartitionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#AlterTableClauseFreezePartition.
    def visitAlterTableClauseFreezePartition(self, ctx:ClickHouseParser.AlterTableClauseFreezePartitionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#AlterTableClauseMaterializeIndex.
    def visitAlterTableClauseMaterializeIndex(self, ctx:ClickHouseParser.AlterTableClauseMaterializeIndexContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#AlterTableClauseMaterializeProjection.
    def visitAlterTableClauseMaterializeProjection(self, ctx:ClickHouseParser.AlterTableClauseMaterializeProjectionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#AlterTableClauseModifyCodec.
    def visitAlterTableClauseModifyCodec(self, ctx:ClickHouseParser.AlterTableClauseModifyCodecContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#AlterTableClauseModifyComment.
    def visitAlterTableClauseModifyComment(self, ctx:ClickHouseParser.AlterTableClauseModifyCommentContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#AlterTableClauseModifyRemove.
    def visitAlterTableClauseModifyRemove(self, ctx:ClickHouseParser.AlterTableClauseModifyRemoveContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#AlterTableClauseModify.
    def visitAlterTableClauseModify(self, ctx:ClickHouseParser.AlterTableClauseModifyContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#AlterTableClauseModifyOrderBy.
    def visitAlterTableClauseModifyOrderBy(self, ctx:ClickHouseParser.AlterTableClauseModifyOrderByContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#AlterTableClauseModifyTTL.
    def visitAlterTableClauseModifyTTL(self, ctx:ClickHouseParser.AlterTableClauseModifyTTLContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#AlterTableClauseMovePartition.
    def visitAlterTableClauseMovePartition(self, ctx:ClickHouseParser.AlterTableClauseMovePartitionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#AlterTableClauseRemoveTTL.
    def visitAlterTableClauseRemoveTTL(self, ctx:ClickHouseParser.AlterTableClauseRemoveTTLContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#AlterTableClauseRename.
    def visitAlterTableClauseRename(self, ctx:ClickHouseParser.AlterTableClauseRenameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#AlterTableClauseReplace.
    def visitAlterTableClauseReplace(self, ctx:ClickHouseParser.AlterTableClauseReplaceContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#AlterTableClauseUpdate.
    def visitAlterTableClauseUpdate(self, ctx:ClickHouseParser.AlterTableClauseUpdateContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#assignmentExprList.
    def visitAssignmentExprList(self, ctx:ClickHouseParser.AssignmentExprListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#assignmentExpr.
    def visitAssignmentExpr(self, ctx:ClickHouseParser.AssignmentExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#tableColumnPropertyType.
    def visitTableColumnPropertyType(self, ctx:ClickHouseParser.TableColumnPropertyTypeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#partitionClause.
    def visitPartitionClause(self, ctx:ClickHouseParser.PartitionClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#AttachDictionaryStmt.
    def visitAttachDictionaryStmt(self, ctx:ClickHouseParser.AttachDictionaryStmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#checkStmt.
    def visitCheckStmt(self, ctx:ClickHouseParser.CheckStmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#CreateDatabaseStmt.
    def visitCreateDatabaseStmt(self, ctx:ClickHouseParser.CreateDatabaseStmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#CreateDictionaryStmt.
    def visitCreateDictionaryStmt(self, ctx:ClickHouseParser.CreateDictionaryStmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#CreateLiveViewStmt.
    def visitCreateLiveViewStmt(self, ctx:ClickHouseParser.CreateLiveViewStmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#CreateMaterializedViewStmt.
    def visitCreateMaterializedViewStmt(self, ctx:ClickHouseParser.CreateMaterializedViewStmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#CreateTableStmt.
    def visitCreateTableStmt(self, ctx:ClickHouseParser.CreateTableStmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#CreateViewStmt.
    def visitCreateViewStmt(self, ctx:ClickHouseParser.CreateViewStmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#dictionarySchemaClause.
    def visitDictionarySchemaClause(self, ctx:ClickHouseParser.DictionarySchemaClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#dictionaryAttrDfnt.
    def visitDictionaryAttrDfnt(self, ctx:ClickHouseParser.DictionaryAttrDfntContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#dictionaryEngineClause.
    def visitDictionaryEngineClause(self, ctx:ClickHouseParser.DictionaryEngineClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#dictionaryPrimaryKeyClause.
    def visitDictionaryPrimaryKeyClause(self, ctx:ClickHouseParser.DictionaryPrimaryKeyClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#dictionaryArgExpr.
    def visitDictionaryArgExpr(self, ctx:ClickHouseParser.DictionaryArgExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#sourceClause.
    def visitSourceClause(self, ctx:ClickHouseParser.SourceClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#lifetimeClause.
    def visitLifetimeClause(self, ctx:ClickHouseParser.LifetimeClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#layoutClause.
    def visitLayoutClause(self, ctx:ClickHouseParser.LayoutClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#rangeClause.
    def visitRangeClause(self, ctx:ClickHouseParser.RangeClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#dictionarySettingsClause.
    def visitDictionarySettingsClause(self, ctx:ClickHouseParser.DictionarySettingsClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#clusterClause.
    def visitClusterClause(self, ctx:ClickHouseParser.ClusterClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#uuidClause.
    def visitUuidClause(self, ctx:ClickHouseParser.UuidClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#destinationClause.
    def visitDestinationClause(self, ctx:ClickHouseParser.DestinationClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#subqueryClause.
    def visitSubqueryClause(self, ctx:ClickHouseParser.SubqueryClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#SchemaDescriptionClause.
    def visitSchemaDescriptionClause(self, ctx:ClickHouseParser.SchemaDescriptionClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#SchemaAsTableClause.
    def visitSchemaAsTableClause(self, ctx:ClickHouseParser.SchemaAsTableClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#SchemaAsFunctionClause.
    def visitSchemaAsFunctionClause(self, ctx:ClickHouseParser.SchemaAsFunctionClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#engineClause.
    def visitEngineClause(self, ctx:ClickHouseParser.EngineClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#partitionByClause.
    def visitPartitionByClause(self, ctx:ClickHouseParser.PartitionByClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#primaryKeyClause.
    def visitPrimaryKeyClause(self, ctx:ClickHouseParser.PrimaryKeyClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#sampleByClause.
    def visitSampleByClause(self, ctx:ClickHouseParser.SampleByClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#ttlClause.
    def visitTtlClause(self, ctx:ClickHouseParser.TtlClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#engineExpr.
    def visitEngineExpr(self, ctx:ClickHouseParser.EngineExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#TableElementExprColumn.
    def visitTableElementExprColumn(self, ctx:ClickHouseParser.TableElementExprColumnContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#TableElementExprConstraint.
    def visitTableElementExprConstraint(self, ctx:ClickHouseParser.TableElementExprConstraintContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#TableElementExprIndex.
    def visitTableElementExprIndex(self, ctx:ClickHouseParser.TableElementExprIndexContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#TableElementExprProjection.
    def visitTableElementExprProjection(self, ctx:ClickHouseParser.TableElementExprProjectionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#tableColumnDfnt.
    def visitTableColumnDfnt(self, ctx:ClickHouseParser.TableColumnDfntContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#tableColumnPropertyExpr.
    def visitTableColumnPropertyExpr(self, ctx:ClickHouseParser.TableColumnPropertyExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#tableIndexDfnt.
    def visitTableIndexDfnt(self, ctx:ClickHouseParser.TableIndexDfntContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#tableProjectionDfnt.
    def visitTableProjectionDfnt(self, ctx:ClickHouseParser.TableProjectionDfntContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#codecExpr.
    def visitCodecExpr(self, ctx:ClickHouseParser.CodecExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#codecArgExpr.
    def visitCodecArgExpr(self, ctx:ClickHouseParser.CodecArgExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#ttlExpr.
    def visitTtlExpr(self, ctx:ClickHouseParser.TtlExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#describeStmt.
    def visitDescribeStmt(self, ctx:ClickHouseParser.DescribeStmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#DropDatabaseStmt.
    def visitDropDatabaseStmt(self, ctx:ClickHouseParser.DropDatabaseStmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#DropTableStmt.
    def visitDropTableStmt(self, ctx:ClickHouseParser.DropTableStmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#ExistsDatabaseStmt.
    def visitExistsDatabaseStmt(self, ctx:ClickHouseParser.ExistsDatabaseStmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#ExistsTableStmt.
    def visitExistsTableStmt(self, ctx:ClickHouseParser.ExistsTableStmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#ExplainASTStmt.
    def visitExplainASTStmt(self, ctx:ClickHouseParser.ExplainASTStmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#ExplainSyntaxStmt.
    def visitExplainSyntaxStmt(self, ctx:ClickHouseParser.ExplainSyntaxStmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#insertStmt.
    def visitInsertStmt(self, ctx:ClickHouseParser.InsertStmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#columnsClause.
    def visitColumnsClause(self, ctx:ClickHouseParser.ColumnsClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#DataClauseFormat.
    def visitDataClauseFormat(self, ctx:ClickHouseParser.DataClauseFormatContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#DataClauseValues.
    def visitDataClauseValues(self, ctx:ClickHouseParser.DataClauseValuesContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#DataClauseSelect.
    def visitDataClauseSelect(self, ctx:ClickHouseParser.DataClauseSelectContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#KillMutationStmt.
    def visitKillMutationStmt(self, ctx:ClickHouseParser.KillMutationStmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#optimizeStmt.
    def visitOptimizeStmt(self, ctx:ClickHouseParser.OptimizeStmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#renameStmt.
    def visitRenameStmt(self, ctx:ClickHouseParser.RenameStmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#projectionSelectStmt.
    def visitProjectionSelectStmt(self, ctx:ClickHouseParser.ProjectionSelectStmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#selectUnionStmt.
    def visitSelectUnionStmt(self, ctx:ClickHouseParser.SelectUnionStmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#selectStmtWithParens.
    def visitSelectStmtWithParens(self, ctx:ClickHouseParser.SelectStmtWithParensContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#selectStmt.
    def visitSelectStmt(self, ctx:ClickHouseParser.SelectStmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#withClause.
    def visitWithClause(self, ctx:ClickHouseParser.WithClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#topClause.
    def visitTopClause(self, ctx:ClickHouseParser.TopClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#fromClause.
    def visitFromClause(self, ctx:ClickHouseParser.FromClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#arrayJoinClause.
    def visitArrayJoinClause(self, ctx:ClickHouseParser.ArrayJoinClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#windowClause.
    def visitWindowClause(self, ctx:ClickHouseParser.WindowClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#prewhereClause.
    def visitPrewhereClause(self, ctx:ClickHouseParser.PrewhereClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#whereClause.
    def visitWhereClause(self, ctx:ClickHouseParser.WhereClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#groupByClause.
    def visitGroupByClause(self, ctx:ClickHouseParser.GroupByClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#groupByRollupClause.
    def visitGroupByRollupClause(self, ctx:ClickHouseParser.GroupByRollupClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#groupByTotalsClause.
    def visitGroupByTotalsClause(self, ctx:ClickHouseParser.GroupByTotalsClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#havingClause.
    def visitHavingClause(self, ctx:ClickHouseParser.HavingClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#orderByClause.
    def visitOrderByClause(self, ctx:ClickHouseParser.OrderByClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#projectionOrderByClause.
    def visitProjectionOrderByClause(self, ctx:ClickHouseParser.ProjectionOrderByClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#limitByClause.
    def visitLimitByClause(self, ctx:ClickHouseParser.LimitByClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#limitClause.
    def visitLimitClause(self, ctx:ClickHouseParser.LimitClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#settingsClause.
    def visitSettingsClause(self, ctx:ClickHouseParser.SettingsClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#withExprList.
    def visitWithExprList(self, ctx:ClickHouseParser.WithExprListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#withExpr.
    def visitWithExpr(self, ctx:ClickHouseParser.WithExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#JoinExprOp.
    def visitJoinExprOp(self, ctx:ClickHouseParser.JoinExprOpContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#JoinExprTable.
    def visitJoinExprTable(self, ctx:ClickHouseParser.JoinExprTableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#JoinExprParens.
    def visitJoinExprParens(self, ctx:ClickHouseParser.JoinExprParensContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#JoinExprCrossOp.
    def visitJoinExprCrossOp(self, ctx:ClickHouseParser.JoinExprCrossOpContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#JoinOpInner.
    def visitJoinOpInner(self, ctx:ClickHouseParser.JoinOpInnerContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#JoinOpLeftRight.
    def visitJoinOpLeftRight(self, ctx:ClickHouseParser.JoinOpLeftRightContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#JoinOpFull.
    def visitJoinOpFull(self, ctx:ClickHouseParser.JoinOpFullContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#joinOpCross.
    def visitJoinOpCross(self, ctx:ClickHouseParser.JoinOpCrossContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#joinConstraintClause.
    def visitJoinConstraintClause(self, ctx:ClickHouseParser.JoinConstraintClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#sampleClause.
    def visitSampleClause(self, ctx:ClickHouseParser.SampleClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#limitExpr.
    def visitLimitExpr(self, ctx:ClickHouseParser.LimitExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#orderExprList.
    def visitOrderExprList(self, ctx:ClickHouseParser.OrderExprListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#orderExpr.
    def visitOrderExpr(self, ctx:ClickHouseParser.OrderExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#orderExprDecoration.
    def visitOrderExprDecoration(self, ctx:ClickHouseParser.OrderExprDecorationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#ratioExpr.
    def visitRatioExpr(self, ctx:ClickHouseParser.RatioExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#settingExprList.
    def visitSettingExprList(self, ctx:ClickHouseParser.SettingExprListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#settingExpr.
    def visitSettingExpr(self, ctx:ClickHouseParser.SettingExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#windowExpr.
    def visitWindowExpr(self, ctx:ClickHouseParser.WindowExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#winPartitionByClause.
    def visitWinPartitionByClause(self, ctx:ClickHouseParser.WinPartitionByClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#winOrderByClause.
    def visitWinOrderByClause(self, ctx:ClickHouseParser.WinOrderByClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#winFrameClause.
    def visitWinFrameClause(self, ctx:ClickHouseParser.WinFrameClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#frameStart.
    def visitFrameStart(self, ctx:ClickHouseParser.FrameStartContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#frameBetween.
    def visitFrameBetween(self, ctx:ClickHouseParser.FrameBetweenContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#winFrameBound.
    def visitWinFrameBound(self, ctx:ClickHouseParser.WinFrameBoundContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#setStmt.
    def visitSetStmt(self, ctx:ClickHouseParser.SetStmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#showCreateDatabaseStmt.
    def visitShowCreateDatabaseStmt(self, ctx:ClickHouseParser.ShowCreateDatabaseStmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#showCreateDictionaryStmt.
    def visitShowCreateDictionaryStmt(self, ctx:ClickHouseParser.ShowCreateDictionaryStmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#showCreateTableStmt.
    def visitShowCreateTableStmt(self, ctx:ClickHouseParser.ShowCreateTableStmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#showDatabasesStmt.
    def visitShowDatabasesStmt(self, ctx:ClickHouseParser.ShowDatabasesStmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#showDictionariesStmt.
    def visitShowDictionariesStmt(self, ctx:ClickHouseParser.ShowDictionariesStmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#showTablesStmt.
    def visitShowTablesStmt(self, ctx:ClickHouseParser.ShowTablesStmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#systemStmt.
    def visitSystemStmt(self, ctx:ClickHouseParser.SystemStmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#truncateStmt.
    def visitTruncateStmt(self, ctx:ClickHouseParser.TruncateStmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#useStmt.
    def visitUseStmt(self, ctx:ClickHouseParser.UseStmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#watchStmt.
    def visitWatchStmt(self, ctx:ClickHouseParser.WatchStmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#ColumnTypeExprSimple.
    def visitColumnTypeExprSimple(self, ctx:ClickHouseParser.ColumnTypeExprSimpleContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#ColumnTypeExprNested.
    def visitColumnTypeExprNested(self, ctx:ClickHouseParser.ColumnTypeExprNestedContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#ColumnTypeExprEnum.
    def visitColumnTypeExprEnum(self, ctx:ClickHouseParser.ColumnTypeExprEnumContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#ColumnTypeExprComplex.
    def visitColumnTypeExprComplex(self, ctx:ClickHouseParser.ColumnTypeExprComplexContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#ColumnTypeExprParam.
    def visitColumnTypeExprParam(self, ctx:ClickHouseParser.ColumnTypeExprParamContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#columnExprList.
    def visitColumnExprList(self, ctx:ClickHouseParser.ColumnExprListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#ColumnsExprAsterisk.
    def visitColumnsExprAsterisk(self, ctx:ClickHouseParser.ColumnsExprAsteriskContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#ColumnsExprSubquery.
    def visitColumnsExprSubquery(self, ctx:ClickHouseParser.ColumnsExprSubqueryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#ColumnsExprColumn.
    def visitColumnsExprColumn(self, ctx:ClickHouseParser.ColumnsExprColumnContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#ColumnExprTernaryOp.
    def visitColumnExprTernaryOp(self, ctx:ClickHouseParser.ColumnExprTernaryOpContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#ColumnExprAlias.
    def visitColumnExprAlias(self, ctx:ClickHouseParser.ColumnExprAliasContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#ColumnExprExtract.
    def visitColumnExprExtract(self, ctx:ClickHouseParser.ColumnExprExtractContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#ColumnExprNegate.
    def visitColumnExprNegate(self, ctx:ClickHouseParser.ColumnExprNegateContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#ColumnExprSubquery.
    def visitColumnExprSubquery(self, ctx:ClickHouseParser.ColumnExprSubqueryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#ColumnExprLiteral.
    def visitColumnExprLiteral(self, ctx:ClickHouseParser.ColumnExprLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#ColumnExprArray.
    def visitColumnExprArray(self, ctx:ClickHouseParser.ColumnExprArrayContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#ColumnExprSubstring.
    def visitColumnExprSubstring(self, ctx:ClickHouseParser.ColumnExprSubstringContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#ColumnExprCast.
    def visitColumnExprCast(self, ctx:ClickHouseParser.ColumnExprCastContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#ColumnExprOr.
    def visitColumnExprOr(self, ctx:ClickHouseParser.ColumnExprOrContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#ColumnExprPrecedence1.
    def visitColumnExprPrecedence1(self, ctx:ClickHouseParser.ColumnExprPrecedence1Context):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#ColumnExprPrecedence2.
    def visitColumnExprPrecedence2(self, ctx:ClickHouseParser.ColumnExprPrecedence2Context):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#ColumnExprPrecedence3.
    def visitColumnExprPrecedence3(self, ctx:ClickHouseParser.ColumnExprPrecedence3Context):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#ColumnExprInterval.
    def visitColumnExprInterval(self, ctx:ClickHouseParser.ColumnExprIntervalContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#ColumnExprIsNull.
    def visitColumnExprIsNull(self, ctx:ClickHouseParser.ColumnExprIsNullContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#ColumnExprWinFunctionTarget.
    def visitColumnExprWinFunctionTarget(self, ctx:ClickHouseParser.ColumnExprWinFunctionTargetContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#ColumnExprTrim.
    def visitColumnExprTrim(self, ctx:ClickHouseParser.ColumnExprTrimContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#ColumnExprTuple.
    def visitColumnExprTuple(self, ctx:ClickHouseParser.ColumnExprTupleContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#ColumnExprArrayAccess.
    def visitColumnExprArrayAccess(self, ctx:ClickHouseParser.ColumnExprArrayAccessContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#ColumnExprBetween.
    def visitColumnExprBetween(self, ctx:ClickHouseParser.ColumnExprBetweenContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#ColumnExprParens.
    def visitColumnExprParens(self, ctx:ClickHouseParser.ColumnExprParensContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#ColumnExprTimestamp.
    def visitColumnExprTimestamp(self, ctx:ClickHouseParser.ColumnExprTimestampContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#ColumnExprAnd.
    def visitColumnExprAnd(self, ctx:ClickHouseParser.ColumnExprAndContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#ColumnExprTupleAccess.
    def visitColumnExprTupleAccess(self, ctx:ClickHouseParser.ColumnExprTupleAccessContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#ColumnExprCase.
    def visitColumnExprCase(self, ctx:ClickHouseParser.ColumnExprCaseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#ColumnExprDate.
    def visitColumnExprDate(self, ctx:ClickHouseParser.ColumnExprDateContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#ColumnExprNot.
    def visitColumnExprNot(self, ctx:ClickHouseParser.ColumnExprNotContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#ColumnExprWinFunction.
    def visitColumnExprWinFunction(self, ctx:ClickHouseParser.ColumnExprWinFunctionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#ColumnExprIdentifier.
    def visitColumnExprIdentifier(self, ctx:ClickHouseParser.ColumnExprIdentifierContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#ColumnExprFunction.
    def visitColumnExprFunction(self, ctx:ClickHouseParser.ColumnExprFunctionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#ColumnExprAsterisk.
    def visitColumnExprAsterisk(self, ctx:ClickHouseParser.ColumnExprAsteriskContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#columnArgList.
    def visitColumnArgList(self, ctx:ClickHouseParser.ColumnArgListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#columnArgExpr.
    def visitColumnArgExpr(self, ctx:ClickHouseParser.ColumnArgExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#columnLambdaExpr.
    def visitColumnLambdaExpr(self, ctx:ClickHouseParser.ColumnLambdaExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#columnIdentifier.
    def visitColumnIdentifier(self, ctx:ClickHouseParser.ColumnIdentifierContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#nestedIdentifier.
    def visitNestedIdentifier(self, ctx:ClickHouseParser.NestedIdentifierContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#TableExprIdentifier.
    def visitTableExprIdentifier(self, ctx:ClickHouseParser.TableExprIdentifierContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#TableExprSubquery.
    def visitTableExprSubquery(self, ctx:ClickHouseParser.TableExprSubqueryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#TableExprAlias.
    def visitTableExprAlias(self, ctx:ClickHouseParser.TableExprAliasContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#TableExprFunction.
    def visitTableExprFunction(self, ctx:ClickHouseParser.TableExprFunctionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#tableFunctionExpr.
    def visitTableFunctionExpr(self, ctx:ClickHouseParser.TableFunctionExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#tableIdentifier.
    def visitTableIdentifier(self, ctx:ClickHouseParser.TableIdentifierContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#tableArgList.
    def visitTableArgList(self, ctx:ClickHouseParser.TableArgListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#tableArgExpr.
    def visitTableArgExpr(self, ctx:ClickHouseParser.TableArgExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#databaseIdentifier.
    def visitDatabaseIdentifier(self, ctx:ClickHouseParser.DatabaseIdentifierContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#floatingLiteral.
    def visitFloatingLiteral(self, ctx:ClickHouseParser.FloatingLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#numberLiteral.
    def visitNumberLiteral(self, ctx:ClickHouseParser.NumberLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#literal.
    def visitLiteral(self, ctx:ClickHouseParser.LiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#interval.
    def visitInterval(self, ctx:ClickHouseParser.IntervalContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#keyword.
    def visitKeyword(self, ctx:ClickHouseParser.KeywordContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#keywordForAlias.
    def visitKeywordForAlias(self, ctx:ClickHouseParser.KeywordForAliasContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#alias.
    def visitAlias(self, ctx:ClickHouseParser.AliasContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#identifier.
    def visitIdentifier(self, ctx:ClickHouseParser.IdentifierContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#identifierOrNull.
    def visitIdentifierOrNull(self, ctx:ClickHouseParser.IdentifierOrNullContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by ClickHouseParser#enumValue.
    def visitEnumValue(self, ctx:ClickHouseParser.EnumValueContext):
        return self.visitChildren(ctx)



del ClickHouseParser