/*
Copyright 2023 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package operators

import (
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func createOperatorFromUpdate(ctx *plancontext.PlanningContext, updStmt *sqlparser.Update) (ops.Operator, error) {
	tableInfo, qt, err := createQueryTableForDML(ctx, updStmt.TableExprs[0], updStmt.Where)
	if err != nil {
		return nil, err
	}

	vindexTable, routing, err := buildVindexTableForDML(ctx, tableInfo, qt, "update")
	if err != nil {
		return nil, err
	}

	updClone := sqlparser.CloneRefOfUpdate(updStmt)
	updOp, err := createUpdateOperator(ctx, updStmt, vindexTable, qt, routing)
	if err != nil {
		return nil, err
	}

	ksMode, err := ctx.VSchema.ForeignKeyMode(vindexTable.Keyspace.Name)
	if err != nil {
		return nil, err
	}
	// Unmanaged foreign-key-mode, we don't need to do anything.
	if ksMode != vschemapb.Keyspace_FK_MANAGED {
		return updOp, nil
	}

	parentFks, childFks := getFKRequirementsForUpdate(ctx, updStmt.Exprs, vindexTable)
	if len(childFks) == 0 && len(parentFks) == 0 {
		return updOp, nil
	}

	// If the delete statement has a limit, we don't support it yet.
	if updStmt.Limit != nil {
		return nil, vterrors.VT12001("foreign keys management at vitess with limit")
	}

	return buildFkOperator(ctx, updOp, updClone, parentFks, childFks, vindexTable)
}

func createUpdateOperator(ctx *plancontext.PlanningContext, updStmt *sqlparser.Update, vindexTable *vindexes.Table, qt *QueryTable, routing Routing) (ops.Operator, error) {
	assignments := make(map[string]sqlparser.Expr)
	for _, set := range updStmt.Exprs {
		assignments[set.Name.Name.String()] = set.Expr
	}

	vp, cvv, ovq, err := getUpdateVindexInformation(updStmt, vindexTable, qt.ID, qt.Predicates)
	if err != nil {
		return nil, err
	}

	tr, ok := routing.(*ShardedRouting)
	if ok {
		tr.VindexPreds = vp
	}

	for _, predicate := range qt.Predicates {
		routing, err = UpdateRoutingLogic(ctx, predicate, routing)
		if err != nil {
			return nil, err
		}
	}

	if routing.OpCode() == engine.Scatter && updStmt.Limit != nil {
		// TODO systay: we should probably check for other op code types - IN could also hit multiple shards (2022-04-07)
		return nil, vterrors.VT12001("multi shard UPDATE with LIMIT")
	}

	r := &Route{
		Source: &Update{
			QTable:              qt,
			VTable:              vindexTable,
			Assignments:         assignments,
			ChangedVindexValues: cvv,
			OwnedVindexQuery:    ovq,
			AST:                 updStmt,
		},
		Routing: routing,
	}

	subq, err := createSubqueryFromStatement(ctx, updStmt)
	if err != nil {
		return nil, err
	}
	if subq == nil {
		return r, nil
	}
	subq.Outer = r
	return subq, nil
}

// getFKRequirementsForUpdate analyzes update expressions to determine which foreign key constraints needs management at the VTGate.
// It identifies parent and child foreign keys that require verification or cascade operations due to column updates.
func getFKRequirementsForUpdate(ctx *plancontext.PlanningContext, updateExprs sqlparser.UpdateExprs, vindexTable *vindexes.Table) ([]vindexes.ParentFKInfo, []vindexes.ChildFKInfo) {
	parentFks := vindexTable.ParentFKsNeedsHandling(ctx.VerifyAllFKs, ctx.ParentFKToIgnore)
	childFks := vindexTable.ChildFKsNeedsHandling(ctx.VerifyAllFKs, vindexes.UpdateAction)
	if len(childFks) == 0 && len(parentFks) == 0 {
		return nil, nil
	}

	pFksRequired := make([]bool, len(parentFks))
	cFksRequired := make([]bool, len(childFks))
	// Go over all the update expressions
	for _, updateExpr := range updateExprs {
		// Any foreign key to a child table for a column that has been updated
		// will require the cascade operations to happen, so we include all such foreign keys.
		for idx, childFk := range childFks {
			if childFk.ParentColumns.FindColumn(updateExpr.Name.Name) >= 0 {
				cFksRequired[idx] = true
			}
		}
		// If we are setting a column to NULL, then we don't need to verify the existance of an
		// equivalent row in the parent table, even if this column was part of a foreign key to a parent table.
		if sqlparser.IsNull(updateExpr.Expr) {
			continue
		}
		// We add all the possible parent foreign key constraints that need verification that an equivalent row
		// exists, given that this column has changed.
		for idx, parentFk := range parentFks {
			if parentFk.ChildColumns.FindColumn(updateExpr.Name.Name) >= 0 {
				pFksRequired[idx] = true
			}
		}
	}
	// For the parent foreign keys, if any of the columns part of the fk is set to NULL,
	// then, we don't care for the existance of an equivalent row in the parent table.
	for idx, parentFk := range parentFks {
		for _, updateExpr := range updateExprs {
			if !sqlparser.IsNull(updateExpr.Expr) {
				continue
			}
			if parentFk.ChildColumns.FindColumn(updateExpr.Name.Name) >= 0 {
				pFksRequired[idx] = false
			}
		}
	}
	// Get the filtered lists and return them.
	var pFksNeedsHandling []vindexes.ParentFKInfo
	var cFksNeedsHandling []vindexes.ChildFKInfo
	for idx, parentFk := range parentFks {
		if pFksRequired[idx] {
			pFksNeedsHandling = append(pFksNeedsHandling, parentFk)
		}
	}
	for idx, childFk := range childFks {
		if cFksRequired[idx] {
			cFksNeedsHandling = append(cFksNeedsHandling, childFk)
		}
	}
	return pFksNeedsHandling, cFksNeedsHandling
}

func buildFkOperator(ctx *plancontext.PlanningContext, updOp ops.Operator, updClone *sqlparser.Update, parentFks []vindexes.ParentFKInfo, childFks []vindexes.ChildFKInfo, updatedTable *vindexes.Table) (ops.Operator, error) {
	op, err := createFKCascadeOp(ctx, updOp, updClone, childFks, updatedTable)
	if err != nil {
		return nil, err
	}

	return createFKVerifyOp(ctx, op, updClone, parentFks)
}

func createFKCascadeOp(ctx *plancontext.PlanningContext, parentOp ops.Operator, updStmt *sqlparser.Update, childFks []vindexes.ChildFKInfo, updatedTable *vindexes.Table) (ops.Operator, error) {
	if len(childFks) == 0 {
		return parentOp, nil
	}

	// We only support simple expressions in update queries with cascade.
	if isNonLiteral(updStmt.Exprs) {
		return nil, vterrors.VT12001("foreign keys management at vitess with non-literal values")
	}

	var fkChildren []*FkChild
	var selectExprs []sqlparser.SelectExpr

	for _, fk := range childFks {
		// Any RESTRICT type foreign keys that arrive here,
		// are cross-shard/cross-keyspace RESTRICT cases, which we don't currently support.
		if isRestrict(fk.OnUpdate) {
			return nil, vterrors.VT12002()
		}

		// We need to select all the parent columns for the foreign key constraint, to use in the update of the child table.
		cols, exprs := selectParentColumns(fk, len(selectExprs))
		selectExprs = append(selectExprs, exprs...)

		fkChild, err := createFkChildForUpdate(ctx, fk, updStmt, cols, updatedTable)
		if err != nil {
			return nil, err
		}
		fkChildren = append(fkChildren, fkChild)
	}

	selectionOp, err := createSelectionOp(ctx, selectExprs, updStmt.TableExprs, updStmt.Where, nil)
	if err != nil {
		return nil, err
	}

	return &FkCascade{
		Selection: selectionOp,
		Children:  fkChildren,
		Parent:    parentOp,
	}, nil
}

func isNonLiteral(updExprs sqlparser.UpdateExprs) bool {
	for _, updateExpr := range updExprs {
		switch updateExpr.Expr.(type) {
		case *sqlparser.Argument, *sqlparser.NullVal, sqlparser.BoolVal, *sqlparser.Literal:
		default:
			return true
		}
	}
	return false
}

// createFkChildForUpdate creates the update query operator for the child table based on the foreign key constraints.
func createFkChildForUpdate(ctx *plancontext.PlanningContext, fk vindexes.ChildFKInfo, updStmt *sqlparser.Update, cols []int, updatedTable *vindexes.Table) (*FkChild, error) {
	// Reserve a bind variable name
	bvName := ctx.ReservedVars.ReserveVariable(foriegnKeyContraintValues)

	// Create child update operator
	// Create a ValTuple of child column names
	var valTuple sqlparser.ValTuple
	for _, column := range fk.ChildColumns {
		valTuple = append(valTuple, sqlparser.NewColName(column.String()))
	}

	// Create a comparison expression for WHERE clause
	compExpr := sqlparser.NewComparisonExpr(sqlparser.InOp, valTuple, sqlparser.NewListArg(bvName), nil)

	// Populate the update expressions and the where clause for the child update query based on the foreign key constraint type.
	var childWhereExpr sqlparser.Expr = compExpr
	var childUpdateExprs sqlparser.UpdateExprs
	var parsedComments *sqlparser.ParsedComments
	var foreignKeyToIgnore string
	var verifyAllFKs bool

	switch fk.OnUpdate {
	case sqlparser.Cascade:
		// For CASCADE type constraint, the query looks like this -
		//	`UPDATE <child_table> SET <child_column_updated_using_update_exprs_from_parent_update_query> WHERE <child_columns_in_fk> IN (<bind variable for the output from SELECT>)`

		// The update expressions are the same as the update expressions in the parent update query
		// with the column names replaced with the child column names.
		for _, updateExpr := range updStmt.Exprs {
			colIdx := fk.ParentColumns.FindColumn(updateExpr.Name.Name)
			if colIdx == -1 {
				continue
			}

			// The where condition is the same as the comparison expression above
			// with the column names replaced with the child column names.
			childUpdateExprs = append(childUpdateExprs, &sqlparser.UpdateExpr{
				Name: sqlparser.NewColName(fk.ChildColumns[colIdx].String()),
				Expr: updateExpr.Expr,
			})
		}
		// Because we could be updating the child to a non-null value,
		// We have to run with foreign key checks OFF because the parent isn't guaranteed to have
		// the data being updated to.
		parsedComments = sqlparser.Comments{
			"/*+ SET_VAR(foreign_key_checks=OFF) */",
		}.Parsed()
		// Since we are running the child update with foreign key checks turned off,
		// we need to verify the validity of the remaining foreign keys on VTGate,
		// while specifically ignoring the parent foreign key in question.
		verifyAllFKs = true
		foreignKeyToIgnore = fk.String(updatedTable)
	case sqlparser.SetNull:
		// For SET NULL type constraint, the query looks like this -
		//		`UPDATE <child_table> SET <child_column_updated_using_update_exprs_from_parent_update_query>
		//		WHERE <child_columns_in_fk> IN (<bind variable for the output from SELECT>)
		//		[AND <child_columns_in_fk> NOT IN (<bind variables in the SET clause of the original update>)]`

		// For the SET NULL type constraint, we need to set all the child columns to NULL.
		for _, column := range fk.ChildColumns {
			childUpdateExprs = append(childUpdateExprs, &sqlparser.UpdateExpr{
				Name: sqlparser.NewColName(column.String()),
				Expr: &sqlparser.NullVal{},
			})
		}

		// SET NULL cascade should be avoided for the case where the parent columns remains unchanged on the update.
		// We need to add a condition to the where clause to handle this case.
		// The additional condition looks like [AND <child_columns_in_fk> NOT IN (<bind variables in the SET clause of the original update>)].
		// If any of the parent columns is being set to NULL, then we don't need this condition.
		var updateValues sqlparser.ValTuple
		colSetToNull := false
		for _, updateExpr := range updStmt.Exprs {
			colIdx := fk.ParentColumns.FindColumn(updateExpr.Name.Name)
			if colIdx >= 0 {
				if sqlparser.IsNull(updateExpr.Expr) {
					colSetToNull = true
					break
				}
				updateValues = append(updateValues, updateExpr.Expr)
			}
		}
		if !colSetToNull {
			childWhereExpr = &sqlparser.AndExpr{
				Left:  compExpr,
				Right: sqlparser.NewComparisonExpr(sqlparser.NotInOp, valTuple, updateValues, nil),
			}
		}
	case sqlparser.SetDefault:
		return nil, vterrors.VT09016()
	}

	childStmt := &sqlparser.Update{
		Comments:   parsedComments,
		Exprs:      childUpdateExprs,
		TableExprs: []sqlparser.TableExpr{sqlparser.NewAliasedTableExpr(fk.Table.GetTableName(), "")},
		Where:      &sqlparser.Where{Type: sqlparser.WhereClause, Expr: childWhereExpr},
	}

	childOp, err := createOpFromStmt(ctx, childStmt, verifyAllFKs, foreignKeyToIgnore)
	if err != nil {
		return nil, err
	}

	return &FkChild{
		BVName: bvName,
		Cols:   cols,
		Op:     childOp,
	}, nil
}

// createFKVerifyOp creates the verify operator for the parent foreign key constraints.
func createFKVerifyOp(ctx *plancontext.PlanningContext, childOp ops.Operator, updStmt *sqlparser.Update, parentFks []vindexes.ParentFKInfo) (ops.Operator, error) {
	if len(parentFks) == 0 {
		return childOp, nil
	}

	if isNonLiteral(updStmt.Exprs) {
		return nil, vterrors.VT12001("foreign keys management at vitess with non-literal values")
	}

	var FkParents []ops.Operator
	for _, fk := range parentFks {
		op, err := createFkVerifyForUpdate(ctx, updStmt, fk)
		if err != nil {
			return nil, err
		}
		FkParents = append(FkParents, op)
	}

	return &FkVerify{
		Verify: FkParents,
		Input:  childOp,
	}, nil
}

// Each foreign key constraint is verified by an anti join query of the form:
// select 1 from child_tbl left join parent_tbl on <parent_child_columns with new value expressions, remaining fk columns join>
// where <parent columns are null> and <unchanged child columns not null> limit 1
// E.g:
// Child (c1, c2) references Parent (p1, p2)
// update Child set c1 = 1 where id = 1
// verify query:
// select 1 from Child left join Parent on Parent.p1 = 1 and Parent.p2 = Child.c2
// where Parent.p1 is null and Parent.p2 is null
// and Child.c2 is not null
// limit 1
func createFkVerifyForUpdate(ctx *plancontext.PlanningContext, updStmt *sqlparser.Update, pFK vindexes.ParentFKInfo) (ops.Operator, error) {
	childTblExpr := updStmt.TableExprs[0].(*sqlparser.AliasedTableExpr)
	childTbl, err := childTblExpr.TableName()
	if err != nil {
		return nil, err
	}
	parentTbl := pFK.Table.GetTableName()
	var whereCond sqlparser.Expr
	var joinCond sqlparser.Expr
	for idx, column := range pFK.ChildColumns {
		var matchedExpr *sqlparser.UpdateExpr
		for _, updateExpr := range updStmt.Exprs {
			if column.Equal(updateExpr.Name.Name) {
				matchedExpr = updateExpr
				break
			}
		}
		parentIsNullExpr := &sqlparser.IsExpr{
			Left:  sqlparser.NewColNameWithQualifier(pFK.ParentColumns[idx].String(), parentTbl),
			Right: sqlparser.IsNullOp,
		}
		var predicate sqlparser.Expr = parentIsNullExpr
		var joinExpr sqlparser.Expr
		if matchedExpr == nil {
			predicate = &sqlparser.AndExpr{
				Left: parentIsNullExpr,
				Right: &sqlparser.IsExpr{
					Left:  sqlparser.NewColNameWithQualifier(pFK.ChildColumns[idx].String(), childTbl),
					Right: sqlparser.IsNotNullOp,
				},
			}
			joinExpr = &sqlparser.ComparisonExpr{
				Operator: sqlparser.EqualOp,
				Left:     sqlparser.NewColNameWithQualifier(pFK.ParentColumns[idx].String(), parentTbl),
				Right:    sqlparser.NewColNameWithQualifier(pFK.ChildColumns[idx].String(), childTbl),
			}
		} else {
			joinExpr = &sqlparser.ComparisonExpr{
				Operator: sqlparser.EqualOp,
				Left:     sqlparser.NewColNameWithQualifier(pFK.ParentColumns[idx].String(), parentTbl),
				Right:    prefixColNames(childTbl, matchedExpr.Expr),
			}
		}

		if idx == 0 {
			joinCond, whereCond = joinExpr, predicate
			continue
		}
		joinCond = &sqlparser.AndExpr{Left: joinCond, Right: joinExpr}
		whereCond = &sqlparser.AndExpr{Left: whereCond, Right: predicate}
	}
	// add existing where condition on the update statement
	if updStmt.Where != nil {
		whereCond = &sqlparser.AndExpr{Left: whereCond, Right: prefixColNames(childTbl, updStmt.Where.Expr)}
	}
	return createSelectionOp(ctx,
		sqlparser.SelectExprs{sqlparser.NewAliasedExpr(sqlparser.NewIntLiteral("1"), "")},
		[]sqlparser.TableExpr{
			sqlparser.NewJoinTableExpr(
				childTblExpr,
				sqlparser.LeftJoinType,
				sqlparser.NewAliasedTableExpr(parentTbl, ""),
				sqlparser.NewJoinCondition(joinCond, nil)),
		},
		sqlparser.NewWhere(sqlparser.WhereClause, whereCond),
		sqlparser.NewLimitWithoutOffset(1))
}
