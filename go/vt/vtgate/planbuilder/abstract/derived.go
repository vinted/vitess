/*
Copyright 2021 The Vitess Authors.

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

package abstract

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

// Derived represents a derived table in the query
type Derived struct {
	Sel   *sqlparser.Select
	Inner Operator
	Alias string
}

var _ Operator = (*Derived)(nil)

// TableID implements the Operator interface
func (d *Derived) TableID() semantics.TableSet {
	return d.Inner.TableID()
}

// PushPredicate implements the Operator interface
func (d *Derived) PushPredicate(expr sqlparser.Expr, semTable *semantics.SemTable) error {
	tableInfo, err := semTable.TableInfoFor(semTable.ExprDeps.Dependencies(expr))
	if err != nil {
		return err
	}

	newExpr := sqlparser.CloneExpr(expr)
	sqlparser.Rewrite(newExpr, func(cursor *sqlparser.Cursor) bool {
		switch node := cursor.Node().(type) {
		case *sqlparser.ColName:
			var exp sqlparser.Expr
			exp, err = tableInfo.GetExprFor(node.Name.String())
			if err != nil {
				return false
			}
			cursor.Replace(exp)
			return false
		}
		return true
	}, nil)

	if err != nil {
		return err
	}
	return d.Inner.PushPredicate(newExpr, semTable)
}
