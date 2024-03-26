package engine

import (
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/query"
)

// EmptyPrimitive represents a no-operation primitive,
// fulfilling the Primitive interface without performing any actions.
type EmptyPrimitive struct {
	noInputs
	noTxNeeded
}

func (ep *EmptyPrimitive) Execute(vcursor VCursor, bindVars map[string]*query.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	return &sqltypes.Result{}, nil
}

func (ep *EmptyPrimitive) StreamExecute(vcursor VCursor, bindVars map[string]*query.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	return callback(&sqltypes.Result{})
}

func (ep *EmptyPrimitive) GetFields(vcursor VCursor, bindVars map[string]*query.BindVariable) (*sqltypes.Result, error) {
	return &sqltypes.Result{}, nil
}

func (ep *EmptyPrimitive) RouteType() string {
	return "Empty"
}

func (ep *EmptyPrimitive) GetKeyspaceName() string {
	return ""
}

func (ep *EmptyPrimitive) GetTableName() string {
	return ""
}

func (ep *EmptyPrimitive) Inputs() []Primitive {
	return nil
}

func (ep *EmptyPrimitive) NeedsTransaction() bool {
	return false
}

func (ep *EmptyPrimitive) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType: "Empty",
	}
}
