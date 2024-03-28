/*
Copyright 2019 The Vitess Authors.

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

package vtgate

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
	"vitess.io/vitess/go/cache/redis"
	"vitess.io/vitess/go/vt/vtgate/boost"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder"
)

func (e *Executor) newExecute(ctx context.Context, safeSession *SafeSession, sql string, bindVars map[string]*querypb.BindVariable, logStats *LogStats) (sqlparser.StatementType, *sqltypes.Result, error) {
	// 1: Prepare before planning and execution

	// Start an implicit transaction if necessary.
	err := e.startTxIfNecessary(ctx, safeSession)
	if err != nil {
		return 0, nil, err
	}

	if bindVars == nil {
		bindVars = make(map[string]*querypb.BindVariable)
	}

	query, comments := sqlparser.SplitMarginComments(sql)
	vcursor, err := newVCursorImpl(ctx, safeSession, comments, e, logStats, e.vm, e.VSchema(), e.resolver.resolver, e.serv, e.warnShardedOnly)
	if err != nil {
		return 0, nil, err
	}

	// 2: Create a plan for the query
	plan, err := e.getPlan(
		vcursor,
		query,
		comments,
		bindVars,
		skipQueryPlanCache(safeSession),
		logStats,
	)
	if err == planbuilder.ErrPlanNotSupported {
		return 0, nil, err
	}
	execStart := e.logPlanningFinished(logStats, plan)

	if err != nil {
		safeSession.ClearWarnings()
		return 0, nil, err
	}

	if plan.Type != sqlparser.StmtShow {
		safeSession.ClearWarnings()
	}

	// add any warnings that the planner wants to add
	for _, warning := range plan.Warnings {
		safeSession.RecordWarning(warning)
	}

	// We need to explicitly handle errors, and begin/commit/rollback, since these control transactions. Everything else
	// will fall through and be handled through planning
	switch plan.Type {
	case sqlparser.StmtBegin:
		qr, err := e.handleBegin(ctx, safeSession, logStats)
		return sqlparser.StmtBegin, qr, err
	case sqlparser.StmtCommit:
		qr, err := e.handleCommit(ctx, safeSession, logStats)
		return sqlparser.StmtCommit, qr, err
	case sqlparser.StmtRollback:
		qr, err := e.handleRollback(ctx, safeSession, logStats)
		return sqlparser.StmtRollback, qr, err
	case sqlparser.StmtSavepoint:
		qr, err := e.handleSavepoint(ctx, safeSession, plan.Original, "Savepoint", logStats, func(_ string) (*sqltypes.Result, error) {
			// Safely to ignore as there is no transaction.
			return &sqltypes.Result{}, nil
		}, vcursor.ignoreMaxMemoryRows)
		return sqlparser.StmtSavepoint, qr, err
	case sqlparser.StmtSRollback:
		qr, err := e.handleSavepoint(ctx, safeSession, plan.Original, "Rollback Savepoint", logStats, func(query string) (*sqltypes.Result, error) {
			// Error as there is no transaction, so there is no savepoint that exists.
			return nil, vterrors.NewErrorf(vtrpcpb.Code_NOT_FOUND, vterrors.SPDoesNotExist, "SAVEPOINT does not exist: %s", query)
		}, vcursor.ignoreMaxMemoryRows)
		return sqlparser.StmtSRollback, qr, err
	case sqlparser.StmtRelease:
		qr, err := e.handleSavepoint(ctx, safeSession, plan.Original, "Release Savepoint", logStats, func(query string) (*sqltypes.Result, error) {
			// Error as there is no transaction, so there is no savepoint that exists.
			return nil, vterrors.NewErrorf(vtrpcpb.Code_NOT_FOUND, vterrors.SPDoesNotExist, "SAVEPOINT does not exist: %s", query)
		}, vcursor.ignoreMaxMemoryRows)
		return sqlparser.StmtRelease, qr, err
	}

	// 3: Prepare for execution
	err = e.addNeededBindVars(plan.BindVarNeeds, bindVars, safeSession)
	if err != nil {
		logStats.Error = err
		return 0, nil, err
	}

	if plan.Instructions.NeedsTransaction() {
		return e.insideTransaction(ctx, safeSession, logStats,
			e.executePlan(ctx, plan, vcursor, bindVars, execStart))
	}

	if plan.BoostPlanConfig != nil && plan.BoostPlanConfig.IsBoosted {
		cacheKey := getCacheKey(plan.BoostPlanConfig, bindVars)
		cacheHit, decodedResults := e.fetchFromRedis(cacheKey)

		if cacheHit {
			return sqlparser.StmtSelect, decodedResults, nil
		} else {
			stmt, sqlResult, err1 := e.executePlan(ctx, plan, vcursor, bindVars, execStart)(logStats, safeSession)
			defer e.setBoostCache(sqlResult, cacheKey)

			return stmt, sqlResult, err1
		}
	}

	return e.executePlan(ctx, plan, vcursor, bindVars, execStart)(logStats, safeSession)
}

func (e *Executor) setBoostCache(sqlResult *sqltypes.Result, cacheKey string) {
	encodedResults, _ := e.encodeResults(sqlResult)
	e.boostCache.Set(cacheKey, encodedResults)
}

func (e *Executor) fetchFromRedis(cacheKey string) (bool, *sqltypes.Result) {
	redisResults, err := e.boostCache.Get(cacheKey)
	var cacheHit bool

	var decodeErr error
	var decodedResults *sqltypes.Result

	if redisResults == "" || err != nil {
		cacheHit = false
	} else {
		decodedResults, decodeErr = e.decodeResults(redisResults)

		if decodeErr != nil {
			fmt.Printf("Failed to decode: %v\n", decodeErr)
			cacheHit = false
		}

		cacheHit = true
	}
	return cacheHit, decodedResults
}

func (e *Executor) encodeResults(result *sqltypes.Result) (string, error) {
	jsonData, err := json.Marshal(result)
	if err != nil {
		fmt.Printf("Failed to encode: %v\n", err)
		return "", err
	}

	jsonString := string(jsonData)

	return jsonString, nil
}

func (e *Executor) decodeResults(redisResults string) (*sqltypes.Result, error) {
	var result sqltypes.Result
	err := json.Unmarshal([]byte(redisResults), &result)
	if err != nil {
		fmt.Printf("Failed to decode: %v\n", err)
		return nil, err
	}

	return &result, nil
}

func getCacheKey(config *boost.PlanConfig, vars map[string]*querypb.BindVariable) string {
	return redis.GenerateCacheKey(cacheKeyParams(config, vars)...)
}

func cacheKeyParams(boostConfig *boost.PlanConfig, vars map[string]*querypb.BindVariable) []string {
	var allColumns []string
	var allValues []string

	for key, vtgValueKey := range boostConfig.Columns {
		allColumns = append(allColumns, key)

		var byteArray = vars[vtgValueKey].Value
		var stringValue = string(byteArray)

		allValues = append(allValues, stringValue)
	}

	tail := append(allColumns, allValues...)

	return append([]string{boostConfig.Table}, tail...)
}

func (e *Executor) startTxIfNecessary(ctx context.Context, safeSession *SafeSession) error {
	if !safeSession.Autocommit && !safeSession.InTransaction() {
		if err := e.txConn.Begin(ctx, safeSession); err != nil {
			return err
		}
	}
	return nil
}

func (e *Executor) insideTransaction(ctx context.Context, safeSession *SafeSession, logStats *LogStats, f currFunc) (sqlparser.StatementType, *sqltypes.Result, error) {
	mustCommit := false
	if safeSession.Autocommit && !safeSession.InTransaction() {
		mustCommit = true
		if err := e.txConn.Begin(ctx, safeSession); err != nil {
			return 0, nil, err
		}
		// The defer acts as a failsafe. If commit was successful,
		// the rollback will be a no-op.
		defer e.txConn.Rollback(ctx, safeSession)
	}

	// The SetAutocommitable flag should be same as mustCommit.
	// If we started a transaction because of autocommit, then mustCommit
	// will be true, which means that we can autocommit. If we were already
	// in a transaction, it means that the app started it, or we are being
	// called recursively. If so, we cannot autocommit because whatever we
	// do is likely not final.
	// The control flow is such that autocommitable can only be turned on
	// at the beginning, but never after.
	safeSession.SetAutocommittable(mustCommit)

	// Execute!
	stmtType, result, err := f(logStats, safeSession)
	if err != nil {
		return 0, nil, err
	}

	if mustCommit {
		commitStart := time.Now()
		if err := e.txConn.Commit(ctx, safeSession); err != nil {
			return 0, nil, err
		}
		logStats.CommitTime = time.Since(commitStart)
	}
	return stmtType, result, nil
}

type currFunc func(*LogStats, *SafeSession) (sqlparser.StatementType, *sqltypes.Result, error)

func (e *Executor) executePlan(ctx context.Context, plan *engine.Plan, vcursor *vcursorImpl, bindVars map[string]*querypb.BindVariable, execStart time.Time) currFunc {
	return func(logStats *LogStats, safeSession *SafeSession) (sqlparser.StatementType, *sqltypes.Result, error) {
		// 4: Execute!
		qr, err := plan.Instructions.Execute(vcursor, bindVars, true)

		// 5: Log and add statistics
		logStats.Keyspace = plan.Instructions.GetKeyspaceName()
		logStats.Table = plan.Instructions.GetTableName()
		logStats.TabletType = vcursor.TabletType().String()
		errCount := e.logExecutionEnd(logStats, execStart, plan, err, qr)
		plan.AddStats(1, time.Since(logStats.StartTime), uint64(logStats.ShardQueries), logStats.RowsAffected, logStats.RowsReturned, errCount)

		// Check if there was partial DML execution. If so, rollback the transaction.
		if err != nil && safeSession.InTransaction() && vcursor.rollbackOnPartialExec {
			_ = e.txConn.Rollback(ctx, safeSession)
			err = vterrors.Errorf(vtrpcpb.Code_ABORTED, "transaction rolled back due to partial DML execution: %v", err)
		}
		return plan.Type, qr, err
	}
}

func (e *Executor) logExecutionEnd(logStats *LogStats, execStart time.Time, plan *engine.Plan, err error, qr *sqltypes.Result) uint64 {
	logStats.ExecuteTime = time.Since(execStart)

	e.updateQueryCounts(plan.Instructions.RouteType(), plan.Instructions.GetKeyspaceName(), plan.Instructions.GetTableName(), int64(logStats.ShardQueries))

	var errCount uint64
	if err != nil {
		logStats.Error = err
		errCount = 1
	} else {
		logStats.RowsAffected = qr.RowsAffected
		logStats.RowsReturned = uint64(len(qr.Rows))
	}
	return errCount
}

func (e *Executor) logPlanningFinished(logStats *LogStats, plan *engine.Plan) time.Time {
	execStart := time.Now()
	if plan != nil {
		logStats.StmtType = plan.Type.String()
	}
	logStats.PlanTime = execStart.Sub(logStats.StartTime)
	return execStart
}
