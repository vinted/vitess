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

// Package vtexplain analyzes a set of sql statements and returns the
// corresponding vtgate and vttablet query plans that will be executed
// on the given statements
package vtexplain

import (
	"bytes"
	"flag"
	"fmt"
	"sort"
	"strings"
	"time"

	"vitess.io/vitess/go/vt/discovery"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/vtgate"

	"github.com/buger/jsonparser"

	"vitess.io/vitess/go/jsonutil"
	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

var (
	batchInterval = flag.Duration("batch-interval", 10*time.Millisecond, "Interval between logical time slots.")
)

const (
	vtexplainCell = "explainCell"

	// ModeMulti is the default mode with autocommit implemented at vtgate
	ModeMulti = "multi"

	// ModeTwoPC enables the twopc feature
	ModeTwoPC = "twopc"
)

type (
	// ExecutorMode controls the mode of operation for the vtexplain simulator
	ExecutorMode string

	// Options to control the explain process
	Options struct {
		// NumShards indicates the number of shards in the topology
		NumShards int

		// PlannerVersion indicates whether or not we should use the Gen4 planner
		PlannerVersion querypb.ExecuteOptions_PlannerVersion

		// ReplicationMode must be set to either "ROW" or "STATEMENT" before
		// initialization
		ReplicationMode string

		// Normalize controls whether or not vtgate does query normalization
		Normalize bool

		// ExecutionMode must be set to one of the modes above
		ExecutionMode string

		// StrictDDL is used in unit tests only to verify that the schema
		// is parsed properly.
		StrictDDL bool

		// Target is used to override the "database" target in the
		// vtgate session to simulate `USE <target>`
		Target string
	}

	// TabletQuery defines a query that was sent to a given tablet and how it was
	// processed in mysql
	TabletQuery struct {
		// Logical time of the query
		Time int

		// SQL command sent to the given tablet
		SQL string

		// BindVars sent with the command
		BindVars map[string]*querypb.BindVariable
	}

	// MysqlQuery defines a query that was sent to a given tablet and how it was
	// processed in mysql
	MysqlQuery struct {
		// Sequence number of the query
		Time int

		// SQL command sent to the given tablet
		SQL string
	}

	// Explain defines how vitess will execute a given sql query, including the vtgate
	// query plans and all queries run on each tablet.
	Explain struct {
		// original sql statement
		SQL string

		// the vtgate plan(s)
		Plans []*engine.Plan

		// list of queries / bind vars sent to each tablet
		TabletActions map[string]*TabletActions

		// vtexplain error
		Error string
	}

	outputQuery struct {
		tablet string
		Time   int
		sql    string
	}

	// VTExplain contains whole topology for query explaining
	VTExplain struct {
		explainTopo    *ExplainTopo
		vtgateExecutor *vtgate.Executor
		healthCheck    *discovery.FakeHealthCheck
		vtgateSession  *vtgatepb.Session
		// TODO: add missing savepoint handling https://github.com/vitessio/vitess/pull/10374/files?diff=split&w=0#diff-be4d874be3c6bd416e8cd3c22040911ef86ae7bcae83185055ed883ca571c406R338

		// time simulator
		batchTime       *sync2.Batcher
		globalTabletEnv *tabletEnv
	}

	// TabletActions contains the set of operations done by a given tablet
	TabletActions struct {
		// Queries sent from vtgate to the tablet
		TabletQueries []*TabletQuery

		// Queries that were run on mysql
		MysqlQueries []*MysqlQuery
	}
)

// MarshalJSON renders the json structure
func (tq *TabletQuery) MarshalJSON() ([]byte, error) {
	// Convert Bindvars to strings for nicer output
	bindVars := make(map[string]string)
	for k, v := range tq.BindVars {
		var b strings.Builder
		sqlparser.EncodeValue(&b, v)
		bindVars[k] = b.String()
	}

	return jsonutil.MarshalNoEscape(&struct {
		Time     int
		SQL      string
		BindVars map[string]string
	}{
		Time:     tq.Time,
		SQL:      tq.SQL,
		BindVars: bindVars,
	})
}

// Init sets up the fake execution environment
func Init(vSchemaStr, sqlSchema, ksShardMapStr string, opts *Options) (*VTExplain, error) {
	// Verify options
	if opts.ReplicationMode != "ROW" && opts.ReplicationMode != "STATEMENT" {
		return nil, fmt.Errorf("invalid replication mode \"%s\"", opts.ReplicationMode)
	}

	parsedDDLs, err := parseSchema(sqlSchema, opts)
	if err != nil {
		return nil, fmt.Errorf("parseSchema: %v", err)
	}

	tabletEnv, err := newTabletEnvironment(parsedDDLs, opts)
	if err != nil {
		return nil, fmt.Errorf("initTabletEnvironment: %v", err)
	}
	vte := &VTExplain{vtgateSession: &vtgatepb.Session{
		TargetString: "",
		Autocommit:   true,
	}}
	vte.setGlobalTabletEnv(tabletEnv)
	err = vte.initVtgateExecutor(vSchemaStr, ksShardMapStr, opts)

	if err != nil {
		return nil, fmt.Errorf("initVtgateExecutor: %v", err)
	}

	return vte, nil
}

// Stop and cleans up fake execution environment
func (vte *VTExplain) Stop() {
	// Cleanup all created fake dbs.
	if vte.explainTopo != nil {
		for _, conn := range vte.explainTopo.TabletConns {
			conn.tsv.StopService()
		}
		for _, conn := range vte.explainTopo.TabletConns {
			conn.db.Close()
		}
	}
}

func parseSchema(sqlSchema string, opts *Options) ([]sqlparser.DDLStatement, error) {
	parsedDDLs := make([]sqlparser.DDLStatement, 0, 16)
	for {
		sql, rem, err := sqlparser.SplitStatement(sqlSchema)
		sqlSchema = rem
		if err != nil {
			return nil, err
		}
		if sql == "" {
			break
		}
		sql, _ = sqlparser.SplitMarginComments(sql)
		if sql == "" {
			continue
		}

		var stmt sqlparser.Statement
		if opts.StrictDDL {
			stmt, err = sqlparser.ParseStrictDDL(sql)
			if err != nil {
				return nil, err
			}
		} else {
			stmt, err = sqlparser.Parse(sql)
			if err != nil {
				log.Errorf("ERROR: failed to parse sql: %s, got error: %v", sql, err)
				continue
			}
		}
		ddl, ok := stmt.(sqlparser.DDLStatement)
		if !ok {
			log.Infof("ignoring non-DDL statement: %s", sql)
			continue
		}
		if ddl.GetAction() != sqlparser.CreateDDLAction {
			log.Infof("ignoring %s table statement", ddl.GetAction().ToString())
			continue
		}
		if ddl.GetTableSpec() == nil && ddl.GetOptLike() == nil {
			log.Errorf("invalid create table statement: %s", sql)
			continue
		}
		parsedDDLs = append(parsedDDLs, ddl)
	}
	return parsedDDLs, nil
}

// Run the explain analysis on the given queries
func (vte *VTExplain) Run(sql string) []*Explain {
	explains := make([]*Explain, 0, 16)

	var (
		rem string
		err error
	)

	for {
		// Need to strip comments in a loop to handle multiple comments
		// in a row.
		for {
			s := sqlparser.StripLeadingComments(sql)
			if s == sql {
				break
			}
			sql = s
		}

		sql, rem, err = sqlparser.SplitStatement(sql)
		if err != nil {
			log.Errorf("failed to parse %s, got error: %s", sql, err)
			continue
		}

		if sql != "" {
			// Reset the global time simulator unless there's an open transaction
			// in the session from the previous staement.
			if vte.vtgateSession == nil || !vte.vtgateSession.GetInTransaction() {
				vte.batchTime = sync2.NewBatcher(*batchInterval)
			}
			log.V(100).Infof("explain %s", sql)
			explains = append(explains, vte.explain(sql))
		}

		sql = rem
		if sql == "" {
			break
		}
	}

	return explains
}

// Replaces SQL bind variable tokens with values
func formatSQLWithBind(sql string, bindVars map[string]*querypb.BindVariable) (string, error) {
	tree, err := sqlparser.Parse(sql)
	if err != nil {
		return "", fmt.Errorf("parse failed for %s: %v", sql, err)
	}
	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf("%v", tree)
	pq := buf.ParsedQuery()
	bytes, err := pq.GenerateQuery(bindVars, nil)
	if err != nil {
		return "", fmt.Errorf("parse err: '%s'", err)
	}
	return string(bytes), nil
}

// Parse SQL from vtgate query log json mesage based on 'SQL' and 'BindVars' fields
func parseJSONQuery(message string) (string, error) {
	json := []byte(message)
	// Extract SQL field
	sql, err := jsonparser.GetString(json, "SQL")
	if err != nil {
		return "", fmt.Errorf("ERROR: failed to extract 'SQL' value from: '%s', got error: %v", message, err)
	}
	// Extract bind variables
	bindVars := map[string]*querypb.BindVariable{}
	err = jsonparser.ObjectEach(json, func(key []byte, value []byte, _ jsonparser.ValueType, _ int) error {
		bindVarType, err := jsonparser.GetString(value, "type")
		if err != nil {
			return fmt.Errorf("ERROR: failed to extract %s BindVar 'type' from: '%s', got error: %v", string(key), message, err)
		}
		typeIndex, ok := querypb.Type_value[strings.ToUpper(bindVarType)]
		if !ok {
			return fmt.Errorf("ERROR: invalid %s BindVar 'type': %s, in: '%s'", string(key), bindVarType, message)
		}
		bvt := querypb.Type(typeIndex)
		if bvt == querypb.Type_TUPLE {
			bindVarValues := []*querypb.Value{}
			_, err = jsonparser.ArrayEach(value, func(innnerValue []byte, _ jsonparser.ValueType, _ int, err error) {
				if err != nil {
					return
				}
				innerBindVarType, err := jsonparser.GetString(innnerValue, "type")
				if err != nil {
					return
				}
				innerBindVarValue, _, _, err := jsonparser.Get(innnerValue, "value")
				if err != nil {
					return
				}
				innerTypeIndex, ok := querypb.Type_value[strings.ToUpper(innerBindVarType)]
				if !ok {
					log.Errorf("invalid BindVar 'type': %s", innerBindVarType)
					return
				}
				bindVarValues = append(
					bindVarValues, &querypb.Value{
						Type:  querypb.Type(innerTypeIndex),
						Value: innerBindVarValue,
					},
				)
			}, "values")
			if err != nil {
				return fmt.Errorf("ERROR: failed to extract %s BindVar 'values' from: '%s', got error: %v", string(key), message, err)
			}
			bindVars[string(key)] = &querypb.BindVariable{
				Type:   bvt,
				Values: bindVarValues,
			}
		} else {
			bindVarValue, _, _, err := jsonparser.Get(value, "value")
			if err != nil {
				return fmt.Errorf("ERROR: failed to extract %s BindVar 'value' from: '%s', got error: %v", string(key), message, err)
			}
			bindVars[string(key)] = &querypb.BindVariable{
				Type:  bvt,
				Value: bindVarValue,
			}
		}
		return nil
	}, "BindVars")
	if err != nil {
		return "", err
	}

	return formatSQLWithBind(sql, bindVars)
}

// RunFromJSON runs the explain analysis on the given json queries
func (vte *VTExplain) RunFromJSON(input string) []*Explain {
	lines := strings.Split(input, "\n")
	explains := make([]*Explain, 0, len(lines))
	for lineNumber, line := range lines {
		// Handle empty lines
		if len(line) == 0 {
			continue
		}
		sql, err := parseJSONQuery(line)
		if err != nil {
			log.Errorf("failed to parse line %v, got error: %s", lineNumber, err)
			continue
		}
		// Need to strip comments in a loop to handle multiple comments
		// in a row.
		for {
			s := sqlparser.StripLeadingComments(sql)
			if s == sql {
				break
			}
			sql = s
		}

		if sql != "" {
			// Reset the global time simulator unless there's an open transaction
			// in the session from the previous staement.
			if vte.vtgateSession == nil || !vte.vtgateSession.GetInTransaction() {
				vte.batchTime = sync2.NewBatcher(*batchInterval)
			}
			log.V(100).Infof("explain %s", sql)
			explains = append(explains, vte.explain(sql))
		}

	}

	return explains
}

func (vte *VTExplain) explain(sql string) *Explain {
	plans, tabletActions, err := vte.vtgateExecute(sql)
	explainError := ""
	if err != nil {
		explainError = err.Error()
	}

	return &Explain{
		SQL:           sql,
		Plans:         plans,
		TabletActions: tabletActions,
		Error:         explainError,
	}
}

// ExplainsAsText returns a text representation of the explains in logical time
// order
func (vte *VTExplain) ExplainsAsText(explains []*Explain) string {
	var b bytes.Buffer
	for _, explain := range explains {
		fmt.Fprintf(&b, "----------------------------------------------------------------------\n")
		fmt.Fprintf(&b, "%s\n\n", explain.SQL)

		if len(explain.Error) > 0 {
			fmt.Fprintf(&b, "ERROR: %s", explain.Error)
		} else {
			queries := make([]outputQuery, 0, 4)
			for tablet, actions := range explain.TabletActions {
				// TODO: add missing savepoint handling https://github.com/vitessio/vitess/pull/10374/files?diff=split&w=0#diff-be4d874be3c6bd416e8cd3c22040911ef86ae7bcae83185055ed883ca571c406R338
				for _, q := range actions.MysqlQueries {
					queries = append(queries, outputQuery{
						tablet: tablet,
						Time:   q.Time,
						sql:    q.SQL,
					})
				}
			}

			// Make sure to sort first by the batch time and then by the
			// shard to avoid flakiness in the tests for parallel queries
			sort.SliceStable(queries, func(i, j int) bool {
				if queries[i].Time == queries[j].Time {
					return queries[i].tablet < queries[j].tablet
				}
				return queries[i].Time < queries[j].Time
			})

			for _, q := range queries {
				fmt.Fprintf(&b, "%d %s: %s\n", q.Time, q.tablet, q.sql)
			}
		}
		fmt.Fprintf(&b, "\n")
	}
	fmt.Fprintf(&b, "----------------------------------------------------------------------\n")
	return b.String()
}

// ExplainsAsJSON returns a json representation of the explains
func (vte *VTExplain) ExplainsAsJSON(explains []*Explain) string {
	explainJSON, _ := jsonutil.MarshalIndentNoEscape(explains, "", "    ")
	return string(explainJSON)
}
