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

package main

import (
	"flag"
	"fmt"
	"io/ioutil"

	"vitess.io/vitess/go/exit"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vtexplain"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

var (
	sqlFlag            = flag.String("sql", "", "A list of semicolon-delimited SQL commands to analyze")
	sqlFileFlag        = flag.String("sql-file", "", "Identifies the file that contains the SQL commands to analyze")
	inputMode          = flag.String("input-mode", "text", "SQL input in text or json (vtgate format) mode")
	schemaFlag         = flag.String("schema", "", "The SQL table schema")
	schemaFileFlag     = flag.String("schema-file", "", "Identifies the file that contains the SQL table schema")
	vschemaFlag        = flag.String("vschema", "", "Identifies the VTGate routing schema")
	vschemaFileFlag    = flag.String("vschema-file", "", "Identifies the VTGate routing schema file")
	ksShardMapFlag     = flag.String("ks-shard-map", "", "JSON map of keyspace name -> shard name -> ShardReference object. The inner map is the same as the output of FindAllShardsInKeyspace")
	ksShardMapFileFlag = flag.String("ks-shard-map-file", "", "File containing json blob of keyspace name -> shard name -> ShardReference object")
	numShards          = flag.Int("shards", 2, "Number of shards per keyspace. Passing -ks-shard-map/-ks-shard-map-file causes this flag to be ignored.")
	executionMode      = flag.String("execution-mode", "multi", "The execution mode to simulate -- must be set to multi, legacy-autocommit, or twopc")
	replicationMode    = flag.String("replication-mode", "ROW", "The replication mode to simulate -- must be set to either ROW or STATEMENT")
	normalize          = flag.Bool("normalize", false, "Whether to enable vtgate normalization")
	outputMode         = flag.String("output-mode", "text", "Output in human-friendly text or json")
	dbName             = flag.String("dbname", "", "Optional database target to override normal routing")
	plannerVersionStr  = flag.String("planner-version", "V3", "Sets the query planner version to use when generating the explain output. Valid values are V3 and Gen4")

	// vtexplainFlags lists all the flags that should show in usage
	vtexplainFlags = []string{
		"input-mode",
		"output-mode",
		"planner-version",
		"normalize",
		"shards",
		"replication-mode",
		"schema",
		"schema-file",
		"sql",
		"sql-file",
		"vschema",
		"vschema-file",
		"ks-shard-map",
		"ks-shard-map-file",
		"dbname",
		"queryserver-config-passthrough-dmls",
	}
)

func usage() {
	fmt.Printf("usage of vtexplain:\n")
	for _, name := range vtexplainFlags {
		f := flag.Lookup(name)
		if f == nil {
			panic("unknown flag " + name)
		}
		flagUsage(f)
	}
}

// Cloned from the source to print out the usage for a given flag
func flagUsage(f *flag.Flag) {
	s := fmt.Sprintf("  -%s", f.Name) // Two spaces before -; see next two comments.
	name, usage := flag.UnquoteUsage(f)
	if len(name) > 0 {
		s += " " + name
	}
	// Boolean flags of one ASCII letter are so common we
	// treat them specially, putting their usage on the same line.
	if len(s) <= 4 { // space, space, '-', 'x'.
		s += "\t"
	} else {
		// Four spaces before the tab triggers good alignment
		// for both 4- and 8-space tab stops.
		s += "\n    \t"
	}
	s += usage
	if name == "string" {
		// put quotes on the value
		s += fmt.Sprintf(" (default %q)", f.DefValue)
	} else {
		s += fmt.Sprintf(" (default %v)", f.DefValue)
	}
	fmt.Printf(s + "\n")
}

func init() {
	logger := logutil.NewConsoleLogger()
	flag.CommandLine.SetOutput(logutil.NewLoggerWriter(logger))
	flag.Usage = usage
}

// getFileParam returns a string containing either flag is not "",
// or the content of the file named flagFile
func getFileParam(flag, flagFile, name string, required bool) (string, error) {
	if flag != "" {
		if flagFile != "" {
			return "", fmt.Errorf("action requires only one of %v or %v-file", name, name)
		}
		return flag, nil
	}

	if flagFile == "" {
		if required {
			return "", fmt.Errorf("action requires one of %v or %v-file", name, name)
		}

		return "", nil
	}
	data, err := ioutil.ReadFile(flagFile)
	if err != nil {
		return "", fmt.Errorf("cannot read file %v: %v", flagFile, err)
	}
	return string(data), nil
}

func main() {
	defer vtexplain.Stop()
	defer exit.RecoverAll()
	defer logutil.Flush()

	servenv.ParseFlags("vtexplain")

	err := parseAndRun()
	if err != nil {
		fmt.Printf("ERROR: %s\n", err)
		exit.Return(1)
	}
}

func parseAndRun() error {
	sql, err := getFileParam(*sqlFlag, *sqlFileFlag, "sql", true)
	if err != nil {
		return err
	}

	schema, err := getFileParam(*schemaFlag, *schemaFileFlag, "schema", true)
	if err != nil {
		return err
	}

	vschema, err := getFileParam(*vschemaFlag, *vschemaFileFlag, "vschema", true)
	if err != nil {
		return err
	}

	ksShardMap, err := getFileParam(*ksShardMapFlag, *ksShardMapFileFlag, "ks-shard-map", false)
	if err != nil {
		return err
	}

	plannerVersion := querypb.ExecuteOptions_PlannerVersion(querypb.ExecuteOptions_PlannerVersion_value[*plannerVersionStr])
	if plannerVersion != querypb.ExecuteOptions_V3 && plannerVersion != querypb.ExecuteOptions_Gen4 {
		return fmt.Errorf("invalid value specified for planner-version of '%s' -- valid values are V3 and Gen4", *plannerVersionStr)
	}

	opts := &vtexplain.Options{
		ExecutionMode:   *executionMode,
		PlannerVersion:  plannerVersion,
		ReplicationMode: *replicationMode,
		NumShards:       *numShards,
		Normalize:       *normalize,
		Target:          *dbName,
	}

	log.V(100).Infof("sql %s\n", sql)
	log.V(100).Infof("schema %s\n", schema)
	log.V(100).Infof("vschema %s\n", vschema)

	err = vtexplain.Init(vschema, schema, ksShardMap, opts)
	if err != nil {
		return err
	}

	var plans []*vtexplain.Explain
	switch *inputMode {
	case "text":
		plans = vtexplain.Run(sql)
	case "json":
		plans = vtexplain.RunFromJSON(sql)
	default:
		return fmt.Errorf("invalid input-mode: %s", *inputMode)
	}

	if *outputMode == "text" {
		fmt.Print(vtexplain.ExplainsAsText(plans))
	} else {
		fmt.Print(vtexplain.ExplainsAsJSON(plans))
	}

	return nil
}
