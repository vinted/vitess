/*
Copyright 2020 The Vitess Authors.

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
package master

import (
	"context"
	"flag"
	"os"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"

	"vitess.io/vitess/go/test/endtoend/cluster"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	masterTablet    cluster.Vttablet
	replicaTablet   cluster.Vttablet
	hostname        = "localhost"
	keyspaceName    = "ks"
	cell            = "zone1"
	fastDropTable   bool
	sqlSchema       = `
	create table if not exists t1(
		id bigint not null auto_increment,
		value varchar(32),
		primary key(id)
	) Engine=InnoDB;
`

	vSchema = `
	{
    "sharded": true,
    "vindexes": {
      "hash": {
        "type": "hash"
      }
    },
    "tables": {
      "t1": {
        "column_vindexes": [
          {
            "column": "id",
            "name": "hash"
          }
        ]
      }
    }
	}`
	gcPurgeCheckInterval     = 2 * time.Second
	waitForTransitionTimeout = 30 * time.Second
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell, hostname)
		defer clusterInstance.Teardown()

		// Start topo server
		err := clusterInstance.StartTopo()
		if err != nil {
			return 1
		}

		// Set extra tablet args for lock timeout
		clusterInstance.VtTabletExtraArgs = []string{
			"-lock_tables_timeout", "5s",
			"-watch_replication_stream",
			"-enable_replication_reporter",
			"-heartbeat_enable",
			"-heartbeat_interval", "250ms",
			"-gc_check_interval", "5s",
			"-gc_purge_check_interval", "5s",
			"-table_gc_lifecycle", "hold,purge,evac,drop",
		}
		// We do not need semiSync for this test case.
		clusterInstance.EnableSemiSync = false

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name:      keyspaceName,
			SchemaSQL: sqlSchema,
			VSchema:   vSchema,
		}

		if err = clusterInstance.StartUnshardedKeyspace(*keyspace, 1, false); err != nil {
			return 1
		}

		// Collect table paths and ports
		tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets
		for _, tablet := range tablets {
			if tablet.Type == "master" {
				masterTablet = *tablet
			} else if tablet.Type != "rdonly" {
				replicaTablet = *tablet
			}
		}

		return m.Run()
	}()
	os.Exit(exitCode)
}

func checkTableRows(t *testing.T, tableName string, expect int64) {
	require.NotEmpty(t, tableName)
	query := `select count(*) as c from %a`
	parsed := sqlparser.BuildParsedQuery(query, tableName)
	rs, err := masterTablet.VttabletProcess.QueryTablet(parsed.Query, keyspaceName, true)
	require.NoError(t, err)
	count := rs.Named().Row().AsInt64("c", 0)
	assert.Equal(t, expect, count)
}

func populateTable(t *testing.T) {
	_, err := masterTablet.VttabletProcess.QueryTablet(sqlSchema, keyspaceName, true)
	require.NoError(t, err)
	_, err = masterTablet.VttabletProcess.QueryTablet("delete from t1", keyspaceName, true)
	require.NoError(t, err)
	_, err = masterTablet.VttabletProcess.QueryTablet("insert into t1 (id, value) values (null, md5(rand()))", keyspaceName, true)
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		_, err = masterTablet.VttabletProcess.QueryTablet("insert into t1 (id, value) select null, md5(rand()) from t1", keyspaceName, true)
		require.NoError(t, err)
	}
	checkTableRows(t, "t1", 1024)
}

// tableExists sees that a given table exists in MySQL
func tableExists(tableExpr string) (exists bool, tableName string, err error) {
	query := `show table status like '%a'`
	parsed := sqlparser.BuildParsedQuery(query, tableExpr)
	rs, err := masterTablet.VttabletProcess.QueryTablet(parsed.Query, keyspaceName, true)
	if err != nil {
		return false, "", err
	}
	for _, row := range rs.Named().Rows {
		return true, row.AsString("table_name", ""), nil
	}
	return false, "", nil
}

func validateTableDoesNotExist(t *testing.T, tableExpr string) {
	ctx, cancel := context.WithTimeout(context.Background(), waitForTransitionTimeout)
	defer cancel()

	ticker := time.NewTicker(time.Second)
	var foundTableName string
	var exists bool
	var err error
	for {
		select {
		case <-ticker.C:
			exists, foundTableName, err = tableExists(tableExpr)
			require.NoError(t, err)
			if !exists {
				return
			}
		case <-ctx.Done():
			assert.NoError(t, ctx.Err(), "validateTableDoesNotExist timed out, table %v still exists (%v)", tableExpr, foundTableName)
			return
		}
	}
}

func validateTableExists(t *testing.T, tableExpr string) {
	ctx, cancel := context.WithTimeout(context.Background(), waitForTransitionTimeout)
	defer cancel()

	ticker := time.NewTicker(time.Second)
	var exists bool
	var err error
	for {
		select {
		case <-ticker.C:
			exists, _, err = tableExists(tableExpr)
			require.NoError(t, err)
			if exists {
				return
			}
		case <-ctx.Done():
			assert.NoError(t, ctx.Err(), "validateTableExists timed out, table %v still does not exist", tableExpr)
			return
		}
	}
}

func validateAnyState(t *testing.T, expectNumRows int64, states ...schema.TableGCState) {
	for _, state := range states {
		expectTableToExist := true
		searchExpr := ""
		switch state {
		case schema.HoldTableGCState:
			searchExpr = `\_vt\_HOLD\_%`
		case schema.PurgeTableGCState:
			searchExpr = `\_vt\_PURGE\_%`
		case schema.EvacTableGCState:
			searchExpr = `\_vt\_EVAC\_%`
		case schema.DropTableGCState:
			searchExpr = `\_vt\_DROP\_%`
		case schema.TableDroppedGCState:
			searchExpr = `\_vt\_%`
			expectTableToExist = false
		default:
			t.Log("Unknown state")
			t.Fail()
		}
		exists, tableName, err := tableExists(searchExpr)
		require.NoError(t, err)

		if exists {
			if expectNumRows >= 0 {
				checkTableRows(t, tableName, expectNumRows)
			}
			// Now that the table is validated, we can drop it
			dropTable(t, tableName)
		}
		if exists == expectTableToExist {
			// condition met
			return
		}
	}
	assert.Failf(t, "could not match any of the states", "states=%v", states)
}

// dropTable drops a table
func dropTable(t *testing.T, tableName string) {
	query := `drop table if exists %a`
	parsed := sqlparser.BuildParsedQuery(query, tableName)
	_, err := masterTablet.VttabletProcess.QueryTablet(parsed.Query, keyspaceName, true)
	require.NoError(t, err)
}

func TestPopulateTable(t *testing.T) {
	populateTable(t)
	{
		exists, _, err := tableExists("t1")
		assert.NoError(t, err)
		assert.True(t, exists)
	}
	{
		exists, _, err := tableExists("no_such_table")
		assert.NoError(t, err)
		assert.False(t, exists)
	}
}

func TestHold(t *testing.T) {
	populateTable(t)
	query, tableName, err := schema.GenerateRenameStatement("t1", schema.HoldTableGCState, time.Now().UTC().Add(10*time.Second))
	assert.NoError(t, err)

	_, err = masterTablet.VttabletProcess.QueryTablet(query, keyspaceName, true)
	assert.NoError(t, err)

	{
		exists, _, err := tableExists("t1")
		assert.NoError(t, err)
		assert.False(t, exists)
	}
	{
		exists, _, err := tableExists(tableName)
		assert.NoError(t, err)
		assert.True(t, exists)
	}

	time.Sleep(5 * time.Second)
	{
		// Table was created with +10s timestamp, so it should still exist
		exists, _, err := tableExists(tableName)
		assert.NoError(t, err)
		assert.True(t, exists)

		checkTableRows(t, tableName, 1024)
	}

	time.Sleep(10 * time.Second)
	{
		// We're now both beyond table's timestamp as well as a tableGC interval
		exists, _, err := tableExists(tableName)
		assert.NoError(t, err)
		assert.False(t, exists)
	}
	{
		// Table should be renamed as _vt_PURGE_...
		exists, purgeTableName, err := tableExists(`\_vt\_PURGE\_%`)
		assert.NoError(t, err)
		assert.True(t, exists)
		dropTable(t, purgeTableName)
	}
}

func TestEvac(t *testing.T) {
	populateTable(t)
	query, tableName, err := schema.GenerateRenameStatement("t1", schema.EvacTableGCState, time.Now().UTC().Add(10*time.Second))
	assert.NoError(t, err)

	_, err = masterTablet.VttabletProcess.QueryTablet(query, keyspaceName, true)
	assert.NoError(t, err)

	{
		exists, _, err := tableExists("t1")
		assert.NoError(t, err)
		assert.False(t, exists)
	}
	{
		exists, _, err := tableExists(tableName)
		assert.NoError(t, err)
		assert.True(t, exists)
	}

	time.Sleep(5 * time.Second)
	{
		// Table was created with +10s timestamp, so it should still exist
		exists, _, err := tableExists(tableName)
		assert.NoError(t, err)
		assert.True(t, exists)

		checkTableRows(t, tableName, 1024)
	}

	time.Sleep(10 * time.Second)
	{
		// We're now both beyond table's timestamp as well as a tableGC interval
		exists, _, err := tableExists(tableName)
		assert.NoError(t, err)
		assert.False(t, exists)
	}
	time.Sleep(5 * time.Second)
	{
		// Table should be renamed as _vt_DROP_... and then dropped!
		exists, _, err := tableExists(`\_vt\_DROP\_%`)
		assert.NoError(t, err)
		assert.False(t, exists)
	}
}

func TestDrop(t *testing.T) {
	populateTable(t)
	query, tableName, err := schema.GenerateRenameStatement("t1", schema.DropTableGCState, time.Now().UTC().Add(10*time.Second))
	assert.NoError(t, err)

	_, err = masterTablet.VttabletProcess.QueryTablet(query, keyspaceName, true)
	assert.NoError(t, err)

	{
		exists, _, err := tableExists("t1")
		assert.NoError(t, err)
		assert.False(t, exists)
	}

	time.Sleep(20 * time.Second) // 10s for timestamp to pass, then 10s for checkTables and drop of table
	{
		// We're now both beyond table's timestamp as well as a tableGC interval
		exists, _, err := tableExists(tableName)
		assert.NoError(t, err)
		assert.False(t, exists)
	}
}

func TestPurge(t *testing.T) {
	populateTable(t)
	query, tableName, err := schema.GenerateRenameStatement("t1", schema.PurgeTableGCState, time.Now().UTC().Add(10*time.Second))
	require.NoError(t, err)

	_, err = masterTablet.VttabletProcess.QueryTablet(query, keyspaceName, true)
	require.NoError(t, err)

	validateTableDoesNotExist(t, "t1")
	if !fastDropTable {
		validateTableExists(t, tableName)
		checkTableRows(t, tableName, 1024)
	}
	if !fastDropTable {
		time.Sleep(5 * gcPurgeCheckInterval) // wait for table to be purged
	}
	validateTableDoesNotExist(t, tableName) // whether purged or not, table should at some point transition to next state
	if fastDropTable {
		// if MySQL supports fast DROP TABLE, TableGC completely skips the PURGE state. Rows are not purged.
		validateAnyState(t, 1024, schema.DropTableGCState, schema.TableDroppedGCState)
	} else {
		validateAnyState(t, 0, schema.EvacTableGCState, schema.DropTableGCState, schema.TableDroppedGCState)
	}
}
