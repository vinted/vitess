/*
Copyright 2026 The Vitess Authors.

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
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

// TestSkipLockedAndNowait checks the MySQL 8 locking read options
// (FOR UPDATE / FOR SHARE with SKIP LOCKED / NOWAIT) end to end through vtgate.
func TestSkipLockedAndNowait(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()

	connA, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer connA.Close()
	connB, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer connB.Close()

	// SKIP LOCKED / NOWAIT / FOR SHARE need MySQL 8.0.
	version := exec(t, connA, "select @@version").Rows[0][0].ToString()
	if strings.HasPrefix(version, "5.") {
		t.Skipf("SKIP LOCKED / NOWAIT need MySQL 8.0, got %s", version)
	}

	exec(t, connA, "create table lock_test (id bigint, val varchar(10), primary key(id))")
	defer func() {
		_, _ = connA.ExecuteFetch("rollback", 1, false)
		_, _ = connB.ExecuteFetch("rollback", 1, false)
		exec(t, connA, "drop table lock_test")
	}()
	exec(t, connA, "insert into lock_test(id, val) values (1, 'a'), (2, 'b'), (3, 'c')")

	// Connection A locks row 1 and keeps its transaction open.
	exec(t, connA, "begin")
	assertRows(t, connA, "select id from lock_test where id = 1 for update", "[[INT64(1)]]")

	t.Run("skip locked", func(t *testing.T) {
		exec(t, connB, "begin")
		assertRows(t, connB, "select id from lock_test order by id for update skip locked", "[[INT64(2)] [INT64(3)]]")
		assertRows(t, connB, "select id from lock_test order by id for share skip locked", "[[INT64(2)] [INT64(3)]]")
		exec(t, connB, "rollback")
	})

	t.Run("nowait", func(t *testing.T) {
		exec(t, connB, "begin")
		// B takes its own lock on row 2 before the NOWAIT failure.
		assertRows(t, connB, "select id from lock_test where id = 2 for update", "[[INT64(2)]]")

		assertLockNowaitError(t, connB, "select id from lock_test where id = 1 for update nowait")
		assertLockNowaitError(t, connB, "select id from lock_test where id = 1 for share nowait")

		// MySQL keeps the transaction open after a NOWAIT failure, so vtgate must not
		// roll it back: B still holds row 2, and A cannot take it.
		assertLockNowaitError(t, connA, "select id from lock_test where id = 2 for update nowait")

		// B's transaction is still usable and commits normally.
		assertRows(t, connB, "select id from lock_test where id = 3 for update", "[[INT64(3)]]")
		exec(t, connB, "commit")

		// After B commits, row 2 is free for A.
		assertRows(t, connA, "select id from lock_test where id = 2 for update nowait", "[[INT64(2)]]")
	})

	exec(t, connA, "rollback")
}

func assertRows(t *testing.T, conn *mysql.Conn, query, expected string) {
	t.Helper()
	qr := exec(t, conn, query)
	assert.Equal(t, expected, fmt.Sprintf("%v", qr.Rows), "for query: %s", query)
}

func assertLockNowaitError(t *testing.T, conn *mysql.Conn, query string) {
	t.Helper()
	_, err := conn.ExecuteFetch(query, 1000, true)
	require.Error(t, err, "for query: %s", query)
	sqlErr, ok := err.(*mysql.SQLError)
	require.True(t, ok, "expected *mysql.SQLError for query %s, got %T: %v", query, err, err)
	assert.Equal(t, mysql.ERLockNowait, sqlErr.Number(), "for query %s: %v", query, err)
}
