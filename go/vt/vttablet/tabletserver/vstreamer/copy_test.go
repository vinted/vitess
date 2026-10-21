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

package vstreamer

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

// TestVStreamCopyPhaseDuration copies a table to a slow consumer, changing rows that are already
// copied and rows that are not copied yet in the middle of the copy, and checks that the latest
// streamed image of every row is equal to the table. With vstream_copy_phase_duration set, the copy
// of the table must take several copy cycles, each one under its own snapshot.
func TestVStreamCopyPhaseDuration(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	testCases := []struct {
		name          string
		phaseDuration time.Duration
		check         func(t *testing.T, copyCycles int)
	}{{
		name:          "disabled by default: single snapshot",
		phaseDuration: 0,
		check: func(t *testing.T, copyCycles int) {
			require.Equal(t, 1, copyCycles)
		},
	}, {
		name:          "enabled: several copy cycles",
		phaseDuration: 500 * time.Millisecond,
		check: func(t *testing.T, copyCycles int) {
			require.GreaterOrEqual(t, copyCycles, 2)
		},
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			copyCycles := copyWithChanges(t, tc.phaseDuration)
			t.Logf("copy cycles: %d", copyCycles)
			tc.check(t, copyCycles)
		})
	}
}

// copyWithChanges streams the copy of a table, changes the table while it is being copied and
// returns the number of copy cycles it took
func copyWithChanges(t *testing.T, phaseDuration time.Duration) (copyCycles int) {
	const (
		table       = "copy_phase_duration"
		numRows     = 100
		changeAtRow = "10"   // change the table once the copy streams this row
		sentinelID  = "1000" // inserted after the copy, its event marks that all changes were streamed
	)

	values := make([]string, 0, numRows)
	for i := 1; i <= numRows; i++ {
		values = append(values, fmt.Sprintf("(%d, 0)", i))
	}
	execStatements(t, []string{
		fmt.Sprintf("create table %s(id int, val int, primary key(id))", table),
		fmt.Sprintf("insert into %s values %s", table, strings.Join(values, ", ")),
	})
	defer execStatements(t, []string{"drop table " + table})
	require.NoError(t, engine.se.Reload(context.Background()))

	// stream a row per packet to a slow consumer, so that copying the table takes a few seconds
	defer AdjustPacketSize(1)()
	defer func(original time.Duration) { *copyPhaseDuration = original }(*copyPhaseDuration)
	*copyPhaseDuration = phaseDuration
	// sends a "Copy Start <table>" event at the start of every copy cycle
	uvstreamerTestMode = true
	defer func() { uvstreamerTestMode = false }()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	// the stream callback runs on more than one goroutine, so it returns errors instead of failing t
	exec := func(queries ...string) error {
		return env.Mysqld.ExecuteSuperQueryList(ctx, queries)
	}

	streamed := make(map[string]string) // id -> val of the latest streamed image of each row
	var changed, copyDone bool

	filter := &binlogdatapb.Filter{Rules: []*binlogdatapb.Rule{{
		Match:  table,
		Filter: "select * from " + table,
	}}}

	err := engine.Stream(ctx, "", nil, filter, func(evs []*binlogdatapb.VEvent) error {
		changeNow, sentinelSeen := false, false

		for _, ev := range evs {
			switch ev.Type {
			case binlogdatapb.VEventType_OTHER:
				switch ev.Gtid {
				case "Copy Start " + table:
					copyCycles++
				case "Copy Done":
					copyDone = true
					// rows changed during the last copy cycle are streamed after the copy
					if err := exec(fmt.Sprintf("insert into %s values (%s, 0)", table, sentinelID)); err != nil {
						return err
					}
				}
			case binlogdatapb.VEventType_ROW:
				for _, change := range ev.RowEvent.RowChanges {
					if change.After == nil {
						delete(streamed, rowValues(change.Before)[0])
						continue
					}
					row := rowValues(change.After)
					streamed[row[0]] = row[1]
					changeNow = changeNow || (!changed && row[0] == changeAtRow)
					sentinelSeen = sentinelSeen || (copyDone && row[0] == sentinelID)
				}
			}
		}

		if sentinelSeen {
			return io.EOF
		}
		if changeNow {
			changed = true
			if err := exec(
				fmt.Sprintf("update %s set val = 1 where id = 1", table),           // already copied
				fmt.Sprintf("delete from %s where id = 2", table),                  // already copied
				fmt.Sprintf("update %s set val = 1 where id = %d", table, numRows), // not copied yet
				fmt.Sprintf("insert into %s values (%d, 1)", table, numRows+1),     // not copied yet
			); err != nil {
				return err
			}
		}
		if !copyDone {
			time.Sleep(20 * time.Millisecond)
		}
		return nil
	})
	if err != io.EOF {
		require.NoError(t, err)
	}
	require.True(t, changed, "the table was not changed during the copy")

	qr, err := env.Mysqld.FetchSuperQuery(context.Background(), "select id, val from "+table)
	require.NoError(t, err)
	want := make(map[string]string, len(qr.Rows))
	for _, row := range qr.Rows {
		want[row[0].ToString()] = row[1].ToString()
	}
	require.Equal(t, want, streamed)

	return copyCycles
}

// rowValues returns the values of a row as strings
func rowValues(row *querypb.Row) []string {
	values := make([]string, 0, len(row.Lengths))
	offset := int64(0)
	for _, length := range row.Lengths {
		if length < 0 {
			values = append(values, "NULL")
			continue
		}
		values = append(values, string(row.Values[offset:offset+length]))
		offset += length
	}
	return values
}
