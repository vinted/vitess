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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"context"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/hack"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/streamlog"
	"vitess.io/vitess/go/vt/callinfo"
	"vitess.io/vitess/go/vt/callinfo/fakecallinfo"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestMain(m *testing.M) {
	hack.DisableProtoBufRandomness()
	os.Exit(m.Run())
}

func testFormat(t *testing.T, stats *LogStats, params url.Values) string {
	var b bytes.Buffer
	err := stats.Logf(&b, params)
	require.NoError(t, err)
	return b.String()
}

func TestLogStatsFormat(t *testing.T) {
	defer func() {
		*streamlog.RedactDebugUIQueries = false
		*streamlog.QueryLogFormat = "text"
	}()
	logStats := NewLogStats(context.Background(), "test", "sql1", "suuid", nil)
	logStats.StartTime = time.Date(2017, time.January, 1, 1, 2, 3, 0, time.UTC)
	logStats.EndTime = time.Date(2017, time.January, 1, 1, 2, 4, 1234, time.UTC)
	logStats.Table = "ks1.tbl1,ks2.tbl2"
	logStats.TabletType = "MASTER"
	logStats.Keyspace = "db"
	params := map[string][]string{"full": {}}
	intBindVar := map[string]*querypb.BindVariable{"intVal": sqltypes.Int64BindVariable(1)}
	stringBindVar := map[string]*querypb.BindVariable{"strVal": sqltypes.StringBindVariable("abc")}

	tests := []struct {
		name     string
		redact   bool
		format   string
		expected string
		bindVars map[string]*querypb.BindVariable
	}{
		{ // 0
			redact:   false,
			format:   "text",
			expected: "test\t\t\t''\t''\t2017-01-01 01:02:03.000000\t2017-01-01 01:02:04.000001\t1.000001\t0.000000\t0.000000\t0.000000\t\t\"sql1\"\t{\"intVal\": {\"Type\": \"INT64\", \"Value\": 1}}\t0\t0\t\"\"\t\"MASTER\"\t\"suuid\"\tfalse\t\"ks1.tbl1,ks2.tbl2\"\t\"db\"\n",
			bindVars: intBindVar,
		}, { // 1
			redact:   true,
			format:   "text",
			expected: "test\t\t\t''\t''\t2017-01-01 01:02:03.000000\t2017-01-01 01:02:04.000001\t1.000001\t0.000000\t0.000000\t0.000000\t\t\"sql1\"\t\"[REDACTED]\"\t0\t0\t\"\"\t\"MASTER\"\t\"suuid\"\tfalse\t\"ks1.tbl1,ks2.tbl2\"\t\"db\"\n",
			bindVars: intBindVar,
		}, { // 2
			redact:   false,
			format:   "json",
			expected: "{\"BindVars\":{\"intVal\":{\"Type\":\"INT64\",\"Value\":1}},\"CommitTime\":0,\"Effective Caller\":\"\",\"End\":\"2017-01-01 01:02:04.000001\",\"Error\":\"\",\"ExecuteTime\":0,\"ImmediateCaller\":\"\",\"InTransaction\":false,\"Keyspace\":\"db\",\"Method\":\"test\",\"PlanTime\":0,\"RemoteAddr\":\"\",\"RowsAffected\":0,\"SQL\":\"sql1\",\"SessionUUID\":\"suuid\",\"ShardQueries\":0,\"Start\":\"2017-01-01 01:02:03.000000\",\"StmtType\":\"\",\"Table\":\"ks1.tbl1,ks2.tbl2\",\"TabletType\":\"MASTER\",\"TotalTime\":1.000001,\"Username\":\"\"}",
			bindVars: intBindVar,
		}, { // 3
			redact:   true,
			format:   "json",
			expected: "{\"BindVars\":\"[REDACTED]\",\"CommitTime\":0,\"Effective Caller\":\"\",\"End\":\"2017-01-01 01:02:04.000001\",\"Error\":\"\",\"ExecuteTime\":0,\"ImmediateCaller\":\"\",\"InTransaction\":false,\"Keyspace\":\"db\",\"Method\":\"test\",\"PlanTime\":0,\"RemoteAddr\":\"\",\"RowsAffected\":0,\"SQL\":\"sql1\",\"SessionUUID\":\"suuid\",\"ShardQueries\":0,\"Start\":\"2017-01-01 01:02:03.000000\",\"StmtType\":\"\",\"Table\":\"ks1.tbl1,ks2.tbl2\",\"TabletType\":\"MASTER\",\"TotalTime\":1.000001,\"Username\":\"\"}",
			bindVars: intBindVar,
		}, { // 4
			redact:   false,
			format:   "text",
			expected: "test\t\t\t''\t''\t2017-01-01 01:02:03.000000\t2017-01-01 01:02:04.000001\t1.000001\t0.000000\t0.000000\t0.000000\t\t\"sql1\"\t{\"strVal\": {\"Type\": \"VARBINARY\", \"Value\": \"abc\"}}\t0\t0\t\"\"\t\"MASTER\"\t\"suuid\"\tfalse\t\"ks1.tbl1,ks2.tbl2\"\t\"db\"\n",
			bindVars: stringBindVar,
		}, { // 5
			redact:   true,
			format:   "text",
			expected: "test\t\t\t''\t''\t2017-01-01 01:02:03.000000\t2017-01-01 01:02:04.000001\t1.000001\t0.000000\t0.000000\t0.000000\t\t\"sql1\"\t\"[REDACTED]\"\t0\t0\t\"\"\t\"MASTER\"\t\"suuid\"\tfalse\t\"ks1.tbl1,ks2.tbl2\"\t\"db\"\n",
			bindVars: stringBindVar,
		}, { // 6
			redact:   false,
			format:   "json",
			expected: "{\"BindVars\":{\"strVal\":{\"Type\":\"VARBINARY\",\"Value\":\"abc\"}},\"CommitTime\":0,\"Effective Caller\":\"\",\"End\":\"2017-01-01 01:02:04.000001\",\"Error\":\"\",\"ExecuteTime\":0,\"ImmediateCaller\":\"\",\"InTransaction\":false,\"Keyspace\":\"db\",\"Method\":\"test\",\"PlanTime\":0,\"RemoteAddr\":\"\",\"RowsAffected\":0,\"SQL\":\"sql1\",\"SessionUUID\":\"suuid\",\"ShardQueries\":0,\"Start\":\"2017-01-01 01:02:03.000000\",\"StmtType\":\"\",\"Table\":\"ks1.tbl1,ks2.tbl2\",\"TabletType\":\"MASTER\",\"TotalTime\":1.000001,\"Username\":\"\"}",
			bindVars: stringBindVar,
		}, { // 7
			redact:   true,
			format:   "json",
			expected: "{\"BindVars\":\"[REDACTED]\",\"CommitTime\":0,\"Effective Caller\":\"\",\"End\":\"2017-01-01 01:02:04.000001\",\"Error\":\"\",\"ExecuteTime\":0,\"ImmediateCaller\":\"\",\"InTransaction\":false,\"Keyspace\":\"db\",\"Method\":\"test\",\"PlanTime\":0,\"RemoteAddr\":\"\",\"RowsAffected\":0,\"SQL\":\"sql1\",\"SessionUUID\":\"suuid\",\"ShardQueries\":0,\"Start\":\"2017-01-01 01:02:03.000000\",\"StmtType\":\"\",\"Table\":\"ks1.tbl1,ks2.tbl2\",\"TabletType\":\"MASTER\",\"TotalTime\":1.000001,\"Username\":\"\"}",
			bindVars: stringBindVar,
		},
	}

	for i, test := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			logStats.BindVariables = test.bindVars
			for _, variable := range logStats.BindVariables {
				fmt.Println("->" + fmt.Sprintf("%v", variable))
			}
			*streamlog.RedactDebugUIQueries = test.redact
			*streamlog.QueryLogFormat = test.format
			if test.format == "text" {
				got := testFormat(t, logStats, params)
				t.Logf("got: %s", got)
				assert.Equal(t, test.expected, got)
				return
			}

			got := testFormat(t, logStats, params)
			t.Logf("got: %s", got)
			var parsed map[string]interface{}
			err := json.Unmarshal([]byte(got), &parsed)
			assert.NoError(t, err)
			assert.NotNil(t, parsed)
			formatted, err := json.Marshal(parsed)
			assert.NoError(t, err)
			assert.Equal(t, test.expected, string(formatted))
		})
	}
}

func TestLogStatsFilter(t *testing.T) {
	defer func() { *streamlog.QueryLogFilterTag = "" }()

	logStats := NewLogStats(context.Background(), "test", "sql1 /* LOG_THIS_QUERY */", "", map[string]*querypb.BindVariable{"intVal": sqltypes.Int64BindVariable(1)})
	logStats.StartTime = time.Date(2017, time.January, 1, 1, 2, 3, 0, time.UTC)
	logStats.EndTime = time.Date(2017, time.January, 1, 1, 2, 4, 1234, time.UTC)
	params := map[string][]string{"full": {}}

	got := testFormat(t, logStats, params)
	want := "test\t\t\t''\t''\t2017-01-01 01:02:03.000000\t2017-01-01 01:02:04.000001\t1.000001\t0.000000\t0.000000\t0.000000\t\t\"sql1 /* LOG_THIS_QUERY */\"\t{\"intVal\": {\"Type\": \"INT64\", \"Value\": 1}}\t0\t0\t\"\"\t\"\"\t\"\"\tfalse\t\"\"\t\"\"\n"
	assert.Equal(t, want, got)

	*streamlog.QueryLogFilterTag = "LOG_THIS_QUERY"
	got = testFormat(t, logStats, params)
	want = "test\t\t\t''\t''\t2017-01-01 01:02:03.000000\t2017-01-01 01:02:04.000001\t1.000001\t0.000000\t0.000000\t0.000000\t\t\"sql1 /* LOG_THIS_QUERY */\"\t{\"intVal\": {\"Type\": \"INT64\", \"Value\": 1}}\t0\t0\t\"\"\t\"\"\t\"\"\tfalse\t\"\"\t\"\"\n"
	assert.Equal(t, want, got)

	*streamlog.QueryLogFilterTag = "NOT_THIS_QUERY"
	got = testFormat(t, logStats, params)
	want = ""
	assert.Equal(t, want, got)
}

func TestLogStatsRowThreshold(t *testing.T) {
	defer func() { *streamlog.QueryLogRowThreshold = 0 }()

	logStats := NewLogStats(context.Background(), "test", "sql1 /* LOG_THIS_QUERY */", "", map[string]*querypb.BindVariable{"intVal": sqltypes.Int64BindVariable(1)})
	logStats.StartTime = time.Date(2017, time.January, 1, 1, 2, 3, 0, time.UTC)
	logStats.EndTime = time.Date(2017, time.January, 1, 1, 2, 4, 1234, time.UTC)
	params := map[string][]string{"full": {}}

	got := testFormat(t, logStats, params)
	want := "test\t\t\t''\t''\t2017-01-01 01:02:03.000000\t2017-01-01 01:02:04.000001\t1.000001\t0.000000\t0.000000\t0.000000\t\t\"sql1 /* LOG_THIS_QUERY */\"\t{\"intVal\": {\"Type\": \"INT64\", \"Value\": 1}}\t0\t0\t\"\"\t\"\"\t\"\"\tfalse\t\"\"\t\"\"\n"
	assert.Equal(t, want, got)

	*streamlog.QueryLogRowThreshold = 0
	got = testFormat(t, logStats, params)
	want = "test\t\t\t''\t''\t2017-01-01 01:02:03.000000\t2017-01-01 01:02:04.000001\t1.000001\t0.000000\t0.000000\t0.000000\t\t\"sql1 /* LOG_THIS_QUERY */\"\t{\"intVal\": {\"Type\": \"INT64\", \"Value\": 1}}\t0\t0\t\"\"\t\"\"\t\"\"\tfalse\t\"\"\t\"\"\n"
	assert.Equal(t, want, got)
	*streamlog.QueryLogRowThreshold = 1
	got = testFormat(t, logStats, params)
	assert.Empty(t, got)
}

func TestLogStatsContextHTML(t *testing.T) {
	html := "HtmlContext"
	callInfo := &fakecallinfo.FakeCallInfo{
		Html: html,
	}
	ctx := callinfo.NewContext(context.Background(), callInfo)
	logStats := NewLogStats(ctx, "test", "sql1", "", map[string]*querypb.BindVariable{})
	if string(logStats.ContextHTML()) != html {
		t.Fatalf("expect to get html: %s, but got: %s", html, logStats.ContextHTML())
	}
}

func TestLogStatsErrorStr(t *testing.T) {
	logStats := NewLogStats(context.Background(), "test", "sql1", "", map[string]*querypb.BindVariable{})
	if logStats.ErrorStr() != "" {
		t.Fatalf("should not get error in stats, but got: %s", logStats.ErrorStr())
	}
	errStr := "unknown error"
	logStats.Error = errors.New(errStr)
	if !strings.Contains(logStats.ErrorStr(), errStr) {
		t.Fatalf("expect string '%s' in error message, but got: %s", errStr, logStats.ErrorStr())
	}
}

func TestLogStatsRemoteAddrUsername(t *testing.T) {
	logStats := NewLogStats(context.Background(), "test", "sql1", "", map[string]*querypb.BindVariable{})
	addr, user := logStats.RemoteAddrUsername()
	if addr != "" {
		t.Fatalf("remote addr should be empty")
	}
	if user != "" {
		t.Fatalf("username should be empty")
	}

	remoteAddr := "1.2.3.4"
	username := "vt"
	callInfo := &fakecallinfo.FakeCallInfo{
		Remote: remoteAddr,
		User:   username,
	}
	ctx := callinfo.NewContext(context.Background(), callInfo)
	logStats = NewLogStats(ctx, "test", "sql1", "", map[string]*querypb.BindVariable{})
	addr, user = logStats.RemoteAddrUsername()
	if addr != remoteAddr {
		t.Fatalf("expected to get remote addr: %s, but got: %s", remoteAddr, addr)
	}
	if user != username {
		t.Fatalf("expected to get username: %s, but got: %s", username, user)
	}
}

func TestLogStatsFormatJSONV2(t *testing.T) {
	defer func() {
		*streamlog.QueryLogFormat = "text"
		*streamlog.QueryLogJSONV2 = false
	}()
	*streamlog.QueryLogFormat = "json"
	*streamlog.QueryLogJSONV2 = true
	params := map[string][]string{"full": {}}
	logStats := NewLogStats(context.Background(), "test", "sql1", "suuid", nil)
	logStats.StartTime = time.Date(2017, time.January, 1, 1, 2, 3, 0, time.UTC)
	logStats.EndTime = time.Date(2017, time.January, 1, 1, 2, 4, 1234, time.UTC)
	logStats.BindVariables = map[string]*querypb.BindVariable{}
	testsEncoded := []struct {
		bindVar  *querypb.BindVariable
		expected string
	}{
		{ // {"Type": "BINARY", "Value": "\x16k@\xb4J\xbaK\xd6"} breaks JSON, so we base64 endcode the value
			&querypb.BindVariable{Type: querypb.Type_BINARY, Value: []byte("\x16k@\xb4J\xbaK\xd6")},
			"FmtAtEq6S9Y=",
		}, {
			&querypb.BindVariable{Type: querypb.Type_BIT, Value: []byte("134")},
			"MTM0",
		}, {
			&querypb.BindVariable{Type: querypb.Type_BLOB, Value: []byte("a")},
			"YQ==",
		}, {
			&querypb.BindVariable{Type: querypb.Type_CHAR, Value: []byte("a")},
			"YQ==",
		}, {
			&querypb.BindVariable{Type: querypb.Type_DATE, Value: []byte("2012-02-24")},
			"MjAxMi0wMi0yNA==",
		}, {
			&querypb.BindVariable{Type: querypb.Type_DATETIME, Value: []byte("2012-02-24 23:19:43")},
			"MjAxMi0wMi0yNCAyMzoxOTo0Mw==",
		}, {
			&querypb.BindVariable{Type: querypb.Type_DECIMAL, Value: []byte("1.00")},
			"MS4wMA==",
		}, {
			&querypb.BindVariable{Type: querypb.Type_ENUM, Value: []byte("a")},
			"YQ==",
		}, {
			&querypb.BindVariable{Type: querypb.Type_SET, Value: []byte("a")},
			"YQ==",
		}, {
			&querypb.BindVariable{Type: querypb.Type_TEXT, Value: []byte("a")},
			"YQ==",
		}, {
			&querypb.BindVariable{Type: querypb.Type_TIME, Value: []byte("23:19:43")},
			"MjM6MTk6NDM=",
		}, {
			&querypb.BindVariable{Type: querypb.Type_TIMESTAMP, Value: []byte("2012-02-24 23:19:43")},
			"MjAxMi0wMi0yNCAyMzoxOTo0Mw==",
		},
	}

	for i, test := range testsEncoded {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			logStats.BindVariables["vtg"] = test.bindVar
			got := testFormat(t, logStats, params)
			t.Logf("got: %s", got)
			var parsed map[string]interface{}
			err := json.Unmarshal([]byte(got), &parsed)
			assert.NoError(t, err)
			assert.NotNil(t, parsed)
			bindType := parsed["BindVars"].(map[string]interface{})["vtg"].(map[string]interface{})["Type"].(string)
			bindVal := parsed["BindVars"].(map[string]interface{})["vtg"].(map[string]interface{})["Value"]
			assert.Equal(t, test.bindVar.Type.String(), bindType)
			assert.Equal(t, test.expected, bindVal)
		})
	}

	testsUnchanged := []struct {
		bindVar  *querypb.BindVariable
		expected interface{}
	}{
		{
			&querypb.BindVariable{Type: querypb.Type_FLOAT32, Value: []byte("1.00")},
			float64(1),
		}, {
			&querypb.BindVariable{Type: querypb.Type_FLOAT64, Value: []byte("1.00")},
			float64(1),
		}, {
			&querypb.BindVariable{Type: querypb.Type_INT8, Value: []byte("1")},
			float64(1),
		}, {
			&querypb.BindVariable{Type: querypb.Type_INT16, Value: []byte("1")},
			float64(1),
		}, {
			&querypb.BindVariable{Type: querypb.Type_INT24, Value: []byte("1")},
			float64(1),
		}, {
			&querypb.BindVariable{Type: querypb.Type_INT32, Value: []byte("1")},
			float64(1),
		}, {
			&querypb.BindVariable{Type: querypb.Type_INT64, Value: []byte("1")},
			float64(1),
		}, {
			&querypb.BindVariable{Type: querypb.Type_UINT8, Value: []byte("1")},
			float64(1),
		}, {
			&querypb.BindVariable{Type: querypb.Type_UINT16, Value: []byte("1")},
			float64(1),
		}, {
			&querypb.BindVariable{Type: querypb.Type_UINT24, Value: []byte("1")},
			float64(1),
		}, {
			&querypb.BindVariable{Type: querypb.Type_UINT32, Value: []byte("1")},
			float64(1),
		}, {
			&querypb.BindVariable{Type: querypb.Type_UINT64, Value: []byte("1")},
			float64(1),
		},
	}

	for i, test := range testsUnchanged {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			logStats.BindVariables["vtg"] = test.bindVar
			got := testFormat(t, logStats, params)
			t.Logf("got: %s", got)
			var parsed map[string]interface{}
			err := json.Unmarshal([]byte(got), &parsed)
			assert.NoError(t, err)
			assert.NotNil(t, parsed)
			bindType := parsed["BindVars"].(map[string]interface{})["vtg"].(map[string]interface{})["Type"].(string)
			bindVal := parsed["BindVars"].(map[string]interface{})["vtg"].(map[string]interface{})["Value"]
			assert.Equal(t, test.bindVar.Type.String(), bindType)
			assert.Equal(t, test.expected, bindVal)
		})
	}

	testsUnchangedTuples := []struct {
		bindVar  *querypb.BindVariable
		expected string
	}{
		{
			&querypb.BindVariable{Type: querypb.Type_TUPLE, Values: []*querypb.Value{
				{Type: querypb.Type_FLOAT64, Value: []byte("1")},
				{Type: querypb.Type_FLOAT64, Value: []byte("2")},
			}},
			"2 items",
		}, {
			&querypb.BindVariable{Type: querypb.Type_TUPLE, Values: []*querypb.Value{
				{Type: querypb.Type_INT64, Value: []byte("1")},
				{Type: querypb.Type_INT64, Value: []byte("2")},
			}},
			"2 items",
		}, {
			&querypb.BindVariable{Type: querypb.Type_TUPLE, Values: []*querypb.Value{
				{Type: querypb.Type_UINT64, Value: []byte("1")},
				{Type: querypb.Type_UINT64, Value: []byte("2")},
			}},
			"2 items",
		}, {
			&querypb.BindVariable{Type: querypb.Type_TUPLE, Values: []*querypb.Value{
				{Type: querypb.Type_VARBINARY, Value: []byte("aa")},
				{Type: querypb.Type_VARBINARY, Value: []byte("bb")},
			}},
			"2 items",
		}, {
			&querypb.BindVariable{Type: querypb.Type_TUPLE, Values: []*querypb.Value{
				{Type: querypb.Type_VARBINARY, Value: []byte("aa")},
				{Type: querypb.Type_INT64, Value: []byte("1")},
			}},
			"2 items",
		},
	}

	for i, test := range testsUnchangedTuples {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			logStats.BindVariables["vtg"] = test.bindVar
			got := testFormat(t, logStats, params)
			t.Logf("got: %s", got)
			var parsed map[string]interface{}
			err := json.Unmarshal([]byte(got), &parsed)
			assert.NoError(t, err)
			assert.NotNil(t, parsed)
			bindType := parsed["BindVars"].(map[string]interface{})["vtg"].(map[string]interface{})["Type"].(string)
			bindVal := parsed["BindVars"].(map[string]interface{})["vtg"].(map[string]interface{})["Value"]
			assert.Equal(t, test.bindVar.Type.String(), bindType)
			assert.Equal(t, test.expected, bindVal)
		})
	}
}
