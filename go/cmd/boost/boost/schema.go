package boost

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqlescape"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/dbconnpool"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"
	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"
)

// getPoolReconnect gets a connection from a pool, tests it, and reconnects if
// the connection is lost.
func getPoolReconnect(ctx context.Context, pool *dbconnpool.ConnectionPool) (*dbconnpool.PooledDBConnection, error) {
	conn, err := pool.Get(ctx)
	if err != nil {
		return conn, err
	}
	// Run a test query to see if this connection is still good.
	if _, err := conn.ExecuteFetch("SELECT 1", 1, false); err != nil {
		// If we get a connection error, try to reconnect.
		if sqlErr, ok := err.(*mysql.SQLError); ok && (sqlErr.Number() == mysql.CRServerGone || sqlErr.Number() == mysql.CRServerLost) {
			if err := conn.Reconnect(ctx); err != nil {
				conn.Recycle()
				return nil, err
			}
			return conn, nil
		}
		conn.Recycle()
		return nil, err
	}
	return conn, nil
}

func buildColInfoMap(ctx context.Context, dbPool *dbconnpool.ConnectionPool) (map[string][]*vreplication.ColumnInfo, error) {
	conn, connErr := getPoolReconnect(ctx, dbPool)
	if connErr != nil {
		return nil, connErr
	}
	defer conn.Recycle()

	// we don't know dbname, because connecting through vtgate
	dbName := ""
	schema, err := GetSchema(ctx, dbName, []string{"/.*/"}, nil, false, dbPool)
	if err != nil {
		log.Errorf("GetSchema error: %s", err)
		return nil, err
	}

	// queryTemplate := "select character_set_name, collation_name, column_name, data_type, column_type, extra from information_schema.columns where table_schema=%s and table_name=%s;"
	queryTemplate := "select character_set_name, collation_name, column_name, data_type, column_type, extra from information_schema.columns where table_schema like 'vt_%%' and table_name=%s;"
	colInfoMap := make(map[string][]*vreplication.ColumnInfo)
	for _, td := range schema.TableDefinitions {
		// query := fmt.Sprintf(queryTemplate, encodeString(dbclient.DBName()), encodeString(td.Name))
		query := fmt.Sprintf(queryTemplate, encodeString(td.Name))
		qr, err := conn.ExecuteFetch(query, 10000, true)
		if err != nil {
			return nil, err
		}
		if len(qr.Rows) == 0 {
			return nil, fmt.Errorf("no data returned from information_schema.columns")
		}

		var pks []string
		if len(td.PrimaryKeyColumns) != 0 {
			pks = td.PrimaryKeyColumns
		} else {
			pks = td.Columns
		}
		var colInfo []*vreplication.ColumnInfo
		for _, row := range qr.Rows {
			charSet := ""
			collation := ""
			columnName := ""
			isPK := false
			isGenerated := false
			var dataType, columnType string
			columnName = row[2].ToString()
			var currentField *querypb.Field
			for _, field := range td.Fields {
				if field.Name == columnName {
					currentField = field
					break
				}
			}
			if currentField == nil {
				continue
			}
			dataType = row[3].ToString()
			columnType = row[4].ToString()
			if sqltypes.IsText(currentField.Type) {
				charSet = row[0].ToString()
				collation = row[1].ToString()
			}
			if dataType == "" || columnType == "" {
				return nil, fmt.Errorf("no dataType/columnType found in information_schema.columns for table %s, column %s", td.Name, columnName)
			}
			for _, pk := range pks {
				if columnName == pk {
					isPK = true
				}
			}
			extra := strings.ToLower(row[5].ToString())
			if strings.Contains(extra, "stored generated") || strings.Contains(extra, "virtual generated") {
				isGenerated = true
			}
			colInfo = append(colInfo, &vreplication.ColumnInfo{
				Name:        columnName,
				CharSet:     charSet,
				Collation:   collation,
				DataType:    dataType,
				ColumnType:  columnType,
				IsPK:        isPK,
				IsGenerated: isGenerated,
			})
		}
		colInfoMap[td.Name] = colInfo
	}
	return colInfoMap, nil
}

func encodeString(in string) string {
	var buf strings.Builder
	sqltypes.NewVarChar(in).EncodeSQL(&buf)
	return buf.String()
}

// GetSchema returns the schema for database for tables listed in
// tables. If tables is empty, return the schema for all tables.
func GetSchema(ctx context.Context, dbName string, tables, excludeTables []string, includeViews bool, dbPool *dbconnpool.ConnectionPool) (*tabletmanagerdatapb.SchemaDefinition, error) {
	sd := &tabletmanagerdatapb.SchemaDefinition{}
	// we don't know dbname, because connecting through vtgate
	// sd.DatabaseSchema = sqlescape.EscapeID(dbName)

	tds, err := collectBasicTableData(ctx, dbName, tables, excludeTables, includeViews, dbPool)
	if err != nil {
		log.Error("collectBasicTableData")
		return nil, err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	allErrors := &concurrency.AllErrorRecorder{}

	// Get per-table schema concurrently.
	tableNames := make([]string, 0, len(tds))
	for _, td := range tds {
		tableNames = append(tableNames, td.Name)
		wg.Add(1)
		go func(td *tabletmanagerdatapb.TableDefinition) {
			defer wg.Done()

			fields, columns, schema, err := collectSchema(ctx, dbName, td.Name, td.Type, dbPool)
			if err != nil {
				allErrors.RecordError(err)
				cancel()
				return
			}

			td.Fields = fields
			td.Columns = columns
			td.Schema = schema
		}(td)
	}

	// Get primary columns concurrently.
	colMap := map[string][]string{}
	if len(tableNames) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()

			var err error
			colMap, err = getPrimaryKeyColumns(ctx, dbName, dbPool, tableNames...)
			if err != nil {
				allErrors.RecordError(err)
				cancel()
				return
			}
		}()
	}

	wg.Wait()
	if err := allErrors.AggrError(vterrors.Aggregate); err != nil {
		return nil, err
	}

	for _, td := range tds {
		td.PrimaryKeyColumns = colMap[td.Name]
	}

	sd.TableDefinitions = tds

	tmutils.GenerateSchemaVersion(sd)
	return sd, nil
}

func getPrimaryKeyColumns(ctx context.Context, dbName string, dbPool *dbconnpool.ConnectionPool, tables ...string) (map[string][]string, error) {
	conn, err := getPoolReconnect(ctx, dbPool)
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()

	tableList, err := tableListSQL(tables)
	if err != nil {
		return nil, err
	}
	// sql uses column name aliases to guarantee lower case sensitivity.
	// we don't know dbname, because connecting through vtgate
	// sql := fmt.Sprintf(`
	// 	SELECT
	// 		table_name AS table_name,
	// 		ordinal_position AS ordinal_position,
	// 		column_name AS column_name
	// 	FROM information_schema.key_column_usage
	// 	WHERE table_schema = '%s'
	// 		AND table_name IN %s
	// 		AND constraint_name='PRIMARY'
	// 	ORDER BY table_name, ordinal_position`, dbName, tableList)
	sql := fmt.Sprintf(`
		SELECT
			table_name AS table_name,
			ordinal_position AS ordinal_position,
			column_name AS column_name
		FROM information_schema.key_column_usage
		WHERE table_schema like 'vt_%%'
			AND table_name IN %s
			AND constraint_name='PRIMARY'
		ORDER BY table_name, ordinal_position`, tableList)
	qr, err := conn.ExecuteFetch(sql, len(tables)*100, true)
	if err != nil {
		return nil, err
	}

	named := qr.Named()
	colMap := map[string][]string{}
	for _, row := range named.Rows {
		tableName := row.AsString("table_name", "")
		colMap[tableName] = append(colMap[tableName], row.AsString("column_name", ""))
	}
	return colMap, err
}

func encodeTableName(tableName string) string {
	var buf strings.Builder
	sqltypes.NewVarChar(tableName).EncodeSQL(&buf)
	return buf.String()
}

// tableListSQL returns an IN clause "('t1', 't2'...) for a list of tables."
func tableListSQL(tables []string) (string, error) {
	if len(tables) == 0 {
		return "", vterrors.New(vtrpc.Code_INTERNAL, "no tables for tableListSQL")
	}

	encodedTables := make([]string, len(tables))
	for i, tableName := range tables {
		encodedTables[i] = encodeTableName(tableName)
	}

	return "(" + strings.Join(encodedTables, ", ") + ")", nil
}

// tableDefinitions is a sortable collection of table definitions
type tableDefinitions []*tabletmanagerdatapb.TableDefinition

func (t tableDefinitions) Len() int {
	return len(t)
}

func (t tableDefinitions) Less(i, j int) bool {
	return t[i].Name < t[j].Name
}

func (t tableDefinitions) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

var _ sort.Interface = (tableDefinitions)(nil)

func collectBasicTableData(ctx context.Context, dbName string, tables, excludeTables []string, includeViews bool, dbPool *dbconnpool.ConnectionPool) ([]*tabletmanagerdatapb.TableDefinition, error) {
	conn, err := getPoolReconnect(ctx, dbPool)
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()
	// get the list of tables we're interested in
	// sql := "SELECT table_name, table_type, data_length, table_rows FROM information_schema.tables WHERE table_schema = '" + dbName + "'"
	// we don't know dbname when connecting to vtgate
	sql := "SELECT table_name, table_type, data_length, table_rows FROM information_schema.tables WHERE table_schema like 'vt_%%'"
	if !includeViews {
		sql += " AND table_type = '" + tmutils.TableBaseTable + "'"
	}
	qr, err := conn.ExecuteFetch(sql, 10000, true)
	if err != nil {
		return nil, err
	}
	if len(qr.Rows) == 0 {
		return nil, nil
	}

	filter, err := tmutils.NewTableFilter(tables, excludeTables, includeViews)
	if err != nil {
		return nil, err
	}

	tds := make(tableDefinitions, 0, len(qr.Rows))
	for _, row := range qr.Rows {
		tableName := row[0].ToString()
		tableType := row[1].ToString()

		if !filter.Includes(tableName, tableType) {
			continue
		}

		// compute dataLength
		var dataLength uint64
		if !row[2].IsNull() {
			// dataLength is NULL for views, then we use 0
			dataLength, err = evalengine.ToUint64(row[2])
			if err != nil {
				return nil, err
			}
		}

		// get row count
		var rowCount uint64
		if !row[3].IsNull() {
			rowCount, err = evalengine.ToUint64(row[3])
			if err != nil {
				return nil, err
			}
		}

		tds = append(tds, &tabletmanagerdatapb.TableDefinition{
			Name:       tableName,
			Type:       tableType,
			DataLength: dataLength,
			RowCount:   rowCount,
		})
	}

	sort.Sort(tds)

	return tds, nil
}

func collectSchema(ctx context.Context, dbName, tableName, tableType string, dbPool *dbconnpool.ConnectionPool) ([]*querypb.Field, []string, string, error) {
	fields, columns, err := GetColumns(ctx, dbName, tableName, dbPool)
	if err != nil {
		return nil, nil, "", err
	}

	schema, err := normalizedSchema(ctx, dbName, tableName, tableType, dbPool)
	if err != nil {
		return nil, nil, "", err
	}

	return fields, columns, schema, nil
}

// GetColumns returns the columns of table.
func GetColumns(ctx context.Context, dbName, table string, dbPool *dbconnpool.ConnectionPool) ([]*querypb.Field, []string, error) {
	conn, err := getPoolReconnect(ctx, dbPool)
	if err != nil {
		return nil, nil, err
	}
	defer conn.Recycle()

	// we don't know dbname, because connecting through vtgate
	// qr, err := conn.ExecuteFetch(fmt.Sprintf("SELECT * FROM %s.%s WHERE 1=0", sqlescape.EscapeID(dbName), sqlescape.EscapeID(table)), 0, true)
	qr, err := conn.ExecuteFetch(fmt.Sprintf("SELECT * FROM %s WHERE 1=0", sqlescape.EscapeID(table)), 0, true)
	if err != nil {
		return nil, nil, err
	}

	columns := make([]string, len(qr.Fields))
	for i, field := range qr.Fields {
		columns[i] = field.Name
	}
	return qr.Fields, columns, nil

}

var autoIncr = regexp.MustCompile(` AUTO_INCREMENT=\d+`)

// normalizedSchema returns a table schema with database names replaced, and auto_increment annotations removed.
func normalizedSchema(ctx context.Context, dbName, tableName, tableType string, dbPool *dbconnpool.ConnectionPool) (string, error) {
	conn, err := getPoolReconnect(ctx, dbPool)
	if err != nil {
		return "", err
	}
	defer conn.Recycle()

	backtickDBName := sqlescape.EscapeID(dbName)
	// we don't know dbname, because connecting through vtgate
	// qr, fetchErr := conn.ExecuteFetch(fmt.Sprintf("SHOW CREATE TABLE %s.%s", backtickDBName, sqlescape.EscapeID(tableName)), 10000, true)
	qr, fetchErr := conn.ExecuteFetch(fmt.Sprintf("SHOW CREATE TABLE %s", sqlescape.EscapeID(tableName)), 10000, true)
	if fetchErr != nil {
		return "", fetchErr
	}
	if len(qr.Rows) == 0 {
		return "", fmt.Errorf("empty create table statement for %v", tableName)
	}

	// FIXME(DeathBorn) Does not support views actually
	// Normalize & remove auto_increment because it changes on every insert
	// FIXME(alainjobart) find a way to share this with
	// vt/tabletserver/table_info.go:162
	norm := qr.Rows[0][1].ToString()
	norm = autoIncr.ReplaceAllLiteralString(norm, "")
	if tableType == tmutils.TableView {
		// Views will have the dbname in there, replace it
		// with {{.DatabaseName}}
		norm = strings.Replace(norm, backtickDBName, "{{.DatabaseName}}", -1)
	}

	return norm, nil
}
