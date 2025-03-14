// mysql_plugin.go
package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	hplugin "github.com/hashicorp/go-plugin"
	easyrest "github.com/onegreyonewhite/easyrest/plugin"
)

var Version = "v0.4.0"

// rowScanner is the interface needed by scanRows to fetch results.
type rowScanner interface {
	Columns() ([]string, error)
	Next() bool
	Scan(dest ...interface{}) error
	Err() error
}

// openDB is a package variable so we can override in tests (to return a sqlmock DB).
var openDB = sql.Open

// RoutineParam holds one parameter definition.
type RoutineParam struct {
	Name     string
	DataType string
	Mode     string
	Ordinal  int
}

// RoutineInfo holds parameter definitions for a routine.
type RoutineInfo struct {
	Name       string
	Params     []RoutineParam
	ReturnType string
}

// mysqlPlugin implements easyrest.DBPlugin for MySQL.
type mysqlPlugin struct {
	db             *sql.DB
	routines       map[string]RoutineInfo
	defaultTimeout time.Duration
}

// injectContext sets *all* keys from ctx under two prefixes: erctx_ and request_.
func (m *mysqlPlugin) injectContext(conn *sql.Conn, ctx map[string]interface{}) error {
	if ctx == nil {
		return nil
	}
	flatCtx, err := easyrest.FormatToContext(ctx)
	if err != nil {
		return fmt.Errorf("failed to format context: %w", err)
	}
	if len(flatCtx) == 0 {
		return nil
	}

	// Sort the flattened keys to guarantee a stable, deterministic order.
	var sortedKeys []string
	sortedKeys = make([]string, 0, len(flatCtx))
	for k := range flatCtx {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)

	var builder strings.Builder
	builder.WriteString("SET ")
	var args []interface{}
	args = make([]interface{}, 0, len(flatCtx)*2+1) // +1 for possible timezone

	first := true
	for _, k := range sortedKeys {
		if !first {
			builder.WriteString(", ")
		}
		builder.WriteString("@erctx_")
		builder.WriteString(k)
		builder.WriteString(" = ?, @request_")
		builder.WriteString(k)
		builder.WriteString(" = ?")
		val := flatCtx[k]
		args = append(args, val, val)
		first = false
	}

	if tzRaw, ok := ctx["timezone"]; ok {
		if tzStr, ok2 := tzRaw.(string); ok2 && tzStr != "" {
			if !first {
				builder.WriteString(", ")
			}
			builder.WriteString("time_zone = ?")
			args = append(args, tzStr)
		}
	}

	_, err = conn.ExecContext(context.Background(), builder.String(), args...)
	if err != nil {
		return fmt.Errorf("failed to set session variables: %w", err)
	}
	return nil
}

// scanRows converts row data into []map[string]interface{}.
// Pre-allocates memory for results with a capacity of 100 for better performance.
func scanRows(r rowScanner) ([]map[string]interface{}, error) {
	cols, err := r.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}
	numCols := len(cols)
	results := make([]map[string]interface{}, 0, 100) // Pre-allocate memory for results
	columns := make([]interface{}, numCols)
	pointers := make([]interface{}, numCols)
	for i := range columns {
		pointers[i] = &columns[i]
	}

	for r.Next() {
		if err := r.Scan(pointers...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		rowMap := make(map[string]interface{}, numCols)
		for i, colName := range cols {
			val := columns[i]
			if t, ok := val.(time.Time); ok {
				if t.Hour() == 0 && t.Minute() == 0 && t.Second() == 0 && t.Nanosecond() == 0 {
					rowMap[colName] = t.Format("2006-01-02")
				} else {
					rowMap[colName] = t.Format("2006-01-02 15:04:05")
				}
			} else if b, ok := val.([]byte); ok {
				rowMap[colName] = string(b)
			} else {
				rowMap[colName] = val
			}
		}
		results = append(results, rowMap)
	}
	if err := r.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error: %w", err)
	}
	return results, nil
}

// InitConnection opens the MySQL connection, sets utf8mb4, loads routines.
// Supports the following URI parameters:
// - maxOpenConns: Maximum number of open connections (default: 100)
// - maxIdleConns: Maximum number of idle connections (default: 20)
// - connMaxLifetime: Connection reuse time in minutes (default: 5)
// - connMaxIdleTime: Connection idle time in minutes (default: 10)
// - timeout: Query timeout in seconds (default: 30)
// - parseTime: Parse MySQL TIME/TIMESTAMP/DATETIME as time.Time
func (m *mysqlPlugin) InitConnection(uri string) error {
	if !strings.HasPrefix(uri, "mysql://") {
		return errors.New("invalid MySQL URI")
	}

	parsedURI, err := url.Parse(uri)
	if err != nil {
		return fmt.Errorf("failed to parse URI: %w", err)
	}

	queryParams := parsedURI.Query()
	maxOpenConns := 100
	maxIdleConns := 20
	connMaxLifetime := 5
	connMaxIdleTime := 10
	timeout := 30 // Timeout in seconds

	// Remove connection parameters from query before creating DSN
	if val := queryParams.Get("maxOpenConns"); val != "" {
		if n, err := fmt.Sscanf(val, "%d", &maxOpenConns); err != nil || n != 1 {
			return fmt.Errorf("invalid maxOpenConns value: %s", val)
		}
		queryParams.Del("maxOpenConns")
	}

	if val := queryParams.Get("maxIdleConns"); val != "" {
		if n, err := fmt.Sscanf(val, "%d", &maxIdleConns); err != nil || n != 1 {
			return fmt.Errorf("invalid maxIdleConns value: %s", val)
		}
		queryParams.Del("maxIdleConns")
	}

	if val := queryParams.Get("connMaxLifetime"); val != "" {
		if n, err := fmt.Sscanf(val, "%d", &connMaxLifetime); err != nil || n != 1 {
			return fmt.Errorf("invalid connMaxLifetime value: %s", val)
		}
		queryParams.Del("connMaxLifetime")
	}

	if val := queryParams.Get("connMaxIdleTime"); val != "" {
		if n, err := fmt.Sscanf(val, "%d", &connMaxIdleTime); err != nil || n != 1 {
			return fmt.Errorf("invalid connMaxIdleTime value: %s", val)
		}
		queryParams.Del("connMaxIdleTime")
	}

	if val := queryParams.Get("timeout"); val != "" {
		if n, err := fmt.Sscanf(val, "%d", &timeout); err != nil || n != 1 {
			return fmt.Errorf("invalid timeout value: %s", val)
		}
		queryParams.Del("timeout")
	}

	var dsn strings.Builder

	if parsedURI.User != nil {
		dsn.WriteString(parsedURI.User.Username())
		if password, ok := parsedURI.User.Password(); ok {
			dsn.WriteByte(':')
			dsn.WriteString(password)
		}
		dsn.WriteByte('@')
	}

	host := parsedURI.Hostname()
	port := parsedURI.Port()
	if strings.Contains(host, "(") && strings.Contains(host, ")") {
		dsn.WriteString(host)
	} else {
		dsn.WriteString("tcp(")
		dsn.WriteString(host)
		if port != "" {
			dsn.WriteByte(':')
			dsn.WriteString(port)
		}
		dsn.WriteByte(')')
	}

	if len(parsedURI.Path) > 1 {
		dsn.WriteByte('/')
		dsn.WriteString(strings.TrimPrefix(parsedURI.Path, "/"))
	}

	if len(queryParams) > 0 {
		dsn.WriteByte('?')
		dsn.WriteString(queryParams.Encode())
	}

	db, err := openDB("mysql", dsn.String())
	if err != nil {
		return fmt.Errorf("failed to open MySQL: %w", err)
	}

	db.SetMaxOpenConns(maxOpenConns)
	db.SetMaxIdleConns(maxIdleConns)
	db.SetConnMaxLifetime(time.Duration(connMaxLifetime) * time.Minute)
	db.SetConnMaxIdleTime(time.Duration(connMaxIdleTime) * time.Minute)
	m.defaultTimeout = time.Duration(timeout) * time.Second

	ctx, cancel := context.WithTimeout(context.Background(), m.defaultTimeout)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to ping MySQL: %w", err)
	}

	m.db = db
	_, err = m.db.ExecContext(ctx, "SET NAMES utf8mb4 COLLATE utf8mb4_general_ci")
	if err != nil {
		return fmt.Errorf("failed to set charset: %w", err)
	}

	if err := m.loadRoutines(); err != nil {
		return fmt.Errorf("failed to load routines: %w", err)
	}

	return nil
}

// loadRoutines populates m.routines from information_schema.parameters.
func (m *mysqlPlugin) loadRoutines() error {
	m.routines = make(map[string]RoutineInfo)
	query := `
SELECT SPECIFIC_NAME, PARAMETER_NAME, DATA_TYPE, PARAMETER_MODE, ORDINAL_POSITION
FROM information_schema.parameters
WHERE SPECIFIC_SCHEMA = DATABASE()
ORDER BY SPECIFIC_NAME, ORDINAL_POSITION;`
	rows, err := m.db.Query(query)
	if err != nil {
		return fmt.Errorf("failed to query routines: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var specificName, paramName, dataType, paramMode sql.NullString
		var ordinal sql.NullInt64
		if err := rows.Scan(&specificName, &paramName, &dataType, &paramMode, &ordinal); err != nil {
			return fmt.Errorf("failed to scan routine row: %w", err)
		}
		rName := specificName.String
		rInfo, ok := m.routines[rName]
		if !ok {
			rInfo = RoutineInfo{Name: rName, Params: []RoutineParam{}}
		}
		op := 0
		if ordinal.Valid {
			op = int(ordinal.Int64)
		}
		if op == 0 {
			// function return
			rInfo.ReturnType = dataType.String
		} else {
			mode := paramMode.String
			if mode == "" {
				mode = "IN"
			}
			if mode == "IN" || mode == "INOUT" {
				rInfo.Params = append(rInfo.Params, RoutineParam{
					Name:     paramName.String,
					DataType: dataType.String,
					Mode:     mode,
					Ordinal:  op,
				})
			}
		}
		m.routines[rName] = rInfo
	}
	return nil
}

// GetSchema enumerates tables + views and routines, building a swagger-ish schema.
func (m *mysqlPlugin) GetSchema(ctx map[string]interface{}) (interface{}, error) {
	tables, err := m.getTablesSchema()
	if err != nil {
		return nil, err
	}
	rpc, err := m.getRPCSchema()
	if err != nil {
		return nil, err
	}
	return map[string]interface{}{
		"tables": tables,
		"rpc":    rpc,
	}, nil
}

// getTablesSchema enumerates both tables and views from INFORMATION_SCHEMA.TABLES.
func (m *mysqlPlugin) getTablesSchema() (map[string]interface{}, error) {
	result := make(map[string]interface{})
	rows, err := m.db.Query(`
SELECT TABLE_NAME, TABLE_TYPE 
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = DATABASE()
`)
	if err != nil {
		return nil, fmt.Errorf("failed to list tables/views: %w", err)
	}
	defer rows.Close()

	type tblInfo struct {
		Name string
		Type string // "BASE TABLE" or "VIEW"
	}
	var entries []tblInfo
	for rows.Next() {
		var tname, ttype string
		if err := rows.Scan(&tname, &ttype); err != nil {
			return nil, err
		}
		entries = append(entries, tblInfo{Name: tname, Type: ttype})
	}
	for _, e := range entries {
		sch, err := m.buildTableSchema(e.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to build schema for %s %s: %w", e.Type, e.Name, err)
		}
		result[e.Name] = sch
	}
	return result, nil
}

// buildTableSchema queries COLUMNS for the given table/view name.
func (m *mysqlPlugin) buildTableSchema(tableName string) (map[string]interface{}, error) {
	query := `
SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, COLUMN_KEY
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
`
	rows, err := m.db.Query(query, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	properties := make(map[string]interface{})
	var required []string
	for rows.Next() {
		var colName, dt, nullable, defv, ckey sql.NullString
		if err := rows.Scan(&colName, &dt, &nullable, &defv, &ckey); err != nil {
			return nil, err
		}
		colType := mapMySQLType(dt.String)
		prop := map[string]interface{}{
			"type": colType,
		}
		isPri := (strings.ToUpper(ckey.String) == "PRI")
		if strings.ToUpper(nullable.String) == "YES" {
			prop["x-nullable"] = true
		}
		if isPri {
			// readOnly => not required
			prop["readOnly"] = true
		} else {
			if strings.ToUpper(nullable.String) == "NO" && !defv.Valid {
				required = append(required, colName.String)
			}
		}
		properties[colName.String] = prop
	}
	schema := map[string]interface{}{
		"type":       "object",
		"properties": properties,
	}
	if len(required) > 0 {
		schema["required"] = required
	}
	return schema, nil
}

// mapMySQLType => returns swagger-ish type for the data type.
func mapMySQLType(dt string) string {
	up := strings.ToUpper(dt)
	if strings.Contains(up, "INT") {
		return "integer"
	}
	if strings.Contains(up, "CHAR") || strings.Contains(up, "TEXT") {
		return "string"
	}
	if strings.Contains(up, "BLOB") {
		return "string"
	}
	if strings.Contains(up, "FLOAT") || strings.Contains(up, "DOUBLE") || strings.Contains(up, "DEC") || strings.Contains(up, "REAL") {
		return "number"
	}
	return "string"
}

// getRPCSchema uses m.routines to build routineName => [inSchema, outSchema].
func (m *mysqlPlugin) getRPCSchema() (map[string]interface{}, error) {
	rmap := make(map[string]interface{})
	for name, info := range m.routines {
		inProps := make(map[string]interface{})
		var inReq []string
		for _, param := range info.Params {
			propType := mapMySQLType(param.DataType)
			prop := map[string]interface{}{
				"type": propType,
			}
			inProps[param.Name] = prop
			inReq = append(inReq, param.Name)
		}
		inSchema := map[string]interface{}{
			"type":       "object",
			"properties": inProps,
		}
		if len(inReq) > 0 {
			inSchema["required"] = inReq
		}
		outSchema := map[string]interface{}{
			"type":       "object",
			"properties": map[string]interface{}{},
		}
		if info.ReturnType != "" {
			t := mapMySQLType(info.ReturnType)
			outSchema["properties"] = map[string]interface{}{
				"result": map[string]interface{}{
					"type": t,
				},
			}
		}
		rmap[name] = []interface{}{inSchema, outSchema}
	}
	return rmap, nil
}

// CallFunction executes a stored procedure/function with final parameters.
func (m *mysqlPlugin) CallFunction(userID, funcName string, data map[string]interface{}, ctx map[string]interface{}) (interface{}, error) {
	rInfo, ok := m.routines[funcName]
	if !ok {
		return nil, fmt.Errorf("routine %s not found", funcName)
	}
	sort.Slice(rInfo.Params, func(i, j int) bool {
		return rInfo.Params[i].Ordinal < rInfo.Params[j].Ordinal
	})
	var placeholders []string
	var callArgs []interface{}
	for _, param := range rInfo.Params {
		placeholders = append(placeholders, "?")
		val, found := data[param.Name]
		if !found {
			return nil, fmt.Errorf("missing required argument: %s", param.Name)
		}
		callArgs = append(callArgs, val)
	}
	// check for extra args
	for k := range data {
		found := false
		for _, rp := range rInfo.Params {
			if strings.EqualFold(rp.Name, k) {
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("unexpected argument: %s", k)
		}
	}
	var callQuery string
	if rInfo.ReturnType != "" {
		callQuery = fmt.Sprintf("SELECT %s(%s) AS result", funcName, strings.Join(placeholders, ", "))
	} else {
		callQuery = fmt.Sprintf("CALL %s(%s)", funcName, strings.Join(placeholders, ", "))
	}
	conn, err := m.db.Conn(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to get connection: %w", err)
	}
	defer conn.Close()
	if ctx != nil {
		if err := m.injectContext(conn, ctx); err != nil {
			return nil, err
		}
	}
	tx, err := conn.BeginTx(context.Background(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin tx: %w", err)
	}
	rows, err := tx.QueryContext(context.Background(), callQuery, callArgs...)
	if err != nil {
		tx.Rollback()
		return nil, fmt.Errorf("failed to call routine: %w", err)
	}
	defer rows.Close()
	result, err := scanRows(rows)
	if err != nil {
		tx.Rollback()
		return nil, fmt.Errorf("failed to scan routine result: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit: %w", err)
	}
	return result, nil
}

// TableGet builds and executes a SELECT query.
func (m *mysqlPlugin) TableGet(userID, table string, selectFields []string, where map[string]interface{},
	ordering []string, groupBy []string, limit, offset int, ctx map[string]interface{}) ([]map[string]interface{}, error) {

	fields := "*"
	if len(selectFields) > 0 {
		fields = strings.Join(selectFields, ", ")
	}
	query := fmt.Sprintf("SELECT %s FROM %s", fields, table)
	whereClause, args, err := easyrest.BuildWhereClause(where)
	if err != nil {
		return nil, fmt.Errorf("failed to build WHERE: %w", err)
	}
	query += whereClause
	if len(groupBy) > 0 {
		query += " GROUP BY " + strings.Join(groupBy, ", ")
	}
	if len(ordering) > 0 {
		query += " ORDER BY " + strings.Join(ordering, ", ")
	}
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}
	if offset > 0 {
		query += fmt.Sprintf(" OFFSET %d", offset)
	}
	conn, err := m.db.Conn(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to get connection: %w", err)
	}
	defer conn.Close()
	if ctx != nil {
		if err := m.injectContext(conn, ctx); err != nil {
			return nil, err
		}
	}
	rows, err := conn.QueryContext(context.Background(), query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()
	return scanRows(rows)
}

// TableCreate builds and executes an INSERT statement from data.
func (m *mysqlPlugin) TableCreate(userID, table string, data []map[string]interface{}, ctx map[string]interface{}) ([]map[string]interface{}, error) {
	conn, err := m.db.Conn(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to get connection: %w", err)
	}
	defer conn.Close()
	tx, err := conn.BeginTx(context.Background(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin tx: %w", err)
	}
	if ctx != nil {
		if err := m.injectContext(conn, ctx); err != nil {
			tx.Rollback()
			return nil, err
		}
	}
	var results []map[string]interface{}
	for _, row := range data {
		var cols []string
		var placeholders []string
		var args []interface{}
		var keys []string
		for k := range row {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			cols = append(cols, k)
			placeholders = append(placeholders, "?")
			args = append(args, row[k])
		}
		insertQ := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, strings.Join(cols, ", "), strings.Join(placeholders, ", "))
		if _, err := tx.ExecContext(context.Background(), insertQ, args...); err != nil {
			tx.Rollback()
			return nil, fmt.Errorf("failed to execute insert: %w", err)
		}
		results = append(results, row)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit: %w", err)
	}
	return results, nil
}

// TableUpdate builds and executes an UPDATE statement.
func (m *mysqlPlugin) TableUpdate(userID, table string, data map[string]interface{}, where map[string]interface{}, ctx map[string]interface{}) (int, error) {
	conn, err := m.db.Conn(context.Background())
	if err != nil {
		return 0, fmt.Errorf("failed to get connection: %w", err)
	}
	defer conn.Close()
	tx, err := conn.BeginTx(context.Background(), nil)
	if err != nil {
		return 0, fmt.Errorf("failed to begin tx: %w", err)
	}
	if ctx != nil {
		if err := m.injectContext(conn, ctx); err != nil {
			tx.Rollback()
			return 0, err
		}
	}
	var keys []string
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var setParts []string
	var args []interface{}
	for _, k := range keys {
		setParts = append(setParts, fmt.Sprintf("%s = ?", k))
		args = append(args, data[k])
	}
	updateQ := fmt.Sprintf("UPDATE %s SET %s", table, strings.Join(setParts, ", "))
	whereClause, whereArgs, err := easyrest.BuildWhereClause(where)
	if err != nil {
		tx.Rollback()
		return 0, fmt.Errorf("failed to build WHERE: %w", err)
	}
	updateQ += whereClause
	args = append(args, whereArgs...)
	res, err := tx.ExecContext(context.Background(), updateQ, args...)
	if err != nil {
		tx.Rollback()
		return 0, fmt.Errorf("failed to execute update: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		tx.Rollback()
		return 0, fmt.Errorf("failed to get rowsAffected: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("failed to commit: %w", err)
	}
	return int(affected), nil
}

// TableDelete builds and executes a DELETE statement.
func (m *mysqlPlugin) TableDelete(userID, table string, where map[string]interface{}, ctx map[string]interface{}) (int, error) {
	conn, err := m.db.Conn(context.Background())
	if err != nil {
		return 0, fmt.Errorf("failed to get connection: %w", err)
	}
	defer conn.Close()
	tx, err := conn.BeginTx(context.Background(), nil)
	if err != nil {
		return 0, fmt.Errorf("failed to begin tx: %w", err)
	}
	if ctx != nil {
		if err := m.injectContext(conn, ctx); err != nil {
			tx.Rollback()
			return 0, err
		}
	}
	whereClause, whereArgs, err := easyrest.BuildWhereClause(where)
	if err != nil {
		tx.Rollback()
		return 0, fmt.Errorf("failed to build WHERE: %w", err)
	}
	delQ := fmt.Sprintf("DELETE FROM %s%s", table, whereClause)
	res, err := tx.ExecContext(context.Background(), delQ, whereArgs...)
	if err != nil {
		tx.Rollback()
		return 0, fmt.Errorf("failed to execute delete: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		tx.Rollback()
		return 0, fmt.Errorf("failed to retrieve rowsAffected: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("failed to commit: %w", err)
	}
	return int(affected), nil
}

func main() {
	showVersion := flag.Bool("version", false, "Show version and exit")
	flag.Parse()
	if *showVersion {
		fmt.Println(Version)
		return
	}
	impl := &mysqlPlugin{}
	hplugin.Serve(&hplugin.ServeConfig{
		HandshakeConfig: easyrest.Handshake,
		Plugins: map[string]hplugin.Plugin{
			"db": &easyrest.DBPluginPlugin{Impl: impl},
		},
	})
}
