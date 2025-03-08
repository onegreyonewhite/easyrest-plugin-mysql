// mysql_plugin.go
package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	hplugin "github.com/hashicorp/go-plugin"
	easyrest "github.com/onegreyonewhite/easyrest/plugin"
)

var Version = "v0.1.1"

// rowScanner interface defines the methods we need from a rows object.
type rowScanner interface {
	Columns() ([]string, error)
	Next() bool
	Scan(dest ...interface{}) error
	Err() error
}

// RoutineParam holds one parameter definition.
type RoutineParam struct {
	Name     string // name of the parameter
	DataType string // e.g. INT, VARCHAR, etc.
	Mode     string // IN, OUT, INOUT
	Ordinal  int    // position in routine (0 for function return)
}

// RoutineInfo holds all parameter definitions for a routine.
type RoutineInfo struct {
	Name       string
	Params     []RoutineParam // only IN and INOUT parameters (ordered by ORDINAL_POSITION, excluding ordinal 0)
	ReturnType string         // if non-empty, the function's return type (from ordinal 0)
}

// mysqlPlugin implements the easyrest.DBPlugin interface for MySQL.
type mysqlPlugin struct {
	db       *sql.DB                // Connection pool for MySQL
	routines map[string]RoutineInfo // Map of routine name to its info
}

var openDB = sql.Open

// injectContext always sets session variables for the entire incoming context.
func (m *mysqlPlugin) injectContext(conn *sql.Conn, ctx map[string]interface{}) error {
	if ctx == nil {
		return nil
	}
	// Wrap the incoming context in a map under "erctx".
	wrappedCtx := map[string]interface{}{"erctx": ctx}
	flatCtx, err := easyrest.FormatToContext(wrappedCtx)
	if err != nil {
		return fmt.Errorf("failed to format context: %w", err)
	}
	if len(flatCtx) == 0 {
		return nil
	}
	var parts []string
	var args []interface{}
	// Build a SET command that assigns each flattened value to a session variable.
	for k, v := range flatCtx {
		parts = append(parts, fmt.Sprintf("@%s = ?", k))
		args = append(args, v)
	}
	setQuery := "SET " + strings.Join(parts, ", ")
	_, err = conn.ExecContext(context.Background(), setQuery, args...)
	if err != nil {
		return fmt.Errorf("failed to execute SET command: %w", err)
	}
	return nil
}

// scanRows converts a rowScanner into a slice of map[string]interface{}.
func scanRows(r rowScanner) ([]map[string]interface{}, error) {
	cols, err := r.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}
	numCols := len(cols)
	var results []map[string]interface{}
	for r.Next() {
		columns := make([]interface{}, numCols)
		columnPointers := make([]interface{}, numCols)
		for i := range columns {
			columnPointers[i] = &columns[i]
		}
		if err := r.Scan(columnPointers...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		rowMap := make(map[string]interface{}, numCols)
		for i, colName := range cols {
			// Leave column names as-is.
			if t, ok := columns[i].(time.Time); ok {
				if t.Hour() == 0 && t.Minute() == 0 && t.Second() == 0 && t.Nanosecond() == 0 {
					rowMap[colName] = t.Format("2006-01-02")
				} else {
					rowMap[colName] = t.Format("2006-01-02 15:04:05")
				}
			} else if b, ok := columns[i].([]byte); ok {
				rowMap[colName] = string(b)
			} else {
				rowMap[colName] = columns[i]
			}
		}
		results = append(results, rowMap)
	}
	if err := r.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}
	return results, nil
}

// InitConnection initializes the MySQL connection pool using a URI.
// Expected format: mysql://username:password@tcp(host:port)/dbname?params
// Also loads all routine metadata from information_schema.
func (m *mysqlPlugin) InitConnection(uri string) error {
	if !strings.HasPrefix(uri, "mysql://") {
		return errors.New("invalid MySQL URI")
	}
	dsn := strings.TrimPrefix(uri, "mysql://")
	db, err := openDB("mysql", dsn)
	if err != nil {
		return fmt.Errorf("failed to open MySQL connection: %w", err)
	}
	db.SetMaxOpenConns(50)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(5 * time.Minute)
	if err := db.Ping(); err != nil {
		return fmt.Errorf("failed to ping MySQL: %w", err)
	}
	m.db = db
	_, err = m.db.Exec("SET NAMES utf8mb4 COLLATE utf8mb4_general_ci")
	if err != nil {
		log.Fatal("Failed to set charset:", err)
	}
	// Load routine metadata.
	if err := m.loadRoutines(); err != nil {
		return fmt.Errorf("failed to load routines: %w", err)
	}
	return nil
}

// loadRoutines queries information_schema.parameters to load all routines in the current database.
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
		var specificName, parameterName, dataType, parameterMode sql.NullString
		var ordinal sql.NullInt64
		if err := rows.Scan(&specificName, &parameterName, &dataType, &parameterMode, &ordinal); err != nil {
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
		// If ordinal is 0, it's a function return value.
		if op == 0 {
			rInfo.ReturnType = dataType.String
		} else {
			// Append only IN or INOUT parameters (treat empty mode as IN).
			if parameterMode.String == "IN" || parameterMode.String == "INOUT" || parameterMode.String == "" {
				rInfo.Params = append(rInfo.Params, RoutineParam{
					Name:     parameterName.String,
					DataType: dataType.String,
					Mode:     parameterMode.String,
					Ordinal:  op,
				})
			}
		}
		m.routines[rName] = rInfo
	}
	return nil
}

// CallFunction calls a stored procedure (or function) with the provided data.
// It validates that all required arguments are provided and replaces any value that starts with "erctx."
// with the corresponding session variable reference. It also injects the entire context as session variables.
func (m *mysqlPlugin) CallFunction(userID, funcName string, data map[string]interface{}, ctx map[string]interface{}) (interface{}, error) {
	rInfo, ok := m.routines[funcName]
	if !ok {
		return nil, fmt.Errorf("routine %s not found", funcName)
	}
	// Order parameters (only those with ORDINAL_POSITION > 0)
	sort.Slice(rInfo.Params, func(i, j int) bool {
		return rInfo.Params[i].Ordinal < rInfo.Params[j].Ordinal
	})
	var placeholders []string
	var callArgs []interface{}
	for _, param := range rInfo.Params {
		var found bool
		var val interface{}
		for k, v := range data {
			if strings.EqualFold(k, param.Name) {
				found = true
				val = v
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("missing required argument: %s", param.Name)
		}
		if s, ok := val.(string); ok && strings.HasPrefix(s, "erctx.") {
			placeholders = append(placeholders, "@"+strings.Replace(s, "erctx.", "erctx_", 1))
		} else {
			placeholders = append(placeholders, "?")
			callArgs = append(callArgs, val)
		}
	}
	// Check for extra arguments.
	for k := range data {
		var found bool
		for _, param := range rInfo.Params {
			if strings.EqualFold(param.Name, k) {
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
		// For functions, use SELECT syntax.
		callQuery = fmt.Sprintf("SELECT %s(%s) AS result", funcName, strings.Join(placeholders, ", "))
	} else {
		callQuery = fmt.Sprintf("CALL %s(%s)", funcName, strings.Join(placeholders, ", "))
	}
	conn, err := m.db.Conn(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to get DB connection: %w", err)
	}
	defer conn.Close()
	if ctx != nil {
		if err := m.injectContext(conn, ctx); err != nil {
			return nil, err
		}
	}
	tx, err := conn.BeginTx(context.Background(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
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
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}
	return result, nil
}

// TableGet performs a SELECT query on the given table.
// It always injects the entire context as session variables.
func (m *mysqlPlugin) TableGet(userID, table string, selectFields []string, where map[string]interface{},
	ordering []string, groupBy []string, limit, offset int, ctx map[string]interface{}) ([]map[string]interface{}, error) {

	fields := "*"
	if len(selectFields) > 0 {
		fields = strings.Join(selectFields, ", ")
	}
	query := fmt.Sprintf("SELECT %s FROM %s", fields, table)
	whereClause, args, err := easyrest.BuildWhereClause(where)
	if err != nil {
		return nil, fmt.Errorf("failed to build WHERE clause: %w", err)
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
	if ctx != nil {
		conn, err := m.db.Conn(context.Background())
		if err != nil {
			return nil, fmt.Errorf("failed to get DB connection: %w", err)
		}
		defer conn.Close()
		if err := m.injectContext(conn, ctx); err != nil {
			return nil, err
		}
		query = strings.ReplaceAll(query, "erctx.", "@erctx_")
		rows, err := conn.QueryContext(context.Background(), query, args...)
		if err != nil {
			return nil, fmt.Errorf("failed to execute query: %w", err)
		}
		defer rows.Close()
		return scanRows(rows)
	}
	rows, err := m.db.QueryContext(context.Background(), query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()
	return scanRows(rows)
}

// TableCreate performs an INSERT operation on the specified table.
// It always injects the entire context as session variables and replaces any value starting with "erctx." with the corresponding session variable reference.
func (m *mysqlPlugin) TableCreate(userID, table string, data []map[string]interface{}, ctx map[string]interface{}) ([]map[string]interface{}, error) {
	conn, err := m.db.Conn(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to get DB connection: %w", err)
	}
	defer conn.Close()
	tx, err := conn.BeginTx(context.Background(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	if ctx != nil {
		if err := m.injectContext(conn, ctx); err != nil {
			tx.Rollback()
			return nil, err
		}
	}
	var results []map[string]interface{}
	for _, row := range data {
		var cols, placeholders []string
		var args []interface{}
		var keys []string
		for k := range row {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			cols = append(cols, k)
			if s, ok := row[k].(string); ok && strings.HasPrefix(s, "erctx.") {
				placeholders = append(placeholders, "@"+strings.Replace(s, "erctx.", "erctx_", 1))
			} else {
				placeholders = append(placeholders, "?")
				args = append(args, row[k])
			}
		}
		baseQuery := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, strings.Join(cols, ", "), strings.Join(placeholders, ", "))
		baseQuery = strings.ReplaceAll(baseQuery, "erctx.", "@erctx_")
		if _, err := tx.ExecContext(context.Background(), baseQuery, args...); err != nil {
			tx.Rollback()
			return nil, fmt.Errorf("failed to execute insert: %w", err)
		}
		results = append(results, row)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}
	return results, nil
}

// TableUpdate performs an UPDATE operation on the specified table.
// It always injects the entire context as session variables and replaces any value starting with "erctx." with the corresponding session variable reference.
func (m *mysqlPlugin) TableUpdate(userID, table string, data map[string]interface{}, where map[string]interface{}, ctx map[string]interface{}) (int, error) {
	conn, err := m.db.Conn(context.Background())
	if err != nil {
		return 0, fmt.Errorf("failed to get DB connection: %w", err)
	}
	defer conn.Close()
	tx, err := conn.BeginTx(context.Background(), nil)
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var setParts []string
	var args []interface{}
	for _, k := range keys {
		if s, ok := data[k].(string); ok && strings.HasPrefix(s, "erctx.") {
			setParts = append(setParts, fmt.Sprintf("%s = @%s", k, strings.Replace(s, "erctx.", "erctx_", 1)))
		} else {
			setParts = append(setParts, fmt.Sprintf("%s = ?", k))
			args = append(args, data[k])
		}
	}
	baseQuery := fmt.Sprintf("UPDATE %s SET %s", table, strings.Join(setParts, ", "))
	whereClause, whereArgs, err := easyrest.BuildWhereClause(where)
	if err != nil {
		tx.Rollback()
		return 0, fmt.Errorf("failed to build WHERE clause: %w", err)
	}
	baseQuery += whereClause
	args = append(args, whereArgs...)
	if ctx != nil {
		if err := m.injectContext(conn, ctx); err != nil {
			tx.Rollback()
			return 0, err
		}
		baseQuery = strings.ReplaceAll(baseQuery, "erctx.", "@erctx_")
	}
	res, err := tx.ExecContext(context.Background(), baseQuery, args...)
	if err != nil {
		tx.Rollback()
		return 0, fmt.Errorf("failed to execute update: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		tx.Rollback()
		return 0, fmt.Errorf("failed to retrieve affected rows: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("failed to commit transaction: %w", err)
	}
	return int(affected), nil
}

// TableDelete performs a DELETE operation on the specified table.
// It always injects the entire context as session variables.
func (m *mysqlPlugin) TableDelete(userID, table string, where map[string]interface{}, ctx map[string]interface{}) (int, error) {
	conn, err := m.db.Conn(context.Background())
	if err != nil {
		return 0, fmt.Errorf("failed to get DB connection: %w", err)
	}
	defer conn.Close()
	tx, err := conn.BeginTx(context.Background(), nil)
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	whereClause, whereArgs, err := easyrest.BuildWhereClause(where)
	if err != nil {
		tx.Rollback()
		return 0, fmt.Errorf("failed to build WHERE clause: %w", err)
	}
	baseQuery := fmt.Sprintf("DELETE FROM %s%s", table, whereClause)
	args := whereArgs
	if ctx != nil {
		if err := m.injectContext(conn, ctx); err != nil {
			tx.Rollback()
			return 0, err
		}
		baseQuery = strings.ReplaceAll(baseQuery, "erctx.", "@erctx_")
	}
	res, err := tx.ExecContext(context.Background(), baseQuery, args...)
	if err != nil {
		tx.Rollback()
		return 0, fmt.Errorf("failed to execute delete: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		tx.Rollback()
		return 0, fmt.Errorf("failed to retrieve affected rows: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("failed to commit transaction: %w", err)
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
