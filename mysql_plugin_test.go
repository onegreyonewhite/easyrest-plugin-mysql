// mysql_plugin_test.go
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// fakeRows implements the rowScanner interface for testing scanRows.
type fakeRows struct {
	cols []string
	data [][]interface{}
	i    int
	err  error
}

func newFakeRows(cols []string, data [][]interface{}) *fakeRows {
	return &fakeRows{
		cols: cols,
		data: data,
		i:    0,
		err:  nil,
	}
}

func (r *fakeRows) Columns() ([]string, error) {
	return r.cols, nil
}

func (r *fakeRows) Next() bool {
	if r.i < len(r.data) {
		r.i++
		return true
	}
	return false
}

func (r *fakeRows) Scan(dest ...interface{}) error {
	if r.i-1 >= len(r.data) {
		return errors.New("no row to scan")
	}
	row := r.data[r.i-1]
	if len(dest) != len(row) {
		return fmt.Errorf("expected %d destinations, got %d", len(row), len(dest))
	}
	for i, v := range row {
		ptr, ok := dest[i].(*interface{})
		if !ok {
			return fmt.Errorf("expected *interface{} for column %d", i)
		}
		*ptr = v
	}
	return nil
}

func (r *fakeRows) Err() error {
	return r.err
}

// newTestPlugin returns a new mysqlPlugin with a sqlmock database.
func newTestPlugin(t *testing.T) (*mysqlPlugin, sqlmock.Sqlmock) {
	// Create a sqlmock database.
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatalf("failed to open sqlmock database: %s", err)
	}
	// Override openDB to return our mocked db.
	openDB = func(driverName, dataSourceName string) (*sql.DB, error) {
		return db, nil
	}
	plugin := &mysqlPlugin{db: db}
	return plugin, mock
}

func TestInitConnectionValid(t *testing.T) {
	plugin, mock := newTestPlugin(t)
	defer plugin.db.Close()

	// Expect a successful Ping.
	mock.ExpectPing().WillReturnError(nil)
	mock.ExpectExec(regexp.QuoteMeta("SET NAMES utf8mb4 COLLATE utf8mb4_general_ci")).
		WillReturnResult(sqlmock.NewResult(1, 1))
	// Simulate empty routines.
	mock.ExpectQuery(regexp.QuoteMeta(`
SELECT SPECIFIC_NAME, PARAMETER_NAME, DATA_TYPE, PARAMETER_MODE, ORDINAL_POSITION
FROM information_schema.parameters
WHERE SPECIFIC_SCHEMA = DATABASE()
ORDER BY SPECIFIC_NAME, ORDINAL_POSITION;`)).
		WillReturnRows(sqlmock.NewRows([]string{"SPECIFIC_NAME", "PARAMETER_NAME", "DATA_TYPE", "PARAMETER_MODE", "ORDINAL_POSITION"}))

	err := plugin.InitConnection("mysql://mock")
	if err != nil {
		t.Fatalf("InitConnection failed: %s", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations in InitConnection: %s", err)
	}
}

func TestInjectContext(t *testing.T) {
	plugin, mock := newTestPlugin(t)
	defer plugin.db.Close()

	conn, err := plugin.db.Conn(context.Background())
	if err != nil {
		t.Fatalf("failed to get connection: %s", err)
	}
	defer conn.Close()

	ctxData := map[string]interface{}{
		"token":    "secret",
		"timezone": "UTC",
	}
	mock.ExpectExec(regexp.QuoteMeta("SET @erctx_token = ?, @erctx_timezone = ?")).
		WithArgs("secret", "UTC").
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := plugin.injectContext(conn, ctxData); err != nil {
		t.Fatalf("injectContext failed: %s", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations in injectContext: %s", err)
	}
}

func TestScanRows(t *testing.T) {
	cols := []string{"id", "name", "created_at"}
	now := time.Date(2025, 3, 7, 15, 30, 0, 0, time.UTC)
	data := [][]interface{}{
		{1, []byte("Alice"), now},
	}
	fr := newFakeRows(cols, data)
	result, err := scanRows(fr)
	if err != nil {
		t.Fatalf("scanRows error: %s", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result))
	}
	if result[0]["name"] != "Alice" {
		t.Errorf("expected 'Alice', got %v", result[0]["name"])
	}
	expectedTime := now.Format("2006-01-02 15:04:05")
	if result[0]["created_at"] != expectedTime {
		t.Errorf("expected %s, got %v", expectedTime, result[0]["created_at"])
	}
}

func TestLoadRoutinesSuccess(t *testing.T) {
	plugin, mock := newTestPlugin(t)
	defer plugin.db.Close()

	rows := sqlmock.NewRows([]string{"SPECIFIC_NAME", "PARAMETER_NAME", "DATA_TYPE", "PARAMETER_MODE", "ORDINAL_POSITION"}).
		AddRow("doSomething", nil, "varchar", "", 0).
		AddRow("doSomething", "param", "varchar", "IN", 1)
	mock.ExpectQuery(regexp.QuoteMeta(`
SELECT SPECIFIC_NAME, PARAMETER_NAME, DATA_TYPE, PARAMETER_MODE, ORDINAL_POSITION
FROM information_schema.parameters
WHERE SPECIFIC_SCHEMA = DATABASE()
ORDER BY SPECIFIC_NAME, ORDINAL_POSITION;`)).
		WillReturnRows(rows)

	if err := plugin.loadRoutines(); err != nil {
		t.Fatalf("loadRoutines failed: %s", err)
	}
	r, ok := plugin.routines["doSomething"]
	if !ok {
		t.Fatalf("routine doSomething not loaded")
	}
	if r.ReturnType != "varchar" {
		t.Errorf("expected return type 'varchar', got %s", r.ReturnType)
	}
	if len(r.Params) != 1 {
		t.Errorf("expected 1 parameter, got %d", len(r.Params))
	}
	if r.Params[0].Name != "param" {
		t.Errorf("expected parameter name 'param', got %s", r.Params[0].Name)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations in loadRoutines: %s", err)
	}
}

// TestTableGetGroupByOrderingLimitOffset verifies that TableGet builds the query correctly.
func TestTableGetGroupByOrderingLimitOffset(t *testing.T) {
	plugin, mock := newTestPlugin(t)
	defer plugin.db.Close()

	// Set selectFields and empty where.
	selectFields := []string{"id", "name"}
	where := map[string]interface{}{}
	groupBy := []string{"name"}
	ordering := []string{"id DESC"}
	limit := 10
	offset := 5

	// easyrest.BuildWhereClause returns empty string if where is empty.
	// So expected query is:
	// SELECT id, name FROM users GROUP BY name ORDER BY id DESC LIMIT 10 OFFSET 5
	expectedQuery := "SELECT id, name FROM users GROUP BY name ORDER BY id DESC LIMIT 10 OFFSET 5"
	mock.ExpectQuery(regexp.QuoteMeta(expectedQuery)).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "John Doe"))

	result, err := plugin.TableGet("user123", "users", selectFields, where, ordering, groupBy, limit, offset, nil)
	if err != nil {
		t.Fatalf("TableGet error: %s", err)
	}
	if len(result) != 1 {
		t.Errorf("expected 1 row, got %d", len(result))
	}
	if result[0]["name"] != "John Doe" {
		t.Errorf("expected name 'John Doe', got %v", result[0]["name"])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations in TestTableGetGroupByOrderingLimitOffset: %s", err)
	}
}

// TestInjectContextNil verifies that injectContext returns nil when context is nil.
func TestInjectContextNil(t *testing.T) {
	plugin, _ := newTestPlugin(t)
	defer plugin.db.Close()

	conn, err := plugin.db.Conn(context.Background())
	if err != nil {
		t.Fatalf("failed to get connection: %s", err)
	}
	defer conn.Close()

	err = plugin.injectContext(conn, nil)
	if err != nil {
		t.Errorf("expected nil error when ctx is nil, got: %s", err)
	}
}

// TestTableGetTimeMidnight tests that a time value at midnight is formatted as "YYYY-MM-DD".
func TestTableGetTimeMidnight(t *testing.T) {
	plugin, mock := newTestPlugin(t)
	defer plugin.db.Close()

	selectFields := []string{"id", "created_at"}
	where := map[string]interface{}{"dummy": "dummy"}
	// Simulate easyrest.BuildWhereClause returns " WHERE dummy = ?" with arg "dummy".
	// For testing purposes we ignore where values; our expected query:
	expectedQuery := "SELECT id, created_at FROM users WHERE dummy = ?"
	mr := sqlmock.NewRows([]string{"id", "created_at"}).AddRow(1, time.Date(2025, 3, 7, 0, 0, 0, 0, time.UTC))
	mock.ExpectQuery(regexp.QuoteMeta(expectedQuery)).WithArgs("dummy").WillReturnRows(mr)

	result, err := plugin.TableGet("user123", "users", selectFields, where, nil, nil, 0, 0, nil)
	if err != nil {
		t.Fatalf("TableGet error: %s", err)
	}
	if len(result) != 1 {
		t.Errorf("expected 1 row, got %d", len(result))
	}
	// Since the time is midnight, it should be formatted as "2006-01-02"
	if result[0]["created_at"] != "2025-03-07" {
		t.Errorf("expected time '2025-03-07', got %v", result[0]["created_at"])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations in TestTableGetTimeMidnight: %s", err)
	}
}

// TestTableCreateWithContext verifies that TableCreate works when ctx is not nil.
func TestTableCreateWithContext(t *testing.T) {
	plugin, mock := newTestPlugin(t)
	defer plugin.db.Close()

	data := []map[string]interface{}{
		{"id": 1, "name": "erctx.claim", "info": "info"},
	}
	ctxData := map[string]interface{}{
		"claim": "Alice",
	}
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("SET @erctx_claim = ?")).
		WithArgs("Alice").WillReturnResult(sqlmock.NewResult(1, 1))
	// Sorted keys: "id", "info", "name". For "name", since value starts with "erctx.claim",
	// it will be replaced with "@erctx_claim".
	expectedQuery := "INSERT INTO users (id, info, name) VALUES (?, ?, @erctx_claim)"
	mock.ExpectExec(regexp.QuoteMeta(expectedQuery)).
		WithArgs(1, "info").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	result, err := plugin.TableCreate("user123", "users", data, ctxData)
	if err != nil {
		t.Fatalf("TableCreate with context error: %s", err)
	}
	if len(result) != 1 {
		t.Errorf("expected 1 inserted row, got %d", len(result))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations in TestTableCreateWithContext: %s", err)
	}
}

func TestCallFunctionFunction(t *testing.T) {
	plugin, mock := newTestPlugin(t)
	defer plugin.db.Close()

	plugin.routines = map[string]RoutineInfo{
		"doSomething": {
			Name:       "doSomething",
			Params:     []RoutineParam{{Name: "param", DataType: "varchar", Mode: "IN", Ordinal: 1}},
			ReturnType: "varchar",
		},
	}

	data := map[string]interface{}{
		"param": "value",
	}
	rows := sqlmock.NewRows([]string{"result"}).AddRow("success")
	mock.ExpectBegin()
	callRegex := regexp.QuoteMeta("SELECT doSomething(?) AS result")
	mock.ExpectQuery(callRegex).WithArgs("value").WillReturnRows(rows)
	mock.ExpectCommit()

	result, err := plugin.CallFunction("user123", "doSomething", data, nil)
	if err != nil {
		t.Fatalf("CallFunction function error: %s", err)
	}
	out, _ := json.Marshal(result)
	var res []map[string]interface{}
	if err := json.Unmarshal(out, &res); err != nil {
		t.Fatalf("failed to unmarshal result: %s", err)
	}
	if len(res) != 1 || res[0]["result"] != "success" {
		t.Errorf("expected result 'success', got %v", res)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations in CallFunctionFunction: %s", err)
	}
}

func TestCallFunctionProcedure(t *testing.T) {
	plugin, mock := newTestPlugin(t)
	defer plugin.db.Close()

	plugin.routines = map[string]RoutineInfo{
		"doSomething": {
			Name:       "doSomething",
			Params:     []RoutineParam{{Name: "param", DataType: "varchar", Mode: "IN", Ordinal: 1}},
			ReturnType: "",
		},
	}

	data := map[string]interface{}{
		"param": "erctx.myparam",
	}
	ctxData := map[string]interface{}{
		"myparam": "value",
	}
	mock.ExpectExec(regexp.QuoteMeta("SET @erctx_myparam = ?")).
		WithArgs("value").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectBegin()
	callRegex := regexp.QuoteMeta("CALL doSomething(@erctx_myparam)")
	mock.ExpectQuery(callRegex).WillReturnRows(sqlmock.NewRows([]string{"result"}).AddRow("proc_success"))
	mock.ExpectCommit()

	result, err := plugin.CallFunction("user123", "doSomething", data, ctxData)
	if err != nil {
		t.Fatalf("CallFunction procedure error: %s", err)
	}
	out, _ := json.Marshal(result)
	var res []map[string]interface{}
	if err := json.Unmarshal(out, &res); err != nil {
		t.Fatalf("failed to unmarshal result: %s", err)
	}
	if len(res) != 1 || res[0]["result"] != "proc_success" {
		t.Errorf("expected result 'proc_success', got %v", res)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations in CallFunctionProcedure: %s", err)
	}
}

func TestTableGetNoContext(t *testing.T) {
	plugin, mock := newTestPlugin(t)
	defer plugin.db.Close()

	selectFields := []string{"id", "name"}
	where := map[string]interface{}{
		"status": map[string]interface{}{"=": "active"},
	}
	queryRegex := regexp.QuoteMeta("SELECT id, name FROM users WHERE status = ?")
	rows := sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "John Doe")
	mock.ExpectQuery(queryRegex).WithArgs("active").WillReturnRows(rows)

	result, err := plugin.TableGet("user123", "users", selectFields, where, nil, nil, 0, 0, nil)
	if err != nil {
		t.Fatalf("TableGet no context error: %s", err)
	}
	if len(result) != 1 {
		t.Errorf("expected 1 row, got %d", len(result))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations in TableGetNoContext: %s", err)
	}
}

func TestTableGetWithContext(t *testing.T) {
	plugin, mock := newTestPlugin(t)
	defer plugin.db.Close()

	selectFields := []string{"id", "name", "erctx.extra"}
	where := map[string]interface{}{
		"status": map[string]interface{}{"=": "active"},
	}
	ctxData := map[string]interface{}{
		"extra": "data",
	}
	mock.ExpectExec(regexp.QuoteMeta("SET @erctx_extra = ?")).
		WithArgs("data").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT id, name, @erctx_extra FROM users WHERE status = ?")).
		WithArgs("active").WillReturnRows(sqlmock.NewRows([]string{"id", "name", "erctx_extra"}).AddRow(1, "John Doe", "data"))

	result, err := plugin.TableGet("user123", "users", selectFields, where, nil, nil, 0, 0, ctxData)
	if err != nil {
		t.Fatalf("TableGet with context error: %s", err)
	}
	if len(result) != 1 {
		t.Errorf("expected 1 row, got %d", len(result))
	}
	if result[0]["erctx_extra"] != "data" {
		t.Errorf("expected value 'data', got %v", result[0]["erctx_extra"])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations in TableGetWithContext: %s", err)
	}
}

func TestTableCreateNoContext(t *testing.T) {
	plugin, mock := newTestPlugin(t)
	defer plugin.db.Close()

	data := []map[string]interface{}{
		{"id": 1, "name": "Alice", "info": "some info"},
	}

	mock.ExpectBegin()
	expectedQuery := "INSERT INTO users (id, info, name) VALUES (?, ?, ?)"
	mock.ExpectExec(regexp.QuoteMeta(expectedQuery)).
		WithArgs(1, "some info", "Alice").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	result, err := plugin.TableCreate("user123", "users", data, nil)
	if err != nil {
		t.Fatalf("TableCreate no context error: %s", err)
	}
	if len(result) != 1 {
		t.Errorf("expected 1 inserted row, got %d", len(result))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations in TableCreateNoContext: %s", err)
	}
}

func TestTableUpdateNoContext(t *testing.T) {
	plugin, mock := newTestPlugin(t)
	defer plugin.db.Close()

	data := map[string]interface{}{
		"name": "Bob",
	}
	where := map[string]interface{}{
		"id": map[string]interface{}{"=": 1},
	}
	mock.ExpectBegin()
	expectedQuery := "UPDATE users SET name = ? WHERE id = ?"
	mock.ExpectExec(regexp.QuoteMeta(expectedQuery)).
		WithArgs("Bob", 1).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	affected, err := plugin.TableUpdate("user123", "users", data, where, nil)
	if err != nil {
		t.Fatalf("TableUpdate no context error: %s", err)
	}
	if affected != 1 {
		t.Errorf("expected 1 affected row, got %d", affected)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations in TableUpdateNoContext: %s", err)
	}
}

func TestTableDeleteNoContext(t *testing.T) {
	plugin, mock := newTestPlugin(t)
	defer plugin.db.Close()

	where := map[string]interface{}{
		"status": map[string]interface{}{"=": "inactive"},
	}
	mock.ExpectBegin()
	expectedQuery := "DELETE FROM users WHERE status = ?"
	mock.ExpectExec(regexp.QuoteMeta(expectedQuery)).
		WithArgs("inactive").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	affected, err := plugin.TableDelete("user123", "users", where, nil)
	if err != nil {
		t.Fatalf("TableDelete no context error: %s", err)
	}
	if affected != 1 {
		t.Errorf("expected 1 affected row, got %d", affected)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations in TableDeleteNoContext: %s", err)
	}
}

func TestTableUpdateWithContextInjection(t *testing.T) {
	plugin, mock := newTestPlugin(t)
	defer plugin.db.Close()

	data := map[string]interface{}{
		"name": "Bob",
		"note": "erctx.update_note",
	}
	where := map[string]interface{}{
		"id": map[string]interface{}{"=": 1},
	}
	ctxData := map[string]interface{}{
		"session": "xyz789",
	}
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("SET @erctx_session = ?")).
		WithArgs("xyz789").WillReturnResult(sqlmock.NewResult(1, 1))
	expectedQuery := "UPDATE users SET name = ?, note = @erctx_update_note WHERE id = ?"
	mock.ExpectExec(regexp.QuoteMeta(expectedQuery)).
		WithArgs("Bob", 1).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	affected, err := plugin.TableUpdate("user123", "users", data, where, ctxData)
	if err != nil {
		t.Fatalf("TableUpdate with context error: %s", err)
	}
	if affected != 1 {
		t.Errorf("expected 1 affected row, got %d", affected)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations in TableUpdateWithContext: %s", err)
	}
}

func TestTableDeleteWithContextInjection(t *testing.T) {
	plugin, mock := newTestPlugin(t)
	defer plugin.db.Close()

	where := map[string]interface{}{
		"status": map[string]interface{}{"=": "inactive erctx.remove"},
	}
	ctxData := map[string]interface{}{
		"session": "delete_session",
	}
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("SET @erctx_session = ?")).
		WithArgs("delete_session").WillReturnResult(sqlmock.NewResult(1, 1))
	expectedQuery := "DELETE FROM users WHERE status = ?"
	mock.ExpectExec(regexp.QuoteMeta(expectedQuery)).
		WithArgs("inactive erctx.remove").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	affected, err := plugin.TableDelete("user123", "users", where, ctxData)
	if err != nil {
		t.Fatalf("TableDelete with context error: %s", err)
	}
	if affected != 1 {
		t.Errorf("expected 1 affected row, got %d", affected)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations in TableDeleteWithContext: %s", err)
	}
}

func TestCallFunctionRollbackOnError(t *testing.T) {
	plugin, mock := newTestPlugin(t)
	defer plugin.db.Close()

	plugin.routines = map[string]RoutineInfo{
		"doSomething": {
			Name:       "doSomething",
			Params:     []RoutineParam{{Name: "param", DataType: "varchar", Mode: "IN", Ordinal: 1}},
			ReturnType: "",
		},
	}

	data := map[string]interface{}{
		"param": "value",
	}
	mock.ExpectBegin()
	callRegex := regexp.QuoteMeta("CALL doSomething(?)")
	mock.ExpectQuery(callRegex).WithArgs("value").
		WillReturnError(errors.New("stored procedure error"))
	mock.ExpectRollback()

	_, err := plugin.CallFunction("user123", "doSomething", data, nil)
	if err == nil || !regexp.MustCompile("stored procedure error").MatchString(err.Error()) {
		t.Fatalf("expected stored procedure error, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations in CallFunctionRollbackOnError: %s", err)
	}
}
