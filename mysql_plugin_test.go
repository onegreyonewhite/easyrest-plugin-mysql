// mysql_plugin_test.go
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// fakeRows implements the rowScanner interface for scanRows testing.
type fakeRows struct {
	cols []string
	data [][]interface{}
	i    int
	err  error
}

func newFakeRows(cols []string, data [][]interface{}) *fakeRows {
	return &fakeRows{cols: cols, data: data, i: 0}
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
		return fmt.Errorf("expected %d columns, got %d", len(row), len(dest))
	}
	for i, v := range row {
		ptr, ok := dest[i].(*interface{})
		if !ok {
			return fmt.Errorf("expected *interface{} at column %d", i)
		}
		*ptr = v
	}
	return nil
}

func (r *fakeRows) Err() error {
	return r.err
}

func newTestPlugin(t *testing.T) (*mysqlPlugin, sqlmock.Sqlmock) {
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatalf("failed to open sqlmock: %v", err)
	}
	openDB = func(driverName, dsn string) (*sql.DB, error) {
		return db, nil
	}
	p := &mysqlPlugin{db: db}
	return p, mock
}

func TestInitConnectionValid(t *testing.T) {
	plugin, mock := newTestPlugin(t)
	defer plugin.db.Close()

	mock.ExpectPing().WillReturnError(nil)
	mock.ExpectExec(regexp.QuoteMeta("SET NAMES utf8mb4 COLLATE utf8mb4_general_ci")).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(regexp.QuoteMeta(`
SELECT SPECIFIC_NAME, PARAMETER_NAME, DATA_TYPE, PARAMETER_MODE, ORDINAL_POSITION
FROM information_schema.parameters
WHERE SPECIFIC_SCHEMA = DATABASE()
ORDER BY SPECIFIC_NAME, ORDINAL_POSITION;`)).
		WillReturnRows(sqlmock.NewRows([]string{"SPECIFIC_NAME", "PARAMETER_NAME", "DATA_TYPE", "PARAMETER_MODE", "ORDINAL_POSITION"}))

	err := plugin.InitConnection("mysql://mock")
	if err != nil {
		t.Fatalf("InitConnection failed: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestInjectContext(t *testing.T) {
	plugin, mock := newTestPlugin(t)
	defer plugin.db.Close()

	conn, err := plugin.db.Conn(context.Background())
	if err != nil {
		t.Fatalf("failed to get db conn: %v", err)
	}
	defer conn.Close()

	ctxData := map[string]interface{}{
		"token":    "secret",
		"timezone": "UTC",
	}

	setQuery := regexp.QuoteMeta("SET @erctx_timezone = ?, @request_timezone = ?, @erctx_token = ?, @request_token = ?, time_zone = ?")
	mock.ExpectExec(setQuery).
		WithArgs("UTC", "UTC", "secret", "secret", "UTC").
		WillReturnResult(sqlmock.NewResult(1, 1))

	err = plugin.injectContext(conn, ctxData)
	if err != nil {
		t.Fatalf("injectContext error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestInjectContextNil(t *testing.T) {
	plugin, _ := newTestPlugin(t)
	defer plugin.db.Close()

	conn, err := plugin.db.Conn(context.Background())
	if err != nil {
		t.Fatalf("failed to get db conn: %v", err)
	}
	defer conn.Close()

	err = plugin.injectContext(conn, nil)
	if err != nil {
		t.Errorf("expected no error on nil context, got %v", err)
	}
}

func TestScanRows(t *testing.T) {
	cols := []string{"id", "name", "created_at"}
	now := time.Date(2025, 3, 7, 15, 30, 0, 0, time.UTC)
	rowsData := [][]interface{}{
		{1, []byte("Alice"), now},
	}
	fr := newFakeRows(cols, rowsData)
	res, err := scanRows(fr)
	if err != nil {
		t.Fatalf("scanRows error: %v", err)
	}
	if len(res) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res))
	}
	if res[0]["name"] != "Alice" {
		t.Errorf("expected name=Alice, got %v", res[0]["name"])
	}
	want := now.Format("2006-01-02 15:04:05")
	if res[0]["created_at"] != want {
		t.Errorf("expected %s, got %v", want, res[0]["created_at"])
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

	err := plugin.loadRoutines()
	if err != nil {
		t.Fatalf("loadRoutines: %v", err)
	}
	info, ok := plugin.routines["doSomething"]
	if !ok {
		t.Fatalf("routine 'doSomething' not found")
	}
	if info.ReturnType != "varchar" {
		t.Errorf("expected return type 'varchar', got %v", info.ReturnType)
	}
	if len(info.Params) != 1 {
		t.Errorf("expected 1 param, got %d", len(info.Params))
	}
	if info.Params[0].Name != "param" {
		t.Errorf("expected param name=param, got %v", info.Params[0].Name)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestTableGetGroupByOrderingLimitOffset(t *testing.T) {
	plugin, mock := newTestPlugin(t)
	defer plugin.db.Close()

	sf := []string{"id", "name"}
	where := map[string]interface{}{}
	gb := []string{"name"}
	ordering := []string{"id DESC"}
	limit := 10
	offset := 5
	expQ := "SELECT id, name FROM users GROUP BY name ORDER BY id DESC LIMIT 10 OFFSET 5"
	mock.ExpectQuery(regexp.QuoteMeta(expQ)).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "John Doe"))

	res, err := plugin.TableGet("u", "users", sf, where, ordering, gb, limit, offset, nil)
	if err != nil {
		t.Fatalf("TableGet error: %v", err)
	}
	if len(res) != 1 {
		t.Errorf("expected 1 row, got %d", len(res))
	}
	if res[0]["name"] != "John Doe" {
		t.Errorf("expected name=John Doe, got %v", res[0]["name"])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestTableGetTimeMidnight(t *testing.T) {
	plugin, mock := newTestPlugin(t)
	defer plugin.db.Close()

	sf := []string{"id", "created_at"}
	where := map[string]interface{}{
		"dummy": "val",
	}
	expQ := "SELECT id, created_at FROM users WHERE dummy = ?"
	mid := time.Date(2025, 3, 7, 0, 0, 0, 0, time.UTC)
	mock.ExpectQuery(regexp.QuoteMeta(expQ)).
		WithArgs("val").
		WillReturnRows(sqlmock.NewRows([]string{"id", "created_at"}).AddRow(1, mid))

	res, err := plugin.TableGet("u", "users", sf, where, nil, nil, 0, 0, nil)
	if err != nil {
		t.Fatalf("TableGet error: %v", err)
	}
	if len(res) != 1 {
		t.Errorf("expected 1 row, got %d", len(res))
	}
	got := res[0]["created_at"]
	if got != "2025-03-07" {
		t.Errorf("expected 2025-03-07, got %v", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestTableCreateWithContext(t *testing.T) {
	plugin, mock := newTestPlugin(t)
	defer plugin.db.Close()

	data := []map[string]interface{}{
		{"id": 1, "info": "info", "name": "erctx.claim"},
	}
	ctxData := map[string]interface{}{
		"claim": "Alice",
		"foo":   123,
	}

	mock.ExpectBegin()

	setQuery := regexp.QuoteMeta("SET @erctx_claim = ?, @request_claim = ?, @erctx_foo = ?, @request_foo = ?")
	mock.ExpectExec(setQuery).
		WithArgs("Alice", "Alice", "123", "123").
		WillReturnResult(sqlmock.NewResult(1, 1))

	expQ := "INSERT INTO users (id, info, name) VALUES (?, ?, ?)"
	mock.ExpectExec(regexp.QuoteMeta(expQ)).
		WithArgs(1, "info", "erctx.claim").
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectCommit()

	res, err := plugin.TableCreate("u", "users", data, ctxData)
	if err != nil {
		t.Fatalf("TableCreate error: %v", err)
	}
	if len(res) != 1 {
		t.Errorf("expected 1 row, got %d", len(res))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestCallFunctionRollbackOnError(t *testing.T) {
	plugin, mock := newTestPlugin(t)
	defer plugin.db.Close()

	plugin.routines = map[string]RoutineInfo{
		"doSomething": {
			Name:       "doSomething",
			ReturnType: "",
			Params: []RoutineParam{
				{Name: "param", DataType: "varchar", Mode: "IN", Ordinal: 1},
			},
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

	_, err := plugin.CallFunction("u", "doSomething", data, nil)
	if err == nil || !strings.Contains(err.Error(), "stored procedure error") {
		t.Fatalf("expected stored procedure error, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestCallFunctionFunction(t *testing.T) {
	plugin, mock := newTestPlugin(t)
	defer plugin.db.Close()

	plugin.routines = map[string]RoutineInfo{
		"fnName": {
			Name:       "fnName",
			ReturnType: "varchar",
			Params: []RoutineParam{
				{Name: "p", DataType: "varchar", Mode: "IN", Ordinal: 1},
			},
		},
	}
	data := map[string]interface{}{
		"p": "hello",
	}
	rows := sqlmock.NewRows([]string{"result"}).AddRow("world")
	mock.ExpectBegin()
	callRegex := regexp.QuoteMeta("SELECT fnName(?) AS result")
	mock.ExpectQuery(callRegex).WithArgs("hello").WillReturnRows(rows)
	mock.ExpectCommit()

	res, err := plugin.CallFunction("x", "fnName", data, nil)
	if err != nil {
		t.Fatalf("CallFunction error: %v", err)
	}
	out, _ := json.Marshal(res)
	if !strings.Contains(string(out), "world") {
		t.Errorf("expected 'world' in output, got %s", string(out))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestGetSchema(t *testing.T) {
	plugin, mock := newTestPlugin(t)
	defer plugin.db.Close()

	// We'll return "users", "orders" (both BASE TABLE), and "v_myview" (VIEW).
	mock.ExpectQuery("SELECT TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = DATABASE()").
		WillReturnRows(sqlmock.NewRows([]string{"TABLE_NAME", "TABLE_TYPE"}).
			AddRow("users", "BASE TABLE").
			AddRow("orders", "BASE TABLE").
			AddRow("v_myview", "VIEW"))

	// columns for 'users'
	mock.ExpectQuery(regexp.QuoteMeta(`
SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, COLUMN_KEY
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
`)).
		WithArgs("users").
		WillReturnRows(sqlmock.NewRows([]string{"COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE", "COLUMN_DEFAULT", "COLUMN_KEY"}).
			AddRow("id", "int", "NO", nil, "PRI").
			AddRow("name", "varchar", "YES", nil, "").
			AddRow("created_at", "timestamp", "NO", nil, ""))

	// columns for 'orders'
	mock.ExpectQuery(regexp.QuoteMeta(`
SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, COLUMN_KEY
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
`)).
		WithArgs("orders").
		WillReturnRows(sqlmock.NewRows([]string{"COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE", "COLUMN_DEFAULT", "COLUMN_KEY"}).
			AddRow("id", "int", "NO", nil, "PRI").
			AddRow("amount", "float", "YES", nil, "").
			AddRow("ts", "datetime", "YES", nil, ""))

	// columns for 'v_myview' (the view)
	mock.ExpectQuery(regexp.QuoteMeta(`
SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, COLUMN_KEY
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
`)).
		WithArgs("v_myview").
		WillReturnRows(sqlmock.NewRows([]string{"COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE", "COLUMN_DEFAULT", "COLUMN_KEY"}).
			AddRow("colA", "int", "NO", nil, "").
			AddRow("colB", "varchar", "YES", nil, ""))

	plugin.routines = map[string]RoutineInfo{} // no routines

	sch, err := plugin.GetSchema(nil)
	if err != nil {
		t.Fatalf("GetSchema error: %v", err)
	}
	js, _ := json.MarshalIndent(sch, "", "  ")
	if !strings.Contains(string(js), `"users"`) {
		t.Errorf("expected 'users' schema, got: %s", js)
	}
	if !strings.Contains(string(js), `"orders"`) {
		t.Errorf("expected 'orders' schema, got: %s", js)
	}
	if !strings.Contains(string(js), `"v_myview"`) {
		t.Errorf("expected 'v_myview' schema, got: %s", js)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations in TestGetSchema: %v", err)
	}
}

func TestCallFunctionProcedure(t *testing.T) {
	plugin, mock := newTestPlugin(t)
	defer plugin.db.Close()

	plugin.routines = map[string]RoutineInfo{
		"doSomething": {
			Name:       "doSomething",
			ReturnType: "",
			Params: []RoutineParam{
				{Name: "p1", DataType: "varchar", Mode: "IN", Ordinal: 1},
			},
		},
	}
	data := map[string]interface{}{
		"p1": "hello",
	}
	mock.ExpectBegin()
	callRegex := regexp.QuoteMeta("CALL doSomething(?)")
	mock.ExpectQuery(callRegex).
		WithArgs("hello").
		WillReturnRows(sqlmock.NewRows([]string{"col"}).AddRow("proc_ok"))
	mock.ExpectCommit()

	res, err := plugin.CallFunction("u", "doSomething", data, nil)
	if err != nil {
		t.Fatalf("CallFunction procedure error: %v", err)
	}
	j, _ := json.Marshal(res)
	if !strings.Contains(string(j), "proc_ok") {
		t.Errorf("expected 'proc_ok' in result, got: %s", j)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

/* NEW TESTS FOR TableUpdate and TableDelete with context injection */

// TestTableUpdateWithContext2 ensures that if the context has multiple keys in random order, they get sorted in the SET query.
func TestTableUpdateWithContext2(t *testing.T) {
	plugin, mock := newTestPlugin(t)
	defer plugin.db.Close()

	// The data to update
	data := map[string]interface{}{
		"note": "SomeNote",
		"qty":  42,
	}
	// The where clause
	where := map[string]interface{}{
		"id": map[string]interface{}{"=": 1},
	}
	// ctx has keys in random order: "xxx", "abc"
	ctxData := map[string]interface{}{
		"xxx": "111",
		"abc": "222",
	}
	mock.ExpectBegin()

	// Because keys are sorted, "abc" < "xxx"
	// So we expect:
	//   SET @erctx_abc = ?, @request_abc = ?, @erctx_xxx = ?, @request_xxx = ?
	setQuery := regexp.QuoteMeta("SET @erctx_abc = ?, @request_abc = ?, @erctx_xxx = ?, @request_xxx = ?")
	mock.ExpectExec(setQuery).
		WithArgs("222", "222", "111", "111").
		WillReturnResult(sqlmock.NewResult(1, 1))

	// Then the update
	upQ := regexp.QuoteMeta("UPDATE items SET note = ?, qty = ? WHERE id = ?")
	mock.ExpectExec(upQ).
		WithArgs("SomeNote", 42, 1).
		WillReturnResult(sqlmock.NewResult(0, 1))

	mock.ExpectCommit()

	affected, err := plugin.TableUpdate("user999", "items", data, where, ctxData)
	if err != nil {
		t.Fatalf("TableUpdate with context error: %v", err)
	}
	if affected != 1 {
		t.Errorf("expected 1 row updated, got %d", affected)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations in TableUpdateWithContext2: %v", err)
	}
}

// TestTableDeleteWithContext2 checks that multiple context keys are sorted and a delete query is run.
func TestTableDeleteWithContext2(t *testing.T) {
	plugin, mock := newTestPlugin(t)
	defer plugin.db.Close()

	where := map[string]interface{}{
		"status": map[string]interface{}{"=": "old"},
	}
	ctxData := map[string]interface{}{
		"ccc": "111",
		"aaa": "222",
	}
	mock.ExpectBegin()

	// sorted => "aaa" then "ccc"
	setQuery := regexp.QuoteMeta("SET @erctx_aaa = ?, @request_aaa = ?, @erctx_ccc = ?, @request_ccc = ?")
	mock.ExpectExec(setQuery).
		WithArgs("222", "222", "111", "111").
		WillReturnResult(sqlmock.NewResult(1, 1))

	delQ := regexp.QuoteMeta("DELETE FROM items WHERE status = ?")
	mock.ExpectExec(delQ).
		WithArgs("old").
		WillReturnResult(sqlmock.NewResult(0, 1))

	mock.ExpectCommit()

	affected, err := plugin.TableDelete("someone", "items", where, ctxData)
	if err != nil {
		t.Fatalf("TableDelete with context error: %v", err)
	}
	if affected != 1 {
		t.Errorf("expected 1 row deleted, got %d", affected)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations in TableDeleteWithContext2: %v", err)
	}
}

/* -- Tests for mysqlCachePlugin -- */

// Helper to create a test cache plugin instance
func newTestCachePlugin(t *testing.T) (*mysqlCachePlugin, *mysqlPlugin, sqlmock.Sqlmock) {
	corePlugin, mock := newTestPlugin(t)
	cachePlugin := &mysqlCachePlugin{dbPluginPointer: corePlugin}
	return cachePlugin, corePlugin, mock
}

func TestCacheInitConnection(t *testing.T) {
	cachePlugin, _, mock := newTestCachePlugin(t)

	// Expect CREATE TABLE IF NOT EXISTS
	createSQL := "CREATE TABLE IF NOT EXISTS easyrest_cache (`key` VARCHAR(255) PRIMARY KEY, value TEXT, expires_at DATETIME)"
	mock.ExpectExec(regexp.QuoteMeta(createSQL)).WillReturnResult(sqlmock.NewResult(0, 0))

	// Call InitConnection (assuming underlying DB init succeeded)
	// We test with autoCleanup=true, but cannot easily verify goroutine start in unit test
	err := cachePlugin.InitConnection("mysql://mock?autoCleanup=true")
	if err != nil {
		t.Fatalf("CacheInitConnection failed: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestCacheInitConnectionTableCreateError(t *testing.T) {
	cachePlugin, _, mock := newTestCachePlugin(t)

	// Expect CREATE TABLE IF NOT EXISTS to fail
	createSQL := "CREATE TABLE IF NOT EXISTS easyrest_cache (`key` VARCHAR(255) PRIMARY KEY, value TEXT, expires_at DATETIME)"
	mock.ExpectExec(regexp.QuoteMeta(createSQL)).WillReturnError(errors.New("table create failed"))

	err := cachePlugin.InitConnection("mysql://mock")
	if err == nil || !strings.Contains(err.Error(), "failed to create cache table") {
		t.Fatalf("Expected table creation error, got: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestCacheInitConnectionNoDB(t *testing.T) {
	cachePlugin, corePlugin, mock := newTestCachePlugin(t)
	corePlugin.db = nil // Start with underlying DB as nil to trigger internal init

	// Expect the internal call to corePlugin.InitConnection to happen and fail (e.g., at ping)
	// Note: openDB mock is part of newTestPlugin setup and returns the mock db
	mock.ExpectPing().WillReturnError(errors.New("mock ping failed"))

	// Call CacheInitConnection, which should internally call corePlugin.InitConnection
	err := cachePlugin.InitConnection("mysql://mock")

	// Assert that the error is the one from the failed underlying initialization
	if err == nil || !strings.Contains(err.Error(), "failed to initialize underlying db connection") || !strings.Contains(err.Error(), "mock ping failed") {
		t.Fatalf("Expected underlying DB init failure error, got: %v", err)
	}

	// Ensure all expected calls (just the ping in this case) were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestCacheSet(t *testing.T) {
	cachePlugin, _, mock := newTestCachePlugin(t)
	key := "mykey"
	value := "myvalue"
	ttl := 5 * time.Minute

	// Expect INSERT ... ON DUPLICATE KEY UPDATE
	setSQL := "INSERT INTO easyrest_cache (`key`, value, expires_at) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE value = VALUES(value), expires_at = VALUES(expires_at)"
	// We don't check exact expiresAt due to potential slight time differences
	mock.ExpectExec(regexp.QuoteMeta(setSQL)).
		WithArgs(key, value, sqlmock.AnyArg()). // Check key and value, ignore exact time
		WillReturnResult(sqlmock.NewResult(1, 1))

	err := cachePlugin.Set(key, value, ttl)
	if err != nil {
		t.Fatalf("CacheSet failed: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestCacheSetDBError(t *testing.T) {
	cachePlugin, _, mock := newTestCachePlugin(t)
	key := "mykey"
	value := "myvalue"
	ttl := 5 * time.Minute

	setSQL := "INSERT INTO easyrest_cache (`key`, value, expires_at) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE value = VALUES(value), expires_at = VALUES(expires_at)"
	mock.ExpectExec(regexp.QuoteMeta(setSQL)).
		WithArgs(key, value, sqlmock.AnyArg()).
		WillReturnError(errors.New("DB write error"))

	err := cachePlugin.Set(key, value, ttl)
	if err == nil || !strings.Contains(err.Error(), "failed to set cache entry") {
		t.Fatalf("Expected DB write error, got: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestCacheSetNoDB(t *testing.T) {
	cachePlugin, corePlugin, _ := newTestCachePlugin(t)
	corePlugin.db = nil // Simulate no DB connection

	err := cachePlugin.Set("key", "value", 1*time.Minute)
	if err == nil || !strings.Contains(err.Error(), "database connection not available") {
		t.Fatalf("Expected DB connection error, got: %v", err)
	}
}

func TestCacheGetHit(t *testing.T) {
	cachePlugin, _, mock := newTestCachePlugin(t)
	key := "existkey"
	expectedValue := "cachedata"

	// Expect SELECT for the key where expires_at > NOW()
	getSQL := "SELECT value FROM easyrest_cache WHERE `key` = ? AND expires_at > NOW()"
	rows := sqlmock.NewRows([]string{"value"}).AddRow(expectedValue)
	mock.ExpectQuery(regexp.QuoteMeta(getSQL)).WithArgs(key).WillReturnRows(rows)

	value, err := cachePlugin.Get(key)
	if err != nil {
		t.Fatalf("CacheGet failed: %v", err)
	}
	if value != expectedValue {
		t.Errorf("Expected value %q, got %q", expectedValue, value)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestCacheGetMiss(t *testing.T) {
	cachePlugin, _, mock := newTestCachePlugin(t)
	key := "nonexistkey"

	// Expect SELECT, return sql.ErrNoRows
	getSQL := "SELECT value FROM easyrest_cache WHERE `key` = ? AND expires_at > NOW()"
	mock.ExpectQuery(regexp.QuoteMeta(getSQL)).WithArgs(key).WillReturnError(sql.ErrNoRows)

	_, err := cachePlugin.Get(key)
	if !errors.Is(err, sql.ErrNoRows) { // Check for the specific error type
		t.Fatalf("Expected sql.ErrNoRows, got: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestCacheGetExpired(t *testing.T) {
	cachePlugin, _, mock := newTestCachePlugin(t)
	key := "expiredkey"

	// The query itself filters expired keys, so the DB returns NoRows
	getSQL := "SELECT value FROM easyrest_cache WHERE `key` = ? AND expires_at > NOW()"
	mock.ExpectQuery(regexp.QuoteMeta(getSQL)).WithArgs(key).WillReturnError(sql.ErrNoRows)

	_, err := cachePlugin.Get(key)
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("Expected sql.ErrNoRows for expired key, got: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestCacheGetDBError(t *testing.T) {
	cachePlugin, _, mock := newTestCachePlugin(t)
	key := "key"

	getSQL := "SELECT value FROM easyrest_cache WHERE `key` = ? AND expires_at > NOW()"
	mock.ExpectQuery(regexp.QuoteMeta(getSQL)).WithArgs(key).WillReturnError(errors.New("DB read error"))

	_, err := cachePlugin.Get(key)
	if err == nil || !strings.Contains(err.Error(), "failed to get cache entry") {
		t.Fatalf("Expected DB read error, got: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestCacheGetNoDB(t *testing.T) {
	cachePlugin, corePlugin, _ := newTestCachePlugin(t)
	corePlugin.db = nil // Simulate no DB connection

	_, err := cachePlugin.Get("key")
	if err == nil || !strings.Contains(err.Error(), "database connection not available") {
		t.Fatalf("Expected DB connection error, got: %v", err)
	}
}
