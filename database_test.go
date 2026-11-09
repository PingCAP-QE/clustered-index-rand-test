package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
)

type recordedSQL struct {
	statements []string
	fail       string
}
type recordingConnector struct{ log *recordedSQL }
type recordingConn struct{ log *recordedSQL }
type recordingDriver struct{}

func (connector recordingConnector) Connect(context.Context) (driver.Conn, error) {
	return &recordingConn{connector.log}, nil
}
func (recordingConnector) Driver() driver.Driver         { return recordingDriver{} }
func (recordingDriver) Open(string) (driver.Conn, error) { return nil, errors.New("use connector") }
func (connection *recordingConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepare")
}
func (connection *recordingConn) Begin() (driver.Tx, error) {
	return nil, errors.New("unexpected transaction")
}
func (connection *recordingConn) Close() error { return nil }
func (connection *recordingConn) ExecContext(ctx context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	connection.log.statements = append(connection.log.statements, query)
	if strings.HasPrefix(query, connection.log.fail) && connection.log.fail != "" {
		return nil, errors.New("injected database failure")
	}
	return driver.RowsAffected(0), nil
}

func recordedDatabase(t *testing.T, fail string) (*testDatabase, *recordedSQL) {
	t.Helper()
	recorder := &recordedSQL{fail: fail}
	pool := sql.OpenDB(recordingConnector{recorder})
	conn, err := pool.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	database := &testDatabase{Conn: conn, pool: pool, name: "sqlgen_test_0123456789abcdef0123456789abcdef"}
	t.Cleanup(func() { database.Close() })
	return database, recorder
}

func TestFailedCreateCannotDeleteExistingDatabase(t *testing.T) {
	database, recorder := recordedDatabase(t, "CREATE DATABASE")
	if err := database.prepare(context.Background()); err == nil {
		t.Fatal("accepted database collision")
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	for _, query := range recorder.statements {
		if strings.HasPrefix(query, "DROP") {
			t.Fatalf("deleted unowned schema: %s", query)
		}
	}
}

func TestEverySetupFailureAfterCreateCleansOnlyOwnedDatabase(t *testing.T) {
	for _, failure := range []string{"USE ", "SET SESSION"} {
		t.Run(failure, func(t *testing.T) {
			database, recorder := recordedDatabase(t, failure)
			if err := database.prepare(context.Background()); err == nil {
				t.Fatal("ignored setup failure")
			}
			if err := database.Close(); err != nil {
				t.Fatal(err)
			}
			last := recorder.statements[len(recorder.statements)-1]
			if last != "DROP DATABASE IF EXISTS "+quotedDatabase(database.name) {
				t.Fatalf("wrong cleanup: %s", last)
			}
			count := len(recorder.statements)
			if err := database.Close(); err != nil {
				t.Fatal(err)
			}
			if len(recorder.statements) != count {
				t.Fatal("cleanup executed twice")
			}
		})
	}
}

func TestCancellationDoesNotPreventCleanupOrChangeGlobalSQLMode(t *testing.T) {
	database, recorder := recordedDatabase(t, "")
	ctx, cancel := context.WithCancel(context.Background())
	if err := database.prepare(ctx); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	for _, query := range recorder.statements {
		if strings.Contains(strings.ToLower(query), "set global") || strings.Contains(strings.ToLower(query), "@@global") || strings.Contains(query, "tidb_general_log") {
			t.Fatalf("instance mutation: %s", query)
		}
	}
	if recorder.statements[len(recorder.statements)-1] != "DROP DATABASE IF EXISTS "+quotedDatabase(database.name) {
		t.Fatal("missing cleanup after cancellation")
	}
}

func TestCleanupFailureIsReported(t *testing.T) {
	database, recorder := recordedDatabase(t, "")
	if err := database.prepare(context.Background()); err != nil {
		t.Fatal(err)
	}
	recorder.fail = "DROP DATABASE"
	if err := database.Close(); err == nil {
		t.Fatal("lost cleanup failure")
	}
}

func TestClosedWorkloadConnectionStillCleansOwnedSchema(t *testing.T) {
	database, recorder := recordedDatabase(t, "")
	if err := database.prepare(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := database.Conn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if recorder.statements[len(recorder.statements)-1] != "DROP DATABASE IF EXISTS "+quotedDatabase(database.name) {
		t.Fatal("missing fresh-connection cleanup")
	}
}
