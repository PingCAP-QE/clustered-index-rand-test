package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/go-sql-driver/mysql"
)

type testDatabase struct {
	Conn    *sql.Conn
	pool    *sql.DB
	name    string
	created bool
}

func quotedDatabase(name string) string { return "`" + name + "`" } // generated from a fixed prefix and random hex

func openTestDatabase(ctx context.Context, policy *targetPolicy, dsn string) (_ *testDatabase, err error) {
	cfg, err := policy.authorize(dsn)
	if err != nil {
		return nil, err
	}
	name, err := newTestDatabaseName()
	if err != nil {
		return nil, err
	}
	host, _, _ := net.SplitHostPort(cfg.Addr)
	tlsConfig, err := policy.tlsConfig(host)
	if err != nil {
		return nil, err
	}
	if err := mysql.RegisterTLSConfig(name, tlsConfig); err != nil {
		return nil, err
	}
	cfg.TLSConfig = name
	connector, err := mysql.NewConnector(cfg)
	mysql.DeregisterTLSConfig(name)
	if err != nil {
		return nil, fmt.Errorf("cannot configure verified database connection")
	}
	database := &testDatabase{pool: sql.OpenDB(connector), name: name}
	// Every early return after allocation has exactly the same cleanup as normal exit.
	defer func() {
		if err != nil {
			err = combineCleanupError(err, database.Close())
		}
	}()
	database.Conn, err = database.pool.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("cannot connect to approved test target")
	}
	if err = database.prepare(ctx); err != nil {
		return nil, err
	}
	return database, nil
}

func (database *testDatabase) prepare(ctx context.Context) error {
	// Never drop or reuse an existing database. A collision is an error and does
	// not confer ownership or permission to delete the colliding schema.
	if _, err := database.Conn.ExecContext(ctx, "CREATE DATABASE "+quotedDatabase(database.name)); err != nil {
		return err
	}
	database.created = true
	if _, err := database.Conn.ExecContext(ctx, "USE "+quotedDatabase(database.name)); err != nil {
		return err
	}
	_, err := database.Conn.ExecContext(ctx, "SET SESSION sql_mode=(SELECT REPLACE(@@SESSION.sql_mode,'ONLY_FULL_GROUP_BY',''))")
	return err
}

func (database *testDatabase) Close() error {
	var cleanupErr error
	if database.Conn != nil {
		if database.created {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			query := "DROP DATABASE IF EXISTS " + quotedDatabase(database.name)
			_, cleanupErr = database.Conn.ExecContext(ctx, query)
			if (errors.Is(cleanupErr, sql.ErrConnDone) || errors.Is(cleanupErr, driver.ErrBadConn)) && database.pool != nil {
				// Query cancellation can discard the workload connection. A new
				// connection from this same verified pool may clean up our schema.
				_, cleanupErr = database.pool.ExecContext(ctx, query)
			}
			cancel()
			database.created = false
		}
		closeErr := database.Conn.Close()
		if !errors.Is(closeErr, sql.ErrConnDone) {
			cleanupErr = combineCleanupError(cleanupErr, closeErr)
		}
		database.Conn = nil
	}
	if database.pool != nil {
		cleanupErr = combineCleanupError(cleanupErr, database.pool.Close())
		database.pool = nil
	}
	return cleanupErr
}

func combineCleanupError(primary, cleanup error) error {
	if cleanup == nil {
		return primary
	}
	if primary == nil {
		return cleanup
	}
	return fmt.Errorf("%v; cleanup: %w", primary, cleanup)
}
