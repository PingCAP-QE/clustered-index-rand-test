package main

import (
	"bytes"
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/tangenta/clustered-index-rand-test/sqlgen"
	"log"
	"math/rand"
	"sort"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/zyguan/sqlz"
	"github.com/zyguan/sqlz/resultset"

	. "github.com/zyguan/just"
)

type runABTestOptions struct {
	Threads      int
	QueryTimeout int
	Continue     func() bool

	Tag1 string
	Tag2 string
	DB1  *sql.DB
	DB2  *sql.DB

	Store Store
}

func runABTest(ctx context.Context, failed chan struct{}, opts runABTestOptions) error {
	store := opts.Store

	if opts.Continue == nil {
		opts.Continue = func() bool { return true }
	}
	for opts.Continue() {
		select {
		case <-failed:
			return nil
		case <-ctx.Done():
			return nil
		default:
		}

		t, err := store.NextPendingTest()
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				log.Printf("no more test")
				select {
				case <-failed:
					return nil
				case <-ctx.Done():
					return nil
				case <-time.After(time.Duration(rand.Intn(10*opts.Threads)) * time.Second):
					continue
				}
			}
			return err
		}

		// using different db names allows us run test on same db instance
		dbName1 := "db1__" + strings.ReplaceAll(t.ID, "-", "_")
		dbName2 := "db2__" + strings.ReplaceAll(t.ID, "-", "_")
		isInitialized := strings.HasSuffix(opts.Tag1, "#") && strings.HasSuffix(opts.Tag2, "#")
		if !isInitialized {
			if !strings.HasSuffix(opts.Tag1, "#") {
				v1, err := selectVersion(opts.DB1)
				if err != nil {
					return err
				}
				opts.Tag1 = fmt.Sprintf("%s(%s)#", opts.Tag1, v1)
			}
			if !strings.HasSuffix(opts.Tag2, "#") {
				v2, err := selectVersion(opts.DB2)
				if err != nil {
					return err
				}
				opts.Tag2 = fmt.Sprintf("%s(%s)#", opts.Tag2, v2)
			}
		}

		conn1, err1 := initTest(ctx, opts.DB1, dbName1, t)
		if err1 != nil {
			store.SetTest(t.ID, TestFailed, "init db1: "+err1.Error())
			log.Printf("failed to init %s/%s: %v", opts.Tag1, dbName1, err1)
			continue
		}
		conn2, err2 := initTest(ctx, opts.DB2, dbName2, t)
		if err2 != nil {
			conn1.Close()
			store.SetTest(t.ID, TestFailed, "init db2: "+err2.Error())
			log.Printf("failed to init %s/%s: %v", opts.Tag2, dbName2, err2)
			continue
		}
		closeConns := func() {
			conn1.Close()
			conn2.Close()
		}

		log.Printf("run test %s", t.ID)
		for i := range t.Steps {
			tx1, err1 := conn1.BeginTx(ctx, nil)
			if err1 != nil {
				closeConns()
				return fmt.Errorf("start txn #%d of test(%s) on %s: %v", i, t.ID, opts.Tag1, err1)
			}
			tx2, err2 := conn2.BeginTx(ctx, nil)
			if err2 != nil {
				tx1.Rollback()
				closeConns()
				return fmt.Errorf("start txn #%d of test(%s) on %s: %v", i, t.ID, opts.Tag2, err2)
			}

			fail := func(err error) error {
				defer func() { recover() }()
				tx1.Rollback()
				tx2.Rollback()
				closeConns()
				store.SetTest(t.ID, TestFailed, err.Error())
				log.Printf("test(%s) failed at txn #%d: %v", t.ID, i, err)
				close(failed)
				return err
			}

			if err := doTxn(ctx, opts, t, i, tx1, tx2); err != nil {
				return fail(err)
			}

			err1, err2 = tx1.Commit(), tx2.Commit()
			if !sqlgen.ValidateErrs(err1, err2) {
				return fail(fmt.Errorf("commit txn #%d: %v <> %v", i, err1, err2))
			}

			hs1, err1 := checkTables(ctx, opts.DB1, dbName1)
			if err1 != nil {
				return fail(fmt.Errorf("check table of %s after txn #%d: %v", opts.Tag1, i, err1))
			}
			hs2, err2 := checkTables(ctx, opts.DB1, dbName2)
			if err2 != nil {
				return fail(fmt.Errorf("check table of %s after txn #%d: %v", opts.Tag2, i, err2))
			}
			for t := range hs2 {
				if hs1[t] != hs2[t] {
					return fail(fmt.Errorf("data mismatch after txn #%d: %s != %s @%s", i, hs1[t], hs2[t], t))
				}
			}
		}

		store.SetTest(t.ID, TestPassed, "")

		conn1.ExecContext(ctx, "drop database if exists "+dbName1)
		conn2.ExecContext(ctx, "drop database if exists "+dbName2)
		closeConns()
	}
	return nil
}

func selectVersion(db *sql.DB) (string, error) {
	var versionInfo string
	const query = "SELECT version()"
	row := db.QueryRow(query)
	err := row.Scan(&versionInfo)
	if err != nil {
		return "", fmt.Errorf("sql: %s, err: %s", query, err.Error())
	}
	return versionInfo, nil
}

func initTest(ctx context.Context, db *sql.DB, name string, t *Test) (conn *sql.Conn, err error) {
	defer Return(&err)
	conn = Try(sqlz.Connect(ctx, db)).(*sql.Conn)
	Try(conn.ExecContext(ctx, "create database "+name))
	Try(conn.ExecContext(ctx, "use "+name))
	for _, stmt := range t.InitSQL {
		// it's ok for some of stmts in init_sql failed
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			log.Printf("init stmt failed: %v @ %s", err, stmt)
		}
	}
	return
}

func checkTables(ctx context.Context, db *sql.DB, name string) (map[string]string, error) {
	rows, err := db.QueryContext(ctx, "select table_name from information_schema.tables where table_schema = ?", name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	hs := make(map[string]string)
	for rows.Next() {
		var t string
		if err = rows.Scan(&t); err != nil {
			return nil, err
		}
		if hs[t], err = checkTable(ctx, db, fmt.Sprintf("`%s`.`%s`", name, t)); err != nil {
			return nil, err
		}
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return hs, nil
}

func checkTable(ctx context.Context, db *sql.DB, name string) (string, error) {
	_, err := db.ExecContext(ctx, "admin check table "+name)
	if err != nil {
		if e, ok := err.(*mysql.MySQLError); !ok || e.Number != 1064 {
			return "", err
		}
	}
	rows, err := db.QueryContext(ctx, "select * from "+name)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	rs, err := resultset.ReadFromRows(rows)
	if err != nil {
		return "", err
	}
	return unorderedDigest(rs, func(col resultset.ColumnDef) bool {
		return col.Type != "JSON"
	}), nil
}

func doTxn(ctx context.Context, opts runABTestOptions, t *Test, i int, tx1 *sql.Tx, tx2 *sql.Tx) error {
	txn := t.Steps[i]

	record := func(seq int, tag string, rs *resultset.ResultSet, err error) {
		if rs != nil {
			raw, _ := rs.Encode()
			opts.Store.PutStmtResult(t.ID, seq, tag, Result{
				Raw:          raw,
				Err:          err,
				RowsAffected: rs.ExecResult().RowsAffected,
				LastInsertId: rs.ExecResult().LastInsertId,
			})
		} else {
			opts.Store.PutStmtResult(t.ID, seq, tag, Result{Err: err})
		}
	}

	for _, stmt := range txn {
		ctx1, _ := context.WithTimeout(ctx, time.Duration(opts.QueryTimeout)*time.Second)
		rs1, err1 := doStmt(ctx1, tx1, stmt)
		record(stmt.Seq, opts.Tag1, rs1, err1)
		ctx2, _ := context.WithTimeout(ctx, time.Duration(opts.QueryTimeout)*time.Second)
		rs2, err2 := doStmt(ctx2, tx2, stmt)
		record(stmt.Seq, opts.Tag2, rs2, err2)
		if !sqlgen.ValidateErrs(err1, err2) {
			return fmt.Errorf("errors mismatch: %v <> %v @(%s,%d) %q", err1, err2, t.ID, stmt.Seq, stmt.Stmt)
		}
		if rs1 == nil || rs2 == nil {
			log.Printf("skip query error: [%v] [%v] @(%s,%d)", err1, err2, t.ID, stmt.Seq)
			continue
		}
		h1, h2 := "", ""
		if q := strings.ToLower(stmt.Stmt); stmt.IsQuery && rs1.NRows() == rs2.NRows() && rs1.NRows() > 1 &&
			(!strings.Contains(q, "order by") || strings.Contains(q, "force-unordered")) {
			h1, h2 = unorderedDigest(rs1, nil), unorderedDigest(rs2, nil)
		} else {
			h1, h2 = rs1.DataDigest(resultset.DigestOptions{}), rs2.DataDigest(resultset.DigestOptions{})
		}
		if h1 != h2 {
			return fmt.Errorf("result digests mismatch: %s != %s @(%s,%d) %q", h1, h2, t.ID, stmt.Seq, stmt.Stmt)
		}
		if rs1.IsExecResult() && rs1.ExecResult().RowsAffected != rs2.ExecResult().RowsAffected {
			return fmt.Errorf("rows affected mismatch: %d != %d @(%s,%d) %q",
				rs1.ExecResult().RowsAffected, rs2.ExecResult().RowsAffected, t.ID, stmt.Seq, stmt.Stmt)
		}
	}
	return nil
}

type rows [][]byte

func (r rows) Len() int { return len(r) }

func (r rows) Less(i, j int) bool { return bytes.Compare(r[i], r[j]) < 0 }

func (r rows) Swap(i, j int) { r[i], r[j] = r[j], r[i] }

func unorderedDigest(rs *resultset.ResultSet, colFilter func(resultset.ColumnDef) bool) string {
	if colFilter == nil {
		colFilter = func(_ resultset.ColumnDef) bool { return true }
	}
	cols := make([]int, 0, rs.NCols())
	for i := 0; i < rs.NCols(); i++ {
		if colFilter(rs.ColumnDef(i)) {
			cols = append(cols, i)
		}
	}
	digests := make(rows, rs.NRows())
	for i := 0; i < rs.NRows(); i++ {
		h := sha1.New()
		for _, j := range cols {
			raw, _ := rs.RawValue(i, j)
			h.Write(raw)
		}
		digests[i] = h.Sum(nil)
	}
	sort.Sort(digests)
	h := sha1.New()
	for _, digest := range digests {
		h.Write(digest)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func doStmt(ctx context.Context, tx *sql.Tx, stmt Stmt) (*resultset.ResultSet, error) {
	if stmt.IsQuery {
		rows, err := tx.QueryContext(ctx, stmt.Stmt)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		return resultset.ReadFromRows(rows)
	} else {
		res, err := tx.ExecContext(ctx, stmt.Stmt)
		if err != nil {
			return nil, err
		}
		return resultset.NewFromResult(res), nil
	}
}
