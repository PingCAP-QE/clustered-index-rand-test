package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/zyguan/sqlz"

	. "github.com/zyguan/just"
)

const (
	TestPending = "Pending"
	TestRunning = "Running"
	TestFailed  = "Failed"
	TestPassed  = "Passed"
	TestUnknown = "Unknown"
)

type Result struct {
	Raw          []byte
	Err          error
	RowsAffected int64
	LastInsertId int64
}

type Store interface {
	Init() error
	Clear() error
	AddTest(test Test) error
	NextPendingTest() (*Test, error)
	SetTest(id string, status string, message string) error
	PutStmtResult(id string, seq int, tag string, result Result) error
}

type Test struct {
	ID         string
	Status     string
	Message    string
	StartedAt  int64
	FinishedAt int64
	InitSQL    []string
	Steps      []Txn
}

type Txn []Stmt

type Stmt struct {
	Seq     int
	Txn     int
	TestID  string
	Stmt    string
	IsQuery bool
}

func NewStore(dsn string) (Store, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	return &store{db: sqlz.WrapDB(context.Background(), db)}, nil
}

type store struct {
	db *sqlz.DB
}

func (s *store) Init() error { return initDB(s.db) }

func (s *store) Clear() error { return clearDB(s.db) }

func (s *store) AddTest(test Test) (err error) {
	defer Return(&err)
	if len(test.ID) == 0 {
		test.ID = uuid.New().String()
	}

	tx := Try(s.db.Begin()).(*sql.Tx)
	defer tx.Rollback()

	seq := 0
	for i, txn := range test.Steps {
		for _, stmt := range txn {
			Try(tx.Exec("insert into stmt (test_id, seq, txn, stmt, is_query) values (?, ?, ?, ?, ?)",
				test.ID, seq, i, stmt.Stmt, naiveQueryDetect(stmt.Stmt)))
			seq += 1
		}
	}
	init := Try(json.Marshal(test.InitSQL)).([]byte)
	Try(tx.Exec("insert into test (id, init_sql, status) values (?, ?, ?)", test.ID, string(init), TestPending))

	return tx.Commit()
}

func (s *store) NextPendingTest() (test *Test, err error) {
	defer Return(&err)
	var (
		t = Test{
			Status:    TestPending,
			StartedAt: time.Now().Unix(),
		}
		buf   []byte
		stmts []Stmt
	)

	t.StartedAt = time.Now().Unix()

	tx := Try(s.db.Begin()).(*sql.Tx)
	defer tx.Rollback()

	Try(tx.QueryRow("select id, init_sql from test where status = ? limit 1 for update", t.Status).Scan(&t.ID, &buf))
	Try(tx.Exec("update test set status = ?, started_at = ? where id = ?", TestRunning, t.StartedAt, t.ID))
	Try(json.Unmarshal(buf, &t.InitSQL))

	rows := Try(tx.Query("select txn, seq, stmt, is_query from stmt where test_id = ? order by txn, seq", t.ID)).(*sql.Rows)
	defer rows.Close()
	for rows.Next() {
		var stmt Stmt
		Try(rows.Scan(&stmt.Txn, &stmt.Seq, &stmt.Stmt, &stmt.IsQuery))
		stmt.TestID = t.ID
		stmts = append(stmts, stmt)
	}
	Try(rows.Err())
	Try(tx.Commit())

	for i := 0; i < len(stmts); {
		j := i + 1
		for ; j < len(stmts); j++ {
			if stmts[i].Txn != stmts[j].Txn {
				break
			}
		}
		t.Steps = append(t.Steps, stmts[i:j])
		i = j
	}

	return &t, nil
}

func (s *store) SetTest(id string, status string, message string) error {
	_, err := s.db.Exec("update test set status = ?, message = ?, finished_at = ? where id = ?",
		status, message, time.Now().Unix(), id)
	return err
}

func (s *store) PutStmtResult(id string, seq int, tag string, result Result) error {
	errmsg := ""
	if result.Err != nil {
		errmsg = result.Err.Error()
	}
	_, err := s.db.Exec("insert into stmt_result (test_id, seq, tag, errmsg, result, rows_affected, last_insert_id, created_at) values (?, ?, ?, ?, ?, ?, ?, ?)",
		id, seq, tag, errmsg, result.Raw, result.RowsAffected, result.LastInsertId, time.Now().Unix())
	return err
}

func initDB(db *sqlz.DB) (err error) {
	defer Return(&err)

	db.MustExec(`create table test (
    id char(36) not null,
    init_sql longtext not null,
    status varchar(20),
    started_at bigint default 0,
    finished_at bigint default 0,
    message text,
    primary key (id),
    key (status)
)`)
	db.MustExec(`create table stmt (
    test_id char(36) not null,
    seq int not null,
    txn int not null,
    stmt text not null,
    is_query bool,
    primary key (test_id, seq)
)`)
	db.MustExec(`create table stmt_result (
    id bigint not null auto_increment,
    test_id char(36) not null,
    seq int not null,
    tag varchar(255) not null,
    errmsg text,
    result longblob,
    rows_affected int,
    last_insert_id int,
    created_at int not null,
    primary key (id),
    key (test_id, seq)
)`)

	return nil
}

func clearDB(db *sqlz.DB) error {
	_, err := db.Exec("drop table if exists test, stmt, stmt_result")
	return err
}

func naiveQueryDetect(sql string) bool {
	sql = strings.ToLower(strings.TrimSpace(sql))
	for _, w := range []string{"select ", "show ", "admin show "} {
		if strings.HasPrefix(sql, w) {
			return true
		}
	}
	return false
}
