package main

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"github.com/pkg/errors"
	"math/rand"
	"strings"

	"github.com/PingCAP-QE/clustered-index-rand-test/sqlgen"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
	. "github.com/zyguan/just"
	"github.com/zyguan/sqlz"
	"github.com/zyguan/sqlz/resultset"
)

type globalOption struct {
	stmtCount int
	seed      int64
	debug     bool
}

func rootCmd() *cobra.Command {
	opt := &globalOption{}
	cmd := &cobra.Command{
		Use: "clustered index random abtest",
	}
	cmd.AddCommand(printCmd())
	cmd.AddCommand(abtestCmd())
	cmd.AddCommand(checkSyntaxCmd(opt))
	cmd.Flags().IntVar(&opt.stmtCount, "count", 100, "number of statements to run")
	cmd.Flags().Int64Var(&opt.seed, "seed", 0, "random seed")
	cmd.Flags().BoolVar(&opt.debug, "debug", false, "print generated SQLs")
	return cmd
}

func checkSyntaxCmd(opt *globalOption) *cobra.Command {
	var (
		dsn string
	)
	cmd := &cobra.Command{
		Use:           "check-syntax",
		Short:         "Run syntax check test",
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, args []string) error {
			rand.Seed(opt.seed)
			conn := setUpDatabaseConnection(dsn)

			state := sqlgen.NewState()
			queries := generatePlainSQLs(state, opt.stmtCount)

			for _, query := range queries {
				if opt.debug {
					fmt.Println(query + ";")
				}
				_, err := runQuery(conn, query)
				if err != nil {
					if strings.Contains(err.Error(), "ERROR 1064") {
						fmt.Println("syntax error")
					}
					return err
				}
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&dsn, "dsn", "", "dsn for database")
	return cmd
}

func abtestCmd() *cobra.Command {
	var (
		stmtCount   int
		dsn1        string
		dsn2        string
		sqlFilePath string
		logPath     string
	)
	cmd := &cobra.Command{
		Use:           "abtest",
		Short:         "Run AB test",
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, args []string) error {
			conn1 := setUpDatabaseConnection(dsn1)
			conn2 := setUpDatabaseConnection(dsn2)

			state := sqlgen.NewState()
			queries := generateInitialSQLs(state)
			queries = append(queries, generatePlainSQLs(state, stmtCount)...)

			for _, query := range queries {
				rs1, err1 := runQuery(conn1, query)
				rs2, err2 := runQuery(conn2, query)
				if !sqlgen.ValidateErrs(err1, err2) {
					return errors.Errorf("error mismatch: %v != %v", err1, err2)
				}
				if rs1 == nil || rs2 == nil {
					return nil
				}
				if err := compareResult(rs1, rs2, query); err != nil {
					return err
				}
			}
			return nil
		},
	}
	cmd.Flags().IntVar(&stmtCount, "count", 100, "number of statements to run")
	cmd.Flags().StringVar(&dsn1, "dsn1", "", "dsn for 1st database")
	cmd.Flags().StringVar(&dsn2, "dsn2", "", "dsn for 2nd database")
	cmd.Flags().StringVar(&sqlFilePath, "sqlfile", "rand.sql", "running SQLs")
	cmd.Flags().StringVar(&logPath, "log", "", "The output of 2 databases")
	return cmd
}

func setUpDatabaseConnection(dsn string) *sql.Conn {
	ctx := context.Background()
	db := Try(sql.Open("mysql", dsn)).(*sql.DB)
	dbName := "db" + strings.ReplaceAll(uuid.New().String(), "-", "_")
	conn := Try(sqlz.Connect(ctx, db)).(*sql.Conn)
	Try(conn.ExecContext(ctx, "create database "+dbName))
	Try(conn.ExecContext(ctx, "use "+dbName))
	return conn
}

func runQuery(conn *sql.Conn, query string) (*resultset.ResultSet, error) {
	ctx := context.Background()
	Try(conn.PingContext(ctx))
	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return resultset.ReadFromRows(rows)
}

func generateInitialSQLs(state *sqlgen.State) []string {
	tableCount, columnCount := 5, 5
	indexCount, rowCount := 2, 10
	sqls := make([]string, 0, tableCount+tableCount*rowCount)
	state.SetRepeat(sqlgen.ColumnDefinition, columnCount, columnCount)
	state.SetRepeat(sqlgen.IndexDefinition, indexCount, indexCount)
	for i := 0; i < tableCount; i++ {
		query := sqlgen.CreateTable.Eval(state)
		sqls = append(sqls, query)
	}
	for _, tb := range state.GetAllTables() {
		state.CreateScope()
		state.Store(sqlgen.ScopeKeyCurrentTables, sqlgen.Tables{tb})
		for i := 0; i < rowCount; i++ {
			query := sqlgen.InsertInto.Eval(state)
			sqls = append(sqls, query)
		}
		state.DestroyScope()
	}
	return sqls
}

func generatePlainSQLs(state *sqlgen.State, count int) []string {
	state.Clear(sqlgen.StateClearOptionAll)
	sqls := make([]string, 0, count)
	for i := 0; i < count; i++ {
		sqls = append(sqls, sqlgen.Start.Eval(state))
	}
	return sqls
}

func compareResult(rs1, rs2 *resultset.ResultSet, query string) error {
	h1, h2 := rs1.OrderedDigest(resultset.DigestOptions{}), rs2.OrderedDigest(resultset.DigestOptions{})
	if h1 != h2 {
		var b1, b2 bytes.Buffer
		rs1.PrettyPrint(&b1)
		rs2.PrettyPrint(&b2)
		return fmt.Errorf("result digests mismatch: %s != %s %q\n%s\n%s", h1, h2, query, b1.String(), b2.String())
	}
	if rs1.IsExecResult() && rs1.ExecResult().RowsAffected != rs2.ExecResult().RowsAffected {
		return fmt.Errorf("rows affected mismatch: %d != %d %q",
			rs1.ExecResult().RowsAffected, rs2.ExecResult().RowsAffected, query)
	}
	return nil
}

func printCmd() *cobra.Command {
	var count int
	cmd := &cobra.Command{
		Use:           "print",
		Short:         "Print SQL statements",
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, args []string) error {
			state := sqlgen.NewState()
			gen := sqlgen.NewGenerator(state)
			for i := 0; i < count; i++ {
				fmt.Printf("%s;\n", gen())
			}
			return nil
		},
	}
	cmd.Flags().IntVar(&count, "count", 1, "number of SQLs")
	return cmd
}
