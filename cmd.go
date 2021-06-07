package main

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"go.uber.org/zap"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/PingCAP-QE/clustered-index-rand-test/sqlgen"
	"github.com/pingcap/log"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	. "github.com/zyguan/just"
	"github.com/zyguan/sqlz"
	"github.com/zyguan/sqlz/resultset"
)

func rootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use: "sqlgen",
	}
	cmd.AddCommand(printCmd())
	cmd.AddCommand(abtestCmd())
	cmd.AddCommand(checkSyntaxCmd())

	return cmd
}

func checkSyntaxCmd() *cobra.Command {
	var (
		stmtCount  int
		seed       string
		debug      bool
		dsn        string
		failfast   bool
		outputFile string
	)
	cmd := &cobra.Command{
		Use:           "check-syntax",
		Short:         "Run syntax check test",
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, args []string) error {
			parseAndSetSeed(seed)
			fileWriter := newFileWriter(outputFile)
			conn := setUpDatabaseConnection(dsn)

			state := sqlgen.NewState()
			queries := generatePlainSQLs(state, stmtCount)
			//queries := generateCreateTables(state, stmtCount)

			for i, query := range queries {
				if debug {
					fmt.Printf("-- statement seq: %d\n", i)
					fmt.Println(query + ";")
				}
				fileWriter.writeSQL(query)
				_, err := executeQuery(conn, query)
				if err != nil {
					errMsg := strings.ToLower(err.Error())
					if strings.Contains(errMsg, "error") &&
						strings.Contains(errMsg, "error 1064") {
						return err
					}
					fmt.Println(colorizeErrorMsg(err))
					if failfast {
						return err
					}
				}
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&dsn, "dsn", "", "dsn for database")
	cmd.Flags().IntVar(&stmtCount, "count", 100, "number of statements to run")
	cmd.Flags().StringVar(&seed, "seed", "1", "random seed")
	cmd.Flags().BoolVar(&debug, "debug", false, "print generated SQLs")
	cmd.Flags().BoolVar(&failfast, "failfast", false, "fail on any error")
	cmd.Flags().StringVar(&outputFile, "out", "", "the file path to put the generated SQLs")
	return cmd
}

func colorizeErrorMsg(msg error) string {
	if msg == nil {
		return ""
	}
	return fmt.Sprintf("\u001B[31m%s\u001B[0m", msg.Error())
}

func parseAndSetSeed(seed string) int64 {
	var seedInt int64
	if seed == "now" {
		seedInt = time.Now().Unix()
		rand.Seed(seedInt)
	} else {
		seedInt = int64(Try(strconv.Atoi(seed)).(int))
		rand.Seed(seedInt)
	}
	fmt.Printf("current seed: %d\n", seedInt)
	return seedInt
}

type fileWriter struct {
	file *os.File
}

func newFileWriter(path string) *fileWriter {
	if len(path) == 0 {
		return &fileWriter{}
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		log.Error("newFileWriter.OpenFile", zap.Error(err))
		return &fileWriter{}
	}
	prependSQLs := []string{
		"drop database if exists test_syntax;",
		"create database test_syntax;",
		"use test_syntax",
	}
	for _, s := range prependSQLs {
		_, err = file.WriteString(fmt.Sprintf("%s\n", s))
		if err != nil {
			log.Error("newFileWriter.prependSQLs", zap.Error(err))
			return &fileWriter{}
		}
	}
	return &fileWriter{file: file}
}

func (f *fileWriter) writeSQL(query string) {
	if f.file == nil {
		return
	}
	_, err := f.file.WriteString(fmt.Sprintf("%s;\n", query))
	if err != nil {
		log.Error("fileWriter.writeSQL", zap.Error(err))
	}
}

func abtestCmd() *cobra.Command {
	var (
		stmtCount   int
		dsn1        string
		dsn2        string
		sqlFilePath string
		logPath     string
		seed        string
		debug       bool
	)
	cmd := &cobra.Command{
		Use:           "abtest",
		Short:         "Run AB test",
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, args []string) error {
			parsedSeed := parseAndSetSeed(seed)

			conn1 := setUpDatabaseConnection(dsn1)
			conn2 := setUpDatabaseConnection(dsn2)

			state := sqlgen.NewState()
			queries := generateInitialSQLs(state)
			queries = append(queries, generatePlainSQLs(state, stmtCount)...)

			for _, query := range queries {
				if debug {
					fmt.Println(query + ";")
				}
				rs1, err1 := executeQuery(conn1, query)
				rs2, err2 := executeQuery(conn2, query)
				if debug {
					fmt.Println(colorizeErrorMsg(err1))
					fmt.Println(colorizeErrorMsg(err2))
				}
				if !ValidateErrs(err1, err2) {
					msg := fmt.Sprintf("error mismatch: %v != %v\nseed: %d\nquery: %s", err1, err2, parsedSeed, query)
					return errors.Errorf(msg)
				}
				if rs1 == nil && rs2 == nil {
					continue
				}
				if debug {
					fmt.Println(rs1.String())
					fmt.Println(rs2.String())
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
	cmd.Flags().StringVar(&seed, "seed", "1", "random seed")
	cmd.Flags().BoolVar(&debug, "debug", false, "print generated SQLs")
	return cmd
}

func setUpDatabaseConnection(dsn string) *sql.Conn {
	ctx := context.Background()
	db := Try(sql.Open("mysql", dsn)).(*sql.DB)
	dbName := "sqlgen_test"
	conn := Try(sqlz.Connect(ctx, db)).(*sql.Conn)
	Try(conn.ExecContext(ctx, "drop database if exists "+dbName))
	Try(conn.ExecContext(ctx, "create database "+dbName))
	Try(conn.ExecContext(ctx, "use "+dbName))
	return conn
}

func executeQuery(conn *sql.Conn, query string) (*resultset.ResultSet, error) {
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

func generateCreateTables(state *sqlgen.State, count int) []string {
	sqls := make([]string, 0, count+1)
	sqls = append(sqls, "set @@tidb_enable_clustered_index=1")
	state.StoreConfig(sqlgen.ConfigKeyIntMaxTableCount, count)
	state.StoreConfig(sqlgen.ConfigKeyUnitLimitIndexKeyLength, struct{}{})
	state.SetWeight(sqlgen.SwitchClustered, 0)
	for i := 0; i < count; i++ {
		sqls = append(sqls, sqlgen.CreateTable.Eval(state))
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
			for i := 0; i < count; i++ {
				fmt.Printf("%s;\n", sqlgen.Start.Eval(state))
			}
			return nil
		},
	}
	cmd.Flags().IntVar(&count, "count", 1, "number of SQLs")
	return cmd
}

func ValidateErrs(err1 error, err2 error) bool {
	ignoreErrMsgs := []string{
		"with index covered now",                         // 4.0 cannot drop column with index
		"Unknown system variable",                        // 4.0 cannot recognize tidb_enable_clustered_index
		"Split table region lower value count should be", // 4.0 not compatible with 'split table between'
		"Column count doesn't match value count",         // 4.0 not compatible with 'split table by'
		"for column '_tidb_rowid'",                       // 4.0 split table between may generate incorrect value.
		"Unknown column '_tidb_rowid'",                   // 5.0 clustered index table don't have _tidb_row_id.
	}
	for _, msg := range ignoreErrMsgs {
		match := OneOfContains(err1, err2, msg)
		if match {
			return true
		}
	}
	return (err1 == nil && err2 == nil) || (err1 != nil && err2 != nil)
}

func OneOfContains(err1, err2 error, msg string) bool {
	c1 := err1 != nil && strings.Contains(err1.Error(), msg) && err2 == nil
	c2 := err2 != nil && strings.Contains(err2.Error(), msg) && err1 == nil
	return c1 || c2
}
