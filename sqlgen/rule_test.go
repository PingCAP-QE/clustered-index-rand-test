package sqlgen_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/PingCAP-QE/clustered-index-rand-test/sqlgen"
	"github.com/stretchr/testify/require"
)

func TestStart(t *testing.T) {
	state := sqlgen.NewState()
	defer state.CheckIntegrity()
	for i := 0; i < 300; i++ {
		res := sqlgen.Start.Eval(state)
		require.Greater(t, len(res), 0, i)
	}
	require.Equal(t, state.Env().Depth(), 0)
}

func TestCreateTable(t *testing.T) {
	state := sqlgen.NewState()
	defer state.CheckIntegrity()
	for i := 0; i < 300; i++ {
		res := sqlgen.CreateTable.Eval(state)
		require.Greater(t, len(res), 0, i)
	}
}

func TestCreateColumnTypes(t *testing.T) {
	state := sqlgen.NewState()
	defer state.CheckIntegrity()

	state.ReplaceRule(sqlgen.ColumnDefinitionType, sqlgen.ColumnDefinitionTypesIntegerInt)
	state.SetRepeat(sqlgen.ColumnDefinition, 5, 5)
	intColCount := 0
	for i := 0; i < 100; i++ {
		res := sqlgen.CreateTable.Eval(state)
		require.Greater(t, len(res), 0, i)
		intColCount += strings.Count(res, "int")
	}
	require.Equal(t, 100*5, intColCount)
}

func TestCreateTableLike(t *testing.T) {
	state := sqlgen.NewState()
	defer state.CheckIntegrity()
	sqlgen.CreateTable.Eval(state)
	for i := 0; i < 100; i++ {
		sqlgen.CreateTableLike.Eval(state)
		state.Env().Table = state.GetRandTable()
		sqlgen.AddColumn.Eval(state)
		dropColTbls := state.FilterTables(func(t *sqlgen.Table) bool {
			state.Env().Table = t
			return sqlgen.MoreThan1Columns(state) && sqlgen.HasDroppableColumn(state)
		})
		if len(dropColTbls) > 0 {
			state.Env().Table = dropColTbls.PickOne()
			sqlgen.DropColumn.Eval(state)
		}
	}
}

func TestAlterColumnPosition(t *testing.T) {
	state := sqlgen.NewState()
	defer state.CheckIntegrity()
	state.SetRepeat(sqlgen.ColumnDefinition, 10, 10)
	_ = sqlgen.CreateTable.Eval(state)
	state.Env().Table = state.GetRandTable()
	for i := 0; i < 10; i++ {
		_ = sqlgen.InsertInto.Eval(state)
	}
	for i := 0; i < 100; i++ {
		_ = sqlgen.AlterColumn.Eval(state)
	}
	// TODO: implement the type-value compatibility check.
	tbl := state.GetRandTable()
	for _, c := range tbl.Columns {
		fmt.Printf("%s ", c.Tp)
	}
	fmt.Println()
	for _, v := range tbl.GetRandRow(nil) {
		fmt.Printf("%s ", v)
	}
	fmt.Println()
}

func TestConfigKeyUnitAvoidAlterPKColumn(t *testing.T) {
	state := sqlgen.NewState()
	defer state.CheckIntegrity()
	state.SetRepeat(sqlgen.ColumnDefinition, 10, 10)
	state.SetRepeat(sqlgen.IndexDefinition, 1, 1)
	state.ReplaceRule(sqlgen.IndexDefinitionType, sqlgen.IndexDefinitionTypePrimary)
	state.ReplaceRule(sqlgen.AlterColumn, sqlgen.AlterColumnNoPK)

	_ = sqlgen.CreateTable.Eval(state)
	tbl := state.GetRandTable()
	pk := tbl.GetRandomIndex()
	require.Equal(t, sqlgen.IndexTypePrimary, pk.Tp)
	pkCols := tbl.Columns.FilterColumns(func(c *sqlgen.Column) bool {
		return pk.ContainsColumn(c)
	})
	state.Env().Table = tbl
	for i := 0; i < 30; i++ {
		sqlgen.AlterColumn.Eval(state)
	}
	for _, pkCol := range pkCols {
		require.True(t, tbl.Columns.ContainColumn(pkCol))
	}
}
