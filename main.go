package main

import (
	"fmt"
	"os"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	cmd := rootCmd()
	if err := cmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "\x1b[0;31mError: %+v\x1b[0m\n", err)
		os.Exit(1)
	}

	// state := sqlgen.NewState()
	// // state.StoreConfig(sqlgen.ConfigKeyIntMaxTableCount, 200)
	// state.SetWeight(sqlgen.IndexDefinitions, 0)
	// state.SetWeight(sqlgen.PartitionDefinition, 0)
	// println(sqlgen.CreateTable.Eval(state))
	//
	// for i := 0; i < 200; i++ {
	// 	sql := sqlgen.CommonDelete.Eval(state)
	// 	fmt.Println(sql)
	// 	fmt.Println(";")
	// }
}
