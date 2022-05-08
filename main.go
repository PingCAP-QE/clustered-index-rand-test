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
}
