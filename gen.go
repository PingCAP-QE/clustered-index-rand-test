package main

import (
	"math/rand"
	"time"

	"github.com/tangenta/tp-test/sqlgen"
)

type genTestOptions struct {
	Grammar    string
	InitRoot   string
	TxnRoot    string
	RecurLimit int
	NumTxn     int
	Debug      bool
	TiFlash    bool
}

func genTest(opts genTestOptions) (test Test, err error) {
	rand.Seed(time.Now().UnixNano())
	return genTestWithoutGrammarFile(opts)
}

func genTestWithoutGrammarFile(opts genTestOptions) (test Test, err error) {
	state := sqlgen.NewState2(opts.TiFlash)
	state.InjectTodoSQL("set @@tidb_enable_clustered_index=true")
	gen := sqlgen.NewGenerator(state)
	for i := 0; i < opts.NumTxn; i++ {
		txnStmtCount := 1 + rand.Intn(200)
		txn := make(Txn, 0, txnStmtCount)
		for j := 0; j < txnStmtCount; j++ {
			txn = append(txn, Stmt{Stmt: gen()})
		}
		test.Steps = append(test.Steps, txn)
	}
	return
}
