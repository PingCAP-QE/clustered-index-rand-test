clustered-index-rand-test
===

This project is another string generator. It provides a flexible way to generate random executable TiDB(MySQL dialect) statements.

As an example, here are 5 statements generated randomly by it:

```sql
create table tbl_0 ( col_0 float not null  ) ;

insert ignore into tbl_0 set col_0 = 8153.190551246335 on duplicate key update col_0 = 3997.4899996753697;

update tbl_0 set col_0 = 3603.5625826319924 , col_0 = 58.00063810721654 , col_0 = 3505.8937799085083 where tbl_0.col_0 <> 4434.149686520456 and tbl_0.col_0 != 3926.1006744991073 and tbl_0.col_0 in ( 9744.643055728244 , 727.5001134612802 , 4280.999473216482 ) ;

insert ignore into tbl_0  values ( 1073.1669938161926 ) , ( 6322.370383941029 ) , ( 1522.1675401347838 ) , ( 3773.6490425815045 ) , ( 3702.7100487371335 ) on duplicate key update col_0 = 9631.248457125721;

( select   row_number() over w from tbl_0 window w as ( order by col_0 rows between 4 preceding and current row) order by col_0 , ntile( 3 ) over w limit 311 for update ) union ( select   percent_rank() over w from tbl_0 window w as ( order by col_0 )  limit 90 ) order by 1 limit 811;
```

To make the generated statements as semantic-correct as possible, an internal state is maintained during the generating process. For example, `INSERT`s can only be generated after a `CREATE TABLE`.

You can adjust the config to allow/disallow generating some statements, or change the probability of the specific statements.

## Quick start

The following snippet prints 200 `CREATE TABLE` SQLs. Each table has no indexes or partitions.

```go
func main() {
    state := sqlgen.NewState()
    state.Config().SetMaxTable(200)
    state.SetWeight(sqlgen.IndexDefinitions, 0)
    state.SetWeight(sqlgen.PartitionDefinition, 0)
    for i := 0; i < 200; i++ {
        sql := sqlgen.CreateTable.Eval(state)
        fmt.Print(sql)
        fmt.Println(";")
    }
}
```

Output:

```sql
...
create table tbl_30 ( col_152 smallint unsigned  ) ;
create table tbl_31 ( col_153 boolean , col_154 bigint unsigned default 3261186910083315146 not null , col_155 tinyint unsigned default 18 not null , col_156 timestamp , col_157 set('Alice','Bob','Charlie','David') default 'Charlie' , col_158 mediumint unsigned  ); 
create table tbl_32 ( col_159 blob(293) collate binary  ) ;
...
```

What is happening:

1. You create a new state through `sqlgen.NewState()`.
2. Before generating SQLs, you adjust the config properly:
    - The max table count is changed to `200`(default is `20`).
    - Index & paritition definitions are `SetWeight()` to `0`.
3. You invoke the `Eval()` of `sqlgen.CreateTable` with the state initialized before. It returns a `CREATE TABLE` string.

To explore more examples, see the file`sqlgen/example_test.go`.

## Features

- Easy to use: As mentioned in "Quick Start", to generate a custom SQL, only a few steps are required. You can use the state config methods to achieve the following purposes:
    - Not to generate part of SQL: `state.SetWeight(rule, 0)`
    - Set the weight of part of SQL: `state.SetWeight(rule, weight)`
    - Set the repeat count of part of SQL: `state.SetRepeat(rule, lower_count, upper_count)`
    - Set the advance config option: `state.ReplaceRule(old_rule, new_rule)`


- Good readability: it uses BNF-style code to describe the grammar. The grammar definition of data manipulation language(DML) is as follows:
    ```go
    var DMLStmt = NewFn(func(state *State) Fn {
        // ...
        return Or(
            CommonDelete,
            CommonInsertOrReplace,
            CommonUpdate,
        )
    })
    ```
    It will randomly pick one of `DELETE`, `INSERT/REPLACE` and `UPDATE` during the generating process(`DMLStmt.Eval()`).
    
    Good readibility means peeking the definition of the "rule" should be enough to know its functionality.

- Flexible state management: the state is used to make sure the generated SQLs are semantic-correct. Unlike randgen/go-randgen, this project uses Golang instead of embedded scripting languages(Perl/Lua) to represent and interact with the state.

    Thus, the state can be managed in a modular way:
    ```go
    type State struct {
        // ...
        tables []*Table
    }
    type Table struct {
        ID      int
        Name    string
        Columns []*Column
        Indices []*Index
        // ...
    }
    ```

    Furthermore, the generating process can be run interactively. You can change the state on the fly according to the response status of the target database server, or other complex conditions.

## Basics

As mentioned above, all the rules in grammar are written in Golang. 

**`Fn`** is a representation of the "rule". Evaluating a `Fn` can get a string.

```go
var SetOperator = NewFn(func(state *State) Fn {
    return Or(
        Str("union"),
        Str("union all"),
        Str("except"),
        Str("intersect"),
    )
})
```

`SetOperator` is one of the `Fn`s. Here we show the basic structure of a `Fn`. It is designed to be an exported global variable, so that users can reference it to generate a string directly through `SetOperator.Eval()`. 

A closure is passed to the `NewFn()`, which picks a string randomly from the set `{"union", "union all", "except", "intersect"}`.

`Or()` is one of the `Fn` combinators. A **combinator** accepts one or more `Fn`s as parameters, and returns exactly one `Fn`. `Or()` randomly pick one of its parameters as the returning value. There are some other combinators:
  - `And()` concatenates all of its parameters, using a space(" ") as the separator.
  - `If()` returns empty string if the first paramenter(bool type) is evaluated to `false`. Otherwise, it returns the second parameter.
  - `Opt()` returns empty string with 1/2 probability. Otherwise, it returns the first parameter.

This allows us to describe the grammar with the style of BNF:

```go
return Or(
    And(...),
    And(..., Opt(...)),
    If(..., 
        Or(
            And(...),
        ),
    )
)
```

You may notice that there is a parameter `state *State` in closure signature:

```go
func(state *State) Fn {}
```

`SetOperator` does not use this parameter because it is simple enough to express. However, some `Fn`s may need more information to generate a string. For example, the `DropColumn` statement requires the existence of the target table.

```go
var DropColumn = NewFn(func(state *State) Fn {
    tbl := state.env.Table
    col := tbl.GetRandDroppableColumn()
    tbl.RemoveColumn(col)
    return Strs(
        "alter table", tbl.Name,
        "drop column", col.Name,
    )
})
```

To get the target table, `state.env.Table` is read. This field is set by an upper level rule, e.g. `DDLStmt`.
The `state.env` is a temporary place to store variables, which is used to pass information between `Fn`.

Next we will show how to set the generating probability of different branches in the `Or()` combinator.

The **weight** is used to control this probability.

In "Quick Start" part, we use the following method to overwrite the weight of specific `Fn`.

```go
state.SetWeight(sqlgen.IndexDefinitions, 0)
```

But what is the default weight configuration for each `Fn`? The answer is `W()`:

```go
var DMLStmt = NewFn(func(state *State) Fn {
    // ...
    return Or(
        CommonDelete.W(1),
        CommonInsertOrReplace.W(3),
        CommonUpdate.W(1),
    )
})
```

In this example, the ratio of generating probability of `[Delete : Insert/Replace : Update]` is `[1 : 3 : 1]`. For those `Fn`s without following `W()`, the default weight is `1`.

Similarly, `R()` is used to change the **repeat** count of a specific `Fn` wrapped in `Repeat()`.

```go
var CommonUpdate = NewFn(func(state *State) Fn {
    tbl := state.Tables.Rand()
    state.env.Table = tbl
    return And(
        Str("update"),
        Str(tbl.Name),
        Str("set"),
        Repeat(AssignClause.R(1, 3), Str(",")), // <-------
        Str("where"),
        Predicates,
        Opt(OrderByLimit),
    )
})
```

`AssignClause.R(1, 3)` means the assignments will appear [1, 3] times in the result.

Finally, let's look back to `state *State`. Except sharing information between `Fn`s, the state plays 2 other roles:

- Database state container: it contains tables, columns, indexes, and other database entity. Each of them has a struct representation. For example, `type Column struct {...}` stands for columns.

  We can change the state through functions in `db_mutator.go`, and read the state through functions in `db_retriever.go`. Here are some examples:
  - `(s *State) AppendTable(tbl *Table)`
  - `(t *Table) AppendColumn(c *Column)`
  - `(t *Table) AppendIndex(idx *Index)`
  - `(s *State) GetRandTable() *Table`
  - `(t *Table) Columns.Rand() *Column`

- Configurations provider: these are used to control `Fn`'s behavior. It is usually set before, and keep unchanged during `Fn` evaluation. The available config option locates in `db_config.go`.

## Test

This project also integrates a simple AB test framework.

### Run AB Test

Send 200 random SQLs to `127.0.0.1:4000` and `127.0.0.1:3306` database instances, and compare the result between them:

```bash
make
./bin/sqlgen abtest \
  --dsn1 'root:@tcp(127.0.0.1:4000)/?time_zone=UTC' \
  --dsn2 'root:@tcp(127.0.0.1:3306)/?time_zone=UTC' --count 200 --debug
```

### Run quick syntax test

Send 100 random SQLs to `127.0.0.1:4000` and using the random seed `1621496851`:

```bash
make
./bin/sqlgen check-syntax --dsn 'root:@tcp(127.0.0.1:4000)/?time_zone=UTC' \
   --debug --seed 1621496851
```

### Print 100 random SQLs

```bash
make
./bin/sqlgen print --count 100
```
