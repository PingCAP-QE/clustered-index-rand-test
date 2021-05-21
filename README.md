clustered-index-rand-test
===

This project provides a flexible way to generate SQL strings.

## Quick start

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

### Generate SQLs as a library

Print 200 SQL statements randomly after setting `@@global.tidb_enable_clustered_index` to true:

```go
func main() {
    state := NewState()
    state.InjectTodoSQL("set @@global.tidb_enable_clustered_index=true")
    gen := NewGenerator(state)
    for i := 0; i < 200; i++ {
        fmt.Printf("%s;\n", gen())
    }
}
```

Generate 5 random `CREATE TABLE` statements, each with 5 columns and 2 indexes and 10 `INSERT INTO` statements.

```go
func printInitSQL() {
    state := sqlgen.NewState()
    tableCount, columnCount := 5, 5
    indexCount, rowCount := 2, 10
    state.SetRepeat(sqlgen.ColumnDefinition, columnCount, columnCount)
    state.SetRepeat(sqlgen.IndexDefinition, indexCount, indexCount)
    for i := 0; i < tableCount; i++ {
        sql := sqlgen.CreateTable.Eval(state)
        fmt.Println(sql)
    }
    for _, tb := range state.GetAllTables() {
        state.CreateScope()
        state.Store(sqlgen.ScopeKeyCurrentTables, sqlgen.Tables{tb})
        for i := 0; i < rowCount; i++ {
            sql := sqlgen.InsertInto.Eval(state)
            fmt.Println(sql)
        }
        state.DestroyScope()
    }
}
```

To check/modify the generation rules, see the file `sqlgen/start.go`.

## Features

- Good readability. It uses Yacc-style code to describe the grammar. Here is the comparison for a simple 'or' branch: 
    ```yacc
    dmlStmt:
     query
    |  commonDelete
    |  CommonInsertOrReplace
    |  commonUpdate
    ```
    ```go
    var DMLStmt = NewFn(func(state *State) Fn {
        if !state.CheckAssumptions(HasTables) {
            return None
        }
        return Or(
            QueryPrepare,
            CommonDelete,
            CommonInsertOrReplace,
            CommonUpdate,
        )
    })
    ```
- Flexible state management. It provides full functions of Golang.

  Why do we need state management? Because the correctness of the generating SQL is guaranteed by the state. 
  
  For example, the index columns in a SQL like `ALTER TABLE t ADD INDEX idx(a, b);` are chosen randomly. This requires the embedded language to store the columns information of `t`. The metadata also including all available table names, all column names in each table, etc. 
  
  However, for scripting language like Perl/Lua, this can mess things up if the there are a lot of metadata to track:
 
  ```lua
   T = {
        cols = {},
        col_types = {},
        cur_col = nil,
        indices = {},
    }

    T.next_idx = function() return G.c_index_num.seq:next() end
    T.next_col = function() return G.c_column_num.seq:next() end
    T.cols[#T.cols+1] = util.col('c_int', G.c_int.rand)
    T.cols[#T.cols+1] = util.col('c_str', G.c_str.rand)
    T.cols[#T.cols+1] = util.col('c_datetime', G.c_datetime.rand)

  ...
  
  add_column:
    alter table t add column {
        local new_col_type = T.new_rand_col_types()
        local col_name = sprintf('col_%d', T.next_col());
        T.cols[#T.cols+1] = util.col(col_name, new_col_type.rand)
        printf('%s %s', col_name, new_col_type.name)
    }
  ```
  We need to maintain many arrays/maps carefully. What's more, if there is a syntax error or semantic/logic error, it is hard to debug due to the lack of syntax highlight support and the debug information.
  
- Ability to interact with foreign data. For randgen/go-randgen, the generating process is isolated. 
  
  Sometimes the users may need to generate SQL dynamically according to the database status or some given complex conditions. It is not convenient to achieve them in a scripting language. On the other hand, managing information in a modular way with Golang is easier.

## Concepts

- **Fn**: a rule that defines how to generate a part of string. We can define the `Fn`s in this way:
    ```go
    fn_name = NewFn(func(state *State) Fn {
        /* assumption check */
        /* initialization and preprocess */
        return Or(
            /* fn body */
            sub_fn1,
            sub_fn2,
            ...
        )
    })
    ```

    Evaluating a `Fn` produces a string:
    ```go
    state := NewState()
    sqlPart := fn_name.Eval(state)
    ```
    ```go
    func (f Fn) Eval(state *State) string { ... }
    ``` 
    
    When a `Fn` is being evaluated, the sub-`Fn`s are also evaluated. Each `Fn` builds its own string by using both information about itself and sub-`Fn`s. Finally, a string is concatenated bottom-up and returned.

- **Fn combinator**: A function that accepts zero or more `Fn`s and returns exactly one `Fn`. It is used to keep `Fn`s readable, there are many combinators that have corresponding notations in Yacc. Here are a few combinators:
  - `Or(...fn)`: Chooses one branch in the sub-`Fn`s, like notation `|` in Yacc.
  - `And(...fn)`: Concatenates all the sub-`Fn`s.
  - `Str(fn)`: Indicates a terminal/leaf `Fn`.
  - `If(cond, ...fn)`: Only generates the sub-`Fn`s if the condition is satisfied.
  - `Repeat(fn, sepFn)`: Concatenates the result of `fn` with the seperation `Fn` `sepFn`. The repeating times is decided by `Fn.Repeat`(with type `Interval{lower:int, upper:int}`).

- **Fn weight**: the probability to select this branch. It is useful in `Or()` combinator. Each `Fn` has an attribute `weight`. You can set the default value through `SetW()`. For example,
    ```go
    return Or(
        dmlStmt.SetW(12),
        ddlStmt.SetW(3),
        splitRegion.SetW(2),
        commonAnalyze.SetW(1),
        ...
    ),
    ```
  The ratio of selecting probability of `[dmlStmt : ddlStmt : splitRegion : commonAnalyze]` is `[12 : 3 : 2 : 1]`.

- **Fn repeat**: the repeating times of a `Fn` in `Repeat()` combinator. Each `Fn` has an attribute `repeat`. You can set the default value through `SetR()`. For example,
    ```go
    var ColumnDefinitions = NewFn(func(state *State) Fn {
        // ...
        return Repeat(ColumnDefinition.SetR(1, 10), Str(","))
    })
    ```
  The `ColumnDefinition` Fn can repeat `[1, 10]` times during evaluation.

- **State**: the place where stores all the metadata, and it provides convenient functions to retrieve/mutate them. The meta data includes:
    - database state(tables, columns, indexes),
    - configurations to control `Fn`'s behavior, and
    - intermediate information during evaluation.
    
    For database state, here is an example of state interaction:
    ```go
    var CreateTable = NewFn(func(state *State) Fn {
        tbl := GenNewTable()
        state.AppendTable(tbl)  // <--------------------- mutate
        // ...
        return And(
			Str("create table"),
			Str(tbl.name),
			Str("("),
			definitions,
			Str(")"),
			OptIf(rand.Intn(5) == 0, partitionDef),
        )
    })
    ```
    ```go
    var AnalyzeTable = NewFn(func(state *State) Fn {
        tbl := state.GetRandTable() // <--------------------- retrieve
        return And(Str("analyze table"), Str(tbl.name))
    })
    ```
    When we generating a `CREATE TABLE` SQL, a table entity `*Table` is also put to the state. The function `GetRandTable()` is used to get a random table from the state.

    For configurations, here is an example to 

- **Messeging between Fns**: there are 2 ways to pass the information between `Fn`s: 
  - Nested definition
  - Scoping mechanism

  for most `Fn`s, the requirement of message passing can be satisfied with *nested* definitions. For example, we have the production dependency `createTable -> definitions -> idxDefs -> idxDef`. We need to modify the state during generating `idxDef` by appending a new index to the current table(`currentTable.Append(newIndex)`). To reference the entity `currentTable`, we can define them in a nested block:
    ```go
    createTable = NewFn("createTable", func() Fn {
        tbl := GenNewTable(state.AllocGlobalID(ScopeKeyTableUniqID)) // <------ `tbl` referenced
        state.AppendTable(tbl)
        // ...
        definitions = NewFn("definitions", func() Fn {
            // ...
            idxDefs = NewFn("idxDefs", func() Fn {
                return Or(
                    idxDef,
                    And(idxDef, Str(","), idxDefs).SetW(2),
                )
            })
            idxDef = NewFn("idxDef", func() Fn {
                idx := GenNewIndex(state.AllocGlobalID(ScopeKeyIndexUniqID), tbl)
                tbl.AppendIndex(idx) // -------------------------------------> referencing `tbl`
            // ...
    ```

  > Advanced usage: one drawback of this approach is the disruption to the readability and reusability. If we really need to reuse a production, the `scoping` functions of state come into rescue. They are:
  > - `(s *State) CreateScope()`: Create a new scope. Similar to entering the blocks (`{}`) in Golang.
  > - `(s *State) DestroyScope()`: Destroy the innermost scope. Similar to leaving the blocks in Golang.
  > - `(s *State) Store(key ScopeKeyType, val ScopeObj)`: Attach an entity to the scope.
  > - `(s *State) Search(key ScopeKeyType) ScopeObj`: Search an entity with a given key.
  >
  > The `CreateScope` and `DestroyScope` can be automated through the `ProductionListener` provided by `generater_lib.go`(which is a hook being called before and after the `Fn` evaluation). Thus, we only need to `Store()` the entity in the parent production, then `Search()` the entity in children productions.

## Directory structure

```bash
sqlgen
├── db_constant.go
├── db_generator.go     # A collection of functions that generate random values, like column types, column values and others
├── db_mutator.go       # A collection of functions that **modify** the state
├── db_printer.go       # A collection of functions that help printing the target string
├── db_retriever.go     # A collection of functions that **read** the state
├── db_transformer.go
├── db_type.go
├── db_util.go
├── declarations.go     # Production declarations
├── example_test.go
├── generator_lib.go    # Production combinators
├── generator_types.go  # Basic struct types
└── start.go            # Yacc-style grammar file
```

## Coding style

Here are some suggestions to keep the code clean:

- Name each function regularly. 
  - the state-mutate functions start with `Set`/`Append`/`Remove`; 
  - the state-retrieve functions start with `Get`; 
  - the generating functions have the pattern `Gen...()`; 
  - the format functions have the pattern `Print...()`;
  - the random entity/value producing functions have the pattern `Rand...()`.
- Put the functions to different files according to their intend.
- `Assert` the assumptions explicitly.
- DRY(Do not Repeat Yourself).
- Avoid passing information through global variables.

## How to add a new production

For the productions with easy syntax, only 4 steps are required normally:
  - Put the declaration to `declarations.go`.
  - Write the production body. Combinators like `And()`, `Or()` and `Strs()` may be helpful.
  - Insert the new production to another existing & used production body.
  - Test it in `example_test.go`. One may need to adjust the weight through `SetW()` to increase the probability to generate.

For the productions with a lot of constraints or complex conditions, answering the following questions and coding step by step should help:

- Does this production involve a new entity? If yes, define it in `db_types.go` and provide corresponding retrieve/mutate functions.
- Do you expect to control the generating strategy with some arguments? If yes, put it in the config `ControlOption` and don't forget to update `DefaultControlOption()`.
- What are the constraints of this production? Consider using `If()` or `OptIf()`. If necessary, the Golang keyword `if` with multiple `return` statements may be a good replacement.
- What is the message passing flow? Consider using *nested* definitions over scoping mechanism.
- Does it require complex transformation? If yes, do the transformation work in `db_transformer.go`.
- Does it need to be printed in a special way? If yes, do the format work in `db_printer.go`.
- Need to make it generate multiple SQLs at a time? Use `InjectTodoSQL()`. It can append custom strings to the queue in the generator.
- Does it require the cleanup work? Here is a signature of a hook: `(p *PostListener) Register(fnName string, fn func())`. A custom function can be registered to a production, it will be called after the production is generated.
