SQL Generator Documentation
===

## Quick start

#### AB Test

Start 2 MySQL/TiDB servers serving the port `4000` and `4001` respectively, run `./run-tptest.sh` in the project root dir.

#### Generate SQLs

Print 200 SQL statements randomly after setting `@@tidb_enable_clustered_index` to true:
```go
func main() {
    state := NewState()
    state.InjectTodoSQL("set @@tidb_enable_clustered_index=true")
    gen := NewGenerator(state)
    for i := 0; i < 200; i++ {
        fmt.Printf("%s;\n", gen())
}
```

To check/modify the generation rules, see the file `sqlgen/start.go`.

## Introduction

This project provides a flexible way to generate SQL strings. Comparing with randgen/go-randgen, it has the following advantages:
- Good readability. It uses Yacc-style code to describe the grammar. Here is the comparison for a simple 'or' branch: 
  ```yacc
  dmlStmt:
     query
  |  commonDelete
  |  commonInsert
  |  commonUpdate
  ```
  ```go
  dmlStmt = NewFn("dmlStmt", func() Fn {
	return Or(
		query,
		commonDelete,
		commonInsert,
		commonUpdate,
	)
  })
  ```
- Flexible state management. It provides full functions of Golang.

  Why do we need state management? Because the correctness of the generating SQL is guaranteed by the state. 
  
  For example, the index columns in a SQL like `ALTER TABLE t ADD INDEX idx(a, b);` are chosen randomly. This requires the embedded language to store these metadata(including all available table names, all column names in each table, etc.). 
  
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

- **Production**: a rule that defines how to generate a part of string. All the productions are described in this way:
    ```go
    prod_name = NewFn("prod_name", func() Fn {
        /* initialization or preprocess */
        /* production body */ return Or(
            sub_prod1,
            sub_prod2,
            ...
        )
    })
    ```
    `Fn` is a representation of production. 
    
    When it is being evaluated, the sub-productions are also evaluated. Each production builds its own string by using both information about itself and sub-productions. Finally, a string is concatenated in a bottom-up way and returned.

- **Production combinator**: A function that accepts zero or more `Fn`s and returns exactly one `Fn`. It is used to keep productions readable, there are many combinators that have corresponding notations in Yacc. Here are a few combinators:
  - `Or(...prod)`: Chooses one branch in the sub-productions, like notation `|` in Yacc.
  - `And(...prod)`: Concatenates all the sub-productions.
  - `Str(prod)`: Indicates a terminal/leaf production.
  - `If(cond, ...prod)`: Only generates the sub-production if the condition is satisfied.
  - `OptIf(cond, ...prod)`: Similar to `If()`, the difference is that `OptIf()` will generate a empty string if the condition is not satisfied. As a dummy rule, please use `If()` inside `Or()` and use `OptIf()` inside `And`. 

- **Production weight**: the probability of this branch being selected. It is only meaningful inside the `Or()` combinator. Each `Fn` has an attribute `weight`, one can set it through `SetW()`. For example,
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

- **State**: the place where stores all of the meta information. The database entities are abstracted to the corresponding structs: `Table`, `Column` and `Index`.

    Each of them also exposes convenient functions to retrieve/mutate the data. Here is an example of the most common usage:

    ```go
    createTable = NewFn("createTable", func() Fn {
        tbl := GenNewTable(state.AllocGlobalID(ScopeKeyTableUniqID))
        // ...
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
    commonAnalyze = NewFn("commonAnalyze", func() Fn {
        tbl := state.GetRandTable() // <--------------------- retrieve
        return And(Str("analyze table"), Str(tbl.name))
    })
    ```
    When we generating a `CREATE TABLE` SQL, a table entity `*Table` is also put to the state. The function `GetRandTable` is used to get a random table from the state.

- **Scope management**: for most productions, the requirement of message passing can be satisfied with *nested* definitions. For example, we have the production dependency `createTable -> definitions -> idxDefs -> idxDef`. We need to modify the state during generating `idxDef` by appending a new index to the current table(`currentTable.Append(newIndex)`). To reference the entity `currentTable`, we can define them in a nested block:
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

## FAQ

1. Why use struct instead of function to represent production?
    
    The functions are evaluated immediately when they are called. So it is not possible to reference each other in the block. However, this pattern is not uncommon in Yacc grammar. What's more, it is also inconvenient to intercept the function call.

