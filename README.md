# sqlr — a tiny, SQL-first builder & mapper for Go
[![License](https://img.shields.io/github/license/mashape/apistatus.svg)](https://github.com/gandaldf/sqlr/blob/master/LICENSE)
[![Go Report Card](https://goreportcard.com/badge/github.com/gandaldf/sqlr)](https://goreportcard.com/report/github.com/gandaldf/sqlr)
[![Go Reference](https://pkg.go.dev/badge/github.com/gandaldf/sqlr.svg)](https://pkg.go.dev/github.com/gandaldf/sqlr)
[![Version](https://img.shields.io/github/tag/gandaldf/sqlr.svg?color=blue&label=version)](https://github.com/gandaldf/sqlr/releases)

sqlr is a minimal SQL builder and result mapper designed to stay very close to the SQL you already write.
It focuses on keeping things simple: turn :named placeholders into driver args, expand IN (...) automatically, support bulk VALUES, and scan rows into your structs efficiently — all without a heavy ORM or a fluent DSL.

## Features:

- SQL-first, no DSL: you write the SQL, sqlr doesn’t invent a DSL; it just binds and scans.
- Multiple dialects: Postgres, MySQL, SQLite, SQL Server.
- Placeholder rendering per dialect: Postgres → $1,$2…; MySQL/SQLite → ?; SQL Server → @p1,@p2….
- Minimal API surface: New, Write/Writef, Bind, Preview/Build, Exec, ScanOne, ScanAll.
- Typed scans, fast: struct mapping via db tags or field names, nested struct flattening, pointer/null handling.
- Bulk insert made simple: :name{a,b,c} emits VALUES (...),(...),... with bound args.
- Plays well with handcrafted SQL (CTEs, JSON ops, window functions…).
- No external dependencies: only the standard library.
- Performance-minded: single-pass parser, sync.Pool builders, cached struct plans, careful allocation.
- Safe by design: values are never interpolated into SQL strings; everything is parameterized.
- Concurrency: share one *SQLR across goroutines; each *Builder is single-use.

## Installation:
```
go get github.com/gandaldf/sqlr@latest
```

## Examples:

### Quick start
```golang
package main

import (
	"database/sql"
	"log"

	_ "github.com/lib/pq"
	"github.com/gandaldf/sqlr"
)

type User struct {
	ID   int    `db:"id"`
	Name string `db:"name"`
}

func main() {
	db, _ := sql.Open("postgres", "<dsn>")

	var users []User
	err := sqlr.New(sqlr.Postgres).
		Write("SELECT id, name FROM users WHERE id IN (:ids) AND active=:active").
		Bind("ids", []int{1,2,3}).
		Bind("active", true). // later binds can add/override keys
		ScanAll(db, &users)
	if err != nil {
        log.Fatal(err)
    }
}
```

### Execute a statement
```golang
res, err := sqlr.New(sqlr.MySQL).
  Write("UPDATE products SET price=:price WHERE id IN (:ids)").
  Bind("price", 999, "ids", []int{7,8,9}).
  Exec(db)
if err != nil { return err }
rows, _ := res.RowsAffected()
```

### Read a single scalar
```golang
var count int
err := sqlr.New(sqlr.Postgres).
  Write("SELECT COUNT(*) FROM orders WHERE customer_id=:c AND status=:s").
  Bind("c", 42, "s", "paid").
  ScanOne(db, &count)
```

### One row exactly
```golang
var u User
err := sqlr.New(sqlr.Postgres).
  Write("SELECT id, name FROM users WHERE email=:e").
  Bind("e", email).
  ScanOne(db, &u)
// returns sql.ErrNoRows if none; sqlr.ErrMoreThanOneRow if >1
```

### Struct scans (tags, flattening, NULLs)
```golang
type Audit struct {
	CreatedAt time.Time `db:"created_at"`
}
type Row struct {
	ID    int     `db:"id"`
	Name  string  `db:"name"`
	Note  *string `db:"note"` // pointer handles NULL
	Audit Audit
}

var out []Row
err := sqlr.New(sqlr.Postgres).
  Write(`SELECT id, name, note, created_at FROM users WHERE active=:a`).
  Bind("a", true).
  ScanAll(db, &out)
```
- created_at maps into Audit.CreatedAt via flattening.
- Pointers become nil when the DB returns NULL.

### Bulk insert
```golang
type NewUser struct {
	ID   int    `db:"id"`
	Name string `db:"name"`
}
rows := []NewUser{{1,"Anna"},{2,"Luca"},{3,"Mia"}}

_, err := sqlr.New(sqlr.SQLite).
  Write("INSERT INTO users (id,name) VALUES :batch{id,name}").
  Bind("batch", rows).
  Exec(db)
```
The placeholder is called ```:batch{...}``` here but it's arbitrary, It's not a keyword, but just a regular named parameter with curly braces.

### Expansion in action
sqlr expands at build time based on your bound values. You write :named params; sqlr turns them into the right placeholders for the dialect, expands slices/rows, and builds the final args in one pass.

#### IN (...) slice expansion
```golang
q, args, _ := sqlr.New(sqlr.Postgres).
  Write("SELECT * FROM t WHERE id IN (:ids) AND active=:a").
  Bind("ids", []int{10,11,12}).
  Bind("a", true).
  Preview()

// q (pretty-printed):
// SELECT * FROM t WHERE id IN ($1,$2,$3) AND active=$4
// args: [10 11 12 true]
```

#### VALUES :rows{...} bulk expansion
```golang
type NewUser struct{ ID int `db:"id"`; Name string `db:"name"` }
rows := []NewUser{{1,"Anna"},{2,"Luca"},{3,"Mia"}}

q, args, _ := sqlr.New(sqlr.Postgres).
  Write("INSERT INTO users (id,name) VALUES :rows{id,name}").
  Bind("rows", rows).
  Preview()

// q:
// INSERT INTO users (id,name) VALUES ($1,$2),($3,$4),($5,$6)
// args: [1 "Anna" 2 "Luca" 3 "Mia"]
```

### Prevent slice expansion (keep one placeholder)
```golang
ids := []int64{1,2,3}

_, _, _ = sqlr.New(sqlr.Postgres).
  Write("SELECT * FROM t WHERE id = ANY(:ids)").
  Bind("ids", sqlr.Scalar(ids)). // keeps a single param
  Build()
```
Using a driver.Valuer (e.g. pq.Array(ids)) also prevents expansion.

### Scalar binding via struct tag
```golang
// Bind a slice as a single scalar param using the ",scalar" option.
type Filter struct {
	IDs    []int  `db:"ids,scalar"` // <- prevents expansion of :ids
	Active bool   `db:"active"`
}

var out []int

f := Filter{IDs: []int{1, 2, 3}, Active: true}

err := sqlr.New(sqlr.Postgres).
  Write(`SELECT id FROM users WHERE id = ANY(:ids) AND active = :active`).
  Bind(f). // struct tags control binding behavior
  ScanAll(db, &out)
```
The ```,scalar``` option on the db tag tells sqlr not to expand the slice; it remains one placeholder whose value is the whole slice (or driver.Valuer).

### driver.Valuer (Postgres array)
```golang
import "github.com/lib/pq"

ids := []int64{1,2,3}
var out []int64

err := sqlr.New(sqlr.Postgres).
  Write("SELECT id FROM users WHERE id = ANY(:ids)").
  Bind("ids", pq.Array(ids)). // single placeholder; driver handles encoding
  ScanAll(db, &out)
```

### Valuer + Scanner (JSONB round-trip)
```golang
type JSONB map[string]any

func (j JSONB) Value() (driver.Value, error) { // driver.Valuer
    b, err := json.Marshal(j)
    return b, err
}
func (j *JSONB) Scan(src any) error { // sql.Scanner
    switch v := src.(type) {
    case []byte:
        return json.Unmarshal(v, j)
    case string:
        return json.Unmarshal([]byte(v), j)
    default:
        return fmt.Errorf("unsupported: %T", src)
    }
}

type Row struct {
    Meta JSONB `db:"meta"`
}

var rows []Row
err := sqlr.New(sqlr.Postgres).
  Write("SELECT meta FROM users WHERE active=:a").
  Bind("a", true).
  ScanAll(db, &rows)
```
In short: Valuer controls how a value is sent to the driver; Scanner controls how a column is read into your type. sqlr lets database/sql do its job here.

### Dynamic composition + Writef()
```golang
table := "audit_events" // trusted constant, not user input

b := sqlr.New(sqlr.Postgres).
  Writef("/* tenant=%d */ ", tenantID). // annotate the query
  Writef("SELECT id, ts, kind FROM %s WHERE ts >= :since", table).
  Bind("since", time.Now().Add(-6*time.Hour))

sql, args, _ := b.Preview()
// Use Exec/Scan to run; Preview does not release the builder.
```
Writef() is for safe, non-user interpolation (comments, known identifiers). Never put untrusted values in Writef().

### Conditional composition & many Bind() calls
```golang
b := sqlr.New(sqlr.Postgres).
  Write(`SELECT id, name, created_at FROM users WHERE 1=1`)

if namePrefix != "" {
  b.Write(` AND name ILIKE :name_prefix`).
    Bind("name_prefix", namePrefix+"%")
}
if len(ids) > 0 {
  b.Write(` AND id IN (:ids)`).
    Bind("ids", ids) // expands only at build time
}
if since != nil {
  b.Write(` AND created_at >= :since`).
    Bind("since", *since)
}

var users []User
if err := b.ScanAll(db, &users); err != nil { /* ... */ }
```
Why many Bind() calls are cheap
- Each Bind(...) simply writes keys into an internal bag (map[string]any) owned by the builder. Later binds with the same key overwrite the previous value (last-write-wins).
- There’s no SQL re-parse and no args slice churn on every Bind. The heavy work happens once at Build/Exec/Scan:
    - single-pass SQL parse,
    - placeholder numbering per dialect,
    - slice/rows expansion,
    - final []any allocation and fill.
- Complexity is roughly O(L + H + E) where:
    - L = SQL length scanned once,
    - H = number of placeholders resolved via O(1) map lookups,
    - E = total items produced by expansions (IN (:ids), :rows{...}, etc).
- Only Bind(struct)/Bind(map) perform reflection or map iteration once per call to materialize/update the bag. Repeated Bind("k", v) pairs are essentially single map writes.

This design lets you compose queries freely with negligible per-bind overhead, while keeping all value interpolation strictly parameterized.

### JOIN into two structs with overlapping field names
```golang
type User struct {
	ID   int    `db:"u_id"` // note the alias-tag mapping
	Name string `db:"u_name"`
}
type Order struct {
	ID     int     `db:"o_id"` // overlaps on name "id", so we alias
	Total  float64 `db:"total"`
}
type Row struct {
	User  User
	Order Order
}

var rows []Row
err := sqlr.New(sqlr.Postgres).
  Write(`
    SELECT
      u.id   AS u_id,
      u.name AS u_name,
      o.id   AS o_id,
      o.total
    FROM users u
    JOIN orders o ON o.user_id = u.id
    WHERE o.status = :st
  `).
  Bind("st", "paid").
  ScanAll(db, &rows)
```

### Alternatives to Bind("k", v)
When you have many parameters—or they already live in a struct/map—it’s often nicer to bind them in one shot instead of writing multiple Bind("k", v) calls. sqlr accepts a literal param map (P{}), any map[string]any, or a struct (using db tags or field names); all end up in the same internal bag, can be mixed freely, and follow last-write-wins when keys overlap.

#### Bind a param map with P{}
```golang
err := sqlr.New(sqlr.Postgres).
  Write("SELECT * FROM products WHERE brand=:b AND price<=:p").
  Bind(sqlr.P{"b": "Acme", "p": 100}).
  ScanAll(db, &out)
```

#### Bind a struct (uses db tags or field names)
```golang
type Filter struct {
  Brand string `db:"b"`
  MaxP  int    `db:"p"`
}
f := Filter{"Acme", 100}

err := sqlr.New(sqlr.Postgres).
  Write("SELECT * FROM products WHERE brand=:b AND price<=:p").
  Bind(f).
  ScanAll(db, &out)
```

#### Bind a generic map
```golang
m := map[string]any{"b": "Acme", "p": 100}

err := sqlr.New(sqlr.Postgres).
  Write("SELECT * FROM products WHERE brand=:b AND price<=:p").
  Bind(m).
  ScanAll(db, &out)
```

### ExecContext with timeout
```golang
ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
defer cancel()

res, err := sqlr.New(sqlr.Postgres).
  Write("UPDATE products SET price=:p WHERE id IN (:ids)").
  Bind("p", 999, "ids", []int{7,8,9}).
  ExecContext(ctx, db)
if err != nil { return err }
```

### ScanAllContext with cancellation
```golang
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

var users []User
err := sqlr.New(sqlr.Postgres).
  Write("SELECT id, name FROM users WHERE active=:a").
  Bind("a", true).
  ScanAllContext(ctx, db, &users)
if err != nil { return err }
```

### ScanOneContext with deadline
```golang
deadline := time.Now().Add(500 * time.Millisecond)
ctx, cancel := context.WithDeadline(context.Background(), deadline)
defer cancel()

var count int
err := sqlr.New(sqlr.Postgres).
  Write("SELECT COUNT(*) FROM orders WHERE status=:s").
  Bind("s", "paid").
  ScanOneContext(ctx, db, &count)
if err != nil { return err }
```

### Builder release & safe reuse
Build, Exec and Scan release the builder back to an internal pool. Don’t keep using it after those calls. Use Preview if you need to inspect without releasing.

#### Don’t reuse after Exec/Build
```golang
b := sqlr.New(sqlr.Postgres).
  Write("UPDATE t SET a=:a WHERE id=:id").
  Bind("a", 1, "id", 7)

_, err := b.Exec(db) // releases b
if err != nil { return err }

// b.Write(" AND ...") // DONT'T: b is released
```

#### Inspect, then execute (Preview doesn’t release)
```golang
b := sqlr.New(sqlr.Postgres).
  Write("SELECT * FROM t WHERE id IN (:ids)").
  Bind("ids", []int{1,2,3})

q, args, _ := b.Preview() // still usable
_ = q; _ = args

var out []int
if err := b.ScanAll(db, &out); err != nil { /* ... */ } // releases here
```

#### Start fresh when you need a new query
```golang
b := sqlr.New(sqlr.Postgres)

// first query
if _, err := b.Write("DELETE FROM sessions WHERE user_id=:u").
  Bind("u", userID).
  Exec(db); err != nil { return err }

// second query → new builder
var user User
if err := b.Write("SELECT id,name FROM users WHERE id=:u").
  Bind("u", userID).
  ScanOne(db, &user); err != nil { return err }
```

### Transactions
```golang
b := sqlr.New(sqlr.Postgres)

ctx := context.Background()
tx, err := db.BeginTx(ctx, nil)
if err != nil {
    return err
}
defer tx.Rollback()

// 1) debit
if _, err := b.Write("UPDATE accounts SET balance=balance-:amt WHERE id=:id").
  Bind("amt", 50, "id", 1001).
  ExecContext(ctx, tx); err != nil { return err }

// 2) credit
if _, err := b.Write("UPDATE accounts SET balance=balance+:amt WHERE id=:id").
  Bind("amt", 50, "id", 2002).
  ExecContext(ctx, tx); err != nil { return err }

// 3) read something within the same tx
var total int
if err := b.Write("SELECT COUNT(*) FROM ledger WHERE ok=:ok").
  Bind("ok", true).
  ScanOneContext(ctx, tx, &total); err != nil { return err }

return tx.Commit()
```

## Gotchas & tips:
- The *SQLR instance is reusable and thread-safe across the app; each Write() spawns a disposable builder that is released by Build, Exec or Scan.
- Builder lifecycle: Build, Exec, and Scan release the builder to an internal pool. Don’t reuse it afterward. Use Preview to inspect without releasing.
- Empty inputs:
    - IN (:ids) with an empty slice → error (ErrSliceEmpty). Decide your own fallback (WHERE 1=0, omit the clause, etc.).
    - :name{...} with an empty slice → error (ErrRowsEmpty).
- Missing binds: referencing :name that isn’t provided yields ErrParamMissing.
- Ambiguous mapping: two struct fields mapping to the same column name cause ErrFieldAmbiguous. Disambiguate with tags/aliases (as in the JOIN example).
- NULL into non-pointer: scanning NULL into a non-pointer field triggers a driver scan error. Use *T or sql.Null*.
- Quotes/comments are respected: :not_a_param inside string literals, comments, or Postgres dollar-quoted blocks is ignored.
- Writef() safety: only use with trusted literals (comments, known identifiers). Never pass user input to Writef().

## Benchmarks:
```
BenchmarkBind_Short_AllDialects/postgres-10              2589922               464.7 ns/op           432 B/op          4 allocs/op
BenchmarkBind_Short_AllDialects/mysql-10                 2671608               450.6 ns/op           432 B/op          4 allocs/op
BenchmarkBind_Short_AllDialects/sqlite-10                2670493               448.9 ns/op           432 B/op          4 allocs/op
BenchmarkBind_Short_AllDialects/sqlserver-10             2579859               467.0 ns/op           432 B/op          4 allocs/op
BenchmarkBind_Medium_AllDialects/postgres-10              785796              1487 ns/op            1296 B/op         20 allocs/op
BenchmarkBind_Medium_AllDialects/mysql-10                 793989              1393 ns/op            1280 B/op         20 allocs/op
BenchmarkBind_Medium_AllDialects/sqlite-10                862000              1371 ns/op            1280 B/op         20 allocs/op
BenchmarkBind_Medium_AllDialects/sqlserver-10             797533              1501 ns/op            1296 B/op         20 allocs/op
BenchmarkBind_Long_AllDialects/postgres-10                 17695             67829 ns/op          121531 B/op        534 allocs/op
BenchmarkBind_Long_AllDialects/mysql-10                    23005             52208 ns/op          104049 B/op        532 allocs/op
BenchmarkBind_Long_AllDialects/sqlite-10                   22969             52065 ns/op          104048 B/op        532 allocs/op
BenchmarkBind_Long_AllDialects/sqlserver-10                17173             70306 ns/op          133826 B/op        535 allocs/op
```

### Performance notes
- Builders are pooled; scanning uses cached plans and reuses holders to minimize allocations.
- Field-index lookups are cached in a compact two-tier map.
- Benchmarks and fuzz tests in the repo guard performance and safety.

## Contributing:

Issues and PRs are welcome — especially additional tests, micro-benchmarks, and dialect edge-cases.

## License:

MIT (see LICENSE).