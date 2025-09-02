package sqlr

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
)

// Dialect identifies the SQL dialect for placeholder rendering and a few
// dialect-specific parsing behaviors.
type Dialect int

// SQLR is the main entry point. It holds the selected dialect, configuration,
// and a pool of reusable *Builder instances.
// A single SQLR instance is safe for concurrent use.
type SQLR struct {
	dialect Dialect
	config  Config
	pool    sync.Pool
}

// Builder assembles a single SQL statement and bound parameters.
// It is NOT safe for concurrent use and is single-use: after Build() it is
// automatically released back to the pool and must not be used again.
type Builder struct {
	s        *SQLR
	parts    []string
	inputs   []any
	released bool
	bag      P
	err      error
}

// Config defines limits and behavior tweaks for the parser/binder.
type Config struct {
	// MaxParams limits the total number of placeholders that can be emitted by
	// a single Build().
	// If = 0 (or omitted), it uses a sensible per-dialect default.
	// If < 0, it's treated as "unlimited".
	MaxParams int
	// MaxNameLen limits the maximum allowed length of a placeholder name,
	// e.g. ":this_is_a_name". Names longer than this cause ErrParamNameTooLong.
	MaxNameLen int
}

// P is a convenient alias for map[string]any to use with Bind().
type P = map[string]any

// Execer abstracts *sql.DB / *sql.Tx ExecContext for easy testing.
type Execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// Queryer abstracts *sql.DB / *sql.Tx QueryContext for easy testing.
type Queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

const (
	Postgres Dialect = iota
	MySQL
	SQLite
	SQLServer
)

const cacheSize = 4096 // Default size for the field-index cache

var (
	ErrParamMissing     = errors.New("sqlr: missing parameter")
	ErrSliceEmpty       = errors.New("sqlr: empty slice")
	ErrRowsEmpty        = errors.New("sqlr: empty rows")
	ErrRowsMalformed    = errors.New("sqlr: malformed :rows placeholder")
	ErrColumnNotFound   = errors.New("sqlr: column not found")
	ErrTooManyParams    = errors.New("sqlr: too many parameters")
	ErrParamNameTooLong = errors.New("sqlr: parameter name too long")
	ErrFieldAmbiguous   = errors.New("sqlr: ambiguous field name")
	ErrBuilderReleased  = errors.New("sqlr: builder already released; call Write() on *SQLR for a new query")
	ErrMoreThanOneRow   = errors.New("sqlr: more than one row")
)

// String returns the string representation of the dialect.
func (d Dialect) String() string {
	switch d {
	case Postgres:
		return "postgres"
	case MySQL:
		return "mysql"
	case SQLite:
		return "sqlite"
	case SQLServer:
		return "sqlserver"
	default:
		return "unknown"
	}
}

// New returns a new SQLR for the given dialect. Optionally provide a Config;
// unspecified fields fall back to sensible per-dialect defaults.
func New(dialect Dialect, cfg ...Config) *SQLR {
	s := &SQLR{
		dialect: dialect,
		config:  defaultConfig(dialect, cfg...),
	}
	s.pool.New = func() any {
		return &Builder{
			s:      s,
			parts:  make([]string, 0, 16),
			inputs: make([]any, 0, 8),
		}
	}
	return s
}

// Write starts a new statement and returns a single-use Builder.
// You can add more chunks via Write/Writef, and bind data via Bind().
func (s *SQLR) Write(sql string) *Builder {
	b := s.pool.Get().(*Builder)
	b.s = s
	b.released = false
	b.err = nil
	b.parts = b.parts[:0]
	b.inputs = b.inputs[:0]
	if sql != "" {
		b.parts = append(b.parts, sql)
	}
	return b
}

// Write appends a raw SQL fragment. No auto-spacing is performed.
func (b *Builder) Write(sql string) *Builder {
	if b.released {
		b.err = ErrBuilderReleased
		return b
	}
	if b.err != nil {
		return b
	}
	b.parts = append(b.parts, sql)
	return b
}

// Writef appends a formatted SQL fragment. No auto-spacing is performed.
func (b *Builder) Writef(format string, args ...any) *Builder {
	if b.released {
		b.err = ErrBuilderReleased
		return b
	}
	if b.err != nil {
		return b
	}
	b.parts = append(b.parts, fmt.Sprintf(format, args...))
	return b
}

// Bind enqueues a parameter source. Supported forms:
//   - nil (ignored)
//   - struct with `db` tags (flattened through nested structs)
//   - map[string]any or any reflect.Map
//   - []struct / []map for :rows{...}
//   - slices of primitives for :name expansion
//   - k/v pairs (even number of args, first is string key)
//
// Multiple Bind() calls are allowed; resolution is "last one wins".
func (b *Builder) Bind(args ...any) *Builder {
	if b.released {
		b.err = ErrBuilderReleased
		return b
	}
	if b.err != nil {
		return b
	}

	switch len(args) {
	case 0:
		b.ensureBag()
		return b

	case 1:
		if args[0] != nil {
			b.inputs = append(b.inputs, args[0])
		}
		return b

	default:
		if len(args)%2 != 0 {
			b.err = fmt.Errorf("sqlr: Bind expects even number of args (key,value,...), got %d", len(args))
			return b
		}
		bag := b.ensureBag()
		for i := 0; i < len(args); i += 2 {
			k, ok := args[i].(string)
			if !ok || k == "" {
				b.err = fmt.Errorf("sqlr: Bind key at position %d must be a non-empty string (got %T)", i, args[i])
				return b
			}
			bag[k] = args[i+1]
		}
		return b
	}
}

// Build concatenates the query, performs binding, and RELEASES the builder
// back into the pool. After Build(), the builder must not be used again.
func (b *Builder) Build() (string, []any, error) {
	if b.released {
		return "", nil, ErrBuilderReleased
	}

	// Snapshot before defer to avoid races with Release()
	q := strings.Join(b.parts, "")
	d := b.s.dialect
	cfg := b.s.config

	// Local copy of inputs and append bag only if it has something
	in := b.inputs
	if len(b.bag) > 0 {
		in = append(in, b.bag)
	}

	defer b.Release()
	if b.err != nil {
		return "", nil, b.err
	}
	out, args, err := parse(d, q, in, cfg)
	return out, args, err
}

// Preview renders the SQL statement and bound args without releasing the Builder.
// Safe to call multiple times; identical to Build() except it does NOT Release().
// Use this to log/inspect the exact SQL and args that would be produced.
//
// If the builder has already been released, it returns ErrBuilderReleased.
func (b *Builder) Preview() (string, []any, error) {
	if b.released {
		return "", nil, ErrBuilderReleased
	}
	if b.err != nil {
		return "", nil, b.err
	}

	q := strings.Join(b.parts, "")
	d := b.s.dialect
	cfg := b.s.config

	// Local copy of inputs; append bag only if it has entries.
	in := b.inputs
	if len(b.bag) > 0 {
		in = append(in, b.bag)
	}

	out, args, err := parse(d, q, in, cfg)
	return out, args, err
}

// Release clears the builder and puts it back into the pool.
// It is safe to call Release multiple times; subsequent calls are no-ops.
func (b *Builder) Release() {
	if b.released {
		return
	}
	b.released = true

	for i := range b.parts {
		b.parts[i] = ""
	}
	b.parts = b.parts[:0]

	for i := range b.inputs {
		b.inputs[i] = nil
	}
	b.inputs = b.inputs[:0]

	b.bag = nil
	b.err = nil
	b.s.pool.Put(b)
}

// Scalar wraps a value to force it to be treated as a single scalar argument
// even if it is a slice/array. Useful for ANY(:ids)-style idioms.
func Scalar(v any) any {
	return scalar{v: v}
}

// Exec is a convenience that builds and executes the statement with context.Background().
func (b *Builder) Exec(db Execer) (sql.Result, error) {
	return b.ExecContext(context.Background(), db)
}

// ScanOne builds and runs the statement, scanning exactly one row into dest.
// It returns sql.ErrNoRows if no rows are returned. It errors if more than one row.
func (b *Builder) ScanOne(db Queryer, dest any) error {
	return b.ScanOneContext(context.Background(), db, dest)
}

// ScanAll builds and runs the statement, scanning all rows into dest slice.
func (b *Builder) ScanAll(db Queryer, dest any) error {
	return b.ScanAllContext(context.Background(), db, dest)
}

// ExecContext builds and executes the statement with the provided context.
func (b *Builder) ExecContext(ctx context.Context, db Execer) (sql.Result, error) {
	q, args, err := b.Build()
	if err != nil {
		return nil, err
	}
	return db.ExecContext(ctx, q, args...)
}

// ScanOneContext is the context-aware variant of ScanOne.
func (b *Builder) ScanOneContext(ctx context.Context, db Queryer, dest any) error {
	q, args, err := b.Build()
	if err != nil {
		return err
	}
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return err
		}
		return sql.ErrNoRows
	}
	if err := scanOne(rows, dest); err != nil {
		return err
	}

	// Must be at most ONE row
	if rows.Next() {
		return ErrMoreThanOneRow
	}
	return rows.Err()
}

// ScanAllContext is the context-aware variant of ScanAll.
func (b *Builder) ScanAllContext(ctx context.Context, db Queryer, dest any) error {
	q, args, err := b.Build()
	if err != nil {
		return err
	}
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	return scanAll(rows, dest)
}

// ensureBag makes sure the builder has a P bag for Bind(); creates if needed.
func (b *Builder) ensureBag() P {
	if b.bag == nil {
		b.bag = make(P, 8)
	}
	return b.bag
}

// defaultConfig merges user config with per-dialect defaults.
func defaultConfig(dialect Dialect, config ...Config) Config {
	c := Config{}

	if len(config) > 0 {
		c = config[0]
	}

	if c.MaxParams == 0 {
		switch dialect {
		case SQLServer:
			c.MaxParams = 2100
		case SQLite:
			c.MaxParams = 999
		case Postgres, MySQL:
			c.MaxParams = 65535
		}
	}

	if c.MaxNameLen <= 0 {
		c.MaxNameLen = 64
	}

	return c
}
