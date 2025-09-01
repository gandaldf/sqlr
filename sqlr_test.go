package sqlr

import (
	"bytes"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"
)

// --------------------------------
// Test utilities
// --------------------------------

// dcase groups a dialect with a display name for table-driven tests.
type dcase struct {
	name string
	d    Dialect
}

// allDialects returns the list of dialects to iterate over in tests.
func allDialects() []dcase {
	return []dcase{
		{"postgres", Postgres},
		{"mysql", MySQL},
		{"sqlite", SQLite},
		{"sqlserver", SQLServer},
	}
}

// placeholderRegex returns a compiled regex that matches placeholders for each dialect.
func placeholderRegex(d Dialect) *regexp.Regexp {
	switch d {
	case Postgres:
		return regexp.MustCompile(`\$(?:[1-9][0-9]*)`)
	case SQLServer:
		return regexp.MustCompile(`@p(?:[1-9][0-9]*)`)
	default: // MySQL, SQLite
		return regexp.MustCompile(`\?`)
	}
}

// countPlaceholders counts the placeholders present in a query for the given dialect.
func countPlaceholders(q string, d Dialect) int {
	return len(placeholderRegex(d).FindAllStringIndex(q, -1))
}

// countNewPlaceholders returns the difference in placeholder counts between out and in.
func countNewPlaceholders(out, in string, d Dialect) int {
	diff := countPlaceholders(out, d) - countPlaceholders(in, d)
	if diff < 0 {
		return 0
	}
	return diff
}

// assertNoError fails the test immediately if err != nil.
func assertNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// mustBuild is a test helper to build a query with binds and assert no error.
func mustBuild(t *testing.T, d Dialect, sql string, binds ...any) (string, []any) {
	t.Helper()
	s := New(d)
	b := s.Write(sql)
	for _, in := range binds {
		b.Bind(in)
	}
	out, args, err := b.Build()
	assertNoError(t, err)
	return out, args
}

// assertArgsEqual compares args semantically (with []byte equality support).
func assertArgsEqual(t *testing.T, got []any, want []any) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("len(args)=%d, want %d\n got=%v\nwant=%v", len(got), len(want), got, want)
	}
	for i := range got {
		if !equalArg(got[i], want[i]) {
			t.Fatalf("arg #%d = %#v, want %#v", i+1, got[i], want[i])
		}
	}
}

// equalArg is a robust equality check for test arguments (handles []byte).
func equalArg(a, b any) bool {
	ab, aok := a.([]byte)
	bb, bok := b.([]byte)
	if aok || bok {
		if !(aok && bok) {
			return false
		}
		return bytes.Equal(ab, bb)
	}
	return fmt.Sprintf("%#v", a) == fmt.Sprintf("%#v", b)
}

// mustContainInOrder asserts that subs appear in s in the given order.
func mustContainInOrder(t *testing.T, s string, subs ...string) {
	t.Helper()
	pos := 0
	for _, sub := range subs {
		i := strings.Index(s[pos:], sub)
		if i < 0 {
			t.Fatalf("substring not found (in order) %q\nTEXT:\n%s", sub, s)
		}
		pos += i + len(sub)
	}
}

// TestDialectString ensures Dialect.String() returns expected values.
func TestDialectString(t *testing.T) {
	tests := []struct {
		in   Dialect
		want string
	}{
		{Postgres, "postgres"},
		{MySQL, "mysql"},
		{SQLite, "sqlite"},
		{SQLServer, "sqlserver"},
		{Dialect(-1), "unknown"},  // default branch: valore fuori enum
		{Dialect(123), "unknown"}, // altro valore non mappato
	}

	for _, tt := range tests {
		if got := tt.in.String(); got != tt.want {
			t.Fatalf("Dialect(%d).String() = %q, want %q", tt.in, got, tt.want)
		}
	}
}

// TestPreview_MatchesBuild_NoRelease_AllDialects ensures that Preview() and Build() return the same
// SQL and args if called in sequence without any further modification to the Builder.
func TestPreview_MatchesBuild_NoRelease_AllDialects(t *testing.T) {
	for _, dc := range allDialects() {
		t.Run(dc.name, func(t *testing.T) {
			s := New(dc.d)
			b := s.Write("SELECT * FROM t WHERE a=:a AND id IN (:ids) AND note=:n")
			b.Bind(P{"a": 7, "ids": []int{1, 2, 3}, "n": "x"})

			// 1) Preview returns a rendered query and args without releasing the builder
			sql1, args1, err := b.Preview()
			assertNoError(t, err)
			if got, want := countPlaceholders(sql1, dc.d), len(args1); got != want {
				t.Fatalf("placeholders=%d, len(args)=%d\nOUT:\n%s", got, want, sql1)
			}

			// Call Preview again: it should still work and be identical
			sql2, args2, err := b.Preview()
			assertNoError(t, err)
			if sql2 != sql1 {
				t.Fatalf("Preview changed SQL between calls:\n1=%s\n2=%s", sql1, sql2)
			}
			assertArgsEqual(t, args2, args1)

			// 2) Keep using the builder after Preview (prove it wasn't released)
			b.Write(" AND extra=:x").Bind(P{"x": true})
			sql3, args3, err := b.Preview()
			assertNoError(t, err)
			if got, want := countPlaceholders(sql3, dc.d), len(args3); got != want {
				t.Fatalf("placeholders=%d, len(args)=%d\nOUT:\n%s", got, want, sql3)
			}

			// 3) Build should match the latest Preview result and release the builder
			sqlBuilt, argsBuilt, err := b.Build()
			assertNoError(t, err)
			if sqlBuilt != sql3 {
				t.Fatalf("Build SQL differs from last Preview:\nPreview=%s\nBuild =%s", sql3, sqlBuilt)
			}
			assertArgsEqual(t, argsBuilt, args3)

			// 4) After Build, Preview must fail with errBuilderReleased
			_, _, err = b.Preview()
			if err == nil || !errors.Is(err, ErrBuilderReleased) {
				t.Fatalf("expected ErrBuilderReleased after Build, got: %v", err)
			}
		})
	}
}

// TestBuilder_SingleUse_AutoFreeOnBuild_NoPanic ensures that after Build() the builder is released
// and any later use produces an error, not a panic.
func TestBuilder_SingleUse_AutoFreeOnBuild_NoPanic(t *testing.T) {
	s := New(Postgres)
	b := s.Write("SELECT :x").Bind(map[string]any{"x": 1})
	_, _, err := b.Build()
	assertNoError(t, err)

	// Any later use must produce an error, not a panic.
	// The chain can remain "fluent" but Build must fail.
	q, args, err := b.Write("SELECT 1").Bind(P{"x": 2}).Build()
	if err == nil || !strings.Contains(err.Error(), "builder already released") {
		t.Fatalf("expected ErrBuilderReleased, got: q=%q args=%v err=%v", q, args, err)
	}
}

// TestBindBag_PairsAccumulate_AllDialects ensures that multiple Bind() calls with k/v pairs
// accumulate into the same bag, with later calls overriding earlier ones for the same key.
func TestBindBag_PairsAccumulate_AllDialects(t *testing.T) {
	for _, dc := range allDialects() {
		t.Run(dc.name, func(t *testing.T) {
			out, args, err := New(dc.d).
				Write("SELECT :a, :b, :c").
				Bind().               // open/use the bag
				Bind("a", 1).         // add to the bag
				Bind("b", 2, "c", 3). // add to the bag again
				Build()
			assertNoError(t, err)
			if got, want := countPlaceholders(out, dc.d), len(args); got != want {
				t.Fatalf("placeholder=%d, len(args)=%d\nOUT:\n%s", got, want, out)
			}
			assertArgsEqual(t, args, []any{1, 2, 3})
		})
	}
}

// TestBindBag_Pairs_EvenArgsEnforced ensures that Bind() with k/v pairs
// enforces an even number of arguments (key,value,...).
func TestBindBag_Pairs_EvenArgsEnforced(t *testing.T) {
	_, _, err := New(Postgres).
		Write("SELECT 1").
		Bind("a", 1, "b"). // odd -> error
		Build()
	if err == nil || !strings.Contains(err.Error(), "even number") {
		t.Fatalf("expected error for odd key/value arity, got: %v", err)
	}
}

// TestBindBag_Pairs_KeyMustBeString ensures that Bind() with k/v pairs
// enforces that keys are non-empty strings.
func TestBindBag_Pairs_KeyMustBeString(t *testing.T) {
	_, _, err := New(Postgres).
		Write("SELECT :a").
		Bind(123, "x"). // non-string key -> error
		Build()
	if err == nil || !strings.Contains(err.Error(), "must be a non-empty string") {
		t.Fatalf("expected error for non-string key, got: %v", err)
	}
}

// TestBindBag_LastOneWins_AgainstMaps_AllDialects ensures that multiple Bind() calls with maps
// accumulate into the same bag, with later calls overriding earlier ones for the same key.
func TestBindBag_LastOneWins_AgainstMaps_AllDialects(t *testing.T) {
	for _, dc := range allDialects() {
		t.Run(dc.name, func(t *testing.T) {
			out, args, err := New(dc.d).
				Write("SELECT :a, :b, :c").
				Bind(P{"a": 1, "b": 2}). // input 1
				Bind(P{"a": 7}).         // input 2: override :a
				Bind("b", 99, "c", "x"). // bag (final input): override :b and add :c
				Build()
			assertNoError(t, err)
			if got, want := countPlaceholders(out, dc.d), len(args); got != want {
				t.Fatalf("placeholder=%d != len(args)=%d\nOUT:\n%s", got, want, out)
			}
			assertArgsEqual(t, args, []any{7, 99, "x"})
		})
	}
}

// TestBindBag_EmptyBag_StillMissingParam ensures that an empty bag
// still produces ErrParamMissing if a :name is used in the query.
func TestBindBag_EmptyBag_StillMissingParam(t *testing.T) {
	_, _, err := New(Postgres).
		Write("SELECT :a").
		Bind().
		Build()
	if err == nil || !errors.Is(err, ErrParamMissing) {
		t.Fatalf("expected ErrParamMissing with empty bag, got: %v", err)
	}
}

// TestBindBag_RowsByName_DefaultRows_AllDialects tests :rows{col1,col2,...} expansion
// with the default struct tag "db" for column names.
func TestBindBag_RowsByName_DefaultRows_AllDialects(t *testing.T) {
	type Row struct {
		A int    `db:"a"`
		B string `db:"b"`
	}
	rows := []Row{{1, "x"}, {2, "y"}}

	for _, dc := range allDialects() {
		t.Run(dc.name, func(t *testing.T) {
			out, args, err := New(dc.d).
				Write("INSERT INTO t(a,b) VALUES :rows{a,b}").
				Bind("rows", rows).
				Build()
			assertNoError(t, err)
			if got, want := countPlaceholders(out, dc.d), 4; got != want {
				t.Fatalf("placeholders=%d, want %d\nOUT:\n%s", got, want, out)
			}
			assertArgsEqual(t, args, []any{1, "x", 2, "y"})
		})
	}
}

// TestBindBag_RowsByName_CustomAlias_AllDialects tests :rows{col1,col2,...} expansion
// with a custom struct tag for column names.
func TestBindBag_RowsByName_CustomAlias_AllDialects(t *testing.T) {
	type Row struct {
		A int    `db:"a"`
		B string `db:"b"`
	}
	rows := []Row{{3, "k"}, {4, "w"}}

	for _, dc := range allDialects() {
		t.Run(dc.name, func(t *testing.T) {
			out, args, err := New(dc.d).
				Write("INSERT INTO t(a,b) VALUES :valori{a,b}").
				Bind("valori", rows).
				Build()
			assertNoError(t, err)
			if got, want := countPlaceholders(out, dc.d), 4; got != want {
				t.Fatalf("placeholders=%d, want %d\nOUT:\n%s", got, want, out)
			}
			assertArgsEqual(t, args, []any{3, "k", 4, "w"})
		})
	}
}

// TestBindBag_MixRowsAndPairs_AllDialects ensures that :rows{...} expansion
// works correctly when combined with k/v pairs in the same bag.
func TestBindBag_MixRowsAndPairs_AllDialects(t *testing.T) {
	type Row struct {
		A int    `db:"a"`
		B string `db:"b"`
	}
	rows := []Row{{1, "x"}, {2, "y"}}

	for _, dc := range allDialects() {
		t.Run(dc.name, func(t *testing.T) {
			out, args, err := New(dc.d).
				Write("INSERT INTO t(a,b) VALUES :rows{a,b}; SELECT :note").
				Bind("rows", rows). // :rows{a,b}
				Bind("note", "ok"). // bag -> :note
				Build()
			assertNoError(t, err)
			if got, want := countPlaceholders(out, dc.d), len(args); got != want {
				t.Fatalf("placeholder=%d != len(args)=%d\nOUT:\n%s", got, want, out)
			}
			assertArgsEqual(t, args, []any{1, "x", 2, "y", "ok"})
		})
	}
}
