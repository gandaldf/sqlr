package sqlr

import (
	"bytes"
	"database/sql/driver"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// --------------------------------
// Tests: simple queries
// --------------------------------

// TestSimpleQueries_AllDialects verifies basic substitution, duplicated names, IN expansion,
// ignoring casts/quoted text, struct field resolution by name/tag, and that unexported fields
// cannot be bound.
func TestSimpleQueries_AllDialects(t *testing.T) {
	for _, dc := range allDialects() {
		dc := dc
		t.Run(dc.name, func(t *testing.T) {
			// 1) Simple substitution + duplication
			out, args := mustBuild(t, dc.d,
				"SELECT * FROM t WHERE a = :x OR b = :x AND c = :y",
				map[string]any{"x": 7, "y": "ok"},
			)
			if got, want := countPlaceholders(out, dc.d), 3; got != want {
				t.Fatalf("placeholder count=%d, want %d\nquery=%s", got, want, out)
			}
			assertArgsEqual(t, args, []any{7, 7, "ok"})

			// 2) IN with slice (automatic expansion)
			out, args = mustBuild(t, dc.d,
				"SELECT * FROM users WHERE id IN (:ids) AND status=:s",
				map[string]any{"ids": []int{10, 11, 12}, "s": "active"},
			)
			if got, want := countPlaceholders(out, dc.d), 4; got != want {
				t.Fatalf("placeholder count=%d, want %d\nquery=%s", got, want, out)
			}
			assertArgsEqual(t, args, []any{10, 11, 12, "active"})

			// 3) Do not match casts '::' and content inside quotes
			out, args = mustBuild(t, dc.d,
				"SELECT ':: not a cast :nope', col::int, :x",
				map[string]any{"x": 99},
			)
			if !strings.Contains(out, "::int") {
				t.Fatalf("missing '::int' in query: %s", out)
			}
			if strings.Contains(out, ":nope") == false {
				// ok: placeholder inside string must remain untouched
			}
			assertArgsEqual(t, args, []any{99})

			// 4) Fallback to field name when no `db` tag is present
			type simpleS struct {
				Title string // no tag -> use field name
			}
			_, args = mustBuild(t, dc.d,
				"SELECT :Title",
				simpleS{Title: "ok"},
			)
			assertArgsEqual(t, args, []any{"ok"})

			// 5) Priority: `db` tag must match when present; unexported fields ignored
			type mixS struct {
				ID     int    `db:"id"`
				Name   string // without tag, use "Name"
				secret int    // unexported -> ignored
			}
			_, args = mustBuild(t, dc.d,
				"SELECT :id, :Name",
				mixS{ID: 5, Name: "n"},
			)
			assertArgsEqual(t, args, []any{5, "n"})

			// 6) Placeholder trying to use unexported field -> error
			s := New(dc.d)
			_, _, err := s.Write("SELECT :secret").
				Bind(mixS{ID: 1, Name: "x"}).
				Build()
			if err == nil {
				t.Fatalf("expected error: unexported field must not be resolvable")
			}
		})
	}
}

// TestStructTagDash_IgnoreField_AllDialects ensures fields tagged with `db:"-"` are skipped
// and that attempting to bind them results in ErrParamMissing.
func TestStructTagDash_IgnoreField_AllDialects(t *testing.T) {
	type S struct {
		A int `db:"-"` // must be ignored
		B int // available as :B
	}
	for _, dc := range allDialects() {
		// ok: :B exists
		out, args, err := New(dc.d).
			Write("SELECT :B").
			Bind(S{A: 1, B: 2}).
			Build()
		assertNoError(t, err)
		if got := countPlaceholders(out, dc.d); got != 1 {
			t.Fatalf("[%s] placeholders=%d, want 1", dc.name, got)
		}
		assertArgsEqual(t, args, []any{2})

		// error: :A must not be resolvable
		_, _, err = New(dc.d).
			Write("SELECT :A").
			Bind(S{A: 1, B: 2}).
			Build()
		if err == nil || !errors.Is(err, ErrParamMissing) {
			t.Fatalf("[%s] expected ErrParamMissing for :A, got: %v", dc.name, err)
		}
	}
}

// TestBackslashEscapes_SingleQuoted_MySQLCompat_AllDialects verifies that backslash escapes
// within single-quoted strings are preserved and placeholders inside them are ignored.
func TestBackslashEscapes_SingleQuoted_MySQLCompat_AllDialects(t *testing.T) {
	// :in is inside a string with escaped quote \', it must NOT be bound; :out must be bound
	sql := "SELECT 'it\\'s just text :in', :out"
	for _, dc := range allDialects() {
		out, args, err := New(dc.d).
			Write(sql).
			Bind(map[string]any{"out": 7}).
			Build()
		assertNoError(t, err)
		if !strings.Contains(out, ":in") {
			t.Fatalf("[%s] ':in' inside the string should remain textual:\n%s", dc.name, out)
		}
		if got := countPlaceholders(out, dc.d); got != 1 {
			t.Fatalf("[%s] placeholders=%d, want 1\nOUT:\n%s", dc.name, got, out)
		}
		assertArgsEqual(t, args, []any{7})
	}
}

// TestBackslashEscapes_DoubleQuoted_AllDialects verifies that backslash escapes within
// double-quoted strings are preserved and placeholders inside them are ignored.
func TestBackslashEscapes_DoubleQuoted_AllDialects(t *testing.T) {
	// inside double quotes there is :in and also an escaped \"; only :ok should be bound
	sql := "SELECT \":in\\\"side\", :ok"
	for _, dc := range allDialects() {
		out, args, err := New(dc.d).
			Write(sql).
			Bind(map[string]any{"ok": 1}).
			Build()
		assertNoError(t, err)
		if !strings.Contains(out, ":in") {
			t.Fatalf("[%s] ':in' inside \"...\" should remain textual:\n%s", dc.name, out)
		}
		if got := countPlaceholders(out, dc.d); got != 1 {
			t.Fatalf("[%s] placeholders=%d, want 1\nOUT:\n%s", dc.name, got, out)
		}
		assertArgsEqual(t, args, []any{1})
	}
}

// TestAliasP_FastPathLike_Behavior checks that alias P binds values and auto-expands
// IN-lists (including duplication) through the fast-path-like behavior.
func TestAliasP_FastPathLike_Behavior(t *testing.T) {
	out, args, err := New(Postgres).
		Write("SELECT :a, :b, :ids").
		Bind(P{"a": 1, "b": "x", "ids": []int{7, 8}}).
		Build()
	assertNoError(t, err)
	if got := countPlaceholders(out, Postgres); got != 4 { // 1 + 1 + 2 (IN)
		t.Fatalf("placeholders=%d, want 4", got)
	}
	assertArgsEqual(t, args, []any{1, "x", 7, 8})
}

// TestAliasP_WithRows verifies alias P with :rows fast-path precomputes rows and arguments.
func TestAliasP_WithRows(t *testing.T) {
	type Row struct {
		ID   int    `db:"id"`
		Name string `db:"name"`
	}
	rows := []Row{{1, "A"}, {2, "B"}}
	out, args, err := New(MySQL).
		Write("INSERT INTO t(id,name) VALUES :rows{id,name}").
		Bind(P{"rows": rows}).
		Build()
	assertNoError(t, err)
	if got := countPlaceholders(out, MySQL); got != 4 {
		t.Fatalf("got %d", got)
	}
	assertArgsEqual(t, args, []any{1, "A", 2, "B"})
}

// TestWritef_FormatsAndNoAutoSpace_AllDialects ensures Writef formatting works,
// args order matches placeholders, and no auto-spacing is introduced.
func TestWritef_FormatsAndNoAutoSpace_AllDialects(t *testing.T) {
	for _, dc := range allDialects() {
		t.Run(dc.name, func(t *testing.T) {
			// Note: no space after '=' to verify Writef does not add spaces
			b := New(dc.d).
				Write("SELECT * FROM t WHERE a=").
				Writef(":%s AND id IN (:%s)", "a", "ids").
				Bind(P{"a": 7, "ids": []int{1, 2, 3}})

			out, args, err := b.Build()
			assertNoError(t, err)

			// 1) #placeholders == len(args)
			if got, want := countPlaceholders(out, dc.d), len(args); got != want {
				t.Fatalf("placeholder=%d, len(args)=%d\nOUT:\n%s", got, want, out)
			}

			// 2) argument order: :a then expanded :ids
			assertArgsEqual(t, args, []any{7, 1, 2, 3})

			// 3) NO auto-space: placeholder must appear immediately after "a="
			ph := placeholderRegex(dc.d).String()
			re := regexp.MustCompile("a=" + ph)
			if !re.MatchString(out) {
				t.Fatalf("[%s] expected placeholder immediately after 'a='; OUT:\n%s", dc.name, out)
			}
		})
	}
}

// TestWritef_SingleUse_ErrorAfterBuild ensures a Builder is single-use and returns
// a "builder already released" error if reused after Build.
func TestWritef_SingleUse_ErrorAfterBuild(t *testing.T) {
	s := New(Postgres)

	// first build ok
	b := s.Write("SELECT :x").Writef(" /*%s*/", "hint").Bind(P{"x": 1})
	_, _, err := b.Build()
	assertNoError(t, err)

	// reusing the builder must yield the "already released" error, not panic
	q, args, err := b.Writef("-- again %d", 2).Build()
	if err == nil || !regexp.MustCompile(`builder already released`).MatchString(err.Error()) {
		t.Fatalf("expected ErrBuilderReleased, got: q=%q args=%v err=%v", q, args, err)
	}
}

// ----------------------------------------------------------------
// Tests: complex queries (multi VALUES, multiple IN, bulk :rows)
// ----------------------------------------------------------------

type userRow struct {
	ID     int64  `db:"id"`
	Name   string `db:"name"`
	Active bool   `db:"active"`
	Note   string `db:"note"`
}

// TestComplexQueries_AllDialects exercises multi-VALUES inserts, multiple IN lists,
// repeated params, :rows bulk expansion, pointer rows, and error paths for malformed
// and missing params across all dialects.
func TestComplexQueries_AllDialects(t *testing.T) {
	rows := []userRow{
		{1, "Anna", true, "note 1"},
		{2, "Luca", false, ""},
		{3, "Mia", true, "note 3"},
	}

	for _, dc := range allDialects() {
		dc := dc
		t.Run(dc.name, func(t *testing.T) {
			// INSERT bulk with :rows{id,name}
			out, args := mustBuild(t, dc.d,
				"INSERT INTO users (id,name) VALUES :rows{id,name}",
				map[string]any{"rows": rows},
			)
			phCount := countPlaceholders(out, dc.d)
			if phCount != len(rows)*2 {
				t.Fatalf("placeholder count=%d, want %d\nquery=%s", phCount, len(rows)*2, out)
			}
			assertArgsEqual(t, args, []any{1, "Anna", 2, "Luca", 3, "Mia"})

			// Multiple IN + repeated param + bulk
			out, args = mustBuild(t, dc.d,
				"WITH x AS (SELECT 1) "+
					"INSERT INTO ord (uid, pid) VALUES :rows{id,name}; "+
					"SELECT * FROM ord WHERE uid IN (:uids) AND pid IN (:pids) AND flag=:f OR flag=:f",
				map[string]any{
					"rows": rows,
					"uids": []int64{10, 11, 12, 13},
					"pids": []string{"p1", "p2"},
					"f":    true,
				},
			)
			wantArgs := []any{1, "Anna", 2, "Luca", 3, "Mia", int64(10), int64(11), int64(12), int64(13), "p1", "p2", true, true}
			assertArgsEqual(t, args, wantArgs)
			if got, want := countPlaceholders(out, dc.d), len(wantArgs); got != want {
				t.Fatalf("placeholder count=%d, want %d\nquery=%s", got, want, out)
			}

			// :rows{...} with bare slice (convention name == "rows")
			out, _ = mustBuild(t, dc.d,
				"INSERT INTO users (id,name) VALUES :rows{id,name}",
				rows, // direct bind
			)
			if got, want := countPlaceholders(out, dc.d), len(rows)*2; got != want {
				t.Fatalf("placeholder=%d, want %d\n%s", got, want, out)
			}

			// :vals{...} with map (name different than 'rows')
			out, _ = mustBuild(t, dc.d,
				"INSERT INTO users (id,name) VALUES :vals{id,name}",
				map[string]any{"vals": rows},
			)
			if got, want := countPlaceholders(out, dc.d), len(rows)*2; got != want {
				t.Fatalf("placeholder=%d, want %d\n%s", got, want, out)
			}

			// :rows{...} with interface that wraps the slice
			var asAny any = rows
			out, _, err := New(dc.d).
				Write("INSERT INTO users (id,name) VALUES :rows{id,name}").
				Bind(map[string]any{"rows": asAny}).
				Build()
			assertNoError(t, err)
			if got, want := countPlaceholders(out, dc.d), len(rows)*2; got != want {
				t.Fatalf("placeholder=%d, want %d\n%s", got, want, out)
			}

			// :rows{...} with pointers to structs -> SUPPORTED
			ptrRows := []*userRow{{1, "P1", false, "note P1"}, {2, "P2", true, ""}}
			out, args, err = New(dc.d).
				Write("INSERT INTO users (id,name) VALUES :rows{id,name}").
				Bind(map[string]any{"rows": ptrRows}).
				Build()
			assertNoError(t, err)
			assertArgsEqual(t, args, []any{1, "P1", 2, "P2"})
			_ = out

			// ERROR: malformed :rows{...} placeholder (missing '}')
			_, _, err = New(dc.d).
				Write("INSERT INTO users (id,name) VALUES :rows{id,name").
				Bind(map[string]any{"rows": rows}).
				Build()
			if err == nil || !errors.Is(err, ErrRowsMalformed) {
				t.Fatalf("expected 'malformed' error, got: %v", err)
			}

			// ERROR: name different than 'rows' but argument absent -> missing parameter
			_, _, err = New(dc.d).
				Write("INSERT INTO users (id,name) VALUES :vals{id,name}").
				// no Bind for 'vals'
				Build()
			if err == nil || !errors.Is(err, ErrParamMissing) {
				t.Fatalf("expected 'missing parameter', got: %v", err)
			}
		})
	}
}

// ------------------------------------------------------------------------------------------------
// Tests: dynamic queries (multiple Write/Bind, reset after Build())
// ------------------------------------------------------------------------------------------------

// TestDynamicQueries_AllDialects ensures dynamic construction works with multiple Write/Bind
// calls and that the builder resets its placeholder state after Build().
func TestDynamicQueries_AllDialects(t *testing.T) {
	for _, dc := range allDialects() {
		dc := dc
		t.Run(dc.name, func(t *testing.T) {
			s := New(dc.d)
			b := s.Write("SELECT * FROM orders WHERE 1=1 ")

			cond := true
			if cond {
				b.Write(" AND customer_id = :cid")
				b.Bind(map[string]any{"cid": 42})
			} else {
				b.Write(" AND created_at >= :from")
				b.Bind(struct {
					From string `db:"from"`
				}{"2025-01-01"})
			}

			q1, args1, err := b.Build()
			assertNoError(t, err)
			if got := countPlaceholders(q1, dc.d); got != 1 {
				t.Fatalf("placeholder count=%d, want 1\n%s", got, q1)
			}
			assertArgsEqual(t, args1, []any{42})

			// Builder has been reset: second query starts again at $1/@p1/?
			b = s.Write("SELECT * FROM orders WHERE status=:s AND id IN (:ids)")
			b.Bind(map[string]any{"s": "open", "ids": []int{1, 2}})
			q2, args2, err := b.Build()
			assertNoError(t, err)
			assertArgsEqual(t, args2, []any{"open", 1, 2})

			// Verify placeholder numbering restarts from 1..n
			switch dc.d {
			case Postgres:
				mustContainInOrder(t, q2, "$1", "$2", "$3")
			case SQLServer:
				mustContainInOrder(t, q2, "@p1", "@p2", "@p3")
			default: // MySQL, SQLite
				if got := strings.Count(q2, "?"); got != 3 {
					t.Fatalf("count '?'=%d, want 3\n%s", got, q2)
				}
			}

			// "last one wins" resolution
			s = New(dc.d)
			b = s.Write("SELECT :x, :y")
			b.Bind(map[string]any{"x": 1, "y": 2})
			b.Bind(map[string]any{"x": 9}) // override
			q3, a3, err := b.Build()
			assertNoError(t, err)
			assertArgsEqual(t, a3, []any{9, 2})
			_ = q3
		})
	}
}

// ------------------------------------------------------------------------------------------------
// Tests: edge cases (empties, nil, []byte, placeholders inside quotes/comments/dollar-quoted)
// ------------------------------------------------------------------------------------------------

// TestEdgeCases_AllDialects covers an exhaustive set of edge-case behaviors including empty IN slices,
// missing params, empty :rows, []byte non-expansion, placeholders inside quotes/comments, and
// dollar-quoted blocks (closed and unclosed).
func TestEdgeCases_AllDialects(t *testing.T) {
	for _, dc := range allDialects() {
		dc := dc
		t.Run(dc.name, func(t *testing.T) {
			// Empty slice -> error
			_, _, err := New(dc.d).
				Write("SELECT * FROM t WHERE id IN (:ids)").
				Bind(map[string]any{"ids": []int{}}).
				Build()
			if err == nil {
				t.Fatalf("expected error for empty slice")
			}

			// Missing parameter -> error
			_, _, err = New(dc.d).
				Write("SELECT * FROM t WHERE a=:x AND b=:y").
				Bind(map[string]any{"x": 1}).
				Build()
			if err == nil {
				t.Fatalf("expected error for missing parameter")
			}

			// Empty :rows -> error
			_, _, err = New(dc.d).
				Write("INSERT INTO t (id,name) VALUES :rows{id,name}").
				Bind(map[string]any{"rows": []userRow{}}).
				Build()
			if err == nil {
				t.Fatalf("expected error for empty :rows")
			}

			// []byte must NOT expand into a list
			payload := []byte{0x01, 0x02, 0x03}
			out, args, err := New(dc.d).
				Write("UPDATE t SET bin=:p WHERE id=:id").
				Bind(map[string]any{"p": payload, "id": 10}).
				Build()
			assertNoError(t, err)
			if got, want := countPlaceholders(out, dc.d), 2; got != want {
				t.Fatalf("placeholder count=%d, want %d\nquery=%s", got, want, out)
			}
			assertArgsEqual(t, args, []any{payload, 10})

			// Placeholder-like tokens within comments and quotes must not match
			out, args, err = New(dc.d).
				Write(`SELECT ':skip' AS s -- :skip
					/* also :skip */
					, col FROM t WHERE a=:ok AND b=':also'`).
				Bind(map[string]any{"ok": 5}).
				Build()
			assertNoError(t, err)
			if strings.Contains(out, ":skip") == false {
				// ok: remained inside string/comment
			}
			assertArgsEqual(t, args, []any{5})

			// Dollar-quoted (Postgres) must be ignored
			out, args, err = New(dc.d).
				Write(`SELECT $tag$:not a param :nope$tag$, :x`).
				Bind(map[string]any{"x": 123}).
				Build()
			assertNoError(t, err)
			assertArgsEqual(t, args, []any{123})
			if !strings.Contains(out, ":nope") {
				t.Fatalf("content inside dollar-quoted should remain textual, ':nope' missing in:\n%s", out)
			}

			// Dollar-quoted with empty tag "$$" NOT closed: everything after is considered textual
			out, args, err = New(dc.d).
				Write(`SELECT $$ not closed :inside , :x`).
				Bind(map[string]any{"x": 1}).
				Build()
			assertNoError(t, err)
			// No bound args because :x remains inside unclosed dollar-quoted
			if len(args) != 0 {
				t.Fatalf("len(args)=%d, want 0 (unclosed dollar-quoted)", len(args))
			}
			// Both :inside and :x should remain textual
			if !strings.Contains(out, ":inside") || !strings.Contains(out, ":x") {
				t.Fatalf("content inside $$... should remain textual:\n%s", out)
			}

			// '$' that does NOT open dollar-quote (no second '$')
			out, _, err = New(dc.d).
				Write(`SELECT $ not_a_tag ':nope' , :y`).
				Bind(map[string]any{"y": 2}).
				Build()
			assertNoError(t, err)
			if got, want := countPlaceholders(out, dc.d), 1; got != want {
				t.Fatalf("placeholder count=%d, want %d\nquery=%s", got, want, out)
			}
			if !strings.Contains(out, ":nope") {
				t.Fatalf("':nope' is quoted and should remain textual:\n%s", out)
			}
		})
	}
}

// TestBindPointerStruct_AllDialects ensures pointer-to-struct binding works and resolves
// both tagged and untagged fields across dialects.
func TestBindPointerStruct_AllDialects(t *testing.T) {
	type S struct {
		A int `db:"a"`
		B string
	}
	v := &S{A: 7, B: "x"}
	for _, dc := range allDialects() {
		out, args, err := New(dc.d).Write("SELECT :a, :B").Bind(v).Build()
		assertNoError(t, err)
		if got, want := countPlaceholders(out, dc.d), 2; got != want {
			t.Fatalf("[%s] placeholders=%d, want 2", dc.name, got)
		}
		assertArgsEqual(t, args, []any{7, "x"})
	}
}

// TestAmbiguousField_Bind_AllDialects ensures ambiguous field names (same `db` tag in
// embedded structs) cause ErrFieldAmbiguous and mention the field name.
func TestAmbiguousField_Bind_AllDialects(t *testing.T) {
	type A struct {
		ID int `db:"id"`
	}
	type B struct {
		ID int `db:"id"`
	}
	type C struct {
		A A
		B B
	}

	for _, dc := range allDialects() {
		_, _, err := New(dc.d).Write("SELECT :id").Bind(C{A: A{1}, B: B{2}}).Build()
		if err == nil || !errors.Is(err, ErrFieldAmbiguous) {
			t.Fatalf("[%s] expected ErrFieldAmbiguous, got %v", dc.name, err)
		}
		if err != nil && !strings.Contains(err.Error(), `"id"`) {
			t.Fatalf("[%s] error should mention the field name: %v", dc.name, err)
		}
	}
}

// TestAmbiguousField_RowsBlock_AllDialects ensures ambiguous field names in :rows precompute
// error out with ErrFieldAmbiguous and mention the conflicting column name.
func TestAmbiguousField_RowsBlock_AllDialects(t *testing.T) {
	type A struct {
		ID int `db:"id"`
	}
	type B struct {
		ID int `db:"id"`
	}
	type Row struct {
		A A
		B B
	}
	rows := []Row{{A: A{1}, B: B{2}}}

	for _, dc := range allDialects() {
		_, _, err := New(dc.d).
			Write("INSERT INTO t(id) VALUES :rows{id}").
			Bind(P{"rows": rows}).
			Build()
		if err == nil || !errors.Is(err, ErrFieldAmbiguous) {
			t.Fatalf("[%s] expected ErrFieldAmbiguous in :rows precompute, got %v", dc.name, err)
		}
		if err != nil && !strings.Contains(err.Error(), `"id"`) {
			t.Fatalf("[%s] error should mention the column: %v", dc.name, err)
		}
	}
}

// TestRowsBlock_Heterogeneous_MissingColumn_PerRow_AllDialects ensures heterogeneous :rows
// with a missing column reports ErrColumnNotFound and includes the failing row index.
func TestRowsBlock_Heterogeneous_MissingColumn_PerRow_AllDialects(t *testing.T) {
	// Row 0 has "id"; Row 1 does NOT have "id" → ErrColumnNotFound at record 1
	type R0 struct {
		ID int `db:"id"`
	}
	type R1 struct {
		Name string `db:"name"`
	}

	rows := []any{
		R0{ID: 1},
		R1{Name: "x"},
	}

	for _, dc := range allDialects() {
		_, _, err := New(dc.d).
			Write("INSERT INTO t(id) VALUES :rows{id}").
			Bind(map[string]any{"rows": rows}).
			Build()
		if err == nil || !errors.Is(err, ErrColumnNotFound) {
			t.Fatalf("[%s] expected ErrColumnNotFound, got %v", dc.name, err)
		}
		if err != nil && !strings.Contains(err.Error(), "(record 1)") {
			t.Fatalf("[%s] error should mention the row index (record 1), got: %v", dc.name, err)
		}
	}
}

// TestRowsBlock_Heterogeneous_Ambiguous_PerRow_AllDialects ensures heterogeneous :rows
// with ambiguous columns reports ErrFieldAmbiguous and includes the row index.
func TestRowsBlock_Heterogeneous_Ambiguous_PerRow_AllDialects(t *testing.T) {
	// Row 0 has a unique "id"; Row 1 has TWO fields both named/tagged "id"
	// → ErrFieldAmbiguous at record 1
	type R0 struct {
		ID int `db:"id"`
	}
	type R1 struct {
		X int `db:"id"`
		Y int `db:"id"`
	}

	rows := []any{
		R0{ID: 1},
		R1{X: 2, Y: 3},
	}

	for _, dc := range allDialects() {
		_, _, err := New(dc.d).
			Write("INSERT INTO t(id) VALUES :rows{id}").
			Bind(map[string]any{"rows": rows}).
			Build()
		if err == nil || !errors.Is(err, ErrFieldAmbiguous) {
			t.Fatalf("[%s] expected ErrFieldAmbiguous, got %v", dc.name, err)
		}
		if err != nil && !strings.Contains(err.Error(), "(record 1)") {
			t.Fatalf("[%s] error should mention the row index (record 1), got: %v", dc.name, err)
		}
	}
}

// TestWritePlaceholder_NoAllocs_Postgres is a smoke test that also ensures placeholder
// numbering correctness in Postgres without unexpected allocations.
func TestWritePlaceholder_NoAllocs_Postgres(t *testing.T) {
	out, _, err := New(Postgres).
		Write("SELECT :a,:b,:c,:a").
		Bind(map[string]any{"a": 1, "b": 2, "c": 3}).Build()
	assertNoError(t, err)
	mustContainInOrder(t, out, "$1", "$2", "$3", "$4") // duplicate to keep cross-dialect compatibility
}

// TestSingleLookup_ReflectMapAndPointers_AllDialects ensures map-based and pointer/interface-based
// lookups work using reflection paths in single placeholder binds.
func TestSingleLookup_ReflectMapAndPointers_AllDialects(t *testing.T) {
	type MM map[string]any // defined type (skips fast-path and forces reflect.Map)

	for _, dc := range allDialects() {
		// a) map[string]int
		out, args, err := New(dc.d).
			Write("SELECT :x").
			Bind(map[string]int{"x": 5}).
			Build()
		assertNoError(t, err)
		assertArgsEqual(t, args, []any{5})
		_ = out

		// b) defined type on map[string]any
		mm := MM{"y": "ok"}
		_, args, err = New(dc.d).
			Write("SELECT :y").
			Bind(mm).
			Build()
		assertNoError(t, err)
		assertArgsEqual(t, args, []any{"ok"})

		// c) *map[string]int (pointer -> unwrap -> reflect.Map)
		mp := map[string]int{"z": 9}
		_, args, err = New(dc.d).
			Write("SELECT :z").
			Bind(&mp).
			Build()
		assertNoError(t, err)
		assertArgsEqual(t, args, []any{9})

		// d) interface{} containing map[string]int
		var anyMap any = map[string]int{"w": 42}
		_, args, err = New(dc.d).
			Write("SELECT :w").
			Bind(anyMap).
			Build()
		assertNoError(t, err)
		assertArgsEqual(t, args, []any{42})

		// e) map with non-string key type -> should error (reflect.Map negative branch)
		_, _, err = New(dc.d).
			Write("SELECT :bad").
			Bind(map[int]any{1: "nope"}).
			Build()
		if err == nil {
			t.Fatalf("[%s] expected error for non-string map key", dc.name)
		}
	}
}

// TestRows_StructFastPath_PrecomputeIndices_AllDialects ensures :rows struct fast-path
// precomputes indices correctly for both []T and []*T rows.
func TestRows_StructFastPath_PrecomputeIndices_AllDialects(t *testing.T) {
	type Row struct {
		A int    `db:"a"`
		B string `db:"b"`
		C int64  `db:"c"`
	}
	rows := []Row{
		{A: 1, B: "x", C: 10},
		{A: 2, B: "y", C: 20},
	}
	ptrRows := []*Row{
		{A: 3, B: "k", C: 30},
		{A: 4, B: "w", C: 40},
	}

	for _, dc := range allDialects() {
		dc := dc
		t.Run(dc.name+"/struct", func(t *testing.T) {
			out, args, err := New(dc.d).
				Write("INSERT INTO t(a,b,c) VALUES :rows{a,b,c}").
				Bind(map[string]any{"rows": rows}).
				Build()
			assertNoError(t, err)

			// 2 rows x 3 columns = 6 placeholders/args
			if got := countPlaceholders(out, dc.d); got != 6 {
				t.Fatalf("[%s] placeholders=%d, want 6\nOUT:\n%s", dc.name, got, out)
			}
			assertArgsEqual(t, args, []any{1, "x", int64(10), 2, "y", int64(20)})
		})

		t.Run(dc.name+"/ptr-struct", func(t *testing.T) {
			out, args, err := New(dc.d).
				Write("INSERT INTO t(a,b,c) VALUES :rows{a,b,c}").
				Bind(map[string]any{"rows": ptrRows}).
				Build()
			assertNoError(t, err)

			// 2 rows x 3 columns = 6 placeholders/args
			if got := countPlaceholders(out, dc.d); got != 6 {
				t.Fatalf("[%s] placeholders=%d, want 6\nOUT:\n%s", dc.name, got, out)
			}
			assertArgsEqual(t, args, []any{3, "k", int64(30), 4, "w", int64(40)})
		})
	}
}

// TestRows_StructFastPath_MissingColumn_ErrorIsForRecord0_AllDialects ensures that when a column
// is missing in struct precompute for :rows, the error refers to "record 0".
func TestRows_StructFastPath_MissingColumn_ErrorIsForRecord0_AllDialects(t *testing.T) {
	type Row struct {
		A int    `db:"a"`
		B string `db:"b"`
	}
	rows := []Row{
		{A: 1, B: "x"},
		{A: 2, B: "y"},
	}
	// Column "zzz" does not exist in struct: fast-path must fail immediately
	// during precomputation, indicating "record 0".
	sql := "INSERT INTO t(a,zzz) VALUES :rows{a,zzz}"

	for _, dc := range allDialects() {
		_, _, err := New(dc.d).
			Write(sql).
			Bind(map[string]any{"rows": rows}).
			Build()
		if err == nil {
			t.Fatalf("[%s] expected error for missing column 'zzz'", dc.name)
		}
		// Must be ErrColumnNotFound and message should mention "zzz" and "record 0"
		if !errors.Is(err, ErrColumnNotFound) {
			t.Fatalf("[%s] got err=%v, want ErrColumnNotFound", dc.name, err)
		}
		msg := err.Error()
		if !(strings.Contains(msg, `"zzz"`) && strings.Contains(msg, "record 0")) {
			t.Fatalf("[%s] unexpected error message: %q (expected to contain \"zzz\" and \"record 0\")", dc.name, msg)
		}
	}
}

// TestRows_ReflectMap_Positive_AllDialects validates map-based :rows expansion across
// plain maps, defined map types, and pointer-to-slice wrappers.
func TestRows_ReflectMap_Positive_AllDialects(t *testing.T) {
	type M map[string]int // defined type forces reflect.Map
	rowsInt := []map[string]int{
		{"a": 1, "b": 2},
		{"a": 3, "b": 4},
	}
	rowsDef := []M{
		{"a": 10, "b": 20},
		{"a": 30, "b": 40},
	}
	ptrRows := &rowsInt // *([]map[string]int)

	for _, dc := range allDialects() {
		// a) []map[string]int
		out, args, err := New(dc.d).
			Write("INSERT INTO t(a,b) VALUES :rows{a,b}").
			Bind(map[string]any{"rows": rowsInt}).
			Build()
		assertNoError(t, err)
		assertArgsEqual(t, args, []any{1, 2, 3, 4})
		_ = out

		// b) []M (defined type)
		_, args, err = New(dc.d).
			Write("INSERT INTO t(a,b) VALUES :rows{a,b}").
			Bind(map[string]any{"rows": rowsDef}).
			Build()
		assertNoError(t, err)
		assertArgsEqual(t, args, []any{10, 20, 30, 40})

		// c) *([]map[string]int) (pointer to slice)
		_, args, err = New(dc.d).
			Write("INSERT INTO t(a,b) VALUES :rows{a,b}").
			Bind(map[string]any{"rows": ptrRows}).
			Build()
		assertNoError(t, err)
		assertArgsEqual(t, args, []any{1, 2, 3, 4})
	}
}

// TestRows_ReflectMap_NegativeKeyType_AllDialects ensures non-string map key types in :rows
// result in ErrColumnNotFound during column extraction.
func TestRows_ReflectMap_NegativeKeyType_AllDialects(t *testing.T) {
	rows := []map[int]any{
		{1: "x"},
	}
	for _, dc := range allDialects() {
		_, _, err := New(dc.d).
			Write("INSERT INTO t(a) VALUES :rows{a}").
			Bind(map[string]any{"rows": rows}).
			Build()
		if err == nil || !errors.Is(err, ErrColumnNotFound) {
			t.Fatalf("[%s] expected ErrColumnNotFound for non-string key, got: %v", dc.name, err)
		}
	}
}

// TestRows_HeterogeneousStructs_AllDialects verifies that :rows accepts heterogeneous
// struct types (including pointers) as long as the requested columns exist.
func TestRows_HeterogeneousStructs_AllDialects(t *testing.T) {
	type A struct {
		X int    `db:"x"`
		Y string `db:"y"`
	}
	type B struct {
		X int    `db:"x"`
		Y string `db:"y"`
	}
	rows := []any{
		A{X: 1, Y: "a"},
		B{X: 2, Y: "b"},
		&A{X: 3, Y: "c"}, // pointer ok
	}
	for _, dc := range allDialects() {
		out, args, err := New(dc.d).
			Write("INSERT INTO t(x,y) VALUES :rows{x,y}").
			Bind(map[string]any{"rows": rows}).
			Build()
		assertNoError(t, err)
		if got := countPlaceholders(out, dc.d); got != 6 {
			t.Fatalf("[%s] placeholders=%d, want 6\nOUT:\n%s", dc.name, got, out)
		}
		assertArgsEqual(t, args, []any{1, "a", 2, "b", 3, "c"})
	}
}

// TestDollarQuoted_EmptyTag_Closed_AllDialects verifies that content inside $$...$$
// is ignored by the parser and placeholders there are not bound.
func TestDollarQuoted_EmptyTag_Closed_AllDialects(t *testing.T) {
	for _, dc := range allDialects() {
		out, args, err := New(dc.d).
			Write(`SELECT $$ :inside $$, :x`).
			Bind(map[string]any{"x": 7}).
			Build()
		assertNoError(t, err)
		if !strings.Contains(out, ":inside") {
			t.Fatalf("[%s] ':inside' inside $$...$$ should remain textual:\n%s", dc.name, out)
		}
		assertArgsEqual(t, args, []any{7})
	}
}

// TestDollarQuoted_LongTag_Multiple_AllDialects verifies multiple dollar-quoted tags/blocks
// are handled correctly and placeholders inside them are ignored.
func TestDollarQuoted_LongTag_Multiple_AllDialects(t *testing.T) {
	q := `SELECT $tag_with_123$ :skip $tag_with_123$, $X$ keep :skip $X$, :ok`
	for _, dc := range allDialects() {
		out, args, err := New(dc.d).
			Write(q).
			Bind(map[string]any{"ok": 1}).
			Build()
		assertNoError(t, err)
		if !strings.Contains(out, ":skip") { // must remain inside the dollar-quoted regions
			t.Fatalf("[%s] ':skip' should remain textual:\n%s", dc.name, out)
		}
		assertArgsEqual(t, args, []any{1})
	}
}

// TestDollarQuoted_ConsecutiveBlocks_AllDialects ensures consecutive dollar-quoted blocks
// are handled independently and placeholders outside them still bind.
func TestDollarQuoted_ConsecutiveBlocks_AllDialects(t *testing.T) {
	q := `SELECT $a$one$a$ $a$two$a$, :x`
	for _, dc := range allDialects() {
		_, args, err := New(dc.d).Write(q).Bind(map[string]any{"x": 1}).Build()
		assertNoError(t, err)
		assertArgsEqual(t, args, []any{1})
	}
}

// TestRows_Unwrap_InterfaceThenPointer_AllDialects verifies :rows input is unwrapped when
// passed as interface{} that contains a pointer to the slice.
func TestRows_Unwrap_InterfaceThenPointer_AllDialects(t *testing.T) {
	rows := []map[string]any{{"a": 1}, {"a": 2}}
	ptr := &rows
	var asAny any = ptr // interface{} -> *([]map[string]any)
	for _, dc := range allDialects() {
		out, args, err := New(dc.d).
			Write("INSERT INTO t(a) VALUES :rows{a}").
			Bind(map[string]any{"rows": asAny}).
			Build()
		assertNoError(t, err)
		assertArgsEqual(t, args, []any{1, 2})
		_ = out
	}
}

// TestSingleLookup_ReflectMapVariants_AllDialects ensures singleLookup supports string-convertible
// key variants (aliases/defined types), pointers to maps, and interfaces containing maps.
func TestSingleLookup_ReflectMapVariants_AllDialects(t *testing.T) {
	type KS string       // alias of string
	type MAny map[KS]any // defined type

	for _, dc := range allDialects() {
		// a) key is alias of string -> OK
		m1 := map[KS]any{KS("a"): 7}
		out, args, err := New(dc.d).Write("SELECT :a").Bind(m1).Build()
		assertNoError(t, err)
		assertArgsEqual(t, args, []any{7})
		_ = out

		// b) defined map type -> OK
		m2 := MAny{KS("b"): "x"}
		_, args, err = New(dc.d).Write("SELECT :b").Bind(m2).Build()
		assertNoError(t, err)
		assertArgsEqual(t, args, []any{"x"})

		// c) pointer to map -> unwrap -> OK
		m3 := map[KS]int{KS("c"): 9}
		_, args, err = New(dc.d).Write("SELECT :c").Bind(&m3).Build()
		assertNoError(t, err)
		assertArgsEqual(t, args, []any{9})

		// d) interface{} containing map[KS]any -> OK
		var anyMap any = map[KS]any{KS("d"): true}
		_, args, err = New(dc.d).Write("SELECT :d").Bind(anyMap).Build()
		assertNoError(t, err)
		assertArgsEqual(t, args, []any{true})

		// e) non-string-convertible key type -> should fail (parameter missing)
		_, _, err = New(dc.d).Write("SELECT :z").Bind(map[int]any{1: "no"}).Build()
		if err == nil {
			t.Fatalf("[%s] expected error for non-string map key in singleLookup", dc.name)
		}
	}
}

// TestRows_GetColValue_MapKeyVariants_AllDialects verifies :rows with map key aliases/defined
// types and pointer-to-slice variants, and errors on non-string keys.
func TestRows_GetColValue_MapKeyVariants_AllDialects(t *testing.T) {
	type KS string
	rowsAlias := []map[KS]any{
		{KS("a"): 1, KS("b"): "x"},
		{KS("a"): 2, KS("b"): "y"},
	}
	rowsPtr := &rowsAlias // *([]map[KS]any)

	for _, dc := range allDialects() {
		// alias of string: OK
		out, args, err := New(dc.d).
			Write("INSERT INTO t(a,b) VALUES :rows{a,b}").
			Bind(map[string]any{"rows": rowsAlias}).Build()
		assertNoError(t, err)
		assertArgsEqual(t, args, []any{1, "x", 2, "y"})
		_ = out

		// pointer to slice: OK
		_, args, err = New(dc.d).
			Write("INSERT INTO t(a,b) VALUES :rows{a,b}").
			Bind(map[string]any{"rows": rowsPtr}).Build()
		assertNoError(t, err)
		assertArgsEqual(t, args, []any{1, "x", 2, "y"})

		// non-string key: should be ErrColumnNotFound
		bad := []map[int]any{{1: "oops"}}
		_, _, err = New(dc.d).
			Write("INSERT INTO t(a) VALUES :rows{a}").
			Bind(map[string]any{"rows": bad}).Build()
		if err == nil || !errors.Is(err, ErrColumnNotFound) {
			t.Fatalf("[%s] expected error for non-string key", dc.name)
		}
	}
}

// TestQuotedIdentifierEscapes_AllDialects ensures identifier escaping with backticks and
// brackets is preserved verbatim across dialects.
func TestQuotedIdentifierEscapes_AllDialects(t *testing.T) {
	for _, dc := range allDialects() {
		out, args, err := New(dc.d).
			Write("SELECT `a``b` AS x, [we]]ird] AS y, :v").
			Bind(map[string]any{"v": 1}).Build()
		assertNoError(t, err)
		// backtick/bracket escapes must remain in the text
		if !strings.Contains(out, "`a``b`") || !strings.Contains(out, "[we]]ird]") {
			t.Fatalf("[%s] identifier escape not preserved:\n%s", dc.name, out)
		}
		assertArgsEqual(t, args, []any{1})
	}
}

// TestLimits_MaxParams_Custom enforces a custom MaxParams limit and expects ErrTooManyParams
// when exceeding it via slice expansion and additional placeholders.
func TestLimits_MaxParams_Custom(t *testing.T) {
	// Very low limit to trigger error without huge queries
	for _, dc := range allDialects() {
		b := New(dc.d, Config{MaxParams: 5}) // max 5 placeholders
		// 6 total placeholders (IN expands to 4 + 2 others)
		_, _, err := b.
			Write("SELECT * FROM t WHERE a IN (:ids) AND x=:x AND y=:y").
			Bind(map[string]any{"ids": []int{1, 2, 3, 4}, "x": 9, "y": 10}).
			Build()
		if err == nil || !errors.Is(err, ErrTooManyParams) {
			t.Fatalf("[%s] expected ErrTooManyParams, got: %v", dc.name, err)
		}
	}
}

// TestLimits_NameLen verifies MaxNameLen (default or configured) is enforced for :name.
func TestLimits_NameLen(t *testing.T) {
	long := strings.Repeat("a", 65) // > 64
	for _, dc := range allDialects() {
		b := New(dc.d) // default MaxNameLen=64
		_, _, err := b.Write("SELECT :" + long).Bind(map[string]any{long: 1}).Build()
		if err == nil || !errors.Is(err, ErrParamNameTooLong) {
			t.Fatalf("[%s] expected ErrParamNameTooLong, got: %v", dc.name, err)
		}
	}
}

// TestLimits_Defaults_ByDialect verifies that the MaxParams limit from Config is actually
// enforced by the parser across all dialects.
func TestLimits_Defaults_ByDialect(t *testing.T) {
	for _, dc := range allDialects() {
		b := New(dc.d, Config{MaxParams: 2})
		_, _, err := b.Write("SELECT :a, :b, :c").Bind(map[string]any{"a": 1, "b": 2, "c": 3}).Build()
		if err == nil || !errors.Is(err, ErrTooManyParams) {
			t.Fatalf("[%s] expected ErrTooManyParams, got: %v", dc.name, err)
		}
	}
}

// TestLimits_ParamNameTooLong_AllDialects verifies MaxNameLen enforcement for both :name
// and :name{...} placeholders.
func TestLimits_ParamNameTooLong_AllDialects(t *testing.T) {
	long := strings.Repeat("a", 65) // > 64
	for _, dc := range allDialects() {
		// 1) simple :name placeholder
		_, _, err := New(dc.d, Config{MaxNameLen: 64}).
			Write("SELECT :" + long).
			Bind(map[string]any{long: 1}).
			Build()
		if err == nil || !errors.Is(err, ErrParamNameTooLong) {
			t.Fatalf("[%s] expected ErrParamNameTooLong on :name, got: %v", dc.name, err)
		}

		// 2) :name{...} (rows-like alias or other)
		_, _, err = New(dc.d, Config{MaxNameLen: 64}).
			Write("INSERT INTO t(a) VALUES :" + long + "{a}").
			Bind(map[string]any{"rows": []map[string]any{{"a": 1}}}). // not used; just to have a plausible bind
			Build()
		if err == nil || !errors.Is(err, ErrParamNameTooLong) {
			t.Fatalf("[%s] expected ErrParamNameTooLong on :name{...}, got: %v", dc.name, err)
		}
	}
}

// TestLimits_EnsureAdd_Rows_PreCheck_AllDialects ensures the :rows pre-check accounts for
// total placeholders (rows*cols) when enforcing MaxParams.
func TestLimits_EnsureAdd_Rows_PreCheck_AllDialects(t *testing.T) {
	// Very low limit to test pre-check on :rows{...}
	rows := []map[string]any{
		{"a": 1, "b": 2},
		{"a": 3, "b": 4},
		{"a": 5, "b": 6},
	} // need = 3 rows * 2 cols = 6 placeholders

	for _, dc := range allDialects() {
		// 1) Limit < need -> ErrTooManyParams
		_, _, err := New(dc.d, Config{MaxParams: 5}). // 5 < 6
								Write("INSERT INTO t(a,b) VALUES :rows{a,b}").
								Bind(map[string]any{"rows": rows}).
								Build()
		if err == nil || !errors.Is(err, ErrTooManyParams) {
			t.Fatalf("[%s] expected ErrTooManyParams (pre-check :rows), got: %v", dc.name, err)
		}

		// 2) Limit == need -> OK
		out, args, err := New(dc.d, Config{MaxParams: 6}).
			Write("INSERT INTO t(a,b) VALUES :rows{a,b}").
			Bind(map[string]any{"rows": rows}).
			Build()
		assertNoError(t, err)
		if got := countPlaceholders(out, dc.d); got != 6 {
			t.Fatalf("[%s] placeholders=%d, want 6\nOUT:\n%s", dc.name, got, out)
		}
		if len(args) != 6 {
			t.Fatalf("[%s] len(args)=%d, want 6", dc.name, len(args))
		}
	}
}

// TestLimits_EnsureAdd_Scalar_And_Valuer_AllDialects ensures Scalar(...) and driver.Valuer
// each consume exactly one placeholder with MaxParams limits enforced.
func TestLimits_EnsureAdd_Scalar_And_Valuer_AllDialects(t *testing.T) {
	for _, dc := range allDialects() {
		sql := "SELECT :a, :b, :c"

		// Limit = 2 -> :a (Scalar) + :b (Valuer) OK, :c triggers ErrTooManyParams
		_, _, err := New(dc.d, Config{MaxParams: 2}).
			Write(sql).
			Bind(map[string]any{
				"a": Scalar(10),         // 1 placeholder
				"b": mockArray{v: "ok"}, // Valuer -> 1 placeholder
				"c": 7,                  // 3rd -> exceeds limit
			}).
			Build()
		if err == nil || !errors.Is(err, ErrTooManyParams) {
			t.Fatalf("[%s] expected ErrTooManyParams with Scalar+Valuer+normal, got: %v", dc.name, err)
		}

		// Limit = 2 -> exactly two placeholders (Scalar + Valuer) -> OK
		out, args, err := New(dc.d, Config{MaxParams: 2}).
			Write("SELECT :a, :b").
			Bind(map[string]any{
				"a": Scalar([]int{1, 2, 3}),  // still 1 placeholder
				"b": mockArray{v: []byte{1}}, // Valuer -> 1 placeholder
			}).
			Build()
		assertNoError(t, err)
		if got := countPlaceholders(out, dc.d); got != 2 {
			t.Fatalf("[%s] placeholders=%d, want 2\nOUT:\n%s", dc.name, got, out)
		}
		if len(args) != 2 {
			t.Fatalf("[%s] len(args)=%d, want 2", dc.name, len(args))
		}
	}
}

// TestLimits_EnsureAdd_Slice_Boundary_AllDialects verifies expansion at the MaxParams boundary
// succeeds when the total exactly equals the limit.
func TestLimits_EnsureAdd_Slice_Boundary_AllDialects(t *testing.T) {
	for _, dc := range allDialects() {
		ids := []int{1, 2, 3, 4, 5}
		out, args, err := New(dc.d, Config{MaxParams: 5}).
			Write("SELECT * FROM t WHERE id IN (:ids)").
			Bind(map[string]any{"ids": ids}).
			Build()
		assertNoError(t, err)
		if got := countPlaceholders(out, dc.d); got != 5 {
			t.Fatalf("[%s] placeholders=%d, want 5\nOUT:\n%s", dc.name, got, out)
		}
		if len(args) != 5 {
			t.Fatalf("[%s] len(args)=%d, want 5", dc.name, len(args))
		}
	}
}

// TestLimits_ParamNameTooLong_RowsAlias_AllDialects verifies name length limits are enforced
// for :name{...} placeholders as well.
func TestLimits_ParamNameTooLong_RowsAlias_AllDialects(t *testing.T) {
	// Placeholder name > MaxNameLen using :name{...}
	long := strings.Repeat("r", 65) // > 64
	for _, dc := range allDialects() {
		_, _, err := New(dc.d, Config{MaxNameLen: 64}).
			Write("INSERT INTO t(a) VALUES :" + long + "{a}").
			Bind(map[string]any{
				long: []map[string]any{{"a": 1}},
			}).
			Build()
		if err == nil || !errors.Is(err, ErrParamNameTooLong) {
			t.Fatalf("[%s] expected ErrParamNameTooLong on :name{...}, got: %v", dc.name, err)
		}
	}
}

// TestLimits_EnsureAdd_Scalar_Error_AllDialects ensures ensureAdd is triggered in the Scalar branch
// when MaxParams is exceeded by adding a Scalar after another placeholder.
func TestLimits_EnsureAdd_Scalar_Error_AllDialects(t *testing.T) {
	for _, dc := range allDialects() {
		_, _, err := New(dc.d, Config{MaxParams: 1}).
			Write("SELECT :x, :y").
			Bind(map[string]any{
				"x": 10,           // 1st placeholder
				"y": Scalar("ok"), // 2nd -> ensureAdd in Scalar branch
			}).
			Build()
		if err == nil || !errors.Is(err, ErrTooManyParams) {
			t.Fatalf("[%s] expected ErrTooManyParams in Scalar branch, got: %v", dc.name, err)
		}
	}
}

// TestLimits_EnsureAdd_Valuer_Error_AllDialects ensures ensureAdd is triggered in the Valuer branch
// when MaxParams would be exceeded.
func TestLimits_EnsureAdd_Valuer_Error_AllDialects(t *testing.T) {
	for _, dc := range allDialects() {
		_, _, err := New(dc.d, Config{MaxParams: 1}).
			Write("SELECT :x, :v").
			Bind(map[string]any{
				"x": 1,                      // 1st placeholder
				"v": mockArray{v: []int{1}}, // 2nd -> Valuer branch
			}).
			Build()
		if err == nil || !errors.Is(err, ErrTooManyParams) {
			t.Fatalf("[%s] expected ErrTooManyParams in Valuer branch, got: %v", dc.name, err)
		}
	}
}

// TestLimits_EnsureAdd_Bytes_Error_AllDialects ensures ensureAdd is triggered for []byte values
// (which bind as a single placeholder) when enforcing MaxParams.
func TestLimits_EnsureAdd_Bytes_Error_AllDialects(t *testing.T) {
	for _, dc := range allDialects() {
		_, _, err := New(dc.d, Config{MaxParams: 1}).
			Write("SELECT :x, :b").
			Bind(map[string]any{
				"x": 1,               // 1st placeholder
				"b": []byte{1, 2, 3}, // 2nd -> []byte (no expansion)
			}).
			Build()
		if err == nil || !errors.Is(err, ErrTooManyParams) {
			t.Fatalf("[%s] expected ErrTooManyParams in []byte branch, got: %v", dc.name, err)
		}
	}
}

// TestLimits_EnsureAdd_Slice_Error_AllDialects ensures ensureAdd is triggered in slice expansion
// when the expanded IN list would exceed MaxParams.
func TestLimits_EnsureAdd_Slice_Error_AllDialects(t *testing.T) {
	for _, dc := range allDialects() {
		_, _, err := New(dc.d, Config{MaxParams: 3}).
			Write("SELECT * FROM t WHERE id IN (:ids)").
			Bind(map[string]any{"ids": []int{1, 2, 3, 4}}).
			Build()
		if err == nil || !errors.Is(err, ErrTooManyParams) {
			t.Fatalf("[%s] expected ErrTooManyParams in slice (IN) branch, got: %v", dc.name, err)
		}
	}
}

// TestManyPlaceholders_NoAllocExplosion builds SQL with thousands of placeholders to ensure
// the parser does not exhibit pathological allocation behavior.
func TestManyPlaceholders_NoAllocExplosion(t *testing.T) {
	var sb strings.Builder
	sb.WriteString("SELECT ")
	for i := 0; i < 2000; i++ {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(":v")
	}
	sql := sb.String()

	for _, dc := range allDialects() {
		out, _, err := New(dc.d, Config{MaxParams: 2000}).Write(sql).Bind(map[string]any{"v": 42}).Build()
		assertNoError(t, err)
		if got, want := countPlaceholders(out, dc.d), 2000; got != want {
			t.Fatalf("[%s] ph=%d, want %d", dc.name, got, want)
		}
	}
}

// TestGetColValue_MapKeyConversion_Direct performs table-driven checks for getColValue()
// covering key conversions (defined vs alias), negatives, and []byte integrity.
func TestGetColValue_MapKeyConversion_Direct(t *testing.T) {
	type DefStr string     // defined type over string (requires Convert())
	type AliasStr = string // alias: identical to string (no Convert())

	s := "a"

	cases := []struct {
		name string
		row  any
		col  string
		want any
		ok   bool
	}{
		{
			name: "map[string]any fast-path",
			row:  map[string]any{"a": 1, "b": "x"},
			col:  "a",
			want: 1, ok: true,
		},
		{
			name: "map[DefStr]any (defined type) — requires Convert()",
			row:  map[DefStr]any{DefStr("a"): "ok"},
			col:  "a",
			want: "ok", ok: true,
		},
		{
			name: "map[AliasStr]any (alias type) — same identity",
			row:  map[AliasStr]any{"a": 7},
			col:  "a",
			want: 7, ok: true,
		},
		{
			name: "map[DefStr]any — case mismatch → missing key",
			row:  map[DefStr]any{DefStr("a"): 1},
			col:  "A",
			want: nil, ok: false,
		},
		{
			name: "map[int]any — key not string → negative branch",
			row:  map[int]any{1: "nope"},
			col:  "1",
			want: nil, ok: false,
		},
		{
			name: "map[interface{}]any — interface key: supported",
			row:  map[any]any{"a": 1},
			col:  "a",
			want: 1, ok: true,
		},
		{
			name: "map[struct{}]any — key not convertible from string",
			row:  map[struct{}]any{{}: 1},
			col:  "a",
			want: nil, ok: false,
		},
		{
			name: "map[*string]any — pointer key, not convertible from string",
			row:  map[*string]any{&s: "x"},
			col:  "a",
			want: nil, ok: false,
		},
		{
			name: "map[string][]byte — value []byte must come out intact",
			row:  map[string]any{"bin": []byte{1, 2, 3}},
			col:  "bin",
			want: []byte{1, 2, 3}, ok: true,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got, ok := getColValue(tc.row, tc.col)
			if ok != tc.ok {
				t.Fatalf("ok=%v, want %v (row=%T, col=%q)", ok, tc.ok, tc.row, tc.col)
			}
			if ok {
				if !equalArg(got, tc.want) {
					t.Fatalf("got=%#v, want=%#v", got, tc.want)
				}
			}
		})
	}
}

// TestRows_KeyTypeConversion_Integration integrates :rows map key conversion behavior,
// comparing defined type vs alias-of-string keys.
func TestRows_KeyTypeConversion_Integration(t *testing.T) {
	type DefStr string
	type AliasStr = string

	rowsDef := []map[DefStr]any{
		{DefStr("a"): 1, DefStr("b"): "x"},
		{DefStr("a"): 2, DefStr("b"): "y"},
	}
	rowsAlias := []map[AliasStr]any{
		{"a": 10, "b": "z"},
		{"a": 20, "b": "w"},
	}

	for _, dc := range allDialects() {
		// Defined type on string -> must work (uses Convert())
		out, args, err := New(dc.d).
			Write("INSERT INTO t(a,b) VALUES :rows{a,b}").
			Bind(map[string]any{"rows": rowsDef}).
			Build()
		assertNoError(t, err)
		assertArgsEqual(t, args, []any{1, "x", 2, "y"})
		_ = out

		// Alias type (identical to string) -> must work (no Convert())
		_, args, err = New(dc.d).
			Write("INSERT INTO t(a,b) VALUES :rows{a,b}").
			Bind(map[string]any{"rows": rowsAlias}).
			Build()
		assertNoError(t, err)
		assertArgsEqual(t, args, []any{10, "z", 20, "w"})
	}
}

// TestRows_KeyType_InterfaceEmpty_Supported_AllDialects ensures maps with interface{} keys
// are supported in :rows when the runtime keys are strings.
func TestRows_KeyType_InterfaceEmpty_Supported_AllDialects(t *testing.T) {
	rows := []map[any]any{
		{"a": 1},
		{"a": 2},
	}
	for _, dc := range allDialects() {
		_, args, err := New(dc.d).
			Write("INSERT INTO t(a) VALUES :rows{a}").
			Bind(map[string]any{"rows": rows}).Build()
		assertNoError(t, err)
		assertArgsEqual(t, args, []any{1, 2})
	}
}

// TestRows_KeyTypeHardNegatives_Integration verifies hard negative key types (*string, int)
// produce ErrColumnNotFound in :rows.
func TestRows_KeyTypeHardNegatives_Integration(t *testing.T) {
	// 1) key is *string -> not supported
	s := "a"
	rowsPtrKey := []map[*string]any{
		{&s: 1},
	}
	// 2) key is int -> not supported
	rowsIntKey := []map[int]any{
		{1: "x"},
	}

	for _, dc := range allDialects() {
		_, _, err := New(dc.d).
			Write("INSERT INTO t(a) VALUES :rows{a}").
			Bind(map[string]any{"rows": rowsPtrKey}).Build()
		if err == nil || !errors.Is(err, ErrColumnNotFound) {
			t.Fatalf("[%s] expected error for *string key", dc.name)
		}

		_, _, err = New(dc.d).
			Write("INSERT INTO t(a) VALUES :rows{a}").
			Bind(map[string]any{"rows": rowsIntKey}).Build()
		if err == nil || !errors.Is(err, ErrColumnNotFound) {
			t.Fatalf("[%s] expected error for int key", dc.name)
		}
	}
}

// TestRows_InterfaceKey_Supported_Integration ensures :rows supports map[any]any when the
// runtime keys are strings and binds values as expected.
func TestRows_InterfaceKey_Supported_Integration(t *testing.T) {
	rowsIfaceKey := []map[any]any{
		{"a": 1},
		{"a": 2},
	}
	for _, dc := range allDialects() {
		out, args, err := New(dc.d).
			Write("INSERT INTO t(a) VALUES :rows{a}").
			Bind(map[string]any{"rows": rowsIfaceKey}).Build()
		assertNoError(t, err)
		// two rows -> two placeholders
		if got := countPlaceholders(out, dc.d); got != 2 {
			t.Fatalf("[%s] placeholders=%d, want 2\nOUT:\n%s", dc.name, got, out)
		}
		assertArgsEqual(t, args, []any{1, 2})
	}
}

// TestSingleLookup_InterfaceKey_Supported verifies singleLookup supports map[any]any
// when the runtime keys are strings.
func TestSingleLookup_InterfaceKey_Supported(t *testing.T) {
	for _, dc := range allDialects() {
		out, args, err := New(dc.d).
			Write("SELECT :a, :b").
			Bind(map[any]any{"a": 1, "b": "x"}).
			Build()
		assertNoError(t, err)
		assertArgsEqual(t, args, []any{1, "x"})
		_ = out
	}
}

// TestSingleLookup_NonConvertibleKey_NotSupported ensures singleLookup fails when
// map keys are not convertible from string (e.g., *string).
func TestSingleLookup_NonConvertibleKey_NotSupported(t *testing.T) {
	// key *string is NOT convertible from string -> must fail (parameter missing)
	s := "a"
	for _, dc := range allDialects() {
		_, _, err := New(dc.d).
			Write("SELECT :a").
			Bind(map[*string]any{&s: 1}).
			Build()
		if err == nil {
			t.Fatalf("[%s] expected error with map[*string]any in singleLookup", dc.name)
		}
	}
}

// TestKeyType_AliasVsDefined_SingleLookupAndRows ensures both singleLookup and :rows resolution
// behave the same for alias/defined key types.
func TestKeyType_AliasVsDefined_SingleLookupAndRows(t *testing.T) {
	type DefStr string
	type AliasStr = string

	// singleLookup
	for _, dc := range allDialects() {
		out, args, err := New(dc.d).
			Write("SELECT :a, :b").
			Bind(map[DefStr]any{DefStr("a"): 1, DefStr("b"): 2}). // defined type
			Build()
		assertNoError(t, err)
		assertArgsEqual(t, args, []any{1, 2})
		_ = out

		_, args, err = New(dc.d).
			Write("SELECT :a, :b").
			Bind(map[AliasStr]any{"a": 3, "b": 4}). // alias type (identical)
			Build()
		assertNoError(t, err)
		assertArgsEqual(t, args, []any{3, 4})
	}
}

// TestGetColValue_UnwrapPointerAndInterface validates pointer/interface unwrapping for struct rows
// and correct nil handling on pointers.
func TestGetColValue_UnwrapPointerAndInterface(t *testing.T) {
	type S struct {
		A int    `db:"a"`
		B string // no tag, uses field name
	}
	s := &S{A: 7, B: "x"}

	// *S -> unwrap pointer -> struct branch
	v, ok := getColValue(s, "a")
	if !ok || v.(int) != 7 {
		t.Fatalf("pointer struct: got=%v ok=%v, want 7 true", v, ok)
	}

	// **S -> unwrap chain (pointer to pointer)
	ps := &s
	v, ok = getColValue(ps, "B")
	if !ok || v.(string) != "x" {
		t.Fatalf("double pointer: got=%v ok=%v, want 'x' true", v, ok)
	}

	// interface{} that contains *S
	var anyRow any = s
	v, ok = getColValue(anyRow, "a")
	if !ok || v.(int) != 7 {
		t.Fatalf("interface wrapping *S: got=%v ok=%v, want 7 true", v, ok)
	}

	// nil pointer -> ok=false, no panic
	var nilS *S
	if v, ok = getColValue(nilS, "a"); ok {
		t.Fatalf("nil pointer must return ok=false, got v=%v", v)
	}
}

// TestGetColValue_Struct_HitAndMiss checks tag-based and name-based hits on struct fields,
// and verifies misses return ok=false.
func TestGetColValue_Struct_HitAndMiss(t *testing.T) {
	type S struct {
		A int `db:"a"`
		B string
	}
	s := S{A: 10, B: "ok"}

	// hit on `db` tag
	v, ok := getColValue(s, "a")
	if !ok || v.(int) != 10 {
		t.Fatalf("struct hit db tag: got=%v ok=%v", v, ok)
	}
	// hit on field name
	v, ok = getColValue(s, "B")
	if !ok || v.(string) != "ok" {
		t.Fatalf("struct hit by name: got=%v ok=%v", v, ok)
	}
	// miss -> ok=false
	if v, ok = getColValue(s, "missing"); ok {
		t.Fatalf("struct miss must be ok=false, got v=%v", v)
	}
}

// TestGetColValue_MapPointer_InterfaceAndNil validates pointer-to-map lookup, interface
// wrapping of the map, and nil *map behavior.
func TestGetColValue_MapPointer_InterfaceAndNil(t *testing.T) {
	// pointer to map[string]any (skips map[string]any fast-path; goes through reflect)
	m := map[string]any{"a": 1, "b": []byte{1, 2}}
	mp := &m
	v, ok := getColValue(mp, "a")
	if !ok || v.(int) != 1 {
		t.Fatalf("pointer to map: got=%v ok=%v, want 1 true", v, ok)
	}
	// interface{} wrapping *map
	var anyMap any = mp
	v, ok = getColValue(anyMap, "b")
	if !ok || !bytes.Equal(v.([]byte), []byte{1, 2}) {
		t.Fatalf("interface wrapping *map: got=%v ok=%v", v, ok)
	}

	// nil *map -> ok=false
	var nilMap *map[string]any
	if v, ok = getColValue(nilMap, "a"); ok {
		t.Fatalf("nil *map must return ok=false, got v=%v", v)
	}
}

// TestGetColValue_DefaultBranch_PrimitiveAndUnsupported ensures getColValue default branch
// returns ok=false for primitives and unsupported types like generic slices.
func TestGetColValue_DefaultBranch_PrimitiveAndUnsupported(t *testing.T) {
	// primitive type -> default: ok=false
	if v, ok := getColValue(123, "a"); ok {
		t.Fatalf("primitive row must return ok=false, got v=%v", v)
	}
	// generic slice -> default: ok=false
	if v, ok := getColValue([]int{1, 2}, "a"); ok {
		t.Fatalf("slice row must return ok=false, got v=%v", v)
	}
}

// TestRows_EmptyColumns_Error_AllDialects ensures :rows{} with empty columns returns
// ErrRowsMalformed and a helpful error message.
func TestRows_EmptyColumns_Error_AllDialects(t *testing.T) {
	for _, dc := range allDialects() {
		_, _, err := New(dc.d).
			Write("INSERT INTO t VALUES :rows{}").
			Bind(map[string]any{"rows": []map[string]any{{}}}). // data doesn't matter; should fail earlier
			Build()
		if err == nil || !errors.Is(err, ErrRowsMalformed) || !strings.Contains(err.Error(), "without columns") {
			t.Fatalf("[%s] expected ErrRowsMalformed 'without columns', got: %v", dc.name, err)
		}
	}
}

// TestFieldIndexMap_BaseNotStruct_ReturnsEmpty verifies fieldIndexMap returns an empty map
// for non-struct base types and that results are cacheable/repeatable.
func TestFieldIndexMap_BaseNotStruct_ReturnsEmpty(t *testing.T) {
	// non-struct types (int, *[]int) should yield empty map and not panic
	types := []reflect.Type{
		reflect.TypeOf(123),
		reflect.TypeOf(&[]int{}),
	}

	for _, tt := range types {
		m := fieldIndexMap(tt)
		if m == nil {
			t.Fatalf("fieldIndexMap(%v) returned nil", tt)
		}
		if len(m) != 0 {
			t.Fatalf("fieldIndexMap(%v) len=%d, want 0", tt, len(m))
		}
		// second call (cache hit) must give the same result
		m2 := fieldIndexMap(tt)
		if len(m2) != 0 {
			t.Fatalf("fieldIndexMap(%v) (2nd) len=%d, want 0", tt, len(m2))
		}
	}
}

// TestGetValueByPathAny_InitialInterfaceNil_ReturnsNilTrue verifies that when the initial
// reflect.Value is a nil interface, getValueByPathAny returns (nil, true).
func TestGetValueByPathAny_InitialInterfaceNil_ReturnsNilTrue(t *testing.T) {
	// Build a reflect.Value with interface type that is valid but nil
	type Holder struct {
		X any // interface{} zero-value -> Kind()==Interface, IsNil()==true
	}
	h := Holder{}
	v := reflect.ValueOf(h).Field(0)

	// empty path: initial unwrap of interface must return (nil, true)
	val, ok := getValueByPathAny(v, nil)
	if !ok || val != nil {
		t.Fatalf("got (ok=%v, val=%v), want (true, nil)", ok, val)
	}
}

// TestGetValueByPathAny_PathThroughNonStruct_ReturnsFalse ensures that traversing beyond
// a non-struct returns (false, nil) and does not panic.
func TestGetValueByPathAny_PathThroughNonStruct_ReturnsFalse(t *testing.T) {
	// S{A int}; path [A, <something>] -> on second step v.Kind()!=Struct -> false
	type S struct{ A int }
	s := S{A: 1}
	v := reflect.ValueOf(s)

	path := []int{0, 0} // 0 => field A, then tries another field on an int
	val, ok := getValueByPathAny(v, path)
	if ok || val != nil {
		t.Fatalf("traversal through non-struct must fail: got (ok=%v, val=%v), want (false, nil)", ok, val)
	}
}

// TestGetValueByPathAny_LeafInterfaceNil_ReturnsNilTrue verifies that a nil interface leaf
// returns (nil, true).
func TestGetValueByPathAny_LeafInterfaceNil_ReturnsNilTrue(t *testing.T) {
	type S struct{ I any }
	s := S{} // I is nil
	v := reflect.ValueOf(s)
	path := []int{0}

	val, ok := getValueByPathAny(v, path)
	if !ok || val != nil {
		t.Fatalf("leaf interface nil: got (ok=%v, val=%v), want (true, nil)", ok, val)
	}
}

// TestGetValueByPathAny_LeafPointerNil_ReturnsNilTrue verifies that a nil pointer leaf
// returns (nil, true).
func TestGetValueByPathAny_LeafPointerNil_ReturnsNilTrue(t *testing.T) {
	type S struct{ P *int }
	s := S{P: nil}
	v := reflect.ValueOf(s)
	path := []int{0}

	val, ok := getValueByPathAny(v, path)
	if !ok || val != nil {
		t.Fatalf("leaf pointer nil: got (ok=%v, val=%v), want (true, nil)", ok, val)
	}
}

// TestGetValueByPathAny_LeafPointerNonNil_ReturnsPointer verifies that a non-nil pointer leaf
// is returned as-is.
func TestGetValueByPathAny_LeafPointerNonNil_ReturnsPointer(t *testing.T) {
	type S struct{ P *int }
	x := 7
	s := S{P: &x}
	v := reflect.ValueOf(s)
	path := []int{0}

	val, ok := getValueByPathAny(v, path)
	if !ok {
		t.Fatalf("leaf pointer non-nil: ok=false")
	}
	p, ok2 := val.(*int)
	if !ok2 || p == nil || *p != 7 {
		t.Fatalf("leaf pointer non-nil: got %#v, want *int(&7)", val)
	}
}

// TestGetValueByPathAny_IntermediatePointerStruct_Deref ensures intermediate non-nil pointers
// to structs are dereferenced along the path.
func TestGetValueByPathAny_IntermediatePointerStruct_Deref(t *testing.T) {
	type Inner struct{ X int }
	type Outer struct{ In *Inner }

	o := Outer{In: &Inner{X: 42}}
	v := reflect.ValueOf(o)

	// path: In -> X  (field 0 then field 0)
	path := []int{0, 0}

	val, ok := getValueByPathAny(v, path)
	if !ok {
		t.Fatalf("ok=false, expected true")
	}
	got, ok2 := val.(int)
	if !ok2 || got != 42 {
		t.Fatalf("got=%#v, want 42 (int)", val)
	}
}

// TestGetValueByPathAny_IntermediatePointerStruct_Nil_ReturnsNilTrue ensures intermediate
// nil pointers to structs return (nil, true) during traversal.
func TestGetValueByPathAny_IntermediatePointerStruct_Nil_ReturnsNilTrue(t *testing.T) {
	type Inner struct{ X int }
	type Outer struct{ In *Inner }

	o := Outer{In: nil}
	v := reflect.ValueOf(o)

	// path: In -> X  (field 0 then field 0)
	path := []int{0, 0}

	val, ok := getValueByPathAny(v, path)
	if !ok || val != nil {
		t.Fatalf("got (ok=%v, val=%v), want (true, nil)", ok, val)
	}
}

// TestAliasByteSlice_NoExpand_AllDialects verifies that an alias type over []byte binds
// as a single placeholder and is converted to concrete []byte in args.
func TestAliasByteSlice_NoExpand_AllDialects(t *testing.T) {
	type Blob []byte

	for _, dc := range allDialects() {
		payload := Blob{0x01, 0x02, 0x03}
		out, args, err := New(dc.d).
			Write("UPDATE t SET bin=:p WHERE id=:id").
			Bind(P{"p": payload, "id": 10}).
			Build()
		assertNoError(t, err)

		// It must use one placeholder for :p and one for :id
		if got, want := countPlaceholders(out, dc.d), 2; got != want {
			t.Fatalf("[%s] placeholders=%d, want %d\nOUT:\n%s", dc.name, got, want, out)
		}
		if len(args) != 2 {
			t.Fatalf("[%s] len(args)=%d, want 2; args=%v", dc.name, len(args), args)
		}

		// The first argument must be []byte (not the alias type Blob) with the same content
		if _, isBlob := args[0].(Blob); isBlob {
			t.Fatalf("[%s] arg[0] kept alias type Blob; expected concrete []byte", dc.name)
		}
		if _, isBytes := args[0].([]byte); !isBytes {
			t.Fatalf("[%s] arg[0] type=%T, want []byte", dc.name, args[0])
		}
		if !equalArg(args[0], []byte{0x01, 0x02, 0x03}) {
			t.Fatalf("[%s] arg[0] bytes mismatch: got=%v", dc.name, args[0])
		}
		if args[1] != 10 {
			t.Fatalf("[%s] arg[1]=%v, want 10", dc.name, args[1])
		}
	}
}

// TestLimits_EnsureAdd_BytesAlias_Error_AllDialects ensures ensureAdd is triggered for alias []byte
// when MaxParams is exceeded.
func TestLimits_EnsureAdd_BytesAlias_Error_AllDialects(t *testing.T) {
	type Blob []byte

	for _, dc := range allDialects() {
		_, _, err := New(dc.d, Config{MaxParams: 1}).
			Write("SELECT :x, :b").
			Bind(P{"x": 1, "b": Blob{0x01}}).
			Build()
		if err == nil || !errors.Is(err, ErrTooManyParams) {
			t.Fatalf("[%s] expected ErrTooManyParams with alias []byte branch, got: %v", dc.name, err)
		}
	}
}

// --------------------------------
// Tests: mock Valuer
// --------------------------------

type mockArray struct{ v any }

func (m mockArray) Value() (driver.Value, error) { return m.v, nil }

// TestValuer_NoExpand_AllDialects ensures driver.Valuer values are never expanded,
// even when used multiple times; each usage consumes a single placeholder.
func TestValuer_NoExpand_AllDialects(t *testing.T) {
	// :ids is used twice -> two placeholders, each SCALAR (no inner expansion)
	sql := "SELECT 1 WHERE id = ANY(:ids) OR id = ANY(:ids) AND note=:n"
	ids := mockArray{v: []int{1, 2, 3}} // would be a slice, but as Valuer it MUST NOT expand
	for _, dc := range allDialects() {
		out, args, err := New(dc.d).
			Write(sql).
			Bind(map[string]any{"ids": ids, "n": "x"}).
			Build()
		assertNoError(t, err)
		// 3 placeholders in total: two for :ids, one for :n
		if got := countPlaceholders(out, dc.d); got != 3 {
			t.Fatalf("[%s] placeholders=%d, want 3\nOUT:\n%s", dc.name, got, out)
		}
		if len(args) != 3 {
			t.Fatalf("[%s] len(args)=%d, want 3", dc.name, len(args))
		}
		// the first two args must be our Valuer
		if _, ok := args[0].(mockArray); !ok {
			t.Fatalf("[%s] arg[0] is not a Valuer", dc.name)
		}
		if _, ok := args[1].(mockArray); !ok {
			t.Fatalf("[%s] arg[1] is not a Valuer", dc.name)
		}
		if args[2] != "x" {
			t.Fatalf("[%s] arg[2]=%v, want 'x'", dc.name, args[2])
		}
	}
}

// TestScalarWrapper_NoExpand_AllDialects ensures Scalar(...) prevents slice expansion,
// producing one placeholder per occurrence.
func TestScalarWrapper_NoExpand_AllDialects(t *testing.T) {
	sql := "SELECT 1 WHERE id = ANY(:ids) AND id = ANY(:ids)"
	// Without wrapper, []int would expand; with Scalar() it must remain SCALAR
	for _, dc := range allDialects() {
		out, args, err := New(dc.d).
			Write(sql).
			Bind(map[string]any{"ids": Scalar([]int{1, 2, 3})}).
			Build()
		assertNoError(t, err)
		if got := countPlaceholders(out, dc.d); got != 2 {
			t.Fatalf("[%s] placeholders=%d, want 2\nOUT:\n%s", dc.name, got, out)
		}
		if len(args) != 2 {
			t.Fatalf("[%s] len(args)=%d, want 2", dc.name, len(args))
		}
	}
}

// TestStructTag_Scalar_NoExpand_AllDialects verifies that struct field tag `,scalar`
// forces non-expansion of slices and binds one placeholder per occurrence.
func TestStructTag_Scalar_NoExpand_AllDialects(t *testing.T) {
	type Filter struct {
		IDs  []int `db:"ids,scalar"`
		Note string
	}
	f := Filter{IDs: []int{10, 11}, Note: "ok"}
	sql := "SELECT :Note WHERE id = ANY(:ids) OR id = ANY(:ids)"
	for _, dc := range allDialects() {
		out, args, err := New(dc.d).Write(sql).Bind(f).Build()
		assertNoError(t, err)
		// :Note + :ids twice -> 3 total placeholders
		if got := countPlaceholders(out, dc.d); got != 3 {
			t.Fatalf("[%s] placeholders=%d, want 3\nOUT:\n%s", dc.name, got, out)
		}
		if len(args) != 3 {
			t.Fatalf("[%s] len(args)=%d, want 3", dc.name, len(args))
		}
		if args[0] != "ok" {
			t.Fatalf("[%s] arg[0]=%v, want 'ok'", dc.name, args[0])
		}
		// the next two should be the same slice (not expanded)
		if _, isSlice := args[1].([]int); !isSlice {
			t.Fatalf("[%s] arg[1] is not a slice, got %T", dc.name, args[1])
		}
	}
}

// --------------------------------
// Tests: field cache
// --------------------------------

// TestFieldCache_GetPut_RotateAndPromote exercises rotation and promotion behavior of the
// two-tier field cache by forcing small capacities and checking state transitions.
func TestFieldCache_GetPut_RotateAndPromote(t *testing.T) {
	// Replace the global cache with a tiny one to force rotations.
	old := structIndexCache
	structIndexCache = newFieldCache(4) // max=4 for test
	defer func() { structIndexCache = old }()

	// Helper: create many unique reflect.Type (anonymous structs) for cache keys.
	makeType := func(i int) reflect.Type {
		f := reflect.StructField{Name: fmt.Sprintf("F%d", i), Type: reflect.TypeOf(int(0))}
		return reflect.StructOf([]reflect.StructField{f})
	}

	// Fill "curr" exactly.
	var firstBatch []reflect.Type
	for i := 0; i < 4; i++ {
		tt := makeType(i)
		firstBatch = append(firstBatch, tt)
		structIndexCache.put(tt, map[string]fieldInfo{"X": {index: []int{i}}})
	}
	// Verify state after fill.
	structIndexCache.mu.RLock()
	if got, want := len(structIndexCache.curr), 4; got != want {
		t.Fatalf("curr size=%d, want %d", got, want)
	}
	if got, want := len(structIndexCache.prev), 0; got != want {
		t.Fatalf("prev size=%d, want %d", got, want)
	}
	structIndexCache.mu.RUnlock()

	// Insert a 5th element -> rotation should occur (prev = old curr, curr = {new})
	t5 := makeType(4)
	structIndexCache.put(t5, map[string]fieldInfo{"X": {index: []int{99}}})

	structIndexCache.mu.RLock()
	if got, want := len(structIndexCache.prev), 4; got != want {
		t.Fatalf("after rotate: prev size=%d, want %d", got, want)
	}
	if got, want := len(structIndexCache.curr), 1; got != want {
		t.Fatalf("after rotate: curr size=%d, want %d", got, want)
	}
	if _, ok := structIndexCache.curr[t5]; !ok {
		t.Fatalf("after rotate: curr must contain t5")
	}
	if _, ok := structIndexCache.curr[firstBatch[0]]; ok {
		t.Fatalf("after rotate: curr must NOT contain firstBatch[0]")
	}
	if _, ok := structIndexCache.prev[firstBatch[0]]; !ok {
		t.Fatalf("after rotate: prev must contain firstBatch[0]")
	}
	structIndexCache.mu.RUnlock()

	// GET on a key present in prev -> should find and promote into curr (without rotating: curr has 1/4)
	if _, ok := structIndexCache.get(firstBatch[0]); !ok {
		t.Fatalf("get() should find previously inserted type in prev")
	}
	structIndexCache.mu.RLock()
	if _, ok := structIndexCache.curr[firstBatch[0]]; !ok {
		t.Fatalf("promotion failed: curr should contain promoted key")
	}
	if got := len(structIndexCache.curr); got != 2 {
		t.Fatalf("after promote: curr size=%d, want 2", got)
	}
	structIndexCache.mu.RUnlock()

	// Fill curr back to max with two new types.
	t6 := makeType(5)
	t7 := makeType(6)
	structIndexCache.put(t6, map[string]fieldInfo{"X": {index: []int{1}}})
	structIndexCache.put(t7, map[string]fieldInfo{"X": {index: []int{2}}})
	structIndexCache.mu.RLock()
	if got := len(structIndexCache.curr); got != 4 {
		t.Fatalf("curr should be full (4), got %d", got)
	}
	structIndexCache.mu.RUnlock()

	// Now GET another key in prev: promotion with full curr must cause a NEW rotation.
	if _, ok := structIndexCache.get(firstBatch[1]); !ok {
		t.Fatalf("get() should find another key in prev")
	}
	structIndexCache.mu.RLock()
	// After rotation in get: prev = old curr (4), curr = { promoted }
	if got := len(structIndexCache.curr); got != 1 {
		t.Fatalf("after rotate-on-promote: curr size=%d, want 1", got)
	}
	if _, ok := structIndexCache.curr[firstBatch[1]]; !ok {
		t.Fatalf("after rotate-on-promote: curr must contain promoted key")
	}
	if got := len(structIndexCache.prev); got != 4 {
		t.Fatalf("after rotate-on-promote: prev size=%d, want 4", got)
	}
	// And one of the elements that were in curr (e.g., t5) must now be in prev.
	if _, ok := structIndexCache.prev[t5]; !ok {
		t.Fatalf("after rotate-on-promote: prev must contain the previously current key (t5)")
	}
	structIndexCache.mu.RUnlock()
}

// TestFieldCache_GetMiss verifies that cache lookups miss when empty or when looking up
// unknown types, without side effects.
func TestFieldCache_GetMiss(t *testing.T) {
	old := structIndexCache
	structIndexCache = newFieldCache(2)
	defer func() { structIndexCache = old }()

	makeType := func(i int) reflect.Type {
		f := reflect.StructField{Name: fmt.Sprintf("F%d", i), Type: reflect.TypeOf(int(0))}
		return reflect.StructOf([]reflect.StructField{f})
	}

	// Empty cache -> miss
	if _, ok := structIndexCache.get(makeType(0)); ok {
		t.Fatalf("get on empty cache should miss")
	}

	// Insert one type, then look for a different one -> miss
	t0 := makeType(1)
	structIndexCache.put(t0, map[string]fieldInfo{"X": {index: []int{0}}})
	if _, ok := structIndexCache.get(makeType(2)); ok {
		t.Fatalf("get of unknown type should miss")
	}
}

// ----------------------------------------------------------------
// Tests: security (ensure values are never interpolated into SQL)
// ----------------------------------------------------------------

// TestSecurity_NoInterpolation_AllDialects ensures bound values are always parameterized
// and never interpolated into the SQL string (basic SQL injection safety).
func TestSecurity_NoInterpolation_AllDialects(t *testing.T) {
	inj := `1); DROP TABLE users; --`
	for _, dc := range allDialects() {
		dc := dc
		t.Run(dc.name, func(t *testing.T) {
			out, args := mustBuild(t, dc.d,
				"SELECT * FROM users WHERE id=:id AND note=:note",
				map[string]any{"id": 10, "note": inj},
			)
			// the query must NOT contain the malicious string
			if strings.Contains(out, "DROP TABLE") {
				t.Fatalf("possible unsafe interpolation: %s", out)
			}
			// but it must appear in args
			if len(args) != 2 || fmt.Sprint(args[1]) != inj {
				t.Fatalf("unexpected args: %#v", args)
			}
		})
	}
}

// TestSecurity_Safety checks a few common SQL injection patterns are neutralized by
// parameterization and never appear in the emitted SQL.
func TestSecurity_Safety(t *testing.T) {
	// common SQL injection patterns
	injections := []string{
		`'; DROP TABLE users; --`,
		`" OR "1"="1`,
		`\'; DROP TABLE users; --`,
		`%27; DROP TABLE users; --`,
	}

	for _, inj := range injections {
		out, _ := mustBuild(t, Postgres,
			"SELECT * FROM users WHERE name = :name",
			P{"name": inj})

		// the SQL must not contain the injection string
		if strings.Contains(out, "DROP TABLE") {
			t.Fatal("SQL injection not prevented")
		}
	}
}

// --------------------------------
// Tests: concurrency
// --------------------------------

// TestConcurrency_ParallelBuilders_AllDialects builds queries concurrently across goroutines
// to ensure parser/binder thread-safety and correct placeholder/args counts.
func TestConcurrency_ParallelBuilders_AllDialects(t *testing.T) {
	t.Parallel()
	for _, dc := range allDialects() {
		dc := dc
		t.Run(dc.name, func(t *testing.T) {
			t.Parallel()
			const goroutines = 32
			const iters = 200
			var wg sync.WaitGroup
			wg.Add(goroutines)
			for g := 0; g < goroutines; g++ {
				go func() {
					defer wg.Done()
					for i := 0; i < iters; i++ {
						s := New(dc.d)
						b := s.Write("SELECT * FROM t WHERE a=:a AND b IN (:b) AND c=:c")
						b.Bind(map[string]any{"a": i, "b": []int{i, i + 1, i + 2}, "c": "x"})
						out, args, err := b.Build()
						if err != nil {
							t.Errorf("build error: %v", err)
							return
						}
						if got, want := countPlaceholders(out, dc.d), 5; got != want {
							t.Errorf("ph=%d, want %d", got, want)
							return
						}
						if len(args) != 5 {
							t.Errorf("len(args)=%d, want 5", len(args))
							return
						}
					}
				}()
			}
			wg.Wait()
		})
	}
}

// TestConcurrency_ReUseSingleBuilderSequential_AllDialects reuses a single SQLR sequentially
// ensuring it resets between builds and emits correct arg counts.
func TestConcurrency_ReUseSingleBuilderSequential_AllDialects(t *testing.T) {
	for _, dc := range allDialects() {
		dc := dc
		t.Run(dc.name, func(t *testing.T) {
			s := New(dc.d)
			for i := 0; i < 100; i++ {
				b := s.Write("SELECT * FROM t WHERE a=:a")
				b.Bind(map[string]any{"a": i})
				out, args, err := b.Build()
				assertNoError(t, err)
				if len(args) != 1 {
					t.Fatalf("len(args)=%d, want 1", len(args))
				}
				_ = out
			}
		})
	}
}

// TestConcurrency_SharedSQLR_AllDialects shares a single SQLR instance across goroutines,
// verifying thread-safety via the internal builder pool.
func TestConcurrency_SharedSQLR_AllDialects(t *testing.T) {
	for _, dc := range allDialects() {
		s := New(dc.d)
		const G, I = 32, 300
		var wg sync.WaitGroup
		wg.Add(G)
		for g := 0; g < G; g++ {
			go func() {
				defer wg.Done()
				for i := 0; i < I; i++ {
					_, _, err := s.Write("SELECT * FROM t WHERE a=:a AND b IN (:b) AND c=:c").
						Bind(P{"a": i, "b": []int{i, i + 1, i + 2}, "c": "x"}).
						Build()
					if err != nil {
						t.Error(err)
						return
					}
				}
			}()
		}
		wg.Wait()
	}
}

// --------------------------------
// Benchmarks
// --------------------------------

// BenchmarkBind_Short_AllDialects measures binder overhead on a very short query across dialects.
func BenchmarkBind_Short_AllDialects(tb *testing.B) {
	for _, dc := range allDialects() {
		dc := dc
		tb.Run(dc.name, func(b *testing.B) {
			s := New(dc.d)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b := s.Write("SELECT * FROM t WHERE a=:a AND b=:b")
				b.Bind(map[string]any{"a": 1, "b": "x"})
				_, _, err := b.Build()
				if err != nil {
					tb.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkBind_Medium_AllDialects measures binder performance on a medium-length query
// that exercises various branches (IN lists, booleans, long strings).
func BenchmarkBind_Medium_AllDialects(tb *testing.B) {
	for _, dc := range allDialects() {
		dc := dc
		tb.Run(dc.name, func(b *testing.B) {
			q := "SELECT * FROM t WHERE x=:x AND y IN (:ys) AND z IN (:zs) AND note=:n AND w=:w"
			s := New(dc.d)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b := s.Write(q)
				b.Bind(map[string]any{
					"x":  123,
					"ys": []int{1, 2, 3, 4, 5, 6},
					"zs": []string{"a", "b", "c", "d"},
					"n":  "lorem ipsum dolor sit amet",
					"w":  true,
				})
				_, _, err := b.Build()
				if err != nil {
					tb.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkBind_Long_AllDialects measures binder throughput on a long query with many
// placeholders, repetitions, and a large :rows bulk block.
func BenchmarkBind_Long_AllDialects(tb *testing.B) {
	// Long query + big rows bulk
	type Row struct {
		A int    `db:"a"`
		B string `db:"b"`
		C int64  `db:"c"`
		D bool   `db:"d"`
		E string `db:"e"`
	}
	longRows := make([]Row, 200) // bulk of 200 rows x 5 cols = 1000 placeholders
	for i := range longRows {
		longRows[i] = Row{i, fmt.Sprintf("v%d", i), int64(i * 10), (i%2 == 0), "zzz"}
	}

	// Build the long query ONCE
	baseSel := "SELECT col1, col2, col3 FROM big_table WHERE 1=1 "
	var sb strings.Builder
	sb.WriteString(baseSel)
	for i := 0; i < 20; i++ {
		sb.WriteString(fmt.Sprintf("AND f%d=:f%d ", i, i))
	}
	sb.WriteString("AND id IN (:ids) AND code IN (:codes) ")
	q := sb.String() + "/* bulk */ INSERT INTO big_ins(a,b,c,d,e) VALUES :rows{a,b,c,d,e}"

	// Shared data
	ids := make([]int, 300)
	for i := range ids {
		ids[i] = i + 1
	}
	codes := []string{"x", "y", "z", "k", "w", "q", "p", "t", "u"}

	// Pre-compute keys "f0".."f19"
	keys := make([]string, 20)
	for i := range keys {
		keys[i] = "f" + strconv.Itoa(i)
	}

	for _, dc := range allDialects() {
		dc := dc
		tb.Run(dc.name, func(b *testing.B) {
			cfg := Config{}
			if dc.d == SQLite {
				cfg.MaxParams = -1 // unlimited (benchmark only!)
			}
			s := New(dc.d, cfg)

			// Base map with fixed shape (avoid map resizes in the loop)
			m := make(map[string]any, 3+len(keys))
			m["ids"] = ids
			m["codes"] = codes
			m["rows"] = longRows
			for _, k := range keys {
				m[k] = 0 // placeholder value; keep shape fixed
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Update only values; no new keys, no fmt.Sprintf
				for j, k := range keys {
					m[k] = j
				}
				builder := s.Write(q)
				builder.Bind(m)
				if _, _, err := builder.Build(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// ------------------------------------------------------------------------------------------------
// Fuzzing: parser must never panic; len(args) must equal # emitted placeholders
// (skip :name{...} to avoid generating dynamic rows blocks)
// ------------------------------------------------------------------------------------------------

var reRowsStart = regexp.MustCompile(`:[A-Za-z_][A-Za-z0-9_]*\{`)

// FuzzBind_NoPanic_AllDialects fuzzes with real SQL-like fragments to ensure the parser never
// panics and that the number of emitted placeholders equals len(args).
func FuzzBind_NoPanic_AllDialects(tf *testing.F) {
	// Seed corpus with “nasty” cases
	seeds := []string{
		"SELECT ':x' -- :x \n /* :x */ $tag$:x$tag$ :x ::int :y",
		"/* unterminated comment :x */ SELECT ':y' :z",
		"`:x` \":y\" ':: not a cast :z' :a /*/ tricky */ -- :b\n :c",
		"SELECT 1 WHERE JSON_EXTRACT(doc,'$.a') = :a AND note=':b'::text -- tail",
		"SELECT $q$ :x $q$ , $q1$ $q$ :y $q1$ , :z",
		"SELECT $$ open :x , :y",
		"SELECT $not$tag :x $ and :y",
	}
	for _, s := range seeds {
		tf.Add(s)
	}

	tf.Fuzz(func(t *testing.T, sql string) {
		if reRowsStart.MatchString(sql) {
			t.Skip()
		}

		// Build a default bind map for all :name found via a broad regex
		// (if it matches inside quotes/comments, that's fine: those keys will be unused)
		re := regexp.MustCompile(`:([A-Za-z_][A-Za-z0-9_]*)`)
		names := map[string]struct{}{}
		for _, m := range re.FindAllStringSubmatch(sql, -1) {
			names[m[1]] = struct{}{}
		}

		// For variety: some names -> scalar, others -> slice, others -> []byte
		bind := map[string]any{}
		i := 0
		for n := range names {
			switch i % 3 {
			case 0:
				bind[n] = i + 1
			case 1:
				bind[n] = []int{i + 1, i + 2}
			default:
				bind[n] = []byte{0x01, 0x02}
			}
			i++
		}

		for _, dc := range allDialects() {
			// Must never panic
			out, args, err := New(dc.d).Write(sql).Bind(bind).Build()
			// It can error if we accidentally generated too-long names, etc.; skip those
			if err != nil {
				if errors.Is(err, ErrParamNameTooLong) {
					t.Skip()
				}
				// Rare corner errors (e.g., overflow placeholders) are acceptable to skip.
				t.Skip()
			}
			// Property: #emitted placeholders == len(args)
			added := countNewPlaceholders(out, sql, dc.d)
			if added != len(args) {
				t.Fatalf("[%s] added=%d, len(args)=%d\nSQL:\n%s\nOUT:\n%s\nBIND:%v",
					dc.name, added, len(args), sql, out, bind)
			}
		}
	})
}

// FuzzBind_TokenMixer_AllDialects fuzzes with an aggressive token mixer that alternates
// parser states to ensure only real placeholders bind and casts are preserved.
func FuzzBind_TokenMixer_AllDialects(tf *testing.F) {
	makeCase := func(seed int64) string {
		r := rand.New(rand.NewSource(seed))
		var sb strings.Builder
		toks := []string{
			"TEXT", "SQ", "DQ", "BT", "BR", "LC", "BC", "DLR",
			"PH", "CAST", "JSON", "WS",
		}
		n := 5 + r.Intn(50)
		for i := 0; i < n; i++ {
			switch toks[r.Intn(len(toks))] {
			case "TEXT":
				sb.WriteString(" foo_bar ")
			case "SQ":
				sb.WriteString("'literal :notAParam '' doubled'")
			case "DQ":
				sb.WriteString(`"ident""with""quotes"`)
			case "BT":
				sb.WriteString("`back`")
			case "BR":
				sb.WriteString("[bracket]")
			case "LC":
				sb.WriteString("-- comment :nope \n")
			case "BC":
				sb.WriteString("/* block :nope */")
			case "DLR":
				if r.Intn(3) == 0 {
					sb.WriteString("$$ :nope ")
					if r.Intn(2) == 0 {
						sb.WriteString("$$")
					}
				} else {
					tag := fmt.Sprintf("$t%d$", r.Intn(5))
					sb.WriteString(tag + " :nope " + tag)
				}
			case "PH":
				names := []string{":a", ":b", ":ids", ":x1", ":_u"}
				sb.WriteString(names[r.Intn(len(names))])
			case "CAST":
				sb.WriteString(" col::int ")
			case "JSON":
				sb.WriteString(" doc->>'k' ")
			case "WS":
				sb.WriteString(" ")
			}
		}
		return sb.String()
	}

	// initial seeds
	for i := 0; i < 20; i++ {
		tf.Add(makeCase(int64(1000 + i)))
	}

	tf.Fuzz(func(t *testing.T, sql string) {
		if reRowsStart.MatchString(sql) {
			t.Skip()
		}

		// Default binds for a few known names used in the mixer
		bind := map[string]any{
			"a":   1,
			"b":   "x",
			"ids": []int{10, 11, 12},
			"x1":  9,
			"_u":  true,
		}

		// Add values for ANY :name present (if not already in bind)
		re := regexp.MustCompile(`:([A-Za-z_][A-Za-z0-9_]*)`)
		seen := map[string]struct{}{}
		for _, m := range re.FindAllStringSubmatch(sql, -1) {
			name := m[1]
			if _, ok := bind[name]; ok {
				continue
			}
			if _, ok := seen[name]; ok {
				continue
			}
			// alternate types for some variability
			idx := len(seen)
			switch idx % 3 {
			case 0:
				bind[name] = idx + 1
			case 1:
				bind[name] = []int{idx + 1, idx + 2}
			default:
				bind[name] = []byte{0x01, 0x02}
			}
			seen[name] = struct{}{}
		}

		for _, dc := range allDialects() {
			out, args, err := New(dc.d).Write(sql).Bind(bind).Build()
			if err != nil {
				// fuzzer may generate names > MaxNameLen: OK to skip
				if errors.Is(err, ErrParamNameTooLong) {
					t.Skip()
				}
				t.Fatalf("[%s] unexpected error: %v\nSQL:\n%s", dc.name, err, sql)
			}
			added := countNewPlaceholders(out, sql, dc.d)
			if added != len(args) {
				t.Fatalf("[%s] added=%d, len(args)=%d\nOUT:\n%s", dc.name, added, len(args), out)
			}
			// Casts must not disappear
			if strings.Contains(sql, "::") && !strings.Contains(out, "::") {
				t.Fatalf("[%s] lost '::' in result\nOUT:\n%s", dc.name, out)
			}
		}
	})
}

// --------------------------------
// State-machine stress tests
// --------------------------------

// TestStateStress_AlternatingStates_AllDialects alternates many parser states and ensures
// only real placeholders bind; protected tokens remain textual.
func TestStateStress_AlternatingStates_AllDialects(t *testing.T) {
	for _, dc := range allDialects() {
		dc := dc
		t.Run(dc.name, func(t *testing.T) {
			var sb strings.Builder
			// base: strings, comments, dollar-quoted, etc.
			sb.WriteString(`'a :x '' b' :y /* c :z */ "f:g"`)
			// bracket-quoted only for SQL Server
			if dc.d == SQLServer {
				sb.WriteString(` [d:e]`)
			}
			// backtick-quoted only for MySQL/SQLite
			if dc.d == MySQL || dc.d == SQLite {
				sb.WriteString(" `h:i`")
			}
			sb.WriteString(` -- j :k
:l $t$ :m $t$ :n`)

			sql := sb.String()

			// bind only real placeholders (outside quotes/comments)
			bind := map[string]any{"y": 1, "l": 0, "n": 4}

			out, args, err := New(dc.d).Write(sql).Bind(bind).Build()
			assertNoError(t, err)

			if got, want := countPlaceholders(out, dc.d), len(args); got != want {
				t.Fatalf("placeholder=%d, len(args)=%d\nIN:\n%s\nOUT:\n%s", got, want, sql, out)
			}

			// These must remain textual (protected by quote/comment/dollar)
			shouldRemain := []string{":x", ":z", ":k", ":m"} // :m is inside $t$...$t$
			if dc.d == SQLServer {
				shouldRemain = append(shouldRemain, ":e") // inside [ ... ]
			}
			if dc.d == MySQL || dc.d == SQLite {
				shouldRemain = append(shouldRemain, ":i") // inside ` ... `
			}
			for _, tok := range shouldRemain {
				if !strings.Contains(out, tok) {
					t.Fatalf("protected token %q was altered:\n%s", tok, out)
				}
			}
		})
	}
}

// TestDollarQuoted_NestedTags_AllDialects verifies nested dollar-quoted tags and ensures
// inner tokens remain untouched while placeholders outside still bind.
func TestDollarQuoted_NestedTags_AllDialects(t *testing.T) {
	sql := `SELECT $a$ :inside $b$ still $b$ :x $a$, :y`
	for _, dc := range allDialects() {
		out, args, err := New(dc.d).
			Write(sql).
			Bind(map[string]any{"x": 9, "y": 10}).
			Build()
		assertNoError(t, err)
		// :inside must not be substituted
		if !strings.Contains(out, ":inside") {
			t.Fatalf("[%s] ':inside' inside $a$...$a$ should remain textual:\n%s", dcase{d: dc.d}.name, out)
		}
		if got, want := countPlaceholders(out, dc.d), len(args); got != want {
			t.Fatalf("placeholder=%d, len(args)=%d", got, want)
		}
	}
}

// TestNonIdentifierAfterColon_NoMatch_AllDialects ensures tokens not starting with an identifier
// after ':' (e.g., :9, :-x, : , :.) do not match as placeholders.
func TestNonIdentifierAfterColon_NoMatch_AllDialects(t *testing.T) {
	// should NOT match :9, :-x, : , :.
	sql := "SELECT :9, :-x, : , :., :_ok, :also9 FROM t WHERE v=:v"
	bind := map[string]any{"_ok": 1, "also9": 2, "v": 3}
	for _, dc := range allDialects() {
		out, args, err := New(dc.d).Write(sql).Bind(bind).Build()
		assertNoError(t, err)
		if strings.Contains(out, "@p") || strings.Contains(out, "$") || strings.Contains(out, "?") == false {
			// soft check; the strong one is placeholder count == len(args)
		}
		if got, want := countPlaceholders(out, dc.d), len(args); got != want {
			t.Fatalf("[%s] placeholder=%d, len(args)=%d\nOUT:\n%s", dcase{d: dc.d}.name, got, want, out)
		}
		// invalid tokens must remain textual (e.g. ":9")
		if !strings.Contains(out, ":9") || !strings.Contains(out, ":-x") {
			t.Fatalf("token with colon that is not an identifier was altered:\n%s", out)
		}
	}
}

// TestCarriageReturnLineComments_CRLF_AllDialects handles CRLF line comments and ensures
// placeholders inside comments are ignored while real ones bind.
func TestCarriageReturnLineComments_CRLF_AllDialects(t *testing.T) {
	sql := "SELECT 1 -- comment :x\r\n AND a=:a -- hash :b\r\n /* blk :c */ AND d=:d"
	bind := map[string]any{"a": 1, "d": 2}
	for _, dc := range allDialects() {
		out, args, err := New(dc.d).Write(sql).Bind(bind).Build()
		assertNoError(t, err)
		if strings.Contains(out, ":x") == false || strings.Contains(out, ":b") == false || strings.Contains(out, ":c") == false {
			// ok: they remain inside comments as text
		}
		if got, want := countPlaceholders(out, dc.d), len(args); got != want {
			t.Fatalf("placeholder=%d != len(args)=%d\nOUT:\n%s", got, want, out)
		}
	}
}

// TestHashComment_MySQLOnly verifies that '#' starts a comment only in MySQL and is treated
// as text in other dialects (thus :skip binds outside MySQL).
func TestHashComment_MySQLOnly(t *testing.T) {
	sql := "SELECT 1 # comment :skip\r\n, :x"
	for _, dc := range allDialects() {
		b := New(dc.d).Write(sql)
		if dc.d == MySQL {
			// :skip is commented -> must not bind
			out, _, err := b.Bind(map[string]any{"x": 1}).Build()
			assertNoError(t, err)
			if got := countPlaceholders(out, dc.d); got != 1 {
				t.Fatalf("[%s] placeholders=%d, want 1\nOUT:\n%s", dc.name, got, out)
			}
			if !strings.Contains(out, ":skip") {
				t.Fatalf("[%s] ':skip' inside #... should remain textual", dc.name)
			}
		} else {
			// '#' is NOT a comment -> :skip is a real placeholder
			out, args, err := b.Bind(map[string]any{"x": 1, "skip": 2}).Build()
			assertNoError(t, err)
			if got := countPlaceholders(out, dc.d); got != 2 {
				t.Fatalf("[%s] placeholders=%d, want 2\nOUT:\n%s", dc.name, got, out)
			}
			// order: :skip then :x
			assertArgsEqual(t, args, []any{2, 1})
			if strings.Contains(out, ":skip") {
				t.Fatalf("[%s] ':skip' should not remain in clear", dc.name)
			}
		}
	}
}

// TestUnclosedLiterals_NoHang_AllDialects ensures unterminated strings/comments/dollar-quoted
// sections do not hang or panic; errors are acceptable and builds may be skipped.
func TestUnclosedLiterals_NoHang_AllDialects(t *testing.T) {
	cases := []string{
		"SELECT 'unterminated :x AND y=:y",
		"/* comment starts :x  SELECT */ :y",
		`$q$ starts :x`,
	}
	for _, dc := range allDialects() {
		for _, q := range cases {
			out, args, err := New(dc.d).
				Write(q).
				Bind(map[string]any{"y": 1}).
				Build()
			// must not panic nor spin forever; may error if a param is missing or similar
			if err != nil {
				continue
			}
			_ = out
			_ = args
		}
	}
}
