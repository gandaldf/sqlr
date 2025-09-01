package sqlr

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// ----------------------------------------------------------------
// Helpers for placeholder counting (Exec tests)
// ----------------------------------------------------------------

func placeholderRegexMapper(d Dialect) *regexp.Regexp {
	switch d {
	case Postgres:
		return regexp.MustCompile(`\$(?:[1-9][0-9]*)`)
	case SQLServer:
		return regexp.MustCompile(`@p(?:[1-9][0-9]*)`)
	default:
		return regexp.MustCompile(`\?`)
	}
}

func countPH(q string, d Dialect) int {
	return len(placeholderRegexMapper(d).FindAllStringIndex(q, -1))
}

// --------------------------------
// Exec: capture query and args
// --------------------------------

type execCatcher struct {
	lastQuery string
	lastArgs  []any
}

type dummyResult struct{ id, rows int64 }

func (d dummyResult) LastInsertId() (int64, error) { return d.id, nil }
func (d dummyResult) RowsAffected() (int64, error) { return d.rows, nil }

func (e *execCatcher) ExecContext(_ context.Context, query string, args ...any) (sql.Result, error) {
	e.lastQuery = query
	e.lastArgs = append([]any(nil), args...)
	return dummyResult{id: 123, rows: int64(len(args))}, nil
}

// TestMapper_Exec_ForwardsQueryAndArgs_AllDialects verifies that, for each
// supported SQL dialect, the builder forwards the final SQL string and the
// bound arguments to the Exec implementation unchanged, and that placeholder
// expansion matches the number of args. It also checks RowsAffected can be read.
func TestMapper_Exec_ForwardsQueryAndArgs_AllDialects(t *testing.T) {
	for _, dc := range allDialects() {
		t.Run(dc.name, func(t *testing.T) {
			ec := &execCatcher{}
			s := New(dc.d)
			b := s.Write("UPDATE t SET x=:x WHERE id IN (:ids) AND code=:c")
			b.Bind(map[string]any{"x": 9, "ids": []int{7, 8, 9}, "c": "k"})
			res, err := b.Exec(ec)
			assertNoError(t, err)

			// query and args were forwarded correctly
			if got, want := len(ec.lastArgs), 5; got != want {
				t.Fatalf("len(args) forwarded = %d, want %d; args=%v", got, want, ec.lastArgs)
			}
			if got := countPH(ec.lastQuery, dc.d); got != 5 {
				t.Fatalf("placeholders in forwarded query = %d, want 5\nQ=%s", got, ec.lastQuery)
			}
			if _, err := res.RowsAffected(); err != nil {
				t.Fatalf("RowsAffected err: %v", err)
			}
		})
	}
}

// --------------------------------
// Query/Scan with sqlmock
// --------------------------------

func newMockDB(t testing.TB) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New(): %v", err)
	}
	return db, mock
}

type Upper string

func (u *Upper) Scan(src any) error {
	switch v := src.(type) {
	case []byte:
		*u = Upper(strings.ToUpper(string(v)))
	case string:
		*u = Upper(strings.ToUpper(v))
	default:
		return fmt.Errorf("unsupported: %T", src)
	}
	return nil
}

// --------------------------------
// Scan: primitives, struct, errors
// --------------------------------

// TestMapper_Scan_Primitive_OneRow ensures ScanOne can read a single primitive
// value (one row, one column) into a basic Go type.
func TestMapper_Scan_Primitive_OneRow(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	mock.ExpectQuery(".*").WithArgs(7).
		WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow(42))

	var v int
	err := New(Postgres).Write("SELECT :id").Bind(map[string]any{"id": 7}).ScanOne(db, &v)
	assertNoError(t, err)
	if v != 42 {
		t.Fatalf("got=%d, want 42", v)
	}
}

// TestMapper_Scan_Primitive_NoRows_Error verifies that ScanOne returns
// sql.ErrNoRows when the query produces zero rows.
func TestMapper_Scan_Primitive_NoRows_Error(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{"v"})) // 0 rows

	var v int
	err := New(Postgres).Write("SELECT 1").ScanOne(db, &v)
	if err == nil || err != sql.ErrNoRows {
		t.Fatalf("want sql.ErrNoRows, got %v", err)
	}
}

// TestMapper_Scan_Primitive_MultiRows_Error verifies that ScanOne fails when
// more than one row is returned for a primitive destination.
func TestMapper_Scan_Primitive_MultiRows_Error(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	rows := sqlmock.NewRows([]string{"v"}).AddRow(1).AddRow(2)
	mock.ExpectQuery(".*").WillReturnRows(rows)

	var v int
	err := New(Postgres).Write("SELECT 1").ScanOne(db, &v)
	if err == nil || !strings.Contains(err.Error(), "more than one row") {
		t.Fatalf("expected 'more than one row' error, got %v", err)
	}
}

// TestMapper_Scan_Primitive_MultiColumns_Error ensures ScanOne complains when
// scanning into a primitive but the row has more than one column.
func TestMapper_Scan_Primitive_MultiColumns_Error(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	rows := sqlmock.NewRows([]string{"a", "b"}).AddRow(1, 2)
	mock.ExpectQuery(".*").WillReturnRows(rows)

	var v int
	err := New(Postgres).Write("SELECT 1").ScanOne(db, &v)
	if err == nil || !strings.Contains(err.Error(), "requires 1 column") {
		t.Fatalf("expected error for #columns!=1, got %v", err)
	}
}

// TestMapper_Scan_DestMustBePointer_Error checks that ScanOne validates
// the destination is a non-nil pointer.
func TestMapper_Scan_DestMustBePointer_Error(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()
	mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow(1))

	var notPtr int
	err := New(Postgres).Write("SELECT 1").ScanOne(db, notPtr)
	if err == nil || !strings.Contains(err.Error(), "non-nil pointer") {
		t.Fatalf("expected error: dest not a pointer, got %v", err)
	}
}

// TestMapper_Scan_Struct_Tags_ExtraColsIgnored verifies that ScanOne maps only
// tagged struct fields and silently discards unknown columns.
func TestMapper_Scan_Struct_Tags_ExtraColsIgnored(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	type Row struct {
		A int    `db:"a"`
		B string `db:"b"`
	}
	rows := sqlmock.NewRows([]string{"a", "b", "ignored"}).
		AddRow(7, "x", "dropme")
	mock.ExpectQuery(".*").WillReturnRows(rows)

	var r Row
	err := New(Postgres).Write("SELECT 1").ScanOne(db, &r)
	assertNoError(t, err)
	if r.A != 7 || r.B != "x" {
		t.Fatalf("got %+v, want {A:7 B:x}", r)
	}
}

// TestMapper_Scan_Struct_PointerField_NULL_OK ensures that a NULL database
// value maps to a nil pointer field in a struct without error.
func TestMapper_Scan_Struct_PointerField_NULL_OK(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	type Row struct {
		A *int   `db:"a"`
		B string `db:"b"`
	}
	rows := sqlmock.NewRows([]string{"a", "b"}).AddRow(nil, "x")
	mock.ExpectQuery(".*").WillReturnRows(rows)

	var r Row
	err := New(Postgres).Write("SELECT 1").ScanOne(db, &r)
	assertNoError(t, err)
	if r.A != nil || r.B != "x" {
		t.Fatalf("got %+v, want {A:nil B:x}", r)
	}
}

// TestMapper_Scan_Struct_NonPointerField_NULL_Error verifies that a NULL
// database value cannot populate a non-pointer field and should produce an error.
func TestMapper_Scan_Struct_NonPointerField_NULL_Error(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	type Row struct {
		A int    `db:"a"` // non-pointer
		B string `db:"b"`
	}
	rows := sqlmock.NewRows([]string{"a", "b"}).AddRow(nil, "x") // NULL in a
	mock.ExpectQuery(".*").WillReturnRows(rows)

	var r Row
	err := New(Postgres).Write("SELECT 1").ScanOne(db, &r)
	if err == nil {
		t.Fatalf("expected error for NULL in non-pointer field")
	}
}

// TestMapper_Scan_Struct_FieldImplementsScanner checks that a struct field
// whose type implements sql.Scanner is filled by invoking its Scan method.
func TestMapper_Scan_Struct_FieldImplementsScanner(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	type Row struct {
		Up Upper `db:"up"`
	}
	rows := sqlmock.NewRows([]string{"up"}).AddRow("ciao")
	mock.ExpectQuery(".*").WillReturnRows(rows)

	var r Row
	err := New(Postgres).Write("SELECT 1").ScanOne(db, &r)
	assertNoError(t, err)
	if r.Up != Upper("CIAO") {
		t.Fatalf("scanner not applied: got %q", r.Up)
	}
}

// TestMapper_Scan_Primitive_ScannerType_Upper_String verifies that a primitive
// destination type implementing sql.Scanner (Upper) is populated from a string
// column using its Scan method.
func TestMapper_Scan_Primitive_ScannerType_Upper_String(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	rows := sqlmock.NewRows([]string{"u"}).AddRow("ciao")
	mock.ExpectQuery(".*").WillReturnRows(rows)

	var u Upper
	err := New(Postgres).Write("SELECT 1").ScanOne(db, &u)
	assertNoError(t, err)

	if u != Upper("CIAO") {
		t.Fatalf("scanner not applied: got=%q, want=CIAO", u)
	}
}

// TestMapper_Scan_Primitive_ScannerType_Upper_Bytes verifies that the same
// Scanner type works when the column is []byte.
func TestMapper_Scan_Primitive_ScannerType_Upper_Bytes(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	rows := sqlmock.NewRows([]string{"u"}).AddRow([]byte("hey"))
	mock.ExpectQuery(".*").WillReturnRows(rows)

	var u Upper
	err := New(Postgres).Write("SELECT 1").ScanOne(db, &u)
	assertNoError(t, err)

	if u != Upper("HEY") {
		t.Fatalf("scanner not applied from []byte: got=%q, want=HEY", u)
	}
}

// TestMapper_Scan_Primitive_ScannerType_Upper_Unsupported_Error ensures that
// when the Scanner returns an error (unsupported type), ScanOne propagates it.
func TestMapper_Scan_Primitive_ScannerType_Upper_Unsupported_Error(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	// Upper.Scan doesn't support int -> should propagate error
	rows := sqlmock.NewRows([]string{"u"}).AddRow(123)
	mock.ExpectQuery(".*").WillReturnRows(rows)

	var u Upper
	err := New(Postgres).Write("SELECT 1").ScanOne(db, &u)
	if err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("expected error from scanner, got: %v", err)
	}
}

// TestMapper_Scan_Primitive_ScannerType_NullString validates support for
// stdlib Scanner types like sql.NullString for single-column results.
func TestMapper_Scan_Primitive_ScannerType_NullString(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	rows := sqlmock.NewRows([]string{"s"}).AddRow("ok")
	mock.ExpectQuery(".*").WillReturnRows(rows)

	var ns sql.NullString
	err := New(Postgres).Write("SELECT 1").ScanOne(db, &ns)
	assertNoError(t, err)

	if !ns.Valid || ns.String != "ok" {
		t.Fatalf("sql.NullString mismatch: %+v", ns)
	}
}

// TestMapper_Scan_Primitive_ScannerType_NullString_MultiCols_Error ensures
// that a Scanner destination used as a primitive still requires exactly one
// column in the result set.
func TestMapper_Scan_Primitive_ScannerType_NullString_MultiCols_Error(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	rows := sqlmock.NewRows([]string{"a", "b"}).AddRow("ok", "extra")
	mock.ExpectQuery(".*").WillReturnRows(rows)

	var ns sql.NullString
	err := New(Postgres).Write("SELECT 1").ScanOne(db, &ns)
	if err == nil || !strings.Contains(err.Error(), "requires 1 column") {
		t.Fatalf("expected error for #columns!=1, got %v", err)
	}
}

// TestAmbiguousField_Scan_Error verifies that scanning into a struct with
// ambiguous field mappings (same tag on multiple fields) returns
// ErrFieldAmbiguous.
func TestAmbiguousField_Scan_Error(t *testing.T) {
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

	db, mock := newMockDB(t)
	defer db.Close()

	// One column named "id" -> mapping is ambiguous between A.ID and B.ID
	rows := sqlmock.NewRows([]string{"id"}).AddRow(7)
	mock.ExpectQuery(".*").WillReturnRows(rows)

	var c C
	err := New(Postgres).Write("SELECT 1").ScanOne(db, &c)
	if err == nil || !errors.Is(err, ErrFieldAmbiguous) {
		t.Fatalf("expected ErrFieldAmbiguous, got %v", err)
	}
}

// ----------------------------------------------------------------
// ScanAll: slice of struct, *struct, primitives, errors
// ----------------------------------------------------------------

// TestMapper_ScanAll_SliceOfStruct checks that ScanAll can build a slice of
// structs from multiple rows with tagged columns.
func TestMapper_ScanAll_SliceOfStruct(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	type Row struct {
		ID   int    `db:"id"`
		Name string `db:"name"`
	}
	rows := sqlmock.NewRows([]string{"id", "name"}).
		AddRow(1, "a").
		AddRow(2, "b").
		AddRow(3, "c")
	mock.ExpectQuery(".*").WillReturnRows(rows)

	var out []Row
	err := New(Postgres).Write("SELECT 1").ScanAll(db, &out)
	assertNoError(t, err)
	if len(out) != 3 || out[0].ID != 1 || out[2].Name != "c" {
		t.Fatalf("slice struct mismatch: %+v", out)
	}
}

// TestMapper_ScanAll_SliceOfPtrStruct ensures ScanAll supports slices of
// pointers to structs and properly allocates each element.
func TestMapper_ScanAll_SliceOfPtrStruct(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	type Row struct {
		X int `db:"x"`
	}
	rows := sqlmock.NewRows([]string{"x"}).AddRow(10).AddRow(20)
	mock.ExpectQuery(".*").WillReturnRows(rows)

	var out []*Row
	err := New(Postgres).Write("SELECT 1").ScanAll(db, &out)
	assertNoError(t, err)
	if len(out) != 2 || out[0].X != 10 || out[1].X != 20 {
		t.Fatalf("slice *struct mismatch: %+v", out)
	}
}

// TestMapper_ScanAll_SliceOfPrimitives_OneColumn verifies that ScanAll can
// collect a single-column result set into a slice of primitives.
func TestMapper_ScanAll_SliceOfPrimitives_OneColumn(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	rows := sqlmock.NewRows([]string{"v"}).AddRow(1).AddRow(2).AddRow(3)
	mock.ExpectQuery(".*").WillReturnRows(rows)

	var out []int
	err := New(Postgres).Write("SELECT 1").ScanAll(db, &out)
	assertNoError(t, err)
	if !reflect.DeepEqual(out, []int{1, 2, 3}) {
		t.Fatalf("got=%v, want [1 2 3]", out)
	}
}

// TestMapper_ScanAll_DestMustBePointerToSlice_Error validates that ScanAll
// checks the destination is a pointer to a slice (and not something else).
func TestMapper_ScanAll_DestMustBePointerToSlice_Error(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	// 1st call: dest not a pointer -> need 1 ExpectQuery
	mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow(1))
	var notPtr []int
	err := New(Postgres).Write("SELECT 1").ScanAll(db, notPtr)
	if err == nil || !strings.Contains(err.Error(), "non-nil pointer") {
		t.Fatalf("expected error: dest not a pointer, got %v", err)
	}

	// 2nd call: dest pointer but not a slice -> need another ExpectQuery
	mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow(1))
	var notSlice int
	err = New(Postgres).Write("SELECT 1").ScanAll(db, &notSlice)
	if err == nil || !strings.Contains(err.Error(), "pointer to slice") {
		t.Fatalf("expected error: dest not a slice, got %v", err)
	}
}

// TestMapper_ScanAll_PrimitiveSlice_MultiColumns_Error ensures that when
// building a slice of primitives, the query must return exactly one column.
func TestMapper_ScanAll_PrimitiveSlice_MultiColumns_Error(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	rows := sqlmock.NewRows([]string{"a", "b"}).AddRow(1, 2)
	mock.ExpectQuery(".*").WillReturnRows(rows)

	var out []int
	err := New(Postgres).Write("SELECT 1").ScanAll(db, &out)
	if err == nil || !strings.Contains(err.Error(), "requires 1 column") {
		t.Fatalf("expected error for #columns!=1, got %v", err)
	}
}

// TestMapper_ScanAll_ScannerAndPtr_ResetPerRow verifies that scanner fields
// and pointer holders are correctly reset between rows, so previous non-nil
// values do not leak into subsequent rows.
func TestMapper_ScanAll_ScannerAndPtr_ResetPerRow(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	type Row struct {
		U Upper `db:"u"` // ckScanner: *Upper implements sql.Scanner
		P *int  `db:"p"` // ckPtr
	}

	rows := sqlmock.NewRows([]string{"u", "p"}).
		AddRow("ciao", 7). // first row: P non-nil
		AddRow("x", nil)   // second row: P should be nil after holder reset

	mock.ExpectQuery(".*").WillReturnRows(rows)

	var out []Row
	err := New(Postgres).Write("SELECT 1").ScanAll(db, &out)
	assertNoError(t, err)

	if len(out) != 2 {
		t.Fatalf("len(out)=%d, want 2", len(out))
	}
	if out[0].U != Upper("CIAO") {
		t.Fatalf("scanner not applied on row 0: %q", out[0].U)
	}
	if out[0].P == nil || *out[0].P != 7 {
		t.Fatalf("r0.P got=%v, want 7", out[0].P)
	}
	if out[1].U != Upper("X") {
		t.Fatalf("scanner not applied on row 1: %q", out[1].U)
	}
	if out[1].P != nil {
		t.Fatalf("r1.P should be nil after reset, got=%v", *out[1].P)
	}
}

// TestMapper_ScanAll_PointerToScannerType ensures that pointer-to-Scanner leaf
// fields are allocated when non-NULL and left nil when NULL.
func TestMapper_ScanAll_PointerToScannerType(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	type Row struct {
		U *Upper `db:"u"` // ckPtr, but *Upper has Scan
	}

	rows := sqlmock.NewRows([]string{"u"}).
		AddRow("hello").
		AddRow(nil)

	mock.ExpectQuery(".*").WillReturnRows(rows)

	var out []Row
	err := New(Postgres).Write("SELECT 1").ScanAll(db, &out)
	assertNoError(t, err)

	if len(out) != 2 {
		t.Fatalf("len(out)=%d, want 2", len(out))
	}
	if out[0].U == nil || *out[0].U != Upper("HELLO") {
		t.Fatalf("row0.U got=%v, want HELLO", out[0].U)
	}
	if out[1].U != nil {
		t.Fatalf("row1.U should be nil, got=%v", out[1].U)
	}
}

// TestMapper_Scan_PointerToScannerType_ScanOne covers ScanOne behavior with a
// pointer-to-Scanner field: it is allocated and scanned for non-NULL, and
// remains nil for NULL values.
func TestMapper_Scan_PointerToScannerType_ScanOne(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	type Row struct {
		U *Upper `db:"u"`
	}

	// 1) non-NULL value -> U must be non-nil and uppercase
	rows1 := sqlmock.NewRows([]string{"u"}).AddRow("hello")
	mock.ExpectQuery(".*").WillReturnRows(rows1)

	var r1 Row
	err := New(Postgres).Write("SELECT 1").ScanOne(db, &r1)
	assertNoError(t, err)
	if r1.U == nil || *r1.U != Upper("HELLO") {
		t.Fatalf("got=%v, want *Upper(\"HELLO\")", r1.U)
	}

	// 2) NULL value -> U must remain nil
	rows2 := sqlmock.NewRows([]string{"u"}).AddRow(nil)
	mock.ExpectQuery(".*").WillReturnRows(rows2)

	var r2 Row
	err = New(Postgres).Write("SELECT 1").ScanOne(db, &r2)
	assertNoError(t, err)
	if r2.U != nil {
		t.Fatalf("want nil, got=%v", r2.U)
	}
}

// TestMapper_ScanAll_MultiPtr_ResetEveryRow verifies that multiple pointer
// fields are independently reset per row so NULLs are represented as nil
// pointers and non-NULLs are properly allocated and assigned.
func TestMapper_ScanAll_MultiPtr_ResetEveryRow(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	type Row struct {
		A *int    `db:"a"`
		B *string `db:"b"`
	}

	rows := sqlmock.NewRows([]string{"a", "b"}).
		AddRow(1, "x").
		AddRow(nil, "y").
		AddRow(3, nil)

	mock.ExpectQuery(".*").WillReturnRows(rows)

	var out []Row
	err := New(Postgres).Write("SELECT 1").ScanAll(db, &out)
	assertNoError(t, err)

	if len(out) != 3 {
		t.Fatalf("len(out)=%d, want 3", len(out))
	}
	// r0: both set
	if out[0].A == nil || *out[0].A != 1 || out[0].B == nil || *out[0].B != "x" {
		t.Fatalf("row0 mismatch: %+v", out[0])
	}
	// r1: A nil, B set
	if out[1].A != nil || out[1].B == nil || *out[1].B != "y" {
		t.Fatalf("row1 mismatch: %+v", out[1])
	}
	// r2: A set, B nil
	if out[2].A == nil || *out[2].A != 3 || out[2].B != nil {
		t.Fatalf("row2 mismatch: %+v", out[2])
	}
}

// TestMapper_ScanAll_AllColsAreSinks_EmptyStruct confirms that queries whose
// columns do not map to any struct fields (empty struct) are still consumed
// without error and produce the correct number of rows.
func TestMapper_ScanAll_AllColsAreSinks_EmptyStruct(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	// No fields -> all columns go to ckSink
	type Row struct{}

	rows := sqlmock.NewRows([]string{"x", "y"}).
		AddRow(1, "a").
		AddRow(2, "b")
	mock.ExpectQuery(".*").WillReturnRows(rows)

	var out []Row
	err := New(Postgres).Write("SELECT 1").ScanAll(db, &out)
	assertNoError(t, err)

	if len(out) != 2 {
		t.Fatalf("len(out)=%d, want 2", len(out))
	}
}

// TestMapper_ScanAll_MixedCols_SomeSinks_SomeMapped verifies that unmapped
// columns are ignored while mapped columns are properly assigned.
func TestMapper_ScanAll_MixedCols_SomeSinks_SomeMapped(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	// "a" and "b" mapped, "ignored" goes to ckSink
	type Row struct {
		A int    `db:"a"`
		B string `db:"b"`
	}

	rows := sqlmock.NewRows([]string{"a", "ignored", "b"}).
		AddRow(7, "dropme", "x").
		AddRow(8, "also_drop", "y")
	mock.ExpectQuery(".*").WillReturnRows(rows)

	var out []Row
	err := New(Postgres).Write("SELECT 1").ScanAll(db, &out)
	assertNoError(t, err)

	if len(out) != 2 {
		t.Fatalf("len(out)=%d, want 2", len(out))
	}
	if out[0].A != 7 || out[0].B != "x" {
		t.Fatalf("row0 mismatch: %+v", out[0])
	}
	if out[1].A != 8 || out[1].B != "y" {
		t.Fatalf("row1 mismatch: %+v", out[1])
	}
}

// TestMapper_ScanAll_SliceOfPtrToNonStruct_Error checks that ScanAll rejects
// destinations of type []*T where T is not a struct.
func TestMapper_ScanAll_SliceOfPtrToNonStruct_Error(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	// Any row (content irrelevant, error happens before Scan)
	mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow(1))

	var out []*int // []*int -> elemT.Kind()==Pointer but elemT.Elem()!=Struct -> error
	err := New(Postgres).Write("SELECT 1").ScanAll(db, &out)
	if err == nil || !strings.Contains(err.Error(), "slice of pointers to non-struct") {
		t.Fatalf("expected 'slice of pointers to non-struct' error, got %v", err)
	}
}

// TestMapper_Scan_Flatten_AnonymousEmbedded ensures that fields from an
// anonymously embedded struct are flattened and mapped by their tags.
func TestMapper_Scan_Flatten_AnonymousEmbedded(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	type Address struct {
		City string `db:"city"`
		Zip  int    `db:"zip"`
	}
	type Row struct {
		ID      int `db:"id"`
		Address     // anonymous
	}

	rows := sqlmock.NewRows([]string{"id", "city", "zip"}).AddRow(1, "Roma", 10100)
	mock.ExpectQuery(".*").WillReturnRows(rows)

	var out Row
	err := New(Postgres).Write("SELECT 1").ScanOne(db, &out)
	assertNoError(t, err)
	if out.ID != 1 || out.City != "Roma" || out.Zip != 10100 {
		t.Fatalf("scan mismatch: %+v", out)
	}
}

// TestMapper_Scan_Flatten_NamedPtr_AutoAlloc verifies that named pointer
// fields to nested structs are automatically allocated when a leaf column is
// present.
func TestMapper_Scan_Flatten_NamedPtr_AutoAlloc(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	type Address struct {
		City string `db:"city"`
	}
	type Row struct {
		Addr *Address // should be auto-allocated
	}

	rows := sqlmock.NewRows([]string{"city"}).AddRow("Roma")
	mock.ExpectQuery(".*").WillReturnRows(rows)

	var out Row
	err := New(Postgres).Write("SELECT 1").ScanOne(db, &out)
	assertNoError(t, err)
	if out.Addr == nil || out.Addr.City != "Roma" {
		t.Fatalf("auto-alloc failed: %+v", out)
	}
}

// TestMapper_Scan_Flatten_DeepPtrChain_AutoAlloc ensures that multi-level
// pointer chains are walked and intermediate structs auto-allocated.
func TestMapper_Scan_Flatten_DeepPtrChain_AutoAlloc(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	type S2 struct {
		Name string `db:"name"`
	}
	type S1 struct{ P *S2 }
	type Root struct{ S *S1 }

	rows := sqlmock.NewRows([]string{"name"}).AddRow("Zed")
	mock.ExpectQuery(".*").WillReturnRows(rows)

	var r Root
	err := New(Postgres).Write("SELECT 1").ScanOne(db, &r)
	assertNoError(t, err)
	if r.S == nil || r.S.P == nil || r.S.P.Name != "Zed" {
		t.Fatalf("deep auto-alloc failed: %+v", r)
	}
}

// TestMapper_ScanAll_Flatten_NestedPtr_ResetPerRow verifies that pointer
// leaves inside nested structs are reset per row and reflect NULL correctly.
func TestMapper_ScanAll_Flatten_NestedPtr_ResetPerRow(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	type Address struct {
		City *string `db:"city"` // leaf pointer to handle NULL
	}
	type Row struct {
		Addr *Address
	}

	rows := sqlmock.NewRows([]string{"city"}).
		AddRow("Roma").
		AddRow(nil) // must reset to nil on second row
	mock.ExpectQuery(".*").WillReturnRows(rows)

	var out []Row
	err := New(Postgres).Write("SELECT 1").ScanAll(db, &out)
	assertNoError(t, err)

	if len(out) != 2 {
		t.Fatalf("len(out)=%d, want 2", len(out))
	}
	if out[0].Addr == nil || out[0].Addr.City == nil || *out[0].Addr.City != "Roma" {
		t.Fatalf("row0 mismatch: %+v", out[0])
	}
	if out[1].Addr == nil || out[1].Addr.City != nil {
		t.Fatalf("row1 Addr.City should be nil: %+v", out[1])
	}
}

// TestMapper_Scan_Flatten_ScannerLeafInNested checks that scanner-typed
// leaves inside nested structs are correctly scanned.
func TestMapper_Scan_Flatten_ScannerLeafInNested(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	type Payload struct {
		U Upper `db:"up"` // Upper implements sql.Scanner
	}
	type Row struct {
		P *Payload
	}

	rows := sqlmock.NewRows([]string{"up"}).AddRow("ciao")
	mock.ExpectQuery(".*").WillReturnRows(rows)

	var out Row
	err := New(Postgres).Write("SELECT 1").ScanOne(db, &out)
	assertNoError(t, err)
	if out.P == nil || out.P.U != Upper("CIAO") {
		t.Fatalf("nested scanner not applied: %+v", out)
	}
}

// TestMapper_Scan_Flatten_TimeLeafInNested ensures time.Time leaves inside
// nested structs are mapped as-is.
func TestMapper_Scan_Flatten_TimeLeafInNested(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	type Audit struct {
		CreatedAt time.Time `db:"created_at"`
	}
	type Row struct {
		A *Audit
	}
	now := time.Now()

	rows := sqlmock.NewRows([]string{"created_at"}).AddRow(now)
	mock.ExpectQuery(".*").WillReturnRows(rows)

	var out Row
	err := New(Postgres).Write("SELECT 1").ScanOne(db, &out)
	assertNoError(t, err)
	if out.A == nil || !out.A.CreatedAt.Equal(now) {
		t.Fatalf("time leaf mismatch: %+v (want %v)", out, now)
	}
}

// TestScanAll_AmbiguousField_InMakeScanPlan_AllDialects verifies that, across
// all dialects, ScanAll fails with ErrFieldAmbiguous when two struct fields
// map to the same column name.
func TestScanAll_AmbiguousField_InMakeScanPlan_AllDialects(t *testing.T) {
	for _, dc := range allDialects() {
		t.Run(dc.name, func(t *testing.T) {
			db, mock := newMockDB(t)
			defer db.Close()

			// Una singola colonna "id" per innescare il mapping ambiguo.
			rows := sqlmock.NewRows([]string{"id"}).AddRow(7)
			mock.ExpectQuery(".*").WillReturnRows(rows)

			// Struct con collisione intenzionale: due campi con lo stesso nome/tag "id".
			type Amb struct {
				X int `db:"id"`
				Y int `db:"id"`
			}

			var out []Amb
			err := New(dc.d).Write("SELECT 1").ScanAll(db, &out)

			if err == nil || !errors.Is(err, ErrFieldAmbiguous) {
				t.Fatalf("[%s] expected ErrFieldAmbiguous, got: %v", dc.name, err)
			}
			if err != nil && !strings.Contains(err.Error(), `"id"`) {
				t.Fatalf("[%s] error should mention the conflicting column name \"id\": %v", dc.name, err)
			}
		})
	}
}

// TestMakeScanPlan_AmbiguousField_Direct is a unit-level test of makeScanPlan
// that checks ambiguity is detected even without running a query.
func TestMakeScanPlan_AmbiguousField_Direct(t *testing.T) {
	type Amb struct {
		X int `db:"id"`
		Y int `db:"id"`
	}
	cols := []string{"id"}

	_, err := makeScanPlan(cols, reflect.TypeOf(Amb{}))
	if err == nil || !errors.Is(err, ErrFieldAmbiguous) {
		t.Fatalf("expected ErrFieldAmbiguous, got: %v", err)
	}
}

// TestBind_NestedInterfaceLeafNil_YieldsNilArg_AllDialects verifies that a
// nested interface leaf bound via tag produces a nil argument when the leaf is
// nil, and that placeholder count matches across all dialects.
func TestBind_NestedInterfaceLeafNil_YieldsNilArg_AllDialects(t *testing.T) {
	// Struct with interface leaf mapped via tag, nil at leaf
	type Holder struct {
		V any `db:"val"`
	}
	h := Holder{V: nil}

	for _, dc := range allDialects() {
		ec := &execCatcher{}
		res, err := New(dc.d).
			Write("UPDATE t SET x=:val").
			Bind(h).
			Exec(ec)
		assertNoError(t, err)

		if got := countPH(ec.lastQuery, dc.d); got != 1 {
			t.Fatalf("[%s] ph=%d, want 1; q=%s", dc.name, got, ec.lastQuery)
		}
		if len(ec.lastArgs) != 1 || ec.lastArgs[0] != nil {
			t.Fatalf("[%s] arg[0]=%v, want nil", dc.name, ec.lastArgs)
		}
		if _, err := res.RowsAffected(); err != nil {
			t.Fatalf("[%s] RowsAffected err: %v", dc.name, err)
		}
	}
}

// TestBind_EmbeddedPointerStruct_DerefAndNil_AllDialects ensures that values
// from an anonymously embedded pointer struct are dereferenced for binding when
// non-nil, and produce a nil argument when the embedded pointer is nil.
func TestBind_EmbeddedPointerStruct_DerefAndNil_AllDialects(t *testing.T) {
	type Inner struct {
		A int `db:"a"`
	}
	type Outer struct {
		*Inner // anonymous embedded pointer
	}

	for _, dc := range allDialects() {
		// case 1: non-nil pointer → should extract 7
		out, args, err := New(dc.d).
			Write("SELECT :a").
			Bind(Outer{Inner: &Inner{A: 7}}).
			Build()
		assertNoError(t, err)
		if got := countPlaceholders(out, dc.d); got != 1 {
			t.Fatalf("[%s] placeholders=%d, want 1\nOUT:\n%s", dc.name, got, out)
		}
		assertArgsEqual(t, args, []any{7})

		// case 2: nil pointer → argument should be nil
		out, args, err = New(dc.d).
			Write("SELECT :a").
			Bind(Outer{Inner: nil}).
			Build()
		assertNoError(t, err)
		if got := countPlaceholders(out, dc.d); got != 1 {
			t.Fatalf("[%s] placeholders=%d, want 1\nOUT:\n%s", dc.name, got, out)
		}
		if len(args) != 1 || args[0] != nil {
			t.Fatalf("[%s] arg[0]=%v, want nil", dc.name, args)
		}
	}
}

// rowsLike minimal adapter (no driver/mock allocs) for benchmarks
type rowsLike struct {
	cols []string
	data [][]any
	i    int
}

func (r *rowsLike) Columns() ([]string, error) { return r.cols, nil }
func (r *rowsLike) Next() bool                 { r.i++; return r.i <= len(r.data) }
func (r *rowsLike) Scan(dest ...any) error {
	row := r.data[r.i-1]
	for i := range dest {
		reflect.ValueOf(dest[i]).Elem().Set(reflect.ValueOf(row[i]))
	}
	return nil
}
func (r *rowsLike) Err() error   { return nil }
func (r *rowsLike) Close() error { return nil }

func scanAllRowsLike(rows *rowsLike, dest any) error {
	rv := reflect.ValueOf(dest)
	rv = rv.Elem()
	elemT := rv.Type().Elem()

	switch elemT.Kind() {
	case reflect.Struct, reflect.Pointer:
		isPtr := elemT.Kind() == reflect.Pointer
		structT := elemT
		if isPtr {
			structT = elemT.Elem()
		}

		cols := rows.cols
		plan, err := makeScanPlan(cols, structT) // <-- now returns (*scanPlan, error)
		if err != nil {
			return err
		}

		for rows.Next() {
			elem := reflect.New(structT).Elem()

			for i := range cols {
				switch plan.kinds[i] {
				case ckSink:
					plan.targets[i] = plan.sinks[i]
				case ckScanner, ckValue:
					// Allocate intermediates along the path and address the leaf
					fv := fieldByIndexAlloc(elem, plan.fPath[i])
					plan.targets[i] = fv.Addr().Interface()
				case ckPtr:
					// **T holder reused (as in production code)
					h := plan.holders[i]
					h.Elem().SetZero()
					plan.targets[i] = h.Interface()
				}
			}
			if err := rows.Scan(plan.targets...); err != nil {
				return err
			}
			// Copy *T into pointer fields allocating intermediates if needed
			for _, i := range plan.ptrIdx {
				setFieldByIndex(elem, plan.fPath[i], plan.holders[i].Elem())
			}
			if isPtr {
				rv.Set(reflect.Append(rv, elem.Addr()))
			} else {
				rv.Set(reflect.Append(rv, elem))
			}
		}
		return rows.Err()

	default:
		// primitives: 1 column
		for rows.Next() {
			item := reflect.New(elemT).Elem()
			if err := rows.Scan(item.Addr().Interface()); err != nil {
				return err
			}
			rv.Set(reflect.Append(rv, item))
		}
		return rows.Err()
	}
}

// BenchmarkMapper_MapOnly_Struct_1k measures the overhead of mapping 1,000
// rows into structs using the scan plan and without involving a real DB driver.
func BenchmarkMapper_MapOnly_Struct_1k(b *testing.B) {
	type Row struct {
		ID   int    `db:"id"`
		Name string `db:"name"`
		Ok   bool   `db:"ok"`
	}
	cols := []string{"id", "name", "ok"}
	data := make([][]any, 1000)
	for i := 0; i < 1000; i++ {
		data[i] = []any{i, "name", i%2 == 0}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var out []Row
		rows := &rowsLike{cols: cols, data: data}
		if err := scanAllRowsLike(rows, &out); err != nil {
			b.Fatal(err)
		}
		if len(out) != 1000 {
			b.Fatal("len != 1000")
		}
	}
}

// BenchmarkMapper_MapOnly_Primitive_1k measures mapping cost for 1,000
// primitive values (single column) into a slice of ints.
func BenchmarkMapper_MapOnly_Primitive_1k(b *testing.B) {
	cols := []string{"v"}
	data := make([][]any, 1000)
	for i := 0; i < 1000; i++ {
		data[i] = []any{i}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var out []int
		rows := &rowsLike{cols: cols, data: data}
		if err := scanAllRowsLike(rows, &out); err != nil {
			b.Fatal(err)
		}
		if len(out) != 1000 {
			b.Fatal("len != 1000")
		}
	}
}

func scanOneStructRowsLike(rows *rowsLike, dest any) error {
	rv := reflect.ValueOf(dest)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return fmt.Errorf("dest must be a non-nil pointer")
	}
	rv = rv.Elem()
	if rv.Kind() != reflect.Struct {
		return fmt.Errorf("dest must be a pointer to struct")
	}

	cols := rows.cols
	fmap := fieldIndexMap(rv.Type()) // column name -> fieldInfo (with path []int)

	targets := make([]any, len(cols))
	sinks := make([]any, len(cols))
	var post []func()

	for i, c := range cols {
		if fi, ok := fmap[c]; ok {
			// leaf type
			sf := rv.Type().FieldByIndex(fi.index)
			ft := sf.Type

			// field implements sql.Scanner?
			if reflect.PointerTo(ft).Implements(scannerIface) {
				fv := fieldByIndexAlloc(rv, fi.index)
				targets[i] = fv.Addr().Interface()
				continue
			}

			// *T field -> use **T and copy post-scan
			if ft.Kind() == reflect.Pointer {
				holder := reflect.New(ft) // **T
				targets[i] = holder.Interface()
				path := append([]int(nil), fi.index...) // capture a copy of the path
				post = append(post, func() {
					setFieldByIndex(rv, path, holder.Elem())
				})
				continue
			}

			// value field: allocate intermediates along the path
			fv := fieldByIndexAlloc(rv, fi.index)
			targets[i] = fv.Addr().Interface()
		} else {
			// unmapped column -> sink
			targets[i] = &sinks[i]
		}
	}

	if !rows.Next() {
		return fmt.Errorf("no rows")
	}
	if err := rows.Scan(targets...); err != nil {
		return err
	}
	for _, f := range post {
		f()
	}
	return nil
}

// benchRow is a representative struct used in single-row struct mapping benchmarks.
type benchRow struct {
	A int    `db:"a"`
	B string `db:"b"`
	C int64  `db:"c"`
	D bool   `db:"d"`
	E string `db:"e"`
}

// BenchmarkMapper_Scan_OneRow_Struct_MapOnly measures allocations and runtime
// for scanning a single row into a struct using the lightweight rowsLike.
func BenchmarkMapper_Scan_OneRow_Struct_MapOnly(b *testing.B) {
	cols := []string{"a", "b", "c", "d", "e"}
	data := [][]any{{7, "name", int64(42), true, "note"}}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var out benchRow
		rows := &rowsLike{cols: cols, data: data}
		if err := scanOneStructRowsLike(rows, &out); err != nil {
			b.Fatal(err)
		}
	}
}

type benchResult struct{ id, rows int64 }

func (r benchResult) LastInsertId() (int64, error) { return r.id, nil }
func (r benchResult) RowsAffected() (int64, error) { return r.rows, nil }

type benchExecer struct{}

func (benchExecer) ExecContext(_ context.Context, _ string, _ ...any) (sql.Result, error) {
	return benchResult{id: 123, rows: 3}, nil
}

// BenchmarkMapper_Exec_BuildAndExec_MapOnly measures the cost of building a
// query with named params, binding, and executing via a stub execer.
func BenchmarkMapper_Exec_BuildAndExec_MapOnly(b *testing.B) {
	ex := benchExecer{}
	s := New(Postgres)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		res, err := s.Write("UPDATE t SET x=:x WHERE id IN (:ids)").
			Bind(P{"x": i, "ids": []int{i, i + 1, i + 2}}).
			Exec(ex)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := res.RowsAffected(); err != nil {
			b.Fatal(err)
		}
	}
}
