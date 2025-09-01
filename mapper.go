package sqlr

import (
	"database/sql"
	"fmt"
	"reflect"
)

// colKind classifies the strategy for scanning a result column into a struct field.
type colKind uint8

// scanPlan precomputes how to scan a row set into a struct efficiently.
// It caches kinds/paths/targets and reusable holders for pointer fields.
type scanPlan struct {
	kinds   []colKind
	fPath   [][]int         // field path (flattened index path)
	targets []any           // reused on every Scan()
	sinks   []any           // &sink for ignored columns
	holders []reflect.Value // for ckPtr: reusable **T holders
	ptrIdx  []int           // indices in fPath of pointer fields (for post-copy)
}

const (
	ckSink    colKind = iota // column is ignored, scan into sink
	ckScanner                // field implements sql.Scanner
	ckPtr                    // field is *T (we use a **T holder)
	ckValue                  // direct value field
)

var scannerIface = reflect.TypeOf((*sql.Scanner)(nil)).Elem()

// scanInto scans the current row into dest. It supports:
//   - pointer to Scanner types (with exactly one column)
//   - primitives (with exactly one column)
//   - structs (flattened mapping via `db` tags or field names)
//
// It returns detailed errors when shapes mismatch.
func scanInto(rows *sql.Rows, dest any) error {
	rv := reflect.ValueOf(dest)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return fmt.Errorf("sqlr: dest must be a non-nil pointer")
	}
	rv = rv.Elem()

	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	if reflect.PointerTo(rv.Type()).Implements(scannerIface) {
		if len(cols) != 1 {
			return fmt.Errorf("sqlr: Scan on type %s requires 1 column, got %d", rv.Type(), len(cols))
		}
		return rows.Scan(rv.Addr().Interface())
	}
	if rv.Kind() != reflect.Struct {
		if len(cols) != 1 {
			return fmt.Errorf("sqlr: Scan on non-struct type requires 1 column, got %d", len(cols))
		}
		return rows.Scan(rv.Addr().Interface())
	}

	return scanOneWithPlan(rows, cols, rv)
}

// scanOneWithPlan scans the current row into dstStruct using a reusable scanPlan.
func scanOneWithPlan(rows *sql.Rows, cols []string, dstStruct reflect.Value) error {
	plan, err := makeScanPlan(cols, dstStruct.Type())
	if err != nil {
		return err
	}

	// prepare targets for this single row
	for i := range cols {
		switch plan.kinds[i] {
		case ckSink:
			plan.targets[i] = plan.sinks[i]
		case ckScanner, ckValue:
			fv := fieldByIndexAlloc(dstStruct, plan.fPath[i])
			plan.targets[i] = fv.Addr().Interface()
		case ckPtr:
			h := plan.holders[i]
			h.Elem().SetZero()
			plan.targets[i] = h.Interface()
		}
	}

	if err := rows.Scan(plan.targets...); err != nil {
		return err
	}
	for _, i := range plan.ptrIdx {
		setFieldByIndex(dstStruct, plan.fPath[i], plan.holders[i].Elem())
	}
	return nil
}

// makeScanPlan builds a plan describing how each result column should be scanned
// into the destination struct type dstT. For every column it determines:
//   - whether it is ignored (sink),
//   - whether the target field implements sql.Scanner (ckScanner),
//   - whether the target field is a pointer that must be handled via a **T holder (ckPtr),
//   - or a plain assignable value (ckValue).
//
// It also preallocates reusable sinks and **T holders so that subsequent scans
// (per-row) only need to reset/reuse them without reallocations.
// If a column name resolves to multiple fields (ambiguous mapping), it returns
// ErrFieldAmbiguous.
func makeScanPlan(cols []string, dstT reflect.Type) (*scanPlan, error) {
	// Normalize destination type (we operate on the concrete struct type).
	for dstT.Kind() == reflect.Pointer {
		dstT = dstT.Elem()
	}

	// Field map: column name -> fieldInfo (flattened path, ambiguity, scalar flag).
	fmap := fieldIndexMap(dstT)

	p := &scanPlan{
		kinds:   make([]colKind, len(cols)),
		fPath:   make([][]int, len(cols)),
		targets: make([]any, len(cols)),           // filled per row
		sinks:   make([]any, len(cols)),           // &sink placeholders for ignored columns
		holders: make([]reflect.Value, len(cols)), // **T holders for pointer fields
		// ptrIdx filled on-demand when we encounter ckPtr columns
	}

	// Precreate addressable sinks so rows.Scan() always has a valid destination.
	for i := range p.sinks {
		p.sinks[i] = new(any)
	}

	for i, col := range cols {
		fi, ok := fmap[col]
		if !ok {
			// Column not mapped to any field -> sink it.
			p.kinds[i] = ckSink
			continue
		}
		if fi.ambiguous {
			// Multiple candidate fields for the same column name.
			return nil, fmt.Errorf("%w: %q", ErrFieldAmbiguous, col)
		}

		// Leaf field type (after walking the flattened index path).
		sf := dstT.FieldByIndex(fi.index)
		ft := sf.Type

		// Case 1: the field type T is scan-capable via *T implementing sql.Scanner.
		// Note: we purposely check PointerTo(ft) to catch value fields whose pointer
		// receiver implements the interface (e.g., sql.NullString, custom Scanner types).
		if reflect.PointerTo(ft).Implements(scannerIface) || ft.Implements(scannerIface) {
			p.kinds[i] = ckScanner
			p.fPath[i] = fi.index
			continue
		}

		// Case 2: pointer field (*T) — scanned via a reusable **T holder;
		// after Scan we copy the *T into the actual field (handling NULL as nil).
		if ft.Kind() == reflect.Pointer {
			p.kinds[i] = ckPtr
			p.fPath[i] = fi.index
			p.holders[i] = reflect.New(ft) // allocate **T holder once (reused per row)
			p.ptrIdx = append(p.ptrIdx, i)
			continue
		}

		// Case 3: plain value field — scanned directly into the address of the leaf.
		p.kinds[i] = ckValue
		p.fPath[i] = fi.index
	}

	return p, nil
}

// scanAll scans all rows into a slice. It supports slices of structs, of *struct,
// and of primitives/Scanner types (with exactly one column).
func scanAll(rows *sql.Rows, dest any) error {
	rv := reflect.ValueOf(dest)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return fmt.Errorf("sqlr: dest must be a non-nil pointer")
	}
	rv = rv.Elem()
	if rv.Kind() != reflect.Slice {
		return fmt.Errorf("sqlr: ScanAll requires a pointer to slice")
	}

	elemT := rv.Type().Elem()
	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	switch elemT.Kind() {
	case reflect.Struct, reflect.Pointer:
		// Support []Struct and []*Struct
		var structT reflect.Type
		isPtr := elemT.Kind() == reflect.Pointer
		if isPtr {
			if elemT.Elem().Kind() != reflect.Struct {
				return fmt.Errorf("sqlr: slice of pointers to non-struct")
			}
			structT = elemT.Elem()
		} else {
			structT = elemT
		}

		plan, err := makeScanPlan(cols, structT)
		if err != nil {
			return err
		}

		for rows.Next() {
			// Create destination element (Struct or *Struct)
			elem := reflect.New(structT).Elem()

			// Prepare targets for this row; reset holders for pointer fields
			for i := range cols {
				switch plan.kinds[i] {
				case ckSink:
					plan.targets[i] = plan.sinks[i]
				case ckScanner, ckValue:
					fv := fieldByIndexAlloc(elem, plan.fPath[i])
					plan.targets[i] = fv.Addr().Interface()
				case ckPtr:
					h := plan.holders[i]
					h.Elem().SetZero() // *T = nil for this row
					plan.targets[i] = h.Interface()
				}
			}

			if err := rows.Scan(plan.targets...); err != nil {
				return err
			}

			// Post: copy *T from the **T holders into the actual fields
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
		// Primitive/Scanner → must be 1 column
		if len(cols) != 1 {
			return fmt.Errorf("sqlr: ScanAll on slice of non-struct requires 1 column, got %d", len(cols))
		}
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

// fieldByIndexAlloc walks a struct by index path, allocating intermediate
// pointer nodes on the way (but NOT allocating the leaf pointer itself).
func fieldByIndexAlloc(root reflect.Value, path []int) reflect.Value {
	v := root
	for i, idx := range path {
		f := v.Field(idx)
		if i == len(path)-1 {
			// Leaf: return field as-is (if it's a pointer, keep it as pointer)
			return f
		}
		// Intermediate: allocate if pointer and nil; then descend
		if f.Kind() == reflect.Pointer {
			if f.IsNil() {
				f.Set(reflect.New(f.Type().Elem()))
			}
			v = f.Elem()
		} else {
			v = f
		}
	}
	return v
}

// setFieldByIndex sets value into the field at path on root,
// allocating any intermediate pointer nodes. 'value' is typically *T.
func setFieldByIndex(root reflect.Value, path []int, value reflect.Value) {
	v := root
	for i, idx := range path {
		f := v.Field(idx)
		if i == len(path)-1 {
			f.Set(value)
			return
		}
		if f.Kind() == reflect.Pointer {
			if f.IsNil() {
				f.Set(reflect.New(f.Type().Elem()))
			}
			v = f.Elem()
		} else {
			v = f
		}
	}
}
