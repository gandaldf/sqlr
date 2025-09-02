package sqlr

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"sync"
)

// colKind classifies the strategy for scanning a result column into a struct field.
type colKind uint8

const (
	ckSink    colKind = iota // column is ignored, scan into sink
	ckScanner                // field implements sql.Scanner
	ckPtr                    // field is *T (we use a **T holder)
	ckValue                  // direct value field
)

var scannerIface = reflect.TypeOf((*sql.Scanner)(nil)).Elem()
var scanPlanCache = newPlanCache(cacheSize)

// scanOne scans the current row into dest. It supports:
//   - pointer to Scanner types (with exactly one column)
//   - primitives (with exactly one column)
//   - structs (flattened mapping via `db` tags or field names)
//
// It returns detailed errors when shapes mismatch.
func scanOne(rows *sql.Rows, dest any) error {
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

// scanOneWithPlan scans the current row into dstStruct using a cached scanPlan.
// A per-scan state is allocated to hold mutable buffers safely.
func scanOneWithPlan(rows *sql.Rows, cols []string, dstStruct reflect.Value) error {
	plan, err := getScanPlan(cols, dstStruct.Type())
	if err != nil {
		return err
	}
	st := plan.newState()

	// prepare targets for this single row
	for i := range cols {
		switch plan.kinds[i] {
		case ckSink:
			st.targets[i] = st.sinks[i]
		case ckScanner, ckValue:
			fv := fieldByIndexAlloc(dstStruct, plan.fPath[i])
			st.targets[i] = fv.Addr().Interface()
		case ckPtr:
			h := st.holders[i]
			h.Elem().SetZero()
			st.targets[i] = h.Interface()
		}
	}

	if err := rows.Scan(st.targets...); err != nil {
		return err
	}
	for _, i := range plan.ptrIdx {
		setFieldByIndex(dstStruct, plan.fPath[i], st.holders[i].Elem())
	}
	return nil
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

	if rv.Len() != 0 {
		rv.Set(rv.Slice(0, 0))
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

		plan, err := getScanPlan(cols, structT)
		if err != nil {
			return err
		}
		st := plan.newState()

		for rows.Next() {
			if isPtr {
				// []*Struct: create *Struct, populate Elem and append the pointer
				ptr := reflect.New(structT)
				dst := ptr.Elem()

				for i := range cols {
					switch plan.kinds[i] {
					case ckSink:
						st.targets[i] = st.sinks[i]
					case ckScanner, ckValue:
						fv := fieldByIndexAlloc(dst, plan.fPath[i])
						st.targets[i] = fv.Addr().Interface()
					case ckPtr:
						h := st.holders[i]
						h.Elem().SetZero()
						st.targets[i] = h.Interface()
					}
				}

				if err := rows.Scan(st.targets...); err != nil {
					return err
				}
				for _, i := range plan.ptrIdx {
					setFieldByIndex(dst, plan.fPath[i], st.holders[i].Elem())
				}

				rv.Set(reflect.Append(rv, ptr))
			} else {
				// []Struct: add a zero element and populate the last one in place
				rv.Set(reflect.Append(rv, reflect.Zero(structT)))
				dst := rv.Index(rv.Len() - 1)

				for i := range cols {
					switch plan.kinds[i] {
					case ckSink:
						st.targets[i] = st.sinks[i]
					case ckScanner, ckValue:
						fv := fieldByIndexAlloc(dst, plan.fPath[i])
						st.targets[i] = fv.Addr().Interface()
					case ckPtr:
						h := st.holders[i]
						h.Elem().SetZero()
						st.targets[i] = h.Interface()
					}
				}

				if err := rows.Scan(st.targets...); err != nil {
					return err
				}
				for _, i := range plan.ptrIdx {
					setFieldByIndex(dst, plan.fPath[i], st.holders[i].Elem())
				}
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

// buildScanPlan builds an immutable scanPlan describing how each result column
// should be scanned into the destination struct type dstT.
// It determines, per column, whether to sink it, use sql.Scanner, treat as *T,
// or as a plain value. For pointer fields it also records the field types
// needed to allocate reusable **T holders in the per-scan state.
func buildScanPlan(cols []string, dstT reflect.Type) (*scanPlan, error) {
	// Normalize destination type (we operate on the concrete struct type).
	for dstT.Kind() == reflect.Pointer {
		dstT = dstT.Elem()
	}

	// Field map: column name -> fieldInfo (flattened path, ambiguity, scalar flag).
	fmap := fieldIndexMap(dstT)

	p := &scanPlan{
		kinds:         make([]colKind, len(cols)),
		fPath:         make([][]int, len(cols)),
		ptrIdx:        make([]int, 0, 8),
		ptrFieldTypes: make([]reflect.Type, len(cols)),
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

		// Case 1: field implements sql.Scanner (via value or pointer receiver).
		if reflect.PointerTo(ft).Implements(scannerIface) || ft.Implements(scannerIface) {
			p.kinds[i] = ckScanner
			p.fPath[i] = fi.index
			continue
		}

		// Case 2: pointer field (*T) scanned via a **T holder and copied post-scan.
		if ft.Kind() == reflect.Pointer {
			p.kinds[i] = ckPtr
			p.fPath[i] = fi.index
			p.ptrFieldTypes[i] = ft // keep *T to allocate **T holder in state
			p.ptrIdx = append(p.ptrIdx, i)
			continue
		}

		// Case 3: plain value field.
		p.kinds[i] = ckValue
		p.fPath[i] = fi.index
	}

	return p, nil
}

// --------------------------------
// Cache
// --------------------------------

// scanState holds per-scan mutable buffers.
// It is created from a cached scanPlan and is not shared across goroutines.
type scanState struct {
	targets []any
	sinks   []any
	holders []reflect.Value
}

// scanPlan describes how to map each result column to a struct field (immutable).
// Mutable, per-scan buffers are not stored here; they are created via newState().
type scanPlan struct {
	kinds         []colKind
	fPath         [][]int
	ptrIdx        []int
	ptrFieldTypes []reflect.Type // for ckPtr: field reflect.Type (which is a pointer type *T)
}

// newState allocates per-scan buffers sized to the plan's column count.
// Buffers are private to the scan execution and safe for reuse within a single scan loop.
func (p *scanPlan) newState() *scanState {
	n := len(p.kinds)
	st := &scanState{
		targets: make([]any, n),
		sinks:   make([]any, n),
		holders: make([]reflect.Value, n),
	}
	// Prepare addressable sinks so rows.Scan() always has a valid destination.
	for i := 0; i < n; i++ {
		st.sinks[i] = new(any)
	}
	// Pre-create **T holders (one per ckPtr column) for reuse across row scans.
	for _, i := range p.ptrIdx {
		ft := p.ptrFieldTypes[i]        // ft is a *T
		st.holders[i] = reflect.New(ft) // **T
	}
	return st
}

// planKey identifies a scanPlan by destination struct type and the column signature.
type planKey struct {
	dstType reflect.Type
	sig     string
}

// planCache implements a two-tier cache for scanPlan, similar to fieldCache.
// It bounds memory by rotating the hot and previous generations.
type planCache struct {
	mu   sync.RWMutex
	curr map[planKey]*scanPlan
	prev map[planKey]*scanPlan
	max  int
}

// newPlanCache creates a new two-tier plan cache with a max size hint.
func newPlanCache(max int) *planCache {
	if max <= 0 {
		max = cacheSize
	}
	return &planCache{
		curr: make(map[planKey]*scanPlan, max/2),
		prev: make(map[planKey]*scanPlan),
		max:  max,
	}
}

// get returns the cached scanPlan for key if present, promoting it to the
// current generation when found in the previous one.
func (c *planCache) get(k planKey) (*scanPlan, bool) {
	c.mu.RLock()
	if p, ok := c.curr[k]; ok {
		c.mu.RUnlock()
		return p, true
	}
	if p, ok := c.prev[k]; ok {
		c.mu.RUnlock()
		c.mu.Lock()
		if len(c.curr) >= c.max {
			c.prev = c.curr
			c.curr = make(map[planKey]*scanPlan, c.max/2)
		}
		c.curr[k] = p
		c.mu.Unlock()
		return p, true
	}
	c.mu.RUnlock()
	return nil, false
}

// put stores the scanPlan for the given key, rotating generations if needed.
func (c *planCache) put(k planKey, p *scanPlan) {
	c.mu.Lock()
	if len(c.curr) >= c.max {
		c.prev = c.curr
		c.curr = make(map[planKey]*scanPlan, c.max/2)
	}
	c.curr[k] = p
	c.mu.Unlock()
}

// columnsSignature returns a stable signature string for an ordered list of column names.
// It avoids allocations of a slice of bytes by using a strings.Builder and a rarely
// used delimiter to prevent collisions.
func columnsSignature(cols []string) string {
	if len(cols) == 0 {
		return ""
	}
	const sep = "\x1f" // unit separator; unlikely to appear in column names
	var b strings.Builder
	// Small capacity hint
	total := 0
	for _, c := range cols {
		total += len(c) + 1
	}
	b.Grow(total)
	for i, c := range cols {
		if i > 0 {
			b.WriteString(sep)
		}
		b.WriteString(c)
	}
	return b.String()
}

// canonicalStructType returns the underlying struct type for a possibly-pointer type.
// If the final type is not a struct, it returns the type as-is.
func canonicalStructType(t reflect.Type) reflect.Type {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t
}

// getScanPlan returns a cached scanPlan for (dst struct type, cols), or builds and caches it.
// The returned plan is immutable and safe for concurrent reuse.
func getScanPlan(cols []string, dstT reflect.Type) (*scanPlan, error) {
	dstT = canonicalStructType(dstT)
	key := planKey{dstType: dstT, sig: columnsSignature(cols)}
	if p, ok := scanPlanCache.get(key); ok {
		return p, nil
	}
	p, err := buildScanPlan(cols, dstT)
	if err != nil {
		return nil, err
	}
	scanPlanCache.put(key, p)
	return p, nil
}
