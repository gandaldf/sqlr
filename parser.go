package sqlr

import (
	"database/sql/driver"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
)

// scalar is a wrapper to force scalar binding semantics.
type scalar struct {
	v any
}

// ambiguousSentinel is used to bubble up an "ambiguous field" condition
// through singleLookup without changing call signatures.
type ambiguousSentinel struct {
	name string
}

var structIndexCache = newFieldCache(cacheSize)

// parse performs the SQL building and parameter binding. It walks the input
// SQL, substitutes :name placeholders (including rows blocks :name{a,b}),
// tracks placeholder counting, and emits dialect-specific placeholders.
func parse(dialect Dialect, q string, inputs []any, config Config) (string, []any, error) {
	// Build fallback resolvers and detect fast bag (map[string]any) materialized in Bind.
	fastBag := parseFastBag(inputs)
	lookupFB, rowsLookupFB, err := makeMultiResolver(inputs)
	if err != nil {
		return "", nil, err
	}
	lookup := parseMakeValueLookup(fastBag, lookupFB)
	rowsLookup := parseMakeRowsLookup(fastBag, rowsLookupFB)

	// Rough placeholder estimate to pre-size buffers.
	est := strings.Count(q, ":") - strings.Count(q, "::")
	if est < 0 {
		est = 0
	}
	args := make([]any, 0, est)

	var buf strings.Builder
	extraPer := 1
	switch dialect {
	case Postgres, SQLServer:
		extraPer = 4
	}
	buf.Grow(len(q) + 16 + est*extraPer)

	// Parser state
	const (
		sText = iota
		sSQ   // '...'
		sDQ   // "..."
		sBT   // `...` (MySQL/SQLite)
		sBR   // [...] (SQL Server)
		sLC   // line comment -- or # (MySQL only)
		sBC   // block comment /* ... */
		sDQD  // $tag$ ... $tag$ (dollar-quoted)
	)
	state := sText
	var dqTag string // active $tag$ for PG-like dollar-quoting
	n := 0

	for i := 0; i < len(q); {
		c := q[i]

		switch state {
		case sText:
			// 1) Try entering a quoted/comment state (writes the opener to buf if any)
			if newState, newI, newTag, ok := parseTryEnterSpecial(q, i, dialect, &buf); ok {
				state, i, dqTag = newState, newI, newTag
				continue
			}
			// 2) Try a :name or :name{...} placeholder
			if parseIsParamStart(q, i) {
				newI, handled, err := parseHandlePlaceholder(q, i, dialect, config, lookup, rowsLookup, &buf, &args, &n)
				if err != nil {
					return "", nil, err
				}
				if handled {
					i = newI
					continue
				}
			}
			// 3) Plain text byte
			buf.WriteByte(c)
			i++

		case sSQ:
			// single-quoted literal with backslash and doubled-quote handling
			if c == '\\' {
				buf.WriteByte(c)
				i++
				if i < len(q) {
					buf.WriteByte(q[i])
					i++
				}
				continue
			}
			buf.WriteByte(c)
			i++
			if c == '\'' {
				if i < len(q) && q[i] == '\'' {
					buf.WriteByte(q[i])
					i++
				} else {
					state = sText
				}
			}

		case sDQ:
			// double-quoted literal with backslash and doubled-quote handling
			if c == '\\' {
				buf.WriteByte(c)
				i++
				if i < len(q) {
					buf.WriteByte(q[i])
					i++
				}
				continue
			}
			buf.WriteByte(c)
			i++
			if c == '"' {
				if i < len(q) && q[i] == '"' {
					buf.WriteByte(q[i])
					i++
				} else {
					state = sText
				}
			}

		case sBT:
			// backtick-quoted identifier (MySQL/SQLite)
			buf.WriteByte(c)
			i++
			if c == '`' {
				if i < len(q) && q[i] == '`' {
					buf.WriteByte(q[i])
					i++
				} else {
					state = sText
				}
			}

		case sBR:
			// bracket-quoted identifier (SQL Server)
			buf.WriteByte(c)
			i++
			if c == ']' {
				if i < len(q) && q[i] == ']' {
					buf.WriteByte(q[i])
					i++
				} else {
					state = sText
				}
			}

		case sLC:
			// line comment: -- ... or # ... (MySQL)
			buf.WriteByte(c)
			i++
			if c == '\n' || c == '\r' {
				state = sText
			}

		case sBC:
			// block comment: /* ... */
			buf.WriteByte(c)
			i++
			if c == '*' && i < len(q) && q[i] == '/' {
				buf.WriteByte('/')
				i++
				state = sText
			}

		case sDQD:
			// dollar-quoted block: $tag$ ... $tag$
			if dqTag == "" {
				buf.WriteString(q[i:])
				i = len(q)
				break
			}
			p := strings.Index(q[i:], dqTag)
			if p < 0 {
				buf.WriteString(q[i:])
				i = len(q)
			} else {
				buf.WriteString(q[i : i+p])
				buf.WriteString(dqTag)
				i += p + len(dqTag)
				dqTag = ""
				state = sText
			}
		}
	}

	return buf.String(), args, nil
}

// parseFastBag returns the last input if it is a map[string]any, otherwise nil.
func parseFastBag(inputs []any) map[string]any {
	if len(inputs) == 0 {
		return nil
	}
	if m, ok := inputs[len(inputs)-1].(map[string]any); ok && m != nil {
		return m
	}
	return nil
}

// parseMakeValueLookup returns a composite lookup that checks the fast bag first,
// then falls back to the generic resolver.
func parseMakeValueLookup(fastBag map[string]any, fallback func(string) (any, bool)) func(string) (any, bool) {
	return func(name string) (any, bool) {
		if fastBag != nil {
			if v, ok := fastBag[name]; ok {
				return v, true
			}
		}
		return fallback(name)
	}
}

// parseMakeRowsLookup returns a composite rows-lookup that checks the fast bag first,
// then falls back to the generic rows resolver.
func parseMakeRowsLookup(
	fastBag map[string]any,
	fallback func(string) ([]rowVal, bool),
) func(string) ([]rowVal, bool) {
	return func(name string) ([]rowVal, bool) {
		if fastBag != nil {
			if v, ok := fastBag[name]; ok {
				if rows, ok := rowsFromSliceValue(reflect.ValueOf(v)); ok {
					return rows, true
				}
			}
		}
		return fallback(name)
	}
}

// parseIsParamStart checks if q[i] starts a :name placeholder (not a :: cast).
func parseIsParamStart(q string, i int) bool {
	return q[i] == ':' && (i+1) < len(q) && q[i+1] != ':' && !(i > 0 && q[i-1] == ':')
}

// parseTryEnterSpecial inspects q[i] for comment/string/identifier openers,
// writes them to buf and returns the new state and cursor when matched.
func parseTryEnterSpecial(q string, i int, dialect Dialect, buf *strings.Builder) (newState int, newI int, dqTag string, ok bool) {
	c := q[i]

	// line comment: -- or # (MySQL)
	if c == '-' && i+1 < len(q) && q[i+1] == '-' {
		buf.WriteString("--")
		return 5 /* sLC */, i + 2, "", true
	}
	if c == '#' && dialect == MySQL {
		buf.WriteByte('#')
		return 5 /* sLC */, i + 1, "", true
	}

	// block comment: /*
	if c == '/' && i+1 < len(q) && q[i+1] == '*' {
		buf.WriteString("/*")
		return 6 /* sBC */, i + 2, "", true
	}

	// single-quoted literal
	if c == '\'' {
		buf.WriteByte(c)
		return 1 /* sSQ */, i + 1, "", true
	}

	// double-quoted literal
	if c == '"' {
		buf.WriteByte(c)
		return 2 /* sDQ */, i + 1, "", true
	}

	// backtick-quoted identifier (MySQL/SQLite)
	if c == '`' && (dialect == MySQL || dialect == SQLite) {
		buf.WriteByte(c)
		return 3 /* sBT */, i + 1, "", true
	}

	// bracket-quoted identifier (SQL Server)
	if c == '[' && dialect == SQLServer {
		buf.WriteByte(c)
		return 4 /* sBR */, i + 1, "", true
	}

	// dollar-quoted: $tag$
	if c == '$' {
		if tag, ok := readDollarTag(q[i:]); ok {
			buf.WriteString(tag)
			return 7 /* sDQD */, i + len(tag), tag, true
		}
	}

	return 0, 0, "", false
}

// parseReadName tries to read an identifier after ':' at position j.
// Returns the name and the index just after the name.
func parseReadName(q string, j int) (name string, k int, ok bool) {
	if !isAlphaUnderscore(q[j]) {
		return "", j, false
	}
	k = j + 1
	for k < len(q) && isAlphaNumUnderscore(q[k]) {
		k++
	}
	return q[j:k], k, true
}

// parseHandlePlaceholder handles :name and :name{...} from q[i] (where q[i]==':').
// On success, it advances the cursor and emits into buf/args adjusting n.
func parseHandlePlaceholder(
	q string,
	i int,
	dialect Dialect,
	config Config,
	lookup func(string) (any, bool),
	rowsLookup func(string) ([]rowVal, bool),
	buf *strings.Builder,
	args *[]any,
	n *int,
) (newI int, handled bool, err error) {
	j := i + 1
	if j >= len(q) {
		return i, false, nil
	}

	name, k, ok := parseReadName(q, j)
	if !ok {
		return i, false, nil
	}

	// Enforce MaxNameLen
	if config.MaxNameLen > 0 && len(name) > config.MaxNameLen {
		return 0, false, fmt.Errorf("%w: %q (%d > %d)", ErrParamNameTooLong, name, len(name), config.MaxNameLen)
	}

	// :name{...} rows-block
	if k < len(q) && q[k] == '{' {
		k2, cols, ok := readCols(q, k)
		if !ok {
			return 0, true, fmt.Errorf("%w: :%s{...}", ErrRowsMalformed, name)
		}
		if len(cols) == 0 {
			return 0, true, fmt.Errorf("%w: :%s{...} without columns", ErrRowsMalformed, name)
		}

		rows, ok := rowsLookup(name)
		if !ok {
			return 0, true, fmt.Errorf("%w: :%s{...}", ErrParamMissing, name)
		}
		if len(rows) == 0 {
			return 0, true, fmt.Errorf("%w: :%s{...}", ErrRowsEmpty, name)
		}

		if err := parseEnsureAdd(*n, len(rows)*len(cols), config); err != nil {
			return 0, true, err
		}
		if err := parseEmitRowsBlock(name, cols, rows, dialect, buf, args, n); err != nil {
			return 0, true, err
		}
		return k2, true, nil
	}

	// Simple :name (single value or slice expansion)
	v, ok := lookup(name)
	if !ok {
		return 0, true, fmt.Errorf("%w: %s", ErrParamMissing, name)
	}

	// Bubble up ambiguous from fallback path only
	if a, isAmbiguous := v.(ambiguousSentinel); isAmbiguous {
		return 0, true, fmt.Errorf("%w: %q", ErrFieldAmbiguous, a.name)
	}

	return parseEmitValue(name, v, dialect, config, buf, args, n, k)
}

// parseEmitValue emits either a single placeholder or a list (slice/array expansion).
func parseEmitValue(
	name string,
	v any,
	dialect Dialect,
	config Config,
	buf *strings.Builder,
	args *[]any,
	n *int,
	k int,
) (newI int, handled bool, err error) {
	// Single placeholder for scalar wrapper / driver.Valuer
	if sc, ok := v.(scalar); ok {
		if err := parseEnsureAdd(*n, 1, config); err != nil {
			return 0, true, err
		}
		*n++
		writePlaceholder(buf, dialect, *n)
		*args = append(*args, sc.v)
		return k, true, nil
	}
	if _, ok := v.(driver.Valuer); ok {
		if err := parseEnsureAdd(*n, 1, config); err != nil {
			return 0, true, err
		}
		*n++
		writePlaceholder(buf, dialect, *n)
		*args = append(*args, v)
		return k, true, nil
	}

	// []byte (or byte-slice-like) → single placeholder
	if bs, ok := v.([]byte); ok {
		if err := parseEnsureAdd(*n, 1, config); err != nil {
			return 0, true, err
		}
		*n++
		writePlaceholder(buf, dialect, *n)
		*args = append(*args, bs)
		return k, true, nil
	}
	rv := reflect.ValueOf(v)
	if rv.IsValid() && rv.Kind() == reflect.Slice && rv.Type().Elem().Kind() == reflect.Uint8 {
		if err := parseEnsureAdd(*n, 1, config); err != nil {
			return 0, true, err
		}
		*n++
		writePlaceholder(buf, dialect, *n)
		if rv.Type() != reflect.TypeOf([]byte(nil)) && rv.Type().ConvertibleTo(reflect.TypeOf([]byte(nil))) {
			*args = append(*args, rv.Convert(reflect.TypeOf([]byte(nil))).Interface())
		} else {
			*args = append(*args, v)
		}
		return k, true, nil
	}

	// Slice/array expansion (non-byte)
	if (rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array) && rv.Type().Elem().Kind() != reflect.Uint8 {
		ln := rv.Len()
		if ln == 0 {
			return 0, true, fmt.Errorf("%w: %s", ErrSliceEmpty, name)
		}
		if err := parseEnsureAdd(*n, ln, config); err != nil {
			return 0, true, err
		}

		parseGrowArgs(args, ln)
		parseGrowSQL(buf, ln)

		for t := 0; t < ln; t++ {
			if t > 0 {
				buf.WriteString(", ")
			}
			*n++
			writePlaceholder(buf, dialect, *n)
			*args = append(*args, rv.Index(t).Interface())
		}
		return k, true, nil
	}

	// Fallback: single placeholder
	if err := parseEnsureAdd(*n, 1, config); err != nil {
		return 0, true, err
	}
	*n++
	writePlaceholder(buf, dialect, *n)
	*args = append(*args, v)
	return k, true, nil
}

// parseEmitRowsBlock emits VALUES-like tuples for :name{col1,col2,...} using rows.
func parseEmitRowsBlock(
	name string,
	cols []string,
	rows []rowVal,
	dialect Dialect,
	buf *strings.Builder,
	args *[]any,
	n *int,
) error {
	// Pre-compute fast-path structures (map keys and struct paths).
	var (
		colKeys       []reflect.Value
		mapKeyT       reflect.Type
		colPathByType map[reflect.Type][][]int
	)

	rv0 := deIndirect(reflect.ValueOf(rows[0]))
	if rv0.IsValid() && rv0.Kind() == reflect.Map {
		mapKeyT = rv0.Type().Key()
		if mapKeyT.Kind() == reflect.String || reflect.TypeOf("").ConvertibleTo(mapKeyT) {
			colKeys = make([]reflect.Value, len(cols))
			for i, col := range cols {
				kv := reflect.ValueOf(col)
				if kv.Type() != mapKeyT && kv.Type().ConvertibleTo(mapKeyT) {
					kv = kv.Convert(mapKeyT)
				}
				colKeys[i] = kv
			}
		}
	}
	if rv0.IsValid() && rv0.Kind() == reflect.Struct {
		colPathByType = make(map[reflect.Type][][]int, 4)
		baseT := rv0.Type()
		baseMap := fieldIndexMap(baseT)
		paths := make([][]int, len(cols))
		for i, col := range cols {
			fi, ok := baseMap[col]
			if !ok {
				return fmt.Errorf("%w: %q in :%s{...} (record 0)", ErrColumnNotFound, col, name)
			}
			if fi.ambiguous {
				return fmt.Errorf("%w: %q in :%s{...} (record 0)", ErrFieldAmbiguous, col, name)
			}
			paths[i] = fi.index
		}
		colPathByType[baseT] = paths
	}

	need := len(rows) * len(cols)
	parseGrowArgs(args, need)
	parseGrowSQLRows(buf, len(cols), len(rows))

	for r := 0; r < len(rows); r++ {
		if r > 0 {
			buf.WriteString(", ")
		}
		buf.WriteByte('(')

		rv := deIndirect(reflect.ValueOf(rows[r]))
		useMapFast := (colKeys != nil && rv.IsValid() && rv.Kind() == reflect.Map && rv.Type().Key() == mapKeyT)

		for cidx := range cols {
			if cidx > 0 {
				buf.WriteString(", ")
			}

			var v any
			var ok bool

			if rv.IsValid() && rv.Kind() == reflect.Struct {
				paths, has := colPathByType[rv.Type()]
				if !has {
					if colPathByType == nil {
						colPathByType = make(map[reflect.Type][][]int, 4)
					}
					fm := fieldIndexMap(rv.Type())
					paths = make([][]int, len(cols))
					for iCol, col := range cols {
						fi, hit := fm[col]
						if !hit {
							return fmt.Errorf("%w: %q in :%s{...} (record %d)", ErrColumnNotFound, col, name, r)
						}
						if fi.ambiguous {
							return fmt.Errorf("%w: %q in :%s{...} (record %d)", ErrFieldAmbiguous, col, name, r)
						}
						paths[iCol] = fi.index
					}
					colPathByType[rv.Type()] = paths
				}
				v, ok = getValueByPathAny(rv, paths[cidx])
			} else if useMapFast {
				mv := rv.MapIndex(colKeys[cidx])
				if mv.IsValid() {
					v, ok = mv.Interface(), true
				}
			} else {
				v, ok = getColValue(rows[r], cols[cidx])
			}

			if !ok {
				return fmt.Errorf("%w: %q in :%s{...} (record %d)", ErrColumnNotFound, cols[cidx], name, r)
			}

			*n++
			writePlaceholder(buf, dialect, *n)
			*args = append(*args, v)
		}

		buf.WriteByte(')')
	}
	return nil
}

// parseEnsureAdd enforces MaxParams, returning an error if the limit would be exceeded.
func parseEnsureAdd(cur, add int, cfg Config) error {
	if cfg.MaxParams > 0 && cur+add > cfg.MaxParams {
		return fmt.Errorf("%w: requested=%d, limit=%d", ErrTooManyParams, cur+add, cfg.MaxParams)
	}
	return nil
}

// parseGrowArgs grows the args slice capacity geometrically to accommodate 'need' more items.
func parseGrowArgs(args *[]any, need int) {
	extra := need - (cap(*args) - len(*args))
	if extra <= 0 {
		return
	}
	// Geometric growth: double current capacity and add extra,
	// but ensure it's at least len+need.
	newCap := cap(*args)*2 + extra
	minCap := len(*args) + need
	if newCap < minCap {
		newCap = minCap
	}
	na := make([]any, len(*args), newCap)
	copy(na, *args)
	*args = na
}

// parseGrowSQL reserves space in the SQL buffer for a slice expansion of length ln.
func parseGrowSQL(buf *strings.Builder, ln int) {
	// approx per placeholder + ", " separators
	const approxPerPlaceholder = 6
	if ln <= 0 {
		return
	}
	buf.Grow(ln*approxPerPlaceholder + 2*(ln-1))
}

// parseGrowSQLRows reserves buffer space for :rows{...} expansion.
func parseGrowSQLRows(buf *strings.Builder, numCols, numRows int) {
	const approxPerPlaceholder = 6
	if numCols <= 0 || numRows <= 0 {
		return
	}
	need := numCols * numRows
	perRowSep := 2 // "(" + ")"
	if numCols > 1 {
		perRowSep += 2 * (numCols - 1) // ", " between cols
	}
	extraSQL := need*approxPerPlaceholder + numRows*perRowSep
	if numRows > 1 {
		extraSQL += 2 * (numRows - 1) // ", " between rows
	}
	buf.Grow(extraSQL)
}

// writePlaceholder emits a dialect-specific placeholder token for argument idx.
func writePlaceholder(b *strings.Builder, d Dialect, idx int) {
	switch d {
	case Postgres:
		b.WriteByte('$')
		var tmp [20]byte
		n := strconv.AppendInt(tmp[:0], int64(idx), 10)
		b.Write(n)
	case SQLServer:
		b.WriteString("@p")
		var tmp [20]byte
		n := strconv.AppendInt(tmp[:0], int64(idx), 10)
		b.Write(n)
	default: // MySQL, SQLite
		b.WriteByte('?')
	}
}

// --------------------------------
// Resolver
// --------------------------------

type rowVal any

// makeMultiResolver returns two resolvers:
//  1. lookup(name) → single value for :name
//  2. rowsLookup(name) → []rowVal for :name{...} blocks
//
// Resolution is "last one wins": later Bind() inputs override earlier ones.
func makeMultiResolver(inputs []any) (
	lookup func(string) (any, bool),
	rowsLookup func(string) ([]rowVal, bool),
	err error,
) {
	// Last-one-wins resolution: iterate inputs in reverse order
	return func(name string) (any, bool) {
			for i := len(inputs) - 1; i >= 0; i-- {
				if v, ok := singleLookup(inputs[i], name); ok {
					return v, true
				}
			}
			return nil, false
		},
		func(name string) ([]rowVal, bool) {
			for i := len(inputs) - 1; i >= 0; i-- {
				if rows, ok := singleRowsLookup(inputs[i], name); ok {
					return rows, true
				}
			}
			return nil, false
		},
		nil
}

// singleLookup resolves a :name from a single Bind() input.
// Supports map-like, struct-like (flattened), and pointers/interfaces thereof.
func singleLookup(in any, name string) (any, bool) {
	v := reflect.ValueOf(in)
	if !v.IsValid() {
		return nil, false
	}
	for v.IsValid() && (v.Kind() == reflect.Interface || v.Kind() == reflect.Pointer) {
		if v.IsNil() {
			return nil, false
		}
		v = v.Elem()
	}
	// FAST-PATH: map[string]any
	if m, ok := v.Interface().(map[string]any); ok {
		val, ok := m[name]
		return val, ok
	}
	switch v.Kind() {
	case reflect.Map:
		keyT := v.Type().Key()
		key := reflect.ValueOf(name)
		if key.Type() != keyT {
			if key.Type().ConvertibleTo(keyT) {
				key = key.Convert(keyT)
			} else {
				return nil, false
			}
		}
		mv := v.MapIndex(key)
		if mv.IsValid() {
			return mv.Interface(), true
		}
		return nil, false
	case reflect.Struct:
		m := fieldIndexMap(v.Type())
		if fi, ok := m[name]; ok {
			if fi.ambiguous {
				// bubble sentinel; parse() will turn this into ErrFieldAmbiguous
				return ambiguousSentinel{name: name}, true
			}
			val, _ := getValueByPathAny(v, fi.index)
			if fi.scalar {
				return scalar{v: val}, true
			}
			return val, true
		}
	}
	return nil, false
}

// getColValue extracts a value by column name from a row (struct/map, possibly wrapped).
// It returns (value, true) on success or (nil, false) if the column is missing/unsupported.
func getColValue(row any, col string) (any, bool) {
	// FAST-PATH: map[string]any
	if m, ok := row.(map[string]any); ok {
		v, ok := m[col]
		return v, ok
	}
	rv := reflect.ValueOf(row)
	for rv.IsValid() && (rv.Kind() == reflect.Pointer || rv.Kind() == reflect.Interface) {
		if rv.IsNil() {
			return nil, false
		}
		rv = rv.Elem()
	}
	switch rv.Kind() {
	case reflect.Map:
		keyT := rv.Type().Key()
		key := reflect.ValueOf(col)
		if key.Type() != keyT {
			if key.Type().ConvertibleTo(keyT) {
				key = key.Convert(keyT)
			} else {
				return nil, false
			}
		}
		v := rv.MapIndex(key)
		if v.IsValid() {
			return v.Interface(), true
		}
		return nil, false
	case reflect.Struct:
		m := fieldIndexMap(rv.Type())
		if fi, ok := m[col]; ok {
			val, _ := getValueByPathAny(rv, fi.index)
			return val, true
		}
		return nil, false
	default:
		return nil, false
	}
}

// singleRowsLookup resolves :name{...} rows from a single Bind() input.
// Accepts: map[string] → []struct/[]map, or bare []struct/[]map with conventional name "rows".
func singleRowsLookup(in any, name string) ([]rowVal, bool) {
	v := reflect.ValueOf(in)
	if !v.IsValid() {
		return nil, false
	}

	// Case 1: map with the given key pointing to a []struct/[]map (possibly via interface or pointer)
	if v.Kind() == reflect.Map && v.Type().Key().Kind() == reflect.String {
		mv := v.MapIndex(reflect.ValueOf(name))
		if mv.IsValid() {
			return rowsFromSliceValue(mv)
		}
	}

	// Case 2: convenience convention—if name == "rows" and 'in' itself is []struct/[]map
	if name == "rows" {
		return rowsFromSliceValue(v)
	}

	return nil, false
}

// rowsFromSliceValue returns the slice elements as []rowVal if v is a slice of
// struct or map (possibly wrapped in interface/pointer). Returns (nil,false) otherwise.
func rowsFromSliceValue(v reflect.Value) ([]rowVal, bool) {
	// Unwrap interface{} and pointer until the actual value
	for v.IsValid() && (v.Kind() == reflect.Interface || v.Kind() == reflect.Pointer) {
		if v.IsNil() {
			return nil, false
		}
		v = v.Elem()
	}

	if v.Kind() != reflect.Slice {
		return nil, false
	}

	ln := v.Len()
	out := make([]rowVal, ln)
	for i := 0; i < ln; i++ {
		out[i] = v.Index(i).Interface()
	}
	if ln == 0 {
		return out, true
	}

	// Accept struct or map for individual elements
	el := v.Index(0)

	// Unwrap potential interface/pointer element
	for el.IsValid() && (el.Kind() == reflect.Interface || el.Kind() == reflect.Pointer) {
		if el.IsNil() {
			return nil, false
		}
		el = el.Elem()
	}
	if el.Kind() == reflect.Struct || el.Kind() == reflect.Map {
		return out, true
	}
	return nil, false
}

// fieldIndexMap returns a mapping from column name → fieldInfo for the given type.
// It flattens nested structs (excluding time.Time), honors `db:"name"` tags,
// and supports `db:"name,scalar"` to force scalar binding.
// The result is cached in a two-tier cache.
func fieldIndexMap(t reflect.Type) map[string]fieldInfo {
	if m, ok := structIndexCache.get(t); ok {
		return m
	}

	// Normalize to struct
	base := t
	for base.Kind() == reflect.Pointer {
		base = base.Elem()
	}
	if base.Kind() != reflect.Struct {
		m := make(map[string]fieldInfo)
		structIndexCache.put(t, m)
		return m
	}

	m := make(map[string]fieldInfo, base.NumField())

	visited := map[reflect.Type]bool{}
	var walk func(rt reflect.Type, path []int)

	walk = func(rt reflect.Type, path []int) {
		// Follow pointers for current type
		for rt.Kind() == reflect.Pointer {
			rt = rt.Elem()
		}
		if rt.Kind() != reflect.Struct {
			return
		}
		if visited[rt] {
			return
		}
		visited[rt] = true
		defer delete(visited, rt)

		for i := 0; i < rt.NumField(); i++ {
			f := rt.Field(i)
			if f.PkgPath != "" { // unexported
				continue
			}
			tag := f.Tag.Get("db")
			if tag == "-" {
				continue
			}
			name := f.Name
			scalar := false
			if tag != "" {
				parts := strings.Split(tag, ",")
				if parts[0] != "" {
					name = parts[0]
				}
				for _, p := range parts[1:] {
					if strings.TrimSpace(p) == "scalar" {
						scalar = true
					}
				}
			}

			ft := f.Type

			// Decide whether to flatten this field
			if shouldFlatten(ft) {
				// Recurse into element (if pointer, Elem())
				nextT := ft
				if nextT.Kind() == reflect.Pointer {
					nextT = nextT.Elem()
				}
				walk(nextT, appendIndex(path, i))
				continue
			}

			// Leaf: handle collisions
			if prev, exists := m[name]; exists {
				// Mark as ambiguous; index is irrelevant once ambiguous.
				if !prev.ambiguous {
					m[name] = fieldInfo{ambiguous: true}
				}
				// If already ambiguous, leave it as-is.
				continue
			}
			m[name] = fieldInfo{index: appendIndex(path, i), scalar: scalar}
		}
	}

	walk(base, nil)
	structIndexCache.put(t, m)
	return m
}

// shouldFlatten decides whether to descend into ft (struct or *struct).
func shouldFlatten(ft reflect.Type) bool {
	// If *T implements sql.Scanner → treat as leaf (no flatten)
	if reflect.PointerTo(ft).Implements(scannerIface) || ft.Implements(scannerIface) {
		return false
	}
	tt := ft
	if tt.Kind() == reflect.Pointer {
		tt = tt.Elem()
	}
	if tt.Kind() != reflect.Struct {
		return false
	}
	// Do not flatten time.Time (common leaf struct)
	if tt.PkgPath() == "time" && tt.Name() == "Time" {
		return false
	}
	return true
}

// appendIndex returns a new index path with idx appended.
func appendIndex(path []int, idx int) []int {
	out := make([]int, len(path)+1)
	copy(out, path)
	out[len(path)] = idx
	return out
}

// --------------------------------
// Cache
// --------------------------------

// fieldInfo describes a leaf field: its full index path and whether it's marked
// as "scalar" via tag option (no slice expansion).
type fieldInfo struct {
	index     []int // full index path for FieldByIndex-like ops
	scalar    bool
	ambiguous bool // true if multiple fields with same name found (only for top-level fields)
}

// fieldCache implements a two-tier map with cheap rotation to bound memory.
// 'curr' is the hot set; 'prev' is the previous generation. Lookups promote.
type fieldCache struct {
	mu   sync.RWMutex
	curr map[reflect.Type]map[string]fieldInfo
	prev map[reflect.Type]map[string]fieldInfo
	max  int
}

// newFieldCache creates a new simple two-tier cache with cheap rotation to limit memory usage.
func newFieldCache(max int) *fieldCache {
	if max <= 0 {
		max = cacheSize
	}
	return &fieldCache{
		curr: make(map[reflect.Type]map[string]fieldInfo, max/2),
		prev: make(map[reflect.Type]map[string]fieldInfo),
		max:  max,
	}
}

// get looks up the field index map for type t.
func (c *fieldCache) get(t reflect.Type) (map[string]fieldInfo, bool) {
	c.mu.RLock()
	if m, ok := c.curr[t]; ok {
		c.mu.RUnlock()
		return m, true
	}
	if m, ok := c.prev[t]; ok {
		c.mu.RUnlock()
		c.mu.Lock()
		if len(c.curr) >= c.max {
			c.prev = c.curr
			c.curr = make(map[reflect.Type]map[string]fieldInfo, c.max/2)
		}
		c.curr[t] = m
		c.mu.Unlock()
		return m, true
	}
	c.mu.RUnlock()
	return nil, false
}

// put stores the field index map for type t.
func (c *fieldCache) put(t reflect.Type, idx map[string]fieldInfo) {
	c.mu.Lock()
	if len(c.curr) >= c.max {
		c.prev = c.curr
		c.curr = make(map[reflect.Type]map[string]fieldInfo, c.max/2)
	}
	c.curr[t] = idx
	c.mu.Unlock()
}

// --------------------------------
// Utils
// --------------------------------

// isAlphaUnderscore reports whether b is [A-Za-z_] .
func isAlphaUnderscore(b byte) bool {
	return (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z') || b == '_'
}

// isAlphaNumUnderscore reports whether b is [A-Za-z0-9_] .
func isAlphaNumUnderscore(b byte) bool {
	return isAlphaUnderscore(b) || (b >= '0' && b <= '9')
}

// readCols parses :name{a,b,c} column list starting at '{' and returns the index
// after '}', the list of columns, and whether parsing succeeded.
func readCols(q string, i int) (int, []string, bool) {
	i++
	start := i
	var cols []string
	for i < len(q) {
		if q[i] == '}' {
			if start < i {
				tok := strings.TrimSpace(q[start:i])
				if tok == "" {
					return -1, nil, false
				}
				cols = append(cols, tok)
			}
			return i + 1, cols, true
		}
		if q[i] == ',' {
			tok := strings.TrimSpace(q[start:i])
			if tok == "" {
				return -1, nil, false
			}
			cols = append(cols, tok)
			i++
			start = i
			continue
		}
		i++
	}
	return -1, nil, false
}

// readDollarTag detects a dollar-quoted opening tag ("$tag$") at the start of s.
// It returns the full tag (e.g. "$tag$") and true if found.
func readDollarTag(s string) (string, bool) {
	if len(s) < 2 || s[0] != '$' {
		return "", false
	}
	j := 1
	for j < len(s) && isAlphaNumUnderscore(s[j]) {
		j++
	}
	if j < len(s) && s[j] == '$' {
		return s[:j+1], true
	}
	return "", false
}

// deIndirect unwraps interface and pointers until a concrete value (or nil).
func deIndirect(v reflect.Value) reflect.Value {
	for v.IsValid() && (v.Kind() == reflect.Interface || v.Kind() == reflect.Pointer) {
		if v.IsNil() {
			return v
		}
		v = v.Elem()
	}
	return v
}

// getValueByPathAny extracts the value at the end of 'path' from 'root'.
// If a pointer along the path is nil, it returns (nil, true) to represent SQL NULL.
// Returns (value, true) on success, or (nil, false) on structural mismatch.
func getValueByPathAny(root reflect.Value, path []int) (any, bool) {
	v := root
	// Initial unwrap of interface
	for v.IsValid() && v.Kind() == reflect.Interface {
		if v.IsNil() {
			return nil, true
		}
		v = v.Elem()
	}
	for i, idx := range path {
		// Follow pointers if necessary
		for v.IsValid() && v.Kind() == reflect.Pointer {
			if v.IsNil() {
				return nil, true
			}
			v = v.Elem()
		}
		if !v.IsValid() || v.Kind() != reflect.Struct {
			return nil, false
		}
		v = v.Field(idx)
		if i == len(path)-1 {
			// Leaf
			for v.IsValid() && v.Kind() == reflect.Interface {
				if v.IsNil() {
					return nil, true
				}
				v = v.Elem()
			}
			if v.Kind() == reflect.Pointer {
				if v.IsNil() {
					return nil, true
				}
				return v.Interface(), true
			}
			return v.Interface(), true
		}
	}
	return nil, false
}
