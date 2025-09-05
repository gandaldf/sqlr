// Package sqlr is a minimal SQL builder and result mapper designed to stay very close to the SQL you already write. It focuses on the 90% use-case: turn :named placeholders into driver args, expand IN (:ids) automatically, support bulk VALUES with a compact :something{...} syntax, and scan rows into your structs efficiently — all without a heavy ORM or a fluent DSL.

package sqlr
