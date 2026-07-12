package postgres

import (
	"fmt"
	"regexp"
	"strings"
)

// insightsSkipRe matches regions the write/file-func denylists must treat as
// opaque: single-quoted string literals (with ” escapes), line comments, and
// block comments — so a word like "delete" inside a literal or comment never
// falsely trips the guard.
var insightsSkipRe = regexp.MustCompile(`'(?:[^']|'')*'|--[^\n]*|/\*[\s\S]*?\*/`)

// insightsWriteRe matches a data-modifying keyword as a whole word, applied to
// the literal/comment-stripped statement — closing the vector a leading-prefix
// check alone would miss (a data-modifying CTE like
// `WITH x AS (DELETE ... RETURNING *) SELECT ...` still starts with "with").
var insightsWriteRe = regexp.MustCompile(`(?i)\b(insert|update|delete|drop|alter|create|truncate|grant|revoke|copy|merge|vacuum)\b`)

// insightsFileFuncRe matches DuckDB-style file-access table functions in call
// position (read_csv/read_parquet/read_json/..., parquet_scan, glob). They are
// reads, so the write-keyword denylist misses them, but they can exfiltrate
// server-local files.
var insightsFileFuncRe = regexp.MustCompile(`(?i)\b(read_[a-z0-9_]+|parquet_scan|glob)\s*\(`)

// precheckInsightsReadOnly rejects anything that isn't a single read-only
// SELECT/WITH statement. It is defense-in-depth only — the READ ONLY
// transaction QueryRaw runs in is the real enforcement (Postgres itself
// rejects any write/DDL, including one embedded in a CTE); this guard exists
// to fail fast with a clear error before a round-trip to the database, and to
// close a vector a read-only transaction doesn't cover on its own:
// DuckDB-style file-access functions that would otherwise read server-local
// files (those are reads as far as Postgres/the RO tx is concerned).
func precheckInsightsReadOnly(sql string) error {
	s := strings.TrimSpace(sql)
	if s == "" {
		return fmt.Errorf("fabriq: empty query")
	}
	// Prefix detection runs on the original (unstripped) text: it only looks
	// at the leading keyword, which stripping never touches, and stripping
	// costs nothing to skip here.
	lower := strings.ToLower(s)
	if !hasInsightsPrefix(lower, "select") && !hasInsightsPrefix(lower, "with") {
		return fmt.Errorf("fabriq: only read-only SELECT/WITH queries are allowed")
	}
	// Strip string literals/comments BEFORE the multi-statement check — a
	// legitimate single statement can contain a literal ';' (e.g. a string
	// comparison like `props->>'q' = 'a;b'`), which must not be mistaken for
	// statement stacking. Allow a single trailing semicolon.
	stripped := insightsSkipRe.ReplaceAllString(s, " ")
	if strings.Contains(strings.TrimSuffix(strings.TrimSpace(stripped), ";"), ";") {
		return fmt.Errorf("fabriq: multiple statements are not allowed")
	}
	if insightsWriteRe.MatchString(stripped) {
		return fmt.Errorf("fabriq: data-modifying keyword found")
	}
	if insightsFileFuncRe.MatchString(stripped) {
		return fmt.Errorf("fabriq: file-access functions are not allowed")
	}
	return nil
}

// hasInsightsPrefix reports whether lower starts with kw followed by a word
// boundary (whitespace, '(', or end-of-string) — so "selectfoo" and "withdraw"
// don't falsely match "select"/"with".
func hasInsightsPrefix(lower, kw string) bool {
	if !strings.HasPrefix(lower, kw) {
		return false
	}
	rest := lower[len(kw):]
	return rest == "" || rest[0] == ' ' || rest[0] == '\t' || rest[0] == '\n' || rest[0] == '('
}
