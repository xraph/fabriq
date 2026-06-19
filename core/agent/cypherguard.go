// core/agent/cypherguard.go
package agent

import (
	"fmt"
	"regexp"
)

// mutatingClause matches openCypher write keywords as whole words
// (case-insensitive). This is a defense-in-depth denylist for the
// graph_traverse escape hatch — it is NOT a substitute for a read-only graph
// connection (a property value that happens to contain a keyword would be
// rejected as a false positive; deployments handling untrusted callers should
// also use a read-only driver).
var mutatingClause = regexp.MustCompile(`(?i)\b(CREATE|MERGE|DELETE|DETACH|SET|REMOVE|DROP|CALL|FOREACH|LOAD)\b`)

// readOnlyCypher returns an error if the cypher contains a mutating clause.
func readOnlyCypher(cypher string) error {
	if loc := mutatingClause.FindStringIndex(cypher); loc != nil {
		return fmt.Errorf("agent: graph_traverse is read-only; rejected clause %q", cypher[loc[0]:loc[1]])
	}
	return nil
}
