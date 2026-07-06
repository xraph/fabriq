//go:build integration

package fabriqtest

import (
	"testing"
	"time"
)

func sleep250(t *testing.T) {
	t.Helper()
	time.Sleep(250 * time.Millisecond)
}

func TestPrimaryStandby_Replicates(t *testing.T) {
	primaryDSN, standbyDSN, proxy := StartPrimaryStandby(t)
	_ = proxy

	ApplyDDL(t, primaryDSN, []string{
		`CREATE TABLE repl_probe (id int primary key)`,
		`INSERT INTO repl_probe VALUES (1)`,
	})
	// Give replication a moment, then read on the standby.
	var got []string
	for i := 0; i < 40; i++ {
		got = QueryStrings(t, standbyDSN, `SELECT id::text FROM repl_probe WHERE id = 1`)
		if len(got) == 1 && got[0] == "1" {
			return
		}
		sleep250(t)
	}
	t.Fatalf("row never replicated to standby: %v", got)
}
