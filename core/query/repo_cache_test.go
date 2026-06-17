package query

import (
	"testing"
)

func TestExtractIDs(t *testing.T) {
	type r struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	ids, err := extractIDs([]*r{{ID: "a1"}, {ID: "a2"}, {Name: "no-id"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 || ids[0] != "a1" || ids[1] != "a2" {
		t.Fatalf("ids=%v (want [a1 a2], skipping the id-less row)", ids)
	}
}
