// fs_node_path_test.go
package fabriq

import "testing"

func TestSplitFsPath(t *testing.T) {
	cases := []struct {
		in      string
		want    []string
		wantErr bool
	}{
		{"/a", []string{"a"}, false},
		{"/a/b/c.txt", []string{"a", "b", "c.txt"}, false},
		{"", nil, true},
		{"/", nil, true},
		{"a/b", nil, true},   // must be absolute
		{"/a//b", nil, true}, // empty segment
	}
	for _, c := range cases {
		got, err := splitFsPath(c.in)
		if (err != nil) != c.wantErr {
			t.Fatalf("splitFsPath(%q) err=%v wantErr=%v", c.in, err, c.wantErr)
		}
		if err != nil {
			continue
		}
		if len(got) != len(c.want) {
			t.Fatalf("splitFsPath(%q) = %v, want %v", c.in, got, c.want)
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Fatalf("splitFsPath(%q) = %v, want %v", c.in, got, c.want)
			}
		}
	}
}
