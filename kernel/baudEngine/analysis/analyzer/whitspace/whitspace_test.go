package whitspace

import "testing"

func TestAnalyze(t *testing.T) {
	analyzer := New()
	sets := analyzer.Analyze([]byte("ab bc  abc"))
	if len(sets) != 3 {
		t.Fatalf("test failed, size %d", len(sets))
	}
	if string(sets[0].Term) != "ab" {
		t.Fatal("test failed")
	}
	if string(sets[1].Term) != "bc" {
		t.Fatal("test failed")
	}
	if string(sets[2].Term) != "abc" {
		t.Fatal("test failed")
	}
}
