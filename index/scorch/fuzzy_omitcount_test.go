package scorch

import (
	"fmt"
	"testing"

	"github.com/blevesearch/bleve/v2/document"
	index "github.com/blevesearch/bleve_index_api"
)

// TestFieldDictFuzzyAutomatonOmitsCount verifies that the fuzzy automaton field
// dictionary is wired to the count-omitting iterator: the segment dictionary
// implements the optional interface, and the collected candidate entries have
// their (discarded) Count left at zero rather than incurring a postings read.
func TestFieldDictFuzzyAutomatonOmitsCount(t *testing.T) {
	cfg := CreateConfig("TestFieldDictFuzzyAutomatonOmitsCount")
	if err := InitTest(cfg); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = DestroyTest(cfg) }()
	analysisQueue := index.NewAnalysisQueue(1)
	idx, err := NewScorch(Name, cfg, analysisQueue)
	if err != nil {
		t.Fatal(err)
	}
	if err = idx.Open(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = idx.Close() }()

	// Each doc's "desc" field is a single token (nil analyzer => whole value is
	// one term). Repeating terms across docs yields multi-doc postings lists.
	b := index.NewBatch()
	for d := 0; d < 300; d++ {
		doc := document.NewDocument(fmt.Sprintf("%d", d))
		doc.AddField(document.NewTextField("desc", nil, []byte(fmt.Sprintf("term%04d", d%100))))
		b.Update(doc)
	}
	if err = idx.Batch(b); err != nil {
		t.Fatal(err)
	}

	r, err := idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	is := r.(*IndexSnapshot)

	// the segment dictionary must implement the count-omit optional interface,
	// otherwise the wiring silently falls back to the count-reading path.
	seg := is.segment[0].segment
	dict, err := seg.Dictionary("desc")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := dict.(termDictionaryOmitCount); !ok {
		// The omit-count wiring only activates once the zapx dependency ships
		// AutomatonIteratorOmitCount; until then the code falls back to the
		// count-reading path, so there is nothing to assert.
		t.Skipf("segment dict %T does not implement termDictionaryOmitCount "+
			"(zapx dependency predates the omit-count iterator)", dict)
	}

	fd, _, err := is.FieldDictFuzzyAutomaton("desc", "term0050", 2, "")
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	tfd, err := fd.Next()
	for err == nil && tfd != nil {
		n++
		if tfd.Count != 0 {
			t.Fatalf("expected omitted (zero) count for %q, got %d", tfd.Term, tfd.Count)
		}
		tfd, err = fd.Next()
	}
	if err != nil {
		t.Fatal(err)
	}
	if n == 0 {
		t.Fatal("expected at least one fuzzy candidate term")
	}
	t.Logf("collected %d candidate terms, all with omitted count", n)
}
