package bleve

import (
	"fmt"
	"math"
	"os"
	"strings"
	"testing"

	"github.com/blevesearch/bleve/v2/search"
)

func floatClose(a, b float64) bool {
	return math.Abs(a-b) <= 1e-6*(1+math.Abs(b))
}

// walkExpl fails on any nil node and, for nodes whose message declares an
// aggregation ("sum of:" / "product of:"), verifies the node's Value equals the
// sum/product of its children. Nodes with a non-aggregating message (e.g.
// "saturation(...)") are only checked for nil children, not arithmetic.
// Returns the node count.
func walkExpl(t *testing.T, e *search.Explanation, path string) int {
	t.Helper()
	if e == nil {
		t.Fatalf("NIL explanation node at %s", path)
	}
	n := 1
	for i, c := range e.Children {
		cp := fmt.Sprintf("%s > child[%d]", path, i)
		if c == nil {
			t.Fatalf("NIL child at %s (parent message=%q)", cp, e.Message)
		}
		n += walkExpl(t, c, cp)
	}
	if len(e.Children) == 0 {
		return n
	}
	isSum := strings.Contains(e.Message, "sum of:")
	isProduct := strings.Contains(e.Message, "product of:")
	if !isSum && !isProduct {
		return n // non-aggregating node (e.g. saturation): child is explanatory only
	}
	agg := 1.0
	if isSum {
		agg = 0.0
	}
	for _, c := range e.Children {
		if isSum {
			agg += c.Value
		} else {
			agg *= c.Value
		}
	}
	kind := "product"
	if isSum {
		kind = "sum"
	}
	if !floatClose(agg, e.Value) {
		t.Errorf("arithmetic mismatch at %s (message=%q): node.Value=%.8f but %s-of-children=%.8f",
			path, e.Message, e.Value, kind, agg)
	}
	return n
}

// TestExplainDisjunctionTreeWellFormed is the regression test for the
// DisjunctionQueryScorer explain bug (first "sum of:" child came back nil).
// Runs under both scoring models; BM25 is the model FTS uses and the one the
// §25 impact-table / NormByte optimizations target.
func TestExplainDisjunctionTreeWellFormed(t *testing.T) {
	for _, model := range []string{"", "bm25"} {
		name := model
		if name == "" {
			name = "tfidf-default"
		}
		t.Run(name, func(t *testing.T) {
			tmp, err := os.MkdirTemp("", "bleve-explain-*")
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(tmp)

			m := NewIndexMapping()
			m.ScoringModel = model // "" also defaults to tf-idf; explicit for clarity

			idx, err := New(tmp, m) // default index type = scorch (writes v18)
			if err != nil {
				t.Fatal(err)
			}
			defer idx.Close()

			docs := map[string]string{
				"a": "the quick brown fox jumps over the lazy dog",
				"b": "quick brown quick brown clever",
				"c": "brown bears and brown crates",
				"d": "nothing relevant here at all",
			}
			batch := idx.NewBatch()
			for id, text := range docs {
				if err := batch.Index(id, map[string]any{"text": text}); err != nil {
					t.Fatal(err)
				}
			}
			if err := idx.Batch(batch); err != nil {
				t.Fatal(err)
			}

			// Multi-term match (OR) => disjunction of TermSearchers => DisjunctionQueryScorer.Score.
			q := NewMatchQuery("quick brown fox")
			q.SetField("text")
			req := NewSearchRequest(q)
			req.Explain = true
			req.Size = 10

			res, err := idx.Search(req)
			if err != nil {
				t.Fatal(err)
			}
			if len(res.Hits) == 0 {
				t.Fatal("expected hits")
			}

			for _, hit := range res.Hits {
				if hit.Expl == nil {
					t.Fatalf("doc %s: nil top-level explanation", hit.ID)
				}
				if !floatClose(hit.Score, hit.Expl.Value) {
					t.Errorf("doc %s: hit.Score=%.8f != Expl.Value=%.8f", hit.ID, hit.Score, hit.Expl.Value)
				}
				nodes := walkExpl(t, hit.Expl, "doc "+hit.ID)
				t.Logf("doc %s: score=%.6f, explanation nodes=%d\n%s", hit.ID, hit.Score, nodes, hit.Expl)
			}
		})
	}
}
