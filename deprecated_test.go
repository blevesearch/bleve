//  Copyright (c) 2026 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bleve

import (
	"errors"
	"strings"
	"testing"

	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/blevesearch/bleve/v2/registry"
	"github.com/blevesearch/bleve/v2/util"
)

// An index whose mapping references a deprecated analysis component (as one
// created while the component was still shipped would) can be built and
// reopened; only the affected field loses its analysis.
func TestIndexWithDeprecatedAnalyzer(t *testing.T) {
	tmpIndexPath := createTmpIndexPath(t)
	defer cleanupTmpIndexPath(t, tmpIndexPath)

	var im *mapping.IndexMappingImpl
	err := util.UnmarshalJSON([]byte(`{
		"analysis": {"analyzers": {"my_he": {"type": "custom",
			"tokenizer": "hebrew", "token_filters": ["stop_he"]}}},
		"default_mapping": {"properties": {
			"he": {"fields": [{"name": "he", "type": "text", "analyzer": "he", "index": true}]},
			"he2": {"fields": [{"name": "he2", "type": "text", "analyzer": "my_he", "index": true}]},
			"en": {"fields": [{"name": "en", "type": "text", "analyzer": "en", "index": true}]}
		}}}`), &im)
	if err != nil {
		t.Fatal(err)
	}
	if err = im.Validate(); !errors.Is(err, registry.ErrDeprecatedComponent) {
		t.Fatalf("expected deprecated component error from Validate, got %v", err)
	}

	idx, err := New(tmpIndexPath, im)
	if err != nil {
		t.Fatalf("expected index to build, got %v", err)
	}
	err = idx.Index("doc", map[string]interface{}{
		"he":  "שלום עולם",
		"he2": "shalom olam",
		"en":  "hello worlds",
	})
	if err != nil {
		t.Fatal(err)
	}
	err = idx.Close()
	if err != nil {
		t.Fatal(err)
	}

	idx, err = Open(tmpIndexPath)
	if err != nil {
		t.Fatalf("expected index to reopen, got %v", err)
	}
	defer func() {
		if err := idx.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	search := func(q string) (uint64, error) {
		res, err := idx.Search(NewSearchRequest(NewQueryStringQuery(q)))
		if err != nil {
			return 0, err
		}
		return res.Total, nil
	}

	// the unaffected field keeps working
	if n, err := search("en:world"); err != nil || n != 1 {
		t.Fatalf("expected 1 hit on the en field, got %d, %v", n, err)
	}

	// queries that need to analyze text with a deprecated analyzer fail, and
	// say why
	for _, q := range []string{"he:shalom", `he:"shalom olam"`, "he2:shalom"} {
		if _, err := search(q); !errors.Is(err, registry.ErrDeprecatedComponent) {
			t.Fatalf("expected %q to fail as deprecated, got %v", q, err)
		}
	}
	mq := NewMatchQuery("world")
	mq.SetField("en")
	mq.Analyzer = "he"
	if _, err = idx.Search(NewSearchRequest(mq)); !errors.Is(err, registry.ErrDeprecatedComponent) {
		t.Fatalf("expected an explicit he analyzer to fail as deprecated, got %v", err)
	}
	mq.Analyzer = "nope"
	if _, err = idx.Search(NewSearchRequest(mq)); err == nil ||
		errors.Is(err, registry.ErrDeprecatedComponent) ||
		!strings.Contains(err.Error(), "no analyzer named 'nope' registered") {
		t.Fatalf("expected an unknown analyzer to fail as before, got %v", err)
	}

	// queries that do not analyze text run against what is on disk; the
	// affected fields were indexed without analysis, so as a single term
	tq := NewTermQuery("שלום עולם")
	tq.SetField("he")
	res, err := idx.Search(NewSearchRequest(tq))
	if err != nil || res.Total != 1 {
		t.Fatalf("expected the whole value as one term, got %v, %v", res, err)
	}
}

func TestNewIndexWithUnknownAnalyzerStillFails(t *testing.T) {
	tmpIndexPath := createTmpIndexPath(t)
	defer cleanupTmpIndexPath(t, tmpIndexPath)

	im := NewIndexMapping()
	im.DefaultAnalyzer = "nope"
	_, err := New(tmpIndexPath, im)
	if err == nil || errors.Is(err, registry.ErrDeprecatedComponent) {
		t.Fatalf("expected a plain unknown analyzer error, got %v", err)
	}
}
