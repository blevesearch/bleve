//  Copyright (c) 2024 Couchbase, Inc.
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
	"context"
	"sort"
	"strconv"
	"time"

	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/query"
	index "github.com/blevesearch/bleve_index_api"
)

// A preSearchResultProcessor processes the data in
// the preSearch result from multiple
// indexes in an alias and merges them together to
// create the final preSearch result
type preSearchResultProcessor interface {
	// adds the preSearch result to the processor
	add(*SearchResult, string)
	// updates the final search result with the finalized
	// data from the processor
	finalize(*SearchResult)
}

// -----------------------------------------------------------------------------
// KNN preSearchResultProcessor for handling KNN presearch results
type knnPreSearchResultProcessor struct {
	addFn      func(sr *SearchResult, indexName string)
	finalizeFn func(sr *SearchResult)
}

func (k *knnPreSearchResultProcessor) add(sr *SearchResult, indexName string) {
	if k.addFn != nil {
		k.addFn(sr, indexName)
	}
}

func (k *knnPreSearchResultProcessor) finalize(sr *SearchResult) {
	if k.finalizeFn != nil {
		k.finalizeFn(sr)
	}
}

// -----------------------------------------------------------------------------
// Synonym preSearchResultProcessor for handling Synonym presearch results
type synonymPreSearchResultProcessor struct {
	finalizedFts search.FieldTermSynonymMap
}

func newSynonymPreSearchResultProcessor() *synonymPreSearchResultProcessor {
	return &synonymPreSearchResultProcessor{}
}

func (s *synonymPreSearchResultProcessor) add(sr *SearchResult, indexName string) {
	// Check if SynonymResult or the synonym data key is nil
	if sr.SynonymResult == nil {
		return
	}

	// Attempt to cast PreSearchResults to FieldTermSynonymMap

	// Merge with finalizedFts or initialize it if nil
	if s.finalizedFts == nil {
		s.finalizedFts = sr.SynonymResult
	} else {
		s.finalizedFts.MergeWith(sr.SynonymResult)
	}
}

func (s *synonymPreSearchResultProcessor) finalize(sr *SearchResult) {
	// Set the finalized synonym data to the PreSearchResults
	if s.finalizedFts != nil {
		sr.SynonymResult = s.finalizedFts
	}
}

type bm25PreSearchResultProcessor struct {
	docCount         float64 // bm25 specific stats
	fieldCardinality map[string]int
}

func newBM25PreSearchResultProcessor() *bm25PreSearchResultProcessor {
	return &bm25PreSearchResultProcessor{
		fieldCardinality: make(map[string]int),
	}
}

// TODO How will this work for queries other than term queries?
func (b *bm25PreSearchResultProcessor) add(sr *SearchResult, indexName string) {
	if sr.BM25Stats != nil {
		b.docCount += sr.BM25Stats.DocCount
		for field, cardinality := range sr.BM25Stats.FieldCardinality {
			b.fieldCardinality[field] += cardinality
		}
	}
}

func (b *bm25PreSearchResultProcessor) finalize(sr *SearchResult) {
	sr.BM25Stats = &search.BM25Stats{
		DocCount:         b.docCount,
		FieldCardinality: b.fieldCardinality,
	}
}

// -----------------------------------------------------------------------------
// more_like_this preSearchResultProcessor: merges, for every more_like_this
// node of the query, the term stats that each member of the alias
// contributed
type mltPreSearchResultProcessor struct {
	merged map[string]*mltMergedNode
}

// mltMergedNode accumulates the contributions for one more_like_this node,
// independently of the order in which the members answer.
//
// A source document is kept once, from whichever member reported it first;
// its term frequencies are the same in every copy. A term's df and document
// count are added up once per contribution that reported the term, so that
// the idf of every term is computed from the members that know the term.
// A contribution carrying source documents is a like_ids one; a
// contribution carrying only terms is a like_text one, whose tf came from
// analyzing the text and is the same in every member.
type mltMergedNode struct {
	docs      map[string]*search.MLTSourceDoc
	docOrder  []string
	terms     map[string]*search.MLTTermStat
	termOrder []string
}

func newMLTPreSearchResultProcessor() *mltPreSearchResultProcessor {
	return &mltPreSearchResultProcessor{
		merged: make(map[string]*mltMergedNode),
	}
}

func mltTermKey(st *search.MLTTermStat) string {
	return st.Field + "\x00" + st.Term
}

// addStat folds the df and document count of a term reported by one
// contribution into the node; counted holds the terms this contribution
// already accounted for, so that a term in two source documents of the same
// member is counted once.
func (n *mltMergedNode) addStat(st *search.MLTTermStat, counted map[string]struct{}) {
	key := mltTermKey(st)
	cur, ok := n.terms[key]
	if !ok {
		cur = &search.MLTTermStat{Field: st.Field, Term: st.Term, TF: st.TF}
		n.terms[key] = cur
		n.termOrder = append(n.termOrder, key)
	}
	if _, done := counted[key]; done {
		return
	}
	counted[key] = struct{}{}
	cur.DF += st.DF
	cur.DocCount += st.DocCount
}

func (p *mltPreSearchResultProcessor) add(sr *SearchResult, _ string) {
	for key, stats := range sr.MLTStats {
		if stats == nil {
			continue
		}
		node, ok := p.merged[key]
		if !ok {
			node = &mltMergedNode{
				docs:  make(map[string]*search.MLTSourceDoc),
				terms: make(map[string]*search.MLTTermStat),
			}
			p.merged[key] = node
		}
		counted := make(map[string]struct{})
		for _, st := range stats.Terms {
			node.addStat(st, counted)
		}
		for _, doc := range stats.SourceDocs {
			if _, dup := node.docs[doc.ID]; !dup {
				cp := &search.MLTSourceDoc{ID: doc.ID, Terms: make([]*search.MLTTermStat, 0, len(doc.Terms))}
				for _, st := range doc.Terms {
					stCopy := *st
					cp.Terms = append(cp.Terms, &stCopy)
				}
				node.docs[doc.ID] = cp
				node.docOrder = append(node.docOrder, doc.ID)
			}
			for _, st := range doc.Terms {
				node.addStat(st, counted)
			}
		}
	}
}

func (p *mltPreSearchResultProcessor) finalize(sr *SearchResult) {
	if len(p.merged) == 0 {
		return
	}
	sr.MLTStats = make(map[string]*search.MLTStats, len(p.merged))
	for key, node := range p.merged {
		merged := &search.MLTStats{}
		if len(node.docs) > 0 {
			// like_ids: the documents, each term carrying the merged df
			// and document count, so that an enclosing alias can merge
			// this contribution the same way
			for _, id := range node.docOrder {
				doc := node.docs[id]
				out := &search.MLTSourceDoc{ID: id, Terms: make([]*search.MLTTermStat, 0, len(doc.Terms))}
				for _, st := range doc.Terms {
					m := node.terms[mltTermKey(st)]
					out.Terms = append(out.Terms, &search.MLTTermStat{
						Field: st.Field, Term: st.Term, TF: st.TF, DF: m.DF, DocCount: m.DocCount,
					})
				}
				merged.SourceDocs = append(merged.SourceDocs, out)
				merged.FoundIDs = append(merged.FoundIDs, id)
			}
			sort.Strings(merged.FoundIDs)
		} else {
			for _, termKey := range node.termOrder {
				st := *node.terms[termKey]
				merged.Terms = append(merged.Terms, &st)
			}
		}
		sr.MLTStats[key] = merged
	}
}

// validateMoreLikeThis validates every more_like_this node of the query,
// including its fields against the mapping when one is known.
func validateMoreLikeThis(nodes []*query.MoreLikeThisQuery, m mapping.IndexMapping) error {
	for _, node := range nodes {
		if err := node.Validate(); err != nil {
			return err
		}
		if err := node.ValidateFields(m); err != nil {
			return err
		}
	}
	return nil
}

// requestHasMoreLikeThis reports whether the request's query tree contains
// a more_like_this node.
func requestHasMoreLikeThis(req *SearchRequest) bool {
	return req != nil && len(query.ExtractMoreLikeThisQueries(req.Query)) > 0
}

// collectMoreLikeThisStats is the leaf side of the more_like_this
// preSearch: it gathers from this index the term stats of every
// more_like_this node, keyed by the node's position in the query tree.
func collectMoreLikeThisStats(ctx context.Context, req *SearchRequest, reader index.IndexReader,
	m mapping.IndexMapping) (map[string]*search.MLTStats, error) {
	nodes := query.ExtractMoreLikeThisQueries(req.Query)
	if len(nodes) == 0 {
		return nil, nil
	}
	if err := validateMoreLikeThis(nodes, m); err != nil {
		return nil, err
	}
	rv := make(map[string]*search.MLTStats, len(nodes))
	for idx, node := range nodes {
		stats, err := node.CollectStats(ctx, reader, m)
		if err != nil {
			return nil, err
		}
		rv[strconv.Itoa(idx)] = stats
	}
	return rv, nil
}

// rewriteMoreLikeThis substitutes every more_like_this node of the query by
// the standard query built from the stats gathered during preSearch. A node
// no index could resolve becomes a match_none query.
func rewriteMoreLikeThis(q query.Query, preSearchResult *SearchResult, opts query.RewriteOptions) (query.Query, error) {
	return query.ReplaceMoreLikeThisQueries(q, func(idx int, mlt *query.MoreLikeThisQuery) (query.Query, error) {
		var stats *search.MLTStats
		if preSearchResult != nil {
			stats = preSearchResult.MLTStats[strconv.Itoa(idx)]
		}
		return mlt.Rewrite(stats, opts), nil
	})
}

// mltRewriteOptions returns how a request's more_like_this nodes are
// rewritten: with the idf of the mapping's scoring model, and with notes
// whenever the rewritten query is going to be returned.
func mltRewriteOptions(req *SearchRequest, m mapping.IndexMapping) query.RewriteOptions {
	return query.RewriteOptions{
		BM25:  isBM25Enabled(m),
		Notes: req.RewriteOnly || req.Explain,
	}
}

// rewriteOnlyResult is the result of a request that asked for the
// rewritten query instead of the hits.
func rewriteOnlyResult(req *SearchRequest, rewritten query.Query, status *SearchStatus, cost uint64,
	took time.Duration) *SearchResult {
	if status == nil {
		status = &SearchStatus{Errors: make(map[string]error)}
	}
	rv := &SearchResult{
		Status:         status,
		Hits:           search.DocumentMatchCollection{},
		TotalRelation:  TotalRelationEq,
		Cost:           cost,
		Took:           took,
		RewrittenQuery: &RewrittenQuery{Query: rewritten},
	}
	if req.Explain {
		rv.Request = req
	}
	return rv
}

// -----------------------------------------------------------------------------
// Master struct that can hold any number of presearch result processors
type compositePreSearchResultProcessor struct {
	presearchResultProcessors []preSearchResultProcessor
}

// Implements the add method, which forwards to all the internal processors
func (m *compositePreSearchResultProcessor) add(sr *SearchResult, indexName string) {
	for _, p := range m.presearchResultProcessors {
		p.add(sr, indexName)
	}
}

// Implements the finalize method, which forwards to all the internal processors
func (m *compositePreSearchResultProcessor) finalize(sr *SearchResult) {
	for _, p := range m.presearchResultProcessors {
		p.finalize(sr)
	}
}

// -----------------------------------------------------------------------------
// Function to create the appropriate preSearchResultProcessor(s)
func createPreSearchResultProcessor(req *SearchRequest, flags *preSearchFlags) preSearchResultProcessor {
	// return nil for invalid input
	if flags == nil || req == nil {
		return nil
	}
	var processors []preSearchResultProcessor
	// Add KNN processor if the request has KNN
	if flags.knn {
		if knnProcessor := newKnnPreSearchResultProcessor(req); knnProcessor != nil {
			processors = append(processors, knnProcessor)
		}
	}
	// Add Synonym processor if the request has Synonym
	if flags.synonyms {
		if synonymProcessor := newSynonymPreSearchResultProcessor(); synonymProcessor != nil {
			processors = append(processors, synonymProcessor)
		}
	}
	if flags.bm25 {
		if bm25Processtor := newBM25PreSearchResultProcessor(); bm25Processtor != nil {
			processors = append(processors, bm25Processtor)
		}
	}
	if flags.mlt {
		processors = append(processors, newMLTPreSearchResultProcessor())
	}
	// Return based on the number of processors, optimizing for the common case of 1 processor
	// If there are no processors, return nil
	switch len(processors) {
	case 0:
		return nil
	case 1:
		return processors[0]
	default:
		return &compositePreSearchResultProcessor{
			presearchResultProcessors: processors,
		}
	}
}
