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

package collector

import (
	"context"
	"reflect"
	"time"

	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/size"
	index "github.com/blevesearch/bleve_index_api"
)

var reflectStaticSizePerSegmentSortedCollector int

func init() {
	var c PerSegmentSortedCollector
	reflectStaticSizePerSegmentSortedCollector = int(reflect.TypeOf(c).Size())
}

// PerSegmentSortedCollector collects the top N hits of a search.PerSegmentSearcher
// ordered by a sort order other than descending score: by the values of fields,
// by document id, by distance, by score as one sort among others.
//
// It visits every match, as the sort values of a match can't be known without
// looking at it, so unlike the score ordered collector it has no pruning to do
// (and so never asks a searcher for its optimized collection, whose algorithms
// all prune by score). What it makes cheap is the visit:
//
//   - the sort values of a match are read from its doc values into bytes that are
//     used as they are, with no string made of them;
//   - a match is compared to the worst of the hits kept (the threshold of a
//     sortedTopN) and turned down if it doesn't beat it, which for most matches
//     is all that is ever done to them;
//   - a hit that is kept is not a DocumentMatch: only the hits returned become
//     those, with their sort values decoded, and with explanations if asked.
//
// Hits come out as the TopNCollector gives them for the same sort order and the
// same searcher: the same order, a tie on all sort values going to the match found
// first, the same total (all of them, nothing is pruned) and the same highest
// score, with scores within float32 precision. A score as one of the sorts is
// compared as a float32, the precision the scores of this path are worked out in.
type PerSegmentSortedCollector struct {
	size int
	skip int
	sort search.SortOrder

	rank         sortRanking
	numKeys      int
	neededFields []string
	needDocIDs   bool

	total     uint64
	maxScore  float64
	took      time.Duration
	bytesRead uint64
	results   search.DocumentMatchCollection

	explain bool
}

// NewPerSegmentSortedCollector builds a collector to find the top 'size' hits,
// skipping over the first 'skip' hits, ordering hits by the sort order, which the
// collector uses and so must not be used by anything else meanwhile (see
// search.SortOrder.Copy).
func NewPerSegmentSortedCollector(size int, skip int, sort search.SortOrder) *PerSegmentSortedCollector {
	hc := &PerSegmentSortedCollector{
		size:         size,
		skip:         skip,
		sort:         sort,
		neededFields: sort.RequiredFields(),
		needDocIDs:   sort.RequiresDocID(),
	}
	for _, s := range sort {
		if _, byScore := s.(*search.SortScore); byScore {
			hc.rank = append(hc.rank, sortComponent{desc: s.Descending(), score: true})
			continue
		}
		hc.rank = append(hc.rank, sortComponent{desc: s.Descending(), key: hc.numKeys})
		hc.numKeys++
	}
	return hc
}

// SetExplain makes the collection explain the hits it returns, as
// PerSegmentTopNCollector does.
func (hc *PerSegmentSortedCollector) SetExplain(explain bool) { hc.explain = explain }

// Collect goes to the searcher to find the matching documents.
func (hc *PerSegmentSortedCollector) Collect(ctx context.Context, searcher search.PerSegmentSearcher,
	reader index.IndexReader) (err error) {
	defer recoverPerSegmentPanic(&err)
	return hc.collect(ctx, searcher, reader)
}

func (hc *PerSegmentSortedCollector) collect(ctx context.Context, searcher search.PerSegmentSearcher,
	reader index.IndexReader) error {
	startTime := time.Now()
	k := hc.size + hc.skip

	var dvReader index.DocValueReader
	var visitor index.DocValueVisitor
	if k > 0 && len(hc.neededFields) > 0 {
		var err error
		dvReader, err = reader.DocValueReader(hc.neededFields)
		if err != nil {
			return err
		}
		visitor = hc.sort.UpdateVisitor
	}
	// ValueBytes of the sorts that have it, Value of the others
	byteSorts := make([]search.SortValueBytes, len(hc.sort))
	for i, s := range hc.sort {
		byteSorts[i], _ = s.(search.SortValueBytes)
	}

	top := newSortedTopN(k, hc.rank)
	var (
		scratch   search.DocumentMatch // what the sorts are asked the value of
		idBuf     []byte
		candidate = sortedHit{keys: make([][]byte, hc.numKeys)}
		total     uint64
		maxScore  float32
	)
	for {
		m, ok, err := searcher.NextMatch()
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		if total%CheckDoneEvery == 0 {
			select {
			case <-ctx.Done():
				search.RecordSearchCost(ctx, search.AbortM, 0)
				return ctx.Err()
			default:
			}
		}
		total++
		if m.Score > maxScore {
			maxScore = m.Score
		}
		if k == 0 {
			continue
		}

		// the sort values of the match
		idBuf = index.NewIndexInternalID(idBuf, m.Doc)
		scratch.IndexInternalID = idBuf
		if dvReader != nil {
			if err = dvReader.VisitDocValues(scratch.IndexInternalID, visitor); err != nil {
				return err
			}
		}
		if hc.needDocIDs {
			if scratch.ID, err = reader.ExternalID(scratch.IndexInternalID); err != nil {
				return err
			}
		}
		for i, s := range hc.sort {
			if hc.rank[i].score {
				continue
			}
			if bs := byteSorts[i]; bs != nil {
				candidate.keys[hc.rank[i].key] = bs.ValueBytes(&scratch)
			} else {
				candidate.keys[hc.rank[i].key] = []byte(s.Value(&scratch))
			}
		}
		candidate.doc, candidate.ord, candidate.score, candidate.seg = m.Doc, total, m.Score, uint32(m.Seg)
		if !top.beats(&candidate) {
			continue
		}
		top.add(candidate.own())
	}
	hc.total = total
	hc.maxScore = float64(maxScore)

	if dvReader != nil {
		hc.bytesRead = dvReader.BytesRead()
	}
	if statsCallbackFn := ctx.Value(search.SearchIOStatsCallbackKey); statsCallbackFn != nil {
		statsCallbackFn.(search.SearchIOStatsCallbackFunc)(hc.bytesRead)
		search.RecordSearchCost(ctx, search.AddM, hc.bytesRead)
	}
	hc.took = time.Since(startTime)

	var best []sortedHit
	if k > 0 {
		best = top.sorted()
	}
	if hc.skip < len(best) {
		best = best[hc.skip:]
	} else {
		best = nil
	}
	if len(best) > hc.size {
		best = best[:hc.size]
	}

	// only the hits to be returned become DocumentMatches
	hc.results = make(search.DocumentMatchCollection, 0, len(best))
	for i := range best {
		hit := &best[i]
		dm := &search.DocumentMatch{
			IndexInternalID: index.NewIndexInternalID(nil, hit.doc),
			Score:           float64(hit.score),
			HitNumber:       hit.ord,
			Sort:            make([]string, len(hc.sort)),
		}
		// a sort by score alone has nothing to decode: TopNCollector gives its hits
		// no decoded sort values
		decode := !(len(hc.sort) == 1 && hc.rank[0].score)
		if decode {
			dm.DecodedSort = make([]string, len(hc.sort))
		}
		var err error
		dm.ID, err = reader.ExternalID(dm.IndexInternalID)
		if err != nil {
			return err
		}
		for j, s := range hc.sort {
			var value string
			if hc.rank[j].score {
				value = s.Value(dm)
			} else {
				value = string(hit.keys[hc.rank[j].key])
			}
			dm.Sort[j] = value
			if decode {
				dm.DecodedSort[j] = s.DecodeValue(value)
			}
		}
		if hc.explain {
			if err := explainPerSegmentHit(searcher, dm, int(hit.seg), hit.doc); err != nil {
				return err
			}
		}
		dm.Complete(nil)
		hc.results = append(hc.results, dm)
	}
	return nil
}

// own is the hit with sort values of its own: those of a hit being looked at are
// the bytes of the doc values just read, which the next doc's overwrite.
func (h sortedHit) own() sortedHit {
	n := 0
	for _, k := range h.keys {
		n += len(k)
	}
	backing := make([]byte, 0, n)
	keys := make([][]byte, len(h.keys))
	for i, k := range h.keys {
		start := len(backing)
		backing = append(backing, k...)
		keys[i] = backing[start:len(backing):len(backing)]
	}
	h.keys = keys
	return h
}

// Size is an estimate of the memory the collector needs: the buffer of hits.
func (hc *PerSegmentSortedCollector) Size() int {
	k := min(hc.size+hc.skip, sortedTopNPreAlloc)
	rv := reflectStaticSizePerSegmentSortedCollector + size.SizeOfPtr + 2*k*int(reflect.TypeOf(sortedHit{}).Size())
	for _, f := range hc.neededFields {
		rv += len(f) + size.SizeOfString
	}
	return rv
}

// Results returns the collected hits
func (hc *PerSegmentSortedCollector) Results() search.DocumentMatchCollection { return hc.results }

// Total returns the total number of hits
func (hc *PerSegmentSortedCollector) Total() uint64 { return hc.total }

// MaxScore returns the maximum score seen across all the matches
func (hc *PerSegmentSortedCollector) MaxScore() float64 { return hc.maxScore }

// Took returns the time spent collecting hits
func (hc *PerSegmentSortedCollector) Took() time.Duration { return hc.took }

// EarlyStopped is always false: every match is visited, so the total is exact.
func (hc *PerSegmentSortedCollector) EarlyStopped() bool { return false }
