//  Copyright (c) 2014 Couchbase, Inc.
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

package searcher

import (
	"context"
	"fmt"

	"github.com/blevesearch/bleve/v2/search"
	index "github.com/blevesearch/bleve_index_api"
)

var MaxFuzziness = 2

func NewFuzzySearcher(ctx context.Context, indexReader index.IndexReader, term string,
	prefix, fuzziness int, field string, boost float64,
	options search.SearcherOptions) (search.Searcher, error) {

	if fuzziness > MaxFuzziness {
		return nil, fmt.Errorf("fuzziness exceeds max (%d)", MaxFuzziness)
	}

	if fuzziness < 0 {
		return nil, fmt.Errorf("invalid fuzziness, negative")
	}

	// Note: we don't byte slice the term for a prefix because of runes.
	prefixTerm := ""
	for i, r := range term {
		if i < prefix {
			prefixTerm += string(r)
		} else {
			break
		}
	}
	fuzzyCandidates, err := findFuzzyCandidateTerms(indexReader, term, fuzziness,
		field, prefixTerm)
	if err != nil {
		return nil, err
	}

	var candidates []string
	var dictBytesRead uint64
	if fuzzyCandidates != nil {
		candidates = fuzzyCandidates.candidates
		dictBytesRead = fuzzyCandidates.bytesRead
	}

	if ctx != nil {
		reportIOStats(ctx, dictBytesRead)
		search.RecordSearchCost(ctx, search.AddM, dictBytesRead)
	}

	return NewMultiTermSearcher(ctx, indexReader, candidates, field,
		boost, options, true)
}

type fuzzyCandidates struct {
	candidates []string
	bytesRead  uint64
}

func reportIOStats(ctx context.Context, bytesRead uint64) {
	// The fuzzy, regexp like queries essentially load a dictionary,
	// which potentially incurs a cost that must be accounted by
	// using the callback to report the value.
	if ctx != nil {
		statsCallbackFn := ctx.Value(search.SearchIOStatsCallbackKey)
		if statsCallbackFn != nil {
			statsCallbackFn.(search.SearchIOStatsCallbackFunc)(bytesRead)
		}
	}
}

func findFuzzyCandidateTerms(indexReader index.IndexReader, term string,
	fuzziness int, field, prefixTerm string) (rv *fuzzyCandidates, err error) {
	rv = &fuzzyCandidates{
		candidates: make([]string, 0),
	}

	// in case of advanced reader implementations directly call
	// the levenshtein automaton based iterator to collect the
	// candidate terms
	if ir, ok := indexReader.(index.IndexReaderFuzzy); ok {
		fieldDict, err := ir.FieldDictFuzzy(field, term, fuzziness, prefixTerm)
		if err != nil {
			return nil, err
		}
		defer func() {
			if cerr := fieldDict.Close(); cerr != nil && err == nil {
				err = cerr
			}
		}()
		tfd, err := fieldDict.Next()
		for err == nil && tfd != nil {
			rv.candidates = append(rv.candidates, tfd.Term)
			if tooManyClauses(len(rv.candidates)) {
				return nil, tooManyClausesErr(field, len(rv.candidates))
			}
			tfd, err = fieldDict.Next()
		}

		rv.bytesRead = fieldDict.BytesRead()
		return rv, err
	}

	var fieldDict index.FieldDict
	if len(prefixTerm) > 0 {
		fieldDict, err = indexReader.FieldDictPrefix(field, []byte(prefixTerm))
	} else {
		fieldDict, err = indexReader.FieldDict(field)
	}
	if err != nil {
		return nil, err
	}
	defer func() {
		if cerr := fieldDict.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	// enumerate terms and check levenshtein distance
	var reuse []int
	tfd, err := fieldDict.Next()
	for err == nil && tfd != nil {
		var ld int
		var exceeded bool
		ld, exceeded, reuse = search.LevenshteinDistanceMaxReuseSlice(term, tfd.Term, fuzziness, reuse)
		if !exceeded && ld <= fuzziness {
			rv.candidates = append(rv.candidates, tfd.Term)
			if tooManyClauses(len(rv.candidates)) {
				return nil, tooManyClausesErr(field, len(rv.candidates))
			}
		}
		tfd, err = fieldDict.Next()
	}

	rv.bytesRead = fieldDict.BytesRead()
	return rv, err
}
