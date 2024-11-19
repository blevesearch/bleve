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

// AutoFuzzinessHighThreshold is the threshold for the term length
// above which the fuzziness is set to MaxFuzziness when the fuzziness
// mode is set to AutoFuzziness.
var AutoFuzzinessHighThreshold = 5

// AutoFuzzinessLowThreshold is the threshold for the term length
// below which the fuzziness is set to zero when the fuzziness mode
// is set to AutoFuzziness.
// For terms with length between AutoFuzzinessLowThreshold and
// AutoFuzzinessHighThreshold, the fuzziness is set to
// MaxFuzziness - 1.
var AutoFuzzinessLowThreshold = 2

func NewFuzzySearcher(ctx context.Context, indexReader index.IndexReader, term string,
	prefix, fuzziness int, field string, boost float64,
	options search.SearcherOptions) (search.Searcher, error) {

	if fuzziness > MaxFuzziness {
		return nil, fmt.Errorf("fuzziness exceeds max (%d)", MaxFuzziness)
	}

	if fuzziness < 0 {
		return nil, fmt.Errorf("invalid fuzziness, negative")
	}
	if fuzziness == 0 {
		// no fuzziness, just do a term search
		// check if the call is made from a phrase searcher
		// and if so, add the term to the fuzzy term matches
		// since the fuzzy candidate terms are not collected
		// for a term search, and the only candidate term is
		// the term itself
		fuzzyTermMatches := ctx.Value(search.FuzzyMatchPhraseKey)
		if fuzzyTermMatches != nil {
			fuzzyTermMatches.(map[string][]string)[term] = []string{term}
		}
		return NewTermSearcher(ctx, indexReader, term, field, boost, options)
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
		fuzzyTermMatches := ctx.Value(search.FuzzyMatchPhraseKey)
		if fuzzyTermMatches != nil {
			fuzzyTermMatches.(map[string][]string)[term] = candidates
		}
	}

	return NewMultiTermSearcher(ctx, indexReader, candidates, field,
		boost, options, true)
}

func getAutoFuzziness(term string) int {
	termLength := len(term)
	if termLength > AutoFuzzinessHighThreshold {
		return MaxFuzziness
	} else if termLength > AutoFuzzinessLowThreshold {
		return MaxFuzziness - 1
	}
	return 0
}

func NewAutoFuzzySearcher(ctx context.Context, indexReader index.IndexReader, term string,
	prefix int, field string, boost float64, options search.SearcherOptions) (search.Searcher, error) {
	return NewFuzzySearcher(ctx, indexReader, term, prefix, getAutoFuzziness(term), field, boost, options)
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
