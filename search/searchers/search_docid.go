//  Copyright (c) 2015 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package searchers

import (
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/search"
	"github.com/blevesearch/bleve/search/scorers"
)

// DocIDSearcher returns documents matching a predefined set of identifiers.
type DocIDSearcher struct {
	reader index.DocIDReader
	scorer *scorers.ConstantScorer
	count  int
}

func NewDocIDSearcher(indexReader index.IndexReader, ids []string, boost float64,
	explain bool) (searcher *DocIDSearcher, err error) {

	// kept := make([]string, len(ids))
	// copy(kept, ids)
	// sort.Strings(kept)
	//
	// if len(ids) > 0 {
	// 	var idReader index.DocIDReader
	// 	endTerm := string(incrementBytes([]byte(kept[len(kept)-1])))
	// 	idReader, err = indexReader.DocIDReader(kept[0], endTerm)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	defer func() {
	// 		if cerr := idReader.Close(); err == nil && cerr != nil {
	// 			err = cerr
	// 		}
	// 	}()
	// 	j := 0
	// 	for _, id := range kept {
	// 		doc, err := idReader.Next()
	// 		if err != nil {
	// 			return nil, err
	// 		}
	// 		// Non-duplicate match
	// 		actualDocID := indexReader.FinalizeDocID(doc)
	// 		if actualDocID == id && (j == 0 || kept[j-1] != id) {
	// 			kept[j] = id
	// 			j++
	// 		}
	// 	}
	// 	kept = kept[:j]
	// }

	reader, err := indexReader.DocIDReaderOnly(ids)
	if err != nil {
		return nil, err
	}
	scorer := scorers.NewConstantScorer(1.0, boost, explain)
	return &DocIDSearcher{
		scorer: scorer,
		reader: reader,
		count:  len(ids),
	}, nil
}

func (s *DocIDSearcher) Count() uint64 {
	// return uint64(len(s.ids))
	return uint64(s.count)
}

func (s *DocIDSearcher) Weight() float64 {
	return s.scorer.Weight()
}

func (s *DocIDSearcher) SetQueryNorm(qnorm float64) {
	s.scorer.SetQueryNorm(qnorm)
}

func (s *DocIDSearcher) Next(preAllocated *search.DocumentMatchInternal) (*search.DocumentMatchInternal, error) {
	// if s.current >= len(s.ids) {
	// 	return nil, nil
	// }
	// id := s.ids[s.current]
	// s.current++
	// docMatch := s.scorer.Score(id)
	// return docMatch, nil

	docidMatch, err := s.reader.Next()
	if err != nil {
		return nil, err
	}
	if docidMatch == nil {
		return nil, nil
	}

	docMatch := s.scorer.Score(docidMatch)
	return docMatch, nil
}

func (s *DocIDSearcher) Advance(ID index.IndexInternalID, preAllocated *search.DocumentMatchInternal) (*search.DocumentMatchInternal, error) {
	// s.current = sort.SearchStrings(s.ids, ID)
	// return s.Next(preAllocated)

	docidMatch, err := s.reader.Advance(ID)
	if err != nil {
		return nil, err
	}
	if docidMatch == nil {
		return nil, nil
	}

	docMatch := s.scorer.Score(docidMatch)
	return docMatch, nil
}

func (s *DocIDSearcher) Close() error {
	return nil
}

func (s *DocIDSearcher) Min() int {
	return 0
}
