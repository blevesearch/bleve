//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package cjk

import (
	"container/ring"

	"github.com/blevesearch/bleve/analysis"
	"github.com/blevesearch/bleve/registry"
)

const BigramName = "cjk_bigram"

type CJKBigramFilter struct {
	outputUnigram bool
}

func NewCJKBigramFilter(outputUnigram bool) *CJKBigramFilter {
	return &CJKBigramFilter{
		outputUnigram: outputUnigram,
	}
}

func (s *CJKBigramFilter) Filter(input analysis.TokenStream) analysis.TokenStream {
	r := ring.New(2)
	itemsInRing := 0

	rv := make(analysis.TokenStream, 0, len(input))

	for _, token := range input {
		if token.Type == analysis.Ideographic {
			if itemsInRing > 0 {
				// if items already buffered
				// check to see if this is aligned
				curr := r.Value.(*analysis.Token)
				if token.Start-curr.End != 0 {
					// not aligned flush
					flushToken := s.flush(r, &itemsInRing)
					if flushToken != nil {
						rv = append(rv, flushToken)
					}
				}
			}
			// now we can add this token to the buffer
			r = r.Next()
			r.Value = token
			if itemsInRing < 2 {
				itemsInRing++
			}
			if itemsInRing > 1 && s.outputUnigram {
				unigram := s.buildUnigram(r, &itemsInRing)
				if unigram != nil {
					rv = append(rv, unigram)
				}
			}
			bigramToken := s.outputBigram(r, &itemsInRing)
			if bigramToken != nil {
				rv = append(rv, bigramToken)
			}
		} else {
			// flush anything already buffered
			flushToken := s.flush(r, &itemsInRing)
			if flushToken != nil {
				rv = append(rv, flushToken)
			}
			// output this token as is
			rv = append(rv, token)
		}
	}

	// deal with possible trailing unigram
	if itemsInRing == 1 || s.outputUnigram {
		if itemsInRing == 2 {
			r = r.Next()
		}
		unigram := s.buildUnigram(r, &itemsInRing)
		if unigram != nil {
			rv = append(rv, unigram)
		}
	}
	return rv
}

func (s *CJKBigramFilter) flush(r *ring.Ring, itemsInRing *int) *analysis.Token {
	var rv *analysis.Token
	if *itemsInRing == 1 {
		rv = s.buildUnigram(r, itemsInRing)
	}
	r.Value = nil
	*itemsInRing = 0
	return rv
}

func (s *CJKBigramFilter) outputBigram(r *ring.Ring, itemsInRing *int) *analysis.Token {
	if *itemsInRing == 2 {
		thisShingleRing := r.Move(-1)
		shingledBytes := make([]byte, 0)

		// do first token
		prev := thisShingleRing.Value.(*analysis.Token)
		shingledBytes = append(shingledBytes, prev.Term...)

		// do second token
		thisShingleRing = thisShingleRing.Next()
		curr := thisShingleRing.Value.(*analysis.Token)
		shingledBytes = append(shingledBytes, curr.Term...)

		token := analysis.Token{
			Type:     analysis.Double,
			Term:     shingledBytes,
			Position: prev.Position,
			Start:    prev.Start,
			End:      curr.End,
		}
		return &token
	}
	return nil
}

func (s *CJKBigramFilter) buildUnigram(r *ring.Ring, itemsInRing *int) *analysis.Token {
	if *itemsInRing == 2 {
		thisShingleRing := r.Move(-1)
		// do first token
		prev := thisShingleRing.Value.(*analysis.Token)
		token := analysis.Token{
			Type:     analysis.Single,
			Term:     prev.Term,
			Position: prev.Position,
			Start:    prev.Start,
			End:      prev.End,
		}
		return &token
	} else if *itemsInRing == 1 {
		// do first token
		prev := r.Value.(*analysis.Token)
		token := analysis.Token{
			Type:     analysis.Single,
			Term:     prev.Term,
			Position: prev.Position,
			Start:    prev.Start,
			End:      prev.End,
		}
		return &token
	}
	return nil
}

func CJKBigramFilterConstructor(config map[string]interface{}, cache *registry.Cache) (analysis.TokenFilter, error) {
	outputUnigram := false
	outVal, ok := config["output_unigram"].(bool)
	if ok {
		outputUnigram = outVal
	}
	return NewCJKBigramFilter(outputUnigram), nil
}

func init() {
	registry.RegisterTokenFilter(BigramName, CJKBigramFilterConstructor)
}
