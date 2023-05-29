//  Copyright (c) 2023 Couchbase, Inc.
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

package synonym

import (
	"bytes"
	"encoding/json"
	"hash/fnv"
	"sort"

	"github.com/blevesearch/bleve/v2/analysis"
	index "github.com/blevesearch/bleve_index_api"
	"github.com/blevesearch/vellum"
)

var equivalentSynonymType = []byte("equivalent")
var explicitSynonymType = []byte("explicit")

// stripQuotes takes as input a byte slice and returns the byte slice without the first and last characters.
// This function is used in the stripJsonQuotes function. The first and last characters are assumed to be quotes.
func stripQuotes(word []byte) []byte {
	return word[1 : len(word)-1]
}

// returns the hash of the input phrase
func hash(phrase []byte) uint64 {
	h := fnv.New64a()
	h.Write(phrase)
	return h.Sum64()
}

// removes the element at index i from slice s and returns the new slice
func remove(s []uint64, i int) []uint64 {
	s[i] = s[len(s)-1]
	return s[:len(s)-1]
}

// updateSynonyms takes as input a map of hashes to a set of hashes, a hashvalue and a slice of hashes.
// If the hashvalue exists in the map, we add all the hashes in the slice to the set which was mapped to the hashvalue.
// Else we create a new set and add all the hashes in the slice to the set, and map the hashvalue to the set.
func updateSynonyms(hashSet map[uint64]map[uint64]interface{}, hashval uint64, hashedPhrases []uint64) {
	synonyms, exists := hashSet[hashval]
	if exists {
		for _, syn := range hashedPhrases {
			synonyms[syn] = struct{}{}
		}
	} else {
		newSet := make(map[uint64]interface{})
		for _, syn := range hashedPhrases {
			newSet[syn] = struct{}{}
		}
		hashSet[hashval] = newSet
	}
}

// ProcessSynonyms takes as input a slice of synonym structs and returns two maps
// 1.	hashToSynonyms: a map from a hash of a phrase to a slice of hashes of all its synonyms
// 2.	hashToPhrase: a map from a hash of a phrase to the phrase itself
//
// The function processes each synonym struct in the slice as follows:
// First we generate a slice of hashes for all the phrases in synonym.Synonyms
// and map each hash to the phrase in hashToPhrase map.
// If the synonym mapping type is equivalent,
//  1. For each hash in the generated slice
//     a.	We add all the other hashes in a slice and map the hash to the slice
//     by calling the updateSynonyms function.
//
// If the synonym mapping type is explicit,
//  1. For each phrase in synonym.Input,
//     a.	Map its hash to it in hashToPhrase map.
//     b.	Map its hash to the generated slice by calling the updateSynonyms function.
func ProcessSynonyms(synonyms []*index.SynonymDefinition) (map[uint64][]uint64, map[uint64][]byte) {
	var hashToSynonyms = make(map[uint64][]uint64)
	var hashSet = make(map[uint64]map[uint64]interface{})
	var hashToPhrase = make(map[uint64][]byte)
	var hashval uint64
	var hashedPhrases []uint64
	var index int
	for _, synonym := range synonyms {
		hashedPhrases = nil
		for _, rhs := range synonym.Synonyms {
			hashval = hash(rhs)
			hashToPhrase[hashval] = rhs
			hashedPhrases = append(hashedPhrases, hashval)
		}
		if bytes.Equal(synonym.MappingType, equivalentSynonymType) {
			for i, hashval := range hashedPhrases {
				hashedPhrasesCopy := make([]uint64, len(hashedPhrases))
				copy(hashedPhrasesCopy, hashedPhrases)
				hashedPhrasesCopy = remove(hashedPhrasesCopy, i)
				updateSynonyms(hashSet, hashval, hashedPhrasesCopy)
			}
		} else if bytes.Equal(synonym.MappingType, explicitSynonymType) {
			for _, lhs := range synonym.Input {
				hashval = hash(lhs)
				hashToPhrase[hashval] = lhs
				updateSynonyms(hashSet, hashval, hashedPhrases)
			}
		}
	}
	for key, set := range hashSet {
		hashedPhrases = make([]uint64, len(set))
		index = 0
		for k := range set {
			hashedPhrases[index] = k
			index++
		}
		hashToSynonyms[key] = hashedPhrases
	}
	return hashToSynonyms, hashToPhrase
}

// This function is used to build the synonym FST from the hashToSynonyms and
// hashToPhrase. The hashToSynonyms maps the hash of a phrase to a slice of
// hashes of the phrase's synonyms. The hashToPhrase maps the of hash of
// the phrase to the phrase itself.
// The synonym FST is built like so :
//  1. Iterate over the keys of hashToSynonyms and get the corresponding phrase from
//     hashToPhrase, and create a slice of structs containing them.
//  2. Sort the slice of structs by the phrase in ascending order.
//  3. Iterate over the sorted slice of structs and insert the phrase and its
//     hash into the FST.
func BuildSynonymFST(hashToPhrase map[uint64][]byte,
	hashToSynonyms map[uint64][]uint64) (*bytes.Buffer, error) {

	type element struct {
		Key   []byte
		Value uint64
	}
	var elementList = make([]element, len(hashToSynonyms))
	var index = 0
	for k := range hashToSynonyms {
		elementList[index] = element{
			Key:   hashToPhrase[k],
			Value: k,
		}
		index++
	}
	sort.Slice(elementList, func(i, j int) bool {
		return (bytes.Compare(elementList[i].Key, elementList[j].Key) == -1)
	})

	var buf bytes.Buffer
	builder, err := vellum.New(&buf, nil)
	if err != nil {
		return nil, err
	}

	for index = 0; index < len(elementList); index++ {
		err = builder.Insert(elementList[index].Key, elementList[index].Value)
		if err != nil {
			return nil, err
		}
	}
	err = builder.Close()
	if err != nil {
		return nil, err
	}
	return &buf, nil
}

// This function is used to convert the token stream to a phrase by using the
// position attribute of the tokens for example if the token stream is
// "hello world" and the position of hello is 2 and world is 4 then the phrase
// will be ["hello","","world"]
// This would essentially maintain the number of stop words between two
// normal words and maintain the order of the words while also stripping
// stop words at the end and start of the phrase.
func TokenStreamToPhrase(tokens analysis.TokenStream) [][]byte {
	firstPosition := int(^uint(0) >> 1)
	lastPosition := 0
	for _, token := range tokens {
		if token.Position < firstPosition {
			firstPosition = token.Position
		}
		if token.Position > lastPosition {
			lastPosition = token.Position
		}
	}
	phraseLen := lastPosition - firstPosition + 1
	rv := make([][]byte, phraseLen)
	if phraseLen > 0 {
		for _, token := range tokens {
			pos := token.Position - firstPosition
			rv[pos] = token.Term
		}
	}
	return rv
}

// applies an analyzer to each string in a slice and returns the result slice.
// if the analyzer is nil, the original slice is returned.
func analyzeSlice(analyzer analysis.Analyzer, slice []json.RawMessage) []json.RawMessage {
	if analyzer == nil {
		return slice
	}
	loc := 0
	rv := make([]json.RawMessage, len(slice))
	for _, val := range slice {
		val = stripQuotes(val)
		analyzedPhrase := TokenStreamToPhrase(analyzer.Analyze(val))
		var combinedPhrase []byte
		for _, tok := range analyzedPhrase {
			combinedPhrase = append(combinedPhrase, tok...)
			combinedPhrase = append(combinedPhrase, SeparatingCharacter)
		}
		sz := len(combinedPhrase)
		if sz > 0 && combinedPhrase[sz-1] == SeparatingCharacter {
			combinedPhrase = combinedPhrase[:sz-1]
			rv[loc] = combinedPhrase
			loc++
		}
	}
	rv = rv[:loc]
	return rv
}

// applies the analyzer specified by the mapping to the input and synonyms
// of the synonym struct.  if the analyzer is nil, the original struct is returned.
func Analyze(syn *index.SynonymDefinition, analyzer analysis.Analyzer) *index.SynonymDefinition {
	return &index.SynonymDefinition{
		MappingType: stripQuotes(syn.MappingType),
		Input:       analyzeSlice(analyzer, syn.Input),
		Synonyms:    analyzeSlice(analyzer, syn.Synonyms),
	}
}
