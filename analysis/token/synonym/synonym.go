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
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/blevesearch/bleve/v2/analysis"
	"github.com/blevesearch/bleve/v2/registry"
	"github.com/blevesearch/bleve/v2/search"
	index "github.com/blevesearch/bleve_index_api"
	"github.com/blevesearch/vellum"
)

const Name = "synonym"

const SeparatingCharacter = ' '

type SynonymFilter struct {
	metadata  []*index.SynonymMetadata
	fuzziness int
	prefix    int
}

// fstPath is used for storing the data associated
// with a particular path in the FST. This would be used for enabling
// fuzzy search in the synonym filter.
type fstPath struct {
	// output is the sum of the outputs of all the states seen in the path
	output uint64
	// state is the state at the end of the path
	state int
	// word is the sequence of characters that were matched in the path
	word []byte
}

func resetPath(path *fstPath) *fstPath {
	path.output = 0
	path.state = 0
	path.word = path.word[:0]
	return path
}

var pathPool = sync.Pool{
	New: func() interface{} { return new(fstPath) },
}

// check if the input FST path ends with a word.
// A word in the FST is defined as a sequence of characters that is either not followed by any other character
// or is followed by a space.
// This function is used for enabling fuzzy search in the synonym filter.
func pathEndsWithWord(path *fstPath, fst *vellum.FST) (bool, error) {
	spaceAccept := fst.Accept(path.state, SeparatingCharacter)
	numEdges, err := fst.GetNumTransitionsForState(path.state)
	if err != nil {
		return false, err
	}
	// if, at the current state of the input FST path, there is a transition available for space, or
	// if there is no transition available, the path ends with a word.
	return spaceAccept != 1 || numEdges == 0, nil
}

// This function performs a DFS from the state present in the input FST path.
// MatchTry is the word that must be fuzzily matched in the FST, starting from path.
// ValidPaths is the list of valid paths that were found in the FST and if the matchTry is fuzzily matched in
// the FST, validPaths is appended with the new path that was found.
// Fuzziness is the maximum Levenshtein distance allowed between matchTry and the word fuzzily matched in the FST.
//
// Basic logic followed is as follows:
//   - At the state in the FST where the character in the input string fails to match,
//     we start a DFS searching for states that end a word
//   - which are states that either do not have any outgoing transition or
//     have a transition for a space
//   - Since it is a FST, multiple such paths will be found, and each path's word is checked for
//     edit distance criteria satisfaction before it is added to validPaths
func fuzzyMatch(path *fstPath, matchTry string, validPaths []*fstPath, fst *vellum.FST, fuzziness int) ([]*fstPath, error) {
	// stack for iterative DFS
	pathStack := []*fstPath{path}
	var pathIsValid bool
	for len(pathStack) > 0 {
		path = pathStack[len(pathStack)-1]
		pathStack = pathStack[:len(pathStack)-1]
		pathIsValid = false
		// if input FST path ends with a word and if the word fuzzily matches the matchTry, append the validPaths with the input FST path.
		endsWithWord, err := pathEndsWithWord(path, fst)
		if err != nil {
			return nil, err
		}
		if endsWithWord {
			if search.LevenshteinDistance(string(path.word), matchTry) <= fuzziness {
				validPaths = append(validPaths, path)
				pathIsValid = true
			}
		}
		// perform a DFS from the current state of the input FST path.
		// even if the path ends with a word, the DFS is performed since there there may be
		// another transition apart from a space at the state in the path.
		//
		// this condition is a shortcut to discard certain fst paths where it becomes known that the word
		// will have a edit distance > fuzziness to matchTry, as path does not end with a word and
		// path.word is already exceeding len(matchTry) by 'fuzziness' amount and any further
		// exploration along this path will need removal of characters >'fuzziness'
		if len(path.word)-len(matchTry) <= fuzziness {
			transitions, err := fst.GetTransitionsForState(path.state)
			if err != nil {
				return nil, err
			}
			// for each transition from the current state of the input FST path, perform a DFS.
			// if the transition is not a space, get a new  fstPath from the pool, and insert it
			// into the stack with the updated parameters
			for _, character := range transitions {
				if character != SeparatingCharacter {
					newState, newOutput := fst.AcceptWithVal(path.state, character)
					newPath := pathPool.Get().(*fstPath)
					newPath.state, newPath.output, newPath.word = newState, path.output+newOutput, append(path.word, character)
					pathStack = append(pathStack, newPath)
					if err != nil {
						return nil, err
					}
				}
			}
		}
		// only discard fstPath if it was not added to validPaths
		if !pathIsValid {
			pathPool.Put(resetPath(path))
		}
	}
	return validPaths, nil
}

// acceptSpaces is a helper function that accepts a sequence of spaces in the input FST path.
// if the desired number of spaces is found, the state and output of the input FST path are updated
// and the input FST path is returned.
func acceptSpaces(numSpaces int, path *fstPath, fst *vellum.FST) *fstPath {
	numAcceptedSpaces := 0
	state := path.state
	var tmp uint64
	for numAcceptedSpaces < numSpaces {
		state, tmp = fst.AcceptWithVal(state, SeparatingCharacter)
		if state == 1 {
			return nil
		}
		path.output += tmp
		numAcceptedSpaces++
	}
	path.state = state
	return path
}

// checkForMatch is a function that checks if a sequence of tokens starting from inputIndex in the input
// token stream matches a phrase in the FST.
// if a match is found, the function returns the new value to be assigned to inputIndex in the Filter, which is
// index of the last matched token, along with the list of synonyms found for the matched phrase.
// The position of the first word and the position of the last word in the matched phrase are also returned.
// Greedy matching is used to find the longest sequence of input tokens that matches a phrase in the FST.
// Two types of matches are possible for a word in the FST:
//  1. Exact match: the word in the FST matches the word in the input token stream exactly.
//  2. Fuzzy match: the word in the FST matches the word in the input token stream with a Levenshtein distance
//     less than or equal to the fuzziness parameter.
func (s *SynonymFilter) checkForMatch(input analysis.TokenStream, inputIndex int, fst *vellum.FST,
	metadata *index.SynonymMetadata) ([]uint64, int, error) {

	numTokensConsumed := 0
	var rv []uint64
	var validPaths []*fstPath
	var tokenLen, matchLen, pathIndex int
	var err error
	// since a token can fuzzily match multiple words in the FST, a slice of valid paths is maintained.
	// with each valid path representing a sequence of transitions in the FST that starts from the start state.
	// for every input token, all the valid paths are checked for an exact match or a fuzzy match.
	startPath := pathPool.Get().(*fstPath)
	startPath.state, startPath.output, startPath.word = fst.Start(), 0, nil
	validPaths = append(validPaths, startPath)
	numValidPaths := len(validPaths)
	prevPos := input[inputIndex].Position
	for inputIndex < len(input) {
		// check how many stop words are present between the current token and the previous token.
		// the number of stop words is the number of times a FST path accepts the space character
		// this code block will be executed only after the first token is exactly or fuzzily matched.
		numStopWords := input[inputIndex].Position - prevPos - 1
		if numStopWords > 0 {
			pathIndex = 0
			for _, path := range validPaths {
				rv := acceptSpaces(numStopWords, path, fst)
				if rv != nil {
					validPaths[pathIndex] = rv
					pathIndex++
				} else {
					pathPool.Put(resetPath(path))
				}
			}
			validPaths = validPaths[:pathIndex]
			numValidPaths = len(validPaths)
		}
		pathIndex = 0
		// for each valid path, check if the current token is exactly or fuzzily matched.
		for pathIndex < numValidPaths {
			tokenLen = len(input[inputIndex].Term)
			matchLen = 0
			path := validPaths[pathIndex]
			for _, character := range input[inputIndex].Term {
				newState, output := fst.AcceptWithVal(path.state, character)
				if newState == 1 {
					// if the current token is not exactly matched, check if it is fuzzily matched, if
					// fuzziness is not 0 and if the match length is greater than or equal to the prefix length.
					// if the current token is fuzzily matched, append the validPaths with the
					// set of new paths that are found.
					if s.fuzziness != 0 && matchLen >= s.prefix {
						validPaths, err = fuzzyMatch(path, string(input[inputIndex].Term),
							validPaths, fst, s.fuzziness)
						if err != nil {
							return nil, 0, err
						}
					}
					break
				} else {
					matchLen++
					path.state = newState
					path.output += output
					path.word = append(path.word, character)
				}
			}
			// if the current token is exactly matched, check if the path ends with a word.
			// this is done so that the path is not considered as a valid path if the current token
			// is a prefix of a word in the FST.
			if matchLen == tokenLen {
				endsWithWord, err := pathEndsWithWord(path, fst)
				if err != nil {
					return nil, 0, err
				}
				if endsWithWord {
					validPaths = append(validPaths, path)
				} else {
					// since the path does not end with a word try to auto-complete the token
					// by calling the fuzzyMatch function.
					// example - if the token is "foo" and the FST contains "foobar", "foobaz" and "foobuzz",
					// the token "foo" will be auto-completed to "foobar", "foobaz" and "foobuzz".
					// provided fuzziness is set to 3.
					if s.fuzziness != 0 && matchLen >= s.prefix {
						validPaths, err = fuzzyMatch(path, string(input[inputIndex].Term),
							validPaths, fst, s.fuzziness)
						if err != nil {
							return nil, 0, err
						}
					}
				}
			}
			pathIndex++
		}
		// filter out the valid paths from the previous iteration.
		validPaths = validPaths[numValidPaths:]
		prevPos = input[inputIndex].Position
		numTokensConsumed++
		inputIndex++
		var seenSynonyms []uint64
		matchFound := false
		pathIndex = 0
		// for each valid path, if the state is a matching state, append the seenSynonyms with the synonyms
		// by using the output of the path as the key to the hashToSynonyms map.
		// filter out all paths that do not have a transition for space from the current state and since each
		// valid path ends with a word in the FST, the "dead-ends", or the paths that do not have any transition
		// from the current state are filtered out.
		for _, path := range validPaths {
			isMatch, finalOutput := fst.IsMatchWithVal(path.state)
			hashedLHS := path.output + finalOutput
			if isMatch && !metadata.HashToPhrase[hashedLHS].IsInvalid {
				for _, RHS := range metadata.HashToSynonyms[hashedLHS] {
					if !RHS.IsInvalid {
						seenSynonyms = append(seenSynonyms, RHS.Hash)
					}
				}
				seenSynonyms = append(seenSynonyms, hashedLHS)
				matchFound = true
			}
			pathAfterSpace := acceptSpaces(1, path, fst)
			if pathAfterSpace != nil {
				validPaths[pathIndex] = pathAfterSpace
				validPaths[pathIndex].word = nil
				pathIndex++
			} else {
				pathPool.Put(resetPath(path))
			}
		}
		validPaths = validPaths[:pathIndex]
		// if a match is found, update rv with the newInputIndex, matchedSynonyms
		if matchFound {
			rv = seenSynonyms
		}
		numValidPaths = len(validPaths)
		if numValidPaths == 0 {
			return rv, numTokensConsumed, nil
		}
	}
	return rv, numTokensConsumed, nil
}

type asyncResult struct {
	err               error
	output            []string
	numTokensConsumed int
}

func (s *SynonymFilter) parallelFilter(input analysis.TokenStream, metadata *index.SynonymMetadata,
	inputIdx int, fst *vellum.FST) *asyncResult {
	rv, numTokensConsumed, err := s.checkForMatch(input, inputIdx, fst, metadata)
	output := make([]string, len(rv))
	for i, hash := range rv {
		output[i] = metadata.HashToPhrase[hash].Phrase
	}
	return &asyncResult{
		output:            output,
		numTokensConsumed: numTokensConsumed,
		err:               err,
	}
}

func (s *SynonymFilter) Filter(input analysis.TokenStream) analysis.TokenStream {
	// Load FST data for each metadata
	allFst, err := loadFSTData(s.metadata)
	if err != nil {
		fmt.Printf("error in synonym filter: %v\n", err)
		return input
	}
	var rv analysis.TokenStream
	idx := 0
	for idx < len(input) {
		asyncResults, maxTokensConsumed := s.runParallelFilters(input, allFst, idx)
		synonyms := s.getSynonymsWithMaxTokensConsumed(asyncResults, maxTokensConsumed)
		rv = appendSynonymsToResult(rv, input, idx, maxTokensConsumed, synonyms)
		idx += maxTokensConsumed
	}
	return rv
}

func loadFSTData(metadataList []*index.SynonymMetadata) ([]*vellum.FST, error) {
	allFst := make([]*vellum.FST, len(metadataList))
	for i, metadata := range metadataList {
		fst, err := vellum.Load(metadata.SynonymFST)
		if err != nil {
			return nil, err
		}
		allFst[i] = fst
	}
	return allFst, nil
}

func (s *SynonymFilter) runParallelFilters(input analysis.TokenStream, allFst []*vellum.FST, idx int) ([]*asyncResult, int) {
	asyncResults := make([]*asyncResult, len(s.metadata))
	var maxTokensConsumed int
	var waitGroup sync.WaitGroup
	for i, metadata := range s.metadata {
		waitGroup.Add(1)
		go func(i int, metadata *index.SynonymMetadata) {
			asyncResults[i] = s.parallelFilter(input, metadata, idx, allFst[i])
			waitGroup.Done()
		}(i, metadata)
	}
	waitGroup.Wait()
	for _, asr := range asyncResults {
		if asr.err != nil {
			fmt.Printf("error in synonym filter: %v\n", asr.err)
			return asyncResults, maxTokensConsumed
		}
		if asr.numTokensConsumed > maxTokensConsumed {
			maxTokensConsumed = asr.numTokensConsumed
		}
	}
	return asyncResults, maxTokensConsumed
}

func (s *SynonymFilter) getSynonymsWithMaxTokensConsumed(asyncResults []*asyncResult, maxTokensConsumed int) []string {
	var synonyms []string
	for _, asr := range asyncResults {
		if asr.numTokensConsumed == maxTokensConsumed {
			synonyms = append(synonyms, asr.output...)
		}
	}
	return synonyms
}

func convertConsumedTokensToPhrase(consumedTokens analysis.TokenStream) *analysis.Token {
	firstPos := consumedTokens[0].Position
	lastPos := consumedTokens[len(consumedTokens)-1].Position
	phrase := make([]string, lastPos-firstPos+1)
	for _, token := range consumedTokens {
		phrase[token.Position-firstPos] = string(token.Term)
	}
	return &analysis.Token{
		Term:     []byte(strings.Join(phrase, " ")),
		Position: consumedTokens[0].Position,
		Start:    consumedTokens[0].Start,
		End:      consumedTokens[len(consumedTokens)-1].End,
		Type:     analysis.Synonym,
	}
}

func appendSynonymsToResult(rv analysis.TokenStream, input analysis.TokenStream, idx, maxTokensConsumed int, synonyms []string) analysis.TokenStream {
	if maxTokensConsumed == 1 {
		rv = append(rv, input[idx])
	} else {
		// convert the consumed tokens to a phrase to be matched by the phrase searcher
		rv = append(rv, convertConsumedTokensToPhrase(input[idx:idx+maxTokensConsumed]))
	}
	for _, i := range synonyms {
		rv = append(rv, &analysis.Token{
			Term:     []byte(i),
			Position: input[idx].Position,
			Start:    input[idx].Start,
			End:      input[idx+maxTokensConsumed-1].End,
			Type:     analysis.Synonym,
		})
	}
	return rv
}

func SynonymFilterConstructor(config map[string]interface{}, cache *registry.Cache) (analysis.TokenFilter, error) {
	return &SynonymFilter{}, nil
}

func NewSynonymTokenFilter(metadata []*index.SynonymMetadata, fuzziness int, prefix int) *SynonymFilter {
	return &SynonymFilter{
		metadata:  metadata,
		fuzziness: fuzziness,
		prefix:    prefix,
	}
}

func AddSynonymFilter(ctx context.Context, analyzer analysis.Analyzer, i index.IndexReader,
	key, synonymSourceName string, fuzziness, prefix int) (analysis.Analyzer, error) {

	synMD, err := getSynonymMetadata(ctx, synonymSourceName, key, i)
	if err != nil {
		return nil, err
	}

	synonymTokenFilter := NewSynonymTokenFilter(synMD, fuzziness, prefix)
	return &analysis.ExtendedAnalyzer{
		BaseAnalyzer:      analyzer,
		ExtraTokenFilters: []analysis.TokenFilter{synonymTokenFilter},
	}, nil
}

func getSynonymMetadata(ctx context.Context, synonymSourceName, key string, i index.IndexReader) ([]*index.SynonymMetadata, error) {
	if searchSynMD := ctx.Value(search.ScatterSynonymMetadataKey); searchSynMD != nil {
		// all required synonym data is already available in the context
		// so we can just use it
		var synSources []byte
		var ok bool
		if synSources, ok = searchSynMD.([]byte); !ok {
			return nil, fmt.Errorf("invalid synonym metadata: data in context must be []byte")
		}

		var extData map[string][]*index.SynonymMetadata
		if err := json.Unmarshal(synSources, &extData); err != nil {
			return nil, fmt.Errorf("error unmarshalling synonym metadata: %v", err)
		}

		if rv, ok := extData[synonymSourceName]; ok {
			return rv, nil
		}

		return nil, fmt.Errorf("no synonym metadata found for synonym source: %s", synonymSourceName)
	}
	return i.SynonymMetadata(key)
}

func init() {
	registry.RegisterTokenFilter(Name, SynonymFilterConstructor)
}
