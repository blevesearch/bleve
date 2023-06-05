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
	"log"
	"sync"

	"github.com/blevesearch/bleve/v2/analysis"
	"github.com/blevesearch/bleve/v2/registry"
	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/vellum"
)

const Name = "synonym"

const SeparatingCharacter = ' '

type SynonymFilter struct {
	fst            []byte
	hashToSynonyms map[uint64][]uint64
	hashToPhrase   map[uint64][]byte
	fuzziness      int
	prefix         int
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

// matchParameters is used for representing the output for the checkForMatch function.
type matchParameters struct {
	// newInputIndex is the new value to be assigned to inputIndex in the Filter.
	// This allows for skipping over the sequence of tokens that were matched in the FST
	// when the checkForMatch function was called.
	newInputIndex int
	// matchedSynonyms is the list of synonyms found for the sequence
	// of input tokens that were matched in the FST. if nil no synonyms were found.
	matchedSynonyms []uint64
	// firstPos is the position of the first token in the sequence of tokens that were matched in the FST.
	firstPos int
	// lastPos is the position of the last token in the sequence of tokens that were matched in the FST.
	lastPos int
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
func checkForMatch(s *SynonymFilter, input analysis.TokenStream, inputIndex int, fst *vellum.FST) (*matchParameters, error) {
	rv := &matchParameters{
		newInputIndex:   inputIndex + 1,
		matchedSynonyms: nil,
		firstPos:        input[inputIndex].Position,
		lastPos:         input[inputIndex].Position,
	}
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
							return nil, err
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
					return nil, err
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
							return nil, err
						}
					}
				}
			}
			pathIndex++
		}
		// filter out the valid paths from the previous iteration.
		validPaths = validPaths[numValidPaths:]
		prevPos = input[inputIndex].Position
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
			if isMatch {
				hashedSynonyms := s.hashToSynonyms[path.output+finalOutput]
				hashedSynonyms = append(hashedSynonyms, path.output+finalOutput)
				seenSynonyms = append(seenSynonyms, hashedSynonyms...)
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
		// if a match is found, update rv with the newInputIndex, matchedSynonyms and lastPos.
		if matchFound {
			rv.newInputIndex = inputIndex
			rv.matchedSynonyms = seenSynonyms
			rv.lastPos = prevPos
		}
		numValidPaths = len(validPaths)
		if numValidPaths == 0 {
			return rv, nil
		}
	}
	return rv, nil
}

func (s *SynonymFilter) Filter(input analysis.TokenStream) analysis.TokenStream {
	fst, err := vellum.Load(s.fst)
	if err != nil {
		log.Println(err)
		return input
	}
	defer fst.Close()
	var outputTokenStream analysis.TokenStream
	// inputIndex is the index of a token in the input token stream.
	// outputIndex is the index of a token in the output token stream.
	var inputIndex int
	outputIndex := 1
	for inputIndex < len(input) {
		rv, err := checkForMatch(s, input, inputIndex, fst)
		if err != nil {
			log.Println(err)
			rv = &matchParameters{
				newInputIndex:   inputIndex + 1,
				matchedSynonyms: nil,
			}
		}
		if rv.matchedSynonyms != nil {
			// We have a match, so we need to add the synonyms to the output
			// token stream.
			for _, hashValue := range rv.matchedSynonyms {
				outputTokenStream = append(outputTokenStream, &analysis.Token{
					Term:          s.hashToPhrase[hashValue],
					Type:          analysis.Synonym,
					Position:      outputIndex,
					FirstPosition: rv.firstPos,
					LastPosition:  rv.lastPos,
				})
			}
		} else {
			input[inputIndex].FirstPosition = input[inputIndex].Position
			input[inputIndex].LastPosition = input[inputIndex].Position
			input[inputIndex].Position = outputIndex
			outputTokenStream = append(outputTokenStream, input[inputIndex])
		}
		// We need to skip over the tokens that were matched by the synonym
		// filter. We do this by setting the inputIndex to the newInputIndex
		// returned by checkForMatch.
		inputIndex = rv.newInputIndex
		outputIndex += 1
	}
	return outputTokenStream
}

// update the synonymFilter.config map with the fst, hashToSynonyms, hashToPhrase, fuzziness and prefix.
func (s *SynonymFilter) SetSynonymInfo(fst []byte, hashToSynonyms map[uint64][]uint64,
	hashToPhrase map[uint64][]byte, fuzziness int, prefix int) {
	s.fst = fst
	s.hashToSynonyms = hashToSynonyms
	s.hashToPhrase = hashToPhrase
	s.fuzziness = fuzziness
	s.prefix = prefix
}

func SynonymFilterConstructor(config map[string]interface{}, cache *registry.Cache) (analysis.TokenFilter, error) {
	return &SynonymFilter{}, nil
}

func init() {
	registry.RegisterTokenFilter(Name, SynonymFilterConstructor)
}
