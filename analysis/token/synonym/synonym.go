package synonym

import (
	"log"

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

// A recursive function that performs a DFS from the state present in the path, which is the input FST path.
//
// MatchTry is the word that must be fuzzily matched in the FST, starting from path.
//
// Depth is the maximum depth to which the DFS must be performed.
//
// ValidPaths is the list of valid paths that were found in the FST and if the matchTry is fuzzily matched in
// the FST, validPaths is appended with the new path that was found.
//
// Fst is the FST that is being searched.
//
// Fuzziness is the maximum Levenshtein distance allowed between matchTry and the word fuzzily matched in the FST.
func fuzzyMatch(path *fstPath, matchTry string, depth int,
	validPaths []*fstPath, fst *vellum.FST, fuzziness int) ([]*fstPath, error) {
	// if input FST path ends with a word and if the word fuzzily matches the matchTry, append the validPaths with the input FST path.
	endsWithWord, err := pathEndsWithWord(path, fst)
	if err != nil {
		return nil, err
	}
	if endsWithWord {
		if search.LevenshteinDistance(string(path.word), matchTry) <= fuzziness {
			validPaths = append(validPaths, path)
		}
	}
	// if depth is not 0, perform a DFS from the current state of the input FST path.
	// even if the path ends with a word, the DFS is performed since there there may be
	// another transition apart from a space at the state in the path.
	if depth != 0 {
		transitions, err := fst.GetTransitionsForState(path.state)
		if err != nil {
			return nil, err
		}
		// for each transition from the current state of the input FST path, perform a DFS.
		// if the transition is not a space, append the character to the word in the input FST path,
		// update the state and output of the input FST path and perform a DFS from the new state by calling
		// the fuzzyMatch function recursively.
		for _, character := range transitions {
			if character != SeparatingCharacter {
				newState, newOutput := fst.AcceptWithVal(path.state, character)
				path.word = append(path.word, character)
				path.state = newState
				path.output += newOutput
				validPaths, err = fuzzyMatch(path, matchTry, depth-1, validPaths, fst, fuzziness)
				if err != nil {
					return nil, err
				}
			}
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

func checkForMatch(s *SynonymFilter, input analysis.TokenStream, inputIndex int, fst *vellum.FST) (*matchParameters, error) {
	rv := &matchParameters{
		newInputIndex:   inputIndex + 1,
		matchedSynonyms: nil,
		firstPos:        input[inputIndex].Position,
		lastPos:         input[inputIndex].Position,
	}
	var seenSynonyms []uint64
	var validPaths []*fstPath
	var tokenLen, matchLen, pathIndex int
	var err error
	validPaths = append(validPaths, &fstPath{
		state:  fst.Start(),
		output: 0,
		word:   nil,
	})
	numValidPaths := len(validPaths)
	prevPos := input[inputIndex].Position
	for inputIndex != len(input) {
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
							tokenLen-matchLen+s.fuzziness, validPaths, fst, s.fuzziness)
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
							s.fuzziness, validPaths, fst, s.fuzziness)
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
		seenSynonyms = nil
		matchFound := false
		pathIndex = 0
		// for each valid path, if the state is a matching state, append the seenSynonyms with the synonyms
		// by using the output of the path as the key to the hashToSynonyms map.
		// filter out all paths that do not have a transition for space from the current state, since each
		// valid path is a word in the FST, the "dead-ends", or the paths that do not have any transition
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
		log.Fatal(err)
	}
	var outputTokenStream analysis.TokenStream
	// inputIndex is the index of a token in the input token stream.
	// outputIndex is the index of a token in the output token stream.
	var inputIndex int
	outputIndex := 1
	for inputIndex < len(input) {
		rv, err := checkForMatch(s, input, inputIndex, fst)
		if err != nil {
			log.Fatal(err)
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
	err = fst.Close()
	if err != nil {
		log.Fatal(err)
	}
	return outputTokenStream
}

func SynonymFilterConstructor(config map[string]interface{}, cache *registry.Cache) (analysis.TokenFilter, error) {
	return &SynonymFilter{
		fst:            config["fst"].([]byte),
		hashToSynonyms: config["hashToSynonyms"].(map[uint64][]uint64),
		hashToPhrase:   config["hashToPhrase"].(map[uint64][]byte),
		fuzziness:      config["fuzziness"].(int),
		prefix:         config["prefix"].(int),
	}, nil
}

func init() {
	registry.RegisterTokenFilter(Name, SynonymFilterConstructor)
}
