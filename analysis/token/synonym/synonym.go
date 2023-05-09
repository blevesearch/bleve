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
	Fst              []byte
	ByteSliceHashMap map[uint64][]byte
	VellumMap        map[uint64][]uint64
	Fuzziness        int
	Prefix           int
}

type fuzzyStruct struct {
	output uint64
	state  int
}

type checkForMatchReturnVal struct {
	maxMatchedTokenPos int
	matchedSynonyms    []uint64
	firstPos           int
	lastPos            int
}

func fuzzySearchFST(output uint64, curr int, depth int,
	fuzzyQueue []*fuzzyStruct, fst *vellum.FST, curWord string, matchTry string,
	fuzziness int) ([]*fuzzyStruct, error) {

	newCur := fst.Accept(curr, SeparatingCharacter)
	numEdges, err := fst.GetNumTransitionsForState(curr)
	if err != nil {
		return nil, err
	}
	if newCur != 1 || numEdges == 0 {
		distance := search.LevenshteinDistance(curWord, matchTry)
		if distance <= fuzziness {
			fuzzyQueue = append(fuzzyQueue, &fuzzyStruct{
				output: output,
				state:  curr,
			})
		}
	}
	if depth != 0 {
		edges, err := fst.GetTransitionsForState(curr)
		if err != nil {
			return nil, err
		}
		for _, i := range edges {
			if i != SeparatingCharacter {
				newCur, newOut := fst.AcceptWithVal(curr, i)
				newCurWord := curWord + string(i)
				fuzzyQueue, err = fuzzySearchFST(output+newOut, newCur, depth-1,
					fuzzyQueue, fst, newCurWord, matchTry, fuzziness)
				if err != nil {
					return nil, err
				}
			}
		}
	}
	return fuzzyQueue, nil
}

func matchWildCardTokens(numStopWords int, currState *fuzzyStruct, fst *vellum.FST) *fuzzyStruct {
	if numStopWords == 0 {
		return nil
	}
	loopVariable := 0
	tmpSum := currState.output
	foundSpace := currState.state
	var tmp uint64
	for loopVariable = 0; loopVariable < numStopWords; loopVariable++ {
		foundSpace, tmp = fst.AcceptWithVal(foundSpace, SeparatingCharacter)
		if foundSpace != 1 {
			tmpSum += tmp
		} else {
			break
		}
	}
	if loopVariable == numStopWords {
		currState.state = foundSpace
		currState.output = tmpSum
		return currState
	}
	return nil
}

func checkForMatch(s *SynonymFilter, input analysis.TokenStream, tokenPos int, fst *vellum.FST) (*checkForMatchReturnVal, error) {
	rv := &checkForMatchReturnVal{
		maxMatchedTokenPos: tokenPos + 1,
		matchedSynonyms:    nil,
		firstPos:           input[tokenPos].Position,
		lastPos:            input[tokenPos].Position,
	}
	var seenSynonyms []uint64
	var fuzzyQueue, fuzzyQueue2 []*fuzzyStruct
	var currOutput, tmp uint64
	var tokenLen, matchLen, queuePos, curr, newCurr int
	var matched, isMatch bool
	var err error

	initFuzzyStruct := fuzzyStruct{
		state:  fst.Start(),
		output: 0,
	}
	fuzzyQueue = append(fuzzyQueue, &initFuzzyStruct)
	fuzzyQueueLen := len(fuzzyQueue)
	prevPos := input[tokenPos].Position
	for tokenPos != len(input) {
		fuzzyQueue2 = nil
		numStopWords := input[tokenPos].Position - prevPos - 1
		if numStopWords > 0 {
			for _, val := range fuzzyQueue {
				rv := matchWildCardTokens(numStopWords, val, fst)
				if rv != nil {
					fuzzyQueue2 = append(fuzzyQueue2, rv)
				}
			}
			fuzzyQueue = fuzzyQueue2
			fuzzyQueueLen = len(fuzzyQueue)
		}
		queuePos = 0
		for queuePos < fuzzyQueueLen {
			tokenLen = len(input[tokenPos].Term)
			matchLen = 0
			var curWord []byte
			curr = fuzzyQueue[0].state
			currOutput = fuzzyQueue[0].output
			fuzzyQueue = fuzzyQueue[1:]
			for _, character := range input[tokenPos].Term {
				newCurr, tmp = fst.AcceptWithVal(curr, character)
				if newCurr == 1 {
					if s.Fuzziness != 0 && matchLen >= s.Prefix {
						fuzzyQueue, err = fuzzySearchFST(currOutput, curr,
							tokenLen-matchLen+s.Fuzziness, fuzzyQueue, fst,
							string(curWord), string(input[tokenPos].Term),
							s.Fuzziness)
						if err != nil {
							return nil, err
						}
					}
					break
				} else {
					matchLen++
					curr = newCurr
					currOutput += tmp
					curWord = append(curWord, character)
				}
			}
			if matchLen == tokenLen {
				wholeWord := fst.Accept(curr, SeparatingCharacter)
				numEdges, err := fst.GetNumTransitionsForState(curr)
				if err != nil {
					return nil, err
				}
				if numEdges == 0 || wholeWord != 1 {
					fuzzyQueue = append(fuzzyQueue, &fuzzyStruct{
						output: currOutput,
						state:  curr,
					})
				} else {
					if s.Fuzziness != 0 && matchLen >= s.Prefix {
						fuzzyQueue, err = fuzzySearchFST(currOutput, curr,
							s.Fuzziness, fuzzyQueue, fst, string(curWord),
							string(input[tokenPos].Term), s.Fuzziness)
						if err != nil {
							return nil, err
						}
					}
				}
			}
			queuePos += 1
		}
		prevPos = input[tokenPos].Position
		tokenPos++
		seenSynonyms = nil
		fuzzyQueue2 = nil
		matched = false
		for _, val := range fuzzyQueue {
			isMatch, tmp = fst.IsMatchWithVal(val.state)
			if isMatch {
				hashedSynonyms := s.VellumMap[val.output+tmp]
				hashedSynonyms = append(hashedSynonyms, val.output+tmp)
				seenSynonyms = append(seenSynonyms, hashedSynonyms...)
				matched = true
			}
			deadEnd, tmp := fst.AcceptWithVal(val.state, SeparatingCharacter)
			if deadEnd != 1 {
				val.state = deadEnd
				val.output = val.output + tmp
				fuzzyQueue2 = append(fuzzyQueue2, val)
			}
		}
		if matched {
			rv.maxMatchedTokenPos = tokenPos
			rv.matchedSynonyms = seenSynonyms
			rv.lastPos = prevPos
		}
		fuzzyQueue = fuzzyQueue2
		fuzzyQueueLen = len(fuzzyQueue)
		if fuzzyQueueLen == 0 {
			return rv, nil
		}
	}
	return rv, nil
}

func (s *SynonymFilter) Filter(input analysis.TokenStream) analysis.TokenStream {
	fst, err := vellum.Load(s.Fst)
	if err != nil {
		log.Fatal(err)
	}
	var outputTokenStream analysis.TokenStream
	var tokenPos int
	tokenIndex := 1
	for tokenPos < len(input) {
		rv, err := checkForMatch(s, input, tokenPos, fst)
		tokenPos = rv.maxMatchedTokenPos
		if err != nil {
			log.Fatal(err)
		}
		if rv.matchedSynonyms != nil {
			for _, hashval := range rv.matchedSynonyms {
				synonym := s.ByteSliceHashMap[hashval]
				outputTokenStream = append(outputTokenStream, &analysis.Token{
					Term:          synonym,
					Type:          analysis.Synonym,
					Position:      tokenIndex,
					FirstPosition: rv.firstPos,
					LastPosition:  rv.lastPos,
				})
			}
		} else {
			input[tokenPos-1].FirstPosition = input[tokenPos-1].Position
			input[tokenPos-1].LastPosition = input[tokenPos-1].Position
			input[tokenPos-1].Position = tokenIndex
			outputTokenStream = append(outputTokenStream, input[tokenPos-1])
		}
		tokenIndex += 1
	}
	return outputTokenStream
}

func SynonymFilterConstructor(config map[string]interface{}, cache *registry.Cache) (analysis.TokenFilter, error) {
	return &SynonymFilter{
		Fst:              config["fst"].([]byte),
		VellumMap:        config["vellumMap"].(map[uint64][]uint64),
		ByteSliceHashMap: config["byteSliceHashMap"].(map[uint64][]byte),
		Fuzziness:        config["fuzziness"].(int),
		Prefix:           config["prefix"].(int),
	}, nil
}

func init() {
	registry.RegisterTokenFilter(Name, SynonymFilterConstructor)
}
