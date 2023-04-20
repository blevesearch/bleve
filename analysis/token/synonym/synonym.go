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

func fuzzySearchFST(output uint64, curr int, depth int,
	fuzzyQueue []fuzzyStruct, fst *vellum.FST, curWord string, matchTry string,
	fuzziness int) ([]fuzzyStruct, error) {

	newCur := fst.Accept(curr, SeparatingCharacter)
	numEdges, err := fst.GetNumTransitionsForState(curr)
	if err != nil {
		return nil, err
	}
	if newCur != 1 || numEdges == 0 {
		distance := search.LevenshteinDistance(curWord, matchTry)
		if distance <= fuzziness {
			fuzzyQueue = append(fuzzyQueue, fuzzyStruct{
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

func checkForMatch(tokenPos int, input analysis.TokenStream, keepOrig *bool,
	fst *vellum.FST, vellumStructMap map[uint64][]uint64, synTokenEnd *int,
	fuzziness int, prefix int) (int, []uint64, analysis.TokenStream, error) {

	maxMatchedTokenPos := tokenPos + 1
	fuzzyQueueLen := 1
	var matchedSynonyms, seenSynonyms []uint64
	var consumedTokens, seenTokens analysis.TokenStream
	var fuzzyQueue, queueWithoutDeadEnds []fuzzyStruct
	var currOutput, tmp uint64
	var tokenLen, matchLen, queuePos, curr, newCurr int
	var matched, isMatch bool
	var err error
	fuzzyQueue = append(fuzzyQueue, fuzzyStruct{
		state:  fst.Start(),
		output: 0,
	})
	for tokenPos != len(input) {
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
					if fuzziness == 0 {
						return maxMatchedTokenPos, matchedSynonyms,
							consumedTokens, nil
					} else if matchLen < prefix {
						break
					} else {
						fuzzyQueue, err = fuzzySearchFST(currOutput, curr,
							tokenLen-matchLen+fuzziness, fuzzyQueue, fst,
							string(curWord), string(input[tokenPos].Term),
							fuzziness)
						if err != nil {
							return -1, nil, nil, err
						}
						break
					}
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
					return -1, nil, nil, err
				}
				if numEdges == 0 || wholeWord != 1 {
					fuzzyQueue = append(fuzzyQueue, fuzzyStruct{
						output: currOutput,
						state:  curr,
					})
				} else {
					if fuzziness == 0 {
						return maxMatchedTokenPos, matchedSynonyms,
							consumedTokens, nil
					} else if matchLen < prefix {
						break
					} else {
						fuzzyQueue, err = fuzzySearchFST(currOutput, curr,
							fuzziness, fuzzyQueue, fst, string(curWord),
							string(input[tokenPos].Term), fuzziness)
						if err != nil {
							return -1, nil, nil, err
						}
					}
				}
			}
			queuePos += 1
		}
		seenTokens = append(seenTokens, input[tokenPos])
		tokenPos++
		seenSynonyms = nil
		queueWithoutDeadEnds = nil
		matched = false
		for _, val := range fuzzyQueue {
			isMatch, tmp = fst.IsMatchWithVal(val.state)
			if isMatch {
				vellumTailValue := vellumStructMap[val.output+tmp]
				hashedSynonyms := vellumTailValue[1:]
				if fuzziness != 0 {
					hashedSynonyms = append(hashedSynonyms, val.output+tmp)
				}
				seenSynonyms = append(seenSynonyms, hashedSynonyms...)
				if vellumTailValue[0] == 1 {
					*keepOrig = true
				}
				matched = true
			}
			deadEnd, tmp := fst.AcceptWithVal(val.state, SeparatingCharacter)
			if deadEnd != 1 {
				val.state = deadEnd
				val.output = val.output + tmp
				queueWithoutDeadEnds = append(queueWithoutDeadEnds, val)
			}
		}
		if matched {
			*synTokenEnd = input[tokenPos-1].End
			maxMatchedTokenPos = tokenPos
			consumedTokens = seenTokens
			matchedSynonyms = seenSynonyms
		}
		fuzzyQueue = queueWithoutDeadEnds
		fuzzyQueueLen = len(fuzzyQueue)
		if fuzzyQueueLen == 0 {
			return maxMatchedTokenPos, matchedSynonyms, consumedTokens, nil
		}
	}
	return maxMatchedTokenPos, matchedSynonyms, consumedTokens, nil
}

func getAdjListOfSynonymGraph(matchedSynonymPos []uint64,
	consumedTokens analysis.TokenStream, startNode int, keepOrig bool,
	byteSliceHashMap map[uint64][]byte, synTokenStart int, synTokenEnd int,
	outputTokenStream analysis.TokenStream) (analysis.TokenStream, int) {

	var newNodeCount, pathEndNode, loopVariable, numberOfNodes int
	var synonymTokensContainer = make([][][]byte, len(matchedSynonymPos))
	var tmpStr []byte
	var finalNode bool

	if consumedTokens != nil {
		numberOfNodes += len(consumedTokens) - 1
	}
	for index, hashval := range matchedSynonymPos {
		for _, character := range byteSliceHashMap[hashval] {
			if character == SeparatingCharacter {
				synonymTokensContainer[index] = append(synonymTokensContainer[index], tmpStr)
				tmpStr = nil
			} else {
				tmpStr = append(tmpStr, character)
			}
		}
		synonymTokensContainer[index] = append(synonymTokensContainer[index], tmpStr)
		numberOfNodes += len(synonymTokensContainer[index]) - 1
		tmpStr = nil
	}
	endNode := startNode + numberOfNodes + 1
	for _, synonymTokens := range synonymTokensContainer {
		finalNode = false
		if len(synonymTokens) == 1 {
			pathEndNode = endNode
			finalNode = true
		} else {
			pathEndNode = startNode + newNodeCount + 1
			newNodeCount += len(synonymTokens) - 1
		}
		outputTokenStream = append(outputTokenStream, &analysis.Token{
			Term:        synonymTokens[0],
			Type:        analysis.Synonym,
			CurrentNode: startNode,
			NextNode:    pathEndNode,
			Start:       synTokenStart,
			End:         synTokenEnd,
			FinalNode:   finalNode,
		})
		if len(synonymTokens) > 1 {
			for loopVariable = 1; loopVariable < len(synonymTokens)-1; loopVariable++ {
				outputTokenStream = append(outputTokenStream, &analysis.Token{
					Term:        synonymTokens[loopVariable],
					Type:        analysis.Synonym,
					CurrentNode: pathEndNode,
					NextNode:    pathEndNode + 1,
					Start:       synTokenStart,
					End:         synTokenEnd,
				})
				pathEndNode++
			}
			outputTokenStream = append(outputTokenStream, &analysis.Token{
				Term:        synonymTokens[loopVariable],
				Type:        analysis.Synonym,
				CurrentNode: pathEndNode,
				NextNode:    endNode,
				Start:       synTokenStart,
				End:         synTokenEnd,
				FinalNode:   true,
			})
		}
	}
	finalNode = false
	if keepOrig {
		if len(consumedTokens) == 1 {
			pathEndNode = endNode
			finalNode = true
		} else {
			pathEndNode = startNode + newNodeCount + 1
		}
		consumedTokens[0].CurrentNode = startNode
		consumedTokens[0].NextNode = pathEndNode
		consumedTokens[0].FinalNode = finalNode
		outputTokenStream = append(outputTokenStream, consumedTokens[0])
		if len(consumedTokens) > 1 {
			for loopVariable = 1; loopVariable < len(consumedTokens)-1; loopVariable++ {
				consumedTokens[loopVariable].CurrentNode = pathEndNode
				consumedTokens[loopVariable].NextNode = pathEndNode + 1
				outputTokenStream = append(outputTokenStream,
					consumedTokens[loopVariable])
				pathEndNode++
			}
			consumedTokens[loopVariable].CurrentNode = pathEndNode
			consumedTokens[loopVariable].NextNode = endNode
			consumedTokens[loopVariable].FinalNode = true
			outputTokenStream = append(outputTokenStream,
				consumedTokens[loopVariable])
		}
	}
	return outputTokenStream, endNode
}

func (s *SynonymFilter) Filter(input analysis.TokenStream) analysis.TokenStream {
	fst, err := vellum.Load(s.Fst)
	if err != nil {
		log.Fatal(err)
	}
	var matchedSynonyms []uint64
	var outputTokenStream, consumedTokens analysis.TokenStream
	var keepOrig bool
	var endNode, tokenPos, synTokenStart, synTokenEnd int
	startNode := 1
	for tokenPos < len(input) {
		consumedTokens = nil
		synTokenStart = input[tokenPos].Start
		synTokenEnd = 0
		tokenPos, matchedSynonyms, consumedTokens, err = checkForMatch(tokenPos,
			input, &keepOrig, fst, s.VellumMap, &synTokenEnd, s.Fuzziness, s.Prefix)
		if err != nil {
			log.Fatal(err)
		}
		if matchedSynonyms != nil {
			outputTokenStream, endNode = getAdjListOfSynonymGraph(matchedSynonyms,
				consumedTokens, startNode, keepOrig, s.ByteSliceHashMap, synTokenStart,
				synTokenEnd, outputTokenStream)
			startNode = endNode
		} else {
			input[tokenPos-1].CurrentNode = startNode
			input[tokenPos-1].NextNode = startNode + 1
			outputTokenStream = append(outputTokenStream, input[tokenPos-1])
			startNode += 1
		}
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
