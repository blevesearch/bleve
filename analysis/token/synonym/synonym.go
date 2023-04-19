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

var equivalentSynonymType = []byte("equivalent")
var explicitSynonymType = []byte("explicit")

type SynonymFilter struct {
	Fst              []byte
	ByteSliceHashMap map[uint64][]byte
	VellumMap        map[uint64][]uint64
	Fuzziness        int
	Prefix           int
}

type synonymGraphNode struct {
	token    []byte
	node     int
	orig     bool
	position int
	start    int
	end      int
}
type fuzzyStruct struct {
	output uint64
	state  int
}

func fuzzySearchFST(output uint64, curr int, depth int, fuzzyQueue []fuzzyStruct, fst *vellum.FST, curWord string, matchTry string, fuzziness int) ([]fuzzyStruct, bool, error) {
	var fuzzyMatched = false
	newCur := fst.Accept(curr, SeparatingCharacter)
	numEdges, err := fst.GetNumTransitionsForState(curr)
	if err != nil {
		return nil, false, err
	}
	if newCur != 1 || numEdges == 0 {
		_, exceeded := search.LevenshteinDistanceMax(curWord, matchTry, fuzziness)
		if !exceeded {
			fuzzyQueue = append(fuzzyQueue, fuzzyStruct{
				output: output,
				state:  curr,
			})
			fuzzyMatched = true
		}
	}
	if depth != 0 {
		edges, err := fst.GetTransitionsForState(curr)
		if err != nil {
			return nil, false, err
		}
		for _, i := range edges {
			if i != SeparatingCharacter {
				newCur, newOut := fst.AcceptWithVal(curr, i)
				newCurWord := curWord + string(i)
				fuzzyQueue, fuzzyMatched, err = fuzzySearchFST(output+newOut, newCur, depth-1, fuzzyQueue, fst, newCurWord, matchTry, fuzziness)
				if err != nil {
					return nil, false, err
				}
			}
		}
	}
	return fuzzyQueue, fuzzyMatched, nil
}

func checkForMatch(tokenPos int, input analysis.TokenStream, keepOrig *bool,
	fst *vellum.FST, vellumStructMap map[uint64][]uint64, synTokenEnd *int, fuzziness int, prefix int) (int, *[]uint64, []*analysis.Token, error) {

	var maxMatchedTokenPos = tokenPos + 1
	var matchedSynonyms *[]uint64
	var consumedTokens []*analysis.Token
	var err error

	var seenTokens []*analysis.Token
	var tmp uint64
	var isMatch bool
	var fuzzyQueue []fuzzyStruct
	var curr int
	var newCurr int
	var currOutput uint64
	var tokenLen int
	var matchLen int
	var fuzzyQueueLen = 1
	var queuePos int
	var fuzzyMatched bool
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
					if fuzziness == 0 || matchLen < prefix {
						return maxMatchedTokenPos, matchedSynonyms, consumedTokens, nil
					} else {
						fuzzyQueue, fuzzyMatched, err = fuzzySearchFST(currOutput, curr, tokenLen-matchLen+fuzziness-1, fuzzyQueue, fst, string(curWord), string(input[tokenPos].Term), fuzziness)
						if err != nil {
							return -1, nil, nil, err
						}
						if !fuzzyMatched {
							return maxMatchedTokenPos, matchedSynonyms, consumedTokens, nil
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
					if fuzziness == 0 || matchLen < prefix {
						return maxMatchedTokenPos, matchedSynonyms, consumedTokens, nil
					} else {
						fuzzyQueue, fuzzyMatched, err = fuzzySearchFST(currOutput, curr, fuzziness, fuzzyQueue, fst, string(curWord), string(input[tokenPos].Term), fuzziness)
						if err != nil {
							return -1, nil, nil, err
						}
						if !fuzzyMatched {
							return maxMatchedTokenPos, matchedSynonyms, consumedTokens, nil
						}
					}
				}
			}
			queuePos += 1
		}
		seenTokens = append(seenTokens, input[tokenPos])
		tokenPos++
		var synonyms []uint64
		var matched = false
		var queueWithoutDeadEnds []fuzzyStruct
		for _, val := range fuzzyQueue {
			isMatch, tmp = fst.IsMatchWithVal(val.state)
			if isMatch {
				vellumTailValue := vellumStructMap[val.output+tmp]
				hashedSynonyms := vellumTailValue[1:]
				hashedSynonyms = append(hashedSynonyms, val.output+tmp)
				synonyms = append(synonyms, hashedSynonyms...)
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
			matchedSynonyms = &synonyms
		}
		fuzzyQueue = queueWithoutDeadEnds
		fuzzyQueueLen = len(fuzzyQueue)
		if fuzzyQueueLen == 0 {
			return maxMatchedTokenPos, matchedSynonyms, consumedTokens, nil
		}
	}
	return maxMatchedTokenPos, matchedSynonyms, consumedTokens, nil
}

func getAdjListOfSynonymGraph(matchedSynonymPos *[]uint64, consumedTokens []*analysis.Token, startNode int,
	byteSliceHashMap map[uint64][]byte, consumedTokenLen int) (map[int][]synonymGraphNode, int) {
	var numberOfNodes = 0
	if consumedTokens != nil {
		numberOfNodes += consumedTokenLen - 1
	}
	var synonymTokensContainer [][][]byte
	var synonymTokens [][]byte
	var tmpStr []byte
	for _, hashval := range *matchedSynonymPos {
		for _, character := range byteSliceHashMap[hashval] {
			if character == SeparatingCharacter {
				synonymTokens = append(synonymTokens, tmpStr)
				tmpStr = nil
				continue
			}
			tmpStr = append(tmpStr, character)
		}
		synonymTokens = append(synonymTokens, tmpStr)
		synonymTokensContainer = append(synonymTokensContainer, synonymTokens)
		numberOfNodes += len(synonymTokens) - 1
		synonymTokens = nil
		tmpStr = nil
	}
	var endNode = startNode + numberOfNodes + 1
	var adjList = make(map[int][]synonymGraphNode)
	var newNodeCount = 0
	var pathEndNode int
	for _, synonymTokens = range synonymTokensContainer {
		if len(synonymTokens) == 1 {
			pathEndNode = endNode
		} else {
			pathEndNode = startNode + newNodeCount + 1
			newNodeCount += len(synonymTokens) - 1
		}
		adjList[startNode] = append(adjList[startNode], synonymGraphNode{
			token: synonymTokens[0],
			node:  pathEndNode,
			orig:  false,
		})
		if len(synonymTokens) > 1 {
			for i := 1; i < len(synonymTokens)-1; i++ {
				adjList[pathEndNode] = append(adjList[pathEndNode], synonymGraphNode{
					token: synonymTokens[i],
					node:  pathEndNode + 1,
					orig:  false,
				})
				pathEndNode++
			}
			adjList[pathEndNode] = append(adjList[pathEndNode], synonymGraphNode{
				token: synonymTokens[len(synonymTokens)-1],
				node:  endNode,
				orig:  false,
			})
		}
	}
	if consumedTokens != nil {
		if consumedTokenLen == 1 {
			pathEndNode = endNode
		} else {
			pathEndNode = startNode + newNodeCount + 1
		}
		adjList[startNode] = append(adjList[startNode], synonymGraphNode{
			token:    consumedTokens[0].Term,
			node:     pathEndNode,
			position: consumedTokens[0].Position,
			start:    consumedTokens[0].Start,
			end:      consumedTokens[0].End,
			orig:     true,
		})
		if consumedTokenLen > 1 {
			for i := 1; i < consumedTokenLen-1; i++ {
				adjList[pathEndNode] = append(adjList[pathEndNode], synonymGraphNode{
					token:    consumedTokens[i].Term,
					node:     pathEndNode + 1,
					orig:     true,
					start:    consumedTokens[i].Start,
					end:      consumedTokens[i].End,
					position: consumedTokens[i].Position,
				})
				pathEndNode++
			}
			adjList[pathEndNode] = append(adjList[pathEndNode], synonymGraphNode{
				token:    consumedTokens[consumedTokenLen-1].Term,
				node:     endNode,
				orig:     true,
				start:    consumedTokens[consumedTokenLen-1].Start,
				end:      consumedTokens[consumedTokenLen-1].End,
				position: consumedTokens[consumedTokenLen-1].Position,
			})
		}
	}
	return adjList, endNode
}

func getOutputTokensFromGraph(adjList map[int][]synonymGraphNode, endNode int, OutputTokenStream *analysis.TokenStream,
	SynTokenStart int, SynTokenEnd int) {
	for node, neighList := range adjList {
		for _, neighbor := range neighList {
			startForToken := SynTokenStart
			endForToken := SynTokenEnd
			typeForToken := analysis.Synonym
			positionForToken := -1
			if neighbor.orig {
				typeForToken = 0
				startForToken = neighbor.start
				endForToken = neighbor.end
				positionForToken = neighbor.position
			}
			posLenForToken := neighbor.node
			OutputToken := analysis.Token{
				Term:        neighbor.token,
				Start:       startForToken,
				End:         endForToken,
				Type:        typeForToken,
				Position:    positionForToken,
				CurrentNode: node,
				NextNode:    posLenForToken,
				FinalNode:   endNode,
			}
			*OutputTokenStream = append(*OutputTokenStream, &OutputToken)
		}
	}
}
func (s *SynonymFilter) Filter(input analysis.TokenStream) analysis.TokenStream {
	fst, err := vellum.Load(s.Fst)
	if err != nil {
		log.Fatal(err)
	}
	var matchedSynonyms *[]uint64
	var outputTokenStream analysis.TokenStream
	var consumedTokens []*analysis.Token
	var keepOrig = false
	var startNode = 1
	var endNode int
	var tokenPos int
	var adjList map[int][]synonymGraphNode
	var synTokenStart int
	var synTokenEnd int
	for tokenPos < len(input) {
		consumedTokens = nil
		synTokenStart = input[tokenPos].Start
		synTokenEnd = 0
		tokenPos, matchedSynonyms, consumedTokens, err = checkForMatch(tokenPos, input, &keepOrig, fst,
			s.VellumMap, &synTokenEnd, s.Fuzziness, s.Prefix)
		if err != nil {
			log.Fatal(err)
		}
		if matchedSynonyms != nil {
			if !keepOrig {
				adjList, endNode = getAdjListOfSynonymGraph(matchedSynonyms, nil, startNode,
					s.ByteSliceHashMap, len(consumedTokens))
			} else {
				adjList, endNode = getAdjListOfSynonymGraph(matchedSynonyms, consumedTokens, startNode,
					s.ByteSliceHashMap, len(consumedTokens))
			}
			getOutputTokensFromGraph(adjList, endNode, &outputTokenStream, synTokenStart, synTokenEnd)
			startNode = endNode
		} else {
			input[tokenPos-1].NextNode = startNode + 1
			input[tokenPos-1].FinalNode = startNode + 1
			input[tokenPos-1].CurrentNode = startNode
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
