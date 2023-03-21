package synonymFilter

import (
	"bytes"
	"fmt"
	"hash/fnv"
	"log"
	"sort"
	"strings"

	"github.com/blevesearch/bleve/v2/analysis"
	"github.com/blevesearch/bleve/v2/registry"
	"github.com/blevesearch/vellum"
)

const Name = "synonymFilter"

type SynonymFilter struct {
	synonymMap *[]analysis.SynonymStruct
}

type SynonymGraphNode struct {
	token string
	node  int
	orig  bool
}

type vellumStruct struct {
	synonymOffsets []uint64
	keepOrig       bool
}

type vellumPair struct {
	word        string
	vellumIndex uint64
}

func hash(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}

func remove(s []uint64, i int) []uint64 {
	s[i] = s[len(s)-1]
	return s[:len(s)-1]
}

func CheckForMatch(tokenPos int, input analysis.TokenStream, consumedTokens *[]*analysis.Token, keepOrig *bool, tokenEnds *[]int, tokenStarts *[]int, fst *vellum.FST, vellumStructMap map[uint64]vellumStruct) (int, *[]uint64, int) {
	inputSize := len(input)
	var matchedSynonyms *[]uint64 = nil
	maxMatchedTokenPos := tokenPos + 1
	curr := fst.Start()
	var output uint64 = 0
	var tmp uint64 = 0
	var isMatch bool = false
	var consumedTokenLen = 0
matchFailed:
	for tokenPos != inputSize {
		for _, character := range input[tokenPos].Term {
			curr, tmp = fst.AcceptWithVal(curr, character)
			if curr == 1 {
				break matchFailed
			}
			output += tmp
		}
		*consumedTokens = append(*consumedTokens, input[tokenPos])
		*tokenStarts = append(*tokenStarts, input[tokenPos].Start)
		*tokenEnds = append(*tokenEnds, input[tokenPos].End)
		tokenPos++
		isMatch, tmp = fst.IsMatchWithVal(curr)
		if isMatch {
			vellStruct := vellumStructMap[output+tmp]
			matchedSynonyms = &(vellStruct.synonymOffsets)
			*keepOrig = vellStruct.keepOrig
			maxMatchedTokenPos = tokenPos
			consumedTokenLen = len(*consumedTokens)
		}
		curr, tmp = fst.AcceptWithVal(curr, ' ')
		output += tmp
	}
	return maxMatchedTokenPos, matchedSynonyms, consumedTokenLen
}

func makeGraphAdjacencyList(numberOfVertices int) map[int][]SynonymGraphNode {
	return make(map[int][]SynonymGraphNode)
}

func getTokensFromOffset(matchedSynonymPos *[]uint64, consumedTokens []*analysis.Token, startNode int, endNode *int, depth *int, indexFile map[uint64]string, consumedTokenLen int) map[int][]SynonymGraphNode {
	NumberOfNodes := 0
	if consumedTokens != nil {
		NumberOfNodes += consumedTokenLen - 1
	}
	var SynonymTokensContainer [][]string

	for _, fileOffset := range *matchedSynonymPos {
		SynonymString := indexFile[fileOffset]
		var SynonymTokens []string
		var sb strings.Builder
		for _, character := range SynonymString {
			if character == ' ' {
				SynonymTokens = append(SynonymTokens, sb.String())
				sb.Reset()
				continue
			}
			sb.WriteRune(character)
		}
		SynonymTokens = append(SynonymTokens, sb.String())
		SynonymTokensContainer = append(SynonymTokensContainer, SynonymTokens)
		if len(SynonymTokens) > *depth {
			*depth = len(SynonymTokens)
		}
		NumberOfNodes += len(SynonymTokens) - 1
	}
	*endNode = startNode + NumberOfNodes + 1

	AdjList := makeGraphAdjacencyList(NumberOfNodes + 1)
	newNodeCount := 0
	for _, SynonymTokens := range SynonymTokensContainer {
		pathEndNode := 0
		if len(SynonymTokens) == 1 {
			pathEndNode = *endNode
		} else {
			pathEndNode = startNode + newNodeCount + 1
			newNodeCount += len(SynonymTokens) - 1
		}
		AdjList[startNode] = append(AdjList[startNode], SynonymGraphNode{
			token: SynonymTokens[0],
			node:  pathEndNode,
			orig:  false,
		})
		if len(SynonymTokens) > 1 {
			for i := 1; i < len(SynonymTokens)-1; i++ {
				AdjList[pathEndNode] = append(AdjList[pathEndNode], SynonymGraphNode{
					token: SynonymTokens[i],
					node:  pathEndNode + 1,
					orig:  false,
				})
				pathEndNode++
			}
			AdjList[pathEndNode] = append(AdjList[pathEndNode], SynonymGraphNode{
				token: SynonymTokens[len(SynonymTokens)-1],
				node:  *endNode,
				orig:  false,
			})
		}
	}
	if consumedTokens != nil {
		if consumedTokenLen > *depth {
			*depth = consumedTokenLen
		}
		pathEndNode := 0
		if consumedTokenLen == 1 {
			pathEndNode = *endNode
		} else {
			pathEndNode = startNode + newNodeCount + 1
		}
		AdjList[startNode] = append(AdjList[startNode], SynonymGraphNode{
			token: string(consumedTokens[0].Term),
			node:  pathEndNode,
			orig:  true,
		})
		if consumedTokenLen > 1 {
			for i := 1; i < consumedTokenLen-1; i++ {
				AdjList[pathEndNode] = append(AdjList[pathEndNode], SynonymGraphNode{
					token: string(consumedTokens[i].Term),
					node:  pathEndNode + 1,
					orig:  true,
				})
				pathEndNode++
			}
			AdjList[pathEndNode] = append(AdjList[pathEndNode], SynonymGraphNode{
				token: string(consumedTokens[consumedTokenLen-1].Term),
				node:  *endNode,
				orig:  true,
			})
		}
	}
	return AdjList
}

func FlattenGraph(AdjList map[int][]SynonymGraphNode, StartNode int, EndNode int, Depth int, OutputTokenStream *analysis.TokenStream, start int, tokenEnds []int, tokenStarts []int, consumedTokenLen int) int {
	Position := StartNode
	FinalPosition := Depth + StartNode
	frontier := []int{StartNode}
	tokenEndPos := 0
	for Position <= FinalPosition && len(frontier) != 0 {
		l := len(frontier)
		for i := 0; i < l; i++ {
			Curnode := frontier[0]
			frontier = frontier[1:]
			for _, neighbor := range AdjList[Curnode] {
				typeForToken := analysis.Synonym
				if neighbor.orig {
					typeForToken = 0
				}
				positionForToken := Position
				posLenForToken := 1
				endForToken := tokenEnds[tokenEndPos]
				if neighbor.node == EndNode {
					posLenForToken = FinalPosition - Position
					endForToken = tokenEnds[consumedTokenLen-1]
				}
				OutputToken := analysis.Token{
					Term:      []byte(neighbor.token),
					Start:     tokenStarts[tokenEndPos],
					End:       endForToken,
					Type:      typeForToken,
					Position:  positionForToken,
					PosLength: posLenForToken,
				}
				*OutputTokenStream = append(*OutputTokenStream, &OutputToken)
				frontier = append(frontier, neighbor.node)
			}
		}
		if tokenEndPos < consumedTokenLen-1 {
			tokenEndPos++
		}
		Position++
	}
	return FinalPosition
}

func buildVellumFST(s *SynonymFilter, indexFile map[uint64]string, vellumStructMap map[uint64]vellumStruct) *vellum.FST {
	var vellumIndex uint64 = 1
	var vellumAddArray []vellumPair
	for _, synonym := range *s.synonymMap {
		if synonym.BiDirectional {
			var numList []uint64
			for _, lhs := range synonym.LHS {
				var hashval = hash(string(lhs))
				indexFile[hashval] = string(lhs)
				numList = append(numList, hashval)
			}
			for lhsNum := 0; lhsNum < len(numList); lhsNum++ {
				synOff := make([]uint64, len(numList))
				copy(synOff, numList)
				synOff = remove(synOff, lhsNum)
				vellumStructMap[vellumIndex] = vellumStruct{
					synonymOffsets: synOff,
					keepOrig:       synonym.KeepOrig,
				}
				vellumAddArray = append(vellumAddArray, vellumPair{
					word:        indexFile[numList[lhsNum]],
					vellumIndex: vellumIndex,
				})
				vellumIndex++
			}
		} else {
			var numList []uint64
			for _, rhs := range synonym.RHS {
				var hashval = hash(string(rhs))
				indexFile[hashval] = string(rhs)
				numList = append(numList, hashval)
			}

			vellumStructMap[vellumIndex] = vellumStruct{
				synonymOffsets: numList,
				keepOrig:       synonym.KeepOrig,
			}
			for _, lhs := range synonym.LHS {
				vellumAddArray = append(vellumAddArray, vellumPair{
					word:        string(lhs),
					vellumIndex: vellumIndex,
				})
			}
			vellumIndex++
		}
	}
	sort.Slice(vellumAddArray, func(i, j int) bool {
		return vellumAddArray[i].word < vellumAddArray[j].word
	})

	var buf bytes.Buffer
	builder, err := vellum.New(&buf, nil)
	if err != nil {
		log.Fatal(err)
	}

	for _, velPair := range vellumAddArray {
		err = builder.Insert([]byte(velPair.word), uint64(velPair.vellumIndex))
		if err != nil {
			log.Fatal(err)
		}
	}
	err = builder.Close()
	if err != nil {
		log.Fatal(err)
	}

	fst, err := vellum.Load(buf.Bytes())
	if err != nil {
		log.Fatal(err)
	}
	return fst
}

func (s *SynonymFilter) Filter(input analysis.TokenStream) analysis.TokenStream {
	var matchedSynonyms *[]uint64 = nil
	var OutputTokenStream analysis.TokenStream
	var consumedTokens []*analysis.Token
	var keepOrig = false
	var startNode = 1
	var endNode = -1
	var depth int = 1
	tokenPos := 0
	var AdjList map[int][]SynonymGraphNode
	var tokenEnds []int
	var tokenStarts []int
	var consumedTokenLen = 0
	indexFile := make(map[uint64]string)
	vellumStructMap := make(map[uint64]vellumStruct)
	fst := buildVellumFST(s, indexFile, vellumStructMap)

	for tokenPos < len(input) {
		matchedSynonyms = nil
		consumedTokens = nil
		consumedTokenLen = 0
		depth = 1
		start := input[tokenPos].Start
		tokenStarts = nil
		tokenEnds = nil
		tokenPos, matchedSynonyms, consumedTokenLen = CheckForMatch(tokenPos, input, &consumedTokens, &keepOrig, &tokenEnds, &tokenStarts, fst, vellumStructMap)
		if matchedSynonyms != nil {
			if !keepOrig {
				AdjList = getTokensFromOffset(matchedSynonyms, nil, startNode, &endNode, &depth, indexFile, consumedTokenLen)
			} else {
				AdjList = getTokensFromOffset(matchedSynonyms, consumedTokens, startNode, &endNode, &depth, indexFile, consumedTokenLen)
			}
			startNode = FlattenGraph(AdjList, startNode, endNode, depth, &OutputTokenStream, start, tokenEnds, tokenStarts, consumedTokenLen)
		} else {
			input[tokenPos-1].PosLength = 1
			input[tokenPos-1].Position = startNode
			OutputTokenStream = append(OutputTokenStream, input[tokenPos-1])
			startNode += 1
		}
	}
	return OutputTokenStream
}

func NewSynonymFilter(synonymMap *[]analysis.SynonymStruct) *SynonymFilter {
	return &SynonymFilter{
		synonymMap: synonymMap,
	}
}

func SynonymFilterConstructor(config map[string]interface{}, cache *registry.Cache) (analysis.TokenFilter, error) {
	synonymMap, ok := config["synonymMap"].(*[]analysis.SynonymStruct)
	if !ok {
		return nil, fmt.Errorf("must specify synonym map")
	}
	return NewSynonymFilter(synonymMap), nil
}

func init() {
	registry.RegisterTokenFilter(Name, SynonymFilterConstructor)
}
