package synonymFilter

import (
	"bytes"
	"fmt"
	"hash/fnv"
	"log"
	"sort"

	"github.com/blevesearch/bleve/v2/analysis"
	"github.com/blevesearch/bleve/v2/registry"
	"github.com/blevesearch/vellum"
	"github.com/goombaio/orderedset"
)

const Name = "synonymFilter"

type SynonymFilter struct {
	synonymMap *[]analysis.SynonymStruct
}

type synonymGraphNode struct {
	token []byte
	node  int
	orig  bool
	start int
	end   int
}

type vellumStruct struct {
	synonyms orderedset.OrderedSet
	keepOrig bool
	hashVal  uint64
}

func hash(s []byte) uint64 {
	h := fnv.New64a()
	h.Write(s)
	return h.Sum64()
}

func remove(s []uint64, i int) []uint64 {
	s[i] = s[len(s)-1]
	return s[:len(s)-1]
}

func cleanSynonymMap(synonymMap *[]analysis.SynonymStruct, byteSliceHashMap map[uint64][]byte) map[uint64]vellumStruct {
	var vellumIndex uint64 = 0
	var vellumMap = make(map[uint64]vellumStruct)
	var hashval uint64
	var byteSliceHash []uint64
	for _, synonym := range *synonymMap {
		byteSliceHash = nil
		if synonym.BiDirectional {
			for _, lhs := range synonym.LHS {
				hashval = hash(lhs)
				byteSliceHashMap[hashval] = lhs
				byteSliceHash = append(byteSliceHash, hashval)
			}
			for i, hashval := range byteSliceHash {
				lhsCopy := make([]uint64, len(byteSliceHash))
				copy(lhsCopy, byteSliceHash)
				lhsCopy = remove(lhsCopy, i)
				Original, exists := vellumMap[hashval]
				if exists {
					for _, syn := range lhsCopy {
						Original.synonyms.Add(syn)
					}
					if !Original.keepOrig {
						Original.keepOrig = synonym.KeepOrig
					}
					vellumMap[hashval] = Original
				} else {
					newSet := orderedset.NewOrderedSet()
					for _, syn := range lhsCopy {
						newSet.Add(syn)
					}
					vellumMap[hashval] = vellumStruct{
						synonyms: *newSet,
						keepOrig: synonym.KeepOrig,
						hashVal:  hashval,
					}
					vellumIndex++
				}
			}
		} else {
			for _, rhs := range synonym.RHS {
				hashval = hash(rhs)
				byteSliceHashMap[hashval] = rhs
				byteSliceHash = append(byteSliceHash, hashval)
			}
			for _, lhs := range synonym.LHS {
				hashval = hash(lhs)
				byteSliceHashMap[hashval] = lhs
				Original, exists := vellumMap[hashval]
				if exists {
					for _, syn := range byteSliceHash {
						Original.synonyms.Add(syn)
					}
					if !Original.keepOrig {
						Original.keepOrig = synonym.KeepOrig
					}
					vellumMap[hashval] = Original
				} else {
					newSet := orderedset.NewOrderedSet()
					for _, syn := range byteSliceHash {
						newSet.Add(syn)
					}
					vellumMap[hashval] = vellumStruct{
						synonyms: *newSet,
						keepOrig: synonym.KeepOrig,
						hashVal:  hashval,
					}
					vellumIndex++
				}
			}
		}
	}
	return vellumMap
}

func checkForMatch(tokenPos int, input analysis.TokenStream, consumedTokens *[]*analysis.Token, keepOrig *bool, fst *vellum.FST, vellumStructMap map[uint64]vellumStruct, synTokenEnd *int) (int, *[]uint64, int) {
	var matchedSynonyms *[]uint64
	var maxMatchedTokenPos = tokenPos + 1
	var output uint64 = 0
	var tmp uint64
	var isMatch bool
	var consumedTokenLen = 0
	curr := fst.Start()
matchFailed:
	for tokenPos != len(input) {
		for _, character := range input[tokenPos].Term {
			curr, tmp = fst.AcceptWithVal(curr, character)
			if curr == 1 {
				break matchFailed
			}
			output += tmp
		}
		*consumedTokens = append(*consumedTokens, input[tokenPos])
		tokenPos++
		isMatch, tmp = fst.IsMatchWithVal(curr)
		if isMatch {
			*synTokenEnd = input[tokenPos-1].End
			vellStruct := vellumStructMap[output+tmp]
			var hashedSynonyms []uint64
			for _, i := range vellStruct.synonyms.Values() {
				hashedSynonyms = append(hashedSynonyms, i.(uint64))
			}
			matchedSynonyms = &hashedSynonyms
			*keepOrig = vellStruct.keepOrig
			maxMatchedTokenPos = tokenPos
			consumedTokenLen = len(*consumedTokens)
		}
		curr, tmp = fst.AcceptWithVal(curr, ' ')
		output += tmp
	}
	return maxMatchedTokenPos, matchedSynonyms, consumedTokenLen
}

func getAdjListOfSynonymGraph(matchedSynonymPos *[]uint64, consumedTokens []*analysis.Token, startNode int, byteSliceHashMap map[uint64][]byte, consumedTokenLen int) (map[int][]synonymGraphNode, int) {
	var numberOfNodes = 0
	if consumedTokens != nil {
		numberOfNodes += consumedTokenLen - 1
	}
	var synonymTokensContainer [][][]byte
	var synonymTokens [][]byte
	var tmpStr []byte
	for _, hashval := range *matchedSynonymPos {
		for _, character := range byteSliceHashMap[hashval] {
			if character == ' ' {
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
			token: consumedTokens[0].Term,
			node:  pathEndNode,
			start: consumedTokens[0].Start,
			end:   consumedTokens[0].End,
			orig:  true,
		})
		if consumedTokenLen > 1 {
			for i := 1; i < consumedTokenLen-1; i++ {
				adjList[pathEndNode] = append(adjList[pathEndNode], synonymGraphNode{
					token: consumedTokens[i].Term,
					node:  pathEndNode + 1,
					orig:  true,
					start: consumedTokens[i].Start,
					end:   consumedTokens[i].End,
				})
				pathEndNode++
			}
			adjList[pathEndNode] = append(adjList[pathEndNode], synonymGraphNode{
				token: consumedTokens[consumedTokenLen-1].Term,
				node:  endNode,
				orig:  true,
				start: consumedTokens[consumedTokenLen-1].Start,
				end:   consumedTokens[consumedTokenLen-1].End,
			})
		}
	}
	return adjList, endNode
}

func getOutputTokensFromGraph(adjList map[int][]synonymGraphNode, startNode int, endNode int, OutputTokenStream *analysis.TokenStream, SynTokenStart int, SynTokenEnd int) {
	for node, neighList := range adjList {
		for _, neighbor := range neighList {
			startForToken := SynTokenStart
			endForToken := SynTokenEnd
			typeForToken := analysis.Synonym
			if neighbor.orig {
				typeForToken = 0
				startForToken = neighbor.start
				endForToken = neighbor.end
			}
			positionForToken := node
			posLenForToken := neighbor.node - node
			OutputToken := analysis.Token{
				Term:      neighbor.token,
				Start:     startForToken,
				End:       endForToken,
				Type:      typeForToken,
				Position:  positionForToken,
				PosLength: posLenForToken,
			}
			*OutputTokenStream = append(*OutputTokenStream, &OutputToken)
		}
	}
}

func buildVellumFST(s *SynonymFilter, byteSliceHashMap map[uint64][]byte, vellumMap map[uint64]vellumStruct) (*vellum.FST, error) {
	type kv struct {
		Key   uint64
		Value []byte
	}
	var kvList []kv
	for k := range vellumMap {
		kvList = append(kvList, kv{k, byteSliceHashMap[k]})
	}
	sort.Slice(kvList, func(i, j int) bool {
		return (bytes.Compare(kvList[i].Value, kvList[j].Value) == -1)
	})
	var buf bytes.Buffer
	builder, err := vellum.New(&buf, nil)
	if err != nil {
		return nil, err
	}
	for _, kv := range kvList {
		err = builder.Insert(kv.Value, vellumMap[kv.Key].hashVal)
		if err != nil {
			return nil, err
		}
	}
	err = builder.Close()
	if err != nil {
		return nil, err
	}

	fst, err := vellum.Load(buf.Bytes())
	if err != nil {
		return nil, err
	}
	return fst, nil
}

func (s *SynonymFilter) Filter(input analysis.TokenStream) analysis.TokenStream {
	var matchedSynonyms *[]uint64
	var outputTokenStream analysis.TokenStream
	var consumedTokens []*analysis.Token
	var keepOrig = false
	var startNode = 1
	var endNode int
	var tokenPos = 0
	var adjList map[int][]synonymGraphNode
	var synTokenStart int
	var synTokenEnd = 0
	var consumedTokenLen int
	var byteSliceHashMap = make(map[uint64][]byte)
	vellumStructMap := cleanSynonymMap(s.synonymMap, byteSliceHashMap)
	fst, err := buildVellumFST(s, byteSliceHashMap, vellumStructMap)
	if err != nil {
		log.Fatal(err)
	}
	for tokenPos < len(input) {
		consumedTokens = nil
		synTokenStart = input[tokenPos].Start
		synTokenEnd = 0
		tokenPos, matchedSynonyms, consumedTokenLen = checkForMatch(tokenPos, input, &consumedTokens, &keepOrig, fst, vellumStructMap, &synTokenEnd)
		if matchedSynonyms != nil {
			if !keepOrig {
				adjList, endNode = getAdjListOfSynonymGraph(matchedSynonyms, nil, startNode, byteSliceHashMap, consumedTokenLen)
			} else {
				adjList, endNode = getAdjListOfSynonymGraph(matchedSynonyms, consumedTokens, startNode, byteSliceHashMap, consumedTokenLen)
			}
			getOutputTokensFromGraph(adjList, startNode, endNode, &outputTokenStream, synTokenStart, synTokenEnd)
			startNode = endNode
		} else {
			input[tokenPos-1].PosLength = 1
			input[tokenPos-1].Position = startNode
			outputTokenStream = append(outputTokenStream, input[tokenPos-1])
			startNode += 1
		}
	}
	return outputTokenStream
}

func newSynonymFilter(synonymMap *[]analysis.SynonymStruct) *SynonymFilter {
	return &SynonymFilter{
		synonymMap: synonymMap,
	}
}

func SynonymFilterConstructor(config map[string]interface{}, cache *registry.Cache) (analysis.TokenFilter, error) {
	synonymMap, ok := config["synonymMap"].(*[]analysis.SynonymStruct)
	if !ok {
		return nil, fmt.Errorf("synonym definitions not specified")
	}
	return newSynonymFilter(synonymMap), nil
}

func init() {
	registry.RegisterTokenFilter(Name, SynonymFilterConstructor)
}
