package synonym

import (
	"bytes"
	"encoding/json"
	"hash/fnv"
	"sort"

	"github.com/blevesearch/bleve/v2/analysis"
	"github.com/blevesearch/vellum"
)

type SynonymConfig struct {
	FST              []byte
	VellumMap        map[uint64][]uint64
	ByteSliceHashMap map[uint64][]byte
}

type SynonymStruct struct {
	Input       []json.RawMessage
	Synonyms    []json.RawMessage
	MappingType json.RawMessage
	KeepOrig    bool
}

func stripQuotes(word []byte) []byte {
	return word[1 : len(word)-1]
}

func stripJsonQuotes(synonym *SynonymStruct) {
	synonym.MappingType = stripQuotes(synonym.MappingType)
	for index, i := range synonym.Input {
		synonym.Input[index] = stripQuotes(i)
	}
	for index, i := range synonym.Synonyms {
		synonym.Synonyms[index] = stripQuotes(i)
	}
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

func CleanSynonymMap(synonymMap []SynonymStruct) (map[uint64][]uint64, map[uint64][]byte) {
	var vellumMap = make(map[uint64][]uint64)
	var hashSet = make(map[uint64]map[uint64]interface{})
	var byteSliceHashMap = make(map[uint64][]byte)
	var hashval uint64
	var byteSliceHash []uint64
	for _, synonym := range synonymMap {
		stripJsonQuotes(&synonym)
		byteSliceHash = nil
		if bytes.Equal(synonym.MappingType, equivalentSynonymType) {
			for _, rhs := range synonym.Synonyms {
				hashval = hash(rhs)
				byteSliceHashMap[hashval] = rhs
				byteSliceHash = append(byteSliceHash, hashval)
			}
			for i, hashval := range byteSliceHash {
				rhsCopy := make([]uint64, len(byteSliceHash))
				copy(rhsCopy, byteSliceHash)
				rhsCopy = remove(rhsCopy, i)
				Original, exists := hashSet[hashval]
				if exists {
					for _, syn := range rhsCopy {
						Original[syn] = struct{}{}
					}
					if vellumMap[hashval][0] == 0 && synonym.KeepOrig {
						vellumMap[hashval][0] = 1
					}
				} else {
					newSet := make(map[uint64]interface{})
					for _, syn := range rhsCopy {
						newSet[syn] = struct{}{}
					}
					hashSet[hashval] = newSet
					vellumMap[hashval] = []uint64{0}
					if synonym.KeepOrig {
						vellumMap[hashval][0] = 1
					}
				}
			}
		} else if bytes.Equal(synonym.MappingType, explicitSynonymType) {
			for _, rhs := range synonym.Synonyms {
				hashval = hash(rhs)
				byteSliceHashMap[hashval] = rhs
				byteSliceHash = append(byteSliceHash, hashval)
			}
			for _, lhs := range synonym.Input {
				hashval = hash(lhs)
				byteSliceHashMap[hashval] = lhs
				Original, exists := hashSet[hashval]
				if exists {
					for _, syn := range byteSliceHash {
						Original[syn] = struct{}{}
					}
					if vellumMap[hashval][0] == 0 && synonym.KeepOrig {
						vellumMap[hashval][0] = 1
					}
				} else {
					newSet := make(map[uint64]interface{})
					for _, syn := range byteSliceHash {
						newSet[syn] = struct{}{}
					}
					hashSet[hashval] = newSet
					vellumMap[hashval] = []uint64{0}
					if synonym.KeepOrig {
						vellumMap[hashval][0] = 1
					}
				}
			}
		}
	}
	for key, set := range hashSet {
		tmpArray := make([]uint64, len(set)+1)
		tmpArray[0] = vellumMap[key][0]
		index := 1
		for k := range set {
			tmpArray[index] = k
			index++
		}
		vellumMap[key] = tmpArray
	}
	return vellumMap, byteSliceHashMap
}

func BuildSynonymFST(byteSliceHashMap map[uint64][]byte, vellumMap map[uint64][]uint64) (*bytes.Buffer, error) {
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
		err = builder.Insert(kv.Value, kv.Key)
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

func SynonymTokenStreamToGraph(tokens analysis.TokenStream) [][]*analysis.Token {
	graphNodes := make([][]*analysis.Token, len(tokens)+2)
	for _, tok := range tokens {
		graphNodes[tok.CurrentNode] = append(graphNodes[tok.CurrentNode], tok)
	}

	return graphNodes
}
