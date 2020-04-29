//  Copyright (c) 2020 Couchbase, Inc.
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

package collector

import (
	"container/heap"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"log"

	"github.com/buger/jsonparser"
	"github.com/couchbase/rhmap/store"

	"github.com/blevesearch/bleve/search"
)

type storeSpillOverHeap struct {
	h    *store.Heap
	path string
	dw   *docWrapper
}

// defaultChunkSize for the heap chunks.
var defaultChunkSize = int(10000)

// docWrapper struct is used for helping with the
// json parsing since the original documentMatch struct
// has a few fields hidden from json parsing.
type docWrapper struct {
	Doc                *search.DocumentMatch      `json:documentMatch`
	InternalID         []byte                     `json:internalID`
	HitNumber          uint64                     `json:hitnumber`
	FieldTermLocations []search.FieldTermLocation `json:"fieldTermLocations,omitempty"`
}

var ran uint32
var randmu sync.Mutex

func reseed() uint32 {
	return uint32(time.Now().UnixNano() + int64(os.Getpid()))
}

func nextRandom() string {
	randmu.Lock()
	r := ran
	if r == 0 {
		r = reseed()
	}
	r = r*1664525 + 1013904223 // constants from Numerical Recipes
	ran = r
	randmu.Unlock()
	return strconv.Itoa(int(1e9 + r%1e9))[1:]
}

func newStoreSpillOverHeap(capacity int, cachedScoring, cachedDesc []bool,
	sort search.SortOrder) *storeSpillOverHeap {
	dir := filepath.Join("", "spillover"+nextRandom())
	rv := &storeSpillOverHeap{
		h: &store.Heap{
			LessFunc: func(a, b []byte) bool {
				// TODO better handling?
				c := 0
				for x := range sort {
					c = 0
					if cachedScoring[x] {
						aScore, err := jsonparser.GetFloat(a, "Doc", "score")
						if err != nil {
							log.Printf("jsonparser.GetFloat, err: %v", err)
						}
						bScore, err := jsonparser.GetFloat(b, "Doc", "score")
						if err != nil {
							log.Printf("jsonparser.GetFloat, err: %v", err)
						}

						if aScore < bScore {
							c = -1
						} else if aScore > bScore {
							c = 1
						}
					} else {
						pos := fmt.Sprintf("[%d]", x)
						aVal, err := jsonparser.GetString(a, "Doc", "sort", pos)
						if err != nil {
							log.Printf("jsonparser.GetString, err: %v", err)
						}
						bVal, err := jsonparser.GetString(b, "Doc", "sort", pos)
						if err != nil {
							log.Printf("jsonparser.GetString, err: %v", err)
						}
						c = strings.Compare(aVal, bVal)
					}

					if c == 0 {
						continue
					}
					if cachedDesc[x] {
						c = -c
					}
					return -c < 0
				}
				// else compare the hit numbers
				ahitNumber, _ := jsonparser.GetInt(a, "HitNumber")
				bhitNumber, _ := jsonparser.GetInt(b, "HitNumber")
				if ahitNumber == bhitNumber {
					c = 0
				} else if ahitNumber > bhitNumber {
					c = 1
				}
				return -c < 0
			},
			Heap: &store.Chunks{
				PathPrefix:     dir,
				FileSuffix:     ".heap",
				ChunkSizeBytes: defaultChunkSize,
			},
			Data: &store.Chunks{
				PathPrefix:     dir,
				FileSuffix:     ".data",
				ChunkSizeBytes: defaultChunkSize,
			},
		},
	}

	rv.path = dir
	return rv
}

func (c *storeSpillOverHeap) Close() error {
	return c.h.Close()
}

func (c *storeSpillOverHeap) Pop() (*search.DocumentMatch, error) {
	docBytes := heap.Pop(c.h).([]byte)
	var doc *search.DocumentMatch
	*c.dw = docWrapper{}
	err := json.Unmarshal(docBytes, &c.dw)
	if err != nil {
		// TODO better handling?
		log.Printf("pop: json unmarshal err: %v, docBytes: %q", err, docBytes)
		return nil, err
	}
	doc = c.dw.Doc
	doc.IndexInternalID = c.dw.InternalID
	doc.HitNumber = c.dw.HitNumber
	doc.FieldTermLocations = c.dw.FieldTermLocations
	return doc, nil
}

func (c *storeSpillOverHeap) docBytes(doc *search.DocumentMatch) []byte {
	c.dw = &docWrapper{Doc: doc,
		InternalID:         doc.IndexInternalID,
		HitNumber:          doc.HitNumber,
		FieldTermLocations: doc.FieldTermLocations,
	}

	docBytes, err := json.Marshal(c.dw)
	if err != nil {
		log.Printf("docBytes: json marshall, err: %v", err)
		return nil
	}

	return docBytes
}

func (c *storeSpillOverHeap) AddNotExceedingSize(doc *search.DocumentMatch,
	size int) *search.DocumentMatch {
	heap.Push(c.h, c.docBytes(doc))

	if c.h.Len() > size {
		doc, _ := c.Pop()
		return doc
	}
	return nil
}

func (c *storeSpillOverHeap) Final(skip int, fixup collectorFixup) (search.DocumentMatchCollection, error) {
	count := c.h.Len()
	size := count - skip
	if size <= 0 {
		return make(search.DocumentMatchCollection, 0), nil
	}

	rv := make(search.DocumentMatchCollection, size)
	for i := size - 1; i >= 0; i-- {
		doc, _ := c.Pop()
		rv[i] = doc
		err := fixup(doc)
		if err != nil {
			return nil, err
		}
	}
	return rv, nil
}
