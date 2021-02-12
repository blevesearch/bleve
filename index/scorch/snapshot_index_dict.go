//  Copyright (c) 2017 Couchbase, Inc.
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

package scorch

import (
	"container/heap"

	index "github.com/blevesearch/bleve_index_api"
	segment "github.com/blevesearch/scorch_segment_api/v2"
)

type segmentDictCursor struct {
	dict segment.TermDictionary
	itr  segment.DictionaryIterator
	curr index.DictEntry
}

type IndexSnapshotFieldDict struct {
	snapshot *IndexSnapshot
	cursors  []*segmentDictCursor
	entry    index.DictEntry
}

func (i *IndexSnapshotFieldDict) Len() int { return len(i.cursors) }
func (i *IndexSnapshotFieldDict) Less(a, b int) bool {
	return i.cursors[a].curr.Term < i.cursors[b].curr.Term
}
func (i *IndexSnapshotFieldDict) Swap(a, b int) {
	i.cursors[a], i.cursors[b] = i.cursors[b], i.cursors[a]
}

func (i *IndexSnapshotFieldDict) Push(x interface{}) {
	i.cursors = append(i.cursors, x.(*segmentDictCursor))
}

func (i *IndexSnapshotFieldDict) Pop() interface{} {
	n := len(i.cursors)
	x := i.cursors[n-1]
	i.cursors = i.cursors[0 : n-1]
	return x
}

func (i *IndexSnapshotFieldDict) Next() (*index.DictEntry, error) {
	if len(i.cursors) == 0 {
		return nil, nil
	}
	i.entry = i.cursors[0].curr
	next, err := i.cursors[0].itr.Next()
	if err != nil {
		return nil, err
	}
	if next == nil {
		// at end of this cursor, remove it
		heap.Pop(i)
	} else {
		// modified heap, fix it
		i.cursors[0].curr = *next
		heap.Fix(i, 0)
	}
	// look for any other entries with the exact same term
	for len(i.cursors) > 0 && i.cursors[0].curr.Term == i.entry.Term {
		i.entry.Count += i.cursors[0].curr.Count
		next, err := i.cursors[0].itr.Next()
		if err != nil {
			return nil, err
		}
		if next == nil {
			// at end of this cursor, remove it
			heap.Pop(i)
		} else {
			// modified heap, fix it
			i.cursors[0].curr = *next
			heap.Fix(i, 0)
		}
	}

	return &i.entry, nil
}

func (i *IndexSnapshotFieldDict) Close() error {
	return nil
}

func (i *IndexSnapshotFieldDict) Contains(key []byte) (bool, error) {
	if len(i.cursors) == 0 {
		return false, nil
	}

	for _, cursor := range i.cursors {
		if found, _ := cursor.dict.Contains(key); found {
			return true, nil
		}
	}

	return false, nil
}
