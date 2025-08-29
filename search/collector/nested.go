//  Copyright (c) 2025 Couchbase, Inc.
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
	"github.com/blevesearch/bleve/v2/search"
	index "github.com/blevesearch/bleve_index_api"
)

type collectStoreNested struct {
	nr index.NestedReader

	backingStore collectorStore

	interim map[uint64]*search.DocumentMatch

	final []*search.DocumentMatch
}

func newStoreNested(store collectorStore) *collectStoreNested {
	rv := &collectStoreNested{
		backingStore: store,
		interim:      make(map[uint64]*search.DocumentMatch),
	}
	return rv
}

func (c *collectStoreNested) AddNotExceedingSize(doc *search.DocumentMatch, size int) *search.DocumentMatch {
	// first get the root document id
	ancestors, err := c.nr.Ancestors(doc.IndexInternalID)
	if err != nil || len(ancestors) == 0 {
		// should not happen, but if it does, just ignore this doc
		return nil
	}
	// root docID is the last ancestor
	rootID := ancestors[len(ancestors)-1]
	// see if we already have an interim entry for this rootID
	rootIDVal, err := rootID.Value()
	if err != nil {
		// should not happen, but if it does, just ignore this doc
		return nil
	}
	rootDocMatch, ok := c.interim[rootIDVal]
	if !ok {
		// create a new interim doc match for this rootID, with the incoming
		// doc as the first child
		rootDocMatch = &search.DocumentMatch{
			IndexInternalID: rootID,
			Children:        []*search.DocumentMatch{doc},
		}
		c.interim[rootIDVal] = rootDocMatch
	} else {
		// add this incoming doc as another child of the existing interim doc match
		rootDocMatch.Children = append(rootDocMatch.Children, doc)
	}
	// return nil, we don't eject anything as we cannot yet score until we have all the children
	return nil
}

func (c *collectStoreNested) processInterim() {
	// for each interim doc, score it and add to final
	for _, doc := range c.interim {
		// now for each interim doc, we need to call all the children recursively
		// and merge them into the root document match
		doc.MergeChildren()

	}
}

func (c *collectStoreNested) Final(skip int, fixup collectorFixup) (search.DocumentMatchCollection, error) {
	return nil, nil

}

func (c *collectStoreNested) Internal() search.DocumentMatchCollection {
	return nil

}
