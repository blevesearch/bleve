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
	"fmt"

	"github.com/blevesearch/bleve/v2/search"
	index "github.com/blevesearch/bleve_index_api"
)

type collectStoreNested struct {
	nr index.NestedReader

	interim map[uint64]*search.DocumentMatch
}

func newStoreNested(nr index.NestedReader) *collectStoreNested {
	rv := &collectStoreNested{
		interim: make(map[uint64]*search.DocumentMatch),
		nr:      nr,
	}
	return rv
}
func (c *collectStoreNested) AddDocument(doc *search.DocumentMatch) (*search.DocumentMatch, error) {
	// find ancestors for the doc
	ancestors, err := c.nr.Ancestors(doc.IndexInternalID)
	if err != nil || len(ancestors) == 0 {
		return nil, fmt.Errorf("error getting ancestors for doc %v: %v", doc.IndexInternalID, err)
	}
	// root docID is the last ancestor
	rootID := ancestors[len(ancestors)-1]
	rootIDVal, err := rootID.Value()
	if err != nil {
		return nil, err
	}
	// lookup existing root
	rootDocument, ok := c.interim[rootIDVal]
	if !ok {
		// no interim root yet
		if len(ancestors) == 1 {
			// incoming doc is the root itself
			c.interim[rootIDVal] = doc
			return nil, nil
		}

		// create new interim root and merge child into it
		rootDocument = &search.DocumentMatch{IndexInternalID: rootID}
		if err := rootDocument.MergeWith(doc); err != nil {
			return nil, err
		}
		c.interim[rootIDVal] = rootDocument

		// return the child for recycling
		return doc, nil
	}

	// merge child into existing root
	if err := rootDocument.MergeWith(doc); err != nil {
		return nil, err
	}
	return doc, nil
}

// NestedDocumentVisitor is the callback invoked for each root document.
// root is the merged root DocumentMatch.
type NestedDocumentVisitor func(root *search.DocumentMatch) error

// VisitRoots walks over all collected interim values and calls the visitor.
func (c *collectStoreNested) VisitRoots(visitor NestedDocumentVisitor) error {
	for _, root := range c.interim {
		// invoke the visitor
		if err := visitor(root); err != nil {
			return err
		}
	}
	return nil
}
