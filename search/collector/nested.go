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

	// the current root document match being built
	currRoot *search.DocumentMatch
}

func newStoreNested(nr index.NestedReader) *collectStoreNested {
	rv := &collectStoreNested{
		nr: nr,
	}
	return rv
}

// ProcessNestedDocument adds a document to the nested store, merging it into its root document
// as needed. If the returned DocumentMatch is nil, the incoming doc has been merged
// into its parent and should not be processed further. If the returned DocumentMatch
// is non-nil, it represents a complete root document that should be processed further.
// NOTE: This implementation assumes that documents are added in increasing order of their internal IDs
// which is guaranteed by all searchers in bleve.
func (c *collectStoreNested) ProcessNestedDocument(ctx *search.SearchContext, doc *search.DocumentMatch) (*search.DocumentMatch, error) {
	// find ancestors for the doc
	ancestors, err := c.nr.Ancestors(doc.IndexInternalID)
	if err != nil {
		return nil, err
	}
	if len(ancestors) == 0 {
		// should not happen, every doc should have at least itself as ancestor
		return nil, nil
	}
	// root docID is the last ancestor
	rootID := ancestors[len(ancestors)-1]
	// check if there is an interim root already and if the incoming doc belongs to it
	if c.currRoot != nil && c.currRoot.IndexInternalID.Equals(rootID) {
		// there is an interim root already, and the incoming doc belongs to it
		if err := c.currRoot.MergeWith(doc); err != nil {
			return nil, err
		}
		// recycle the child document now that it's merged into the interim root
		ctx.DocumentMatchPool.Put(doc)
		return nil, nil
	}
	// completedRoot is the root document match to return, if any
	var completedRoot *search.DocumentMatch
	if c.currRoot != nil {
		// we have an existing interim root, return it for processing
		completedRoot = c.currRoot
		// clear current root
		c.currRoot = nil
	}
	// no interim root for now so either we have a root document incoming
	// or we have a child doc and need to create an interim root
	if len(ancestors) == 1 {
		// incoming doc is the root itself
		c.currRoot = doc
		return completedRoot, nil
	}
	// this is a child doc, create interim root
	c.currRoot = &search.DocumentMatch{IndexInternalID: rootID}
	if err := c.currRoot.MergeWith(doc); err != nil {
		return nil, err
	}
	// recycle the child document now that it's merged into the interim root
	ctx.DocumentMatchPool.Put(doc)
	return completedRoot, nil
}

// CurrentRoot returns the current interim root document match being built, if any
func (c *collectStoreNested) CurrentRoot() *search.DocumentMatch {
	return c.currRoot
}
