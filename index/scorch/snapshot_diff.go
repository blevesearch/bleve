//  Copyright (c) 2024 Couchbase, Inc.
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
	segment "github.com/blevesearch/scorch_segment_api/v2"
)

// classifyBatchIDs classifies each batch docID as inserted, updated, or
// deleted.  oldSnapshot is the previous snapshot's live docIDs.  newData is
// the new segment (nil means pure deletes).  Uses simple nested loops.
func classifyBatchIDs(ids []string, oldSnapshot *IndexSnapshot, newData segment.Segment) (
	inserted, updated, deleted []string,
) {
	for _, id := range ids {
		inNew := false
		if newData != nil {
			bm, err := newData.DocNumbers([]string{id})
			if err == nil && !bm.IsEmpty() {
				inNew = true
			}
		}
		inOld := false
		if oldSnapshot != nil {
			doc, err := oldSnapshot.Document(id)
			if err == nil && doc != nil {
				inOld = true
			}
		}

		switch {
		case inNew && inOld:
			updated = append(updated, id)
		case inNew && !inOld:
			inserted = append(inserted, id)
		case !inNew && inOld:
			deleted = append(deleted, id)
		}
	}
	return inserted, updated, deleted
}
