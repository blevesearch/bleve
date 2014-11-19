//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package bleve

import (
	"strings"
	"testing"

	"github.com/blevesearch/bleve/search"
)

func TestSearchResultString(t *testing.T) {

	searchResult := &SearchResult{
		Request: &SearchRequest{
			Size: 0,
		},
		Total: 5,
		Hits:  search.DocumentMatchCollection{},
	}

	srstring := searchResult.String()
	if !strings.HasPrefix(srstring, "5 matches") {
		t.Errorf("expected prefix '5 matches', got %s", srstring)
	}
}
