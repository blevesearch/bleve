//  Copyright (c) 2014 Couchbase, Inc.
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

package analysis

import (
	index "github.com/blevesearch/bleve_index_api"
	"reflect"
	"testing"
)

func TestTokenFrequency(t *testing.T) {
	tokens := TokenStream{
		&Token{
			Term:     []byte("water"),
			Position: 1,
			Start:    0,
			End:      5,
		},
		&Token{
			Term:     []byte("water"),
			Position: 2,
			Start:    6,
			End:      11,
		},
	}
	expectedResult := index.TokenFrequencies{
		"water": &index.TokenFreq{
			Term: []byte("water"),
			Locations: []*index.TokenLocation{
				{
					Position: 1,
					Start:    0,
					End:      5,
				},
				{
					Position: 2,
					Start:    6,
					End:      11,
				},
			},
		},
	}
	expectedResult["water"].SetFrequency(2)
	result := TokenFrequency(tokens, nil, index.IncludeTermVectors)
	if !reflect.DeepEqual(result, expectedResult) {
		t.Errorf("expected %#v, got %#v", expectedResult, result)
	}
}
