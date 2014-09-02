//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package lower_case_filter

import (
	"reflect"
	"testing"

	"github.com/blevesearch/bleve/analysis"
)

func TestLowerCaseFilter(t *testing.T) {

	inputTokenStream := analysis.TokenStream{
		&analysis.Token{
			Term: []byte("ONE"),
		},
		&analysis.Token{
			Term: []byte("two"),
		},
		&analysis.Token{
			Term: []byte("ThReE"),
		},
		&analysis.Token{
			Term: []byte("steven's"),
		},
	}

	expectedTokenStream := analysis.TokenStream{
		&analysis.Token{
			Term: []byte("one"),
		},
		&analysis.Token{
			Term: []byte("two"),
		},
		&analysis.Token{
			Term: []byte("three"),
		},
		&analysis.Token{
			Term: []byte("steven's"),
		},
	}

	filter := NewLowerCaseFilter()
	ouputTokenStream := filter.Filter(inputTokenStream)
	if !reflect.DeepEqual(ouputTokenStream, expectedTokenStream) {
		t.Errorf("expected %#v got %#v", expectedTokenStream, ouputTokenStream)
	}
}
