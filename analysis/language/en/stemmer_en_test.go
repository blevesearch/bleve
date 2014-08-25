//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

// +build libstemmer full

package en

import (
	"reflect"
	"testing"

	"github.com/couchbaselabs/bleve/analysis"
	"github.com/couchbaselabs/bleve/registry"
)

func TestEnglishStemmer(t *testing.T) {
	tests := []struct {
		input  analysis.TokenStream
		output analysis.TokenStream
	}{
		{
			input: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("walking"),
				},
				&analysis.Token{
					Term: []byte("talked"),
				},
				&analysis.Token{
					Term: []byte("business"),
				},
				&analysis.Token{
					Term:    []byte("protected"),
					KeyWord: true,
				},
			},
			output: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("walk"),
				},
				&analysis.Token{
					Term: []byte("talk"),
				},
				&analysis.Token{
					Term: []byte("busi"),
				},
				&analysis.Token{
					Term:    []byte("protected"),
					KeyWord: true,
				},
			},
		},
	}

	cache := registry.NewCache()
	stemmerFilter, err := cache.TokenFilterNamed(StemmerName)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range tests {
		actual := stemmerFilter.Filter(test.input)
		if !reflect.DeepEqual(actual, test.output) {
			t.Errorf("expected %s, got %s", test.output, actual)
		}
	}
}
