//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

// +build icu full

package th

import (
	"reflect"
	"testing"

	"github.com/couchbaselabs/bleve/analysis"
	"github.com/couchbaselabs/bleve/registry"
)

// tried to adapt these from the lucene tests, most of which either
// use the empty stop dictionary or the english one.

func TestThaiAnalyzer(t *testing.T) {
	tests := []struct {
		input  []byte
		output analysis.TokenStream
	}{
		// stop words
		{
			input: []byte("การที่ได้ต้องแสดงว่างานดี"),
			output: analysis.TokenStream{
				&analysis.Token{
					Term:     []byte("แสดง"),
					Position: 5,
					Start:    39,
					End:      51,
				},
				&analysis.Token{
					Term:     []byte("งาน"),
					Position: 7,
					Start:    60,
					End:      69,
				},
				&analysis.Token{
					Term:     []byte("ดี"),
					Position: 8,
					Start:    69,
					End:      75,
				},
			},
		},
	}

	cache := registry.NewCache()
	analyzer, err := cache.AnalyzerNamed(AnalyzerName)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range tests {
		actual := analyzer.Analyze(test.input)
		if !reflect.DeepEqual(actual, test.output) {
			t.Errorf("expected %v, got %v", test.output, actual)
		}
	}
}

func TestThaiAnalyzerWihtoutOffsets(t *testing.T) {
	tests := []struct {
		input  []byte
		output analysis.TokenStream
	}{
		// stop words
		{
			input: []byte("บริษัทชื่อ XY&Z - คุยกับ xyz@demo.com"),
			output: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("บริษัท"),
				},
				&analysis.Token{
					Term: []byte("ชื่อ"),
				},
				&analysis.Token{
					Term: []byte("xy"),
				},
				&analysis.Token{
					Term: []byte("z"),
				},
				&analysis.Token{
					Term: []byte("คุย"),
				},
				&analysis.Token{
					Term: []byte("xyz"),
				},
				&analysis.Token{
					Term: []byte("demo.com"),
				},
			},
		},
	}

	cache := registry.NewCache()
	analyzer, err := cache.AnalyzerNamed(AnalyzerName)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range tests {
		actual := analyzer.Analyze(test.input)
		if len(actual) != len(test.output) {
			t.Errorf("expected length: %d, got %d", len(test.output), len(actual))
		}
		for i, tok := range actual {
			if !reflect.DeepEqual(tok.Term, test.output[i].Term) {
				t.Errorf("expected term %s (% x) got %s (% x)", test.output[i].Term, test.output[i].Term, tok.Term, tok.Term)
			}
		}
	}
}
