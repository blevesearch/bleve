//  Copyright (c) 2015 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package firestorm

import (
	"reflect"
	"testing"

	"github.com/blevesearch/bleve/index"
)

func TestTermFreqRows(t *testing.T) {
	tests := []struct {
		input  index.IndexRow
		outKey []byte
		outVal []byte
	}{
		{
			NewTermFreqRow(0, []byte("test"), []byte("doca"), 1, 3, 7.0, nil),
			[]byte{TermFreqKeyPrefix[0], 0, 0, 't', 'e', 's', 't', ByteSeparator, 'd', 'o', 'c', 'a', ByteSeparator, 1},
			[]byte{8, 3, 21, 0, 0, 224, 64},
		},
		{
			NewTermFreqRow(2, []byte("cats"), []byte("docb"), 254, 3, 7.0, nil),
			[]byte{TermFreqKeyPrefix[0], 2, 0, 'c', 'a', 't', 's', ByteSeparator, 'd', 'o', 'c', 'b', ByteSeparator, 254, 1},
			[]byte{8, 3, 21, 0, 0, 224, 64},
		},
		{
			NewTermFreqRow(2, []byte("cats"), []byte("docb"), 254, 7, 3.0, nil),
			[]byte{TermFreqKeyPrefix[0], 2, 0, 'c', 'a', 't', 's', ByteSeparator, 'd', 'o', 'c', 'b', ByteSeparator, 254, 1},
			[]byte{8, 7, 21, 0, 0, 64, 64},
		},
		{
			NewTermFreqRow(2, []byte("cats"), []byte("docb"), 254, 7, 3.0, []*TermVector{NewTermVector(2, 1, 0, 5, nil)}),
			[]byte{TermFreqKeyPrefix[0], 2, 0, 'c', 'a', 't', 's', ByteSeparator, 'd', 'o', 'c', 'b', ByteSeparator, 254, 1},
			[]byte{8, 7, 21, 0, 0, 64, 64, 26, 8, 8, 2, 16, 1, 24, 0, 32, 5},
		},
		{
			NewTermFreqRow(2, []byte("cats"), []byte("docb"), 254, 7, 3.0, []*TermVector{NewTermVector(2, 1, 0, 5, []uint64{0})}),
			[]byte{TermFreqKeyPrefix[0], 2, 0, 'c', 'a', 't', 's', ByteSeparator, 'd', 'o', 'c', 'b', ByteSeparator, 254, 1},
			[]byte{8, 7, 21, 0, 0, 64, 64, 26, 10, 8, 2, 16, 1, 24, 0, 32, 5, 40, 0},
		},
		{
			NewTermFreqRow(2, []byte("cats"), []byte("docb"), 254, 7, 3.0, []*TermVector{NewTermVector(2, 1, 0, 5, []uint64{0, 1, 2})}),
			[]byte{TermFreqKeyPrefix[0], 2, 0, 'c', 'a', 't', 's', ByteSeparator, 'd', 'o', 'c', 'b', ByteSeparator, 254, 1},
			[]byte{8, 7, 21, 0, 0, 64, 64, 26, 14, 8, 2, 16, 1, 24, 0, 32, 5, 40, 0, 40, 1, 40, 2},
		},
		// test empty term, used by _id field
		{
			NewTermFreqRow(0, []byte{}, []byte("doca"), 1, 0, 0.0, nil),
			[]byte{TermFreqKeyPrefix[0], 0, 0, ByteSeparator, 'd', 'o', 'c', 'a', ByteSeparator, 1},
			[]byte{8, 0, 21, 0, 0, 0, 0},
		},
	}

	// test going from struct to k/v bytes
	for i, test := range tests {
		rk := test.input.Key()
		if !reflect.DeepEqual(rk, test.outKey) {
			t.Errorf("Expected key to be %v got: %v", test.outKey, rk)
		}
		rv := test.input.Value()
		if !reflect.DeepEqual(rv, test.outVal) {
			t.Errorf("Expected value to be %v got: %v for %d", test.outVal, rv, i)
		}
	}

	// now test going back from k/v bytes to struct
	for i, test := range tests {
		row, err := NewTermFreqRowKV(test.outKey, test.outVal)
		if err != nil {
			t.Errorf("error parsking key/value: %v", err)
		}
		if !reflect.DeepEqual(row, test.input) {
			t.Errorf("Expected:\n%vgot:\n%vfor %d", test.input, row, i)
		}
	}
}
