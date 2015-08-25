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
	"bytes"
	"encoding/binary"
	"reflect"
	"testing"

	"github.com/blevesearch/bleve/index"
)

func TestPartialMerge(t *testing.T) {

	tests := []struct {
		in  [][]byte
		out uint64
	}{
		{
			in:  [][]byte{dictionaryTermIncr, dictionaryTermIncr, dictionaryTermIncr, dictionaryTermIncr, dictionaryTermIncr},
			out: 5,
		},
	}

	mo := &firestormMerge{}
	for _, test := range tests {
		curr := test.in[0]
		for _, next := range test.in[1:] {
			var ok bool
			curr, ok = mo.PartialMerge([]byte("key"), curr, next)
			if !ok {
				t.Errorf("expected partial merge ok")
			}
		}
		actual := decodeCount(curr)
		if actual != test.out {
			t.Errorf("expected %d, got %d", test.out, actual)
		}
	}

}

func decodeCount(in []byte) uint64 {
	buf := bytes.NewBuffer(in)
	count, _ := binary.ReadUvarint(buf)
	return count
}

func TestFullMerge(t *testing.T) {

	tests := []struct {
		existing index.IndexRow
		operands [][]byte
		result   index.IndexRow
		success  bool
	}{
		{
			existing: NewDictionaryRow(1, []byte("term"), 3),
			operands: [][]byte{dictionaryTermIncr, dictionaryTermIncr},
			result:   NewDictionaryRow(1, []byte("term"), 5),
			success:  true,
		},
		{
			existing: NewDictionaryRow(1, []byte("term"), 3),
			operands: [][]byte{dictionaryTermDecr, dictionaryTermDecr},
			result:   NewDictionaryRow(1, []byte("term"), 1),
			success:  true,
		},
	}

	mo := &firestormMerge{}
	for _, test := range tests {

		existingVal := test.existing.Value()
		actual, success := mo.FullMerge([]byte("key"), existingVal, test.operands)
		if success != test.success {
			t.Errorf("expected error %t, got %t", test.success, success)
		}
		expectedVal := test.result.Value()
		if !reflect.DeepEqual(expectedVal, actual) {
			t.Errorf("expected result %v, got %v", expectedVal, actual)
		}
	}

}
