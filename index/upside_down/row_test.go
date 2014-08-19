//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
package upside_down

import (
	"reflect"
	"testing"

	"code.google.com/p/goprotobuf/proto"
)

func TestRows(t *testing.T) {
	tests := []struct {
		input  UpsideDownCouchRow
		outKey []byte
		outVal []byte
	}{
		{
			NewVersionRow(1),
			[]byte{'v'},
			[]byte{0x1},
		},
		{
			NewFieldRow(0, "name"),
			[]byte{'f', 0, 0},
			[]byte{'n', 'a', 'm', 'e', BYTE_SEPARATOR},
		},
		{
			NewFieldRow(1, "desc"),
			[]byte{'f', 1, 0},
			[]byte{'d', 'e', 's', 'c', BYTE_SEPARATOR},
		},
		{
			NewFieldRow(513, "style"),
			[]byte{'f', 1, 2},
			[]byte{'s', 't', 'y', 'l', 'e', BYTE_SEPARATOR},
		},
		{
			NewTermFrequencyRow([]byte{'b', 'e', 'e', 'r'}, 0, "", 3, 3.14),
			[]byte{'t', 0, 0, 'b', 'e', 'e', 'r', BYTE_SEPARATOR},
			[]byte{3, 0, 0, 0, 0, 0, 0, 0, 195, 245, 72, 64},
		},
		{
			NewTermFrequencyRow([]byte{'b', 'e', 'e', 'r'}, 0, "budweiser", 3, 3.14),
			[]byte{'t', 0, 0, 'b', 'e', 'e', 'r', BYTE_SEPARATOR, 'b', 'u', 'd', 'w', 'e', 'i', 's', 'e', 'r'},
			[]byte{3, 0, 0, 0, 0, 0, 0, 0, 195, 245, 72, 64},
		},
		{
			NewTermFrequencyRowWithTermVectors([]byte{'b', 'e', 'e', 'r'}, 0, "budweiser", 3, 3.14, []*TermVector{&TermVector{field: 0, pos: 1, start: 3, end: 11}, &TermVector{field: 0, pos: 2, start: 23, end: 31}, &TermVector{field: 0, pos: 3, start: 43, end: 51}}),
			[]byte{'t', 0, 0, 'b', 'e', 'e', 'r', BYTE_SEPARATOR, 'b', 'u', 'd', 'w', 'e', 'i', 's', 'e', 'r'},
			[]byte{3, 0, 0, 0, 0, 0, 0, 0, 195, 245, 72, 64, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 11, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 23, 0, 0, 0, 0, 0, 0, 0, 31, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 43, 0, 0, 0, 0, 0, 0, 0, 51, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			NewBackIndexRow("budweiser", []*BackIndexTermEntry{&BackIndexTermEntry{Term: proto.String("beer"), Field: proto.Uint32(0)}}, nil),
			[]byte{'b', 'b', 'u', 'd', 'w', 'e', 'i', 's', 'e', 'r'},
			[]byte{10, 8, 10, 4, 'b', 'e', 'e', 'r', 16, 0},
		},
		{
			NewBackIndexRow("budweiser", []*BackIndexTermEntry{&BackIndexTermEntry{Term: proto.String("beer"), Field: proto.Uint32(0)}, &BackIndexTermEntry{Term: proto.String("beat"), Field: proto.Uint32(1)}}, nil),
			[]byte{'b', 'b', 'u', 'd', 'w', 'e', 'i', 's', 'e', 'r'},
			[]byte{10, 8, 10, 4, 'b', 'e', 'e', 'r', 16, 0, 10, 8, 10, 4, 'b', 'e', 'a', 't', 16, 1},
		},
		{
			NewBackIndexRow("budweiser", []*BackIndexTermEntry{&BackIndexTermEntry{Term: proto.String("beer"), Field: proto.Uint32(0)}, &BackIndexTermEntry{Term: proto.String("beat"), Field: proto.Uint32(1)}}, []*BackIndexStoreEntry{&BackIndexStoreEntry{Field: proto.Uint32(3)}, &BackIndexStoreEntry{Field: proto.Uint32(4)}, &BackIndexStoreEntry{Field: proto.Uint32(5)}}),
			[]byte{'b', 'b', 'u', 'd', 'w', 'e', 'i', 's', 'e', 'r'},
			[]byte{10, 8, 10, 4, 'b', 'e', 'e', 'r', 16, 0, 10, 8, 10, 4, 'b', 'e', 'a', 't', 16, 1, 18, 2, 8, 3, 18, 2, 8, 4, 18, 2, 8, 5},
		},
		{
			NewStoredRow("budweiser", 0, []uint64{}, byte('t'), []byte("an american beer")),
			[]byte{'s', 'b', 'u', 'd', 'w', 'e', 'i', 's', 'e', 'r', BYTE_SEPARATOR, 0, 0},
			[]byte{'t', 'a', 'n', ' ', 'a', 'm', 'e', 'r', 'i', 'c', 'a', 'n', ' ', 'b', 'e', 'e', 'r'},
		},
		{
			NewInternalRow([]byte("mapping"), []byte(`{"mapping":"json content"}`)),
			[]byte{'i', 'm', 'a', 'p', 'p', 'i', 'n', 'g'},
			[]byte{'{', '"', 'm', 'a', 'p', 'p', 'i', 'n', 'g', '"', ':', '"', 'j', 's', 'o', 'n', ' ', 'c', 'o', 'n', 't', 'e', 'n', 't', '"', '}'},
		},
	}

	// test going from struct to k/v bytes
	for _, test := range tests {
		rk := test.input.Key()
		if !reflect.DeepEqual(rk, test.outKey) {
			t.Errorf("Expected key to be %v got: %v", test.outKey, rk)
		}
		rv := test.input.Value()
		if !reflect.DeepEqual(rv, test.outVal) {
			t.Errorf("Expected value to be %v got: %v", test.outVal, rv)
		}
	}

	// now test going back from k/v bytes to struct
	for i, test := range tests {
		row, err := ParseFromKeyValue(test.outKey, test.outVal)
		if err != nil {
			t.Error(err)
		}
		if !reflect.DeepEqual(row, test.input) {
			t.Errorf("Expected: %#v got: %#v for %d", test.input, row, i)
		}
	}

}

func TestInvalidRows(t *testing.T) {
	tests := []struct {
		key []byte
		val []byte
	}{
		// empty key
		{
			[]byte{},
			[]byte{},
		},
		// no such type q
		{
			[]byte{'q'},
			[]byte{},
		},
		// type v, invalid empty value
		{
			[]byte{'v'},
			[]byte{},
		},
		// type f, invalid key
		{
			[]byte{'f'},
			[]byte{},
		},
		// type f, valid key, invalid value
		{
			[]byte{'f', 0, 0},
			[]byte{},
		},
		// type t, invalid key (missing field)
		{
			[]byte{'t'},
			[]byte{},
		},
		// type t, invalid key (missing term)
		{
			[]byte{'t', 0, 0},
			[]byte{},
		},
		// type t, invalid key (missing id)
		{
			[]byte{'t', 0, 0, 'b', 'e', 'e', 'r', BYTE_SEPARATOR},
			[]byte{},
		},
		// type t, invalid val (misisng freq)
		{
			[]byte{'t', 0, 0, 'b', 'e', 'e', 'r', BYTE_SEPARATOR, 'b', 'u', 'd', 'w', 'e', 'i', 's', 'e', 'r'},
			[]byte{},
		},
		// type t, invalid val (missing norm)
		{
			[]byte{'t', 0, 0, 'b', 'e', 'e', 'r', BYTE_SEPARATOR, 'b', 'u', 'd', 'w', 'e', 'i', 's', 'e', 'r'},
			[]byte{3, 0, 0, 0, 0, 0, 0, 0},
		},
		// type t, invalid val (half missing tv field, full missing is valid (no term vectors))
		{
			[]byte{'t', 0, 0, 'b', 'e', 'e', 'r', BYTE_SEPARATOR, 'b', 'u', 'd', 'w', 'e', 'i', 's', 'e', 'r'},
			[]byte{3, 0, 0, 0, 0, 0, 0, 0, 195, 245, 72, 64, 0},
		},
		// type t, invalid val (missing tv pos)
		{
			[]byte{'t', 0, 0, 'b', 'e', 'e', 'r', BYTE_SEPARATOR, 'b', 'u', 'd', 'w', 'e', 'i', 's', 'e', 'r'},
			[]byte{3, 0, 0, 0, 0, 0, 0, 0, 195, 245, 72, 64, 0, 0},
		},
		// type t, invalid val (missing tv start)
		{
			[]byte{'t', 0, 0, 'b', 'e', 'e', 'r', BYTE_SEPARATOR, 'b', 'u', 'd', 'w', 'e', 'i', 's', 'e', 'r'},
			[]byte{3, 0, 0, 0, 0, 0, 0, 0, 195, 245, 72, 64, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0},
		},
		// type t, invalid val (missing tv end)
		{
			[]byte{'t', 0, 0, 'b', 'e', 'e', 'r', BYTE_SEPARATOR, 'b', 'u', 'd', 'w', 'e', 'i', 's', 'e', 'r'},
			[]byte{3, 0, 0, 0, 0, 0, 0, 0, 195, 245, 72, 64, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0},
		},
		// type b, invalid key (missing id)
		{
			[]byte{'b'},
			[]byte{'b', 'e', 'e', 'r', BYTE_SEPARATOR, 0, 0},
		},
		// type b, invalid val (missing field)
		{
			[]byte{'b', 'b', 'u', 'd', 'w', 'e', 'i', 's', 'e', 'r'},
			[]byte{'g', 'a', 'r', 'b', 'a', 'g', 'e'},
		},
		// type s, invalid key (missing id)
		{
			[]byte{'s'},
			[]byte{'t', 'a', 'n', ' ', 'a', 'm', 'e', 'r', 'i', 'c', 'a', 'n', ' ', 'b', 'e', 'e', 'r'},
		},
		// type b, invalid val (missing field)
		{
			[]byte{'s', 'b', 'u', 'd', 'w', 'e', 'i', 's', 'e', 'r', BYTE_SEPARATOR},
			[]byte{'t', 'a', 'n', ' ', 'a', 'm', 'e', 'r', 'i', 'c', 'a', 'n', ' ', 'b', 'e', 'e', 'r'},
		},
	}

	for _, test := range tests {
		_, err := ParseFromKeyValue(test.key, test.val)
		if err == nil {
			t.Errorf("expected error, got nil")
		}
	}
}
