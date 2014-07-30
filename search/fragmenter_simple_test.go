//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
package search

import (
	"reflect"
	"testing"
)

func TestSimpleFragmenter(t *testing.T) {

	tests := []struct {
		orig      []byte
		fragments []*Fragment
		ot        termLocations
	}{
		{
			orig: []byte("this is a test"),
			fragments: []*Fragment{
				&Fragment{
					orig:  []byte("this is a test"),
					start: 0,
					end:   14,
				},
			},
			ot: termLocations{
				&termLocation{
					Term:  "test",
					Pos:   4,
					Start: 10,
					End:   14,
				},
			},
		},
		{
			orig: []byte("0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"),
			fragments: []*Fragment{
				&Fragment{
					orig:  []byte("0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"),
					start: 0,
					end:   100,
				},
			},
			ot: termLocations{
				&termLocation{
					Term:  "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789",
					Pos:   1,
					Start: 0,
					End:   100,
				},
			},
		},
		{
			orig: []byte("01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"),
			fragments: []*Fragment{
				&Fragment{
					orig:  []byte("01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"),
					start: 0,
					end:   100,
				},
				&Fragment{
					orig:  []byte("01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"),
					start: 10,
					end:   101,
				},
				&Fragment{
					orig:  []byte("01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"),
					start: 20,
					end:   101,
				},
				&Fragment{
					orig:  []byte("01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"),
					start: 30,
					end:   101,
				},
				&Fragment{
					orig:  []byte("01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"),
					start: 40,
					end:   101,
				},
				&Fragment{
					orig:  []byte("01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"),
					start: 50,
					end:   101,
				},
				&Fragment{
					orig:  []byte("01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"),
					start: 60,
					end:   101,
				},
				&Fragment{
					orig:  []byte("01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"),
					start: 70,
					end:   101,
				},
				&Fragment{
					orig:  []byte("01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"),
					start: 80,
					end:   101,
				},
				&Fragment{
					orig:  []byte("01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"),
					start: 90,
					end:   101,
				},
			},
			ot: termLocations{
				&termLocation{
					Term:  "0123456789",
					Pos:   1,
					Start: 0,
					End:   10,
				},
				&termLocation{
					Term:  "0123456789",
					Pos:   2,
					Start: 10,
					End:   20,
				},
				&termLocation{
					Term:  "0123456789",
					Pos:   3,
					Start: 20,
					End:   30,
				},
				&termLocation{
					Term:  "0123456789",
					Pos:   4,
					Start: 30,
					End:   40,
				},
				&termLocation{
					Term:  "0123456789",
					Pos:   5,
					Start: 40,
					End:   50,
				},
				&termLocation{
					Term:  "0123456789",
					Pos:   6,
					Start: 50,
					End:   60,
				},
				&termLocation{
					Term:  "0123456789",
					Pos:   7,
					Start: 60,
					End:   70,
				},
				&termLocation{
					Term:  "0123456789",
					Pos:   8,
					Start: 70,
					End:   80,
				},
				&termLocation{
					Term:  "0123456789",
					Pos:   9,
					Start: 80,
					End:   90,
				},
				&termLocation{
					Term:  "0123456789",
					Pos:   10,
					Start: 90,
					End:   100,
				},
			},
		},
	}

	fragmenter := NewSimpleFragmenter()
	for _, test := range tests {
		fragments := fragmenter.Fragment(test.orig, test.ot)
		if !reflect.DeepEqual(fragments, test.fragments) {
			t.Errorf("expected %#v, got %#v", test.fragments, fragments)
			for _, fragment := range fragments {
				t.Logf("frag: %#v", fragment)
			}
		}
	}
}

func TestSimpleFragmenterWithSize(t *testing.T) {

	tests := []struct {
		orig      []byte
		fragments []*Fragment
		ot        termLocations
	}{
		{
			orig: []byte("this is a test"),
			fragments: []*Fragment{
				&Fragment{
					orig:  []byte("this is a test"),
					start: 0,
					end:   5,
				},
				&Fragment{
					orig:  []byte("this is a test"),
					start: 9,
					end:   14,
				},
			},
			ot: termLocations{
				&termLocation{
					Term:  "this",
					Pos:   1,
					Start: 0,
					End:   5,
				},
				&termLocation{
					Term:  "test",
					Pos:   4,
					Start: 10,
					End:   14,
				},
			},
		},
	}

	fragmenter := NewSimpleFragmenterWithSize(5)
	for _, test := range tests {
		fragments := fragmenter.Fragment(test.orig, test.ot)
		if !reflect.DeepEqual(fragments, test.fragments) {
			t.Errorf("expected %#v, got %#v", test.fragments, fragments)
			for _, fragment := range fragments {
				t.Logf("frag: %#v", fragment)
			}
		}
	}
}
