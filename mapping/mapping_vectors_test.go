//  Copyright (c) 2023 Couchbase, Inc.
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

//go:build vectors
// +build vectors

package mapping

import (
	"testing"
)

type vectorTest struct {
	vector       interface{} // input vector
	dims         int         // expected dims of input vector
	valid        bool        // expected validity of input vector
	parsedVector []float32   // expected parsed vector
}

func TestProcessVector(t *testing.T) {
	tests := []vectorTest{
		// # Flat vectors

		// ## numeric cases
		// (all numeric elements)
		{[]any{1, 2.2, 3}, 3, true, []float32{1, 2.2, 3}}, // len==dims
		{[]any{1, 2.2, 3}, 2, false, nil},                 // len>dims
		{[]any{1, 2.2, 3}, 4, false, nil},                 // len<dims

		// ## imposter cases
		// (len==dims, some elements are non-numeric)
		{[]any{1, 2, "three"}, 3, false, nil},    // string
		{[]any{1, nil, 3}, 3, false, nil},        // nil
		{[]any{nil, 1}, 2, false, nil},           // nil head
		{[]any{1, 2, struct{}{}}, 3, false, nil}, // struct

		// non-slice cases
		// (vector is of types other than slice)
		{nil, 1, false, nil},
		{struct{}{}, 1, false, nil},
		{1, 1, false, nil},

		// # Nested vectors

		// ## numeric cases
		// (all numeric elements)
		{[]any{[]any{1, 2, 3}, []any{4, 5, 6}}, 3, true,
			[]float32{1, 2, 3, 4, 5, 6}}, // len==dims
		{[]any{[]any{1, 2, 3}}, 3, true, []float32{1, 2, 3}}, // len==dims
		{[]any{[]any{1, 2, 3}}, 4, false, nil},               // len>dims
		{[]any{[]any{1, 2, 3}}, 2, false, nil},               // len<dims

		// ## imposter cases
		// some inner vectors are short
		{[]any{[]any{1, 2, 3}, []any{4, 5}}, 3, false, nil},
		// some inner vectors are long
		{[]any{[]any{1, 2, 3}, []any{4, 5, 6, 7}}, 3, false, nil},
		// contains string
		{[]any{[]any{1, 2, "three"}, []any{4, 5, 6}}, 3, false, nil},
		// contains nil
		{[]any{[]any{1, 2, nil}, []any{4, 5, 6}}, 3, false, nil},

		// non-slice cases (inner vectors)
		{[]any{[]any{1, 2, 3}, nil}, 3, false, nil},        // nil
		{[]any{nil, []any{1, 2, 3}}, 3, false, nil},        // nil head
		{[]any{[]any{1, 2, 3}, struct{}{}}, 3, false, nil}, // struct
		{[]any{[]any{1, 2, 3}, 4}, 3, false, nil},          // int
	}

	for _, test := range tests {
		vec, ok := processVector(test.vector, test.dims)
		if ok != test.valid {
			t.Errorf("validity mismatch for %v: expected %v, got %v",
				test.vector, test.valid, ok)
			t.Fail()
		}

		// If input vector is valid, then compare parsed vector with expected
		if test.valid {
			if len(vec) != len(test.parsedVector) {
				t.Errorf("parsed vector mismatch for: %v: expected %v, got %v",
					test.vector, test.parsedVector, vec)
				t.Fail()
			}

			for i := 0; i < len(vec); i++ {
				if vec[i] != test.parsedVector[i] {
					t.Errorf("parsed vector mismatch for: %v: expected %v, got %v",
						test.vector, test.parsedVector, vec)
					t.Fail()
				}
			}
		}
	}
}
