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
	"reflect"
	"testing"
)

func TestVectorFieldAliasValidation(t *testing.T) {
	tests := []struct {
		// input
		name       string // name of the test
		mappingStr string // index mapping json string

		// expected output
		expValidity bool     // validity of the mapping
		errMsgs     []string // error message, given expValidity is false
	}{
		{
			name: "test1",
			mappingStr: `
				{
					"default_mapping": {
						"properties": {
							"cityVec": {
								"fields": [
									{
										"type": "vector",
										"dims": 3
									},
									{
										"name": "cityVec",
										"type": "vector",
										"dims": 4
									}
								]
							}
						}
					}
				}`,
			expValidity: false,
			errMsgs:     []string{`field: 'cityVec', invalid alias (different dimensions 4 and 3)`},
		},
		{
			name: "test2",
			mappingStr: `
				{
					"default_mapping": {
						"properties": {
							"cityVec": {
								"fields": [
									{
										"type": "vector",
										"dims": 3,
										"similarity": "l2_norm"
									},
									{
										"name": "cityVec",
										"type": "vector",
										"dims": 3,
										"similarity": "dot_product"
									}
								]
							}
						}
					}
				}`,
			expValidity: false,
			errMsgs:     []string{`field: 'cityVec', invalid alias (different similarity values dot_product and l2_norm)`},
		},
		{
			name: "test3",
			mappingStr: `
				{
					"default_mapping": {
						"properties": {
							"cityVec": {
								"fields": [
									{
										"type": "vector",
										"dims": 3
									},
									{
										"name": "cityVec",
										"type": "vector",
										"dims": 3
									}
								]
							}
						}
					}
				}`,
			expValidity: true,
			errMsgs:     []string{},
		},
		{
			name: "test4",
			mappingStr: `
				{
					"default_mapping": {
						"properties": {
							"cityVec": {
								"fields": [
									{
										"name": "vecData",
										"type": "vector",
										"dims": 4
									}
								]
							},
							"countryVec": {
								"fields": [
									{
										"name": "vecData",
										"type": "vector",
										"dims": 3
									}
								]
							}
						}
					}
				}`,
			expValidity: false,
			errMsgs:     []string{`field: 'vecData', invalid alias (different dimensions 3 and 4)`, `field: 'vecData', invalid alias (different dimensions 4 and 3)`},
		},
		{
			name: "test5",
			mappingStr: `
				{
					"default_mapping": {
						"properties": {
							"cityVec": {
								"fields": [
									{
										"name": "vecData",
										"type": "vector",
										"dims": 3
									}
								]
							}
						}
					},
					"types": {
						"type1": {
							"properties": {
								"cityVec": {
									"fields": [
										{
											"name": "vecData",
											"type": "vector",
											"dims": 4
										}
									]
								}
							}
						}
					}
				}`,
			expValidity: false,
			errMsgs:     []string{`field: 'vecData', invalid alias (different dimensions 4 and 3)`},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			im := NewIndexMapping()
			err := im.UnmarshalJSON([]byte(test.mappingStr))
			if err != nil {
				t.Fatalf("failed to unmarshal index mapping: %v", err)
			}

			err = im.Validate()
			isValid := err == nil
			if test.expValidity != isValid {
				t.Fatalf("validity mismatch, expected: %v, got: %v",
					test.expValidity, isValid)
			}

			if !isValid {
				errStringMatched := false
				for _, possibleErrMsg := range test.errMsgs {
					if err.Error() == possibleErrMsg {
						errStringMatched = true
						break
					}
				}
				if !errStringMatched {
					t.Fatalf("invalid error message, expected one of: %v, got: %v",
						test.errMsgs, err.Error())
				}
			}
		})
	}
}

// A test case for processVector function
type vectorTest struct {
	// Input

	ipVec interface{} // input vector
	dims  int         // dimensionality of input vector

	// Expected Output

	expValidity bool      // expected validity of the input
	expOpVec    []float32 // expected output vector, given the input is valid
}

func TestProcessVector(t *testing.T) {
	// Note: while creating vectors, we are using []any instead of []float32,
	// this is done to enhance our test coverage.
	// When we unmarshal a vector from a JSON, we get []any, not []float32.
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
		opVec, valid := processVector(test.ipVec, test.dims)

		// check the validity of the input, as returned by processVector,
		// against the expected validity.
		if valid != test.expValidity {
			t.Errorf("validity mismatch, ipVec:%v, dims:%v, expected:%v, got:%v",
				test.ipVec, test.dims, test.expValidity, valid)
			t.Fail()
		}

		// If input vector is valid, check the correctness of the output vector
		// against the expected output vector.
		if valid {
			if len(opVec) != len(test.expOpVec) {
				t.Errorf("output vector mismatch, ipVec:%v, dims:%v, "+
					"expected:%v, got:%v", test.ipVec, test.dims, test.expOpVec,
					opVec)
				t.Fail()
			}

			for i := 0; i < len(opVec); i++ {
				if opVec[i] != test.expOpVec[i] {
					t.Errorf("output vector mismatch, ipVec:%v, dims:%v, "+
						"expected:%v, got:%v", test.ipVec, test.dims, test.expOpVec,
						opVec)
					t.Fail()
				}
			}
		}
	}
}

func TestNormalizeVector(t *testing.T) {
	vectors := [][]float32{
		[]float32{1, 2, 3, 4, 5},
		[]float32{1, 0, 0, 0, 0},
		[]float32{0.182574183, 0.365148365, 0.547722578, 0.730296731},
		[]float32{1, 1, 1, 1, 1, 1, 1, 1},
		[]float32{0},
	}

	expectedNormalizedVectors := [][]float32{
		[]float32{0.13483998, 0.26967996, 0.40451995, 0.5393599, 0.67419994},
		[]float32{1, 0, 0, 0, 0},
		[]float32{0.18257418, 0.36514837, 0.5477226, 0.73029673},
		[]float32{0.35355338, 0.35355338, 0.35355338, 0.35355338, 0.35355338, 0.35355338, 0.35355338, 0.35355338},
		[]float32{0},
	}

	for i := 0; i < len(vectors); i++ {
		normalizedVector := NormalizeVector(vectors[i])
		if !reflect.DeepEqual(normalizedVector, expectedNormalizedVectors[i]) {
			t.Errorf("[vector-%d] Expected: %v, Got: %v", i+1, expectedNormalizedVectors[i], normalizedVector)
		}
	}
}
