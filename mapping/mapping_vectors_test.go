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
	"math"
	"reflect"
	"strings"
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
		// Test 6: Different vector index optimization values (alias case)
		{
			name: "different_optimization_alias",
			mappingStr: `
				{
					"default_mapping": {
						"properties": {
							"cityVec": {
								"fields": [
									{
										"type": "vector",
										"dims": 3,
										"vector_index_optimized_for": "recall"
									},
									{
										"name": "cityVec",
										"type": "vector",
										"dims": 3,
										"vector_index_optimized_for": "latency"
									}
								]
							}
						}
					}
				}`,
			expValidity: false,
			errMsgs:     []string{`field: 'cityVec', invalid alias (different vector index optimization values latency and recall)`},
		},
		// Test 7: Invalid dimensions - below minimum
		{
			name: "dims_below_minimum",
			mappingStr: `
				{
					"default_mapping": {
						"properties": {
							"cityVec": {
								"fields": [
									{
										"type": "vector",
										"dims": 0
									}
								]
							}
						}
					}
				}`,
			expValidity: false,
			errMsgs:     []string{`field: 'cityVec', invalid vector dimension: 0, value should be in range [1, 4096]`},
		},
		// Test 8: Invalid dimensions - above maximum
		{
			name: "dims_above_maximum",
			mappingStr: `
				{
					"default_mapping": {
						"properties": {
							"cityVec": {
								"fields": [
									{
										"type": "vector",
										"dims": 5000
									}
								]
							}
						}
					}
				}`,
			expValidity: false,
			errMsgs:     []string{`field: 'cityVec', invalid vector dimension: 5000, value should be in range [1, 4096]`},
		},
		// Test 9: Invalid similarity metric
		{
			name: "invalid_similarity_metric",
			mappingStr: `
				{
					"default_mapping": {
						"properties": {
							"cityVec": {
								"fields": [
									{
										"type": "vector",
										"dims": 3,
										"similarity": "invalid_metric"
									}
								]
							}
						}
					}
				}`,
			expValidity: false,
			// Note: error message contains map keys which have non-deterministic order
			errMsgs: []string{`invalid similarity metric: 'invalid_metric'`},
		},
		// Test 10: Invalid vector index optimization
		{
			name: "invalid_optimization",
			mappingStr: `
				{
					"default_mapping": {
						"properties": {
							"cityVec": {
								"fields": [
									{
										"type": "vector",
										"dims": 3,
										"vector_index_optimized_for": "invalid_opt"
									}
								]
							}
						}
					}
				}`,
			expValidity: false,
			// Note: error message contains map keys which have non-deterministic order
			errMsgs: []string{`invalid vector index optimization: 'invalid_opt'`},
		},
		// Test 11: vector_base64 type with valid dimensions
		{
			name: "vector_base64_valid",
			mappingStr: `
				{
					"default_mapping": {
						"properties": {
							"cityVec": {
								"fields": [
									{
										"type": "vector_base64",
										"dims": 128
									}
								]
							}
						}
					}
				}`,
			expValidity: true,
			errMsgs:     []string{},
		},
		// Test 12: vector_base64 alias with different dimensions
		{
			name: "vector_base64_different_dims_alias",
			mappingStr: `
				{
					"default_mapping": {
						"properties": {
							"cityVec": {
								"fields": [
									{
										"type": "vector_base64",
										"dims": 128
									},
									{
										"name": "cityVec",
										"type": "vector_base64",
										"dims": 256
									}
								]
							}
						}
					}
				}`,
			expValidity: false,
			errMsgs:     []string{`field: 'cityVec', invalid alias (different dimensions 256 and 128)`},
		},
		// Test 13: Default similarity matching explicit similarity in alias
		{
			name: "default_similarity_matches_explicit",
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
										"dims": 3,
										"similarity": "l2_norm"
									}
								]
							}
						}
					}
				}`,
			expValidity: true,
			errMsgs:     []string{},
		},
		// Test 14: Default optimization matching explicit optimization in alias
		{
			name: "default_optimization_matches_explicit",
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
										"dims": 3,
										"vector_index_optimized_for": "recall"
									}
								]
							}
						}
					}
				}`,
			expValidity: true,
			errMsgs:     []string{},
		},
		// Test 15: Valid alias with all explicit matching values
		{
			name: "valid_alias_all_explicit_matching",
			mappingStr: `
				{
					"default_mapping": {
						"properties": {
							"cityVec": {
								"fields": [
									{
										"type": "vector",
										"dims": 64,
										"similarity": "dot_product",
										"vector_index_optimized_for": "latency"
									},
									{
										"name": "cityVec",
										"type": "vector",
										"dims": 64,
										"similarity": "dot_product",
										"vector_index_optimized_for": "latency"
									}
								]
							}
						}
					}
				}`,
			expValidity: true,
			errMsgs:     []string{},
		},
		// Test 16: Cross-property alias with different similarity
		{
			name: "cross_property_different_similarity",
			mappingStr: `
				{
					"default_mapping": {
						"properties": {
							"cityVec": {
								"fields": [
									{
										"name": "vecData",
										"type": "vector",
										"dims": 3,
										"similarity": "cosine"
									}
								]
							},
							"countryVec": {
								"fields": [
									{
										"name": "vecData",
										"type": "vector",
										"dims": 3,
										"similarity": "l2_norm"
									}
								]
							}
						}
					}
				}`,
			expValidity: false,
			errMsgs: []string{
				`field: 'vecData', invalid alias (different similarity values l2_norm and cosine)`,
				`field: 'vecData', invalid alias (different similarity values cosine and l2_norm)`,
			},
		},
		// Test 17: Cross-property alias with different optimization
		{
			name: "cross_property_different_optimization",
			mappingStr: `
				{
					"default_mapping": {
						"properties": {
							"cityVec": {
								"fields": [
									{
										"name": "vecData",
										"type": "vector",
										"dims": 3,
										"vector_index_optimized_for": "recall"
									}
								]
							},
							"countryVec": {
								"fields": [
									{
										"name": "vecData",
										"type": "vector",
										"dims": 3,
										"vector_index_optimized_for": "memory-efficient"
									}
								]
							}
						}
					}
				}`,
			expValidity: false,
			errMsgs: []string{
				`field: 'vecData', invalid alias (different vector index optimization values memory-efficient and recall)`,
				`field: 'vecData', invalid alias (different vector index optimization values recall and memory-efficient)`,
			},
		},
		// Test 18: Valid cross-property alias with matching values
		{
			name: "valid_cross_property_alias",
			mappingStr: `
				{
					"default_mapping": {
						"properties": {
							"cityVec": {
								"fields": [
									{
										"name": "vecData",
										"type": "vector",
										"dims": 64,
										"similarity": "dot_product",
										"vector_index_optimized_for": "latency"
									}
								]
							},
							"countryVec": {
								"fields": [
									{
										"name": "vecData",
										"type": "vector",
										"dims": 64,
										"similarity": "dot_product",
										"vector_index_optimized_for": "latency"
									}
								]
							}
						}
					}
				}`,
			expValidity: true,
			errMsgs:     []string{},
		},
		// Test 20: Different fully qualified paths - a.b.c.f vs f (different effective names, no conflict)
		{
			name: "different_fq_paths_no_conflict",
			mappingStr: `
				{
					"default_mapping": {
						"properties": {
							"a": {
								"properties": {
									"b": {
										"properties": {
											"c": {
												"fields": [
													{
														"name": "f",
														"type": "vector",
														"dims": 64
													}
												]
											}
										}
									}
								}
							},
							"x": {
								"fields": [
									{
										"name": "f",
										"type": "vector",
										"dims": 128
									}
								]
							}
						}
					}
				}`,
			expValidity: true,
			errMsgs:     []string{},
		},
		// Test 21: Same leaf property name at different paths (a.b.vec vs x.y.vec) - no conflict
		{
			name: "same_leaf_different_paths_no_conflict",
			mappingStr: `
				{
					"default_mapping": {
						"properties": {
							"a": {
								"properties": {
									"b": {
										"properties": {
											"vec": {
												"fields": [
													{
														"type": "vector",
														"dims": 64
													}
												]
											}
										}
									}
								}
							},
							"x": {
								"properties": {
									"y": {
										"properties": {
											"vec": {
												"fields": [
													{
														"type": "vector",
														"dims": 128
													}
												]
											}
										}
									}
								}
							}
						}
					}
				}`,
			expValidity: true,
			errMsgs:     []string{},
		},
		// Test 22: Field name override creates same effective name - alias conflict
		// a.b with name "data" → effective "a.data"
		// a with name "data" → effective "data"
		// These are different, so no conflict
		{
			name: "field_name_override_different_parents_no_conflict",
			mappingStr: `
				{
					"default_mapping": {
						"properties": {
							"a": {
								"properties": {
									"b": {
										"fields": [
											{
												"name": "data",
												"type": "vector",
												"dims": 64
											}
										]
									}
								}
							},
							"a2": {
								"fields": [
									{
										"name": "data",
										"type": "vector",
										"dims": 128
									}
								]
							}
						}
					}
				}`,
			expValidity: true,
			errMsgs:     []string{},
		},
		// Test 23: Same effective field name via name override - should conflict
		// a.b with name "sharedVec" → effective "a.sharedVec"
		// a.c with name "sharedVec" → effective "a.sharedVec"
		// Both resolve to same effective name with different dims → conflict
		{
			name: "same_effective_name_via_override_conflict",
			mappingStr: `
				{
					"default_mapping": {
						"properties": {
							"a": {
								"properties": {
									"b": {
										"fields": [
											{
												"name": "sharedVec",
												"type": "vector",
												"dims": 64
											}
										]
									},
									"c": {
										"fields": [
											{
												"name": "sharedVec",
												"type": "vector",
												"dims": 128
											}
										]
									}
								}
							}
						}
					}
				}`,
			expValidity: false,
			errMsgs: []string{
				`field: 'a.sharedVec', invalid alias (different dimensions 128 and 64)`,
				`field: 'a.sharedVec', invalid alias (different dimensions 64 and 128)`,
			},
		},
		// Test 24: Deep nesting with same effective name via name override - should conflict
		// level1.level2.propA with name "vec" → effective "level1.level2.vec"
		// level1.level2.propB with name "vec" → effective "level1.level2.vec"
		{
			name: "deep_nesting_same_effective_name_conflict",
			mappingStr: `
				{
					"default_mapping": {
						"properties": {
							"level1": {
								"properties": {
									"level2": {
										"properties": {
											"propA": {
												"fields": [
													{
														"name": "vec",
														"type": "vector",
														"dims": 64
													}
												]
											},
											"propB": {
												"fields": [
													{
														"name": "vec",
														"type": "vector",
														"dims": 128
													}
												]
											}
										}
									}
								}
							}
						}
					}
				}`,
			expValidity: false,
			errMsgs: []string{
				`field: 'level1.level2.vec', invalid alias (different dimensions 128 and 64)`,
				`field: 'level1.level2.vec', invalid alias (different dimensions 64 and 128)`,
			},
		},
		// Test 25: Root level field vs nested field with same name - no conflict
		// Root: "embedding" → effective "embedding"
		// Nested: a.b.embedding → effective "a.b.embedding"
		{
			name: "root_vs_nested_same_name_no_conflict",
			mappingStr: `
				{
					"default_mapping": {
						"properties": {
							"embedding": {
								"fields": [
									{
										"type": "vector",
										"dims": 64
									}
								]
							},
							"nested": {
								"properties": {
									"deep": {
										"properties": {
											"embedding": {
												"fields": [
													{
														"type": "vector",
														"dims": 256
													}
												]
											}
										}
									}
								}
							}
						}
					}
				}`,
			expValidity: true,
			errMsgs:     []string{},
		},
		// Test 26: Multiple levels with name override targeting same effective path
		// a.b.x with name "target" → effective "a.b.target"
		// a.b.target (no override) → effective "a.b.target"
		// Same effective name, different dims → conflict
		{
			name: "name_override_matches_sibling_path_conflict",
			mappingStr: `
				{
					"default_mapping": {
						"properties": {
							"a": {
								"properties": {
									"b": {
										"properties": {
											"x": {
												"fields": [
													{
														"name": "target",
														"type": "vector",
														"dims": 64
													}
												]
											},
											"target": {
												"fields": [
													{
														"type": "vector",
														"dims": 128
													}
												]
											}
										}
									}
								}
							}
						}
					}
				}`,
			expValidity: false,
			errMsgs: []string{
				`field: 'a.b.target', invalid alias (different dimensions 128 and 64)`,
				`field: 'a.b.target', invalid alias (different dimensions 64 and 128)`,
			},
		},
		// Test 27: Valid alias at deep nesting level
		{
			name: "valid_alias_deep_nesting",
			mappingStr: `
				{
					"default_mapping": {
						"properties": {
							"a": {
								"properties": {
									"b": {
										"properties": {
											"c": {
												"properties": {
													"vec": {
														"fields": [
															{
																"type": "vector",
																"dims": 128,
																"similarity": "dot_product"
															},
															{
																"name": "vec",
																"type": "vector",
																"dims": 128,
																"similarity": "dot_product"
															}
														]
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}`,
			expValidity: true,
			errMsgs:     []string{},
		},
		// Test 28: Valid alias with different paths but same effective field name
		// vectors.vec with name "vec" → effective "vectors.vec"
		// vec with name "vec" → effective "vec"
		// Different effective names, so no conflict
		{
			name: "valid_alias_different_paths_same_field_name",
			mappingStr: `
				{
					"default_mapping": {
						"dynamic": false,
						"enabled": true,
						"properties": {
							"vectors": {
								"dynamic": true,
								"enabled": true,
								"properties": {
									"vec": {
										"enabled": true,
										"dynamic": false,
										"fields": [
											{
												"dims": 3,
												"index": true,
												"name": "vec",
												"type": "vector"
											}
										]
									}
								}
							},
							"vec": {
								"enabled": true,
								"dynamic": false,
								"fields": [
									{
										"dims": 3,
										"index": true,
										"name": "vec",
										"similarity": "l2_norm",
										"type": "vector",
										"vector_index_optimized_for": "recall"
									}
								]
							}
						}
					}
				}`,
			expValidity: true,
			errMsgs:     []string{},
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
					// Use Contains for matching since some error messages include
					// map keys which have non-deterministic ordering
					if err.Error() == possibleErrMsg || strings.Contains(err.Error(), possibleErrMsg) {
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
		{1, 2, 3, 4, 5},
		{1, 0, 0, 0, 0},
		{0.182574183, 0.365148365, 0.547722578, 0.730296731},
		{1, 1, 1, 1, 1, 1, 1, 1},
		{0},
	}

	expectedNormalizedVectors := [][]float32{
		{0.13483998, 0.26967996, 0.40451995, 0.5393599, 0.67419994},
		{1, 0, 0, 0, 0},
		{0.18257418, 0.36514837, 0.5477226, 0.73029673},
		{0.35355338, 0.35355338, 0.35355338, 0.35355338, 0.35355338, 0.35355338, 0.35355338, 0.35355338},
		{0},
	}

	for i := 0; i < len(vectors); i++ {
		normalizedVector := NormalizeVector(vectors[i])
		if !reflect.DeepEqual(normalizedVector, expectedNormalizedVectors[i]) {
			t.Errorf("[vector-%d] Expected: %v, Got: %v", i+1, expectedNormalizedVectors[i], normalizedVector)
		}
	}
}

func TestNormalizeMultiVectors(t *testing.T) {
	tests := []struct {
		name     string
		input    []float32
		dims     int
		expected []float32
	}{
		{
			name:     "single vector - already normalized",
			input:    []float32{1, 0, 0},
			dims:     3,
			expected: []float32{1, 0, 0},
		},
		{
			name:     "single vector - needs normalization",
			input:    []float32{3, 0, 0},
			dims:     3,
			expected: []float32{1, 0, 0},
		},
		{
			name:     "two vectors - X and Y directions",
			input:    []float32{3, 0, 0, 0, 4, 0},
			dims:     3,
			expected: []float32{1, 0, 0, 0, 1, 0},
		},
		{
			name:     "three vectors",
			input:    []float32{3, 0, 0, 0, 4, 0, 0, 0, 5},
			dims:     3,
			expected: []float32{1, 0, 0, 0, 1, 0, 0, 0, 1},
		},
		{
			name:     "two 2D vectors",
			input:    []float32{3, 4, 5, 12},
			dims:     2,
			expected: []float32{0.6, 0.8, 0.38461538, 0.92307693},
		},
		{
			name:     "empty vector",
			input:    []float32{},
			dims:     3,
			expected: []float32{},
		},
		{
			name:     "zero dims",
			input:    []float32{1, 2, 3},
			dims:     0,
			expected: []float32{1, 2, 3},
		},
		{
			name:     "negative dims",
			input:    []float32{1, 2, 3},
			dims:     -1,
			expected: []float32{1, 2, 3},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Make a copy of input to verify original is not modified
			inputCopy := make([]float32, len(tt.input))
			copy(inputCopy, tt.input)

			result := NormalizeMultiVector(tt.input, tt.dims)

			// Check result matches expected
			if len(result) != len(tt.expected) {
				t.Errorf("length mismatch: expected %d, got %d", len(tt.expected), len(result))
				return
			}

			for i := range result {
				if !floatApproxEqual(result[i], tt.expected[i], 1e-5) {
					t.Errorf("value mismatch at index %d: expected %v, got %v",
						i, tt.expected[i], result[i])
				}
			}

			// Verify original input was not modified
			if !reflect.DeepEqual(tt.input, inputCopy) {
				t.Errorf("original input was modified: was %v, now %v", inputCopy, tt.input)
			}

			// For valid multi-vectors, verify each sub-vector has unit magnitude
			if tt.dims > 0 && len(tt.input) > 0 && len(tt.input)%tt.dims == 0 {
				numVecs := len(result) / tt.dims
				for i := 0; i < numVecs; i++ {
					subVec := result[i*tt.dims : (i+1)*tt.dims]
					mag := magnitude(subVec)
					// Allow for zero vectors (magnitude 0) or unit vectors (magnitude 1)
					if mag > 1e-6 && !floatApproxEqual(mag, 1.0, 1e-5) {
						t.Errorf("sub-vector %d has magnitude %v, expected 1.0", i, mag)
					}
				}
			}
		})
	}
}

// Helper to compute magnitude of a vector
func magnitude(v []float32) float32 {
	var sum float32
	for _, x := range v {
		sum += x * x
	}
	return float32(math.Sqrt(float64(sum)))
}

// Helper for approximate float comparison
func floatApproxEqual(a, b, epsilon float32) bool {
	diff := a - b
	if diff < 0 {
		diff = -diff
	}
	return diff < epsilon
}
