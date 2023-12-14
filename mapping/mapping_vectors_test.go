//  Copyright (c) 2023 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build vectors
// +build vectors

package mapping

import "testing"

func TestVectorFieldAliasValidation(t *testing.T) {
	tests := []struct {
		// input
		name       string // name of the test
		mappingStr string // index mapping json string

		// expected output
		expValidity bool   // validity of the mapping
		errMsg      string // error message, given expValidity is false
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
			errMsg:      `field: 'cityVec', invalid alias (different dimensions 4 and 3)`,
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
			errMsg:      `field: 'cityVec', invalid alias (different similarity values dot_product and l2_norm)`,
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
			errMsg:      "",
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
			errMsg:      `field: 'vecData', invalid alias (different dimensions 3 and 4)`,
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
			errMsg:      `field: 'vecData', invalid alias (different dimensions 4 and 3)`,
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

			if !isValid && err.Error() != test.errMsg {
				t.Fatalf("invalid error message, expected: %v, got: %v",
					test.errMsg, err.Error())
			}
		})
	}
}
