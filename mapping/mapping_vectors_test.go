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
		mappingStr string //index mapping json string

		// expected output
		expValidity bool // validity of the mapping
	}{
		{
			name: "no vector field alias",
			mappingStr: `{
					"default_mapping": {
						"properties": {
							"cityVec" {
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
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			im := NewIndexMapping()
			err := im.UnmarshalJSON([]byte(test.mappingStr))
			if err != nil {
				t.Fatalf("failed to unmarshal index mapping: %v", err)
			}

			im.Validate()

		})
	}
}
