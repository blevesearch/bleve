//  Copyright (c) 2024 Couchbase, Inc.
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

package document

import (
	"encoding/base64"
	"encoding/json"
	"fmt"

	index "github.com/blevesearch/bleve_index_api"
)

type VectorBase64Field struct {
	vectorField  *VectorField
	encodedValue string
}

func (n *VectorBase64Field) Size() int {
	return n.vectorField.Size()
}

func (n *VectorBase64Field) Name() string {
	return n.vectorField.Name()
}

func (n *VectorBase64Field) ArrayPositions() []uint64 {
	return n.vectorField.ArrayPositions()
}

func (n *VectorBase64Field) Options() index.FieldIndexingOptions {
	return n.vectorField.Options()
}

func (n *VectorBase64Field) NumPlainTextBytes() uint64 {
	return n.vectorField.NumPlainTextBytes()
}

func (n *VectorBase64Field) AnalyzedLength() int {
	return n.vectorField.AnalyzedLength()
}

func (n *VectorBase64Field) EncodedFieldType() byte {
	return 'e' // CHECK
}

func (n *VectorBase64Field) AnalyzedTokenFrequencies() index.TokenFrequencies {
	return n.vectorField.AnalyzedTokenFrequencies()
}

func (n *VectorBase64Field) Analyze() {
	// CHECK
}

func (n *VectorBase64Field) Value() []byte {
	return n.vectorField.Value()
}

func (n *VectorBase64Field) GoString() string {
	return fmt.Sprintf("&document.vectorFieldBase64Field{Name:%s, Options: %s, "+
		"Value: %+v}", n.vectorField.Name(), n.vectorField.Options(), n.vectorField.Value())
}

// For the sake of not polluting the API, we are keeping arrayPositions as a
// parameter, but it is not used.
func NewVectorBase64Field(name string, arrayPositions []uint64, encodedValue string,
	dims int, similarity, vectorIndexOptimizedFor string) (*VectorBase64Field, error) {

	vector, err := DecodeVector(encodedValue)
	if err != nil {
		return nil, err
	}

	return &VectorBase64Field{
		vectorField: NewVectorFieldWithIndexingOptions(name, arrayPositions,
			vector, dims, similarity,
			vectorIndexOptimizedFor, DefaultVectorIndexingOptions),

		encodedValue: encodedValue,
	}, nil
}

func DecodeVector(encodedValue string) ([]float32, error) {
	decodedString, err := base64.StdEncoding.DecodeString(encodedValue)
	if err != nil {
		fmt.Println("Error decoding string:", err)
		return nil, err
	}

	var decodedVector []float32
	err = json.Unmarshal(decodedString, &decodedVector)
	if err != nil {
		fmt.Println("Error decoding string:", err)
		return nil, err
	}

	return decodedVector, nil
}

func (n *VectorBase64Field) Vector() []float32 {
	return n.vectorField.Vector()
}

func (n *VectorBase64Field) Dims() int {
	return n.vectorField.Dims()
}

func (n *VectorBase64Field) Similarity() string {
	return n.vectorField.Similarity()
}

func (n *VectorBase64Field) IndexOptimizedFor() string {
	return n.vectorField.IndexOptimizedFor()
}
