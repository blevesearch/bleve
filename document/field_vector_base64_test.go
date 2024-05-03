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
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"
)

func TestDecodeVector(t *testing.T) {
	vec := make([]float32, 2048)
	for i := range vec {
		vec[i] = rand.Float32()
	}

	vecBytes := bytifyVec(vec)
	encodedVec := base64.StdEncoding.EncodeToString(vecBytes)

	decodedVector, err := DecodeVector(encodedVec)
	if err != nil {
		t.Error(err)
	}
	if len(decodedVector) != len(vec) {
		t.Errorf("Decoded vector dimensions not same as original vector dimensions")
	}

	for i := range vec {
		if vec[i] != decodedVector[i] {
			t.Fatalf("Decoded vector not the same as original vector %v != %v", vec[i], decodedVector[i])
		}
	}
}

func BenchmarkDecodeVector128(b *testing.B) {
	vec := make([]float32, 128)
	for i := range vec {
		vec[i] = rand.Float32()
	}

	vecBytes := bytifyVec(vec)
	encodedVec := base64.StdEncoding.EncodeToString(vecBytes)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = DecodeVector(encodedVec)
	}
}

func BenchmarkDecodeVector784(b *testing.B) {
	vec := make([]float32, 784)
	for i := range vec {
		vec[i] = rand.Float32()
	}

	vecBytes := bytifyVec(vec)
	encodedVec := base64.StdEncoding.EncodeToString(vecBytes)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = DecodeVector(encodedVec)
	}
}

func BenchmarkDecodeVector1536(b *testing.B) {
	vec := make([]float32, 1536)
	for i := range vec {
		vec[i] = rand.Float32()
	}

	vecBytes := bytifyVec(vec)
	encodedVec := base64.StdEncoding.EncodeToString(vecBytes)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = DecodeVector(encodedVec)
	}
}

func bytifyVec(vec []float32) []byte {
	buf := new(bytes.Buffer)

	for _, v := range vec {
		err := binary.Write(buf, binary.LittleEndian, v)
		if err != nil {
			fmt.Println(err)
		}
	}

	return buf.Bytes()
}
