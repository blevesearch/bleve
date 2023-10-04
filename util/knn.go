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

package util

const (
	EuclideanDistance = "l2_norm"

	// dotProduct(vecA, vecB) = vecA . vecB = |vecA| * |vecB| * cos(theta);
	//  where, theta is the angle between vecA and vecB
	// If vecA and vecB are normalized (unit magnitude), then
	// vecA . vecB = cos(theta), which is the cosine similarity.
	// Thus, we don't need a separate similarity type for cosine similarity
	CosineSimilarity = "dot_product"
)

const DefaultSimilarityMetric = EuclideanDistance

// Supported similarity metrics for vector fields
var SupportedSimilarityMetrics = map[string]struct{}{
	EuclideanDistance: {},
	CosineSimilarity:  {},
}

