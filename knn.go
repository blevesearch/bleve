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

//go:build vector
// +build vector

package bleve

import (
	"github.com/blevesearch/bleve/v2/search/query"
)

func disjunctQueryWithKNN(req *SearchRequest) query.Query {
	if req.KNN != nil {
		knnQuery := query.NewKNNQuery(req.KNN.Vector)
		knnQuery.SetFieldVal(req.KNN.Field)
		knnQuery.SetK(req.KNN.K)
		knnQuery.SetBoost(req.KNN.Boost.Value())
		return query.NewDisjunctionQuery([]query.Query{req.Query, knnQuery})
	}
	return req.Query
}
