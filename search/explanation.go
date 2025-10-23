//  Copyright (c) 2014 Couchbase, Inc.
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

package search

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/blevesearch/bleve/v2/size"
)

var reflectStaticSizeExplanation int

func init() {
	var e Explanation
	reflectStaticSizeExplanation = int(reflect.TypeOf(e).Size())
}

type Explanation struct {
	Value        float64        `json:"value"`
	Message      string         `json:"message"`
	PartialMatch bool           `json:"partial_match,omitempty"`
	Children     []*Explanation `json:"children,omitempty"`
}

func (expl *Explanation) String() string {
	js, err := json.MarshalIndent(expl, "", "  ")
	if err != nil {
		return fmt.Sprintf("error serializing explanation to json: %v", err)
	}
	return string(js)
}

func (expl *Explanation) Size() int {
	sizeInBytes := reflectStaticSizeExplanation + size.SizeOfPtr +
		len(expl.Message)

	for _, entry := range expl.Children {
		sizeInBytes += entry.Size()
	}

	return sizeInBytes
}

const MergedExplMessage = "sum of merged explanations:"

func MergeExpl(first, second *Explanation) *Explanation {
	if first == nil {
		return second
	}
	if second == nil {
		return first
	}
	if first.Message == MergedExplMessage {
		// reuse first explanation as the merged one
		first.Value += second.Value
		first.Children = append(first.Children, second)
		return first
	}
	if second.Message == MergedExplMessage {
		// reuse second explanation as the merged one
		second.Value += first.Value
		second.Children = append(second.Children, first)
		return second
	}
	// create a new explanation to hold the merged one
	rv := &Explanation{
		Value:    first.Value + second.Value,
		Message:  MergedExplMessage,
		Children: []*Explanation{first, second},
	}
	return rv
}

func MergeScoreBreakdown(first, second map[int]float64) map[int]float64 {
	if first == nil {
		return second
	}
	if second == nil {
		return first
	}
	// reuse first to store the union of both
	for k, v := range second {
		first[k] += v
	}
	return first
}
