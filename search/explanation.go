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

const MergedExplMessage = "sum of merged explanations:"

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

// MergeExpl merges two explanations into one.
// If either explanation is nil, the other is returned.
// If the first explanation is already a merged explanation,
// the second explanation is appended to its children.
// Otherwise, a new merged explanation is created
// with the two explanations as its children.
func (expl *Explanation) MergeWith(other *Explanation) *Explanation {
	if expl == nil {
		return other
	}
	if other == nil || expl == other {
		return expl
	}

	newScore := expl.Value + other.Value

	// if both are merged explanations, combine children
	if expl.Message == MergedExplMessage && other.Message == MergedExplMessage {
		expl.Value = newScore
		expl.Children = append(expl.Children, other.Children...)
		return expl
	}

	// atleast one is not a merged explanation see which one it is
	// if expl is merged, append other
	if expl.Message == MergedExplMessage {
		// append other as a child to first
		expl.Value = newScore
		expl.Children = append(expl.Children, other)
		return expl
	}

	// if other is merged, append expl
	if other.Message == MergedExplMessage {
		other.Value = newScore
		other.Children = append(other.Children, expl)
		return other
	}
	// create a new explanation to hold the merged one
	rv := &Explanation{
		Value:    expl.Value + other.Value,
		Message:  MergedExplMessage,
		Children: []*Explanation{expl, other},
	}
	return rv
}
