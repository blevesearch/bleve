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

package util

import (
	"fmt"
	"reflect"
	"testing"
)

func TestMergeIntervals(t *testing.T) {
	tests := []struct {
		intervals [][2]uint64
		expected  [][2]uint64
	}{
		{
			[][2]uint64{
				{2, 6},
				{1, 3},
				{8, 10},
				{15, 18},
			},
			[][2]uint64{
				{1, 6},
				{8, 10},
				{15, 18},
			},
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("test#%d", i), func(t *testing.T) {
			merged := MergeIntervals(test.intervals)
			if !reflect.DeepEqual(merged, test.expected) {
				t.Errorf("Expected: %v, got: %v", test.expected, merged)
			}
		})
	}
}

func TestOverlapRatio(t *testing.T) {
	tests := []struct {
		ranges      [][2]uint64
		targetRange [2]uint64

		expected float32
	}{
		{
			[][2]uint64{
				{2, 6},
				{1, 3},
				{8, 10},
				{15, 18},
			},
			[2]uint64{
				2, 9,
			},
			float32(5) / float32(7),
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("test#%d", i), func(t *testing.T) {
			ratio := OverlapRatio(test.ranges, test.targetRange)
			if ratio != test.expected {
				t.Errorf("Expected: %v, got: %v", test.expected, ratio)
			}
		})
	}
}
