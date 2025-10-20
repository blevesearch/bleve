//  Copyright (c) 2025 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fusion

import (
	"github.com/blevesearch/bleve/v2/search"
)

// scoreBreakdownSortFunc returns a comparison function for sorting DocumentMatch objects
// by their ScoreBreakdown at the specified index in descending order.
// In case of ties, documents with lower HitNumber (earlier hits) are preferred.
// If either document is missing the ScoreBreakdown for the specified index,
// it's treated as having a score of 0.0.
func scoreBreakdownSortFunc(idx int) func(i, j *search.DocumentMatch) int {
	return func(i, j *search.DocumentMatch) int {
		// Safely extract scores, defaulting to 0.0 if missing
		iScore := 0.0
		jScore := 0.0

		if i.ScoreBreakdown != nil {
			if score, ok := i.ScoreBreakdown[idx]; ok {
				iScore = score
			}
		}

		if j.ScoreBreakdown != nil {
			if score, ok := j.ScoreBreakdown[idx]; ok {
				jScore = score
			}
		}

		// Sort by score in descending order (higher scores first)
		if iScore > jScore {
			return -1
		} else if iScore < jScore {
			return 1
		}

		// Break ties by HitNumber in ascending order (lower HitNumber wins)
		if i.HitNumber < j.HitNumber {
			return -1
		} else if i.HitNumber > j.HitNumber {
			return 1
		}

		return 0 // Equal scores and HitNumbers
	}
}

func scoreSortFunc() func(i, j *search.DocumentMatch) int {
	return func(i, j *search.DocumentMatch) int {
		// Sort by score in descending order
		if i.Score > j.Score {
			return -1
		} else if i.Score < j.Score {
			return 1
		}

		// Break ties by HitNumber
		if i.HitNumber < j.HitNumber {
			return -1
		} else if i.HitNumber > j.HitNumber {
			return 1
		}

		return 0
	}
}

func getFusionExplAt(hit *search.DocumentMatch, i int, value float64, message string) *search.Explanation {
	return &search.Explanation{
		Value: value,
		Message: message,
		Children: []*search.Explanation{hit.Expl.Children[i]},
	}
}

func finalizeFusionExpl(hit *search.DocumentMatch, explChildren []*search.Explanation) {
	hit.Expl.Children = explChildren

	hit.Expl.Value = hit.Score
	hit.Expl.Message = "sum of"
}
