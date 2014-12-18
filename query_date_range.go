//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package bleve

import (
	"fmt"
	"math"

	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/numeric_util"
	"github.com/blevesearch/bleve/search"
	"github.com/blevesearch/bleve/search/searchers"
)

type dateRangeQuery struct {
	Start          *string `json:"start,omitempty"`
	End            *string `json:"end,omitempty"`
	InclusiveStart *bool   `json:"inclusive_start,omitempty"`
	InclusiveEnd   *bool   `json:"inclusive_end,omitempty"`
	FieldVal       string  `json:"field,omitempty"`
	BoostVal       float64 `json:"boost,omitempty"`
	DateTimeParser *string `json:"datetime_parser,omitempty"`
}

// NewDateRangeQuery creates a new Query for ranges
// of date values.
// A DateTimeParser is chosen based on the field.
// Either, but not both endpoints can be nil.
func NewDateRangeQuery(start, end *string) *dateRangeQuery {
	return NewDateRangeInclusiveQuery(start, end, nil, nil)
}

// NewDateRangeInclusiveQuery creates a new Query for ranges
// of date values.
// A DateTimeParser is chosen based on the field.
// Either, but not both endpoints can be nil.
// startInclusive and endInclusive control inclusion of the endpoints.
func NewDateRangeInclusiveQuery(start, end *string, startInclusive, endInclusive *bool) *dateRangeQuery {
	return &dateRangeQuery{
		Start:          start,
		End:            end,
		InclusiveStart: startInclusive,
		InclusiveEnd:   endInclusive,
		BoostVal:       1.0,
	}
}

func (q *dateRangeQuery) Boost() float64 {
	return q.BoostVal
}

func (q *dateRangeQuery) SetBoost(b float64) Query {
	q.BoostVal = b
	return q
}

func (q *dateRangeQuery) Field() string {
	return q.FieldVal
}

func (q *dateRangeQuery) SetField(f string) Query {
	q.FieldVal = f
	return q
}

func (q *dateRangeQuery) Searcher(i index.IndexReader, m *IndexMapping, explain bool) (search.Searcher, error) {

	dateTimeParserName := ""
	if q.DateTimeParser != nil {
		dateTimeParserName = *q.DateTimeParser
	} else {
		dateTimeParserName = m.datetimeParserNameForPath(q.FieldVal)
	}
	dateTimeParser := m.dateTimeParserNamed(dateTimeParserName)
	if dateTimeParser == nil {
		return nil, fmt.Errorf("no datetime parser named '%s' registered", *q.DateTimeParser)
	}

	field := q.FieldVal
	if q.FieldVal == "" {
		field = m.DefaultField
	}

	// now parse the endpoints
	min := math.Inf(-1)
	max := math.Inf(1)
	if q.Start != nil && *q.Start != "" {
		startTime, err := dateTimeParser.ParseDateTime(*q.Start)
		if err != nil {
			return nil, err
		}
		min = numeric_util.Int64ToFloat64(startTime.UnixNano())
	}
	if q.End != nil && *q.End != "" {
		endTime, err := dateTimeParser.ParseDateTime(*q.End)
		if err != nil {
			return nil, err
		}
		max = numeric_util.Int64ToFloat64(endTime.UnixNano())
	}

	return searchers.NewNumericRangeSearcher(i, &min, &max, q.InclusiveStart, q.InclusiveEnd, field, q.BoostVal, explain)
}

func (q *dateRangeQuery) Validate() error {
	if q.Start == nil && q.Start == q.End {
		return fmt.Errorf("must specify start or end")
	}
	return nil
}
