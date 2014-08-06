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

	"github.com/couchbaselabs/bleve/analysis"
	"github.com/couchbaselabs/bleve/numeric_util"
	"github.com/couchbaselabs/bleve/search"
)

type DateRangeQuery struct {
	Start          *string `json:"start,omitempty"`
	End            *string `json:"end,omitempty"`
	FieldVal       string  `json:"field,omitempty"`
	BoostVal       float64 `json:"boost,omitempty"`
	DateTimeParser *string `json:"datetime_parser,omitempty"`
}

func NewDateRangeQuery(start, end *string) *DateRangeQuery {
	return &DateRangeQuery{
		Start:    start,
		End:      end,
		BoostVal: 1.0,
	}
}

func (q *DateRangeQuery) Boost() float64 {
	return q.BoostVal
}

func (q *DateRangeQuery) SetBoost(b float64) *DateRangeQuery {
	q.BoostVal = b
	return q
}

func (q *DateRangeQuery) Field() string {
	return q.FieldVal
}

func (q *DateRangeQuery) SetField(f string) *DateRangeQuery {
	q.FieldVal = f
	return q
}

func (q *DateRangeQuery) Searcher(i *indexImpl, explain bool) (search.Searcher, error) {

	var dateTimeParser analysis.DateTimeParser
	if q.DateTimeParser != nil {
		dateTimeParser = Config.Analysis.DateTimeParsers[*q.DateTimeParser]
	} else {
		dateTimeParser = i.m.datetimeParserForPath(q.FieldVal)
	}
	if dateTimeParser == nil {
		return nil, fmt.Errorf("no datetime parser named '%s' registered", *q.DateTimeParser)
	}

	field := q.FieldVal
	if q.FieldVal == "" {
		field = i.m.defaultField()
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

	return search.NewNumericRangeSearcher(i.i, &min, &max, field, q.BoostVal, explain)
}

func (q *DateRangeQuery) Validate() error {
	if q.Start == nil && q.Start == q.End {
		return fmt.Errorf("must specify start or end")
	}
	return nil
}
