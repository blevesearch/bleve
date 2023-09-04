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

package query

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/blevesearch/bleve/v2/analysis/datetime/optional"
	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/blevesearch/bleve/v2/numeric"
	"github.com/blevesearch/bleve/v2/registry"
	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/searcher"
	index "github.com/blevesearch/bleve_index_api"
)

// QueryDateTimeParser controls the default query date time parser.
var QueryDateTimeParser = optional.Name

// QueryDateTimeFormat controls the format when Marshaling to JSON.
var QueryDateTimeFormat = time.RFC3339

var cache = registry.NewCache()

type BleveQueryTime struct {
	time.Time
}

var MinRFC3339CompatibleTime time.Time
var MaxRFC3339CompatibleTime time.Time

func init() {
	MinRFC3339CompatibleTime, _ = time.Parse(time.RFC3339, "1677-12-01T00:00:00Z")
	MaxRFC3339CompatibleTime, _ = time.Parse(time.RFC3339, "2262-04-11T11:59:59Z")
}

func queryTimeFromString(t string) (time.Time, error) {
	dateTimeParser, err := cache.DateTimeParserNamed(QueryDateTimeParser)
	if err != nil {
		return time.Time{}, err
	}
	rv, _, err := dateTimeParser.ParseDateTime(t)
	if err != nil {
		return time.Time{}, err
	}
	return rv, nil
}

func (t *BleveQueryTime) MarshalJSON() ([]byte, error) {
	tt := time.Time(t.Time)
	return []byte("\"" + tt.Format(QueryDateTimeFormat) + "\""), nil
}

func (t *BleveQueryTime) UnmarshalJSON(data []byte) error {
	var timeString string
	err := json.Unmarshal(data, &timeString)
	if err != nil {
		return err
	}
	dateTimeParser, err := cache.DateTimeParserNamed(QueryDateTimeParser)
	if err != nil {
		return err
	}
	t.Time, _, err = dateTimeParser.ParseDateTime(timeString)
	if err != nil {
		return err
	}
	return nil
}

type DateRangeQuery struct {
	Start          BleveQueryTime `json:"start,omitempty"`
	End            BleveQueryTime `json:"end,omitempty"`
	InclusiveStart *bool          `json:"inclusive_start,omitempty"`
	InclusiveEnd   *bool          `json:"inclusive_end,omitempty"`
	FieldVal       string         `json:"field,omitempty"`
	BoostVal       *Boost         `json:"boost,omitempty"`
	InheritParser  bool           `json:"inherit_parser"`
	RawStart       string         `json:"raw_start,omitempty"`
	RawEnd         string         `json:"raw_end,omitempty"`
}

// UnmarshalJSON offers custom unmarshaling
func (q *DateRangeQuery) UnmarshalJSON(data []byte) error {
	var tmp map[string]json.RawMessage
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}
	// set defaults
	q.InheritParser = false

	for k, v := range tmp {
		switch k {
		case "inclusive_start":
			err := json.Unmarshal(v, &q.InclusiveStart)
			if err != nil {
				return err
			}
		case "inclusive_end":
			err := json.Unmarshal(v, &q.InclusiveEnd)
			if err != nil {
				return err
			}
		case "field":
			err := json.Unmarshal(v, &q.FieldVal)
			if err != nil {
				return err
			}
		case "boost":
			err := json.Unmarshal(v, &q.BoostVal)
			if err != nil {
				return err
			}
		case "inherit_parser":
			err := json.Unmarshal(v, &q.InheritParser)
			if err != nil {
				return err
			}

		}
	}
	if tmp["start"] != nil {
		if q.InheritParser {
			// inherit parser from index mapping
			err := json.Unmarshal(tmp["start"], &q.RawStart)
			if err != nil {
				return err
			}
		} else {
			// use QueryDateTimeParser
			err := json.Unmarshal(tmp["start"], &q.Start)
			if err != nil {
				return err
			}
		}
	}
	if tmp["end"] != nil {
		if q.InheritParser {
			// inherit parser from index mapping
			err := json.Unmarshal(tmp["end"], &q.RawEnd)
			if err != nil {
				return err
			}
		} else {
			// use QueryDateTimeParser
			err := json.Unmarshal(tmp["end"], &q.End)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// NewDateRangeQuery creates a new Query for ranges
// of date values.
// Date strings are parsed using the DateTimeParser configured in the
// top-level config.QueryDateTimeParser
// Either, but not both endpoints can be nil.
func NewDateRangeQuery(start, end time.Time) *DateRangeQuery {
	return NewDateRangeInclusiveQuery(start, end, nil, nil)
}

// NewDateRangeInclusiveQuery creates a new Query for ranges
// of date values.
// Date strings are parsed using the DateTimeParser configured in the
// top-level config.QueryDateTimeParser
// Either, but not both endpoints can be nil.
// startInclusive and endInclusive control inclusion of the endpoints.
func NewDateRangeInclusiveQuery(start, end time.Time, startInclusive, endInclusive *bool) *DateRangeQuery {
	return &DateRangeQuery{
		Start:          BleveQueryTime{start},
		End:            BleveQueryTime{end},
		InclusiveStart: startInclusive,
		InclusiveEnd:   endInclusive,
	}
}

// NewRawDateRangeQuery creates a new Query for ranges
// of date values.
// Date strings are not parsed, and are used as is.
// Either, but not both endpoints can be nil.
// Used when start and end must be parsed by deriving
// the parser from the index mapping for the queried field.
func NewDateRangeRawQuery(start, end string) *DateRangeQuery {
	return NewDateRangeRawInclusiveQuery(start, end, nil, nil)
}

// NewDateRangeInclusiveQuery creates a new Query for ranges
// of date values.
// Date strings are parsed using the DateTimeParser configured for the
// queried field in the index mapping.
// Either, but not both endpoints can be nil.
// startInclusive and endInclusive control inclusion of the endpoints.
func NewDateRangeRawInclusiveQuery(start, end string, startInclusive, endInclusive *bool) *DateRangeQuery {
	return &DateRangeQuery{
		RawStart:       start,
		RawEnd:         end,
		InclusiveStart: startInclusive,
		InclusiveEnd:   endInclusive,
		InheritParser:  true,
	}
}

func (q *DateRangeQuery) SetBoost(b float64) {
	boost := Boost(b)
	q.BoostVal = &boost
}

func (q *DateRangeQuery) Boost() float64 {
	return q.BoostVal.Value()
}

func (q *DateRangeQuery) SetField(f string) {
	q.FieldVal = f
}

func (q *DateRangeQuery) Field() string {
	return q.FieldVal
}

func (q *DateRangeQuery) SetInheritParser(i bool) {
	q.InheritParser = i
}

func (q *DateRangeQuery) Searcher(ctx context.Context, i index.IndexReader, m mapping.IndexMapping, options search.SearcherOptions) (search.Searcher, error) {
	field := q.FieldVal
	if q.FieldVal == "" {
		field = m.DefaultSearchField()
	}
	if q.InheritParser {
		var err error
		// inherit parser from index mapping
		dateTimeParserName := m.DatetimeParserNameForPath(field)
		dateTimeParser := m.DateTimeParserNamed(dateTimeParserName)
		if q.RawStart != "" {
			q.Start.Time, _, err = dateTimeParser.ParseDateTime(q.RawStart)
			if err != nil {
				return nil, fmt.Errorf("%v, date time parser name: %s", err, dateTimeParserName)
			}
		}
		if q.RawEnd != "" {
			q.End.Time, _, err = dateTimeParser.ParseDateTime(q.RawEnd)
			if err != nil {
				return nil, fmt.Errorf("%v, date time parser name: %s", err, dateTimeParserName)
			}
		}
	}
	min, max, err := q.parseEndpoints()
	if err != nil {
		return nil, err
	}
	return searcher.NewNumericRangeSearcher(ctx, i, min, max, q.InclusiveStart, q.InclusiveEnd, field, q.BoostVal.Value(), options)
}

func (q *DateRangeQuery) parseEndpoints() (*float64, *float64, error) {
	min := math.Inf(-1)
	max := math.Inf(1)

	if q.Start.IsZero() && q.End.IsZero() {
		return nil, nil, fmt.Errorf("date range query must specify at least one of start/end")
	}

	if !q.Start.IsZero() {
		if !isDatetimeCompatible(q.Start) {
			// overflow
			return nil, nil, fmt.Errorf("invalid/unsupported date range, start: %v", q.Start)
		}
		startInt64 := q.Start.UnixNano()
		min = numeric.Int64ToFloat64(startInt64)
	}
	if !q.End.IsZero() {
		if !isDatetimeCompatible(q.End) {
			// overflow
			return nil, nil, fmt.Errorf("invalid/unsupported date range, end: %v", q.End)
		}
		endInt64 := q.End.UnixNano()
		max = numeric.Int64ToFloat64(endInt64)
	}

	return &min, &max, nil
}

func (q *DateRangeQuery) Validate() error {
	// either start or end must be specified
	if q.Start.IsZero() && q.End.IsZero() {
		// if inherit parser is true, perform check if RawStart/RawEnd is specified
		if q.InheritParser {
			if q.RawStart == "" && q.RawEnd == "" {
				// Really invalid now
				return fmt.Errorf("date range query must specify at least one of start/end")
			}
		} else {
			return fmt.Errorf("date range query must specify at least one of start/end")
		}
	}
	_, _, err := q.parseEndpoints()
	if err != nil {
		return err
	}
	return nil
}

func isDatetimeCompatible(t BleveQueryTime) bool {
	if QueryDateTimeFormat == time.RFC3339 &&
		(t.Before(MinRFC3339CompatibleTime) || t.After(MaxRFC3339CompatibleTime)) {
		return false
	}

	return true
}
