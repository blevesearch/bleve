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
	"strconv"
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
	rv, err := dateTimeParser.ParseDateTime(t)
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
	// called where we can use the default date time parser.
	var timeString string
	err := json.Unmarshal(data, &timeString)
	if err != nil {
		return err
	}
	dateTimeParser, err := cache.DateTimeParserNamed(QueryDateTimeParser)
	if err != nil {
		return err
	}
	t.Time, err = dateTimeParser.ParseDateTime(timeString)
	if err != nil {
		return err
	}
	return nil
}

func dateRangeUnmarshal(input []byte, obj *DateRangeQuery) error {
	// Only called in ParseQuery path, since we do not know the date time parser.
	var objmap map[string]interface{}
	err := json.Unmarshal(input, &objmap)
	if err != nil {
		return err
	}
	if objmap["start"] != nil {
		rawStart, canConvert := objmap["start"].(string)
		if !canConvert {
			return fmt.Errorf("invalid start")
		}
		obj.rawStart = rawStart
	}
	if objmap["end"] != nil {
		rawEnd, canConvert := objmap["end"].(string)
		if !canConvert {
			return fmt.Errorf("invalid end")
		}
		obj.rawEnd = rawEnd
	}
	if objmap["inclusive_start"] != nil {
		inclusiveStart, canConvert := objmap["inclusive_start"].(bool)
		if !canConvert {
			return fmt.Errorf("invalid inclusive_start")
		}
		obj.InclusiveStart = &inclusiveStart
	}
	if objmap["inclusive_end"] != nil {
		inclusiveEnd, canConvert := objmap["inclusive_end"].(bool)
		if !canConvert {
			return fmt.Errorf("invalid inclusive_end")
		}
		obj.InclusiveEnd = &inclusiveEnd
	}
	if objmap["boost"] != nil {
		boost, canConvert := objmap["boost"].(float64)
		if !canConvert {
			return fmt.Errorf("invalid boost")
		}
		boostVal := Boost(boost)
		obj.BoostVal = &boostVal
	}
	if objmap["field"] != nil {
		fieldVal, canConvert := objmap["field"].(string)
		if !canConvert {
			return fmt.Errorf("invalid field")
		}
		obj.FieldVal = fieldVal
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
	rawStart       string         `json:"-"`
	rawEnd         string         `json:"-"`
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

func (q *DateRangeQuery) Searcher(ctx context.Context, i index.IndexReader, m mapping.IndexMapping, options search.SearcherOptions) (search.Searcher, error) {
	field := q.FieldVal
	if q.FieldVal == "" {
		field = m.DefaultSearchField()
	}
	if q.rawStart != "" || q.rawEnd != "" {
		// ParseQuery path since at least one of rawStart and rawEnd is not empty
		// parse rawStart and rawEnd to time.Time objects
		var err error
		dateTimeParserName := m.DatetimeParserNameForPath(field)
		bounds, isUnixFormat := analysis.UnixTimestampFormats[dateTimeParserName]
		if isUnixFormat {
			// unix timestamp format
			// rawStart and rawEnd are timestamps so do not parse them
			min := bounds.Min
			max := bounds.Max
			if q.rawStart != "" {
				min, err = strconv.ParseInt(q.rawStart, 10, 64)
				if err != nil {
					return nil, fmt.Errorf("invalid start: %v", err)
				}
				min, err = analysis.ValidateAndConvertTimestamp(min, bounds, dateTimeParserName)
				if err != nil {
					return nil, fmt.Errorf("invalid start: %v", err)
				}
			}
			if q.rawEnd != "" {
				max, err = strconv.ParseInt(q.rawEnd, 10, 64)
				if err != nil {
					return nil, fmt.Errorf("invalid end: %v", err)
				}
				max, err = analysis.ValidateAndConvertTimestamp(max, bounds, dateTimeParserName)
				if err != nil {
					return nil, fmt.Errorf("invalid end: %v", err)
				}
			}
			minFloat := numeric.Int64ToFloat64(min)
			maxFloat := numeric.Int64ToFloat64(max)
			return searcher.NewNumericRangeSearcher(ctx, i, &minFloat, &maxFloat, q.InclusiveStart, q.InclusiveEnd, field, q.BoostVal.Value(), options)
		}
		dateTimeParser := m.DateTimeParserNamed(dateTimeParserName)
		if q.rawStart != "" {
			q.Start.Time, err = dateTimeParser.ParseDateTime(q.rawStart)
			if err != nil {
				return nil, fmt.Errorf("%v, date time parser name: %s", err, dateTimeParserName)
			}
		}
		if q.rawEnd != "" {
			q.End.Time, err = dateTimeParser.ParseDateTime(q.rawEnd)
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
	if q.Start.IsZero() && q.End.IsZero() {
		// Test for ParseQuery path
		if q.rawStart == "" && q.rawEnd == "" {
			// Really invalid now
			return fmt.Errorf("date range query must specify at least one of start/end")
		}
	}
	if !q.Start.IsZero() || !q.End.IsZero() {
		// dateRangeUnmarshal not used
		_, _, err := q.parseEndpoints()
		if err != nil {
			return err
		}
	}
	// Do not validate endpoints for ParseQuery path since we do not know the date time parser
	// Instead validate in the Searcher, where we get the index mapping.
	return nil
}

func isDatetimeCompatible(t BleveQueryTime) bool {
	if QueryDateTimeFormat == time.RFC3339 &&
		(t.Before(MinRFC3339CompatibleTime) || t.After(MaxRFC3339CompatibleTime)) {
		return false
	}

	return true
}
