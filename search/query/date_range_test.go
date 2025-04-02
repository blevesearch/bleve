//  Copyright (c) 2016 Couchbase, Inc.
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
	"encoding/json"
	"testing"
	"time"
)

func TestBleveQueryTime(t *testing.T) {
	testTimes := []time.Time{
		time.Now(),
		{},
	}

	for i, testTime := range testTimes {
		bqt := &BleveQueryTime{testTime}

		buf, err := json.Marshal(bqt)
		if err != nil {
			t.Errorf("expected no err")
		}

		var bqt2 BleveQueryTime
		err = json.Unmarshal(buf, &bqt2)
		if err != nil {
			t.Errorf("expected no unmarshal err, got: %v", err)
		}

		if bqt.Time.Format(time.RFC3339) != bqt2.Time.Format(time.RFC3339) {
			t.Errorf("test %d - expected same time, %#v != %#v", i, bqt.Time, bqt2.Time)
		}

		if testTime.Format(time.RFC3339) != bqt2.Time.Format(time.RFC3339) {
			t.Errorf("test %d - expected orig time, %#v != %#v", i, testTime, bqt2.Time)
		}
	}
}

func TestValidateDatetimeRanges(t *testing.T) {
	tests := []struct {
		start  string
		end    string
		expect bool
	}{
		{
			start:  "2019-03-22T13:25:00Z",
			end:    "2019-03-22T18:25:00Z",
			expect: true,
		},
		{
			start:  "2019-03-22T13:25:00Z",
			end:    "9999-03-22T13:25:00Z",
			expect: false,
		},
		{
			start:  "2019-03-22T13:25:00Z",
			end:    "2262-04-11T11:59:59Z",
			expect: true,
		},
		{
			start:  "2019-03-22T13:25:00Z",
			end:    "2262-04-12T00:00:00Z",
			expect: false,
		},
		{
			start:  "1950-03-22T12:23:23Z",
			end:    "1960-02-21T15:23:34Z",
			expect: true,
		},
		{
			start:  "0001-01-01T00:00:00Z",
			end:    "0001-01-01T00:00:00Z",
			expect: false,
		},
		{
			start:  "0001-01-01T00:00:00Z",
			end:    "2000-01-01T00:00:00Z",
			expect: true,
		},
		{
			start:  "1677-11-30T11:59:59Z",
			end:    "2262-04-11T11:59:59Z",
			expect: false,
		},
		{
			start:  "2262-04-12T00:00:00Z",
			end:    "2262-04-11T11:59:59Z",
			expect: false,
		},
		{
			start:  "1677-12-01T00:00:00Z",
			end:    "2262-04-12T00:00:00Z",
			expect: false,
		},
		{
			start:  "1677-12-01T00:00:00Z",
			end:    "1677-11-30T11:59:59Z",
			expect: false,
		},
		{
			start:  "1677-12-01T00:00:00Z",
			end:    "2262-04-11T11:59:59Z",
			expect: true,
		},
	}

	for _, test := range tests {
		startTime, _ := time.Parse(time.RFC3339, test.start)
		endTime, _ := time.Parse(time.RFC3339, test.end)

		dateRangeQuery := NewDateRangeQuery(startTime, endTime)
		if (dateRangeQuery.Validate() == nil) != test.expect {
			t.Errorf("unexpected results while validating date range query with"+
				" {start: %v, end: %v}, expected: %v",
				test.start, test.end, test.expect)
		}
	}
}
