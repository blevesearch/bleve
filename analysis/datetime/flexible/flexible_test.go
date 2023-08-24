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

package flexible

import (
	"reflect"
	"testing"
	"time"

	"github.com/blevesearch/bleve/v2/analysis"
)

func TestFlexibleDateTimeParser(t *testing.T) {
	testLocation := time.FixedZone("", -8*60*60)

	rfc3339NoTimezone := "2006-01-02T15:04:05"
	rfc3339NoTimezoneNoT := "2006-01-02 15:04:05"
	rfc3339NoTime := "2006-01-02"

	dateOptionalTimeParser := New(
		[]string{
			time.RFC3339Nano,
			time.RFC3339,
			rfc3339NoTimezone,
			rfc3339NoTimezoneNoT,
			rfc3339NoTime,
		})

	tests := []struct {
		input          string
		expectedTime   time.Time
		expectedLayout string
		expectedError  error
	}{
		{
			input:          "2014-08-03",
			expectedTime:   time.Date(2014, 8, 3, 0, 0, 0, 0, time.UTC),
			expectedLayout: rfc3339NoTime,
			expectedError:  nil,
		},
		{
			input:          "2014-08-03T15:59:30",
			expectedTime:   time.Date(2014, 8, 3, 15, 59, 30, 0, time.UTC),
			expectedLayout: rfc3339NoTimezone,
			expectedError:  nil,
		},
		{
			input:          "2014-08-03 15:59:30",
			expectedTime:   time.Date(2014, 8, 3, 15, 59, 30, 0, time.UTC),
			expectedLayout: rfc3339NoTimezoneNoT,
			expectedError:  nil,
		},
		{
			input:          "2014-08-03T15:59:30-08:00",
			expectedTime:   time.Date(2014, 8, 3, 15, 59, 30, 0, testLocation),
			expectedLayout: time.RFC3339Nano,
			expectedError:  nil,
		},
		{

			input:          "2014-08-03T15:59:30.999999999-08:00",
			expectedTime:   time.Date(2014, 8, 3, 15, 59, 30, 999999999, testLocation),
			expectedLayout: time.RFC3339Nano,
			expectedError:  nil,
		},
		{
			input:          "not a date time",
			expectedTime:   time.Time{},
			expectedLayout: "",
			expectedError:  analysis.ErrInvalidDateTime,
		},
	}

	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			actualTime, actualLayout, actualErr := dateOptionalTimeParser.ParseDateTime(test.input)
			if actualErr != test.expectedError {
				t.Fatalf("expected error %#v, got %#v", test.expectedError, actualErr)
			}
			if !reflect.DeepEqual(actualTime, test.expectedTime) {
				t.Errorf("expected time %v, got %v", test.expectedTime, actualTime)
			}
			if !reflect.DeepEqual(actualLayout, test.expectedLayout) {
				t.Errorf("expected layout %v, got %v", test.expectedLayout, actualLayout)
			}
		})
	}
}
