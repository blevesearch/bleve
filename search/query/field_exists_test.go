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
	"reflect"
	"testing"
)

func TestFieldExistsQuery(t *testing.T) {
	q := NewFieldExistsQuery("foo")
	if q.Field() != "foo" {
		t.Errorf("expected field 'foo', got '%s'", q.Field())
	}

	q.SetField("bar")
	if q.Field() != "bar" {
		t.Errorf("expected field 'bar', got '%s'", q.Field())
	}

	q.SetBoost(2.0)
	if q.Boost() != 2.0 {
		t.Errorf("expected boost 2.0, got %f", q.Boost())
	}
}

func TestFieldExistsQueryJSON(t *testing.T) {
	tests := []struct {
		input []byte
		want  Query
	}{
		{
			input: []byte(`{"field_exists": "name"}`),
			want:  NewFieldExistsQuery("name"),
		},
		{
			input: []byte(`{"field_exists": "email", "boost": 2.0}`),
			want: func() *FieldExistsQuery {
				q := NewFieldExistsQuery("email")
				q.SetBoost(2.0)
				return q
			}(),
		},
	}

	for _, test := range tests {
		got, err := ParseQuery(test.input)
		if err != nil {
			t.Fatalf("unexpected error parsing query: %v", err)
		}
		if !reflect.DeepEqual(got, test.want) {
			t.Errorf("expected %#v, got %#v", test.want, got)
		}
	}
}
