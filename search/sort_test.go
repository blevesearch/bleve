package search

import (
	"reflect"
	"testing"
)

func TestParseSearchSortObj(t *testing.T) {
	tests := []struct {
		name    string
		input   map[string]interface{}
		want    SearchSort
		wantErr bool
	}{
		{
			name: "sort by id",
			input: map[string]interface{}{
				"by":   "id",
				"desc": false,
			},
			want: &SortDocID{
				Desc: false,
			},
			wantErr: false,
		},
		{
			name: "sort by id descending",
			input: map[string]interface{}{
				"by":   "id",
				"desc": true,
			},
			want: &SortDocID{
				Desc: true,
			},
			wantErr: false,
		},
		{
			name: "sort by score",
			input: map[string]interface{}{
				"by":   "score",
				"desc": false,
			},
			want: &SortScore{
				Desc: false,
			},
			wantErr: false,
		},
		{
			name: "sort by score descending",
			input: map[string]interface{}{
				"by":   "score",
				"desc": true,
			},
			want: &SortScore{
				Desc: true,
			},
			wantErr: false,
		},
		{
			name: "sort by geo_distance",
			input: map[string]interface{}{
				"by":    "geo_distance",
				"field": "location",
				"location": map[string]interface{}{
					"lon": 1.0,
					"lat": 2.0,
				},
				"unit": "km",
				"desc": false,
			},
			want: &SortGeoDistance{
				Field:    "location",
				Desc:     false,
				Lon:      1.0,
				Lat:      2.0,
				Unit:     "km",
				unitMult: 1000.0,
			},
			wantErr: false,
		},
		{
			name: "sort by field",
			input: map[string]interface{}{
				"by":      "field",
				"field":   "name",
				"desc":    false,
				"type":    "auto",
				"mode":    "default",
				"missing": "last",
			},
			want: &SortField{
				Field:   "name",
				Desc:    false,
				Type:    SortFieldAuto,
				Mode:    SortFieldDefault,
				Missing: SortFieldMissingLast,
			},
			wantErr: false,
		},
		{
			name: "sort by field with missing",
			input: map[string]interface{}{
				"by":      "field",
				"field":   "name",
				"desc":    false,
				"type":    "auto",
				"mode":    "default",
				"missing": "first",
			},
			want: &SortField{
				Field:   "name",
				Desc:    false,
				Type:    SortFieldAuto,
				Mode:    SortFieldDefault,
				Missing: SortFieldMissingFirst,
			},
			wantErr: false,
		},
		{
			name: "sort by field descending",
			input: map[string]interface{}{
				"by":      "field",
				"field":   "name",
				"desc":    true,
				"type":    "string",
				"mode":    "min",
				"missing": "first",
			},
			want: &SortField{
				Field:   "name",
				Desc:    true,
				Type:    SortFieldAsString,
				Mode:    SortFieldMin,
				Missing: SortFieldMissingFirst,
			},
			wantErr: false,
		},
		{
			name: "missing by",
			input: map[string]interface{}{
				"desc": true,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "unknown by",
			input: map[string]interface{}{
				"by": "unknown",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "missing field for geo_distance",
			input: map[string]interface{}{
				"by": "geo_distance",
				"location": map[string]interface{}{
					"lon": 1.0,
					"lat": 2.0,
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "missing location for geo_distance",
			input: map[string]interface{}{
				"by":    "geo_distance",
				"field": "location",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "invalid unit for geo_distance",
			input: map[string]interface{}{
				"by":    "geo_distance",
				"field": "location",
				"location": map[string]interface{}{
					"lon": 1.0,
					"lat": 2.0,
				},
				"unit": "invalid",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "missing field for field sort",
			input: map[string]interface{}{
				"by": "field",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "unknown type for field sort",
			input: map[string]interface{}{
				"by":    "field",
				"field": "name",
				"type":  "unknown",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "number type for field sort with desc",
			input: map[string]interface{}{
				"by":      "field",
				"field":   "name",
				"type":    "number",
				"mode":    "default",
				"desc":    true,
				"missing": "last",
			},
			want: &SortField{
				Field:   "name",
				Desc:    true,
				Type:    SortFieldAsNumber,
				Mode:    SortFieldDefault,
				Missing: SortFieldMissingLast,
			},
			wantErr: false,
		},
		{
			name: "date type for field sort with desc",
			input: map[string]interface{}{
				"by":      "field",
				"field":   "name",
				"type":    "date",
				"mode":    "default",
				"desc":    true,
				"missing": "last",
			},
			want: &SortField{
				Field:   "name",
				Desc:    true,
				Type:    SortFieldAsDate,
				Mode:    SortFieldDefault,
				Missing: SortFieldMissingLast,
			},
			wantErr: false,
		},
		{
			name: "unknown type for field sort with missing",
			input: map[string]interface{}{
				"by":      "field",
				"field":   "name",
				"type":    "unknown",
				"mode":    "default",
				"missing": "last",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "unknown mode for field sort",
			input: map[string]interface{}{
				"by":    "field",
				"field": "name",
				"mode":  "unknown",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "default mode for field sort",
			input: map[string]interface{}{
				"by":    "field",
				"field": "name",
				"mode":  "default",
			},
			want: &SortField{
				Field:   "name",
				Desc:    false,
				Type:    SortFieldAuto,
				Mode:    SortFieldDefault,
				Missing: SortFieldMissingLast,
			},
			wantErr: false,
		},
		{
			name: "max mode for field sort",
			input: map[string]interface{}{
				"by":    "field",
				"field": "name",
				"mode":  "max",
			},
			want: &SortField{
				Field:   "name",
				Desc:    false,
				Type:    SortFieldAuto,
				Mode:    SortFieldMax,
				Missing: SortFieldMissingLast,
			},
			wantErr: false,
		},
		{
			name: "min mode for field sort",
			input: map[string]interface{}{
				"by":    "field",
				"field": "name",
				"mode":  "min",
			},
			want: &SortField{
				Field:   "name",
				Desc:    false,
				Type:    SortFieldAuto,
				Mode:    SortFieldMin,
				Missing: SortFieldMissingLast,
			},
			wantErr: false,
		},
		{
			name: "unknown missing for field sort",
			input: map[string]interface{}{
				"by":      "field",
				"field":   "name",
				"missing": "unknown",
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseSearchSortObj(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseSearchSortObj() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseSearchSortObj() = %v, want %v", got, tt.want)
			}
		})
	}
}
