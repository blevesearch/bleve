//  Copyright (c) 2025 Couchbase, Inc.
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

package bleve

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/blevesearch/bleve/v2/analysis/lang/en"
	"github.com/blevesearch/bleve/v2/index/scorch"
	"github.com/blevesearch/bleve/v2/index/scorch/mergeplan"
	"github.com/blevesearch/bleve/v2/mapping"
	index "github.com/blevesearch/bleve_index_api"
)

func TestCompareFieldMapping(t *testing.T) {
	tests := []struct {
		original       *mapping.FieldMapping
		updated        *mapping.FieldMapping
		defaultChanges *defaultInfo
		indexFieldInfo *index.UpdateFieldInfo
		changed        bool
		err            bool
	}{
		{ // both nil => no op
			original:       nil,
			updated:        nil,
			defaultChanges: nil,
			indexFieldInfo: nil,
			changed:        false,
			err:            false,
		},
		{ // updated nil => delete all
			original: &mapping.FieldMapping{},
			updated:  nil,
			defaultChanges: &defaultInfo{
				analyzer:       false,
				dateTimeParser: false,
				synonymSource:  false,
			},
			indexFieldInfo: &index.UpdateFieldInfo{
				RemoveAll: true,
			},
			changed: true,
			err:     false,
		},
		{ // type changed => not updatable
			original: &mapping.FieldMapping{
				Type: "text",
			},
			updated: &mapping.FieldMapping{
				Type: "datetime",
			},
			defaultChanges: &defaultInfo{
				analyzer:       false,
				dateTimeParser: false,
				synonymSource:  false,
			},
			indexFieldInfo: nil,
			changed:        false,
			err:            true,
		},
		{ // synonym source changed for text => not updatable
			original: &mapping.FieldMapping{
				Type:          "text",
				SynonymSource: "a",
			},
			updated: &mapping.FieldMapping{
				Type:          "text",
				SynonymSource: "b",
			},
			defaultChanges: &defaultInfo{
				analyzer:       false,
				dateTimeParser: false,
				synonymSource:  false,
			},
			indexFieldInfo: nil,
			changed:        false,
			err:            true,
		},
		{ // analyser changed for text => not updatable
			original: &mapping.FieldMapping{
				Type:     "text",
				Analyzer: "a",
			},
			updated: &mapping.FieldMapping{
				Type:     "text",
				Analyzer: "b",
			},
			defaultChanges: &defaultInfo{
				analyzer:       false,
				dateTimeParser: false,
				synonymSource:  false,
			},
			indexFieldInfo: nil,
			changed:        false,
			err:            true,
		},
		{ // default analyser changed when inherited => not updatable
			original: &mapping.FieldMapping{
				Type:     "text",
				Analyzer: "inherit",
			},
			updated: &mapping.FieldMapping{
				Type:     "text",
				Analyzer: "inherit",
			},
			defaultChanges: &defaultInfo{
				analyzer:       true,
				dateTimeParser: false,
				synonymSource:  false,
			},
			indexFieldInfo: nil,
			changed:        false,
			err:            true,
		},
		{ // default datetimeparser changed for inherited datetime field => not updatable
			original: &mapping.FieldMapping{
				Type:       "datetime",
				DateFormat: "inherit",
			},
			updated: &mapping.FieldMapping{
				Type:       "datetime",
				DateFormat: "inherit",
			},
			defaultChanges: &defaultInfo{
				analyzer:       false,
				dateTimeParser: true,
				synonymSource:  false,
			},
			indexFieldInfo: nil,
			changed:        false,
			err:            true,
		},
		{ // dims changed for vector => not updatable
			original: &mapping.FieldMapping{
				Type:                    "vector",
				Dims:                    128,
				Similarity:              "l2_norm",
				VectorIndexOptimizedFor: "memory-efficient",
			},
			updated: &mapping.FieldMapping{
				Type:                    "vector",
				Dims:                    1024,
				Similarity:              "l2_norm",
				VectorIndexOptimizedFor: "memory-efficient",
			},
			defaultChanges: &defaultInfo{
				analyzer:       false,
				dateTimeParser: false,
				synonymSource:  false,
			},
			indexFieldInfo: nil,
			changed:        false,
			err:            true,
		},
		{ // similarity changed for vectorbase64 => not updatable
			original: &mapping.FieldMapping{
				Type:                    "vector_base64",
				Similarity:              "l2_norm",
				Dims:                    128,
				VectorIndexOptimizedFor: "memory-efficient",
			},
			updated: &mapping.FieldMapping{
				Type:                    "vector_base64",
				Similarity:              "dot_product",
				Dims:                    128,
				VectorIndexOptimizedFor: "memory-efficient",
			},
			defaultChanges: &defaultInfo{
				analyzer:       false,
				dateTimeParser: false,
				synonymSource:  false,
			},
			indexFieldInfo: nil,
			changed:        false,
			err:            true,
		},
		{ // vectorindexoptimizedfor chagned for vector => not updatable
			original: &mapping.FieldMapping{
				Type:                    "vector",
				Similarity:              "dot_product",
				Dims:                    128,
				VectorIndexOptimizedFor: "memory-efficient",
			},
			updated: &mapping.FieldMapping{
				Type:                    "vector",
				Similarity:              "dot_product",
				Dims:                    128,
				VectorIndexOptimizedFor: "latency",
			},
			defaultChanges: &defaultInfo{
				analyzer:       false,
				dateTimeParser: false,
				synonymSource:  false,
			},
			indexFieldInfo: nil,
			changed:        false,
			err:            true,
		},
		{ // includeinall changed => not updatable
			original: &mapping.FieldMapping{
				Type:         "numeric",
				IncludeInAll: true,
			},
			updated: &mapping.FieldMapping{
				Type:         "numeric",
				IncludeInAll: false,
			},
			defaultChanges: &defaultInfo{
				analyzer:       false,
				dateTimeParser: false,
				synonymSource:  false,
			},
			indexFieldInfo: nil,
			changed:        false,
			err:            true,
		},
		{ //includetermvectors changed => not updatable
			original: &mapping.FieldMapping{
				Type:               "numeric",
				IncludeTermVectors: false,
			},
			updated: &mapping.FieldMapping{
				Type:               "numeric",
				IncludeTermVectors: true,
			},
			defaultChanges: &defaultInfo{
				analyzer:       false,
				dateTimeParser: false,
				synonymSource:  false,
			},
			indexFieldInfo: nil,
			changed:        false,
			err:            true,
		},
		{ // store changed after all checks => updatable with store delete
			original: &mapping.FieldMapping{
				Type:         "numeric",
				SkipFreqNorm: true,
			},
			updated: &mapping.FieldMapping{
				Type:         "numeric",
				SkipFreqNorm: false,
			},
			defaultChanges: &defaultInfo{
				analyzer:       false,
				dateTimeParser: false,
				synonymSource:  false,
			},
			indexFieldInfo: nil,
			changed:        false,
			err:            true,
		},
		{ // index changed after all checks => updatable with index and docvalues delete
			original: &mapping.FieldMapping{
				Type:  "geopoint",
				Index: true,
			},
			updated: &mapping.FieldMapping{
				Type:  "geopoint",
				Index: false,
			},
			defaultChanges: &defaultInfo{
				analyzer:       false,
				dateTimeParser: false,
				synonymSource:  false,
			},
			indexFieldInfo: &index.UpdateFieldInfo{
				Index:     true,
				DocValues: true,
			},
			changed: true,
			err:     false,
		},
		{ // docvalues changed after all checks => docvalues delete
			original: &mapping.FieldMapping{
				Type:      "numeric",
				DocValues: true,
			},
			updated: &mapping.FieldMapping{
				Type:      "numeric",
				DocValues: false,
			},
			defaultChanges: &defaultInfo{
				analyzer:       false,
				dateTimeParser: false,
				synonymSource:  false,
			},
			indexFieldInfo: &index.UpdateFieldInfo{
				DocValues: true,
			},
			changed: true,
			err:     false,
		},
		{ // no relavent changes => continue but no op
			original: &mapping.FieldMapping{
				Name:                    "",
				Type:                    "datetime",
				Analyzer:                "a",
				Store:                   true,
				Index:                   false,
				IncludeTermVectors:      true,
				IncludeInAll:            false,
				DateFormat:              "a",
				DocValues:               false,
				SkipFreqNorm:            true,
				Dims:                    128,
				Similarity:              "dot_product",
				VectorIndexOptimizedFor: "memory-efficient",
				SynonymSource:           "a",
			},
			updated: &mapping.FieldMapping{
				Name:                    "",
				Type:                    "datetime",
				Analyzer:                "b",
				Store:                   true,
				Index:                   false,
				IncludeTermVectors:      true,
				IncludeInAll:            false,
				DateFormat:              "a",
				DocValues:               false,
				SkipFreqNorm:            true,
				Dims:                    256,
				Similarity:              "l2_norm",
				VectorIndexOptimizedFor: "latency",
				SynonymSource:           "b",
			},
			defaultChanges: &defaultInfo{
				analyzer:       false,
				dateTimeParser: false,
				synonymSource:  false,
			},
			indexFieldInfo: &index.UpdateFieldInfo{},
			changed:        false,
			err:            false,
		},
	}

	for i, test := range tests {
		rv, changed, err := compareFieldMapping(test.original, test.updated, test.defaultChanges)

		if err == nil && test.err || err != nil && !test.err {
			t.Errorf("Unexpected error value for test %d, expecting %t, got %v\n", i, test.err, err)
		}
		if changed != test.changed {
			t.Errorf("Unexpected changed value for test %d, expecting %t, got %t, err %v\n", i, test.changed, changed, err)
		}
		if rv == nil && test.indexFieldInfo != nil || rv != nil && test.indexFieldInfo == nil || !reflect.DeepEqual(rv, test.indexFieldInfo) {
			t.Errorf("Unexpected index field info value for test %d, expecting %+v, got %+v, err %v", i, test.indexFieldInfo, rv, err)
		}
	}
}

func TestCompareMappings(t *testing.T) {
	tests := []struct {
		original *mapping.IndexMappingImpl
		updated  *mapping.IndexMappingImpl
		info     *defaultInfo
		err      bool
	}{
		{ // changed type field when non empty mappings are present => error
			original: &mapping.IndexMappingImpl{
				TypeField: "a",
				TypeMapping: map[string]*mapping.DocumentMapping{
					"a": {},
					"b": {},
				},
			},
			updated: &mapping.IndexMappingImpl{
				TypeField: "b",
				TypeMapping: map[string]*mapping.DocumentMapping{
					"a": {},
					"b": {},
				},
			},
			info: nil,
			err:  true,
		},
		{ // changed default type => error
			original: &mapping.IndexMappingImpl{
				DefaultType: "a",
			},
			updated: &mapping.IndexMappingImpl{
				DefaultType: "b",
			},
			info: nil,
			err:  true,
		},
		{ // changed default analyzer => analyser true
			original: &mapping.IndexMappingImpl{
				DefaultAnalyzer: "a",
			},
			updated: &mapping.IndexMappingImpl{
				DefaultAnalyzer: "b",
			},
			info: &defaultInfo{
				analyzer:       true,
				dateTimeParser: false,
				synonymSource:  false,
			},
			err: false,
		},
		{ // changed default datetimeparser => datetimeparser true
			original: &mapping.IndexMappingImpl{
				DefaultDateTimeParser: "a",
			},
			updated: &mapping.IndexMappingImpl{
				DefaultDateTimeParser: "b",
			},
			info: &defaultInfo{
				analyzer:       false,
				dateTimeParser: true,
				synonymSource:  false,
			},
			err: false,
		},
		{ // changed default synonym source => synonym source true
			original: &mapping.IndexMappingImpl{
				DefaultSynonymSource: "a",
			},
			updated: &mapping.IndexMappingImpl{
				DefaultSynonymSource: "b",
			},
			info: &defaultInfo{
				analyzer:       false,
				dateTimeParser: false,
				synonymSource:  true,
			},
			err: false,
		},
		{ // changed default field => error
			original: &mapping.IndexMappingImpl{
				DefaultField: "a",
			},
			updated: &mapping.IndexMappingImpl{
				DefaultField: "b",
			},
			info: nil,
			err:  true,
		},
		{ // changed index dynamic => error
			original: &mapping.IndexMappingImpl{
				IndexDynamic: true,
			},
			updated: &mapping.IndexMappingImpl{
				IndexDynamic: false,
			},
			info: nil,
			err:  true,
		},
		{ // changed store dynamic => error
			original: &mapping.IndexMappingImpl{
				StoreDynamic: false,
			},
			updated: &mapping.IndexMappingImpl{
				StoreDynamic: true,
			},
			info: nil,
			err:  true,
		},
		{ // changed docvalues dynamic => error
			original: &mapping.IndexMappingImpl{
				DocValuesDynamic: true,
			},
			updated: &mapping.IndexMappingImpl{
				DocValuesDynamic: false,
			},
			info: nil,
			err:  true,
		},
	}

	for i, test := range tests {
		info, err := compareMappings(test.original, test.updated)

		if err == nil && test.err || err != nil && !test.err {
			t.Errorf("Unexpected error value for test %d, expecting %t, got %v\n", i, test.err, err)
		}
		if info == nil && test.info != nil || info != nil && test.info == nil || !reflect.DeepEqual(info, test.info) {
			t.Errorf("Unexpected default info value for test %d, expecting %+v, got %+v, err %v", i, test.info, info, err)
		}
	}
}

func TestDeletedFields(t *testing.T) {
	tests := []struct {
		original  *mapping.IndexMappingImpl
		updated   *mapping.IndexMappingImpl
		fieldInfo map[string]*index.UpdateFieldInfo
		err       bool
	}{
		{
			// no change between original and updated having type and default mapping
			// => empty fieldInfo with no error
			original: &mapping.IndexMappingImpl{
				TypeMapping: map[string]*mapping.DocumentMapping{
					"map1": {
						Enabled: true,
						Dynamic: false,
						Properties: map[string]*mapping.DocumentMapping{
							"a": {
								Enabled:    true,
								Dynamic:    false,
								Properties: map[string]*mapping.DocumentMapping{},
								Fields: []*mapping.FieldMapping{
									{
										Type:  "numeric",
										Index: true,
									},
								},
								DefaultAnalyzer:      "",
								DefaultSynonymSource: "",
							},
						},
						Fields:               []*mapping.FieldMapping{},
						DefaultAnalyzer:      "",
						DefaultSynonymSource: "",
					},
					"map2": {
						Enabled: true,
						Dynamic: false,
						Properties: map[string]*mapping.DocumentMapping{
							"b": {
								Enabled:    true,
								Dynamic:    false,
								Properties: map[string]*mapping.DocumentMapping{},
								Fields: []*mapping.FieldMapping{
									{
										Type:  "numeric",
										Index: true,
									},
								},
								DefaultAnalyzer:      "",
								DefaultSynonymSource: "",
							},
						},
						Fields:               []*mapping.FieldMapping{},
						DefaultAnalyzer:      "",
						DefaultSynonymSource: "",
					},
				},
				DefaultMapping: &mapping.DocumentMapping{
					Enabled: true,
					Dynamic: false,
					Properties: map[string]*mapping.DocumentMapping{
						"c": {
							Enabled:    true,
							Dynamic:    false,
							Properties: map[string]*mapping.DocumentMapping{},
							Fields: []*mapping.FieldMapping{
								{
									Type:  "numeric",
									Index: true,
								},
							},
							DefaultAnalyzer:      "",
							DefaultSynonymSource: "",
						},
					},
					Fields:               []*mapping.FieldMapping{},
					DefaultAnalyzer:      "",
					DefaultSynonymSource: "",
				},
				IndexDynamic:     false,
				StoreDynamic:     false,
				DocValuesDynamic: false,
			},
			updated: &mapping.IndexMappingImpl{
				TypeMapping: map[string]*mapping.DocumentMapping{
					"map1": {
						Enabled: true,
						Dynamic: false,
						Properties: map[string]*mapping.DocumentMapping{
							"a": {
								Enabled:    true,
								Dynamic:    false,
								Properties: map[string]*mapping.DocumentMapping{},
								Fields: []*mapping.FieldMapping{
									{
										Type:  "numeric",
										Index: true,
									},
								},
								DefaultAnalyzer:      "",
								DefaultSynonymSource: "",
							},
						},
						Fields:               []*mapping.FieldMapping{},
						DefaultAnalyzer:      "",
						DefaultSynonymSource: "",
					},
					"map2": {
						Enabled: true,
						Dynamic: false,
						Properties: map[string]*mapping.DocumentMapping{
							"b": {
								Enabled:    true,
								Dynamic:    false,
								Properties: map[string]*mapping.DocumentMapping{},
								Fields: []*mapping.FieldMapping{
									{
										Type:  "numeric",
										Index: true,
									},
								},
								DefaultAnalyzer:      "",
								DefaultSynonymSource: "",
							},
						},
						Fields:               []*mapping.FieldMapping{},
						DefaultAnalyzer:      "",
						DefaultSynonymSource: "",
					},
				},
				DefaultMapping: &mapping.DocumentMapping{
					Enabled: true,
					Dynamic: false,
					Properties: map[string]*mapping.DocumentMapping{
						"c": {
							Enabled:    true,
							Dynamic:    false,
							Properties: map[string]*mapping.DocumentMapping{},
							Fields: []*mapping.FieldMapping{
								{
									Type:  "numeric",
									Index: true,
								},
							},
							DefaultAnalyzer:      "",
							DefaultSynonymSource: "",
						},
					},
					Fields:               []*mapping.FieldMapping{},
					DefaultAnalyzer:      "",
					DefaultSynonymSource: "",
				},
				IndexDynamic:     false,
				StoreDynamic:     false,
				DocValuesDynamic: false,
			},
			fieldInfo: map[string]*index.UpdateFieldInfo{},
			err:       false,
		},
		{
			// no changes in type mappings and default mapping disabled with changes
			// => empty fieldInfo with no error
			original: &mapping.IndexMappingImpl{
				TypeMapping: map[string]*mapping.DocumentMapping{
					"map1": {
						Enabled: true,
						Dynamic: false,
						Properties: map[string]*mapping.DocumentMapping{
							"a": {
								Enabled:    true,
								Dynamic:    false,
								Properties: map[string]*mapping.DocumentMapping{},
								Fields: []*mapping.FieldMapping{
									{
										Type:  "numeric",
										Index: true,
									},
								},
								DefaultAnalyzer:      "",
								DefaultSynonymSource: "",
							},
						},
						Fields:               []*mapping.FieldMapping{},
						DefaultAnalyzer:      "",
						DefaultSynonymSource: "",
					},
					"map2": {
						Enabled: true,
						Dynamic: false,
						Properties: map[string]*mapping.DocumentMapping{
							"b": {
								Enabled:    true,
								Dynamic:    false,
								Properties: map[string]*mapping.DocumentMapping{},
								Fields: []*mapping.FieldMapping{
									{
										Type:  "numeric",
										Index: true,
									},
								},
								DefaultAnalyzer:      "",
								DefaultSynonymSource: "",
							},
						},
						Fields:               []*mapping.FieldMapping{},
						DefaultAnalyzer:      "",
						DefaultSynonymSource: "",
					},
				},
				DefaultMapping: &mapping.DocumentMapping{
					Enabled: false,
					Dynamic: false,
					Properties: map[string]*mapping.DocumentMapping{
						"c": {
							Enabled:    true,
							Dynamic:    false,
							Properties: map[string]*mapping.DocumentMapping{},
							Fields: []*mapping.FieldMapping{
								{
									Type:  "numeric",
									Index: true,
								},
							},
							DefaultAnalyzer:      "",
							DefaultSynonymSource: "",
						},
					},
					Fields:               []*mapping.FieldMapping{},
					DefaultAnalyzer:      "",
					DefaultSynonymSource: "",
				},
				IndexDynamic:     false,
				StoreDynamic:     false,
				DocValuesDynamic: false,
			},
			updated: &mapping.IndexMappingImpl{
				TypeMapping: map[string]*mapping.DocumentMapping{
					"map1": {
						Enabled: true,
						Dynamic: false,
						Properties: map[string]*mapping.DocumentMapping{
							"a": {
								Enabled:    true,
								Dynamic:    false,
								Properties: map[string]*mapping.DocumentMapping{},
								Fields: []*mapping.FieldMapping{
									{
										Type:  "numeric",
										Index: true,
									},
								},
								DefaultAnalyzer:      "",
								DefaultSynonymSource: "",
							},
						},
						Fields:               []*mapping.FieldMapping{},
						DefaultAnalyzer:      "",
						DefaultSynonymSource: "",
					},
					"map2": {
						Enabled: true,
						Dynamic: false,
						Properties: map[string]*mapping.DocumentMapping{
							"b": {
								Enabled:    true,
								Dynamic:    false,
								Properties: map[string]*mapping.DocumentMapping{},
								Fields: []*mapping.FieldMapping{
									{
										Type:  "numeric",
										Index: true,
									},
								},
								DefaultAnalyzer:      "",
								DefaultSynonymSource: "",
							},
						},
						Fields:               []*mapping.FieldMapping{},
						DefaultAnalyzer:      "",
						DefaultSynonymSource: "",
					},
				},
				DefaultMapping: &mapping.DocumentMapping{
					Enabled: false,
					Dynamic: false,
					Properties: map[string]*mapping.DocumentMapping{
						"d": {
							Enabled:    true,
							Dynamic:    false,
							Properties: map[string]*mapping.DocumentMapping{},
							Fields: []*mapping.FieldMapping{
								{
									Type:  "numeric",
									Index: true,
								},
							},
							DefaultAnalyzer:      "",
							DefaultSynonymSource: "",
						},
					},
					Fields:               []*mapping.FieldMapping{},
					DefaultAnalyzer:      "",
					DefaultSynonymSource: "",
				},
				IndexDynamic:     false,
				StoreDynamic:     false,
				DocValuesDynamic: false,
			},
			fieldInfo: map[string]*index.UpdateFieldInfo{},
			err:       false,
		},
		{
			// new type mappings in updated => error
			original: &mapping.IndexMappingImpl{
				TypeMapping: map[string]*mapping.DocumentMapping{
					"map1": {
						Enabled: true,
						Dynamic: false,
						Properties: map[string]*mapping.DocumentMapping{
							"a": {
								Enabled:    true,
								Dynamic:    false,
								Properties: map[string]*mapping.DocumentMapping{},
								Fields: []*mapping.FieldMapping{
									{
										Type:  "numeric",
										Index: true,
									},
								},
								DefaultAnalyzer:      "",
								DefaultSynonymSource: "",
							},
						},
						Fields:               []*mapping.FieldMapping{},
						DefaultAnalyzer:      "",
						DefaultSynonymSource: "",
					},
					"map2": {
						Enabled: true,
						Dynamic: false,
						Properties: map[string]*mapping.DocumentMapping{
							"b": {
								Enabled:    true,
								Dynamic:    false,
								Properties: map[string]*mapping.DocumentMapping{},
								Fields: []*mapping.FieldMapping{
									{
										Type:  "numeric",
										Index: true,
									},
								},
								DefaultAnalyzer:      "",
								DefaultSynonymSource: "",
							},
						},
						Fields:               []*mapping.FieldMapping{},
						DefaultAnalyzer:      "",
						DefaultSynonymSource: "",
					},
				},
				DefaultMapping: &mapping.DocumentMapping{
					Enabled:              true,
					Dynamic:              false,
					Properties:           map[string]*mapping.DocumentMapping{},
					Fields:               []*mapping.FieldMapping{},
					DefaultAnalyzer:      "",
					DefaultSynonymSource: "",
				},
				IndexDynamic:     false,
				StoreDynamic:     false,
				DocValuesDynamic: false,
			},
			updated: &mapping.IndexMappingImpl{
				TypeMapping: map[string]*mapping.DocumentMapping{
					"map1": {
						Enabled: true,
						Dynamic: false,
						Properties: map[string]*mapping.DocumentMapping{
							"a": {
								Enabled:    true,
								Dynamic:    false,
								Properties: map[string]*mapping.DocumentMapping{},
								Fields: []*mapping.FieldMapping{
									{
										Type:  "numeric",
										Index: true,
									},
								},
								DefaultAnalyzer:      "",
								DefaultSynonymSource: "",
							},
						},
						Fields:               []*mapping.FieldMapping{},
						DefaultAnalyzer:      "",
						DefaultSynonymSource: "",
					},
					"map2": {
						Enabled: true,
						Dynamic: false,
						Properties: map[string]*mapping.DocumentMapping{
							"c": {
								Enabled:    true,
								Dynamic:    false,
								Properties: map[string]*mapping.DocumentMapping{},
								Fields: []*mapping.FieldMapping{
									{
										Type:  "numeric",
										Index: true,
									},
								},
								DefaultAnalyzer:      "",
								DefaultSynonymSource: "",
							},
						},
						Fields:               []*mapping.FieldMapping{},
						DefaultAnalyzer:      "",
						DefaultSynonymSource: "",
					},
				},
				DefaultMapping: &mapping.DocumentMapping{
					Enabled:              true,
					Dynamic:              false,
					Properties:           map[string]*mapping.DocumentMapping{},
					Fields:               []*mapping.FieldMapping{},
					DefaultAnalyzer:      "",
					DefaultSynonymSource: "",
				},
				IndexDynamic:     false,
				StoreDynamic:     false,
				DocValuesDynamic: false,
			},
			fieldInfo: nil,
			err:       true,
		},
		{
			// new mappings in default mapping => error
			original: &mapping.IndexMappingImpl{
				TypeMapping: map[string]*mapping.DocumentMapping{},
				DefaultMapping: &mapping.DocumentMapping{
					Enabled: true,
					Dynamic: false,
					Properties: map[string]*mapping.DocumentMapping{
						"a": {
							Enabled:    true,
							Dynamic:    false,
							Properties: map[string]*mapping.DocumentMapping{},
							Fields: []*mapping.FieldMapping{
								{
									Type:  "numeric",
									Index: true,
								},
							},
							DefaultAnalyzer:      "",
							DefaultSynonymSource: "",
						},
					},
					Fields:               []*mapping.FieldMapping{},
					DefaultAnalyzer:      "",
					DefaultSynonymSource: "",
				},
				IndexDynamic:     false,
				StoreDynamic:     false,
				DocValuesDynamic: false,
			},
			updated: &mapping.IndexMappingImpl{
				TypeMapping: map[string]*mapping.DocumentMapping{},
				DefaultMapping: &mapping.DocumentMapping{
					Enabled: true,
					Dynamic: false,
					Properties: map[string]*mapping.DocumentMapping{
						"b": {
							Enabled:    true,
							Dynamic:    false,
							Properties: map[string]*mapping.DocumentMapping{},
							Fields: []*mapping.FieldMapping{
								{
									Type:  "numeric",
									Index: true,
								},
							},
							DefaultAnalyzer:      "",
							DefaultSynonymSource: "",
						},
					},
					Fields:               []*mapping.FieldMapping{},
					DefaultAnalyzer:      "",
					DefaultSynonymSource: "",
				},
				IndexDynamic:     false,
				StoreDynamic:     false,
				DocValuesDynamic: false,
			},
			fieldInfo: nil,
			err:       true,
		},
		{
			// fully removed mapping in type with some dynamic => error
			original: &mapping.IndexMappingImpl{
				TypeMapping: map[string]*mapping.DocumentMapping{
					"map1": {
						Enabled: true,
						Dynamic: false,
						Properties: map[string]*mapping.DocumentMapping{
							"a": {
								Enabled:    true,
								Dynamic:    false,
								Properties: map[string]*mapping.DocumentMapping{},
								Fields: []*mapping.FieldMapping{
									{
										Type:  "numeric",
										Index: true,
									},
								},
								DefaultAnalyzer:      "",
								DefaultSynonymSource: "",
							},
						},
						Fields:               []*mapping.FieldMapping{},
						DefaultAnalyzer:      "",
						DefaultSynonymSource: "",
					},
					"map2": {
						Enabled: true,
						Dynamic: false,
						Properties: map[string]*mapping.DocumentMapping{
							"b": {
								Enabled:    true,
								Dynamic:    false,
								Properties: map[string]*mapping.DocumentMapping{},
								Fields: []*mapping.FieldMapping{
									{
										Type:  "numeric",
										Index: true,
									},
								},
								DefaultAnalyzer:      "",
								DefaultSynonymSource: "",
							},
						},
						Fields:               []*mapping.FieldMapping{},
						DefaultAnalyzer:      "",
						DefaultSynonymSource: "",
					},
				},
				DefaultMapping: &mapping.DocumentMapping{
					Enabled: true,
					Dynamic: false,
					Properties: map[string]*mapping.DocumentMapping{
						"c": {
							Enabled:    true,
							Dynamic:    false,
							Properties: map[string]*mapping.DocumentMapping{},
							Fields: []*mapping.FieldMapping{
								{
									Type:  "numeric",
									Index: true,
								},
							},
							DefaultAnalyzer:      "",
							DefaultSynonymSource: "",
						},
					},
					Fields:               []*mapping.FieldMapping{},
					DefaultAnalyzer:      "",
					DefaultSynonymSource: "",
				},
				IndexDynamic:     false,
				StoreDynamic:     false,
				DocValuesDynamic: false,
			},
			updated: &mapping.IndexMappingImpl{
				TypeMapping: map[string]*mapping.DocumentMapping{
					"map1": {
						Enabled: true,
						Dynamic: false,
						Properties: map[string]*mapping.DocumentMapping{
							"a": {
								Enabled:    true,
								Dynamic:    false,
								Properties: map[string]*mapping.DocumentMapping{},
								Fields: []*mapping.FieldMapping{
									{
										Type:  "numeric",
										Index: true,
									},
								},
								DefaultAnalyzer:      "",
								DefaultSynonymSource: "",
							},
						},
						Fields:               []*mapping.FieldMapping{},
						DefaultAnalyzer:      "",
						DefaultSynonymSource: "",
					},
				},
				DefaultMapping: &mapping.DocumentMapping{
					Enabled: true,
					Dynamic: false,
					Properties: map[string]*mapping.DocumentMapping{
						"c": {
							Enabled:    true,
							Dynamic:    false,
							Properties: map[string]*mapping.DocumentMapping{},
							Fields: []*mapping.FieldMapping{
								{
									Type:  "numeric",
									Index: true,
								},
							},
							DefaultAnalyzer:      "",
							DefaultSynonymSource: "",
						},
					},
					Fields:               []*mapping.FieldMapping{},
					DefaultAnalyzer:      "",
					DefaultSynonymSource: "",
				},
				IndexDynamic:     true,
				StoreDynamic:     false,
				DocValuesDynamic: false,
			},
			fieldInfo: nil,
			err:       true,
		},
		{
			// semi removed mapping in default with some dynamic
			// proper fieldInfo with no errors
			original: &mapping.IndexMappingImpl{
				TypeMapping: map[string]*mapping.DocumentMapping{
					"map1": {
						Enabled: true,
						Dynamic: false,
						Properties: map[string]*mapping.DocumentMapping{
							"a": {
								Enabled:    true,
								Dynamic:    false,
								Properties: map[string]*mapping.DocumentMapping{},
								Fields: []*mapping.FieldMapping{
									{
										Type:  "numeric",
										Index: true,
									},
								},
								DefaultAnalyzer:      "",
								DefaultSynonymSource: "",
							},
						},
						Fields:               []*mapping.FieldMapping{},
						DefaultAnalyzer:      "",
						DefaultSynonymSource: "",
					},
					"map2": {
						Enabled: true,
						Dynamic: false,
						Properties: map[string]*mapping.DocumentMapping{
							"b": {
								Enabled:    true,
								Dynamic:    false,
								Properties: map[string]*mapping.DocumentMapping{},
								Fields: []*mapping.FieldMapping{
									{
										Type:  "numeric",
										Index: true,
									},
								},
								DefaultAnalyzer:      "",
								DefaultSynonymSource: "",
							},
						},
						Fields:               []*mapping.FieldMapping{},
						DefaultAnalyzer:      "",
						DefaultSynonymSource: "",
					},
				},
				DefaultMapping: &mapping.DocumentMapping{
					Enabled: true,
					Dynamic: false,
					Properties: map[string]*mapping.DocumentMapping{
						"c": {
							Enabled:    true,
							Dynamic:    false,
							Properties: map[string]*mapping.DocumentMapping{},
							Fields: []*mapping.FieldMapping{
								{
									Type:  "numeric",
									Index: true,
								},
							},
							DefaultAnalyzer:      "",
							DefaultSynonymSource: "",
						},
					},
					Fields:               []*mapping.FieldMapping{},
					DefaultAnalyzer:      "",
					DefaultSynonymSource: "",
				},
				IndexDynamic:     false,
				StoreDynamic:     false,
				DocValuesDynamic: false,
			},
			updated: &mapping.IndexMappingImpl{
				TypeMapping: map[string]*mapping.DocumentMapping{
					"map1": {
						Enabled: true,
						Dynamic: false,
						Properties: map[string]*mapping.DocumentMapping{
							"a": {
								Enabled:    true,
								Dynamic:    false,
								Properties: map[string]*mapping.DocumentMapping{},
								Fields: []*mapping.FieldMapping{
									{
										Type:  "numeric",
										Index: true,
									},
								},
								DefaultAnalyzer:      "",
								DefaultSynonymSource: "",
							},
						},
						Fields:               []*mapping.FieldMapping{},
						DefaultAnalyzer:      "",
						DefaultSynonymSource: "",
					},
					"map2": {
						Enabled: true,
						Dynamic: false,
						Properties: map[string]*mapping.DocumentMapping{
							"b": {
								Enabled:    true,
								Dynamic:    false,
								Properties: map[string]*mapping.DocumentMapping{},
								Fields: []*mapping.FieldMapping{
									{
										Type:  "numeric",
										Index: false,
									},
								},
								DefaultAnalyzer:      "",
								DefaultSynonymSource: "",
							},
						},
						Fields:               []*mapping.FieldMapping{},
						DefaultAnalyzer:      "",
						DefaultSynonymSource: "",
					},
				},
				DefaultMapping: &mapping.DocumentMapping{
					Enabled: true,
					Dynamic: false,
					Properties: map[string]*mapping.DocumentMapping{
						"c": {
							Enabled:    true,
							Dynamic:    false,
							Properties: map[string]*mapping.DocumentMapping{},
							Fields: []*mapping.FieldMapping{
								{
									Type:  "numeric",
									Index: true,
								},
							},
							DefaultAnalyzer:      "",
							DefaultSynonymSource: "",
						},
					},
					Fields:               []*mapping.FieldMapping{},
					DefaultAnalyzer:      "",
					DefaultSynonymSource: "",
				},
				IndexDynamic:     false,
				StoreDynamic:     false,
				DocValuesDynamic: false,
			},
			fieldInfo: map[string]*index.UpdateFieldInfo{
				"b": {
					Index:     true,
					DocValues: true,
				},
			},
			err: false,
		},
		{
			// two fields from diff paths with removed content matching
			// => relavent fieldInfo
			original: &mapping.IndexMappingImpl{
				TypeMapping: map[string]*mapping.DocumentMapping{
					"map1": {
						Enabled: true,
						Dynamic: false,
						Properties: map[string]*mapping.DocumentMapping{
							"a": {
								Enabled:    true,
								Dynamic:    false,
								Properties: map[string]*mapping.DocumentMapping{},
								Fields: []*mapping.FieldMapping{
									{
										Type:  "numeric",
										Index: true,
									},
								},
								DefaultAnalyzer:      "",
								DefaultSynonymSource: "",
							},
						},
						Fields:               []*mapping.FieldMapping{},
						DefaultAnalyzer:      "",
						DefaultSynonymSource: "",
					},
					"map2": {
						Enabled: true,
						Dynamic: false,
						Properties: map[string]*mapping.DocumentMapping{
							"a": {
								Enabled:    true,
								Dynamic:    false,
								Properties: map[string]*mapping.DocumentMapping{},
								Fields: []*mapping.FieldMapping{
									{
										Type:  "numeric",
										Index: true,
									},
								},
								DefaultAnalyzer:      "",
								DefaultSynonymSource: "",
							},
						},
						Fields:               []*mapping.FieldMapping{},
						DefaultAnalyzer:      "",
						DefaultSynonymSource: "",
					},
				},
				DefaultMapping: &mapping.DocumentMapping{
					Enabled: true,
					Dynamic: false,
					Properties: map[string]*mapping.DocumentMapping{
						"b": {
							Enabled:    true,
							Dynamic:    false,
							Properties: map[string]*mapping.DocumentMapping{},
							Fields: []*mapping.FieldMapping{
								{
									Type:  "numeric",
									Index: true,
								},
							},
							DefaultAnalyzer:      "",
							DefaultSynonymSource: "",
						},
					},
					Fields:               []*mapping.FieldMapping{},
					DefaultAnalyzer:      "",
					DefaultSynonymSource: "",
				},
				IndexDynamic:     false,
				StoreDynamic:     false,
				DocValuesDynamic: false,
			},
			updated: &mapping.IndexMappingImpl{
				TypeMapping: map[string]*mapping.DocumentMapping{
					"map1": {
						Enabled: true,
						Dynamic: false,
						Properties: map[string]*mapping.DocumentMapping{
							"a": {
								Enabled:    true,
								Dynamic:    false,
								Properties: map[string]*mapping.DocumentMapping{},
								Fields: []*mapping.FieldMapping{
									{
										Type:  "numeric",
										Index: false,
									},
								},
								DefaultAnalyzer:      "",
								DefaultSynonymSource: "",
							},
						},
						Fields:               []*mapping.FieldMapping{},
						DefaultAnalyzer:      "",
						DefaultSynonymSource: "",
					},
					"map2": {
						Enabled: true,
						Dynamic: false,
						Properties: map[string]*mapping.DocumentMapping{
							"a": {
								Enabled:    true,
								Dynamic:    false,
								Properties: map[string]*mapping.DocumentMapping{},
								Fields: []*mapping.FieldMapping{
									{
										Type:  "numeric",
										Index: false,
									},
								},
								DefaultAnalyzer:      "",
								DefaultSynonymSource: "",
							},
						},
						Fields:               []*mapping.FieldMapping{},
						DefaultAnalyzer:      "",
						DefaultSynonymSource: "",
					},
				},
				DefaultMapping: &mapping.DocumentMapping{
					Enabled: true,
					Dynamic: false,
					Properties: map[string]*mapping.DocumentMapping{
						"b": {
							Enabled:    true,
							Dynamic:    false,
							Properties: map[string]*mapping.DocumentMapping{},
							Fields: []*mapping.FieldMapping{
								{
									Type:  "numeric",
									Index: true,
								},
							},
							DefaultAnalyzer:      "",
							DefaultSynonymSource: "",
						},
					},
					Fields:               []*mapping.FieldMapping{},
					DefaultAnalyzer:      "",
					DefaultSynonymSource: "",
				},
				IndexDynamic:     false,
				StoreDynamic:     false,
				DocValuesDynamic: false,
			},
			fieldInfo: map[string]*index.UpdateFieldInfo{
				"a": {
					Index:     true,
					DocValues: true,
				},
			},
			err: false,
		},
		{
			// two fields from diff paths with removed content not matching
			// => error
			original: &mapping.IndexMappingImpl{
				TypeMapping: map[string]*mapping.DocumentMapping{
					"map1": {
						Enabled: true,
						Dynamic: false,
						Properties: map[string]*mapping.DocumentMapping{
							"a": {
								Enabled:    true,
								Dynamic:    false,
								Properties: map[string]*mapping.DocumentMapping{},
								Fields: []*mapping.FieldMapping{
									{
										Type:  "numeric",
										Index: true,
									},
								},
								DefaultAnalyzer:      "",
								DefaultSynonymSource: "",
							},
						},
						Fields:               []*mapping.FieldMapping{},
						DefaultAnalyzer:      "",
						DefaultSynonymSource: "",
					},
					"map2": {
						Enabled: true,
						Dynamic: false,
						Properties: map[string]*mapping.DocumentMapping{
							"a": {
								Enabled:    true,
								Dynamic:    false,
								Properties: map[string]*mapping.DocumentMapping{},
								Fields: []*mapping.FieldMapping{
									{
										Type:  "numeric",
										Index: true,
									},
								},
								DefaultAnalyzer:      "",
								DefaultSynonymSource: "",
							},
						},
						Fields:               []*mapping.FieldMapping{},
						DefaultAnalyzer:      "",
						DefaultSynonymSource: "",
					},
				},
				DefaultMapping: &mapping.DocumentMapping{
					Enabled: true,
					Dynamic: false,
					Properties: map[string]*mapping.DocumentMapping{
						"b": {
							Enabled:    true,
							Dynamic:    false,
							Properties: map[string]*mapping.DocumentMapping{},
							Fields: []*mapping.FieldMapping{
								{
									Type:  "numeric",
									Index: true,
								},
							},
							DefaultAnalyzer:      "",
							DefaultSynonymSource: "",
						},
					},
					Fields:               []*mapping.FieldMapping{},
					DefaultAnalyzer:      "",
					DefaultSynonymSource: "",
				},
				IndexDynamic:     false,
				StoreDynamic:     false,
				DocValuesDynamic: false,
			},
			updated: &mapping.IndexMappingImpl{
				TypeMapping: map[string]*mapping.DocumentMapping{
					"map1": {
						Enabled: true,
						Dynamic: false,
						Properties: map[string]*mapping.DocumentMapping{
							"a": {
								Enabled:    true,
								Dynamic:    false,
								Properties: map[string]*mapping.DocumentMapping{},
								Fields: []*mapping.FieldMapping{
									{
										Type:  "numeric",
										Index: true,
									},
								},
								DefaultAnalyzer:      "",
								DefaultSynonymSource: "",
							},
						},
						Fields:               []*mapping.FieldMapping{},
						DefaultAnalyzer:      "",
						DefaultSynonymSource: "",
					},
					"map2": {
						Enabled: true,
						Dynamic: false,
						Properties: map[string]*mapping.DocumentMapping{
							"a": {
								Enabled:    true,
								Dynamic:    false,
								Properties: map[string]*mapping.DocumentMapping{},
								Fields: []*mapping.FieldMapping{
									{
										Type:  "numeric",
										Index: false,
									},
								},
								DefaultAnalyzer:      "",
								DefaultSynonymSource: "",
							},
						},
						Fields:               []*mapping.FieldMapping{},
						DefaultAnalyzer:      "",
						DefaultSynonymSource: "",
					},
				},
				DefaultMapping: &mapping.DocumentMapping{
					Enabled: true,
					Dynamic: false,
					Properties: map[string]*mapping.DocumentMapping{
						"b": {
							Enabled:    true,
							Dynamic:    false,
							Properties: map[string]*mapping.DocumentMapping{},
							Fields: []*mapping.FieldMapping{
								{
									Type:  "numeric",
									Index: true,
								},
							},
							DefaultAnalyzer:      "",
							DefaultSynonymSource: "",
						},
					},
					Fields:               []*mapping.FieldMapping{},
					DefaultAnalyzer:      "",
					DefaultSynonymSource: "",
				},
				IndexDynamic:     false,
				StoreDynamic:     false,
				DocValuesDynamic: false,
			},
			fieldInfo: nil,
			err:       true,
		},
		{
			// two fields from the same path => relavent fieldInfo
			original: &mapping.IndexMappingImpl{
				TypeMapping: map[string]*mapping.DocumentMapping{
					"map1": {
						Enabled: true,
						Dynamic: false,
						Properties: map[string]*mapping.DocumentMapping{
							"a": {
								Enabled:    true,
								Dynamic:    false,
								Properties: map[string]*mapping.DocumentMapping{},
								Fields: []*mapping.FieldMapping{
									{
										Name:  "a",
										Type:  "numeric",
										Index: true,
										Store: true,
									},
									{
										Name:  "b",
										Type:  "numeric",
										Index: true,
										Store: true,
									},
								},
								DefaultAnalyzer:      "",
								DefaultSynonymSource: "",
							},
						},
						Fields:               []*mapping.FieldMapping{},
						DefaultAnalyzer:      "",
						DefaultSynonymSource: "",
					},
				},
				DefaultMapping: &mapping.DocumentMapping{
					Enabled: true,
					Dynamic: false,
					Properties: map[string]*mapping.DocumentMapping{
						"c": {
							Enabled:    true,
							Dynamic:    false,
							Properties: map[string]*mapping.DocumentMapping{},
							Fields: []*mapping.FieldMapping{
								{
									Type:  "numeric",
									Index: true,
								},
							},
							DefaultAnalyzer:      "",
							DefaultSynonymSource: "",
						},
					},
					Fields:               []*mapping.FieldMapping{},
					DefaultAnalyzer:      "",
					DefaultSynonymSource: "",
				},
				IndexDynamic:     false,
				StoreDynamic:     false,
				DocValuesDynamic: false,
			},
			updated: &mapping.IndexMappingImpl{
				TypeMapping: map[string]*mapping.DocumentMapping{
					"map1": {
						Enabled: true,
						Dynamic: false,
						Properties: map[string]*mapping.DocumentMapping{
							"a": {
								Enabled:    true,
								Dynamic:    false,
								Properties: map[string]*mapping.DocumentMapping{},
								Fields: []*mapping.FieldMapping{
									{
										Name:  "a",
										Type:  "numeric",
										Index: false,
										Store: true,
									},
									{
										Name:  "b",
										Type:  "numeric",
										Index: true,
										Store: false,
									},
								},
								DefaultAnalyzer:      "",
								DefaultSynonymSource: "",
							},
						},
						Fields:               []*mapping.FieldMapping{},
						DefaultAnalyzer:      "",
						DefaultSynonymSource: "",
					},
				},
				DefaultMapping: &mapping.DocumentMapping{
					Enabled: true,
					Dynamic: false,
					Properties: map[string]*mapping.DocumentMapping{
						"c": {
							Enabled:    true,
							Dynamic:    false,
							Properties: map[string]*mapping.DocumentMapping{},
							Fields: []*mapping.FieldMapping{
								{
									Type:  "numeric",
									Index: true,
								},
							},
							DefaultAnalyzer:      "",
							DefaultSynonymSource: "",
						},
					},
					Fields:               []*mapping.FieldMapping{},
					DefaultAnalyzer:      "",
					DefaultSynonymSource: "",
				},
				IndexDynamic:     false,
				StoreDynamic:     false,
				DocValuesDynamic: false,
			},
			fieldInfo: map[string]*index.UpdateFieldInfo{
				"a": {
					Index:     true,
					DocValues: true,
				},
				"b": {
					Store: true,
				},
			},
			err: false,
		},
		{
			// one store, one index, one dynamic and one all removed in type and default
			// => relavent fieldInfo without error
			original: &mapping.IndexMappingImpl{
				TypeMapping: map[string]*mapping.DocumentMapping{
					"map1": {
						Enabled: true,
						Dynamic: false,
						Properties: map[string]*mapping.DocumentMapping{
							"a": {
								Enabled:    true,
								Dynamic:    false,
								Properties: map[string]*mapping.DocumentMapping{},
								Fields: []*mapping.FieldMapping{
									{
										Type:  "numeric",
										Index: true,
									},
								},
								DefaultAnalyzer:      "",
								DefaultSynonymSource: "",
							},
						},
						Fields:               []*mapping.FieldMapping{},
						DefaultAnalyzer:      "",
						DefaultSynonymSource: "",
					},
					"map2": {
						Enabled: true,
						Dynamic: false,
						Properties: map[string]*mapping.DocumentMapping{
							"b": {
								Enabled:    true,
								Dynamic:    false,
								Properties: map[string]*mapping.DocumentMapping{},
								Fields: []*mapping.FieldMapping{
									{
										Type:  "numeric",
										Store: true,
									},
								},
								DefaultAnalyzer:      "",
								DefaultSynonymSource: "",
							},
						},
						Fields:               []*mapping.FieldMapping{},
						DefaultAnalyzer:      "",
						DefaultSynonymSource: "",
					},
					"map3": {
						Enabled: true,
						Dynamic: false,
						Properties: map[string]*mapping.DocumentMapping{
							"c": {
								Enabled:    true,
								Dynamic:    false,
								Properties: map[string]*mapping.DocumentMapping{},
								Fields: []*mapping.FieldMapping{
									{
										Type:      "numeric",
										DocValues: true,
									},
								},
								DefaultAnalyzer:      "",
								DefaultSynonymSource: "",
							},
						},
						Fields:               []*mapping.FieldMapping{},
						DefaultAnalyzer:      "",
						DefaultSynonymSource: "",
					},
				},
				DefaultMapping: &mapping.DocumentMapping{
					Enabled: true,
					Dynamic: false,
					Properties: map[string]*mapping.DocumentMapping{
						"d": {
							Enabled:    true,
							Dynamic:    false,
							Properties: map[string]*mapping.DocumentMapping{},
							Fields: []*mapping.FieldMapping{
								{
									Type:      "numeric",
									Index:     true,
									Store:     true,
									DocValues: true,
								},
							},
							DefaultAnalyzer:      "",
							DefaultSynonymSource: "",
						},
					},
					Fields:               []*mapping.FieldMapping{},
					DefaultAnalyzer:      "",
					DefaultSynonymSource: "",
				},
				IndexDynamic:     false,
				StoreDynamic:     false,
				DocValuesDynamic: false,
			},
			updated: &mapping.IndexMappingImpl{
				TypeMapping: map[string]*mapping.DocumentMapping{
					"map1": {
						Enabled: true,
						Dynamic: false,
						Properties: map[string]*mapping.DocumentMapping{
							"a": {
								Enabled:    true,
								Dynamic:    false,
								Properties: map[string]*mapping.DocumentMapping{},
								Fields: []*mapping.FieldMapping{
									{
										Type:  "numeric",
										Index: false,
									},
								},
								DefaultAnalyzer:      "",
								DefaultSynonymSource: "",
							},
						},
						Fields:               []*mapping.FieldMapping{},
						DefaultAnalyzer:      "",
						DefaultSynonymSource: "",
					},
					"map2": {
						Enabled: true,
						Dynamic: false,
						Properties: map[string]*mapping.DocumentMapping{
							"b": {
								Enabled:    true,
								Dynamic:    false,
								Properties: map[string]*mapping.DocumentMapping{},
								Fields: []*mapping.FieldMapping{
									{
										Type:  "numeric",
										Store: false,
									},
								},
								DefaultAnalyzer:      "",
								DefaultSynonymSource: "",
							},
						},
						Fields:               []*mapping.FieldMapping{},
						DefaultAnalyzer:      "",
						DefaultSynonymSource: "",
					},
					"map3": {
						Enabled: true,
						Dynamic: false,
						Properties: map[string]*mapping.DocumentMapping{
							"c": {
								Enabled:    true,
								Dynamic:    false,
								Properties: map[string]*mapping.DocumentMapping{},
								Fields: []*mapping.FieldMapping{
									{
										Type:      "numeric",
										DocValues: false,
									},
								},
								DefaultAnalyzer:      "",
								DefaultSynonymSource: "",
							},
						},
						Fields:               []*mapping.FieldMapping{},
						DefaultAnalyzer:      "",
						DefaultSynonymSource: "",
					},
				},
				DefaultMapping: &mapping.DocumentMapping{
					Enabled:              true,
					Dynamic:              false,
					Properties:           map[string]*mapping.DocumentMapping{},
					Fields:               []*mapping.FieldMapping{},
					DefaultAnalyzer:      "",
					DefaultSynonymSource: "",
				},
				IndexDynamic:     false,
				StoreDynamic:     false,
				DocValuesDynamic: false,
			},
			fieldInfo: map[string]*index.UpdateFieldInfo{
				"a": {
					Index:     true,
					DocValues: true,
				},
				"b": {
					Store: true,
				},
				"c": {
					DocValues: true,
				},
				"d": {
					RemoveAll: true,
				},
			},
			err: false,
		},
	}

	for i, test := range tests {
		info, err := DeletedFields(test.original, test.updated)

		if err == nil && test.err || err != nil && !test.err {
			t.Errorf("Unexpected error value for test %d, expecting %t, got %v\n", i, test.err, err)
		}
		if info == nil && test.fieldInfo != nil || info != nil && test.fieldInfo == nil || !reflect.DeepEqual(info, test.fieldInfo) {
			t.Errorf("Unexpected default info value for test %d, expecting %+v, got %+v, err %v", i, test.fieldInfo, info, err)
		}
	}
}

func TestIndexUpdateText(t *testing.T) {
	tmpIndexPath := createTmpIndexPath(t)
	defer cleanupTmpIndexPath(t, tmpIndexPath)

	indexMappingBefore := mapping.NewIndexMapping()
	indexMappingBefore.TypeMapping = map[string]*mapping.DocumentMapping{}
	indexMappingBefore.DefaultMapping = &mapping.DocumentMapping{
		Enabled: true,
		Dynamic: false,
		Properties: map[string]*mapping.DocumentMapping{
			"a": {
				Enabled:    true,
				Dynamic:    false,
				Properties: map[string]*mapping.DocumentMapping{},
				Fields: []*mapping.FieldMapping{
					{
						Type:  "text",
						Index: true,
						Store: true,
					},
				},
				DefaultAnalyzer:      "standard",
				DefaultSynonymSource: "",
			},
			"b": {
				Enabled:    true,
				Dynamic:    false,
				Properties: map[string]*mapping.DocumentMapping{},
				Fields: []*mapping.FieldMapping{
					{
						Type:  "text",
						Index: true,
						Store: true,
					},
				},
				DefaultAnalyzer:      "standard",
				DefaultSynonymSource: "",
			},
			"c": {
				Enabled:    true,
				Dynamic:    false,
				Properties: map[string]*mapping.DocumentMapping{},
				Fields: []*mapping.FieldMapping{
					{
						Type:  "text",
						Index: true,
						Store: true,
					},
				},
				DefaultAnalyzer:      "standard",
				DefaultSynonymSource: "",
			},
			"d": {
				Enabled:    true,
				Dynamic:    false,
				Properties: map[string]*mapping.DocumentMapping{},
				Fields: []*mapping.FieldMapping{
					{
						Type:  "text",
						Index: true,
						Store: true,
					},
				},
				DefaultAnalyzer:      "standard",
				DefaultSynonymSource: "",
			},
		},
		Fields:               []*mapping.FieldMapping{},
		DefaultAnalyzer:      "standard",
		DefaultSynonymSource: "",
	}
	indexMappingBefore.IndexDynamic = false
	indexMappingBefore.StoreDynamic = false
	indexMappingBefore.DocValuesDynamic = false

	index, err := New(tmpIndexPath, indexMappingBefore)
	if err != nil {
		t.Fatal(err)
	}
	doc1 := map[string]interface{}{"a": "xyz", "b": "abc", "c": "def", "d": "ghi"}
	doc2 := map[string]interface{}{"a": "uvw", "b": "rst", "c": "klm", "d": "pqr"}
	doc3 := map[string]interface{}{"a": "xyz", "b": "def", "c": "abc", "d": "mno"}
	batch := index.NewBatch()
	err = batch.Index("001", doc1)
	if err != nil {
		t.Fatal(err)
	}
	err = batch.Index("002", doc2)
	if err != nil {
		t.Fatal(err)
	}
	err = batch.Index("003", doc3)
	if err != nil {
		t.Fatal(err)
	}
	err = index.Batch(batch)
	if err != nil {
		t.Fatal(err)
	}
	err = index.Close()
	if err != nil {
		t.Fatal(err)
	}

	indexMappingAfter := mapping.NewIndexMapping()
	indexMappingAfter.TypeMapping = map[string]*mapping.DocumentMapping{}
	indexMappingAfter.DefaultMapping = &mapping.DocumentMapping{
		Enabled: true,
		Dynamic: false,
		Properties: map[string]*mapping.DocumentMapping{
			"a": {
				Enabled:    true,
				Dynamic:    false,
				Properties: map[string]*mapping.DocumentMapping{},
				Fields: []*mapping.FieldMapping{
					{
						Type:  "text",
						Index: true,
						Store: true,
					},
				},
				DefaultAnalyzer:      "standard",
				DefaultSynonymSource: "",
			},
			"b": {
				Enabled:    true,
				Dynamic:    false,
				Properties: map[string]*mapping.DocumentMapping{},
				Fields: []*mapping.FieldMapping{
					{
						Type:  "text",
						Index: false,
						Store: true,
					},
				},
				DefaultAnalyzer:      "standard",
				DefaultSynonymSource: "",
			},
			"c": {
				Enabled:    true,
				Dynamic:    false,
				Properties: map[string]*mapping.DocumentMapping{},
				Fields: []*mapping.FieldMapping{
					{
						Type:  "text",
						Index: true,
						Store: false,
					},
				},
				DefaultAnalyzer:      "standard",
				DefaultSynonymSource: "",
			},
		},
		Fields:               []*mapping.FieldMapping{},
		DefaultAnalyzer:      "standard",
		DefaultSynonymSource: "",
	}
	indexMappingAfter.IndexDynamic = false
	indexMappingAfter.StoreDynamic = false
	indexMappingAfter.DocValuesDynamic = false

	mappingString, err := json.Marshal(indexMappingAfter)
	if err != nil {
		t.Fatal(err)
	}
	index, err = Update(tmpIndexPath, string(mappingString))
	if err != nil {
		t.Fatal(err)
	}

	q1 := NewSearchRequest(NewQueryStringQuery("a:*"))
	q1.Fields = append(q1.Fields, "a")
	res1, err := index.Search(q1)
	if err != nil {
		t.Fatal(err)
	}
	if len(res1.Hits) != 3 {
		t.Fatalf("Expected 3 hits, got %d\n", len(res1.Hits))
	}
	if len(res1.Hits[0].Fields) != 1 {
		t.Fatalf("Expected 1 field, got %d\n", len(res1.Hits[0].Fields))
	}
	q2 := NewSearchRequest(NewQueryStringQuery("b:*"))
	q2.Fields = append(q2.Fields, "b")
	res2, err := index.Search(q2)
	if err != nil {
		t.Fatal(err)
	}
	if len(res2.Hits) != 0 {
		t.Fatalf("Expected 0 hits, got %d\n", len(res2.Hits))
	}
	q3 := NewSearchRequest(NewQueryStringQuery("c:*"))
	q3.Fields = append(q3.Fields, "c")
	res3, err := index.Search(q3)
	if err != nil {
		t.Fatal(err)
	}
	if len(res3.Hits) != 3 {
		t.Fatalf("Expected 3 hits, got %d\n", len(res3.Hits))
	}
	if len(res3.Hits[0].Fields) != 0 {
		t.Fatalf("Expected 0 fields, got %d\n", len(res3.Hits[0].Fields))
	}
	q4 := NewSearchRequest(NewQueryStringQuery("d:*"))
	q4.Fields = append(q4.Fields, "d")
	res4, err := index.Search(q4)
	if err != nil {
		t.Fatal(err)
	}
	if len(res4.Hits) != 0 {
		t.Fatalf("Expected 0 hits, got %d\n", len(res4.Hits))
	}
}

func TestIndexUpdateVector(t *testing.T) {
	tmpIndexPath := createTmpIndexPath(t)
	defer cleanupTmpIndexPath(t, tmpIndexPath)

	indexMappingBefore := mapping.NewIndexMapping()
	indexMappingBefore.TypeMapping = map[string]*mapping.DocumentMapping{}
	indexMappingBefore.DefaultMapping = &mapping.DocumentMapping{
		Enabled: true,
		Dynamic: false,
		Properties: map[string]*mapping.DocumentMapping{
			"a": {
				Enabled:    true,
				Dynamic:    false,
				Properties: map[string]*mapping.DocumentMapping{},
				Fields: []*mapping.FieldMapping{
					{
						Type:                    "vector",
						Index:                   true,
						Dims:                    4,
						Similarity:              "l2_norm",
						VectorIndexOptimizedFor: "latency",
					},
				},
			},
			"b": {
				Enabled:    true,
				Dynamic:    false,
				Properties: map[string]*mapping.DocumentMapping{},
				Fields: []*mapping.FieldMapping{
					{
						Type:                    "vector",
						Index:                   true,
						Dims:                    4,
						Similarity:              "l2_norm",
						VectorIndexOptimizedFor: "latency",
					},
				},
			},
			"c": {
				Enabled:    true,
				Dynamic:    false,
				Properties: map[string]*mapping.DocumentMapping{},
				Fields: []*mapping.FieldMapping{
					{
						Type:                    "vector_base64",
						Index:                   true,
						Dims:                    4,
						Similarity:              "l2_norm",
						VectorIndexOptimizedFor: "latency",
					},
				},
			},
			"d": {
				Enabled:    true,
				Dynamic:    false,
				Properties: map[string]*mapping.DocumentMapping{},
				Fields: []*mapping.FieldMapping{
					{
						Type:                    "vector_base64",
						Index:                   true,
						Dims:                    4,
						Similarity:              "l2_norm",
						VectorIndexOptimizedFor: "latency",
					},
				},
			},
		},
		Fields: []*mapping.FieldMapping{},
	}
	indexMappingBefore.IndexDynamic = false
	indexMappingBefore.StoreDynamic = false
	indexMappingBefore.DocValuesDynamic = false

	index, err := New(tmpIndexPath, indexMappingBefore)
	if err != nil {
		t.Fatal(err)
	}
	doc1 := map[string]interface{}{"a": []float32{0.32894259691238403, 0.6973215341567993, 0.6835201978683472, 0.38296082615852356}, "b": []float32{0.32894259691238403, 0.6973215341567993, 0.6835201978683472, 0.38296082615852356}, "c": "L5MOPw7NID5SQMU9pHUoPw==", "d": "L5MOPw7NID5SQMU9pHUoPw=="}
	doc2 := map[string]interface{}{"a": []float32{0.0018692062003538013, 0.41076546907424927, 0.5675257444381714, 0.45832985639572144}, "b": []float32{0.0018692062003538013, 0.41076546907424927, 0.5675257444381714, 0.45832985639572144}, "c": "czloP94ZCD71ldY+GbAOPw==", "d": "czloP94ZCD71ldY+GbAOPw=="}
	doc3 := map[string]interface{}{"a": []float32{0.7853356599807739, 0.6904757618904114, 0.5643226504325867, 0.682637631893158}, "b": []float32{0.7853356599807739, 0.6904757618904114, 0.5643226504325867, 0.682637631893158}, "c": "Chh6P2lOqT47mjg/0odlPg==", "d": "Chh6P2lOqT47mjg/0odlPg=="}
	batch := index.NewBatch()
	err = batch.Index("001", doc1)
	if err != nil {
		t.Fatal(err)
	}
	err = batch.Index("002", doc2)
	if err != nil {
		t.Fatal(err)
	}
	err = batch.Index("003", doc3)
	if err != nil {
		t.Fatal(err)
	}
	err = index.Batch(batch)
	if err != nil {
		t.Fatal(err)
	}
	err = index.Close()
	if err != nil {
		t.Fatal(err)
	}

	indexMappingAfter := mapping.NewIndexMapping()
	indexMappingAfter.TypeMapping = map[string]*mapping.DocumentMapping{}
	indexMappingAfter.DefaultMapping = &mapping.DocumentMapping{
		Enabled: true,
		Dynamic: false,
		Properties: map[string]*mapping.DocumentMapping{
			"a": {
				Enabled:    true,
				Dynamic:    false,
				Properties: map[string]*mapping.DocumentMapping{},
				Fields: []*mapping.FieldMapping{
					{
						Type:                    "vector",
						Index:                   true,
						Dims:                    4,
						Similarity:              "l2_norm",
						VectorIndexOptimizedFor: "latency",
					},
				},
			},
			"c": {
				Enabled:    true,
				Dynamic:    false,
				Properties: map[string]*mapping.DocumentMapping{},
				Fields: []*mapping.FieldMapping{
					{
						Type:                    "vector_base64",
						Index:                   true,
						Dims:                    4,
						Similarity:              "l2_norm",
						VectorIndexOptimizedFor: "latency",
					},
				},
			},
			"d": {
				Enabled:    true,
				Dynamic:    false,
				Properties: map[string]*mapping.DocumentMapping{},
				Fields: []*mapping.FieldMapping{
					{
						Type:                    "vector_base64",
						Index:                   false,
						Dims:                    4,
						Similarity:              "l2_norm",
						VectorIndexOptimizedFor: "latency",
					},
				},
			},
		},
		Fields: []*mapping.FieldMapping{},
	}
	indexMappingAfter.IndexDynamic = false
	indexMappingAfter.StoreDynamic = false
	indexMappingAfter.DocValuesDynamic = false

	mappingString, err := json.Marshal(indexMappingAfter)
	if err != nil {
		t.Fatal(err)
	}
	index, err = Update(tmpIndexPath, string(mappingString))
	if err != nil {
		t.Fatal(err)
	}

	q1 := NewSearchRequest(NewMatchNoneQuery())
	q1.AddKNN("a", []float32{1, 2, 3, 4}, 3, 1.0)
	res1, err := index.Search(q1)
	if err != nil {
		t.Fatal(err)
	}
	if len(res1.Hits) != 3 {
		t.Fatalf("Expected 3 hits, got %d\n", len(res1.Hits))
	}
	q2 := NewSearchRequest(NewMatchNoneQuery())
	q2.AddKNN("e", []float32{1, 2, 3, 4}, 3, 1.0)
	res2, err := index.Search(q2)
	if err != nil {
		t.Fatal(err)
	}
	if len(res2.Hits) != 0 {
		t.Fatalf("Expected 0 hits, got %d\n", len(res2.Hits))
	}
	q3 := NewSearchRequest(NewMatchNoneQuery())
	q3.AddKNN("c", []float32{1, 2, 3, 4}, 3, 1.0)
	res3, err := index.Search(q3)
	if err != nil {
		t.Fatal(err)
	}
	if len(res3.Hits) != 3 {
		t.Fatalf("Expected 3 hits, got %d\n", len(res3.Hits))
	}
	q4 := NewSearchRequest(NewMatchNoneQuery())
	q4.AddKNN("d", []float32{1, 2, 3, 4}, 3, 1.0)
	res4, err := index.Search(q4)
	if err != nil {
		t.Fatal(err)
	}
	if len(res4.Hits) != 0 {
		t.Fatalf("Expected 0 hits, got %d\n", len(res4.Hits))
	}
}

func TestIndexUpdateSynonym(t *testing.T) {
	tmpIndexPath := createTmpIndexPath(t)
	defer cleanupTmpIndexPath(t, tmpIndexPath)

	synonymCollection := "collection1"
	synonymSourceName := "english"
	analyzer := en.AnalyzerName
	synonymSourceConfig := map[string]interface{}{
		"collection": synonymCollection,
		"analyzer":   analyzer,
	}

	a := mapping.NewTextFieldMapping()
	a.Analyzer = analyzer
	a.SynonymSource = synonymSourceName
	a.IncludeInAll = false

	b := mapping.NewTextFieldMapping()
	b.Analyzer = analyzer
	b.SynonymSource = synonymSourceName
	b.IncludeInAll = false

	c := mapping.NewTextFieldMapping()
	c.Analyzer = analyzer
	c.SynonymSource = synonymSourceName
	c.IncludeInAll = false

	indexMappingBefore := mapping.NewIndexMapping()
	indexMappingBefore.DefaultMapping.AddFieldMappingsAt("a", a)
	indexMappingBefore.DefaultMapping.AddFieldMappingsAt("b", b)
	indexMappingBefore.DefaultMapping.AddFieldMappingsAt("c", c)
	err := indexMappingBefore.AddSynonymSource(synonymSourceName, synonymSourceConfig)
	if err != nil {
		t.Fatal(err)
	}

	indexMappingBefore.IndexDynamic = false
	indexMappingBefore.StoreDynamic = false
	indexMappingBefore.DocValuesDynamic = false

	index, err := New(tmpIndexPath, indexMappingBefore)
	if err != nil {
		t.Fatal(err)
	}

	doc1 := map[string]interface{}{
		"a": `The hardworking employee consistently strives to exceed expectations.
				His industrious nature makes him a valuable asset to any team.
				His conscientious attention to detail ensures that projects are completed efficiently and accurately.
				He remains persistent even in the face of challenges.`,
		"b": `The hardworking employee consistently strives to exceed expectations.
				His industrious nature makes him a valuable asset to any team.
				His conscientious attention to detail ensures that projects are completed efficiently and accurately.
				He remains persistent even in the face of challenges.`,
		"c": `The hardworking employee consistently strives to exceed expectations.
				His industrious nature makes him a valuable asset to any team.
				His conscientious attention to detail ensures that projects are completed efficiently and accurately.
				He remains persistent even in the face of challenges.`,
	}
	doc2 := map[string]interface{}{
		"a": `The tranquil surroundings of the retreat provide a perfect escape from the hustle and bustle of city life. 
				Guests enjoy the peaceful atmosphere, which is perfect for relaxation and rejuvenation. 
				The calm environment offers the ideal place to meditate and connect with nature. 
				Even the most stressed individuals find themselves feeling relaxed and at ease.`,
		"b": `The tranquil surroundings of the retreat provide a perfect escape from the hustle and bustle of city life. 
				Guests enjoy the peaceful atmosphere, which is perfect for relaxation and rejuvenation. 
				The calm environment offers the ideal place to meditate and connect with nature. 
				Even the most stressed individuals find themselves feeling relaxed and at ease.`,
		"c": `The tranquil surroundings of the retreat provide a perfect escape from the hustle and bustle of city life. 
				Guests enjoy the peaceful atmosphere, which is perfect for relaxation and rejuvenation. 
				The calm environment offers the ideal place to meditate and connect with nature. 
				Even the most stressed individuals find themselves feeling relaxed and at ease.`,
	}
	synDoc1 := &SynonymDefinition{Synonyms: []string{"hardworking", "industrious", "conscientious", "persistent", "focused", "devoted"}}
	synDoc2 := &SynonymDefinition{Synonyms: []string{"tranquil", "peaceful", "calm", "relaxed", "unruffled"}}

	batch := index.NewBatch()
	err = batch.IndexSynonym("001", synonymCollection, synDoc1)
	if err != nil {
		t.Fatal(err)
	}
	err = batch.IndexSynonym("002", synonymCollection, synDoc2)
	if err != nil {
		t.Fatal(err)
	}
	err = batch.Index("003", doc1)
	if err != nil {
		t.Fatal(err)
	}
	err = batch.Index("004", doc2)
	if err != nil {
		t.Fatal(err)
	}
	err = index.Batch(batch)
	if err != nil {
		t.Fatal(err)
	}
	err = index.Close()
	if err != nil {
		t.Fatal(err)
	}

	indexMappingAfter := mapping.NewIndexMapping()
	indexMappingAfter.DefaultMapping.AddFieldMappingsAt("a", a)
	b.Index = false
	indexMappingAfter.DefaultMapping.AddFieldMappingsAt("b", b)
	err = indexMappingAfter.AddSynonymSource(synonymSourceName, synonymSourceConfig)
	if err != nil {
		t.Fatal(err)
	}

	indexMappingAfter.IndexDynamic = false
	indexMappingAfter.StoreDynamic = false
	indexMappingAfter.DocValuesDynamic = false

	mappingString, err := json.Marshal(indexMappingAfter)
	if err != nil {
		t.Fatal(err)
	}
	index, err = Update(tmpIndexPath, string(mappingString))
	if err != nil {
		t.Fatal(err)
	}

	q1 := NewSearchRequest(NewQueryStringQuery("a:devoted"))
	res1, err := index.Search(q1)
	if err != nil {
		t.Fatal(err)
	}
	if len(res1.Hits) != 1 {
		t.Fatalf("Expected 1 hit, got %d\n", len(res1.Hits))
	}

	q2 := NewSearchRequest(NewQueryStringQuery("b:devoted"))
	res2, err := index.Search(q2)
	if err != nil {
		t.Fatal(err)
	}
	if len(res2.Hits) != 0 {
		t.Fatalf("Expected 0 hits, got %d\n", len(res2.Hits))
	}

	q3 := NewSearchRequest(NewQueryStringQuery("c:unruffled"))
	res3, err := index.Search(q3)
	if err != nil {
		t.Fatal(err)
	}
	if len(res3.Hits) != 0 {
		t.Fatalf("Expected 0 hits, got %d\n", len(res3.Hits))
	}
}

func TestIndexUpdateMerge(t *testing.T) {
	tmpIndexPath := createTmpIndexPath(t)
	defer cleanupTmpIndexPath(t, tmpIndexPath)

	indexMappingBefore := mapping.NewIndexMapping()
	indexMappingBefore.TypeMapping = map[string]*mapping.DocumentMapping{}
	indexMappingBefore.DefaultMapping = &mapping.DocumentMapping{
		Enabled: true,
		Dynamic: false,
		Properties: map[string]*mapping.DocumentMapping{
			"a": {
				Enabled:    true,
				Dynamic:    false,
				Properties: map[string]*mapping.DocumentMapping{},
				Fields: []*mapping.FieldMapping{
					{
						Type:  "text",
						Index: true,
						Store: true,
					},
				},
				DefaultAnalyzer:      "standard",
				DefaultSynonymSource: "",
			},
			"b": {
				Enabled:    true,
				Dynamic:    false,
				Properties: map[string]*mapping.DocumentMapping{},
				Fields: []*mapping.FieldMapping{
					{
						Type:  "text",
						Index: true,
						Store: true,
					},
				},
				DefaultAnalyzer:      "standard",
				DefaultSynonymSource: "",
			},
			"c": {
				Enabled:    true,
				Dynamic:    false,
				Properties: map[string]*mapping.DocumentMapping{},
				Fields: []*mapping.FieldMapping{
					{
						Type:  "text",
						Index: true,
						Store: true,
					},
				},
				DefaultAnalyzer:      "standard",
				DefaultSynonymSource: "",
			},
			"d": {
				Enabled:    true,
				Dynamic:    false,
				Properties: map[string]*mapping.DocumentMapping{},
				Fields: []*mapping.FieldMapping{
					{
						Type:  "text",
						Index: true,
						Store: true,
					},
				},
				DefaultAnalyzer:      "standard",
				DefaultSynonymSource: "",
			},
		},
		Fields:               []*mapping.FieldMapping{},
		DefaultAnalyzer:      "standard",
		DefaultSynonymSource: "",
	}
	indexMappingBefore.IndexDynamic = false
	indexMappingBefore.StoreDynamic = false
	indexMappingBefore.DocValuesDynamic = false

	index, err := New(tmpIndexPath, indexMappingBefore)
	if err != nil {
		t.Fatal(err)
	}

	numDocsPerBatch := 1000
	numBatches := 3

	var batch *Batch
	doc := make(map[string]interface{})
	const letters = "abcdefghijklmnopqrstuvwxyz"

	randStr := func() string {
		result := make([]byte, 3)
		for i := 0; i < 3; i++ {
			result[i] = letters[rand.Intn(len(letters))]
		}
		return string(result)
	}
	for i := 0; i < numBatches; i++ {
		batch = index.NewBatch()
		for j := 0; j < numDocsPerBatch; j++ {
			doc["a"] = randStr()
			doc["b"] = randStr()
			doc["c"] = randStr()
			doc["d"] = randStr()
			err = batch.Index(fmt.Sprintf("%d", i*numDocsPerBatch+j), doc)
			if err != nil {
				t.Fatal(err)
			}
		}
		err = index.Batch(batch)
		if err != nil {
			t.Fatal(err)
		}
	}

	err = index.Close()
	if err != nil {
		t.Fatal(err)
	}

	indexMappingAfter := mapping.NewIndexMapping()
	indexMappingAfter.TypeMapping = map[string]*mapping.DocumentMapping{}
	indexMappingAfter.DefaultMapping = &mapping.DocumentMapping{
		Enabled: true,
		Dynamic: false,
		Properties: map[string]*mapping.DocumentMapping{
			"a": {
				Enabled:    true,
				Dynamic:    false,
				Properties: map[string]*mapping.DocumentMapping{},
				Fields: []*mapping.FieldMapping{
					{
						Type:  "text",
						Index: true,
						Store: true,
					},
				},
				DefaultAnalyzer:      "standard",
				DefaultSynonymSource: "",
			},
			"b": {
				Enabled:    true,
				Dynamic:    false,
				Properties: map[string]*mapping.DocumentMapping{},
				Fields: []*mapping.FieldMapping{
					{
						Type:  "text",
						Index: false,
						Store: true,
					},
				},
				DefaultAnalyzer:      "standard",
				DefaultSynonymSource: "",
			},
			"c": {
				Enabled:    true,
				Dynamic:    false,
				Properties: map[string]*mapping.DocumentMapping{},
				Fields: []*mapping.FieldMapping{
					{
						Type:  "text",
						Index: true,
						Store: false,
					},
				},
				DefaultAnalyzer:      "standard",
				DefaultSynonymSource: "",
			},
		},
		Fields:               []*mapping.FieldMapping{},
		DefaultAnalyzer:      "standard",
		DefaultSynonymSource: "",
	}
	indexMappingAfter.IndexDynamic = false
	indexMappingAfter.StoreDynamic = false
	indexMappingAfter.DocValuesDynamic = false

	mappingString, err := json.Marshal(indexMappingAfter)
	if err != nil {
		t.Fatal(err)
	}
	index, err = Update(tmpIndexPath, string(mappingString))
	if err != nil {
		t.Fatal(err)
	}

	impl, ok := index.(*indexImpl)
	if !ok {
		t.Fatalf("Typecasting index to indexImpl failed")
	}
	sindex, ok := impl.i.(*scorch.Scorch)
	if !ok {
		t.Fatalf("Typecasting index to scorch index failed")
	}

	err = sindex.ForceMerge(context.Background(), &mergeplan.SingleSegmentMergePlanOptions)
	if err != nil {
		t.Fatal(err)
	}

	q1 := NewSearchRequest(NewQueryStringQuery("a:*"))
	q1.Fields = append(q1.Fields, "a")

	res1, err := index.Search(q1)
	if err != nil {
		t.Fatal(err)
	}
	if len(res1.Hits) != 10 {
		t.Fatalf("Expected 10 hits, got %d\n", len(res1.Hits))
	}
	if len(res1.Hits[0].Fields) != 1 {
		t.Fatalf("Expected 1 field, got %d\n", len(res1.Hits[0].Fields))
	}
	q2 := NewSearchRequest(NewQueryStringQuery("b:*"))
	q2.Fields = append(q2.Fields, "b")
	res2, err := index.Search(q2)
	if err != nil {
		t.Fatal(err)
	}
	if len(res2.Hits) != 0 {
		t.Fatalf("Expected 0 hits, got %d\n", len(res2.Hits))
	}
	q3 := NewSearchRequest(NewQueryStringQuery("c:*"))
	q3.Fields = append(q3.Fields, "c")
	res3, err := index.Search(q3)
	if err != nil {
		t.Fatal(err)
	}
	if len(res3.Hits) != 10 {
		t.Fatalf("Expected 10 hits, got %d\n", len(res3.Hits))
	}
	if len(res3.Hits[0].Fields) != 0 {
		t.Fatalf("Expected 0 fields, got %d\n", len(res3.Hits[0].Fields))
	}
	q4 := NewSearchRequest(NewQueryStringQuery("d:*"))
	q4.Fields = append(q4.Fields, "d")
	res4, err := index.Search(q4)
	if err != nil {
		t.Fatal(err)
	}
	if len(res4.Hits) != 0 {
		t.Fatalf("Expected 0 hits, got %d\n", len(res4.Hits))
	}

}

func BenchmarkIndexUpdateText(b *testing.B) {

	tmpIndexPath := createTmpIndexPath(b)
	defer cleanupTmpIndexPath(b, tmpIndexPath)

	indexMappingBefore := mapping.NewIndexMapping()
	indexMappingBefore.TypeMapping = map[string]*mapping.DocumentMapping{}
	indexMappingBefore.DefaultMapping = &mapping.DocumentMapping{
		Enabled: true,
		Dynamic: false,
		Properties: map[string]*mapping.DocumentMapping{
			"a": {
				Enabled:    true,
				Dynamic:    false,
				Properties: map[string]*mapping.DocumentMapping{},
				Fields: []*mapping.FieldMapping{
					{
						Type:  "text",
						Index: true,
						Store: true,
					},
				},
				DefaultAnalyzer:      "standard",
				DefaultSynonymSource: "",
			},
		},
		Fields:               []*mapping.FieldMapping{},
		DefaultAnalyzer:      "standard",
		DefaultSynonymSource: "",
	}
	indexMappingBefore.IndexDynamic = false
	indexMappingBefore.StoreDynamic = false
	indexMappingBefore.DocValuesDynamic = false

	index, err := New(tmpIndexPath, indexMappingBefore)
	if err != nil {
		b.Fatal(err)
	}

	numDocsPerBatch := 1000
	numBatches := 5

	var batch *Batch
	doc := make(map[string]interface{})
	const letters = "abcdefghijklmnopqrstuvwxyz"

	randStr := func() string {
		result := make([]byte, 3)
		for i := 0; i < 3; i++ {
			result[i] = letters[rand.Intn(len(letters))]
		}
		return string(result)
	}
	for i := 0; i < numBatches; i++ {
		batch = index.NewBatch()
		for j := 0; j < numDocsPerBatch; j++ {
			doc["a"] = randStr()
			err = batch.Index(fmt.Sprintf("%d", i*numDocsPerBatch+j), doc)
			if err != nil {
				b.Fatal(err)
			}
		}
		err = index.Batch(batch)
		if err != nil {
			b.Fatal(err)
		}
	}

	err = index.Close()
	if err != nil {
		b.Fatal(err)
	}

	indexMappingAfter := mapping.NewIndexMapping()
	indexMappingAfter.TypeMapping = map[string]*mapping.DocumentMapping{}
	indexMappingAfter.DefaultMapping = &mapping.DocumentMapping{
		Enabled: true,
		Dynamic: false,
		Properties: map[string]*mapping.DocumentMapping{
			"a": {
				Enabled:    true,
				Dynamic:    false,
				Properties: map[string]*mapping.DocumentMapping{},
				Fields: []*mapping.FieldMapping{
					{
						Type:  "text",
						Index: true,
						Store: false,
					},
				},
				DefaultAnalyzer:      "standard",
				DefaultSynonymSource: "",
			},
		},
		Fields:               []*mapping.FieldMapping{},
		DefaultAnalyzer:      "standard",
		DefaultSynonymSource: "",
	}
	indexMappingAfter.IndexDynamic = false
	indexMappingAfter.StoreDynamic = false
	indexMappingAfter.DocValuesDynamic = false

	mappingString, err := json.Marshal(indexMappingAfter)
	if err != nil {
		b.Fatal(err)
	}
	index, err = Update(tmpIndexPath, string(mappingString))
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		q := NewQueryStringQuery("a:*")
		req := NewSearchRequest(q)
		if _, err = index.Search(req); err != nil {
			b.Fatal(err)
		}
	}
}
