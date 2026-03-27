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

	"github.com/blevesearch/bleve/v2/analysis/analyzer/custom"
	"github.com/blevesearch/bleve/v2/analysis/analyzer/simple"
	"github.com/blevesearch/bleve/v2/analysis/analyzer/standard"
	"github.com/blevesearch/bleve/v2/analysis/datetime/percent"
	"github.com/blevesearch/bleve/v2/analysis/datetime/sanitized"
	"github.com/blevesearch/bleve/v2/analysis/lang/en"
	"github.com/blevesearch/bleve/v2/analysis/token/lowercase"
	"github.com/blevesearch/bleve/v2/analysis/tokenizer/letter"
	"github.com/blevesearch/bleve/v2/analysis/tokenizer/whitespace"
	"github.com/blevesearch/bleve/v2/index/scorch"
	"github.com/blevesearch/bleve/v2/index/scorch/mergeplan"
	"github.com/blevesearch/bleve/v2/mapping"
	index "github.com/blevesearch/bleve_index_api"
)

func TestCompareFieldMapping(t *testing.T) {
	tests := []struct {
		original       *mapping.FieldMapping
		updated        *mapping.FieldMapping
		indexFieldInfo *index.UpdateFieldInfo
		err            bool
	}{
		{ // both nil => error
			original:       nil,
			updated:        nil,
			indexFieldInfo: nil,
			err:            true,
		},
		{ // updated nil => delete all
			original: &mapping.FieldMapping{},
			updated:  nil,
			indexFieldInfo: &index.UpdateFieldInfo{
				Deleted: true,
			},
			err: false,
		},
		{ // type changed => not updatable
			original: &mapping.FieldMapping{
				Type: "text",
			},
			updated: &mapping.FieldMapping{
				Type: "datetime",
			},
			indexFieldInfo: nil,
			err:            true,
		},
		{ // synonym source changed for text => updatable
			original: &mapping.FieldMapping{
				Type:          "text",
				SynonymSource: "a",
			},
			updated: &mapping.FieldMapping{
				Type:          "text",
				SynonymSource: "b",
			},
			indexFieldInfo: &index.UpdateFieldInfo{},
			err:            false,
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
			indexFieldInfo: nil,
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
			indexFieldInfo: nil,
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
			indexFieldInfo: nil,
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
			indexFieldInfo: nil,
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
			indexFieldInfo: nil,
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
			indexFieldInfo: nil,
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
			indexFieldInfo: nil,
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
			indexFieldInfo: &index.UpdateFieldInfo{
				Index:     true,
				DocValues: true,
			},
			err: false,
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
			indexFieldInfo: &index.UpdateFieldInfo{
				DocValues: true,
			},
			err: false,
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
			indexFieldInfo: &index.UpdateFieldInfo{},
			err:            false,
		},
	}

	for i, test := range tests {
		rv, err := compareFieldMapping(test.original, test.updated)

		if err == nil && test.err || err != nil && !test.err {
			t.Errorf("Unexpected error value for test %d, expecting %t, got %v\n", i, test.err, err)
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
			err: true,
		},
		{ // changed default type => error
			original: &mapping.IndexMappingImpl{
				DefaultType: "a",
			},
			updated: &mapping.IndexMappingImpl{
				DefaultType: "b",
			},
			err: true,
		},
		{ // changed default analyzer => analyser true
			original: &mapping.IndexMappingImpl{
				DefaultAnalyzer: "a",
			},
			updated: &mapping.IndexMappingImpl{
				DefaultAnalyzer: "b",
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
			err: false,
		},
		{ // changed default synonym source => synonym source true
			original: &mapping.IndexMappingImpl{
				DefaultSynonymSource: "a",
			},
			updated: &mapping.IndexMappingImpl{
				DefaultSynonymSource: "b",
			},
			err: false,
		},
		{ // changed default field => false
			original: &mapping.IndexMappingImpl{
				DefaultField: "a",
			},
			updated: &mapping.IndexMappingImpl{
				DefaultField: "b",
			},
			err: false,
		},
		{ // changed index dynamic => error
			original: &mapping.IndexMappingImpl{
				IndexDynamic: true,
			},
			updated: &mapping.IndexMappingImpl{
				IndexDynamic: false,
			},
			err: true,
		},
		{ // changed store dynamic => error
			original: &mapping.IndexMappingImpl{
				StoreDynamic: false,
			},
			updated: &mapping.IndexMappingImpl{
				StoreDynamic: true,
			},
			err: true,
		},
		{ // changed docvalues dynamic => error
			original: &mapping.IndexMappingImpl{
				DocValuesDynamic: true,
			},
			updated: &mapping.IndexMappingImpl{
				DocValuesDynamic: false,
			},
			err: true,
		},
	}

	for i, test := range tests {
		err := compareMappings(test.original, test.updated)

		if err == nil && test.err || err != nil && !test.err {
			t.Errorf("Unexpected error value for test %d, expecting %t, got %v\n", i, test.err, err)
		}
	}
}

func TestCompareAnalysers(t *testing.T) {

	ori := mapping.NewIndexMapping()
	ori.DefaultMapping.AddFieldMappingsAt("a", NewTextFieldMapping())
	ori.DefaultMapping.AddFieldMappingsAt("b", NewTextFieldMapping())
	ori.DefaultMapping.AddFieldMappingsAt("c", NewTextFieldMapping())
	ori.DefaultMapping.Properties["b"].DefaultAnalyzer = "3xbla"
	ori.DefaultMapping.Properties["c"].DefaultAnalyzer = simple.Name

	upd := mapping.NewIndexMapping()
	upd.DefaultMapping.AddFieldMappingsAt("a", NewTextFieldMapping())
	upd.DefaultMapping.AddFieldMappingsAt("b", NewTextFieldMapping())
	upd.DefaultMapping.AddFieldMappingsAt("c", NewTextFieldMapping())
	upd.DefaultMapping.Properties["b"].DefaultAnalyzer = "3xbla"
	upd.DefaultMapping.Properties["c"].DefaultAnalyzer = simple.Name

	if err := ori.AddCustomAnalyzer("3xbla", map[string]interface{}{
		"type":          custom.Name,
		"tokenizer":     whitespace.Name,
		"token_filters": []interface{}{lowercase.Name, "stop_en"},
	}); err != nil {
		t.Fatal(err)
	}

	if err := upd.AddCustomAnalyzer("3xbla", map[string]interface{}{
		"type":          custom.Name,
		"tokenizer":     whitespace.Name,
		"token_filters": []interface{}{lowercase.Name, "stop_en"},
	}); err != nil {
		t.Fatal(err)
	}

	oriPaths := map[string]*pathInfo{
		"a": {
			fieldMapInfo: []*fieldMapInfo{
				{
					fieldMapping: &mapping.FieldMapping{
						Type: "text",
					},
				},
			},
			dynamic:    false,
			path:       "a",
			parentPath: "",
		},
		"b": {
			fieldMapInfo: []*fieldMapInfo{
				{
					fieldMapping: &mapping.FieldMapping{
						Type: "text",
					},
				},
			},
			dynamic:    false,
			path:       "b",
			parentPath: "",
		},
		"c": {
			fieldMapInfo: []*fieldMapInfo{
				{
					fieldMapping: &mapping.FieldMapping{
						Type: "text",
					},
				},
			},
			dynamic:    false,
			path:       "c",
			parentPath: "",
		},
	}

	updPaths := map[string]*pathInfo{
		"a": {
			fieldMapInfo: []*fieldMapInfo{
				{
					fieldMapping: &mapping.FieldMapping{
						Type: "text",
					},
				},
			},
			dynamic:    false,
			path:       "a",
			parentPath: "",
		},
		"b": {
			fieldMapInfo: []*fieldMapInfo{
				{
					fieldMapping: &mapping.FieldMapping{
						Type: "text",
					},
				},
			},
			dynamic:    false,
			path:       "b",
			parentPath: "",
		},
		"c": {
			fieldMapInfo: []*fieldMapInfo{
				{
					fieldMapping: &mapping.FieldMapping{
						Type: "text",
					},
				},
			},
			dynamic:    false,
			path:       "c",
			parentPath: "",
		},
	}

	// Test case has identical analysers for text fields
	err := compareAnalysers(oriPaths, updPaths, ori, upd)
	if err != nil {
		t.Errorf("Expected error to be nil, got %v", err)
	}

	ori2 := mapping.NewIndexMapping()
	ori2.DefaultMapping.AddFieldMappingsAt("a", NewTextFieldMapping())
	ori2.DefaultMapping.AddFieldMappingsAt("b", NewTextFieldMapping())
	ori2.DefaultMapping.AddFieldMappingsAt("c", NewTextFieldMapping())
	ori2.DefaultMapping.Properties["b"].DefaultAnalyzer = "3xbla"
	ori2.DefaultMapping.Properties["c"].DefaultAnalyzer = simple.Name

	upd2 := mapping.NewIndexMapping()
	upd2.DefaultMapping.AddFieldMappingsAt("a", NewTextFieldMapping())
	upd2.DefaultMapping.AddFieldMappingsAt("b", NewTextFieldMapping())
	upd2.DefaultMapping.AddFieldMappingsAt("c", NewTextFieldMapping())
	upd2.DefaultMapping.Properties["b"].DefaultAnalyzer = "3xbla"
	upd2.DefaultMapping.Properties["c"].DefaultAnalyzer = simple.Name

	if err := ori2.AddCustomAnalyzer("3xbla", map[string]interface{}{
		"type":          custom.Name,
		"tokenizer":     whitespace.Name,
		"token_filters": []interface{}{lowercase.Name, "stop_en"},
	}); err != nil {
		t.Fatal(err)
	}

	if err := upd2.AddCustomAnalyzer("3xbla", map[string]interface{}{
		"type":          custom.Name,
		"tokenizer":     letter.Name,
		"token_filters": []interface{}{lowercase.Name, "stop_en"},
	}); err != nil {
		t.Fatal(err)
	}

	// Test case has different custom analyser for field "b"
	err = compareAnalysers(oriPaths, updPaths, ori2, upd2)
	if err == nil {
		t.Errorf("Expected error, got nil")
	}
}

func TestCompareDatetimeParsers(t *testing.T) {

	ori := mapping.NewIndexMapping()
	ori.DefaultMapping.AddFieldMappingsAt("a", NewDateTimeFieldMapping())
	ori.DefaultMapping.AddFieldMappingsAt("b", NewDateTimeFieldMapping())
	ori.DefaultMapping.AddFieldMappingsAt("c", NewDateTimeFieldMapping())
	ori.DefaultMapping.Properties["b"].Fields[0].DateFormat = "customDT"
	ori.DefaultMapping.Properties["c"].Fields[0].DateFormat = percent.Name

	upd := mapping.NewIndexMapping()
	upd.DefaultMapping.AddFieldMappingsAt("a", NewDateTimeFieldMapping())
	upd.DefaultMapping.AddFieldMappingsAt("b", NewDateTimeFieldMapping())
	upd.DefaultMapping.AddFieldMappingsAt("c", NewDateTimeFieldMapping())
	upd.DefaultMapping.Properties["b"].Fields[0].DateFormat = "customDT"
	upd.DefaultMapping.Properties["c"].Fields[0].DateFormat = percent.Name

	err := ori.AddCustomDateTimeParser("customDT", map[string]interface{}{
		"type": sanitized.Name,
		"layouts": []interface{}{
			"02/01/2006 15:04:05",
			"2006/01/02 3:04PM",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	err = upd.AddCustomDateTimeParser("customDT", map[string]interface{}{
		"type": sanitized.Name,
		"layouts": []interface{}{
			"02/01/2006 15:04:05",
			"2006/01/02 3:04PM",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	oriPaths := map[string]*pathInfo{
		"a": {
			fieldMapInfo: []*fieldMapInfo{
				{
					fieldMapping: &mapping.FieldMapping{
						Type: "datetime",
					},
				},
			},
			dynamic:    false,
			path:       "a",
			parentPath: "",
		},
		"b": {
			fieldMapInfo: []*fieldMapInfo{
				{
					fieldMapping: &mapping.FieldMapping{
						Type:       "datetime",
						DateFormat: "customDT",
					},
				},
			},
			dynamic:    false,
			path:       "b",
			parentPath: "",
		},
		"c": {
			fieldMapInfo: []*fieldMapInfo{
				{
					fieldMapping: &mapping.FieldMapping{
						Type: "datetime",
					},
				},
			},
			dynamic:    false,
			path:       "c",
			parentPath: "",
		},
	}

	updPaths := map[string]*pathInfo{
		"a": {
			fieldMapInfo: []*fieldMapInfo{
				{
					fieldMapping: &mapping.FieldMapping{
						Type: "datetime",
					},
				},
			},
			dynamic:    false,
			path:       "a",
			parentPath: "",
		},
		"b": {
			fieldMapInfo: []*fieldMapInfo{
				{
					fieldMapping: &mapping.FieldMapping{
						Type:       "datetime",
						DateFormat: "customDT",
					},
				},
			},
			dynamic:    false,
			path:       "b",
			parentPath: "",
		},
		"c": {
			fieldMapInfo: []*fieldMapInfo{
				{
					fieldMapping: &mapping.FieldMapping{
						Type: "datetime",
					},
				},
			},
			dynamic:    false,
			path:       "c",
			parentPath: "",
		},
	}

	// Test case has identical datetime parsers for all fields
	err = compareDateTimeParsers(oriPaths, updPaths, ori, upd)
	if err != nil {
		t.Fatalf("Expected error to be nil, got %v", err)
	}

	ori2 := mapping.NewIndexMapping()
	ori2.DefaultMapping.AddFieldMappingsAt("a", NewDateTimeFieldMapping())
	ori2.DefaultMapping.AddFieldMappingsAt("b", NewDateTimeFieldMapping())
	ori2.DefaultMapping.AddFieldMappingsAt("c", NewDateTimeFieldMapping())
	ori2.DefaultMapping.Properties["b"].Fields[0].DateFormat = "customDT"
	ori2.DefaultMapping.Properties["c"].Fields[0].DateFormat = percent.Name

	upd2 := mapping.NewIndexMapping()
	upd2.DefaultMapping.AddFieldMappingsAt("a", NewDateTimeFieldMapping())
	upd2.DefaultMapping.AddFieldMappingsAt("b", NewDateTimeFieldMapping())
	upd2.DefaultMapping.AddFieldMappingsAt("c", NewDateTimeFieldMapping())
	upd2.DefaultMapping.Properties["b"].Fields[0].DateFormat = "customDT"
	upd2.DefaultMapping.Properties["c"].Fields[0].DateFormat = percent.Name

	err = ori2.AddCustomDateTimeParser("customDT", map[string]interface{}{
		"type": sanitized.Name,
		"layouts": []interface{}{
			"02/01/2006 15:04:05",
			"2006/01/02 3:04PM",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	err = upd2.AddCustomDateTimeParser("customDT", map[string]interface{}{
		"type": sanitized.Name,
		"layouts": []interface{}{
			"02/01/2006 15:04:05",
			"2006/01/02",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// test case has different custom datetime parser for field "b"
	err = compareDateTimeParsers(oriPaths, updPaths, ori2, upd2)
	if err == nil {
		t.Errorf("Expected error, got nil")
	}
}

func TestCompareSynonymSources(t *testing.T) {

	ori := mapping.NewIndexMapping()
	ori.DefaultMapping.AddFieldMappingsAt("a", NewTextFieldMapping())
	ori.DefaultMapping.AddFieldMappingsAt("b", NewTextFieldMapping())
	ori.DefaultMapping.DefaultSynonymSource = "syn1"
	ori.DefaultMapping.Properties["b"].Fields[0].SynonymSource = "syn2"

	upd := mapping.NewIndexMapping()
	upd.DefaultMapping.AddFieldMappingsAt("a", NewTextFieldMapping())
	upd.DefaultMapping.AddFieldMappingsAt("b", NewTextFieldMapping())
	upd.DefaultMapping.DefaultSynonymSource = "syn1"
	upd.DefaultMapping.Properties["b"].Fields[0].SynonymSource = "syn2"

	err := ori.AddSynonymSource("syn1", map[string]interface{}{
		"collection": "col1",
		"analyzer":   simple.Name,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = ori.AddSynonymSource("syn2", map[string]interface{}{
		"collection": "col2",
		"analyzer":   standard.Name,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = upd.AddSynonymSource("syn1", map[string]interface{}{
		"collection": "col1",
		"analyzer":   simple.Name,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = upd.AddSynonymSource("syn2", map[string]interface{}{
		"collection": "col2",
		"analyzer":   standard.Name,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Test case has identical synonym sources
	err = compareSynonymSources(ori, upd)
	if err != nil {
		t.Errorf("Expected error to be nil, got %v", err)
	}

	ori2 := mapping.NewIndexMapping()
	ori2.DefaultMapping.AddFieldMappingsAt("a", NewTextFieldMapping())
	ori2.DefaultMapping.AddFieldMappingsAt("b", NewTextFieldMapping())
	ori2.DefaultMapping.DefaultSynonymSource = "syn1"
	ori2.DefaultMapping.Properties["b"].Fields[0].SynonymSource = "syn2"

	upd2 := mapping.NewIndexMapping()
	upd2.DefaultMapping.AddFieldMappingsAt("a", NewTextFieldMapping())
	upd2.DefaultMapping.AddFieldMappingsAt("b", NewTextFieldMapping())
	upd2.DefaultMapping.DefaultSynonymSource = "syn1"
	upd2.DefaultMapping.Properties["b"].Fields[0].SynonymSource = "syn2"

	err = ori2.AddSynonymSource("syn1", map[string]interface{}{
		"collection": "col1",
		"analyzer":   simple.Name,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = ori2.AddSynonymSource("syn2", map[string]interface{}{
		"collection": "col2",
		"analyzer":   standard.Name,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = upd2.AddSynonymSource("syn1", map[string]interface{}{
		"collection": "col1",
		"analyzer":   simple.Name,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = upd2.AddSynonymSource("syn2", map[string]interface{}{
		"collection": "col3",
		"analyzer":   standard.Name,
	})
	if err != nil {
		t.Fatal(err)
	}

	// test case has different synonym sources
	err = compareSynonymSources(ori2, upd2)
	if err == nil {
		t.Errorf("Expected error, got nil")
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
			// changed default analyzer with index dynamic
			// => error
			original: &mapping.IndexMappingImpl{
				TypeMapping:      map[string]*mapping.DocumentMapping{},
				DefaultMapping:   &mapping.DocumentMapping{},
				DefaultAnalyzer:  standard.Name,
				IndexDynamic:     true,
				StoreDynamic:     false,
				DocValuesDynamic: false,
				CustomAnalysis:   NewIndexMapping().CustomAnalysis,
			},
			updated: &mapping.IndexMappingImpl{
				TypeMapping:      map[string]*mapping.DocumentMapping{},
				DefaultMapping:   &mapping.DocumentMapping{},
				DefaultAnalyzer:  simple.Name,
				IndexDynamic:     true,
				StoreDynamic:     false,
				DocValuesDynamic: false,
				CustomAnalysis:   NewIndexMapping().CustomAnalysis,
			},
			fieldInfo: nil,
			err:       true,
		},
		{
			// changed default analyzer within a mapping with index dynamic
			// => error
			original: &mapping.IndexMappingImpl{
				TypeMapping: map[string]*mapping.DocumentMapping{},
				DefaultMapping: &mapping.DocumentMapping{
					Enabled:         true,
					Dynamic:         true,
					DefaultAnalyzer: standard.Name,
				},
				DefaultAnalyzer:  "",
				IndexDynamic:     true,
				StoreDynamic:     false,
				DocValuesDynamic: false,
				CustomAnalysis:   NewIndexMapping().CustomAnalysis,
			},
			updated: &mapping.IndexMappingImpl{
				TypeMapping: map[string]*mapping.DocumentMapping{},
				DefaultMapping: &mapping.DocumentMapping{
					Enabled:         true,
					Dynamic:         true,
					DefaultAnalyzer: simple.Name,
				},
				IndexDynamic:     true,
				StoreDynamic:     false,
				DocValuesDynamic: false,
				CustomAnalysis:   NewIndexMapping().CustomAnalysis,
			},
			fieldInfo: nil,
			err:       true,
		},
		{
			// changed default datetime parser with index dynamic
			// => error
			original: &mapping.IndexMappingImpl{
				TypeMapping:           map[string]*mapping.DocumentMapping{},
				DefaultMapping:        &mapping.DocumentMapping{},
				DefaultDateTimeParser: percent.Name,
				IndexDynamic:          true,
				StoreDynamic:          false,
				DocValuesDynamic:      false,
				CustomAnalysis:        NewIndexMapping().CustomAnalysis,
			},
			updated: &mapping.IndexMappingImpl{
				TypeMapping:           map[string]*mapping.DocumentMapping{},
				DefaultMapping:        &mapping.DocumentMapping{},
				DefaultDateTimeParser: sanitized.Name,
				IndexDynamic:          true,
				StoreDynamic:          false,
				DocValuesDynamic:      false,
				CustomAnalysis:        NewIndexMapping().CustomAnalysis,
			},
			fieldInfo: nil,
			err:       true,
		},
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
				CustomAnalysis:   NewIndexMapping().CustomAnalysis,
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
				CustomAnalysis:   NewIndexMapping().CustomAnalysis,
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
				CustomAnalysis:   NewIndexMapping().CustomAnalysis,
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
				CustomAnalysis:   NewIndexMapping().CustomAnalysis,
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
				CustomAnalysis:   NewIndexMapping().CustomAnalysis,
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
				CustomAnalysis:   NewIndexMapping().CustomAnalysis,
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
				CustomAnalysis:   NewIndexMapping().CustomAnalysis,
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
				CustomAnalysis:   NewIndexMapping().CustomAnalysis,
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
				CustomAnalysis:   NewIndexMapping().CustomAnalysis,
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
				CustomAnalysis:   NewIndexMapping().CustomAnalysis,
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
				CustomAnalysis:   NewIndexMapping().CustomAnalysis,
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
				CustomAnalysis:   NewIndexMapping().CustomAnalysis,
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
				CustomAnalysis:   NewIndexMapping().CustomAnalysis,
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
				CustomAnalysis:   NewIndexMapping().CustomAnalysis,
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
				CustomAnalysis:   NewIndexMapping().CustomAnalysis,
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
				CustomAnalysis:   NewIndexMapping().CustomAnalysis,
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
				CustomAnalysis:   NewIndexMapping().CustomAnalysis,
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
				CustomAnalysis:   NewIndexMapping().CustomAnalysis,
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
				CustomAnalysis:   NewIndexMapping().CustomAnalysis,
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
				CustomAnalysis:   NewIndexMapping().CustomAnalysis,
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
					Deleted: true,
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

	config := map[string]interface{}{
		"updated_mapping": string(mappingString),
	}

	index, err = OpenUsing(tmpIndexPath, config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := index.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	q1 := NewSearchRequest(NewQueryStringQuery("a:*"))
	q1.Fields = append(q1.Fields, "a")
	res1, err := index.Search(q1)
	if err != nil {
		t.Fatal(err)
	}
	if len(res1.Hits) != 3 {
		t.Errorf("Expected 3 hits, got %d\n", len(res1.Hits))
	}
	if len(res1.Hits[0].Fields) != 1 {
		t.Errorf("Expected 1 field, got %d\n", len(res1.Hits[0].Fields))
	}
	q2 := NewSearchRequest(NewQueryStringQuery("b:*"))
	q2.Fields = append(q2.Fields, "b")
	res2, err := index.Search(q2)
	if err != nil {
		t.Fatal(err)
	}
	if len(res2.Hits) != 0 {
		t.Errorf("Expected 0 hits, got %d\n", len(res2.Hits))
	}
	q3 := NewSearchRequest(NewQueryStringQuery("c:*"))
	q3.Fields = append(q3.Fields, "c")
	res3, err := index.Search(q3)
	if err != nil {
		t.Fatal(err)
	}
	if len(res3.Hits) != 3 {
		t.Errorf("Expected 3 hits, got %d\n", len(res3.Hits))
	}
	if len(res3.Hits[0].Fields) != 0 {
		t.Errorf("Expected 0 fields, got %d\n", len(res3.Hits[0].Fields))
	}
	q4 := NewSearchRequest(NewQueryStringQuery("d:*"))
	q4.Fields = append(q4.Fields, "d")
	res4, err := index.Search(q4)
	if err != nil {
		t.Fatal(err)
	}
	if len(res4.Hits) != 0 {
		t.Errorf("Expected 0 hits, got %d\n", len(res4.Hits))
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
	config := map[string]interface{}{
		"updated_mapping": string(mappingString),
	}

	index, err = OpenUsing(tmpIndexPath, config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := index.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	q1 := NewSearchRequest(NewQueryStringQuery("a:devoted"))
	res1, err := index.Search(q1)
	if err != nil {
		t.Fatal(err)
	}
	if len(res1.Hits) != 1 {
		t.Errorf("Expected 1 hit, got %d\n", len(res1.Hits))
	}

	q2 := NewSearchRequest(NewQueryStringQuery("b:devoted"))
	res2, err := index.Search(q2)
	if err != nil {
		t.Fatal(err)
	}
	if len(res2.Hits) != 0 {
		t.Errorf("Expected 0 hits, got %d\n", len(res2.Hits))
	}

	q3 := NewSearchRequest(NewQueryStringQuery("c:unruffled"))
	res3, err := index.Search(q3)
	if err != nil {
		t.Fatal(err)
	}
	if len(res3.Hits) != 0 {
		t.Errorf("Expected 0 hits, got %d\n", len(res3.Hits))
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
	numBatches := 10

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
	config := map[string]interface{}{
		"updated_mapping": string(mappingString),
	}

	index, err = OpenUsing(tmpIndexPath, config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := index.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

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
		t.Errorf("Expected 10 hits, got %d\n", len(res1.Hits))
	}
	if len(res1.Hits[0].Fields) != 1 {
		t.Errorf("Expected 1 field, got %d\n", len(res1.Hits[0].Fields))
	}
	q2 := NewSearchRequest(NewQueryStringQuery("b:*"))
	q2.Fields = append(q2.Fields, "b")
	res2, err := index.Search(q2)
	if err != nil {
		t.Fatal(err)
	}
	if len(res2.Hits) != 0 {
		t.Errorf("Expected 0 hits, got %d\n", len(res2.Hits))
	}
	q3 := NewSearchRequest(NewQueryStringQuery("c:*"))
	q3.Fields = append(q3.Fields, "c")
	res3, err := index.Search(q3)
	if err != nil {
		t.Fatal(err)
	}
	if len(res3.Hits) != 10 {
		t.Errorf("Expected 10 hits, got %d\n", len(res3.Hits))
	}
	if len(res3.Hits[0].Fields) != 0 {
		t.Errorf("Expected 0 fields, got %d\n", len(res3.Hits[0].Fields))
	}
	q4 := NewSearchRequest(NewQueryStringQuery("d:*"))
	q4.Fields = append(q4.Fields, "d")
	res4, err := index.Search(q4)
	if err != nil {
		t.Fatal(err)
	}
	if len(res4.Hits) != 0 {
		t.Errorf("Expected 0 hits, got %d\n", len(res4.Hits))
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
	config := map[string]interface{}{
		"updated_mapping": string(mappingString),
	}

	index, err = OpenUsing(tmpIndexPath, config)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		err := index.Close()
		if err != nil {
			b.Fatal(err)
		}
	}()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		q := NewQueryStringQuery("a:*")
		req := NewSearchRequest(q)
		if _, err = index.Search(req); err != nil {
			b.Fatal(err)
		}
	}
}

func TestIndexUpdateNestedMapping(t *testing.T) {
	// Helper: create a mapping with optional nested structure
	createCompanyMapping := func(nestedEmployees, nestedDepartments, nestedProjects, nestedLocations bool) *mapping.IndexMappingImpl {
		rv := mapping.NewIndexMapping()
		companyMapping := mapping.NewDocumentMapping()

		// Basic fields
		companyMapping.AddFieldMappingsAt("id", mapping.NewTextFieldMapping())
		companyMapping.AddFieldMappingsAt("name", mapping.NewTextFieldMapping())

		var deptMapping *mapping.DocumentMapping
		// Departments nested conditionally
		if !nestedDepartments {
			deptMapping = mapping.NewDocumentMapping()
		} else {
			deptMapping = mapping.NewNestedDocumentMapping()
		}
		deptMapping.AddFieldMappingsAt("name", mapping.NewTextFieldMapping())
		deptMapping.AddFieldMappingsAt("budget", mapping.NewNumericFieldMapping())

		// Employees nested conditionally
		var empMapping *mapping.DocumentMapping
		if !nestedEmployees {
			empMapping = mapping.NewNestedDocumentMapping()
		} else {
			empMapping = mapping.NewDocumentMapping()
		}
		empMapping.AddFieldMappingsAt("name", mapping.NewTextFieldMapping())
		empMapping.AddFieldMappingsAt("role", mapping.NewTextFieldMapping())
		deptMapping.AddSubDocumentMapping("employees", empMapping)

		// Projects nested conditionally
		var projMapping *mapping.DocumentMapping
		if !nestedProjects {
			projMapping = mapping.NewNestedDocumentMapping()
		} else {
			projMapping = mapping.NewDocumentMapping()
		}
		projMapping.AddFieldMappingsAt("title", mapping.NewTextFieldMapping())
		projMapping.AddFieldMappingsAt("status", mapping.NewTextFieldMapping())
		deptMapping.AddSubDocumentMapping("projects", projMapping)

		companyMapping.AddSubDocumentMapping("departments", deptMapping)

		// Locations nested conditionally
		var locMapping *mapping.DocumentMapping
		if nestedLocations {
			locMapping = mapping.NewNestedDocumentMapping()
		} else {
			locMapping = mapping.NewDocumentMapping()
		}
		locMapping.AddFieldMappingsAt("address", mapping.NewTextFieldMapping())
		locMapping.AddFieldMappingsAt("city", mapping.NewTextFieldMapping())

		companyMapping.AddSubDocumentMapping("locations", locMapping)

		rv.DefaultMapping.AddSubDocumentMapping("company", companyMapping)
		return rv
	}

	tests := []struct {
		name      string
		original  *mapping.IndexMappingImpl
		updated   *mapping.IndexMappingImpl
		expectErr bool
	}{
		{
			name:      "No nested to all nested",
			original:  createCompanyMapping(false, false, false, false),
			updated:   createCompanyMapping(true, true, true, true),
			expectErr: true,
		},
		{
			name:      "No nested to mixed nested",
			original:  createCompanyMapping(false, false, false, false),
			updated:   createCompanyMapping(true, false, true, false),
			expectErr: true,
		},
		{
			name:      "No nested to mixed nested",
			original:  createCompanyMapping(false, false, false, false),
			updated:   createCompanyMapping(true, true, true, false),
			expectErr: true,
		},
		{
			name:      "Mixed nested to no nested",
			original:  createCompanyMapping(false, true, false, true),
			updated:   createCompanyMapping(false, false, true, true),
			expectErr: true,
		},
		{
			name:      "All nested to no nested",
			original:  createCompanyMapping(true, true, true, true),
			updated:   createCompanyMapping(false, false, false, false),
			expectErr: true,
		},
		{
			name:      "Mixed nested to all nested",
			original:  createCompanyMapping(true, false, true, false),
			updated:   createCompanyMapping(true, true, true, true),
			expectErr: true,
		},
		{
			name:      "All nested to mixed nested",
			original:  createCompanyMapping(true, true, true, true),
			updated:   createCompanyMapping(true, false, true, false),
			expectErr: true,
		},
		{
			name:      "No nested to no nested",
			original:  createCompanyMapping(false, false, false, false),
			updated:   createCompanyMapping(false, false, false, false),
			expectErr: false,
		},
		{
			name:      "All nested to all nested",
			original:  createCompanyMapping(true, true, true, true),
			updated:   createCompanyMapping(true, true, true, true),
			expectErr: false,
		},
	}

	for _, test := range tests {
		_, err := DeletedFields(test.original, test.updated)
		if (err != nil) != test.expectErr {
			t.Errorf("Test '%s' unexpected error state: got %v, expectErr %t", test.name, err, test.expectErr)
		}
	}
}
