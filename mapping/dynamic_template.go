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

package mapping

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/blevesearch/bleve/v2/util"
	"github.com/bmatcuk/doublestar/v4"
)

// DynamicTemplate defines a rule for mapping dynamically detected fields.
// When a field is encountered that has no explicit mapping and dynamic mapping
// is enabled, templates are checked in order. The first matching template's
// mapping is used for the field.
//
// This is similar to Elasticsearch's dynamic_templates feature.
type DynamicTemplate struct {
	// Name is an optional identifier for this template (useful for debugging)
	Name string `json:"name,omitempty"`

	// Match is a glob pattern to match against the field name (last path element).
	// Supports * and ** wildcards via doublestar library.
	// Example: "*_text" matches "title_text", "body_text"
	Match string `json:"match,omitempty"`

	// Unmatch is a glob pattern; if it matches the field name, the template is skipped.
	// Example: "skip_*" would exclude fields like "skip_this"
	Unmatch string `json:"unmatch,omitempty"`

	// PathMatch is a glob pattern to match against the full dotted path.
	// Supports ** for matching multiple path segments.
	// Example: "metadata.**" matches "metadata.author", "metadata.tags.primary"
	PathMatch string `json:"path_match,omitempty"`

	// PathUnmatch is a glob pattern; if it matches the full path, the template is skipped.
	PathUnmatch string `json:"path_unmatch,omitempty"`

	// MatchMappingType filters by the detected data type.
	// Valid values: "string", "number", "boolean", "date", "object"
	MatchMappingType string `json:"match_mapping_type,omitempty"`

	// Mapping is the field mapping to apply when this template matches.
	Mapping *FieldMapping `json:"mapping,omitempty"`
}

// NewDynamicTemplate creates a new DynamicTemplate with the given name.
func NewDynamicTemplate(name string) *DynamicTemplate {
	return &DynamicTemplate{Name: name}
}

// MatchField sets the field name pattern to match.
func (t *DynamicTemplate) MatchField(pattern string) *DynamicTemplate {
	t.Match = pattern
	return t
}

// UnmatchField sets the field name pattern to exclude.
func (t *DynamicTemplate) UnmatchField(pattern string) *DynamicTemplate {
	t.Unmatch = pattern
	return t
}

// MatchPath sets the path pattern to match.
func (t *DynamicTemplate) MatchPath(pattern string) *DynamicTemplate {
	t.PathMatch = pattern
	return t
}

// UnmatchPath sets the path pattern to exclude.
func (t *DynamicTemplate) UnmatchPath(pattern string) *DynamicTemplate {
	t.PathUnmatch = pattern
	return t
}

// MatchType sets the mapping type to match.
func (t *DynamicTemplate) MatchType(typeName string) *DynamicTemplate {
	t.MatchMappingType = typeName
	return t
}

// WithMapping sets the field mapping to use when this template matches.
func (t *DynamicTemplate) WithMapping(m *FieldMapping) *DynamicTemplate {
	t.Mapping = m
	return t
}

// Matches checks if this template matches the given field.
// All specified criteria must match for the template to be considered a match.
func (t *DynamicTemplate) Matches(fieldName, pathStr, detectedType string) bool {
	// Check match_mapping_type first (most selective usually)
	if t.MatchMappingType != "" && t.MatchMappingType != detectedType {
		return false
	}

	// Check field name match pattern
	if t.Match != "" {
		matched, err := doublestar.Match(t.Match, fieldName)
		if err != nil || !matched {
			return false
		}
	}

	// Check field name unmatch pattern (exclusion)
	if t.Unmatch != "" {
		matched, err := doublestar.Match(t.Unmatch, fieldName)
		if err == nil && matched {
			return false
		}
	}

	// Check path match pattern
	if t.PathMatch != "" {
		matched, err := doublestar.Match(t.PathMatch, pathStr)
		if err != nil || !matched {
			return false
		}
	}

	// Check path unmatch pattern (exclusion)
	if t.PathUnmatch != "" {
		matched, err := doublestar.Match(t.PathUnmatch, pathStr)
		if err == nil && matched {
			return false
		}
	}

	return true
}

// detectMappingType returns the bleve mapping type name for a reflected value.
// This is used to match against MatchMappingType in dynamic templates.
func detectMappingType(val reflect.Value) string {
	if !val.IsValid() {
		return ""
	}

	switch val.Kind() {
	case reflect.String:
		return "string"
	case reflect.Float32, reflect.Float64,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "number"
	case reflect.Bool:
		return "boolean"
	case reflect.Struct:
		// Check if it's a time.Time
		if val.Type() == reflect.TypeOf(time.Time{}) {
			return "date"
		}
		return "object"
	case reflect.Map:
		return "object"
	case reflect.Slice, reflect.Array:
		return "array"
	case reflect.Ptr:
		if !val.IsNil() {
			return detectMappingType(val.Elem())
		}
		return ""
	default:
		return ""
	}
}

// UnmarshalJSON offers custom unmarshaling with optional strict validation
func (t *DynamicTemplate) UnmarshalJSON(data []byte) error {
	var tmp map[string]json.RawMessage
	err := util.UnmarshalJSON(data, &tmp)
	if err != nil {
		return err
	}

	var invalidKeys []string
	for k, v := range tmp {
		switch k {
		case "name":
			err := util.UnmarshalJSON(v, &t.Name)
			if err != nil {
				return err
			}
		case "match":
			err := util.UnmarshalJSON(v, &t.Match)
			if err != nil {
				return err
			}
		case "unmatch":
			err := util.UnmarshalJSON(v, &t.Unmatch)
			if err != nil {
				return err
			}
		case "path_match":
			err := util.UnmarshalJSON(v, &t.PathMatch)
			if err != nil {
				return err
			}
		case "path_unmatch":
			err := util.UnmarshalJSON(v, &t.PathUnmatch)
			if err != nil {
				return err
			}
		case "match_mapping_type":
			err := util.UnmarshalJSON(v, &t.MatchMappingType)
			if err != nil {
				return err
			}
		case "mapping":
			err := util.UnmarshalJSON(v, &t.Mapping)
			if err != nil {
				return err
			}
		default:
			invalidKeys = append(invalidKeys, k)
		}
	}

	if MappingJSONStrict && len(invalidKeys) > 0 {
		return fmt.Errorf("dynamic template contains invalid keys: %v", invalidKeys)
	}

	return nil
}
