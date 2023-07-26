//go:build !densevector
// +build !densevector

package mapping

import (
	"encoding/json"
	"fmt"
)

// A FieldMapping describes how a specific item
// should be put into the index.
type FieldMapping struct {
	Name string `json:"name,omitempty"`
	Type string `json:"type,omitempty"`

	// Analyzer specifies the name of the analyzer to use for this field.  If
	// Analyzer is empty, traverse the DocumentMapping tree toward the root and
	// pick the first non-empty DefaultAnalyzer found. If there is none, use
	// the IndexMapping.DefaultAnalyzer.
	Analyzer string `json:"analyzer,omitempty"`

	// Store indicates whether to store field values in the index. Stored
	// values can be retrieved from search results using SearchRequest.Fields.
	Store bool `json:"store,omitempty"`
	Index bool `json:"index,omitempty"`

	// IncludeTermVectors, if true, makes terms occurrences to be recorded for
	// this field. It includes the term position within the terms sequence and
	// the term offsets in the source document field. Term vectors are required
	// to perform phrase queries or terms highlighting in source documents.
	IncludeTermVectors bool   `json:"include_term_vectors,omitempty"`
	IncludeInAll       bool   `json:"include_in_all,omitempty"`
	DateFormat         string `json:"date_format,omitempty"`

	// DocValues, if true makes the index uninverting possible for this field
	// It is useful for faceting and sorting queries.
	DocValues bool `json:"docvalues,omitempty"`

	// SkipFreqNorm, if true, avoids the indexing of frequency and norm values
	// of the tokens for this field. This option would be useful for saving
	// the processing of freq/norm details when the default score based relevancy
	// isn't needed.
	SkipFreqNorm bool `json:"skip_freq_norm,omitempty"`
}

func NewDenseVectorFieldMapping() *FieldMapping {
	return nil
}

func (fm *FieldMapping) processDenseVector(propertyMightBeDenseVector interface{},
	pathString string, path []string, indexes []uint64, context *walkContext) {
	return
}

// UnmarshalJSON offers custom unmarshaling with optional strict validation
func (fm *FieldMapping) UnmarshalJSON(data []byte) error {

	var tmp map[string]json.RawMessage
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}

	var invalidKeys []string
	for k, v := range tmp {
		switch k {
		case "name":
			err := json.Unmarshal(v, &fm.Name)
			if err != nil {
				return err
			}
		case "type":
			err := json.Unmarshal(v, &fm.Type)
			if err != nil {
				return err
			}
		case "analyzer":
			err := json.Unmarshal(v, &fm.Analyzer)
			if err != nil {
				return err
			}
		case "store":
			err := json.Unmarshal(v, &fm.Store)
			if err != nil {
				return err
			}
		case "index":
			err := json.Unmarshal(v, &fm.Index)
			if err != nil {
				return err
			}
		case "include_term_vectors":
			err := json.Unmarshal(v, &fm.IncludeTermVectors)
			if err != nil {
				return err
			}
		case "include_in_all":
			err := json.Unmarshal(v, &fm.IncludeInAll)
			if err != nil {
				return err
			}
		case "date_format":
			err := json.Unmarshal(v, &fm.DateFormat)
			if err != nil {
				return err
			}
		case "docvalues":
			err := json.Unmarshal(v, &fm.DocValues)
			if err != nil {
				return err
			}
		case "skip_freq_norm":
			err := json.Unmarshal(v, &fm.SkipFreqNorm)
			if err != nil {
				return err
			}
		default:
			invalidKeys = append(invalidKeys, k)
		}
	}

	if MappingJSONStrict && len(invalidKeys) > 0 {
		return fmt.Errorf("field mapping contains invalid keys: %v", invalidKeys)
	}

	return nil
}
