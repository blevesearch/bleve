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
	"fmt"

	"github.com/blevesearch/bleve/v2/mapping"
	index "github.com/blevesearch/bleve_index_api"
)

// Store all the fields that interact with the data
// from a document path
type pathInfo struct {
	fieldMapInfo []*fieldMapInfo
	dynamic      bool
	path         string
	parentPath   string
}

// Store the field information with respect to the
// document paths
type fieldMapInfo struct {
	fieldMapping *mapping.FieldMapping
	rootName     string
	parent       *pathInfo
}

// Store all of the changes to defaults
type defaultInfo struct {
	analyzer       bool
	dateTimeParser bool
	synonymSource  bool
}

// Compare two index mappings to identify all of the updatable changes
func DeletedFields(ori, upd *mapping.IndexMappingImpl) (map[string]*index.UpdateFieldInfo, error) {
	var err error

	defaultChanges, err := compareMappings(ori, upd)
	if err != nil {
		return nil, err
	}

	// Check for new mappings present in the type mappings
	// of the updated compared to the original
	for name, updDMapping := range upd.TypeMapping {
		err = checkUpdatedMapping(ori.TypeMapping[name], updDMapping)
		if err != nil {
			return nil, err
		}
	}

	// Check for new mappings present in the default mappings
	// of the updated compared to the original
	err = checkUpdatedMapping(ori.DefaultMapping, upd.DefaultMapping)
	if err != nil {
		return nil, err
	}

	oriPaths := make(map[string]*pathInfo)
	updPaths := make(map[string]*pathInfo)

	// Go through each mapping present in the original
	// and consolidate according to the document paths
	for name, oriDMapping := range ori.TypeMapping {
		addPathInfo(oriPaths, "", oriDMapping, ori, nil, name)
	}
	addPathInfo(oriPaths, "", ori.DefaultMapping, ori, nil, "")

	// Go through each mapping present in the updated
	// and consolidate according to the document paths
	for name, updDMapping := range upd.TypeMapping {
		addPathInfo(updPaths, "", updDMapping, ori, nil, name)
	}
	addPathInfo(updPaths, "", upd.DefaultMapping, ori, nil, "")

	// Compare both the mappings based on the document paths
	// and create a list of index, docvalues, store differences
	// for every single field possible
	fieldInfo := make(map[string]*index.UpdateFieldInfo)
	for path, info := range oriPaths {
		err = addFieldInfo(fieldInfo, info, updPaths[path], defaultChanges)
		if err != nil {
			return nil, err
		}
	}

	// Remove entries from the list with no changes between the
	// original and the updated mapping
	for name, info := range fieldInfo {
		if !info.RemoveAll && !info.Index && !info.DocValues && !info.Store {
			delete(fieldInfo, name)
		}
		if info.RemoveAll {
			if upd.IndexDynamic {
				return nil, fmt.Errorf("Mapping cannot be removed when index dynamic is true")
			}
			if upd.StoreDynamic {
				return nil, fmt.Errorf("Mapping cannot be removed when store dynamic is true")
			}
			if upd.DocValuesDynamic {
				return nil, fmt.Errorf("Mapping cannot be removed when docvalues dynamic is true")
			}
		}
	}
	return fieldInfo, nil
}

func compareMappings(ori, upd *mapping.IndexMappingImpl) (*defaultInfo, error) {
	rv := &defaultInfo{}

	if ori.TypeField != upd.TypeField &&
		(len(ori.TypeMapping) != 0 || len(upd.TypeMapping) != 0) {
		return nil, fmt.Errorf("type field cannot be changed when type mappings are present")
	}

	if ori.DefaultType != upd.DefaultType {
		return nil, fmt.Errorf("default type cannot be changed")
	}

	if ori.DefaultAnalyzer != upd.DefaultAnalyzer {
		rv.analyzer = true
	}

	if ori.DefaultDateTimeParser != upd.DefaultDateTimeParser {
		rv.dateTimeParser = true
	}

	if ori.DefaultSynonymSource != upd.DefaultSynonymSource {
		rv.synonymSource = true
	}

	if ori.DefaultField != upd.DefaultField {
		return nil, fmt.Errorf("default field cannot be changed")
	}

	if ori.IndexDynamic != upd.IndexDynamic {
		return nil, fmt.Errorf("index dynamic cannot be changed")
	}

	if ori.StoreDynamic != upd.StoreDynamic {
		return nil, fmt.Errorf(("store dynamic cannot be changed"))
	}

	if ori.DocValuesDynamic != upd.DocValuesDynamic {
		return nil, fmt.Errorf(("docvalues dynamic cannot be changed"))
	}

	return rv, nil
}

// Ensures updated document mapping does not contain new
// field mappings or document mappings
func checkUpdatedMapping(ori, upd *mapping.DocumentMapping) error {

	// Check to verify both original and updated are not nil
	// and are enabled before proceeding
	if ori == nil {
		if upd == nil || !upd.Enabled {
			return nil
		}
		return fmt.Errorf("updated index mapping contains new properties")
	}

	if upd == nil || !upd.Enabled {
		return nil
	}

	var err error
	// Recursively go through the child mappings
	for name, updDMapping := range upd.Properties {
		err = checkUpdatedMapping(ori.Properties[name], updDMapping)
		if err != nil {
			return err
		}
	}

	// Simple checks to ensure no new field mappings present
	// in updated
	for _, updFMapping := range upd.Fields {
		var oriFMapping *mapping.FieldMapping

		for _, fMapping := range ori.Fields {
			if updFMapping.Name == fMapping.Name {
				oriFMapping = fMapping
			}
		}
		if oriFMapping == nil {
			return fmt.Errorf("updated index mapping contains new fields")
		}
	}

	return nil
}

// Adds all of the field mappings while maintaining a tree of the document structure
// to ensure traversal and verification is possible incase of multiple mappings defined
// for a single field or multiple document fields' data getting written to a single zapx field
func addPathInfo(paths map[string]*pathInfo, name string, mp *mapping.DocumentMapping,
	im *mapping.IndexMappingImpl, parent *pathInfo, rootName string) {

	// Early exit if mapping has been disabled
	// Comparisions later on will be done with a nil object
	if !mp.Enabled {
		return
	}

	// Consolidate path information like index dynamic across multiple
	// mappings if path is the same
	var pInfo *pathInfo
	if val, ok := paths[name]; ok {
		pInfo = val
	} else {
		pInfo = &pathInfo{
			fieldMapInfo: make([]*fieldMapInfo, 0),
		}
		pInfo.dynamic = mp.Dynamic && im.IndexDynamic
	}

	pInfo.dynamic = (pInfo.dynamic || mp.Dynamic) && im.IndexDynamic
	pInfo.path = name
	if parent != nil {
		pInfo.parentPath = parent.path
	}

	// Recursively add path information for all child mappings
	for cName, cMapping := range mp.Properties {
		var pathName string
		if name == "" {
			pathName = cName
		} else {
			pathName = name + "." + cName
		}
		addPathInfo(paths, pathName, cMapping, im, pInfo, rootName)
	}

	// Add field mapping information keeping the document structure intact
	for _, fMap := range mp.Fields {
		fieldMapInfo := &fieldMapInfo{
			fieldMapping: fMap,
			rootName:     rootName,
			parent:       pInfo,
		}
		pInfo.fieldMapInfo = append(pInfo.fieldMapInfo, fieldMapInfo)
	}

	paths[name] = pInfo
}

// Compare all of the fields at a particular document path and add its field information
func addFieldInfo(fInfo map[string]*index.UpdateFieldInfo, ori, upd *pathInfo, defaultChanges *defaultInfo) error {

	var info *index.UpdateFieldInfo
	var updated bool
	var err error

	// Assume deleted or disabled mapping if upd is nil. Checks for ori being nil
	// or upd having mappings not in orihave already been done before this stage
	if upd == nil {
		for _, oriFMapInfo := range ori.fieldMapInfo {
			info, updated, err = compareFieldMapping(oriFMapInfo.fieldMapping, nil, defaultChanges)
			if err != nil {
				return err
			}
			err = validateFieldInfo(info, updated, fInfo, ori, oriFMapInfo)
			if err != nil {
				return err
			}
		}
	} else {
		for _, oriFMapInfo := range ori.fieldMapInfo {
			var updFMap *mapping.FieldMapping
			// For multiple fields at a single document path, compare
			// only with the matching ones
			for _, updFMapInfo := range upd.fieldMapInfo {
				if oriFMapInfo.rootName == updFMapInfo.rootName &&
					oriFMapInfo.fieldMapping.Name == updFMapInfo.fieldMapping.Name {
					updFMap = updFMapInfo.fieldMapping
				}
			}

			info, updated, err = compareFieldMapping(oriFMapInfo.fieldMapping, updFMap, defaultChanges)
			if err != nil {
				return err
			}
			err = validateFieldInfo(info, updated, fInfo, ori, oriFMapInfo)
			if err != nil {
				return err
			}
		}
	}
	if err != nil {
		return err
	}

	return nil
}

// Compares two field mappings against each other, checking for changes in index, store, doc values
// and complete deletiion of the mapping while noting that the changes made are doable based on
// other values like includeInAll and dynamic
// first return argument gives an empty fieldInfo if no changes detected
// second return argument gives a flag indicating whether any changes, if detected, are doable or if
// update is impossible
// third argument is an error explaining exactly why the change is not possible
func compareFieldMapping(original, updated *mapping.FieldMapping, defaultChanges *defaultInfo) (*index.UpdateFieldInfo, bool, error) {

	rv := &index.UpdateFieldInfo{}

	if updated == nil {
		if original != nil && !original.IncludeInAll {
			rv.RemoveAll = true
			return rv, true, nil
		} else if original == nil {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("deleted field present in '_all' field")
	} else if original == nil {
		return nil, false, fmt.Errorf("matching field not found in original index mapping")
	}

	if original.Type != updated.Type {
		return nil, false, fmt.Errorf("field type cannot be updated")
	}
	if original.Type == "text" {
		if original.SynonymSource != updated.SynonymSource {
			return nil, false, fmt.Errorf("synonym source cannot be changed for text field")
		} else if original.SynonymSource == "inherit" && defaultChanges.synonymSource {
			return nil, false, fmt.Errorf("synonym source cannot be changed for possible inherited text field")
		}
		if original.Analyzer != updated.Analyzer {
			return nil, false, fmt.Errorf("analyzer cannot be updated for text fields")
		} else if original.Analyzer == "inherit" && defaultChanges.analyzer {
			return nil, false, fmt.Errorf("default analyzer changed for possible inherited text field")
		}
	}
	if original.Type == "datetime" {
		if original.DateFormat != updated.DateFormat {
			return nil, false, fmt.Errorf("dateFormat cannot be updated for datetime fields")
		} else if original.DateFormat == "inherit" && defaultChanges.dateTimeParser {
			return nil, false, fmt.Errorf("default analyzer changed for possible inherited text field")
		}
	}
	if original.Type == "vector" || original.Type == "vector_base64" {
		if original.Dims != updated.Dims {
			return nil, false, fmt.Errorf("dimensions cannot be updated for vector and vector_base64 fields")
		}
		if original.Similarity != updated.Similarity {
			return nil, false, fmt.Errorf("similarity cannot be updated for vector and vector_base64 fields")
		}
		if original.VectorIndexOptimizedFor != updated.VectorIndexOptimizedFor {
			return nil, false, fmt.Errorf("vectorIndexOptimizedFor cannot be updated for vector and vector_base64 fields")
		}
	}
	if original.IncludeInAll != updated.IncludeInAll {
		return nil, false, fmt.Errorf("includeInAll cannot be changed")
	}
	if original.IncludeTermVectors != updated.IncludeTermVectors {
		return nil, false, fmt.Errorf("includeTermVectors cannot be changed")
	}
	if original.SkipFreqNorm != updated.SkipFreqNorm {
		return nil, false, fmt.Errorf("skipFreqNorm cannot be changed")
	}

	// Updating is not possible if store changes from true
	// to false when the field is included in _all
	if original.Store != updated.Store {
		if updated.Store || updated.IncludeInAll {
			return nil, false, fmt.Errorf("store cannot be changed if field present in `_all' field")
		} else {
			rv.Store = true
		}
	}

	// Updating is not possible if index changes from true
	// to false when the field is included in _all
	if original.Index != updated.Index {
		if updated.Index || updated.IncludeInAll {
			return nil, false, fmt.Errorf("index cannot be changed if field present in `_all' field")
		} else {
			rv.Index = true
			rv.DocValues = true
		}
	}

	// Updating is not possible if docvalues changes from true
	// to false when the field is included in _all
	if original.DocValues != updated.DocValues {
		if updated.DocValues || updated.IncludeInAll {
			return nil, false, fmt.Errorf("docvalues cannot be changed if field present in `_all' field")
		} else {
			rv.DocValues = true
		}
	}

	if rv.RemoveAll || rv.Index || rv.Store || rv.DocValues {
		return rv, true, nil
	}
	return rv, false, nil
}

// After identifying changes, validate against the existing changes incase of duplicate fields.
// In such a situation, any conflicting changes found will abort the update process
func validateFieldInfo(newInfo *index.UpdateFieldInfo, updated bool, fInfo map[string]*index.UpdateFieldInfo,
	ori *pathInfo, oriFMapInfo *fieldMapInfo) error {

	var name string
	if oriFMapInfo.parent.parentPath == "" {
		if oriFMapInfo.fieldMapping.Name == "" {
			name = oriFMapInfo.parent.path
		} else {
			name = oriFMapInfo.fieldMapping.Name
		}
	} else {
		if oriFMapInfo.fieldMapping.Name == "" {
			name = oriFMapInfo.parent.parentPath + "." + oriFMapInfo.parent.path
		} else {
			name = oriFMapInfo.parent.parentPath + "." + oriFMapInfo.fieldMapping.Name
		}
	}
	if updated {
		if ori.dynamic {
			return fmt.Errorf("updated field is under a dynamic property")
		}
	}
	if oldInfo, ok := fInfo[name]; ok {
		if oldInfo.RemoveAll != newInfo.RemoveAll || oldInfo.Index != newInfo.Index ||
			oldInfo.DocValues != newInfo.DocValues || oldInfo.Store != newInfo.Store {
			return fmt.Errorf("updated field impossible to verify because multiple mappings point to the same field name")
		}
	} else {
		fInfo[name] = newInfo
	}
	return nil
}
