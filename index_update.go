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

// Compare two index mappings to identify all of the updatable changes
func DeletedFields(ori, upd *mapping.IndexMappingImpl) (map[string]*index.FieldInfo, error) {
	var err error

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
	fieldInfo := make(map[string]*index.FieldInfo)
	for path, info := range oriPaths {
		err = addFieldInfo(fieldInfo, info, updPaths[path])
		if err != nil {
			return nil, err
		}
	}

	// Remove entries from the list with no changes between the
	// original and the updated mapping
	for name, info := range fieldInfo {
		if !info.All && !info.Index && !info.DocValues && !info.Store {
			delete(fieldInfo, name)
		}
	}
	return fieldInfo, nil
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
func addFieldInfo(fInfo map[string]*index.FieldInfo, ori, upd *pathInfo) error {

	var info *index.FieldInfo
	var updated bool
	var err error

	// Assume deleted or disabled mapping if upd is nil. Checks for ori being nil
	// or upd having mappings not in orihave already been done before this stage
	if upd == nil {
		for _, oriFMapInfo := range ori.fieldMapInfo {
			info, updated, err = compareFieldMapping(oriFMapInfo.fieldMapping, nil)
			if err != nil {
				return err
			}
			err = validateFieldInfo(info, updated, fInfo, ori)
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

			info, updated, err = compareFieldMapping(oriFMapInfo.fieldMapping, updFMap)
			if err != nil {
				return err
			}
			err = validateFieldInfo(info, updated, fInfo, ori)
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
func compareFieldMapping(original, updated *mapping.FieldMapping) (*index.FieldInfo, bool, error) {

	rv := &index.FieldInfo{}

	if updated == nil {
		if original != nil && !original.IncludeInAll {
			rv.All = true
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
	if original.Analyzer != updated.Analyzer && original.Type == "text" {
		return nil, false, fmt.Errorf("analyzer cannot be updated for text fields")
	}
	if original.DateFormat != updated.DateFormat && original.Type == "datetime" {
		return nil, false, fmt.Errorf("dateFormat cannot be updated for datetime fields")
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

	if rv.All || rv.Index || rv.Store {
		return rv, true, nil
	}
	return rv, false, nil
}

// After identifying changes, validate against the existing changes incase of duplicate fields.
// In such a situation, any conflicting changes found will abort the update process
func validateFieldInfo(newInfo *index.FieldInfo, updated bool, fInfo map[string]*index.FieldInfo,
	ori *pathInfo) error {

	var name string
	if ori.fieldMapInfo[0].parent.parentPath == "" {
		name = ori.fieldMapInfo[0].fieldMapping.Name
	} else {
		name = ori.fieldMapInfo[0].parent.parentPath + "." + ori.fieldMapInfo[0].fieldMapping.Name
	}
	if updated {
		if ori.dynamic {
			return fmt.Errorf("updated field is under a dynamic property")
		}
	}
	if oldInfo, ok := fInfo[name]; ok {
		if oldInfo.All != newInfo.All || oldInfo.Index != newInfo.Index ||
			oldInfo.DocValues != newInfo.DocValues || oldInfo.Store != newInfo.Store {
			return fmt.Errorf("updated field impossible to verify because multiple mappings point to the same field name")
		}
	} else {
		fInfo[name] = newInfo
	}
	return nil
}
