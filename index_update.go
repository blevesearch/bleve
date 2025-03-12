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
	"reflect"

	"github.com/blevesearch/bleve/v2/analysis"
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
	fieldMapping   *mapping.FieldMapping
	analyzer       string
	datetimeParser string
	synonymSource  string
	rootName       string
	parent         *pathInfo
}

// Compare two index mappings to identify all of the updatable changes
func DeletedFields(ori, upd *mapping.IndexMappingImpl) (map[string]*index.UpdateFieldInfo, error) {
	// Compare all of the top level fields in an index mapping
	err := compareMappings(ori, upd)
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
		addPathInfo(updPaths, "", updDMapping, upd, nil, name)
	}
	addPathInfo(updPaths, "", upd.DefaultMapping, upd, nil, "")

	// Compare all components of custom analysis currently in use
	err = compareCustomComponents(oriPaths, updPaths, ori, upd)
	if err != nil {
		return nil, err
	}

	// Compare both the mappings based on the document paths
	// and create a list of index, docvalues, store differences
	// for every single field possible
	fieldInfo := make(map[string]*index.UpdateFieldInfo)
	for path, info := range oriPaths {
		err = addFieldInfo(fieldInfo, info, updPaths[path])
		if err != nil {
			return nil, err
		}
	}

	// Remove entries from the list with no changes between the
	// original and the updated mapping
	for name, info := range fieldInfo {
		if !info.Deleted && !info.Index && !info.DocValues && !info.Store {
			delete(fieldInfo, name)
		}
		// A field cannot be completely deleted with any dynamic value turned on
		if info.Deleted {
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

// Ensures non of the top level index mapping fields have changed
func compareMappings(ori, upd *mapping.IndexMappingImpl) error {
	if ori.TypeField != upd.TypeField &&
		(len(ori.TypeMapping) != 0 || len(upd.TypeMapping) != 0) {
		return fmt.Errorf("type field cannot be changed when type mappings are present")
	}

	if ori.DefaultType != upd.DefaultType {
		return fmt.Errorf("default type cannot be changed")
	}

	if ori.DefaultField != upd.DefaultField {
		return fmt.Errorf("default field cannot be changed")
	}

	if ori.IndexDynamic != upd.IndexDynamic {
		return fmt.Errorf("index dynamic cannot be changed")
	}

	if ori.StoreDynamic != upd.StoreDynamic {
		return fmt.Errorf(("store dynamic cannot be changed"))
	}

	if ori.DocValuesDynamic != upd.DocValuesDynamic {
		return fmt.Errorf(("docvalues dynamic cannot be changed"))
	}

	return nil
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

// Compares all of the custom analysis components in use
func compareCustomComponents(oriPaths, updPaths map[string]*pathInfo, ori, upd *mapping.IndexMappingImpl) error {
	// Compare all analysers currently in use
	err := compareAnalysers(oriPaths, updPaths, ori, upd)
	if err != nil {
		return err
	}

	// Compare all datetime parsers currently in use
	err = compareDateTimeParsers(oriPaths, updPaths, ori, upd)
	if err != nil {
		return err
	}

	// Compare all synonum sources
	err = compareSynonymSources(oriPaths, updPaths, ori, upd)
	if err != nil {
		return err
	}

	return nil
}

// Compares all analysers currently in use
// Standard analysers not in custom analysis are not compared
// Analysers in custom analysis but not in use are not compared
func compareAnalysers(oriPaths, updPaths map[string]*pathInfo, ori, upd *mapping.IndexMappingImpl) error {
	oriAnalyzers := make(map[string]interface{})
	updAnalyzers := make(map[string]interface{})
	oriCustomAnalysers := ori.CustomAnalysis.Analyzers
	updCustomAnalysers := upd.CustomAnalysis.Analyzers

	for path, info := range oriPaths {
		if len(info.fieldMapInfo) == 0 {
			continue
		}
		for _, fInfo := range info.fieldMapInfo {
			if fInfo.fieldMapping.Type == "text" {
				analyzerName := ori.AnalyzerNameForPath(path)
				fInfo.analyzer = analyzerName
				if val, ok := oriCustomAnalysers[analyzerName]; ok {
					oriAnalyzers[analyzerName] = val
				}
			}
		}
	}

	for path, info := range updPaths {
		if len(info.fieldMapInfo) == 0 {
			continue
		}
		for _, fInfo := range info.fieldMapInfo {
			if fInfo.fieldMapping.Type == "text" {
				analyzerName := upd.AnalyzerNameForPath(path)
				fInfo.analyzer = analyzerName
				if val, ok := updCustomAnalysers[analyzerName]; ok {
					updAnalyzers[analyzerName] = val
				}
			}
		}
	}

	for name, anUpd := range updAnalyzers {
		if anOri, ok := oriAnalyzers[name]; ok {
			if !reflect.DeepEqual(anUpd, anOri) {
				return fmt.Errorf("analyser %s changed while being used by fields", name)
			}
		} else {
			return fmt.Errorf("analyser %s newly added to an existing field", name)
		}
	}

	return nil
}

// Compares all date time parsers currently in use
// Date time parsers in custom analysis but not in use are not compared
func compareDateTimeParsers(oriPaths, updPaths map[string]*pathInfo, ori, upd *mapping.IndexMappingImpl) error {
	oriDateTimeParsers := make(map[string]analysis.DateTimeParser)
	updDateTimeParsers := make(map[string]analysis.DateTimeParser)

	for _, info := range oriPaths {
		if len(info.fieldMapInfo) == 0 {
			continue
		}
		for _, fInfo := range info.fieldMapInfo {
			if fInfo.fieldMapping.Type == "datetime" {
				if fInfo.fieldMapping.DateFormat == "" {
					fInfo.datetimeParser = ori.DefaultDateTimeParser
					oriDateTimeParsers[ori.DefaultDateTimeParser] = ori.DateTimeParserNamed(ori.DefaultDateTimeParser)
				} else {
					oriDateTimeParsers[fInfo.fieldMapping.DateFormat] = ori.DateTimeParserNamed(fInfo.fieldMapping.DateFormat)
				}
			}
		}
	}

	for _, info := range updPaths {
		if len(info.fieldMapInfo) == 0 {
			continue
		}
		for _, fInfo := range info.fieldMapInfo {
			if fInfo.fieldMapping.Type == "datetime" {
				if fInfo.fieldMapping.DateFormat == "" {
					fInfo.datetimeParser = upd.DefaultDateTimeParser
					updDateTimeParsers[upd.DefaultDateTimeParser] = upd.DateTimeParserNamed(upd.DefaultDateTimeParser)
				} else {
					fInfo.datetimeParser = fInfo.fieldMapping.DateFormat
					updDateTimeParsers[fInfo.fieldMapping.DateFormat] = upd.DateTimeParserNamed(fInfo.fieldMapping.DateFormat)
				}
			}
		}
	}

	for name, dtUpd := range updDateTimeParsers {
		if dtOri, ok := oriDateTimeParsers[name]; ok {
			if !reflect.DeepEqual(dtUpd, dtOri) {
				return fmt.Errorf("datetime parser %s changed while being used by fields", name)
			}
		} else {
			return fmt.Errorf("datetime parser %s added to an existing field", name)
		}
	}

	return nil
}

// Compares all synonym sources
// Synonym sources currently not in use are also compared
func compareSynonymSources(oriPaths, updPaths map[string]*pathInfo, ori, upd *mapping.IndexMappingImpl) error {
	oriSynonymSources := make(map[string]analysis.SynonymSource)
	updSynonymSources := make(map[string]analysis.SynonymSource)

	for path, info := range oriPaths {
		if len(info.fieldMapInfo) == 0 {
			continue
		}
		for _, fInfo := range info.fieldMapInfo {
			if fInfo.fieldMapping.Type == "text" {
				synonymSourceName := ori.SynonymSourceForPath(path)
				fInfo.synonymSource = synonymSourceName
				oriSynonymSources[synonymSourceName] = ori.SynonymSourceNamed(synonymSourceName)
			}
		}
	}

	for path, info := range updPaths {
		if len(info.fieldMapInfo) == 0 {
			continue
		}
		for _, fInfo := range info.fieldMapInfo {
			if fInfo.fieldMapping.Type == "text" {
				synonymSourceName := upd.SynonymSourceForPath(path)
				fInfo.synonymSource = synonymSourceName
				updSynonymSources[synonymSourceName] = upd.SynonymSourceNamed(synonymSourceName)
			}
		}
	}

	if !reflect.DeepEqual(ori.CustomAnalysis.SynonymSources, upd.CustomAnalysis.SynonymSources) {
		return fmt.Errorf("synonym sources cannot be changed")
	}

	return nil
}

// Compare all of the fields at a particular document path and add its field information
func addFieldInfo(fInfo map[string]*index.UpdateFieldInfo, ori, upd *pathInfo) error {
	var info *index.UpdateFieldInfo
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
			err = validateFieldInfo(info, updated, fInfo, ori, oriFMapInfo)
			if err != nil {
				return err
			}
		}
	} else {
		for _, oriFMapInfo := range ori.fieldMapInfo {
			var updFMap *mapping.FieldMapping
			var updAnalyser string
			var updDatetimeParser string
			var updSynonymSource string

			// For multiple fields at a single document path, compare
			// only with the matching ones
			for _, updFMapInfo := range upd.fieldMapInfo {
				if oriFMapInfo.rootName == updFMapInfo.rootName &&
					oriFMapInfo.fieldMapping.Name == updFMapInfo.fieldMapping.Name {
					updFMap = updFMapInfo.fieldMapping
					if updFMap.Type == "text" {
						updAnalyser = updFMapInfo.analyzer
						updSynonymSource = updFMapInfo.synonymSource
					} else if updFMap.Type == "datetime" {
						updDatetimeParser = updFMapInfo.datetimeParser
					}
				}
			}
			// Compare analyser, datetime parser and synonym source before comparing
			// the field mapping as it might not have this information
			if updAnalyser != "" && oriFMapInfo.analyzer != updAnalyser {
				return fmt.Errorf("analyser has been changed for a text field")
			}
			if updDatetimeParser != "" && oriFMapInfo.datetimeParser != updDatetimeParser {
				return fmt.Errorf("datetime parser has been changed for a text field")
			}
			if updSynonymSource != "" && oriFMapInfo.synonymSource != updSynonymSource {
				return fmt.Errorf("synonym source has been changed for a text field")
			}
			info, updated, err = compareFieldMapping(oriFMapInfo.fieldMapping, updFMap)
			if err != nil {
				return err
			}

			// Validate to ensure change is possible
			// Needed if multiple mappings are aliased to the same field
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
func compareFieldMapping(original, updated *mapping.FieldMapping) (*index.UpdateFieldInfo, bool, error) {
	rv := &index.UpdateFieldInfo{}

	if updated == nil {
		if original != nil && !original.IncludeInAll {
			rv.Deleted = true
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
		}
		if original.Analyzer != updated.Analyzer {
			return nil, false, fmt.Errorf("analyzer cannot be updated for text fields")
		}
	}
	if original.Type == "datetime" {
		if original.DateFormat != updated.DateFormat {
			return nil, false, fmt.Errorf("dateFormat cannot be updated for datetime fields")
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

	if rv.Deleted || rv.Index || rv.Store || rv.DocValues {
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
		if oldInfo.Deleted != newInfo.Deleted || oldInfo.Index != newInfo.Index ||
			oldInfo.DocValues != newInfo.DocValues || oldInfo.Store != newInfo.Store {
			return fmt.Errorf("updated field impossible to verify because multiple mappings point to the same field name")
		}
	} else {
		fInfo[name] = newInfo
	}
	return nil
}
