//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package bleve

import (
	"reflect"
	"strings"
)

func lookupPropertyPath(data interface{}, path string) interface{} {
	pathParts := decodePath(path)

	current := data
	for _, part := range pathParts {
		current = lookupProptyPathPart(current, part)
		if current == nil {
			break
		}
	}

	return current
}

func lookupProptyPathPart(data interface{}, part string) interface{} {
	val := reflect.ValueOf(data)
	typ := val.Type()
	switch typ.Kind() {
	case reflect.Map:
		// FIXME can add support for other map keys in the future
		if typ.Key().Kind() == reflect.String {
			key := reflect.ValueOf(part)
			entry := val.MapIndex(key)
			if entry.IsValid() {
				return entry.Interface()
			}
		}
	case reflect.Struct:
		field := val.FieldByName(part)
		if field.IsValid() && field.CanInterface() {
			return field.Interface()
		}
	}
	return nil
}

const pathSeparator = "."

func decodePath(path string) []string {
	return strings.Split(path, pathSeparator)
}

func encodePath(pathElements []string) string {
	return strings.Join(pathElements, pathSeparator)
}

func mustString(data interface{}) (string, bool) {
	if data != nil {
		str, ok := data.(string)
		if ok {
			return str, true
		}
	}
	return "", false
}

// parseJSONTagName extracts the JSON field name from a struct tag
func parseJSONTagName(tag string) string {
	if tag == "-" {
		return ""
	}
	if idx := strings.Index(tag, ","); idx != -1 {
		return tag[:idx]
	}
	return tag
}
