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

package util

import (
	"reflect"
	"strings"
)

func LookupPropertyPathStr(data interface{}, path string) (string, bool) {
	return mustString(lookupPropertyPath(data, path))
}

// ParseTagName extracts the field name from a struct tag
func ParseTagName(tag string) string {
	if idx := strings.Index(tag, ","); idx != -1 {
		return tag[:idx]
	}
	return tag
}

// DecodePath splits a path into its parts
// For example:
// (1) a.b.c will be split into a, b and c
// (2) a.`b.c` will be split into a and b.c
// (3) `a.b`.c will be split into a.b and c
// (4) `a`.`b`.c will be split into a, b and c
func DecodePath(path string) []string {
	var parts []string
	var start int
	var inQuote bool
	for i, c := range path {
		if c == '`' {
			inQuote = !inQuote
		} else if c == '.' && !inQuote {
			parts = append(parts, stripEnclosingBackticks(path[start:i]))
			start = i + 1
		}
	}
	parts = append(parts, stripEnclosingBackticks(path[start:]))
	return parts
}

// EncodePath concats a list of strings into a path
// by individually enclosing each string in backticks
// and separating them with a dot.
func EncodePath(pathElements []string) string {
	var rv string
	for i := 0; i < len(pathElements); i++ {
		rv += encloseInBackticks(pathElements[i])
		if i < len(pathElements)-1 {
			rv += pathSeparator
		}
	}

	return rv
}

var internalFields = map[string]bool{
	"_all":   true,
	"_id":    true,
	"_score": true,
}

// CleansePath cleanses a path by decoding and re-encoding it
// to make sure it is in the right format.
func CleansePath(path string) string {
	if len(path) == 0 || internalFields[path] {
		return path
	}
	return EncodePath(DecodePath(path))
}

// -----------------------------------------------------------------------------

func lookupPropertyPath(data interface{}, path string) interface{} {
	pathParts := DecodePath(path)

	current := data
	for _, part := range pathParts {
		current = lookupPropertyPathPart(current, part)
		if current == nil {
			break
		}
	}

	return current
}

func lookupPropertyPathPart(data interface{}, part string) interface{} {
	val := reflect.ValueOf(data)
	if !val.IsValid() {
		return nil
	}
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
	case reflect.Ptr:
		ptrElem := val.Elem()
		if ptrElem.IsValid() && ptrElem.CanInterface() {
			return lookupPropertyPathPart(ptrElem.Interface(), part)
		}
	}
	return nil
}

const pathSeparator = "."

func encloseInBackticks(s string) string {
	return "`" + s + "`"
}

func stripEnclosingBackticks(s string) string {
	if len(s) > 1 && s[0] == '`' && s[len(s)-1] == '`' {
		return s[1 : len(s)-1]
	}
	return s
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
