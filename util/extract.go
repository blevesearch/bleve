//  Copyright (c) 2023 Couchbase, Inc.
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
)

// extract numeric value (if possible) and returns a float64
func ExtractNumericValFloat64(v interface{}) (float64, bool) {
	val := reflect.ValueOf(v)
	if !val.IsValid() {
		return 0, false
	}
	typ := val.Type()
	switch typ.Kind() {
	case reflect.Float32, reflect.Float64:
		return val.Float(), true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(val.Int()), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(val.Uint()), true
	}

	return 0, false
}

// extract numeric value (if possible) and returns a float32
func ExtractNumericValFloat32(v interface{}) (float32, bool) {
	val := reflect.ValueOf(v)
	if !val.IsValid() {
		return 0, false
	}
	typ := val.Type()
	switch typ.Kind() {
	case reflect.Float32, reflect.Float64:
		return float32(val.Float()), true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float32(val.Int()), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float32(val.Uint()), true
	}

	return 0, false
}
