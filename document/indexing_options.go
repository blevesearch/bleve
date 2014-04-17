//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
package document

const (
	INDEX_FIELD = 1 << iota
	STORE_FIELD
	INCLUDE_TERM_VECTORS
)

func IsIndexedField(arg int) bool {
	return arg&INDEX_FIELD != 0
}

func IsStoredField(arg int) bool {
	return arg&STORE_FIELD != 0
}

func IncludeTermVectors(arg int) bool {
	return arg&INCLUDE_TERM_VECTORS != 0
}
