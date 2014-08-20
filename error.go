//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
package bleve

const (
	ERROR_INDEX_PATH_EXISTS Error = iota
	ERROR_INDEX_PATH_DOES_NOT_EXIST
	ERROR_INDEX_META_MISSING
	ERROR_INDEX_META_CORRUPT
	ERROR_DISJUNCTION_FEWER_THAN_MIN_CLAUSES
	ERROR_BOOLEAN_QUERY_NEEDS_MUST_OR_SHOULD
	ERROR_NUMERIC_QUERY_NO_BOUNDS
	ERROR_PHRASE_QUERY_NO_TERMS
	ERROR_UNKNOWN_QUERY_TYPE
	ERROR_UNKNOWN_STORAGE_TYPE
)

type Error int

func (e Error) Error() string {
	return errorMessages[int(e)]
}

var errorMessages = map[int]string{
	int(ERROR_INDEX_PATH_EXISTS):                  "cannot create new index, path already exists",
	int(ERROR_INDEX_PATH_DOES_NOT_EXIST):          "cannot open index, path does not exist",
	int(ERROR_INDEX_META_MISSING):                 "cannot open index, metadata missing",
	int(ERROR_INDEX_META_CORRUPT):                 "cannot open index, metadata corrupt",
	int(ERROR_DISJUNCTION_FEWER_THAN_MIN_CLAUSES): "disjunction query has fewer than the minimum number of clauses to satisfy",
	int(ERROR_BOOLEAN_QUERY_NEEDS_MUST_OR_SHOULD): "boolean query must contain at least one must or should clause",
	int(ERROR_NUMERIC_QUERY_NO_BOUNDS):            "numeric range query must specify min or max",
	int(ERROR_PHRASE_QUERY_NO_TERMS):              "phrase query must contain at least one term",
	int(ERROR_UNKNOWN_QUERY_TYPE):                 "unknown query type",
	int(ERROR_UNKNOWN_STORAGE_TYPE):               "unkown storage type",
}
