//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package bleve

// Constant Error values which can be compared to determine the type of error
const (
	ErrorIndexPathExists Error = iota
	ErrorIndexPathDoesNotExist
	ErrorIndexMetaMissing
	ErrorIndexMetaCorrupt
	ErrorDisjunctionFewerThanMinClauses
	ErrorBooleanQueryNeedsMustOrShouldOrNotMust
	ErrorNumericQueryNoBounds
	ErrorPhraseQueryNoTerms
	ErrorUnknownQueryType
	ErrorUnknownStorageType
	ErrorIndexClosed
	ErrorAliasMulti
	ErrorAliasEmpty
	ErrorUnknownIndexType
	ErrorEmptyID
)

// Error represents a more strongly typed bleve error for detecting
// and handling specific types of errors.
type Error int

func (e Error) Error() string {
	return errorMessages[int(e)]
}

var errorMessages = map[int]string{
	int(ErrorIndexPathExists):                        "cannot create new index, path already exists",
	int(ErrorIndexPathDoesNotExist):                  "cannot open index, path does not exist",
	int(ErrorIndexMetaMissing):                       "cannot open index, metadata missing",
	int(ErrorIndexMetaCorrupt):                       "cannot open index, metadata corrupt",
	int(ErrorDisjunctionFewerThanMinClauses):         "disjunction query has fewer than the minimum number of clauses to satisfy",
	int(ErrorBooleanQueryNeedsMustOrShouldOrNotMust): "boolean query must contain at least one must or should or not must clause",
	int(ErrorNumericQueryNoBounds):                   "numeric range query must specify min or max",
	int(ErrorPhraseQueryNoTerms):                     "phrase query must contain at least one term",
	int(ErrorUnknownQueryType):                       "unknown query type",
	int(ErrorUnknownStorageType):                     "unknown storage type",
	int(ErrorIndexClosed):                            "index is closed",
	int(ErrorAliasMulti):                             "cannot perform single index operation on multiple index alias",
	int(ErrorAliasEmpty):                             "cannot perform operation on empty alias",
	int(ErrorUnknownIndexType):                       "unknown index type",
	int(ErrorEmptyID):                                "document ID cannot be empty",
}
