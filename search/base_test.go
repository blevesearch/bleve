//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
package search

import (
	"github.com/couchbaselabs/bleve/document"
	"github.com/couchbaselabs/bleve/index/mock"
)

// sets up some mock data used in many tests in this package

var twoDocIndexDocs = []*document.Document{
	// must have 4/4 beer
	document.NewDocument("1").
		AddField(document.NewTextField("name", []byte("marty"))).
		AddField(document.NewTextField("desc", []byte("beer beer beer beer"))),
	// must have 1/4 beer
	document.NewDocument("2").
		AddField(document.NewTextField("name", []byte("steve"))).
		AddField(document.NewTextField("desc", []byte("angst beer couch database"))),
	// must have 1/4 beer
	document.NewDocument("3").
		AddField(document.NewTextField("name", []byte("dustin"))).
		AddField(document.NewTextField("desc", []byte("apple beer column dank"))),
	// must have 65/65 beer
	document.NewDocument("4").
		AddField(document.NewTextField("name", []byte("ravi"))).
		AddField(document.NewTextField("desc", []byte("beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer beer"))),
	// must have 0/x beer
	document.NewDocument("5").
		AddField(document.NewTextField("name", []byte("bobert"))).
		AddField(document.NewTextField("desc", []byte("water"))),
}

var twoDocIndex *mock.MockIndex = mock.NewMockIndexWithDocs(twoDocIndexDocs)
