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
	"encoding/json"
	"reflect"
	"testing"
)

var mappingSource = []byte(`{
    "types": {
    	"beer": {
    		"properties": {
    			"name": {
    				"fields": [
    					{
    						"name": "name",
    						"type": "text",
    						"analyzer": "standard",
    						"store": true,
    						"index": true,
                            "include_term_vectors": true,
                            "include_in_all": true
    					}
    				]
    			}
    		}
    	},
    	"brewery": {
    	}
    },
    "type_field": "_type",
    "default_type": "_default"
}`)

var beerNameField = NewFieldMapping("name", "text", "standard", true, true, true, true)
var beerNameMapping = NewDocumentMapping().AddFieldMapping(beerNameField)
var beerMapping = NewDocumentMapping().AddSubDocumentMapping("name", beerNameMapping)
var breweryMapping = NewDocumentMapping()
var mappingObject = NewIndexMapping().
	AddDocumentMapping("beer", beerMapping).
	AddDocumentMapping("brewery", breweryMapping)

func TestUnmarshalMappingJSON(t *testing.T) {
	var indexMapping IndexMapping
	err := json.Unmarshal(mappingSource, &indexMapping)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(&indexMapping, mappingObject) {
		t.Errorf("expected %#v,\n got %#v", mappingObject, &indexMapping)
	}
}
