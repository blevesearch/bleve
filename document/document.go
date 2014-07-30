//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
package document

import (
	"fmt"
	"log"
)

type Document struct {
	ID              string  `json:"id"`
	Fields          []Field `json:"fields"`
	CompositeFields []*CompositeField
}

func NewDocument(id string) *Document {
	return &Document{
		ID:              id,
		Fields:          make([]Field, 0),
		CompositeFields: make([]*CompositeField, 0),
	}
}

func (d *Document) AddField(f Field) *Document {
	switch f := f.(type) {
	case *CompositeField:
		d.CompositeFields = append(d.CompositeFields, f)
	default:
		d.Fields = append(d.Fields, f)
	}
	return d
}

func (d *Document) GoString() string {
	fields := ""
	for i, field := range d.Fields {
		if i != 0 {
			fields += ", "
		}
		fields += fmt.Sprintf("%#v", field)
	}
	compositeFields := ""
	for i, field := range d.CompositeFields {
		log.Printf("see composite field")
		if i != 0 {
			compositeFields += ", "
		}
		compositeFields += fmt.Sprintf("%#v", field)
	}
	return fmt.Sprintf("&document.Document{ID:%s, Fields: %s, CompositeFields: %s}", d.ID, fields, compositeFields)
}
