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
	"github.com/couchbaselabs/bleve/search"
)

type PrefixQuery struct {
	Prefix   string  `json:"prefix"`
	FieldVal string  `json:"field,omitempty"`
	BoostVal float64 `json:"boost,omitempty"`
}

func NewPrefixQuery(prefix string) *PrefixQuery {
	return &PrefixQuery{
		Prefix:   prefix,
		BoostVal: 1.0,
	}
}

func (q *PrefixQuery) Boost() float64 {
	return q.BoostVal
}

func (q *PrefixQuery) SetBoost(b float64) *PrefixQuery {
	q.BoostVal = b
	return q
}

func (q *PrefixQuery) Field() string {
	return q.FieldVal
}

func (q *PrefixQuery) SetField(f string) *PrefixQuery {
	q.FieldVal = f
	return q
}

func (q *PrefixQuery) Searcher(i *indexImpl, explain bool) (search.Searcher, error) {
	field := q.FieldVal
	if q.FieldVal == "" {
		field = i.m.DefaultField
	}
	return search.NewTermPrefixSearcher(i.i, q.Prefix, field, q.BoostVal, explain)
}

func (q *PrefixQuery) Validate() error {
	return nil
}
