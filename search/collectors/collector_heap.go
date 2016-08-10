//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
//
package collectors

import (
	"container/heap"
	"math"
	"time"

	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/search"
	"golang.org/x/net/context"
)

type collectedDoc struct {
	match search.DocumentMatch
	doc   *document.Document
}

type HeapCollector struct {
	size          int
	skip          int
	total         uint64
	took          time.Duration
	sort          search.SortOrder
	results       []*collectedDoc
	facetsBuilder *search.FacetsBuilder
	reader        index.IndexReader
}

var COLLECT_CHECK_DONE_EVERY = uint64(1024)

func NewHeapCollector(size int, skip int, reader index.IndexReader, sort search.SortOrder) *HeapCollector {
	hc := &HeapCollector{size: size, skip: skip, reader: reader, sort: sort}
	heap.Init(hc)
	return hc
}

func (hc *HeapCollector) Collect(ctx context.Context, searcher search.Searcher) error {
	startTime := time.Now()
	var err error
	var pre search.DocumentMatch // A single pre-alloc'ed, reused instance.
	var next *search.DocumentMatch
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		next, err = searcher.Next(&pre)
	}
	for err == nil && next != nil {
		if hc.total%COLLECT_CHECK_DONE_EVERY == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
		err = hc.collectSingle(next)
		if err != nil {
			break
		}
		if hc.facetsBuilder != nil {
			err = hc.facetsBuilder.Update(next)
			if err != nil {
				break
			}
		}
		next, err = searcher.Next(pre.Reset())
	}
	// compute search duration
	hc.took = time.Since(startTime)
	if err != nil {
		return err
	}
	return nil
}

func (hc *HeapCollector) collectSingle(dmIn *search.DocumentMatch) error {
	// increment total hits
	hc.total++
	single := new(collectedDoc)
	single.match = *dmIn
	var err error
	if len(hc.sort) > 0 {
		single.doc, err = hc.reader.Document(dmIn.ID)
		if err != nil {
			return err
		}
	}
	heap.Push(hc, single)
	if hc.Len() > hc.size+hc.skip {
		heap.Pop(hc)
	}
	return nil
}

func (hc *HeapCollector) SetFacetsBuilder(facetsBuilder *search.FacetsBuilder) {
	hc.facetsBuilder = facetsBuilder
}

func (hc *HeapCollector) Results() search.DocumentMatchCollection {
	count := hc.Len()
	size := count - hc.skip
	rv := make(search.DocumentMatchCollection, size)
	for count > 0 {
		count--
		if count >= hc.skip {
			size--
			doc := heap.Pop(hc).(*collectedDoc)
			rv[size] = &doc.match
		}
	}
	return rv
}

func (hc *HeapCollector) Total() uint64 {
	return hc.total
}

func (hc *HeapCollector) MaxScore() float64 {
	var max float64
	for _, res := range hc.results {
		max = math.Max(max, res.match.Score)
	}
	return max
}

func (hc *HeapCollector) Took() time.Duration {
	return hc.took
}

func (hc *HeapCollector) FacetResults() search.FacetResults {
	if hc.facetsBuilder != nil {
		return hc.facetsBuilder.Results()
	}
	return search.FacetResults{}
}

func (hc *HeapCollector) Len() int {
	return len(hc.results)
}

func field(doc *document.Document, field string) document.Field {
	if doc == nil {
		return nil
	}
	for _, f := range doc.Fields {
		if f.Name() == field {
			return f
		}
	}
	return nil
}

func textFieldCompare(i, j *document.TextField, ascends bool) (bool, bool) {
	ivalue := string(i.Value())
	jvalue := string(j.Value())
	if ivalue == jvalue {
		return true, false
	}
	if ascends {
		return false, ivalue < jvalue
	}
	return false, ivalue > jvalue
}

func numericFieldCompare(i, j *document.NumericField, ascends bool) (bool, bool) {
	ivalue, _ := i.Number()
	jvalue, _ := i.Number()
	if ivalue == jvalue {
		return true, false
	}
	if ascends {
		return false, ivalue < jvalue
	}
	return false, ivalue > jvalue
}

func dateTimeFieldCompare(i, j *document.DateTimeField, ascends bool) (bool, bool) {
	ivalue, _ := i.DateTime()
	jvalue, _ := i.DateTime()
	if ivalue.Equal(jvalue) {
		return true, false
	}
	if ascends {
		return false, ivalue.Before(jvalue)
	}
	return false, ivalue.After(jvalue)
}

func boolFieldCompare(i, j *document.BooleanField, ascends bool) (bool, bool) {
	ivalue, _ := i.Boolean()
	jvalue, _ := i.Boolean()
	if ivalue == jvalue {
		return true, false
	}
	return false, ascends && jvalue
}

//returns: equals, less
func fieldCompare(i, j document.Field, ascends bool) (bool, bool) {
	switch ft := i.(type) {
	case *document.TextField:
		return textFieldCompare(ft, j.(*document.TextField), ascends)
	case *document.NumericField:
		return numericFieldCompare(ft, j.(*document.NumericField), ascends)
	case *document.DateTimeField:
		return dateTimeFieldCompare(ft, j.(*document.DateTimeField), ascends)
	case *document.BooleanField:
		return boolFieldCompare(ft, j.(*document.BooleanField), ascends)
	}
	return false, false
}

func (hc *HeapCollector) Less(i, j int) bool {
	if len(hc.sort) > 0 {
		doci := hc.results[i].doc
		docj := hc.results[j].doc
		if doci != nil && docj != nil {
			for _, so := range hc.sort {
				fieldi := field(doci, so.Field)
				fieldj := field(docj, so.Field)
				equals, lt := fieldCompare(fieldi, fieldj, so.Ascends)
				if !equals {
					return lt
				}
			}
		}
	}
	scori := hc.results[i].match.Score
	scorj := hc.results[j].match.Score
	// make sure the list is ordered if everything else is the same...
	if scori == scorj {
		return hc.results[i].match.ID > hc.results[j].match.ID
	}
	return scori < scorj
}

func (hc *HeapCollector) Swap(i, j int) {
	hc.results[i], hc.results[j] = hc.results[j], hc.results[i]
}

func (hc *HeapCollector) Push(x interface{}) {
	hc.results = append(hc.results, x.(*collectedDoc))
}

func (hc *HeapCollector) Pop() interface{} {
	n := len(hc.results)
	doc := hc.results[n-1]
	hc.results = hc.results[0 : n-1]
	return doc
}
