//  Copyright (c) 2017 Couchbase, Inc.
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

package zap

import (
	"os"
	"testing"

	"github.com/RoaringBitmap/roaring"
	"github.com/blevesearch/bleve/analysis"
	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/scorch/segment/mem"
)

func TestMerge(t *testing.T) {
	_ = os.RemoveAll("/tmp/scorch.zap")
	_ = os.RemoveAll("/tmp/scorch2.zap")
	_ = os.RemoveAll("/tmp/scorch3.zap")

	memSegment := buildMemSegmentMulti()
	err := PersistSegment(memSegment, "/tmp/scorch.zap", 1024)
	if err != nil {
		t.Fatal(err)
	}

	memSegment2 := buildMemSegmentMulti2()
	err = PersistSegment(memSegment2, "/tmp/scorch2.zap", 1024)
	if err != nil {
		t.Fatal(err)
	}

	segment, err := Open("/tmp/scorch.zap")
	if err != nil {
		t.Fatalf("error opening segment: %v", err)
	}
	defer func() {
		cerr := segment.Close()
		if cerr != nil {
			t.Fatalf("error closing segment: %v", err)
		}
	}()

	segment2, err := Open("/tmp/scorch2.zap")
	if err != nil {
		t.Fatalf("error opening segment: %v", err)
	}
	defer func() {
		cerr := segment2.Close()
		if cerr != nil {
			t.Fatalf("error closing segment: %v", err)
		}
	}()

	segsToMerge := make([]*Segment, 2)
	segsToMerge[0] = segment.(*Segment)
	segsToMerge[1] = segment2.(*Segment)

	_, err = Merge(segsToMerge, []*roaring.Bitmap{nil, nil}, "/tmp/scorch3.zap", 1024)
	if err != nil {
		t.Fatal(err)
	}
}

func TestMergeAndDrop(t *testing.T) {
	_ = os.RemoveAll("/tmp/scorch.zap")
	_ = os.RemoveAll("/tmp/scorch2.zap")
	_ = os.RemoveAll("/tmp/scorch3.zap")

	memSegment := buildMemSegmentMulti()
	err := PersistSegment(memSegment, "/tmp/scorch.zap", 1024)
	if err != nil {
		t.Fatal(err)
	}

	memSegment2 := buildMemSegmentMulti2()
	err = PersistSegment(memSegment2, "/tmp/scorch2.zap", 1024)
	if err != nil {
		t.Fatal(err)
	}

	segment, err := Open("/tmp/scorch.zap")
	if err != nil {
		t.Fatalf("error opening segment: %v", err)
	}
	defer func() {
		cerr := segment.Close()
		if cerr != nil {
			t.Fatalf("error closing segment: %v", err)
		}
	}()

	segment2, err := Open("/tmp/scorch2.zap")
	if err != nil {
		t.Fatalf("error opening segment: %v", err)
	}
	defer func() {
		cerr := segment2.Close()
		if cerr != nil {
			t.Fatalf("error closing segment: %v", err)
		}
	}()

	segsToMerge := make([]*Segment, 2)
	segsToMerge[0] = segment.(*Segment)
	segsToMerge[1] = segment2.(*Segment)

	docsToDrop := make([]*roaring.Bitmap, 2)
	docsToDrop[0] = roaring.NewBitmap()
	docsToDrop[0].AddInt(1)
	docsToDrop[1] = roaring.NewBitmap()
	docsToDrop[1].AddInt(1)

	_, err = Merge(segsToMerge, docsToDrop, "/tmp/scorch3.zap", 1024)
	if err != nil {
		t.Fatal(err)
	}
}

func buildMemSegmentMulti2() *mem.Segment {

	doc := &document.Document{
		ID: "c",
		Fields: []document.Field{
			document.NewTextFieldCustom("_id", nil, []byte("c"), document.IndexField|document.StoreField, nil),
			document.NewTextFieldCustom("name", nil, []byte("mat"), document.IndexField|document.StoreField|document.IncludeTermVectors, nil),
			document.NewTextFieldCustom("desc", nil, []byte("some thing"), document.IndexField|document.StoreField|document.IncludeTermVectors, nil),
			document.NewTextFieldCustom("tag", []uint64{0}, []byte("cold"), document.IndexField|document.StoreField|document.IncludeTermVectors, nil),
			document.NewTextFieldCustom("tag", []uint64{1}, []byte("dark"), document.IndexField|document.StoreField|document.IncludeTermVectors, nil),
		},
		CompositeFields: []*document.CompositeField{
			document.NewCompositeField("_all", true, nil, []string{"_id"}),
		},
	}

	doc2 := &document.Document{
		ID: "d",
		Fields: []document.Field{
			document.NewTextFieldCustom("_id", nil, []byte("d"), document.IndexField|document.StoreField, nil),
			document.NewTextFieldCustom("name", nil, []byte("joa"), document.IndexField|document.StoreField|document.IncludeTermVectors, nil),
			document.NewTextFieldCustom("desc", nil, []byte("some thing"), document.IndexField|document.StoreField|document.IncludeTermVectors, nil),
			document.NewTextFieldCustom("tag", []uint64{0}, []byte("cold"), document.IndexField|document.StoreField|document.IncludeTermVectors, nil),
			document.NewTextFieldCustom("tag", []uint64{1}, []byte("dark"), document.IndexField|document.StoreField|document.IncludeTermVectors, nil),
		},
		CompositeFields: []*document.CompositeField{
			document.NewCompositeField("_all", true, nil, []string{"_id"}),
		},
	}

	// forge analyzed docs
	results := []*index.AnalysisResult{
		&index.AnalysisResult{
			Document: doc,
			Analyzed: []analysis.TokenFrequencies{
				analysis.TokenFrequency(analysis.TokenStream{
					&analysis.Token{
						Start:    0,
						End:      1,
						Position: 1,
						Term:     []byte("c"),
					},
				}, nil, false),
				analysis.TokenFrequency(analysis.TokenStream{
					&analysis.Token{
						Start:    0,
						End:      3,
						Position: 1,
						Term:     []byte("mat"),
					},
				}, nil, true),
				analysis.TokenFrequency(analysis.TokenStream{
					&analysis.Token{
						Start:    0,
						End:      4,
						Position: 1,
						Term:     []byte("some"),
					},
					&analysis.Token{
						Start:    5,
						End:      10,
						Position: 2,
						Term:     []byte("thing"),
					},
				}, nil, true),
				analysis.TokenFrequency(analysis.TokenStream{
					&analysis.Token{
						Start:    0,
						End:      4,
						Position: 1,
						Term:     []byte("cold"),
					},
				}, []uint64{0}, true),
				analysis.TokenFrequency(analysis.TokenStream{
					&analysis.Token{
						Start:    0,
						End:      4,
						Position: 1,
						Term:     []byte("dark"),
					},
				}, []uint64{1}, true),
			},
			Length: []int{
				1,
				1,
				2,
				1,
				1,
			},
		},
		&index.AnalysisResult{
			Document: doc2,
			Analyzed: []analysis.TokenFrequencies{
				analysis.TokenFrequency(analysis.TokenStream{
					&analysis.Token{
						Start:    0,
						End:      1,
						Position: 1,
						Term:     []byte("d"),
					},
				}, nil, false),
				analysis.TokenFrequency(analysis.TokenStream{
					&analysis.Token{
						Start:    0,
						End:      3,
						Position: 1,
						Term:     []byte("joa"),
					},
				}, nil, true),
				analysis.TokenFrequency(analysis.TokenStream{
					&analysis.Token{
						Start:    0,
						End:      4,
						Position: 1,
						Term:     []byte("some"),
					},
					&analysis.Token{
						Start:    5,
						End:      10,
						Position: 2,
						Term:     []byte("thing"),
					},
				}, nil, true),
				analysis.TokenFrequency(analysis.TokenStream{
					&analysis.Token{
						Start:    0,
						End:      4,
						Position: 1,
						Term:     []byte("cold"),
					},
				}, []uint64{0}, true),
				analysis.TokenFrequency(analysis.TokenStream{
					&analysis.Token{
						Start:    0,
						End:      4,
						Position: 1,
						Term:     []byte("dark"),
					},
				}, []uint64{1}, true),
			},
			Length: []int{
				1,
				1,
				2,
				1,
				1,
			},
		},
	}

	// fix up composite fields
	for _, ar := range results {
		for i, f := range ar.Document.Fields {
			for _, cf := range ar.Document.CompositeFields {
				cf.Compose(f.Name(), ar.Length[i], ar.Analyzed[i])
			}
		}
	}

	segment := mem.NewFromAnalyzedDocs(results)

	return segment
}
