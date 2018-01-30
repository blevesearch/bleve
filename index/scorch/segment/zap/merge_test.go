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
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
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

	segm, err := Open("/tmp/scorch3.zap")
	if err != nil {
		t.Fatalf("error opening merged segment: %v", err)
	}
	seg3 := segm.(*Segment)
	defer func() {
		cerr := seg3.Close()
		if cerr != nil {
			t.Fatalf("error closing segment: %v", err)
		}
	}()

	if seg3.Path() != "/tmp/scorch3.zap" {
		t.Fatalf("wrong path")
	}
	if seg3.Count() != 4 {
		t.Fatalf("wrong count")
	}
	if len(seg3.Fields()) != 5 {
		t.Fatalf("wrong # fields: %#v\n", seg3.Fields())
	}

	testMergeWithSelf(t, seg3, 4)
}

func testMergeWithSelf(t *testing.T, segCur *Segment, expectedCount uint64) {
	// trying merging the segment with itself for a few rounds
	var diffs []string

	for i := 0; i < 10; i++ {
		fname := fmt.Sprintf("scorch-self-%d.zap", i)

		_ = os.RemoveAll("/tmp/" + fname)

		segsToMerge := make([]*Segment, 1)
		segsToMerge[0] = segCur

		_, err := Merge(segsToMerge, []*roaring.Bitmap{nil, nil}, "/tmp/"+fname, 1024)
		if err != nil {
			t.Fatal(err)
		}

		segm, err := Open("/tmp/" + fname)
		if err != nil {
			t.Fatalf("error opening merged segment: %v", err)
		}
		segNew := segm.(*Segment)
		defer func(s *Segment) {
			cerr := s.Close()
			if cerr != nil {
				t.Fatalf("error closing segment: %v", err)
			}
		}(segNew)

		if segNew.Count() != expectedCount {
			t.Fatalf("wrong count")
		}
		if len(segNew.Fields()) != 5 {
			t.Fatalf("wrong # fields: %#v\n", segNew.Fields())
		}

		diff := compareSegments(segCur, segNew)
		if diff != "" {
			diffs = append(diffs, fname+" is different than previous:\n"+diff)
		}

		segCur = segNew
	}

	if len(diffs) > 0 {
		t.Errorf("mismatches after repeated self-merging: %v", strings.Join(diffs, "\n"))
	}
}

func compareSegments(a, b *Segment) string {
	var rv []string

	if a.Count() != b.Count() {
		return "counts"
	}

	afields := append([]string(nil), a.Fields()...)
	bfields := append([]string(nil), b.Fields()...)
	sort.Strings(afields)
	sort.Strings(bfields)
	if !reflect.DeepEqual(afields, bfields) {
		return "fields"
	}

	for _, fieldName := range afields {
		adict, err := a.Dictionary(fieldName)
		if err != nil {
			return fmt.Sprintf("adict err: %v", err)
		}
		bdict, err := b.Dictionary(fieldName)
		if err != nil {
			return fmt.Sprintf("bdict err: %v", err)
		}

		if adict.(*Dictionary).fst.Len() != bdict.(*Dictionary).fst.Len() {
			rv = append(rv, fmt.Sprintf("field %s, dict fst Len()'s  different: %v %v",
				fieldName, adict.(*Dictionary).fst.Len(), bdict.(*Dictionary).fst.Len()))
		}

		aitr := adict.Iterator()
		bitr := bdict.Iterator()
		for {
			anext, aerr := aitr.Next()
			bnext, berr := bitr.Next()
			if aerr != berr {
				rv = append(rv, fmt.Sprintf("field %s, dict iterator Next() errors different: %v %v",
					fieldName, aerr, berr))
				break
			}
			if !reflect.DeepEqual(anext, bnext) {
				rv = append(rv, fmt.Sprintf("field %s, dict iterator Next() results different: %#v %#v",
					fieldName, anext, bnext))
				// keep going to try to see more diff details at the postingsList level
			}
			if aerr != nil || anext == nil ||
				berr != nil || bnext == nil {
				break
			}

			for _, next := range []*index.DictEntry{anext, bnext} {
				if next == nil {
					continue
				}

				aplist, aerr := adict.(*Dictionary).postingsList([]byte(next.Term), nil)
				bplist, berr := bdict.(*Dictionary).postingsList([]byte(next.Term), nil)
				if aerr != berr {
					rv = append(rv, fmt.Sprintf("field %s, term: %s, postingsList() errors different: %v %v",
						fieldName, next.Term, aerr, berr))
				}

				if (aplist != nil) != (bplist != nil) {
					rv = append(rv, fmt.Sprintf("field %s, term: %s, postingsList() results different: %v %v",
						fieldName, next.Term, aplist, bplist))
					break
				}

				if aerr != nil || aplist == nil ||
					berr != nil || bplist == nil {
					break
				}

				if aplist.Count() != bplist.Count() {
					rv = append(rv, fmt.Sprintf("field %s, term: %s, postingsList().Count()'s different: %v %v",
						fieldName, next.Term, aplist.Count(), bplist.Count()))
				}

				apitr := aplist.Iterator()
				bpitr := bplist.Iterator()
				if (apitr != nil) != (bpitr != nil) {
					rv = append(rv, fmt.Sprintf("field %s, term: %s, postingsList.Iterator() results different: %v %v",
						fieldName, next.Term, apitr, bpitr))
					break
				}

				for {
					apitrn, aerr := apitr.Next()
					bpitrn, aerr := bpitr.Next()
					if aerr != berr {
						rv = append(rv, fmt.Sprintf("field %s, term: %s, postingsListIterator Next() errors different: %v %v",
							fieldName, next.Term, aerr, berr))
					}

					if (apitrn != nil) != (bpitrn != nil) {
						rv = append(rv, fmt.Sprintf("field %s, term: %s, postingsListIterator Next() results different: %v %v",
							fieldName, next.Term, apitrn, bpitrn))
						break
					}

					if aerr != nil || apitrn == nil ||
						berr != nil || bpitrn == nil {
						break
					}

					if apitrn.Number() != bpitrn.Number() {
						rv = append(rv, fmt.Sprintf("field %s, term: %s, postingsListIterator Next() Number()'s different: %v %v",
							fieldName, next.Term, apitrn.Number(), bpitrn.Number()))
					}

					if apitrn.Frequency() != bpitrn.Frequency() {
						rv = append(rv, fmt.Sprintf("field %s, term: %s, postingsListIterator Next() Frequency()'s different: %v %v",
							fieldName, next.Term, apitrn.Frequency(), bpitrn.Frequency()))
					}

					if apitrn.Norm() != bpitrn.Norm() {
						rv = append(rv, fmt.Sprintf("field %s, term: %s, postingsListIterator Next() Norm()'s different: %v %v",
							fieldName, next.Term, apitrn.Norm(), bpitrn.Norm()))
					}

					if len(apitrn.Locations()) != len(bpitrn.Locations()) {
						rv = append(rv, fmt.Sprintf("field %s, term: %s, postingsListIterator Next() Locations() len's different: %v %v",
							fieldName, next.Term, len(apitrn.Locations()), len(bpitrn.Locations())))
					}

					for loci, aloc := range apitrn.Locations() {
						bloc := bpitrn.Locations()[loci]

						if (aloc != nil) != (bloc != nil) {
							rv = append(rv, fmt.Sprintf("field %s, term: %s, postingsListIterator Next() loc different: %v %v",
								fieldName, next.Term, aloc, bloc))
							break
						}

						if aloc.Field() != bloc.Field() ||
							aloc.Start() != bloc.Start() ||
							aloc.End() != bloc.End() ||
							aloc.Pos() != bloc.Pos() ||
							!reflect.DeepEqual(aloc.ArrayPositions(), bloc.ArrayPositions()) {
							rv = append(rv, fmt.Sprintf("field %s, term: %s, postingsListIterator Next() loc details different: %v %v",
								fieldName, next.Term, aloc, bloc))
						}
					}
				}
			}
		}
	}

	return strings.Join(rv, "\n")
}

func TestMergeAndDrop(t *testing.T) {
	docsToDrop := make([]*roaring.Bitmap, 2)
	docsToDrop[0] = roaring.NewBitmap()
	docsToDrop[0].AddInt(1)
	docsToDrop[1] = roaring.NewBitmap()
	docsToDrop[1].AddInt(1)
	testMergeAndDrop(t, docsToDrop)
}

func TestMergeAndDropAllFromOneSegment(t *testing.T) {
	docsToDrop := make([]*roaring.Bitmap, 2)
	docsToDrop[0] = roaring.NewBitmap()
	docsToDrop[0].AddInt(0)
	docsToDrop[0].AddInt(1)
	docsToDrop[1] = roaring.NewBitmap()
	testMergeAndDrop(t, docsToDrop)
}

func testMergeAndDrop(t *testing.T, docsToDrop []*roaring.Bitmap) {
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

	_, err = Merge(segsToMerge, docsToDrop, "/tmp/scorch3.zap", 1024)
	if err != nil {
		t.Fatal(err)
	}

	segm, err := Open("/tmp/scorch3.zap")
	if err != nil {
		t.Fatalf("error opening merged segment: %v", err)
	}
	defer func() {
		cerr := segm.Close()
		if cerr != nil {
			t.Fatalf("error closing segment: %v", err)
		}
	}()

	if segm.Count() != 2 {
		t.Fatalf("wrong count, got: %d", segm.Count())
	}
	if len(segm.Fields()) != 5 {
		t.Fatalf("wrong # fields: %#v\n", segm.Fields())
	}

	testMergeWithSelf(t, segm.(*Segment), 2)
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
