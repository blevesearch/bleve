//  Copyright (c) 2022 Couchbase, Inc.
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

package bleve

import (
	"strconv"
	"testing"
	"time"

	"github.com/blevesearch/bleve/v2/analysis/analyzer/keyword"
	"github.com/blevesearch/bleve/v2/mapping"
)

func BenchmarkQueryTerm(b *testing.B) {
	tmpIndexPath := createTmpIndexPath(b)
	defer cleanupTmpIndexPath(b, tmpIndexPath)

	fm := mapping.NewTextFieldMapping()
	fm.Analyzer = keyword.Name
	dmap := mapping.NewDocumentMapping()
	dmap.AddFieldMappingsAt("text", fm)
	imap := mapping.NewIndexMapping()
	imap.DefaultMapping = dmap

	idx, err := New(tmpIndexPath, imap)
	if err != nil {
		b.Fatal(err)
	}

	defer func() {
		err = idx.Close()
		if err != nil {
			b.Fatal(err)
		}
	}()

	members := []string{"abc", "abcdef", "ghi", "jkl", "jklmno"}
	for i := 0; i < 100; i++ {
		if err = idx.Index(strconv.Itoa(i),
			map[string]interface{}{"text": members[i%len(members)]}); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		q := NewTermQuery(members[i%len(members)])
		q.SetField("text")
		req := NewSearchRequest(q)
		if _, err = idx.Search(req); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryTermRange(b *testing.B) {
	tmpIndexPath := createTmpIndexPath(b)
	defer cleanupTmpIndexPath(b, tmpIndexPath)

	fm := mapping.NewTextFieldMapping()
	fm.Analyzer = keyword.Name
	dmap := mapping.NewDocumentMapping()
	dmap.AddFieldMappingsAt("text", fm)
	imap := mapping.NewIndexMapping()
	imap.DefaultMapping = dmap

	idx, err := New(tmpIndexPath, imap)
	if err != nil {
		b.Fatal(err)
	}

	defer func() {
		err = idx.Close()
		if err != nil {
			b.Fatal(err)
		}
	}()

	members := []string{"abc", "abcdef", "ghi", "jkl", "jklmno"}
	for i := 0; i < 100; i++ {
		if err = idx.Index(strconv.Itoa(i),
			map[string]interface{}{"text": members[i%len(members)]}); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	inclusive := true
	for i := 0; i < b.N; i++ {
		q := NewTermRangeInclusiveQuery(
			members[i%(len(members)-2)],
			members[(i+2)%(len(members)-2)],
			&inclusive,
			&inclusive,
		)
		q.SetField("text")
		req := NewSearchRequest(q)
		if _, err = idx.Search(req); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryWildcard(b *testing.B) {
	tmpIndexPath := createTmpIndexPath(b)
	defer cleanupTmpIndexPath(b, tmpIndexPath)

	fm := mapping.NewTextFieldMapping()
	fm.Analyzer = keyword.Name
	dmap := mapping.NewDocumentMapping()
	dmap.AddFieldMappingsAt("text", fm)
	imap := mapping.NewIndexMapping()
	imap.DefaultMapping = dmap

	idx, err := New(tmpIndexPath, imap)
	if err != nil {
		b.Fatal(err)
	}

	defer func() {
		err = idx.Close()
		if err != nil {
			b.Fatal(err)
		}
	}()

	members := []string{"abc", "abcdef", "ghi", "jkl", "jklmno"}
	for i := 0; i < 100; i++ {
		if err = idx.Index(strconv.Itoa(i),
			map[string]interface{}{"text": members[i%len(members)]}); err != nil {
			b.Fatal(err)
		}
	}

	wildcards := []string{"ab*", "jk*"}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		q := NewWildcardQuery(wildcards[i%len(wildcards)])
		q.SetField("text")
		req := NewSearchRequest(q)
		if _, err = idx.Search(req); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryNumericRange(b *testing.B) {
	tmpIndexPath := createTmpIndexPath(b)
	defer cleanupTmpIndexPath(b, tmpIndexPath)

	fm := mapping.NewNumericFieldMapping()
	dmap := mapping.NewDocumentMapping()
	dmap.AddFieldMappingsAt("number", fm)
	imap := mapping.NewIndexMapping()
	imap.DefaultMapping = dmap

	idx, err := New(tmpIndexPath, imap)
	if err != nil {
		b.Fatal(err)
	}

	defer func() {
		err = idx.Close()
		if err != nil {
			b.Fatal(err)
		}
	}()

	for i := 0; i < 100; i++ {
		if err = idx.Index(strconv.Itoa(i),
			map[string]interface{}{"number": i}); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	inclusive := true
	for i := 0; i < b.N; i++ {
		start := float64(i % 90)
		end := float64((i + 10) % 90)
		q := NewNumericRangeInclusiveQuery(&start, &end, &inclusive, &inclusive)
		q.SetField("number")
		req := NewSearchRequest(q)
		if _, err = idx.Search(req); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryDateRange(b *testing.B) {
	tmpIndexPath := createTmpIndexPath(b)
	defer cleanupTmpIndexPath(b, tmpIndexPath)

	fm := mapping.NewDateTimeFieldMapping()
	dmap := mapping.NewDocumentMapping()
	dmap.AddFieldMappingsAt("date", fm)
	imap := mapping.NewIndexMapping()
	imap.DefaultMapping = dmap

	idx, err := New(tmpIndexPath, imap)
	if err != nil {
		b.Fatal(err)
	}

	defer func() {
		err = idx.Close()
		if err != nil {
			b.Fatal(err)
		}
	}()

	members := []string{
		"2022-11-16T18:45:45Z",
		"2022-11-17T18:45:45Z",
		"2022-11-18T18:45:45Z",
		"2022-11-19T18:45:45Z",
		"2022-11-20T18:45:45Z",
	}
	for i := 0; i < 100; i++ {
		if err = idx.Index(strconv.Itoa(i),
			map[string]interface{}{"date": members[i%len(members)]}); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	inclusive := true
	for i := 0; i < b.N; i++ {
		start, _ := time.Parse("2006-01-02T12:00:00Z", members[i%(len(members)-2)])
		end, _ := time.Parse("2006-01-02T12:00:00Z", members[(i+2)%(len(members)-2)])
		q := NewDateRangeInclusiveQuery(start, end, &inclusive, &inclusive)
		q.SetField("date")
		req := NewSearchRequest(q)
		if _, err = idx.Search(req); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryGeoDistance(b *testing.B) {
	tmpIndexPath := createTmpIndexPath(b)
	defer cleanupTmpIndexPath(b, tmpIndexPath)

	fm := mapping.NewGeoPointFieldMapping()
	dmap := mapping.NewDocumentMapping()
	dmap.AddFieldMappingsAt("geo", fm)
	imap := mapping.NewIndexMapping()
	imap.DefaultMapping = dmap

	idx, err := New(tmpIndexPath, imap)
	if err != nil {
		b.Fatal(err)
	}

	defer func() {
		err = idx.Close()
		if err != nil {
			b.Fatal(err)
		}
	}()

	members := [][]float64{
		{-121.96713072883645, 37.380331474621045},
		{-97.75518866579938, 30.38974491308761},
		{-0.08653451918110022, 51.51063984942306},
		{-2.230759791360498, 53.481514330841236},
		{77.59542326042589, 12.97215865921956},
	}
	for i := 0; i < 100; i++ {
		if err = idx.Index(strconv.Itoa(i),
			map[string]interface{}{"geo": members[i%len(members)]}); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		coordinates := members[i%len(members)]
		q := NewGeoDistanceQuery(coordinates[0], coordinates[1], "1mi")
		q.SetField("geo")
		req := NewSearchRequest(q)
		if _, err = idx.Search(req); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryGeoBoundingBox(b *testing.B) {
	tmpIndexPath := createTmpIndexPath(b)
	defer cleanupTmpIndexPath(b, tmpIndexPath)

	fm := mapping.NewGeoPointFieldMapping()
	dmap := mapping.NewDocumentMapping()
	dmap.AddFieldMappingsAt("geo", fm)
	imap := mapping.NewIndexMapping()
	imap.DefaultMapping = dmap

	idx, err := New(tmpIndexPath, imap)
	if err != nil {
		b.Fatal(err)
	}

	defer func() {
		err = idx.Close()
		if err != nil {
			b.Fatal(err)
		}
	}()

	members := [][]float64{
		{-121.96713072883645, 37.380331474621045},
		{-97.75518866579938, 30.38974491308761},
		{-0.08653451918110022, 51.51063984942306},
		{-2.230759791360498, 53.481514330841236},
		{77.59542326042589, 12.97215865921956},
	}
	for i := 0; i < 100; i++ {
		if err = idx.Index(strconv.Itoa(i),
			map[string]interface{}{"geo": members[i%len(members)]}); err != nil {
			b.Fatal(err)
		}
	}

	boundingBoxes := []struct {
		topLeft     []float64
		bottomRight []float64
	}{
		{
			topLeft:     []float64{-122.14424992609722, 37.49751487670511},
			bottomRight: []float64{-121.78076546622579, 37.26963069737202},
		},
		{
			topLeft:     []float64{-97.85362236226437, 30.473743975245725},
			bottomRight: []float64{-97.58691085968482, 30.285211697102895},
		},
		{
			topLeft:     []float64{-0.28538822102223094, 51.61106497119687},
			bottomRight: []float64{0.16776748108466677, 51.395702237541286},
		},
		{
			topLeft:     []float64{-2.373683904907921, 53.54371945714075},
			bottomRight: []float64{-2.134365533113197, 53.41788831720595},
		},
		{
			topLeft:     []float64{77.52617635172015, 13.037587208986437},
			bottomRight: []float64{77.66508989028102, 12.924426170584738},
		},
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		topLeftCoordinates := boundingBoxes[i%len(boundingBoxes)].topLeft
		bottomRightCoordinates := boundingBoxes[i%len(boundingBoxes)].bottomRight
		q := NewGeoBoundingBoxQuery(
			topLeftCoordinates[0],
			topLeftCoordinates[1],
			bottomRightCoordinates[0],
			bottomRightCoordinates[1],
		)
		q.SetField("geo")
		req := NewSearchRequest(q)
		if _, err = idx.Search(req); err != nil {
			b.Fatal(err)
		}
	}
}
