//  Copyright (c) 2026 Couchbase, Inc.
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

package searcher

import (
	"strconv"
	"time"

	"github.com/blevesearch/bleve/v2/analysis/datetime/timestamp/microseconds"
	"github.com/blevesearch/bleve/v2/analysis/datetime/timestamp/milliseconds"
	"github.com/blevesearch/bleve/v2/analysis/datetime/timestamp/nanoseconds"
	"github.com/blevesearch/bleve/v2/analysis/datetime/timestamp/seconds"
	"github.com/blevesearch/bleve/v2/search"
	index "github.com/blevesearch/bleve_index_api"
)

// loadFieldsOnHit resolves the external document ID and loads the requested
// stored fields into hit.Fields using the supplied IndexReader.
// It always resolves hit.ID; stored field loading is a no-op when fields is
// empty.
func loadFieldsOnHit(hit *search.DocumentMatch, r index.IndexReader, fields []string) error {
	if hit == nil || r == nil {
		return nil
	}

	// Resolve external ID so the callback can read hit.ID and so we can
	// load the stored document.
	if hit.ID == "" {
		extID, err := r.ExternalID(hit.IndexInternalID)
		if err != nil {
			return err
		}
		hit.ID = extID
	}

	if len(fields) == 0 {
		return nil
	}

	doc, err := r.Document(hit.ID)
	if err != nil {
		return err
	}
	if doc == nil {
		return nil
	}

	wantAll := false
	want := make(map[string]struct{}, len(fields))
	for _, f := range fields {
		if f == "*" {
			wantAll = true
			break
		}
		if f != "" {
			want[f] = struct{}{}
		}
	}

	doc.VisitFields(func(docF index.Field) {
		name := docF.Name()
		if !wantAll {
			if _, ok := want[name]; !ok {
				return
			}
		}

		var value interface{}
		switch typedF := docF.(type) {
		case index.TextField:
			value = typedF.Text()
		case index.NumericField:
			if num, err := typedF.Number(); err == nil {
				value = num
			}
		case index.BooleanField:
			if b, err := typedF.Boolean(); err == nil {
				value = b
			}
		case index.DateTimeField:
			if dt, layout, err := typedF.DateTime(); err == nil {
				if layout == "" {
					value = dt.Format(time.RFC3339)
				} else {
					switch layout {
					case seconds.Name:
						value = strconv.FormatInt(dt.Unix(), 10)
					case milliseconds.Name:
						value = strconv.FormatInt(dt.UnixMilli(), 10)
					case microseconds.Name:
						value = strconv.FormatInt(dt.UnixMicro(), 10)
					case nanoseconds.Name:
						value = strconv.FormatInt(dt.UnixNano(), 10)
					default:
						value = dt.Format(layout)
					}
				}
			}
		case index.GeoPointField:
			if lon, err := typedF.Lon(); err == nil {
				if lat, err := typedF.Lat(); err == nil {
					value = []float64{lon, lat}
				}
			}
		case index.IPField:
			if ip, err := typedF.IP(); err == nil {
				value = ip.String()
			}
		case index.GeoShapeField:
			if shape, err := typedF.GeoShape(); err == nil {
				value = shape
			}
		}

		if value != nil {
			hit.AddFieldValue(name, value)
		}
	})

	return nil
}
