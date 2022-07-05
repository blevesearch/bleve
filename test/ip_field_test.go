//  Copyright (c) 2021 Couchbase, Inc.
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

package test

import (
	"net"
	"testing"

	"bleve/v2"
	"bleve/v2/mapping"
)

type doc struct {
	IP string `json:"ip"`
}

func createIdx(t *testing.T) bleve.Index {
	ipIndexed := mapping.NewIPFieldMapping()
	ipIndexed.Name = "ip"

	lineMapping := bleve.NewDocumentStaticMapping()
	lineMapping.AddFieldMappingsAt("ip", ipIndexed)

	mapping := bleve.NewIndexMapping()
	mapping.DefaultMapping = lineMapping
	mapping.DefaultAnalyzer = "standard"

	idx, err := bleve.NewMemOnly(mapping)
	if err != nil {
		t.Fatal(err)
	}
	return idx
}

func Test_ipv4CidrQuery(t *testing.T) {
	idx := createIdx(t)
	defer idx.Close()

	err := idx.Index("id1", doc{"192.168.1.21"})
	if err != nil {
		t.Fatal(err)
	}

	reqStr := `192.168.1.0/24`
	query := bleve.NewIPRangeQuery(reqStr)
	query.FieldVal = "ip"

	search := bleve.NewSearchRequest(query)
	res, err := idx.Search(search)
	if err != nil {
		t.Fatal(err)
	}

	if res.Total != 1 {
		t.Fatalf("failed to find %q, res -> %s", reqStr, res)
	}
	if res.Hits[0].ID != "id1" {
		t.Fatalf("expected %q got %q", "id1", res.Hits[0].Index)
	}
}

func Test_ipv6CidrQuery(t *testing.T) {
	idx := createIdx(t)
	defer idx.Close()

	err := idx.Index("id1", doc{"2a00:23c8:7283:ff00:1fa8:2af6:9dec:6b19"})
	if err != nil {
		t.Fatal(err)
	}

	reqStr := `2a00:23c8:7283:ff00:1fa8:0:0:0/80`
	query := bleve.NewIPRangeQuery(reqStr)
	query.FieldVal = "ip"

	search := bleve.NewSearchRequest(query)
	res, err := idx.Search(search)
	if err != nil {
		t.Fatal(err)
	}

	if res.Total != 1 {
		t.Fatalf("failed to find %q, res -> %s", reqStr, res)
	}
	if res.Hits[0].ID != "id1" {
		t.Fatalf("expected %q got %q", "id1", res.Hits[0].Index)
	}
}

func Test_MultiIPvr4CidrQuery(t *testing.T) {
	idx := createIdx(t)
	defer idx.Close()

	err := idx.Index("id1", doc{"192.168.1.0"})
	if err != nil {
		t.Fatal(err)
	}
	err = idx.Index("id2", doc{"192.168.1.255"})
	if err != nil {
		t.Fatal(err)
	}
	err = idx.Index("id3", doc{"192.168.2.22"})
	if err != nil {
		t.Fatal(err)
	}

	reqStr := `192.168.1.0/24`
	query := bleve.NewIPRangeQuery(reqStr)
	query.FieldVal = "ip"

	search := bleve.NewSearchRequest(query)
	res, err := idx.Search(search)
	if err != nil {
		t.Fatal(err)
	}

	if res.Total != 2 {
		t.Fatalf("failed to find %q, res -> %s", reqStr, res)
	}
	if res.Hits[0].ID != "id1" {
		t.Fatalf("expected %q got %q", "id1", res.Hits[0].ID)
	}
	if res.Hits[1].ID != "id2" {
		t.Fatalf("expected %q got %q", "id2", res.Hits[0].Index)
	}
}

func Test_CidrQueryNonDivisibleBy8(t *testing.T) {
	idx := createIdx(t)
	defer idx.Close()

	err := idx.Index("id1", doc{"192.168.1.1"})
	if err != nil {
		t.Fatal(err)
	}
	err = idx.Index("id2", doc{"192.168.1.2"})
	if err != nil {
		t.Fatal(err)
	}
	err = idx.Index("id3", doc{"192.168.2.5"})
	if err != nil {
		t.Fatal(err)
	}
	err = idx.Index("id4", doc{"192.168.2.6"})
	if err != nil {
		t.Fatal(err)
	}

	reqStr := `192.168.1.0/30`
	query := bleve.NewIPRangeQuery(reqStr)
	query.FieldVal = "ip"

	search := bleve.NewSearchRequest(query)
	res, err := idx.Search(search)
	if err != nil {
		t.Fatal(err)
	}

	if res.Total != 2 {
		t.Fatalf("failed to find %q, res -> %s", reqStr, res)
	}
	if res.Hits[0].ID != "id1" {
		t.Fatalf("expected %q got %q", "id1", res.Hits[0].ID)
	}
	if res.Hits[1].ID != "id2" {
		t.Fatalf("expected %q got %q", "id2", res.Hits[0].Index)
	}
}

func Test_simpleIPv4MatchQuery(t *testing.T) {
	idx := createIdx(t)
	defer idx.Close()

	err := idx.Index("id1", doc{"192.168.1.21"})
	if err != nil {
		t.Fatal(err)
	}

	reqStr := `192.168.1.21`
	query := bleve.NewIPRangeQuery(reqStr)
	query.FieldVal = "ip"

	search := bleve.NewSearchRequest(query)
	res, err := idx.Search(search)
	if err != nil {
		t.Fatal(err)
	}

	if res.Total != 1 {
		t.Fatalf("failed to find %q, res -> %s", reqStr, res)
	}
	if res.Hits[0].ID != "id1" {
		t.Fatalf("expected %q got %q", "id1", res.Hits[0].Index)
	}
}

func Test_ipv4LiteralData(t *testing.T) {
	idx := createIdx(t)
	defer idx.Close()

	type stronglyTyped struct {
		IP net.IP `json:"ip"`
	}

	err := idx.Index("id1", stronglyTyped{net.ParseIP("192.168.1.21")})
	if err != nil {
		t.Fatal(err)
	}

	reqStr := `192.168.1.0/24`
	query := bleve.NewIPRangeQuery(reqStr)
	query.FieldVal = "ip"

	search := bleve.NewSearchRequest(query)
	res, err := idx.Search(search)
	if err != nil {
		t.Fatal(err)
	}

	if res.Total != 1 {
		t.Fatalf("failed to find %q, res -> %s", reqStr, res)
	}
	if res.Hits[0].ID != "id1" {
		t.Fatalf("expected %q got %q", "id1", res.Hits[0].Index)
	}
}

func Test_badIPFmt(t *testing.T) {
	idx := createIdx(t)
	defer idx.Close()

	reqStr := `192.168.1.`
	query := bleve.NewIPRangeQuery(reqStr)
	query.FieldVal = "ip"

	search := bleve.NewSearchRequest(query)
	_, err := idx.Search(search)
	if err == nil {
		t.Errorf("%q is not a valid IP", reqStr)
	}
}

func Test_badCIDRFmt(t *testing.T) {
	idx := createIdx(t)
	defer idx.Close()

	reqStr := `/`
	query := bleve.NewIPRangeQuery(reqStr)
	query.FieldVal = "ip"

	err := query.Validate()
	if err == nil {
		t.Errorf("%q is not a valid CIDR", reqStr)
	}

	search := bleve.NewSearchRequest(query)
	_, err = idx.Search(search)
	if err == nil {
		t.Errorf("%q is not a valid CIDR", reqStr)
	}
}
