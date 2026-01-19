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

package bleve

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/highlight/highlighter/ansi"
	"github.com/blevesearch/bleve/v2/search/query"
)

func createNestedIndexMapping() mapping.IndexMapping {
	/*
		company
		├── id
		├── name
		├── departments[] (nested)
		│     ├── name
		│     ├── budget
		│     ├── employees[] (nested)
		│     │     ├── name
		│     │     ├── role
		│     └── projects[] (nested)
		│           ├── title
		│           ├── status
		└── locations[] (nested)
				├── city
				├── country
	*/

	// Create the index mapping
	imap := mapping.NewIndexMapping()

	// Create company mapping
	companyMapping := mapping.NewDocumentMapping()

	// Company ID field
	companyIDField := mapping.NewTextFieldMapping()
	companyMapping.AddFieldMappingsAt("id", companyIDField)

	// Company name field
	companyNameField := mapping.NewTextFieldMapping()
	companyMapping.AddFieldMappingsAt("name", companyNameField)

	// Departments mapping
	departmentsMapping := mapping.NewNestedDocumentMapping()

	// Department name field
	deptNameField := mapping.NewTextFieldMapping()
	departmentsMapping.AddFieldMappingsAt("name", deptNameField)

	// Department budget field
	deptBudgetField := mapping.NewNumericFieldMapping()
	departmentsMapping.AddFieldMappingsAt("budget", deptBudgetField)

	// Employees mapping
	employeesMapping := mapping.NewNestedDocumentMapping()

	// Employee name field
	empNameField := mapping.NewTextFieldMapping()
	employeesMapping.AddFieldMappingsAt("name", empNameField)

	// Employee role field
	empRoleField := mapping.NewTextFieldMapping()
	employeesMapping.AddFieldMappingsAt("role", empRoleField)

	departmentsMapping.AddSubDocumentMapping("employees", employeesMapping)

	// Projects mapping
	projectsMapping := mapping.NewNestedDocumentMapping()

	// Project title field
	projTitleField := mapping.NewTextFieldMapping()
	projectsMapping.AddFieldMappingsAt("title", projTitleField)

	// Project status field
	projStatusField := mapping.NewTextFieldMapping()
	projectsMapping.AddFieldMappingsAt("status", projStatusField)

	departmentsMapping.AddSubDocumentMapping("projects", projectsMapping)

	companyMapping.AddSubDocumentMapping("departments", departmentsMapping)

	// Locations mapping
	locationsMapping := mapping.NewNestedDocumentMapping()

	// Location city field
	cityField := mapping.NewTextFieldMapping()
	locationsMapping.AddFieldMappingsAt("city", cityField)

	// Location country field
	countryField := mapping.NewTextFieldMapping()
	locationsMapping.AddFieldMappingsAt("country", countryField)

	companyMapping.AddSubDocumentMapping("locations", locationsMapping)

	// Add company to type mapping
	imap.DefaultMapping.AddSubDocumentMapping("company", companyMapping)

	return imap
}

func TestNestedPrefixes(t *testing.T) {
	imap := createNestedIndexMapping()

	tmpIndexPath := createTmpIndexPath(t)
	defer cleanupTmpIndexPath(t, tmpIndexPath)

	idx, err := New(tmpIndexPath, imap)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := idx.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	nmap, ok := imap.(mapping.NestedMapping)
	if !ok {
		t.Fatal("index mapping is not a NestedMapping")
	}

	// ----------------------------------------------------------------------
	// Test 1: Employee Role AND Employee Name
	// ----------------------------------------------------------------------
	fs := search.NewFieldSet()
	fs.AddField("company.departments.employees.role")
	fs.AddField("company.departments.employees.name")

	expectedCommon := 2
	expectedMax := 2

	common, max := nmap.NestedDepth(fs)
	if common != expectedCommon || max != expectedMax {
		t.Fatalf("Test1: expected (common=%d, max=%d), got (common=%d, max=%d)",
			expectedCommon, expectedMax, common, max)
	}

	// ----------------------------------------------------------------------
	// Test 2: Employee Role AND Employee Name AND Department Name
	// ----------------------------------------------------------------------
	fs = search.NewFieldSet()
	fs.AddField("company.departments.employees.role")
	fs.AddField("company.departments.employees.name")
	fs.AddField("company.departments.name")

	expectedCommon = 1
	expectedMax = 2 // employees nested deeper

	common, max = nmap.NestedDepth(fs)
	if common != expectedCommon || max != expectedMax {
		t.Fatalf("Test2: expected (common=%d, max=%d), got (common=%d, max=%d)",
			expectedCommon, expectedMax, common, max)
	}

	// ----------------------------------------------------------------------
	// Test 3: Employee Role AND Location City
	// ----------------------------------------------------------------------
	fs = search.NewFieldSet()
	fs.AddField("company.departments.employees.role")
	fs.AddField("company.locations.city")

	expectedCommon = 0
	expectedMax = 2 // employees deeper than locations (1)

	common, max = nmap.NestedDepth(fs)
	if common != expectedCommon || max != expectedMax {
		t.Fatalf("Test3: expected (common=%d, max=%d), got (common=%d, max=%d)",
			expectedCommon, expectedMax, common, max)
	}

	// ----------------------------------------------------------------------
	// Test 4: Company Name AND Location Country
	// ----------------------------------------------------------------------
	fs = search.NewFieldSet()
	fs.AddField("company.name")
	fs.AddField("company.locations.country")
	fs.AddField("company.locations.city")

	expectedCommon = 0
	expectedMax = 1 // locations.country and locations.city share depth 1

	common, max = nmap.NestedDepth(fs)
	if common != expectedCommon || max != expectedMax {
		t.Fatalf("Test4: expected (common=%d, max=%d), got (common=%d, max=%d)",
			expectedCommon, expectedMax, common, max)
	}

	// ----------------------------------------------------------------------
	// Test 5: Department Budget AND Project Status AND Employee Name
	// ----------------------------------------------------------------------
	fs = search.NewFieldSet()
	fs.AddField("company.departments.budget")
	fs.AddField("company.departments.projects.status")
	fs.AddField("company.departments.employees.name")

	expectedCommon = 1
	expectedMax = 2 // employees + projects go deeper

	common, max = nmap.NestedDepth(fs)
	if common != expectedCommon || max != expectedMax {
		t.Fatalf("Test5: expected (common=%d, max=%d), got (common=%d, max=%d)",
			expectedCommon, expectedMax, common, max)
	}

	// ----------------------------------------------------------------------
	// Test 6: Single Field
	// ----------------------------------------------------------------------
	fs = search.NewFieldSet()
	fs.AddField("company.id")

	expectedCommon = 0
	expectedMax = 0

	common, max = nmap.NestedDepth(fs)
	if common != expectedCommon || max != expectedMax {
		t.Fatalf("Test6: expected (common=%d, max=%d), got (common=%d, max=%d)",
			expectedCommon, expectedMax, common, max)
	}

	// ----------------------------------------------------------------------
	// Test 7: No Fields
	// ----------------------------------------------------------------------
	fs = search.NewFieldSet()

	expectedCommon = 0
	expectedMax = 0

	common, max = nmap.NestedDepth(fs)
	if common != expectedCommon || max != expectedMax {
		t.Fatalf("Test7: expected (common=%d, max=%d), got (common=%d, max=%d)",
			expectedCommon, expectedMax, common, max)
	}

	// ----------------------------------------------------------------------
	// Test 8: All Fields
	// ----------------------------------------------------------------------
	fs = search.NewFieldSet()
	fs.AddField("company.id")
	fs.AddField("company.name")
	fs.AddField("company.departments.name")
	fs.AddField("company.departments.budget")
	fs.AddField("company.departments.employees.name")
	fs.AddField("company.departments.employees.role")
	fs.AddField("company.departments.projects.title")
	fs.AddField("company.departments.projects.status")
	fs.AddField("company.locations.city")
	fs.AddField("company.locations.country")

	expectedCommon = 0 // spans different contexts
	expectedMax = 2

	common, max = nmap.NestedDepth(fs)
	if common != expectedCommon || max != expectedMax {
		t.Fatalf("Test8: expected (common=%d, max=%d), got (common=%d, max=%d)",
			expectedCommon, expectedMax, common, max)
	}

	// ----------------------------------------------------------------------
	// Test 9: Project Title AND Project Status
	// ----------------------------------------------------------------------
	fs = search.NewFieldSet()
	fs.AddField("company.departments.projects.title")
	fs.AddField("company.departments.projects.status")

	expectedCommon = 2
	expectedMax = 2

	common, max = nmap.NestedDepth(fs)
	if common != expectedCommon || max != expectedMax {
		t.Fatalf("Test9: expected (common=%d, max=%d), got (common=%d, max=%d)",
			expectedCommon, expectedMax, common, max)
	}

	// ----------------------------------------------------------------------
	// Test 10: Department Name AND Location Country
	// ----------------------------------------------------------------------
	fs = search.NewFieldSet()
	fs.AddField("company.departments.name")
	fs.AddField("company.locations.country")
	fs.AddField("company.locations.city")

	expectedCommon = 0
	expectedMax = 1 // locations share depth 1

	common, max = nmap.NestedDepth(fs)
	if common != expectedCommon || max != expectedMax {
		t.Fatalf("Test10: expected (common=%d, max=%d), got (common=%d, max=%d)",
			expectedCommon, expectedMax, common, max)
	}
}

func TestNestedConjunctionQuery(t *testing.T) {
	imap := createNestedIndexMapping()
	err := imap.Validate()
	if err != nil {
		t.Fatalf("expected valid nested index mapping, got error: %v", err)
	}
	tmpIndexPath := createTmpIndexPath(t)
	defer cleanupTmpIndexPath(t, tmpIndexPath)
	idx, err := New(tmpIndexPath, imap)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()
	// Index 3 sample documents
	docs := []struct {
		id   string
		data string
	}{
		{
			id: "doc1",
			data: `{
				"company": {
					"id": "c1",
					"name": "TechCorp",
					"departments": [
						{
							"name": "Engineering",
							"budget": 2000000,
							"employees": [
								{"name": "Alice", "role": "Engineer"},
								{"name": "Bob", "role": "Manager"}
							],
							"projects": [
								{"title": "Project X", "status": "ongoing"},
								{"title": "Project Y", "status": "completed"}
							]
						},
						{
							"name": "Sales",
							"budget": 300000,
							"employees": [
								{"name": "Eve", "role": "Salesperson"},
								{"name": "Mallory", "role": "Manager"}
							],
							"projects": [
								{"title": "Project A", "status": "completed"},
								{"title": "Project B", "status": "ongoing"}
							]
						}	
					],
					"locations": [
						{"city": "Athens", "country": "Greece"},
						{"city": "Berlin", "country": "USA"}
					]
				}
			}`,
		},
		{
			id: "doc2",
			data: `{
				"company" : {
					"id": "c2",
					"name": "BizInc",
					"departments": [
						{
							"name": "Marketing",
							"budget": 800000,
							"employees": [
								{"name": "Eve", "role": "Marketer"},
								{"name": "David", "role": "Manager"}
							],
							"projects": [
								{"title": "Project Z", "status": "ongoing"},
								{"title": "Project W", "status": "planned"}
							]
						},
						{
							"name": "Engineering",
							"budget": 800000,
							"employees": [
								{"name": "Frank", "role": "Manager"},
								{"name": "Grace", "role": "Engineer"}
							],
							"projects": [
								{"title": "Project Alpha", "status": "completed"},
								{"title": "Project Beta", "status": "ongoing"}
							]
						}	
					],
					"locations": [
						{"city": "Athens", "country": "USA"},
						{"city": "London", "country": "UK"}
					]
				}
			}`,
		},
		{
			id: "doc3",
			data: `{
				"company": {
					"id": "c3",
					"name": "WebSolutions",
					"departments": [
						{
							"name": "HR",
							"budget": 800000,
							"employees": [
								{"name": "Eve", "role": "Manager"},
								{"name": "Frank", "role": "HR"}
							],
							"projects": [
								{"title": "Project Beta", "status": "completed"},
								{"title": "Project B", "status": "ongoing"}
							]
						},
						{
							"name": "Engineering",
							"budget": 200000,
							"employees": [
								{"name": "Heidi", "role": "Support Engineer"},
								{"name": "Ivan", "role": "Manager"}
							],
							"projects": [
								{"title": "Project Helpdesk", "status": "ongoing"},
								{"title": "Project FAQ", "status": "completed"}
							]
						}
					],
					"locations": [
						{"city": "Edinburgh", "country": "UK"},
						{"city": "London", "country": "Canada"}
					]
				}
			}`,
		},
	}

	for _, doc := range docs {
		var dataMap map[string]interface{}
		err := json.Unmarshal([]byte(doc.data), &dataMap)
		if err != nil {
			t.Fatalf("failed to unmarshal document %s: %v", doc.id, err)
		}
		err = idx.Index(doc.id, dataMap)
		if err != nil {
			t.Fatalf("failed to index document %s: %v", doc.id, err)
		}
	}

	var buildReq = func(subQueries []query.Query) *SearchRequest {
		rv := NewSearchRequest(query.NewConjunctionQuery(subQueries))
		rv.SortBy([]string{"_id"})
		rv.Fields = []string{"*"}
		rv.Highlight = NewHighlightWithStyle(ansi.Name)
		return rv
	}

	var (
		req             *SearchRequest
		res             *SearchResult
		deptNameQuery   *query.MatchQuery
		deptBudgetQuery *query.NumericRangeQuery
		empNameQuery    *query.MatchQuery
		empRoleQuery    *query.MatchQuery
		projTitleQuery  *query.MatchPhraseQuery
		projStatusQuery *query.MatchQuery
		countryQuery    *query.MatchQuery
		cityQuery       *query.MatchQuery
	)

	// Test 1: Find companies with a department named "Engineering" AND budget over 900000
	deptNameQuery = query.NewMatchQuery("Engineering")
	deptNameQuery.SetField("company.departments.name")

	min := float64(800000)
	deptBudgetQuery = query.NewNumericRangeQuery(&min, nil)
	deptBudgetQuery.SetField("company.departments.budget")

	req = buildReq([]query.Query{deptNameQuery, deptBudgetQuery})
	res, err = idx.Search(req)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if len(res.Hits) != 2 {
		t.Fatalf("expected 2 hit, got %d", len(res.Hits))
	}
	if res.Hits[0].ID != "doc1" || res.Hits[1].ID != "doc2" {
		t.Fatalf("unexpected hit IDs: %v, %v", res.Hits[0].ID, res.Hits[1].ID)
	}

	// Test 2: Find companies with an employee named "Eve" AND project status "completed"
	empNameQuery = query.NewMatchQuery("Eve")
	empNameQuery.SetField("company.departments.employees.name")

	projStatusQuery = query.NewMatchQuery("completed")
	projStatusQuery.SetField("company.departments.projects.status")

	req = buildReq([]query.Query{empNameQuery, projStatusQuery})
	res, err = idx.Search(req)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if len(res.Hits) != 2 {
		t.Fatalf("expected 2 hits, got %d", len(res.Hits))
	}
	if res.Hits[0].ID != "doc1" || res.Hits[1].ID != "doc3" {
		t.Fatalf("unexpected hit IDs: %v, %v", res.Hits[0].ID, res.Hits[1].ID)
	}

	// Test 3: Find companies located in "Athens, USA" AND with an Engineering department
	countryQuery = query.NewMatchQuery("USA")
	countryQuery.SetField("company.locations.country")

	cityQuery = query.NewMatchQuery("Athens")
	cityQuery.SetField("company.locations.city")

	locQuery := query.NewConjunctionQuery([]query.Query{countryQuery, cityQuery})

	deptNameQuery = query.NewMatchQuery("Engineering")
	deptNameQuery.SetField("company.departments.name")

	req = buildReq([]query.Query{locQuery, deptNameQuery})
	res, err = idx.Search(req)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if len(res.Hits) != 1 {
		t.Fatalf("expected 1 hit, got %d", len(res.Hits))
	}
	if res.Hits[0].ID != "doc2" {
		t.Fatalf("unexpected hit ID: %v", res.Hits[0].ID)
	}

	// Test 4a: Find companies located in "Athens, USA" AND with an Engineering department with a budget over 1M
	countryQuery = query.NewMatchQuery("USA")
	countryQuery.SetField("company.locations.country")

	cityQuery = query.NewMatchQuery("Athens")
	cityQuery.SetField("company.locations.city")

	locQuery = query.NewConjunctionQuery([]query.Query{countryQuery, cityQuery})

	deptNameQuery = query.NewMatchQuery("Engineering")
	deptNameQuery.SetField("company.departments.name")

	min = float64(1000000)
	deptBudgetQuery = query.NewNumericRangeQuery(&min, nil)
	deptBudgetQuery.SetField("company.departments.budget")

	deptQuery := query.NewConjunctionQuery([]query.Query{deptNameQuery, deptBudgetQuery})

	req = buildReq([]query.Query{locQuery, deptQuery})
	res, err = idx.Search(req)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if len(res.Hits) != 0 {
		t.Fatalf("expected 0 hits, got %d", len(res.Hits))
	}

	// Test 4b: Find companies located in "Athens, Greece" AND with an Engineering department with a budget over 1M
	countryQuery = query.NewMatchQuery("Greece")
	countryQuery.SetField("company.locations.country")

	cityQuery = query.NewMatchQuery("Athens")
	cityQuery.SetField("company.locations.city")

	locQuery = query.NewConjunctionQuery([]query.Query{countryQuery, cityQuery})

	deptNameQuery = query.NewMatchQuery("Engineering")
	deptNameQuery.SetField("company.departments.name")

	min = float64(1000000)
	deptBudgetQuery = query.NewNumericRangeQuery(&min, nil)
	deptBudgetQuery.SetField("company.departments.budget")

	deptQuery = query.NewConjunctionQuery([]query.Query{deptNameQuery, deptBudgetQuery})

	req = buildReq([]query.Query{locQuery, deptQuery})
	res, err = idx.Search(req)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if len(res.Hits) != 1 {
		t.Fatalf("expected 1 hits, got %d", len(res.Hits))
	}
	if res.Hits[0].ID != "doc1" {
		t.Fatalf("unexpected hit ID: %v", res.Hits[0].ID)
	}

	// Test 5a: Find companies with an employee named "Frank" AND role "Manager" whose department is
	// handling a project titled "Project Beta" which is marked as "completed"
	empNameQuery = query.NewMatchQuery("Frank")
	empNameQuery.SetField("company.departments.employees.name")

	empRoleQuery = query.NewMatchQuery("Manager")
	empRoleQuery.SetField("company.departments.employees.role")

	empQuery := query.NewConjunctionQuery([]query.Query{empNameQuery, empRoleQuery})

	projTitleQuery = query.NewMatchPhraseQuery("Project Beta")
	projTitleQuery.SetField("company.departments.projects.title")

	projStatusQuery = query.NewMatchQuery("completed")
	projStatusQuery.SetField("company.departments.projects.status")

	projQuery := query.NewConjunctionQuery([]query.Query{projTitleQuery, projStatusQuery})

	req = buildReq([]query.Query{empQuery, projQuery})
	res, err = idx.Search(req)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if len(res.Hits) != 0 {
		t.Fatalf("expected 0 hit, got %d", len(res.Hits))
	}

	// Test 5b: Find companies with an employee named "Frank" AND role "Manager" whose department is
	// handling a project titled "Project Beta" which is marked as "ongoing"
	empNameQuery = query.NewMatchQuery("Frank")
	empNameQuery.SetField("company.departments.employees.name")

	empRoleQuery = query.NewMatchQuery("Manager")
	empRoleQuery.SetField("company.departments.employees.role")

	empQuery = query.NewConjunctionQuery([]query.Query{empNameQuery, empRoleQuery})

	projTitleQuery = query.NewMatchPhraseQuery("Project Beta")
	projTitleQuery.SetField("company.departments.projects.title")

	projStatusQuery = query.NewMatchQuery("ongoing")
	projStatusQuery.SetField("company.departments.projects.status")

	projQuery = query.NewConjunctionQuery([]query.Query{projTitleQuery, projStatusQuery})

	req = buildReq([]query.Query{empQuery, projQuery})
	res, err = idx.Search(req)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if len(res.Hits) != 1 {
		t.Fatalf("expected 1 hit, got %d", len(res.Hits))
	}
	if res.Hits[0].ID != "doc2" {
		t.Fatalf("unexpected hit ID: %v", res.Hits[0].ID)
	}

	// Test 6a: Find companies with an employee named "Eve" AND role "Manager"
	// who is working in a department located in "London, UK"
	empNameQuery = query.NewMatchQuery("Eve")
	empNameQuery.SetField("company.departments.employees.name")

	empRoleQuery = query.NewMatchQuery("Manager")
	empRoleQuery.SetField("company.departments.employees.role")

	empQuery = query.NewConjunctionQuery([]query.Query{empNameQuery, empRoleQuery})

	countryQuery = query.NewMatchQuery("UK")
	countryQuery.SetField("company.locations.country")

	cityQuery = query.NewMatchQuery("London")
	cityQuery.SetField("company.locations.city")

	locQuery = query.NewConjunctionQuery([]query.Query{countryQuery, cityQuery})

	req = buildReq([]query.Query{empQuery, locQuery})
	res, err = idx.Search(req)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if len(res.Hits) != 0 {
		t.Fatalf("expected 0 hit, got %d", len(res.Hits))
	}

	// Test 6b: Find companies with an employee named "Eve" AND role "Manager"
	// who is working in a department located in "London, Canada"
	empNameQuery = query.NewMatchQuery("Eve")
	empNameQuery.SetField("company.departments.employees.name")

	empRoleQuery = query.NewMatchQuery("Manager")
	empRoleQuery.SetField("company.departments.employees.role")

	empQuery = query.NewConjunctionQuery([]query.Query{empNameQuery, empRoleQuery})

	countryQuery = query.NewMatchQuery("Canada")
	countryQuery.SetField("company.locations.country")

	cityQuery = query.NewMatchQuery("London")
	cityQuery.SetField("company.locations.city")

	locQuery = query.NewConjunctionQuery([]query.Query{countryQuery, cityQuery})

	req = buildReq([]query.Query{empQuery, locQuery})
	res, err = idx.Search(req)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if len(res.Hits) != 1 {
		t.Fatalf("expected 1 hit, got %d", len(res.Hits))
	}
	if res.Hits[0].ID != "doc3" {
		t.Fatalf("unexpected hit ID: %v", res.Hits[0].ID)
	}

	// Test 7a: Find companies where Ivan the Manager works London, UK

	empNameQuery = query.NewMatchQuery("Ivan")
	empNameQuery.SetField("company.departments.employees.name")

	empRoleQuery = query.NewMatchQuery("Manager")
	empRoleQuery.SetField("company.departments.employees.role")

	empQuery = query.NewConjunctionQuery([]query.Query{empNameQuery, empRoleQuery})

	countryQuery = query.NewMatchQuery("UK")
	countryQuery.SetField("company.locations.country")

	cityQuery = query.NewMatchQuery("London")
	cityQuery.SetField("company.locations.city")

	locQuery = query.NewConjunctionQuery([]query.Query{countryQuery, cityQuery})

	req = buildReq([]query.Query{empQuery, locQuery})
	res, err = idx.Search(req)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if len(res.Hits) != 0 {
		t.Fatalf("expected 0 hit, got %d", len(res.Hits))
	}

	// Test 7b: Find companies where Ivan the Manager works London, Canada

	empNameQuery = query.NewMatchQuery("Ivan")
	empNameQuery.SetField("company.departments.employees.name")

	empRoleQuery = query.NewMatchQuery("Manager")
	empRoleQuery.SetField("company.departments.employees.role")

	empQuery = query.NewConjunctionQuery([]query.Query{empNameQuery, empRoleQuery})

	countryQuery = query.NewMatchQuery("Canada")
	countryQuery.SetField("company.locations.country")

	cityQuery = query.NewMatchQuery("London")
	cityQuery.SetField("company.locations.city")

	locQuery = query.NewConjunctionQuery([]query.Query{countryQuery, cityQuery})

	req = buildReq([]query.Query{empQuery, locQuery})
	res, err = idx.Search(req)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if len(res.Hits) != 1 {
		t.Fatalf("expected 1 hit, got %d", len(res.Hits))
	}
	if res.Hits[0].ID != "doc3" {
		t.Fatalf("unexpected hit ID: %v", res.Hits[0].ID)
	}

	// Test 8: Find companies where Frank the Manager works in Engineering department located in London, UK
	empNameQuery = query.NewMatchQuery("Frank")
	empNameQuery.SetField("company.departments.employees.name")

	empRoleQuery = query.NewMatchQuery("Manager")
	empRoleQuery.SetField("company.departments.employees.role")

	empQuery = query.NewConjunctionQuery([]query.Query{empNameQuery, empRoleQuery})

	deptNameQuery = query.NewMatchQuery("Engineering")
	deptNameQuery.SetField("company.departments.name")

	deptQuery = query.NewConjunctionQuery([]query.Query{empQuery, deptNameQuery})

	countryQuery = query.NewMatchQuery("UK")
	countryQuery.SetField("company.locations.country")

	cityQuery = query.NewMatchQuery("London")
	cityQuery.SetField("company.locations.city")

	locQuery = query.NewConjunctionQuery([]query.Query{countryQuery, cityQuery})

	req = buildReq([]query.Query{deptQuery, locQuery})
	res, err = idx.Search(req)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if len(res.Hits) != 1 {
		t.Fatalf("expected 1 hit, got %d", len(res.Hits))
	}
	if res.Hits[0].ID != "doc2" {
		t.Fatalf("unexpected hit ID: %v", res.Hits[0].ID)
	}
}

func TestNestedArrayConjunctionQuery(t *testing.T) {
	imap := NewIndexMapping()
	groupsMapping := mapping.NewNestedDocumentMapping()

	nameField := mapping.NewTextFieldMapping()
	groupsMapping.AddFieldMappingsAt("first_name", nameField)
	groupsMapping.AddFieldMappingsAt("last_name", nameField)

	imap.DefaultMapping.AddSubDocumentMapping("groups", groupsMapping)

	tmpIndexPath := createTmpIndexPath(t)
	defer cleanupTmpIndexPath(t, tmpIndexPath)
	idx, err := New(tmpIndexPath, imap)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	docs := []string{
		`{
			"groups": [
				[
					{
						"first_name": "Alice",
						"last_name": "Smith"
					},
					{
						"first_name": "Bob",
						"last_name": "Johnson"
					}
				],
				[
					{
						"first_name": "Charlie",
						"last_name": "Williams"
					},
					{
						"first_name": "Diana",
						"last_name": "Brown"
					}
				]
			]
		}`,
		`{
			"groups": [
				{
					"first_name": "Alice",
					"last_name": "Smith"
				},
				{
					"first_name": "Bob",
					"last_name": "Johnson"
				},
				{
					"first_name": "Charlie",
					"last_name": "Williams"
				},
				{
					"first_name": "Diana",
					"last_name": "Brown"
				}
			]
		}`,
	}

	for i, doc := range docs {
		var dataMap map[string]interface{}
		err := json.Unmarshal([]byte(doc), &dataMap)
		if err != nil {
			t.Fatalf("failed to unmarshal document %d: %v", i, err)
		}
		err = idx.Index(fmt.Sprintf("%d", i+1), dataMap)
		if err != nil {
			t.Fatalf("failed to index document %d: %v", i, err)
		}
	}

	var (
		firstNameQuery *query.MatchQuery
		lastNameQuery  *query.MatchQuery
		conjQuery      *query.ConjunctionQuery
		searchReq      *SearchRequest
		res            *SearchResult
	)

	// Search for documents where first_name is "Alice" AND last_name is "Johnson"
	firstNameQuery = query.NewMatchQuery("Alice")
	firstNameQuery.SetField("groups.first_name")

	lastNameQuery = query.NewMatchQuery("Johnson")
	lastNameQuery.SetField("groups.last_name")

	conjQuery = query.NewConjunctionQuery([]query.Query{firstNameQuery, lastNameQuery})

	searchReq = NewSearchRequest(conjQuery)
	searchReq.SortBy([]string{"_id"})

	res, err = idx.Search(searchReq)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}

	if len(res.Hits) != 0 {
		t.Fatalf("expected 0 hits, got %d", len(res.Hits))
	}

	// Search for documents where first_name is "Bob" AND last_name is "Johnson"
	firstNameQuery = query.NewMatchQuery("Bob")
	firstNameQuery.SetField("groups.first_name")

	lastNameQuery = query.NewMatchQuery("Johnson")
	lastNameQuery.SetField("groups.last_name")

	conjQuery = query.NewConjunctionQuery([]query.Query{firstNameQuery, lastNameQuery})

	searchReq = NewSearchRequest(conjQuery)
	searchReq.SortBy([]string{"_id"})

	res, err = idx.Search(searchReq)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}

	if len(res.Hits) != 2 {
		t.Fatalf("expected 2 hits, got %d", len(res.Hits))
	}

	if res.Hits[0].ID != "1" || res.Hits[1].ID != "2" {
		t.Fatalf("unexpected hit IDs: %v, %v", res.Hits[0].ID, res.Hits[1].ID)
	}

	// Search for documents where first_name is "Alice" AND last_name is "Williams"
	firstNameQuery = query.NewMatchQuery("Alice")
	firstNameQuery.SetField("groups.first_name")

	lastNameQuery = query.NewMatchQuery("Williams")
	lastNameQuery.SetField("groups.last_name")

	conjQuery = query.NewConjunctionQuery([]query.Query{firstNameQuery, lastNameQuery})

	searchReq = NewSearchRequest(conjQuery)
	searchReq.SortBy([]string{"_id"})

	res, err = idx.Search(searchReq)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}

	if len(res.Hits) != 0 {
		t.Fatalf("expected 0 hits, got %d", len(res.Hits))
	}

	// Search for documents where first_name is "Diana" AND last_name is "Brown"
	firstNameQuery = query.NewMatchQuery("Diana")
	firstNameQuery.SetField("groups.first_name")

	lastNameQuery = query.NewMatchQuery("Brown")
	lastNameQuery.SetField("groups.last_name")

	conjQuery = query.NewConjunctionQuery([]query.Query{firstNameQuery, lastNameQuery})

	searchReq = NewSearchRequest(conjQuery)
	searchReq.SortBy([]string{"_id"})

	res, err = idx.Search(searchReq)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}

	if len(res.Hits) != 2 {
		t.Fatalf("expected 2 hits, got %d", len(res.Hits))
	}

	if res.Hits[0].ID != "1" || res.Hits[1].ID != "2" {
		t.Fatalf("unexpected hit IDs: %v, %v", res.Hits[0].ID, res.Hits[1].ID)
	}
}

func TestValidNestedMapping(t *testing.T) {
	// ensure that top-level mappings - DefaultMapping and any type mappings - cannot be nested mappings
	imap := mapping.NewIndexMapping()
	nestedMapping := mapping.NewNestedDocumentMapping()
	imap.DefaultMapping = nestedMapping
	err := imap.Validate()
	if err == nil {
		t.Fatalf("expected error for nested DefaultMapping, got nil")
	}
	// invalid nested type mapping
	imap = mapping.NewIndexMapping()
	imap.AddDocumentMapping("type1", nestedMapping)
	err = imap.Validate()
	if err == nil {
		t.Fatalf("expected error for nested type mapping, got nil")
	}
	// valid nested mappings within DefaultMapping
	imap = mapping.NewIndexMapping()
	docMapping := mapping.NewDocumentMapping()
	nestedMapping = mapping.NewNestedDocumentMapping()
	fieldMapping := mapping.NewTextFieldMapping()
	nestedMapping.AddFieldMappingsAt("field1", fieldMapping)
	docMapping.AddSubDocumentMapping("nestedField", nestedMapping)
	imap.DefaultMapping = docMapping
	err = imap.Validate()
	if err != nil {
		t.Fatalf("expected valid nested mapping, got error: %v", err)
	}
	// valid nested mappings within type mapping
	imap = mapping.NewIndexMapping()
	docMapping = mapping.NewDocumentMapping()
	nestedMapping = mapping.NewNestedDocumentMapping()
	fieldMapping = mapping.NewTextFieldMapping()
	nestedMapping.AddFieldMappingsAt("field1", fieldMapping)
	docMapping.AddSubDocumentMapping("nestedField", nestedMapping)
	imap.AddDocumentMapping("type1", docMapping)
	err = imap.Validate()
	if err != nil {
		t.Fatalf("expected valid nested mapping, got error: %v", err)
	}
	// some nested type mappings
	imap = mapping.NewIndexMapping()
	nestedMapping = mapping.NewNestedDocumentMapping()
	regularMapping := mapping.NewDocumentMapping()
	imap.AddDocumentMapping("non_nested1", regularMapping)
	imap.AddDocumentMapping("non_nested2", regularMapping)
	imap.AddDocumentMapping("nested1", nestedMapping)
	imap.AddDocumentMapping("nested2", nestedMapping)
	err = imap.Validate()
	if err == nil {
		t.Fatalf("expected error for nested type mappings, got nil")
	}
}
