# Hierarchical nested search

* *v2.6.0* (and after) will come with support for **Array indexing and hierarchical nested search**.
* We've achieved this by embedding nested documents within our bleve (scorch) indexes.
* Usage of zap file format: [v17](https://github.com/blevesearch/zapx/blob/master/zap.md). Here we preserve hierarchical document relationships within segments, continuing to conform to the segmented architecture of *scorch*.

## Supported

* Indexing `Arrays` allows specifying fields that contain arrays of objects. Each object in the array can have its own set of fields, enabling the representation of hierarchical data structures within a single document.

```json
{
    "id": "1",
    "name": "John Doe",
    "addresses": [
        {
            "type": "home",
            "street": "123 Main St",
            "city": "Hometown",
            "zip": "12345"
        },
        {
            "type": "work",
            "street": "456 Corporate Blvd",
            "city": "Metropolis",
            "zip": "67890"
        }
    ]
}
```

* Multi-level arrays: Arrays can contain objects that themselves have array fields, allowing for deeply nested structures, such as a list of projects, each with its own list of tasks.

```json
{
    "id": "2",
    "name": "Jane Smith",
    "projects": [
        {
            "name": "Project Alpha",
            "tasks": [
                {"title": "Task 1", "status": "completed"},
                {"title": "Task 2", "status": "in-progress"}
            ]
        },
        {
            "name": "Project Beta",
            "tasks": [
                {"title": "Task A", "status": "not-started"},
                {"title": "Task B", "status": "completed"}
            ]
        }
    ]
}
```

* Multiple arrays: A document can have multiple fields that are arrays, each representing different hierarchical data, such as a list of phone numbers and a list of email addresses.

```json
{
    "id": "3",
    "name": "Alice Johnson",
    "phones": [
        {"type": "mobile", "number": "555-1234"},
        {"type": "home", "number": "555-5678"}
    ],
    "emails": [
        {"type": "personal", "address": "alice@example.com"},
        {"type": "work", "address": "alice@work.com"}
    ]
}
```

* Hybrid arrays: Multi-level and multiple arrays can be combined within the same document to represent complex hierarchical data structures, such as a company with multiple departments, each having its own list of employees and projects.

```json
{
    "id": "doc1",
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
            {"city": "Athens","country": "Greece"},
            {"city": "Berlin","country": "USA"}
        ]
    }
}
```

* Earlier versions of Bleve only supported flat arrays of primitive types (e.g., strings, numbers), and would flatten nested structures, losing the hierarchical relationships, so the above complex documents could not be accurately represented or queried. For example, the "employees" and "projects" fields within each department would be flattened, making it impossible to associate employees with their respective departments.

* From v2.6.0 onwards, Bleve allows for accurate representation and querying of complex nested structures, preserving the relationships between different levels of the hierarchy, across multi-level, multiple and hybrid arrays.

* The addition of `nested` document mappings enable defining fields that contain arrays of objects, giving the option to preserve the hierarchical relationships within the array during indexing. Having `nested` as false (default) will flatten the objects within the array, losing the hierarchy, which was the earlier behavior.

```json
{
    "departments": {  
        "dynamic": false,
        "enabled": true,
        "nested": true,
        "properties": {
            "employees": {
                "dynamic": false,
                "enabled": true,
                "nested": true 
            },
            "projects": {
                "dynamic": false,
                "enabled": true,
                "nested": true
            }
        }
    },
    "locations": {
        "dynamic": false,
        "enabled": true,
        "nested": true
    }
}
```

* Any Bleve query (e.g., `match`, `phrase`, `term`, `fuzzy`, `numeric/date range` etc.) can be executed against fields within nested documents, with no special handling required. The query processor will automatically traverse the nested structures to find matches. Additional search constructs
like vector search, synonym search, hybrid and pre-filtered vector search integrate seamlessly with hierarchy search.

* Conjunction Queries (AND queries) and other queries that depend on term co-occurrence within the same hierarchical context will respect the boundaries of nested documents. This means that terms must appear within the same nested object to be considered a match. For example, a conjunction query searching for an employee named "Alice" with the role "Engineer" within the "Engineering" department will only return results where both name and role terms are found within the same employee object, which is itself within a "Engineering" department object.

* Some other search constructs will have enhanced precision with hierarchy search.
  * Field-Level Highlighting: Only fields within the matched nested object are retrieved and highlighted, ensuring highlights appear in the correct hierarchical context. For example, a match in `departments[name=Engineering].employees` highlights only employees in that department.

  * Nested Faceting / Aggregations: Facets are computed within matched nested objects, producing context-aware buckets. E.g., a facet on `departments.projects.status` returns ongoing or completed only for projects in matched departments.

  * Sorting by Nested Fields: Sorting can use fields from the relevant nested object, e.g., ordering companies by `departments.budget` sorts based on the budget of the specific matched department, not unrelated departments.

* Vector Search (KNN / Multi-KNN): When a document contains an array of objects with vector/multi-vector fields, the final document score and ranking are identical whether or not the array is marked as `nested`. In both cases, the highest-scoring vector is selected; either directly from the array (non-nested) or from the best-matching nested object with its score bubbled up to the parent document.

* Pre-Filtered Vector Search: When vector search is combined with filters on fields inside a nested array, the filters are applied first to pick which nested items are eligible. Vector similarity is then computed only on the vector fields of those filtered nested objects. For example, if `departments.employees` is a `nested` array, a pre-filtered KNN query for employees with a `skills_vector` matching `machine learning engineer`, a role of `Manager`, and belonging to the `Sales` department will first narrow the candidate set to only employees who meet the requirement, and then compute vector similarity on the `skills_vector` of that filtered subset. This ensures that vector search results come only from the employees that satisfy the filter, and not from unrelated employees in other departments.

## Indexing

Below is an example of using the Bleve API to index documents with hierarchical structures, using hybrid arrays and nested mappings.

```go
// Define a document to be indexed.
docJSON :=
    `{
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
    }`

// Define departments as a nested document mapping (since it contains arrays of objects)
// and index name and budget fields
departmentsMapping := bleve.NewNestedDocumentMapping()
departmentsMapping.AddFieldMappingsAt("name", bleve.NewTextFieldMapping())
departmentsMapping.AddFieldMappingsAt("budget", bleve.NewNumericFieldMapping())

// Define employees as a nested document mapping within departments (since it contains arrays of objects)
// and index name and role fields
employeesMapping := bleve.NewNestedDocumentMapping()
employeesMapping.AddFieldMappingsAt("name", bleve.NewTextFieldMapping())
employeesMapping.AddFieldMappingsAt("role", bleve.NewTextFieldMapping())
departmentsMapping.AddSubDocumentMapping("employees", employeesMapping)

// Define projects as a nested document mapping within departments (since it contains arrays of objects)
// and index title and status fields
projectsMapping := bleve.NewNestedDocumentMapping()
projectsMapping.AddFieldMappingsAt("title", bleve.NewTextFieldMapping())
projectsMapping.AddFieldMappingsAt("status", bleve.NewTextFieldMapping())
departmentsMapping.AddSubDocumentMapping("projects", projectsMapping)

// Define locations as a nested document mapping (since it contains arrays of objects) 
// and index city and country fields
locationsMapping := bleve.NewNestedDocumentMapping()
locationsMapping.AddFieldMappingsAt("city", bleve.NewTextFieldMapping())
locationsMapping.AddFieldMappingsAt("country", bleve.NewTextFieldMapping())

// Define company as a document mapping and index its name field and 
// add departments and locations as sub-document mappings
companyMapping := bleve.NewDocumentMapping()
companyMapping.AddFieldMappingsAt("name", bleve.NewTextFieldMapping())
companyMapping.AddSubDocumentMapping("departments", departmentsMapping)
companyMapping.AddSubDocumentMapping("locations", locationsMapping)

// Define the final index mapping and add company as a sub-document mapping in the default mapping
indexMapping := bleve.NewIndexMapping()
indexMapping.DefaultMapping.AddSubDocumentMapping("company", companyMapping)

// Create the index with the defined mapping
index, err := bleve.New("hierarchy_example.bleve", indexMapping)
if err != nil {
    panic(err)
}

// Unmarshal the document JSON into a map, for indexing
var doc map[string]interface{}
err = json.Unmarshal([]byte(docJSON), &doc)
if err != nil {
    panic(err)
}

// Index the document
err = index.Index("doc1", doc)
if err != nil {
    panic(err)
}
```

## Querying

```go
// Open the index
index, err := bleve.Open("hierarchy_example.bleve")
if err != nil {
    panic(err)
}

var (
    req *bleve.SearchRequest
    res *bleve.SearchResult
)

// Example 1: Simple Match Query on a field within a nested document, should work as if it were a flat field
q1 := bleve.NewMatchQuery("Engineer")
q1.SetField("company.departments.employees.role")
req = bleve.NewSearchRequest(q1)
res, err = index.Search(req)
if err != nil {
    panic(err)
}
fmt.Println("Match Query Results:", res)

// Example 2: Conjunction Query (AND) on fields within the same nested document
// like finding employees with name "Eve" and role "Manager". This will only match
// if both terms are in the same employee object.
q1 = bleve.NewMatchQuery("Eve")
q1.SetField("company.departments.employees.name")
q2 := bleve.NewMatchQuery("Manager")
q2.SetField("company.departments.employees.role")
conjQuery := bleve.NewConjunctionQuery(
    q1,
    q2,
)
req = bleve.NewSearchRequest(conjQuery)
res, err = index.Search(req)
if err != nil {
    panic(err)
}
fmt.Println("Conjunction Query Results:", res)

// Example 3: Multi-level Nested Query, finding projects with status "ongoing"
// within the "Engineering" department. This ensures both conditions are met
// within the correct hierarchy, i.e., the ongoing project must belong to the
// Engineering department.
q1 = bleve.NewMatchQuery("Engineering")
q1.SetField("company.departments.name")
q2 = bleve.NewMatchQuery("ongoing")
q2.SetField("company.departments.projects.status")
multiLevelQuery := bleve.NewConjunctionQuery(
    q1,
    q2,
)
req = bleve.NewSearchRequest(multiLevelQuery)
res, err = index.Search(req)
if err != nil {
    panic(err)
}
fmt.Println("Multi-level Nested Query Results:", res)

// Example 4: Multiple Arrays Query, finding documents with a location in "London"
// and an employee with the role "Manager". This checks conditions across different arrays.
q1 = bleve.NewMatchQuery("London")
q1.SetField("company.locations.city")
q2 = bleve.NewMatchQuery("Manager")
q2.SetField("company.departments.employees.role")
multiArrayQuery := bleve.NewConjunctionQuery(
    q1,
    q2,
)
req = bleve.NewSearchRequest(multiArrayQuery)
res, err = index.Search(req)
if err != nil {
    panic(err)
}
fmt.Println("Multiple Arrays Query Results:", res)

// Hybrid Arrays Query, combining multi-level and multiple arrays,
// finding documents with a Manager named Ivan working in Edinburgh, UK
q1 = bleve.NewMatchQuery("Ivan")
q1.SetField("company.departments.employees.name")
q2 = bleve.NewMatchQuery("Manager")
q2.SetField("company.departments.employees.role")
q3 := bleve.NewMatchQuery("Edinburgh")
q3.SetField("company.locations.city")
q4 := bleve.NewMatchQuery("UK")
q4.SetField("company.locations.country")
hybridArrayQuery := bleve.NewConjunctionQuery(
    bleve.NewConjunctionQuery(
        q1,
        q2,
    ),
    bleve.NewConjunctionQuery(
        q3,
        q4,
    ),
)
req = bleve.NewSearchRequest(hybridArrayQuery)
res, err = index.Search(req)
if err != nil {
    panic(err)
}
fmt.Println("Hybrid Arrays Query Results:", res)

// Close the index when done
err = index.Close()
if err != nil {
    panic(err)
}
```
