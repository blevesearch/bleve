# Creating a Bleve Index

A simple how-to example using Bleve in Go to create an index, add documents, and run search queries with results.

```go
package main

import (
    "fmt"
    "log"

    bleve "github.com/blevesearch/bleve/v2"
)

type Document struct {
    ID      string `json:"id"`
    Title   string `json:"title"`
    Content string `json:"content"`
}

func main() {
    indexPath := "example.bleve"
    // Create a new index
    mapping := bleve.NewIndexMapping()
    index, err := bleve.New(indexPath, mapping)
    if err != nil {
        log.Fatal(err)
    }
    defer index.Close()

    // Add documents
    documents := []Document{
        {
            ID:      "doc",
            Title:   "Bleve documentation",
            Content: "Bleve provides full-text search capabilities.",
        },
        {
            ID:      "doc1",
            Title:   "Elasticsearch documentation",
            Content: "Elasticsearch provides full-text search capabilities as well.",
        },
    }

    // Iterate and index the documents
    batch := index.NewBatch()
    for _, doc := range documents {
        batch.Index(doc.ID, doc)
    }
    if err := index.Batch(batch); err != nil {
        log.Fatal(err)
    }

    // Search the created index
    query := bleve.NewQueryStringQuery("bleve")
    searchRequest := bleve.NewSearchRequest(query)
    searchRequest.Explain = true
    searchRequest.Fields = []string{"title", "content"}
    searchResult, err := index.Search(searchRequest)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println(searchResult)
}

```

## Output

```bash
$ go run main.go

1 matches, showing 1 through 1, took 262.333µs
    1. doc (0.471405)
        title
                Bleve documentation
        content
                Bleve provides full-text search capabilities.
```

## Step-by-Step Breakdown

### 1. Index Creation

```go
// Create a new index mapping
mapping := bleve.NewIndexMapping()
// Create a new index (this creates a directory on disk)
index, err := bleve.New(indexPath, mapping)
```

**What happens:**

- Creates an index mapping with default settings
- Creates a new index directory `example.bleve/`
- Sets up the underlying storage (Scorch engine by default)

### 2. Document Indexing

```go
// Index a document with a unique ID
err := index.Index("doc", map[string]interface{}{
    "title":   "My Document",
    "content": "This is the document content",
    "author":  "John Doe",
})
```

**What happens:**

- Document gets a unique ID (`doc`)
- Fields are automatically mapped based on their Go types
- Text fields are analyzed (tokenized, lowercased, etc.) based on the mapping chosen (here, the default one)
- Document is stored in the search index

### 3. Searching

```go
// Create a query
query := bleve.NewQueryStringQuery("search terms")
request := bleve.NewSearchRequest(query)

// Execute search
results, err := index.Search(request)
```

**What happens:**

- Query string is parsed and analyzed
- Index is searched for matching documents
- Results are scored and ranked by relevance by the algorithm used
- Document metadata and highlights are returned

## Working with Existing Indexes

To open an existing index instead of creating a new one:

```go
// Open existing index
index, err := bleve.Open("example.bleve")
if err != nil {
    log.Fatal(err)
}
defer index.Close()
```

## Different Query Types

### 1. Query String Query (Simple)

```go
query := bleve.NewQueryStringQuery("golang programming")
```

### 2. Match Query (Exact Field)

```go
query := bleve.NewMatchQuery("bleve")
query.SetField("title")  // Search only in title field
```

### 3. Boolean Query (Complex)

```go
mustQuery := bleve.NewMatchQuery("golang")
shouldQuery := bleve.NewMatchQuery("programming")

boolQuery := bleve.NewBooleanQuery()
boolQuery.AddMust(mustQuery)
boolQuery.AddShould(shouldQuery)
```

### 4. Range Query (Numeric/Date)

```go
minPrice := 20.50
maxPrice := 40.75
query := bleve.NewNumericRangeQuery(&minPrice, &maxPrice)
query.SetField("price")
```

## Advanced Index Configuration

### Custom Field Mapping

```go
// We can create customised mapping as well by specifying about analyzers
mapping := bleve.NewIndexMapping()

// Text field with custom analyzer
titleMapping := bleve.NewTextFieldMapping()
titleMapping.Analyzer = "en"  // English analyzer

// Numeric field
priceMapping := bleve.NewNumericFieldMapping()

// Date field
dateMapping := bleve.NewDateTimeFieldMapping()

// Document mapping
docMapping := bleve.NewDocumentMapping()
docMapping.AddFieldMappingsAt("title", titleMapping)
docMapping.AddFieldMappingsAt("price", priceMapping)
docMapping.AddFieldMappingsAt("created_at", dateMapping)

// Add to index mapping
mapping.AddDocumentMapping("product", docMapping)

// Create index with custom mapping
index, err := bleve.New("products.bleve", mapping)
```

### Batch Operations

For better performance when indexing many documents, we can do indexing in batches:

```go
batch := index.NewBatch()

for _, doc := range documents {
    batch.Index(doc.ID, doc)
}

// Execute batch
err := index.Batch(batch)
```
