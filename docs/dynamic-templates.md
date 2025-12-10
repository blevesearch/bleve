# Dynamic Templates

Dynamic templates allow you to define rules for how dynamically detected fields are mapped. When a document contains fields that don't have explicit mappings and dynamic mapping is enabled, templates are evaluated in order to determine how those fields should be indexed.

This feature is inspired by [Elasticsearch's dynamic_templates](https://www.elastic.co/guide/en/elasticsearch/reference/current/dynamic-templates.html) functionality.

## Overview

When indexing documents with dynamic mapping enabled, bleve automatically detects field types and creates appropriate field mappings. Dynamic templates give you fine-grained control over this process by letting you:

- Apply specific analyzers to fields matching name patterns
- Control which fields are stored, indexed, or have doc values
- Override the default type detection
- Exclude certain fields from specific mappings

## Template Definition

A dynamic template consists of matching criteria and a field mapping to apply:

```go
type DynamicTemplate struct {
    Name             string        // Optional identifier for debugging
    Match            string        // Glob pattern for field name
    Unmatch          string        // Exclusion pattern for field name
    PathMatch        string        // Glob pattern for full field path
    PathUnmatch      string        // Exclusion pattern for full path
    MatchMappingType string        // Filter by detected type
    Mapping          *FieldMapping // Mapping to apply when matched
}
```

### Matching Criteria

Templates support multiple matching criteria that are evaluated together (all specified criteria must match):

| Field | Description | Example |
|-------|-------------|---------|
| `Match` | Glob pattern for the field name (last path element) | `*_text`, `field_*` |
| `Unmatch` | Exclusion pattern for field name | `skip_*` |
| `PathMatch` | Glob pattern for the full dotted path | `metadata.**`, `user.*.name` |
| `PathUnmatch` | Exclusion pattern for full path | `internal.**` |
| `MatchMappingType` | Filter by detected type | `string`, `number`, `boolean`, `date` |

### Glob Pattern Syntax

Patterns use the [doublestar](https://github.com/bmatcuk/doublestar) library for matching:

- `*` - matches any sequence of characters within a single path segment
- `**` - matches any sequence of characters across multiple path segments

Examples:
- `*_text` matches `title_text`, `body_text`
- `field_*` matches `field_name`, `field_value`
- `metadata.**` matches `metadata.author`, `metadata.tags.primary`
- `*.name` matches `user.name`, `product.name`

### Detected Types

The `MatchMappingType` field accepts these values:

| Type | Description |
|------|-------------|
| `string` | String values |
| `number` | Numeric values (int, float) |
| `boolean` | Boolean values |
| `date` | time.Time values |
| `object` | Nested objects/maps |

## Usage

### Go API

```go
import "github.com/blevesearch/bleve/v2"

// Create an index mapping
mapping := bleve.NewIndexMapping()

// Add a template for keyword fields
mapping.DefaultMapping.AddDynamicTemplate(
    mapping.NewDynamicTemplate("keyword_fields").
        MatchField("*_keyword").
        MatchType("string").
        WithMapping(&mapping.FieldMapping{
            Type:     "text",
            Analyzer: "keyword",
        }),
)

// Add a template for text fields with English analyzer
mapping.DefaultMapping.AddDynamicTemplate(
    mapping.NewDynamicTemplate("english_text").
        MatchField("*_text").
        MatchType("string").
        WithMapping(&mapping.FieldMapping{
            Type:     "text",
            Analyzer: "en",
        }),
)

// Add a template for all strings under metadata path
mapping.DefaultMapping.AddDynamicTemplate(
    mapping.NewDynamicTemplate("metadata_strings").
        MatchPath("metadata.**").
        MatchType("string").
        WithMapping(&mapping.FieldMapping{
            Type:     "text",
            Analyzer: "keyword",
            Store:    true,
        }),
)
```

### JSON Configuration

```json
{
  "default_mapping": {
    "enabled": true,
    "dynamic": true,
    "dynamic_templates": [
      {
        "name": "keyword_fields",
        "match": "*_keyword",
        "match_mapping_type": "string",
        "mapping": {
          "type": "text",
          "analyzer": "keyword"
        }
      },
      {
        "name": "english_text",
        "match": "*_text",
        "match_mapping_type": "string",
        "mapping": {
          "type": "text",
          "analyzer": "en"
        }
      },
      {
        "name": "metadata_strings",
        "path_match": "metadata.**",
        "match_mapping_type": "string",
        "mapping": {
          "type": "text",
          "analyzer": "keyword",
          "store": true
        }
      }
    ]
  }
}
```

## Template Inheritance

Templates are inherited through the document mapping hierarchy:

1. Templates defined at parent mappings are inherited by child mappings
2. Child mappings can define their own templates that take precedence
3. Templates at the closest mapping level are checked first
4. The first matching template wins

```go
// Root-level template
mapping.DefaultMapping.AddDynamicTemplate(
    mapping.NewDynamicTemplate("all_strings").
        MatchType("string").
        WithMapping(&mapping.FieldMapping{
            Type:     "text",
            Analyzer: "standard",
        }),
)

// Sub-document specific template (takes precedence for fields under "logs")
logsMapping := bleve.NewDocumentMapping()
logsMapping.AddDynamicTemplate(
    mapping.NewDynamicTemplate("log_strings").
        MatchType("string").
        WithMapping(&mapping.FieldMapping{
            Type:     "text",
            Analyzer: "keyword", // logs use keyword analyzer
        }),
)
mapping.DefaultMapping.AddSubDocumentMapping("logs", logsMapping)
```

## Template Evaluation Order

1. When a dynamic field is encountered, templates at the closest document mapping are checked first
2. If no match is found, parent templates are checked (inherited templates)
3. Templates within each level are evaluated in the order they were added
4. The first template whose criteria all match is used
5. If no template matches, default dynamic mapping behavior applies

## Examples

### Index Log Levels as Keywords

```go
mapping.DefaultMapping.AddDynamicTemplate(
    mapping.NewDynamicTemplate("log_levels").
        MatchField("level").
        MatchType("string").
        WithMapping(&mapping.FieldMapping{
            Type:     "text",
            Analyzer: "keyword",
            DocValues: true,  // Enable for faceting
        }),
)
```

### Exclude Internal Fields from Full-Text Search

```go
mapping.DefaultMapping.AddDynamicTemplate(
    mapping.NewDynamicTemplate("internal_fields").
        MatchPath("_internal.**").
        WithMapping(&mapping.FieldMapping{
            Type:  "text",
            Index: false,  // Don't index these fields
            Store: true,   // But still store them
        }),
)
```

### Different Analyzers for Different Languages

```go
// English content
mapping.DefaultMapping.AddDynamicTemplate(
    mapping.NewDynamicTemplate("english_content").
        MatchPath("content.en.**").
        MatchType("string").
        WithMapping(&mapping.FieldMapping{
            Type:     "text",
            Analyzer: "en",
        }),
)

// German content
mapping.DefaultMapping.AddDynamicTemplate(
    mapping.NewDynamicTemplate("german_content").
        MatchPath("content.de.**").
        MatchType("string").
        WithMapping(&mapping.FieldMapping{
            Type:     "text",
            Analyzer: "de",
        }),
)
```

### Combine Name and Type Matching

```go
// Only match numeric fields ending in "_count"
mapping.DefaultMapping.AddDynamicTemplate(
    mapping.NewDynamicTemplate("count_fields").
        MatchField("*_count").
        MatchType("number").
        WithMapping(&mapping.FieldMapping{
            Type:      "number",
            DocValues: true,
            Store:     true,
        }),
)
```

## Default Behavior

When a template matches but doesn't explicitly set `Store`, `Index`, or `DocValues`, the global dynamic settings are applied:

- `IndexDynamic` - whether to index dynamic fields (default: true)
- `StoreDynamic` - whether to store dynamic fields (default: true)
- `DocValuesDynamic` - whether to enable doc values for dynamic fields (default: true)

These can be configured at the `IndexMapping` level:

```go
mapping := bleve.NewIndexMapping()
mapping.StoreDynamic = false    // Don't store dynamic fields by default
mapping.IndexDynamic = true     // But still index them
mapping.DocValuesDynamic = true // Enable doc values for sorting/faceting
```

## Strict JSON Validation

When `mapping.MappingJSONStrict = true`, invalid keys in template JSON will cause an error:

```go
mapping.MappingJSONStrict = true

// This will error due to "invalid_key"
jsonData := `{
    "name": "test",
    "invalid_key": "value"
}`
var template mapping.DynamicTemplate
err := json.Unmarshal([]byte(jsonData), &template) // Returns error
```
