# Synonym search

* *v2.5.0* (and after) will come with support for **synonym definition indexing and search**.
* We've achieved this by embedding synonym indexes within our bleve (scorch) indexes.
* Usage of zap file format: [v16](https://github.com/blevesearch/zapx/blob/master/zap.md). Here we co-locate text, vector and synonym indexes as neighbors within segments, continuing to conform to the segmented architecture of *scorch*.

## Supported

* Indexing `Synonym Definitions` allows specifying equivalent terms that will be used to construct the synonym index. There are currently two types of `Synonym Definitions` supported:

    1. Equivalent Mapping:

        In this type, all terms in the *synonyms* list are considered equal and can replace one another. Any of these terms can match a query or document containing any other term in the group, ensuring full synonym coverage.

        ```json
        {
            "synonyms": [
                "tranquil",
                "peaceful",
                "calm",
                "relaxed",
                "unruffled"
            ]
        }
        ```

    2. Explicit Mapping:

        In this mapping, only the terms in the *input* list ("blazing") will have the terms in *synonyms* as their synonyms. The input terms are not equivalent to each other, and the synonym relationship is explicitly directional, applying only from the *input* to the *synonyms*.

        ```json
        {
            "input": [
                "blazing"
            ],
            "synonyms": [
                "intense",
                "radiant",
                "burning",
                "fiery",
                "glowing"
            ]
        }
        ```

* The addition of `Synonym Sources` in the index mapping enables associating a set of `synonym definitions` (called a `synonym collection`) with a specific analyzer. This allows for preprocessing of terms in both the *input* and *synonyms* lists before the synonym index is created. By using an analyzer, you can normalize or transform terms (e.g., case folding, stemming) to improve synonym matching.

    ```json
    {
        "analysis": {
            "synonym_sources": {
                "english": {
                    "collection": "en_thesaurus",
                    "analyzer": "en"
                },
                "german": {
                    "collection": "de_thesaurus",
                    "analyzer": "de"
                }
            }
        }
   }
   ```

    There are two `synonym sources` named "english" and "german," each associated with its respective `synonym collection` and analyzer. In any text field mapping, a `synonym source` can be specified to enable synonym expansion when the field is queried. The analyzer of the synonym source must match the analyzer of the field mapping to which it is applied.

* Any text-based Bleve query (e.g., match, phrase, term, fuzzy, etc.) will use the `synonym source` (if available) for the queried field to expand the search terms using the thesaurus created from user-defined synonym definitions. The behavior for specific query types is as follows:

    1. Queries with `fuzziness` parameter: For queries like match, phrase, and match-phrase that support the `fuzziness` parameter, the queried terms are fuzzily matched with the thesaurus's LHS terms to generate candidate terms. These terms are then combined with the results of fuzzy matching against the field dictionary, which contains the terms present in the queried field.

    2. Wildcard, Regexp, and Prefix queries: These queries follow a similar approach. First, the thesaurus is used to expand terms (e.g., LHS terms that match the prefix or regex). The resulting terms are then combined with candidate terms from dictionary expansion.

## Indexing

Below is an example of using the Bleve API to define synonym sources, index synonym definitions, and associate them with a text field mapping:

```go
// Define a document to be indexed.
doc := struct {
    Text string `json:"text"`
}{
    Text: "hardworking employee",
}

// Define a synonym definition where "hardworking" has equivalent terms.
synDef := &bleve.SynonymDefinition{
    Synonyms: []string{
        "hardworking",
        "industrious",
        "conscientious",
        "persistent",
    },
}

// Define the name of the `synonym collection`.
// This collection groups multiple synonym definitions.
synonymCollection := "collection1"

// Define the name of the `synonym source`.
// This source will be associated with specific field mappings.
synonymSourceName := "english"

// Define the analyzer to process terms in the synonym definitions.
// This analyzer must match the one applied to the field using the synonym source.
analyzer := "en"

// Configure the synonym source by associating it with the synonym collection and analyzer.
synonymSourceConfig := map[string]interface{}{
    "collection": synonymCollection,
    "analyzer":   analyzer,
}

// Create a new index mapping.
bleveMapping := bleve.NewIndexMapping()

// Add the synonym source configuration to the index mapping.
err := bleveMapping.AddSynonymSource(synonymSourceName, synonymSourceConfig)
if err != nil {
    panic(err)
}

// Create a text field mapping with the specified analyzer and synonym source.
textFieldMapping := bleve.NewTextFieldMapping()
textFieldMapping.Analyzer = analyzer
textFieldMapping.SynonymSource = synonymSourceName

// Associate the text field mapping with the "text" field in the default document mapping.
bleveMapping.DefaultMapping.AddFieldMappingsAt("text", textFieldMapping)

// Create a new index with the specified mapping.
index, err := bleve.New("example.bleve", bleveMapping)
if err != nil {
    panic(err)
}

// Index the document into the created index.
err = index.Index("doc1", doc)
if err != nil {
    panic(err)
}

// Check if the index supports synonym indexing and add the synonym definition.
if synIndex, ok := index.(bleve.SynonymIndex); ok {
    err = synIndex.IndexSynonym("synDoc1", synonymCollection, synDef)
    if err != nil {
        panic(err)
    }
} else {
    // If the index does not support synonym indexing, raise an error.
    panic("expected synonym index")
}
```

## Querying

```go
// Query the index created above.
// Create a match query for the term "persistent".
query := bleve.NewMatchQuery("persistent")

// Specify the field to search within, in this case, the "text" field.
query.SetField("text")

// Create a search request with the query and enable explanation to understand how results are scored.
searchRequest := bleve.NewSearchRequest(query)
searchRequest.Explain = true

// Execute the search on the index.
searchResult, err := index.Search(searchRequest)
if err != nil {
    // Handle any errors that occur during the search.
    panic(err)
}

// The search result will contain one match: "doc1". This document includes the term "hardworking", 
// which is a synonym for the queried term "persistent". The synonym relationship is based on 
// the user-defined thesaurus associated with the index.
// Print the search results, which will include the explanation for the match.
fmt.Println(searchResult)
```
